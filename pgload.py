#!/usr/bin/env python3
"""
pgload.py - PostgreSQL load generator for pgmon testing.

Usage:
    python3 pgload.py [DSN]

Default DSN: postgresql://postgres:postgres@127.0.0.1:5432/postgres

This script creates a mixed workload:
  - idle / idle-in-transaction / blocking / deadlock sessions for Activity demos
  - larger demo tables with intentionally missing secondary indexes so pgmon can
    surface slow statements, generic plans, obvious sequential scans, and
    explicit demo index status

Connection breakdown:
  IDLE_CONNS       = 10  open connections, never run a query     → state: idle
  IDLE_IN_TX       = 10  open transaction, never committed       → state: idle in transaction
  BLOCKING_ITX     = 5   idle in transaction holding a lock      → state: idle in transaction (blocking others)
  BLOCKED_SESSIONS = 5   trying to get a lock held by blocker    → state: active (wait_event_type: Lock)
  DEADLOCK_WORKERS = 4   trying to cause a deadlock periodically
  ACTIVE_WORKERS   = 10  run realistic read/write queries via pool → state: active / idle
"""

import os
import random
import signal
import sys
import textwrap
import threading
import time

import psycopg2
from psycopg2 import pool as pg_pool

DEFAULT_DSN     = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
DSN             = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PGMON_DSN", DEFAULT_DSN)

# ── tunables ──────────────────────────────────────────────────────────────────
IDLE_CONNS       = 10
IDLE_IN_TX       = 10
BLOCKING_ITX     = 5
BLOCKED_SESSIONS = 5
DEADLOCK_WORKERS = 4
ACTIVE_WORKERS   = 10
POOL_MIN         =  5
POOL_MAX         = 15
SLOW_EVERY       = 12
LONG_QUERY_PROBABILITY = 0.05
INDEX_STATUS_INTERVAL  = 30
DEMO_ACCOUNTS    = 15_000
DEMO_ORDERS      = 180_000
DEMO_AUDIT_ROWS  = 300_000
# ─────────────────────────────────────────────────────────────────────────────

stop_event = threading.Event()
_lock      = threading.Lock()
_print_lock = threading.Lock()
stats      = {"queries": 0, "errors": 0}

REGIONS = ("us-east", "us-west", "eu-central", "ap-south")
ORDER_STATUSES = ("pending", "paid", "shipped", "cancelled")
DEMO_INDEXES = (
    (
        "pgload_accounts",
        "(email)",
        "using btree (email)",
        "CREATE INDEX ON pgload_accounts (email);",
    ),
    (
        "pgload_accounts",
        "(tenant_id, created_at DESC)",
        "using btree (tenant_id, created_at desc)",
        "CREATE INDEX ON pgload_accounts (tenant_id, created_at DESC);",
    ),
    (
        "pgload_orders",
        "(account_id, created_at DESC)",
        "using btree (account_id, created_at desc)",
        "CREATE INDEX ON pgload_orders (account_id, created_at DESC);",
    ),
    (
        "pgload_orders",
        "(status, created_at DESC)",
        "using btree (status, created_at desc)",
        "CREATE INDEX ON pgload_orders (status, created_at DESC);",
    ),
    (
        "pgload_audit_log",
        "(account_id, created_at DESC)",
        "using btree (account_id, created_at desc)",
        "CREATE INDEX ON pgload_audit_log (account_id, created_at DESC);",
    ),
)

# ── scratch table ─────────────────────────────────────────────────────────────
def setup():
    c = psycopg2.connect(DSN, application_name="pgload-setup")
    c.autocommit = True
    c.cursor().execute("""
        CREATE TABLE IF NOT EXISTS pgload_scratch (
            id  SERIAL PRIMARY KEY,
            val DOUBLE PRECISION,
            ts  TIMESTAMPTZ DEFAULT now()
        );
        CREATE TABLE IF NOT EXISTS pgload_deadlock (
            id INT PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS pgload_accounts (
            id          SERIAL PRIMARY KEY,
            tenant_id   INT NOT NULL,
            email       TEXT NOT NULL,
            region      TEXT NOT NULL,
            status      TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL,
            last_seen_at TIMESTAMPTZ NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pgload_orders (
            id          BIGSERIAL PRIMARY KEY,
            account_id  INT NOT NULL REFERENCES pgload_accounts(id),
            status      TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL,
            total       NUMERIC(10,2) NOT NULL,
            shipped_at  TIMESTAMPTZ,
            notes       TEXT
        );
        CREATE TABLE IF NOT EXISTS pgload_audit_log (
            id          BIGSERIAL PRIMARY KEY,
            account_id  INT NOT NULL REFERENCES pgload_accounts(id),
            event_type  TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL,
            payload     TEXT NOT NULL
        );

        TRUNCATE
            pgload_audit_log,
            pgload_orders,
            pgload_accounts,
            pgload_scratch,
            pgload_deadlock
        RESTART IDENTITY CASCADE;

        INSERT INTO pgload_scratch (val)
            SELECT random()*1000 FROM generate_series(1,500);

        INSERT INTO pgload_deadlock VALUES (1), (2);

        INSERT INTO pgload_accounts (tenant_id, email, region, status, created_at, last_seen_at)
        SELECT
            1 + mod(i, 50),
            'user' || lpad(i::text, 5, '0') || '@example.test',
            (ARRAY['us-east', 'us-west', 'eu-central', 'ap-south'])[1 + mod(i, 4)],
            (ARRAY['active', 'trial', 'suspended'])[1 + mod(i, 3)],
            now() - (mod(i, 365) || ' days')::interval - (mod(i, 86400) || ' seconds')::interval,
            now() - (mod(i, 90) || ' days')::interval - (mod(i, 7200) || ' seconds')::interval
        FROM generate_series(1, %(accounts)s) s(i);

        INSERT INTO pgload_orders (account_id, status, created_at, total, shipped_at, notes)
        SELECT
            1 + mod(i * 37, %(accounts)s),
            (ARRAY['pending', 'pending', 'pending', 'paid', 'paid', 'shipped', 'cancelled'])[1 + mod(i, 7)],
            now() - (mod(i, 180) || ' days')::interval - (mod(i, 86400) || ' seconds')::interval,
            round((10 + mod(i, 5000))::numeric / 3, 2),
            CASE WHEN mod(i, 7) = 5 THEN now() - (mod(i, 120) || ' days')::interval ELSE NULL END,
            repeat(md5((i * 17)::text), 2)
        FROM generate_series(1, %(orders)s) s(i);

        INSERT INTO pgload_audit_log (account_id, event_type, created_at, payload)
        SELECT
            1 + mod(i * 53, %(accounts)s),
            (ARRAY['login', 'page_view', 'cart_add', 'checkout', 'support', 'refund'])[1 + mod(i, 6)],
            now() - (mod(i, 120) || ' days')::interval - (mod(i, 86400) || ' seconds')::interval,
            repeat(md5((i * 97)::text), 2)
        FROM generate_series(1, %(audit_rows)s) s(i);

        ANALYZE pgload_scratch;
        ANALYZE pgload_accounts;
        ANALYZE pgload_orders;
        ANALYZE pgload_audit_log;
    """, {
        "accounts": DEMO_ACCOUNTS,
        "orders": DEMO_ORDERS,
        "audit_rows": DEMO_AUDIT_ROWS,
    })
    c.close()
    print("[pgload] demo tables ready (secondary indexes intentionally missing)", file=sys.stderr)

def teardown():
    try:
        c = psycopg2.connect(DSN, application_name="pgload-teardown")
        c.autocommit = True
        c.cursor().execute("""
            DROP TABLE IF EXISTS
                pgload_audit_log,
                pgload_orders,
                pgload_accounts,
                pgload_scratch,
                pgload_deadlock
            CASCADE;
        """)
        c.close()
    except Exception as e:
        print(f"[pgload] teardown: {e}", file=sys.stderr)

# ── queries ───────────────────────────────────────────────────────────────────
def log_lines(lines, leading_newline=False):
    with _print_lock:
        if leading_newline:
            sys.stderr.write("\n")
        for line in lines:
            sys.stderr.write(f"{line}\n")
        sys.stderr.flush()


def normalize_definition(definition):
    return " ".join(definition.lower().split())


def fetch_demo_index_definitions():
    conn = psycopg2.connect(DSN, application_name="pgload-index-status")
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("""
        SELECT tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = current_schema()
          AND tablename IN ('pgload_accounts', 'pgload_orders', 'pgload_audit_log')
        ORDER BY tablename, indexname
    """)
    definitions = {}
    for table_name, indexdef in cur.fetchall():
        definitions.setdefault(table_name, set()).add(normalize_definition(indexdef))
    conn.close()
    return definitions


def print_demo_index_status(reason, leading_newline=False):
    try:
        definitions = fetch_demo_index_definitions()
        lines = [f"[pgload] demo index status ({reason}):"]
        for table_name, label, match_tail, create_stmt in DEMO_INDEXES:
            present = any(
                indexdef.endswith(match_tail)
                for indexdef in definitions.get(table_name, set())
            )
            status = "PRESENT" if present else "MISSING"
            lines.append(f"[pgload]   {status:<7} {table_name} {label}")
            if not present:
                lines.append(f"[pgload]     fix: {create_stmt}")
        log_lines(lines, leading_newline=leading_newline)
    except Exception as exc:
        log_lines(
            [f"[pgload] demo index status ({reason}): unavailable ({exc})"],
            leading_newline=leading_newline,
        )


def random_account_id():
    return random.randint(1, DEMO_ACCOUNTS)


def random_email(account_id=None):
    account_id = account_id or random_account_id()
    return f"user{account_id:05d}@example.test"


def build_fast_query():
    account_id = random_account_id()
    return random.choice([
        ("SELECT count(*) FROM pgload_scratch", None),
        ("SELECT avg(val), max(val) FROM pgload_scratch", None),
        ("SELECT * FROM pgload_scratch ORDER BY val DESC LIMIT 10", None),
        ("SELECT count(*) FROM pg_stat_activity", None),
        ("SELECT count(*) FROM pg_locks", None),
        ("SELECT sum(xact_commit) FROM pg_stat_database", None),
        ("UPDATE pgload_scratch SET val=random()*1000 WHERE id = %s",
         (random.randint(1, 500),)),
        ("INSERT INTO pgload_scratch (val) VALUES (%s)",
         (random.uniform(1, 1000),)),
        ("SELECT id, email, region FROM pgload_accounts WHERE email = %s",
         (random_email(account_id),)),
        ("SELECT id, account_id, status, created_at "
         "FROM pgload_orders WHERE account_id = %s "
         "ORDER BY created_at DESC LIMIT 25",
         (account_id,)),
        ("SELECT count(*) FROM pgload_orders "
         "WHERE status = %s AND created_at >= now() - interval '7 days'",
         (random.choice(ORDER_STATUSES),)),
        ("SELECT event_type, count(*) "
         "FROM pgload_audit_log "
         "WHERE account_id = %s AND created_at >= now() - interval '30 days' "
         "GROUP BY event_type ORDER BY count(*) DESC",
         (account_id,)),
        ("SELECT id, tenant_id, email "
         "FROM pgload_accounts "
         "WHERE tenant_id = %s AND created_at >= now() - interval '60 days' "
         "ORDER BY created_at DESC LIMIT 40",
         (random.randint(1, 50),)),
    ])


def build_long_query():
    account_id = random_account_id()
    return random.choice([
        (
            "/* pgload-long:accounts */ "
            "SELECT a.id, a.tenant_id, a.email, a.region, a.status, a.created_at, a.last_seen_at, "
            "CASE "
            "WHEN a.last_seen_at >= now() - interval '1 day' THEN 'hot' "
            "WHEN a.last_seen_at >= now() - interval '7 days' THEN 'warm' "
            "WHEN a.last_seen_at >= now() - interval '30 days' THEN 'cool' "
            "ELSE 'stale' "
            "END AS activity_bucket, "
            "to_char(a.created_at, 'YYYY-MM-DD HH24:MI:SS TZ') AS created_label, "
            "to_char(a.last_seen_at, 'YYYY-MM-DD HH24:MI:SS TZ') AS last_seen_label, "
            "coalesce(nullif(substr(a.email, 1, 32), ''), 'n/a') AS email_preview "
            "FROM pgload_accounts a "
            "WHERE a.tenant_id = %s "
            "AND a.created_at >= now() - interval '60 days' "
            "ORDER BY a.created_at DESC LIMIT 40",
            (random.randint(1, 50),),
        ),
        (
            "/* pgload-long:audit */ "
            "SELECT l.account_id, l.event_type, count(*) AS event_count, "
            "min(l.created_at) AS first_seen_at, max(l.created_at) AS last_seen_at, "
            "left(string_agg(substr(l.payload, 1, 12), ',' ORDER BY l.created_at DESC), 120) "
            "AS payload_preview "
            "FROM pgload_audit_log l "
            "WHERE l.account_id = %s "
            "AND l.created_at >= now() - interval '30 days' "
            "GROUP BY l.account_id, l.event_type "
            "ORDER BY event_count DESC, last_seen_at DESC, l.event_type",
            (account_id,),
        ),
    ])


def build_slow_query():
    account_id = random_account_id()
    return random.choice([
        ("SELECT pg_sleep(%s)", (round(random.uniform(2, 5), 1),)),
        ("WITH delay AS (SELECT pg_sleep(%s)) "
         "SELECT count(*) FROM pgload_scratch, delay",
         (round(random.uniform(2, 5), 1),)),
        ("SELECT count(*) FROM pgload_scratch a, pgload_scratch b WHERE a.val + b.val > 999",
         None),
        ("SELECT account_id, count(*) "
         "FROM pgload_orders "
         "WHERE created_at >= now() - interval '30 days' "
         "GROUP BY account_id ORDER BY count(*) DESC LIMIT 50",
         None),
        ("SELECT event_type, count(*) "
         "FROM pgload_audit_log "
         "WHERE account_id = %s AND created_at >= now() - interval '90 days' "
         "GROUP BY event_type ORDER BY count(*) DESC",
         (account_id,)),
        ("SELECT a.region, count(*) "
         "FROM pgload_orders o "
         "JOIN pgload_accounts a ON a.id = o.account_id "
         "WHERE o.status = %s AND o.created_at >= now() - interval '30 days' "
         "GROUP BY a.region ORDER BY count(*) DESC",
         (random.choice(ORDER_STATUSES),)),
    ])


def execute_query(cur, sql, params):
    if params is None:
        cur.execute(sql)
    else:
        cur.execute(sql, params)

    if cur.description is not None:
        cur.fetchall()


def print_demo_hints():
    log_lines(textwrap.dedent(f"""
        [pgload] demo data        : accounts={DEMO_ACCOUNTS:,} orders={DEMO_ORDERS:,} audit={DEMO_AUDIT_ROWS:,}
        [pgload] index status     : startup + every {INDEX_STATUS_INTERVAL}s
        [pgload] pgmon workflow   : open Statements, inspect pgload_* queries with i:Info, then x:Explain
        [pgload] missing-index demo queries:
        [pgload]   - SELECT ... FROM pgload_accounts WHERE email = ?
        [pgload]   - SELECT ... FROM pgload_orders WHERE account_id = ? ORDER BY created_at DESC
        [pgload]   - SELECT ... FROM pgload_orders WHERE status = ? AND created_at >= ...
        [pgload]   - SELECT ... FROM pgload_audit_log WHERE account_id = ? AND created_at >= ...
        [pgload] formatting demo  : look for /* pgload-long:* */ statements in pgmon
    """).strip().splitlines())


def index_status_worker():
    while not stop_event.wait(INDEX_STATUS_INTERVAL):
        print_demo_index_status("periodic", leading_newline=True)

# ── workers ───────────────────────────────────────────────────────────────────

def idle_holder(wid):
    """Open one connection and do absolutely nothing. Always state=idle."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-idle-{wid:02d}")
        stop_event.wait()
        conn.close()
    except Exception:
        pass


def idle_in_tx_holder(wid):
    """Open a transaction and never commit. Always state=idle in transaction."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-itx-{wid:02d}")
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM pgload_scratch")
        stop_event.wait()
        conn.rollback()
        conn.close()
    except Exception:
        pass

def blocking_itx_holder(wid):
    """Idle in transaction while holding an exclusive row lock."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-blocker-{wid:02d}")
        conn.autocommit = False
        cur = conn.cursor()
        row_id = (wid % BLOCKING_ITX) + 1
        cur.execute(f"SELECT * FROM pgload_scratch WHERE id = {row_id} FOR UPDATE")
        stop_event.wait()
        conn.rollback()
        conn.close()
    except Exception:
        pass

def blocked_session_holder(wid):
    """Try to update a row held by a blocker. Will show as active & waiting on Lock."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-blocked-{wid:02d}")
        conn.autocommit = False
        cur = conn.cursor()
        row_id = (wid % BLOCKING_ITX) + 1
        cur.execute(f"UPDATE pgload_scratch SET val = val + 1 WHERE id = {row_id}")
        if not stop_event.is_set():
            conn.commit()
        conn.close()
    except Exception:
        pass

def deadlock_worker(wid):
    """Attempt to cause a deadlock by locking rows in different orders."""
    while not stop_event.is_set():
        conn = None
        try:
            conn = psycopg2.connect(DSN, application_name=f"pgload-deadlock-{wid:02d}")
            conn.autocommit = False
            cur = conn.cursor()

            # Switch order based on worker ID to cause deadlock
            first, second = (1, 2) if wid % 2 == 0 else (2, 1)

            cur.execute(f"SELECT * FROM pgload_deadlock WHERE id = {first} FOR UPDATE")
            time.sleep(1) # Wait for others to get their first lock
            cur.execute(f"SELECT * FROM pgload_deadlock WHERE id = {second} FOR UPDATE")

            conn.commit()
        except psycopg2.errors.DeadlockDetected:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
        except Exception:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
        time.sleep(random.uniform(2, 5))


def active_worker(wid, the_pool):
    """Borrow conn from pool → run query → return → sleep. Oscillates active↔idle."""
    last_slow = time.time() + random.uniform(0, SLOW_EVERY)
    while not stop_event.is_set():
        conn = None
        try:
            conn = the_pool.getconn()
            conn.autocommit = False
            now = time.time()
            cur = conn.cursor()
            if random.random() < LONG_QUERY_PROBABILITY:
                sql, params = build_long_query()
            elif now - last_slow >= SLOW_EVERY:
                sql, params = build_slow_query()
                last_slow = now
            else:
                sql, params = build_fast_query()
            execute_query(cur, sql, params)
            conn.commit()
            with _lock:
                stats["queries"] += 1
        except pg_pool.PoolError:
            pass
        except Exception:
            with _lock:
                stats["errors"] += 1
            try:
                conn and conn.rollback()
            except Exception:
                pass
        finally:
            if conn:
                try:
                    the_pool.putconn(conn)
                except Exception:
                    pass
        time.sleep(random.uniform(0.4, 1.5))


def printer():
    start = time.time()
    while not stop_event.is_set():
        time.sleep(3)
        elapsed = int(time.time() - start)
        with _lock:
            q, e = stats["queries"], stats["errors"]
        try:
            c = psycopg2.connect(DSN, application_name="pgload-mon")
            c.autocommit = True
            cur = c.cursor()
            cur.execute("""
                SELECT coalesce(state,'bg'), count(*)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                GROUP BY 1 ORDER BY 2 DESC
            """)
            counts = "  ".join(f"{s}:{n}" for s, n in cur.fetchall())
            c.close()
        except Exception:
            counts = "?"
        with _print_lock:
            sys.stderr.write(
                f"\r[{elapsed:4d}s]  {counts}   queries={q} errors={e}   "
            )
            sys.stderr.flush()


def handle_signal(sig, frame):
    log_lines(["[pgload] stopping…"], leading_newline=True)
    stop_event.set()


# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    total = IDLE_CONNS + IDLE_IN_TX + BLOCKING_ITX + BLOCKED_SESSIONS + DEADLOCK_WORKERS + ACTIVE_WORKERS
    print(textwrap.dedent(f"""
        [pgload] DSN            : {DSN}
        [pgload] idle holders   : {IDLE_CONNS:>3}
        [pgload] idle-in-tx     : {IDLE_IN_TX:>3}
        [pgload] blocking itx   : {BLOCKING_ITX:>3}
        [pgload] blocked sessions: {BLOCKED_SESSIONS:>3}
        [pgload] deadlock workers: {DEADLOCK_WORKERS:>3}
        [pgload] active workers : {ACTIVE_WORKERS:>3}
        [pgload] Ctrl-C to stop
    """), file=sys.stderr)

    setup()
    print_demo_hints()
    print_demo_index_status("startup")

    the_pool = pg_pool.ThreadedConnectionPool(POOL_MIN, POOL_MAX, DSN,
                                              application_name="pgload-pool")

    threads = []
    for i in range(IDLE_CONNS):
        t = threading.Thread(target=idle_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(IDLE_IN_TX):
        t = threading.Thread(target=idle_in_tx_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(BLOCKING_ITX):
        t = threading.Thread(target=blocking_itx_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(BLOCKED_SESSIONS):
        t = threading.Thread(target=blocked_session_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(DEADLOCK_WORKERS):
        t = threading.Thread(target=deadlock_worker, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(ACTIVE_WORKERS):
        t = threading.Thread(target=active_worker, args=(i, the_pool), daemon=True)
        t.start(); threads.append(t)

    printer_thread = threading.Thread(target=printer, daemon=True)
    printer_thread.start()
    status_thread = threading.Thread(target=index_status_worker, daemon=True)
    status_thread.start()

    stop_event.wait()
    for t in threads:
        t.join(timeout=1)
    status_thread.join(timeout=1)

    the_pool.closeall()
    teardown()
    log_lines(["[pgload] done."], leading_newline=True)
