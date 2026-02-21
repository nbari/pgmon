#!/usr/bin/env python3
"""
pgload.py - PostgreSQL load generator for pgmon testing.

Usage:
    python3 pgload.py [DSN]

Default DSN: postgresql://postgres:postgres@127.0.0.1:5432/postgres

Connection breakdown (50 total by default):
  IDLE_CONNS    = 30  open connections, never run a query     → state: idle
  IDLE_IN_TX    = 10  open transaction, never committed       → state: idle in transaction
  ACTIVE_WORKERS = 10  run queries in a loop via pool         → state: active / idle
"""

import os, sys, time, random, signal, threading, textwrap
import psycopg2
from psycopg2 import pool as pg_pool

DEFAULT_DSN     = "postgresql://postgres:postgres@127.0.0.1:5432/postgres"
DSN             = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("PGMON_DSN", DEFAULT_DSN)

# ── tunables ──────────────────────────────────────────────────────────────────
IDLE_CONNS      = 30   # always-idle connections (state=idle, never query)
IDLE_IN_TX      = 10   # always idle-in-transaction (state=idle in transaction)
ACTIVE_WORKERS  = 10   # workers querying via a connection pool
POOL_MIN        =  5   # minimum connections kept open in pool
POOL_MAX        = 15   # pool ceiling
SLOW_EVERY      = 12   # seconds between slow queries per worker
# ─────────────────────────────────────────────────────────────────────────────

stop_event = threading.Event()
_lock      = threading.Lock()
stats      = {"queries": 0, "errors": 0}

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
        TRUNCATE pgload_scratch;
        INSERT INTO pgload_scratch (val)
            SELECT random()*1000 FROM generate_series(1,500);
    """)
    c.close()
    print("[pgload] scratch table ready", file=sys.stderr)

def teardown():
    try:
        c = psycopg2.connect(DSN, application_name="pgload-teardown")
        c.autocommit = True
        c.cursor().execute("DROP TABLE IF EXISTS pgload_scratch;")
        c.close()
    except Exception as e:
        print(f"[pgload] teardown: {e}", file=sys.stderr)

# ── queries ───────────────────────────────────────────────────────────────────
FAST = [
    "SELECT count(*) FROM pgload_scratch",
    "SELECT avg(val), max(val) FROM pgload_scratch",
    "SELECT * FROM pgload_scratch ORDER BY val DESC LIMIT 10",
    "SELECT count(*) FROM pg_stat_activity",
    "SELECT count(*) FROM pg_locks",
    "SELECT sum(xact_commit) FROM pg_stat_database",
    "UPDATE pgload_scratch SET val=random()*1000 "
        "WHERE id=(SELECT id FROM pgload_scratch ORDER BY random() LIMIT 1)",
    "INSERT INTO pgload_scratch (val) VALUES (random()*1000)",
]

SLOW = [
    "SELECT pg_sleep({s})",
    "SELECT pg_sleep({s}), count(*) FROM pgload_scratch",
    "SELECT count(*) FROM pgload_scratch a, pgload_scratch b WHERE a.val+b.val > 999",
]

# ── workers ───────────────────────────────────────────────────────────────────

def idle_holder(wid):
    """Open one connection and do absolutely nothing. Always state=idle."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-idle-{wid:02d}")
        stop_event.wait()
        conn.close()
    except Exception as e:
        print(f"[idle-{wid}] {e}", file=sys.stderr)


def idle_in_tx_holder(wid):
    """Open a transaction and never commit. Always state=idle in transaction."""
    try:
        conn = psycopg2.connect(DSN, application_name=f"pgload-itx-{wid:02d}")
        conn.autocommit = False
        conn.cursor().execute("SELECT pg_backend_pid()")   # starts implicit tx
        stop_event.wait()
        conn.rollback()
        conn.close()
    except Exception as e:
        print(f"[itx-{wid}] {e}", file=sys.stderr)


def active_worker(wid, the_pool):
    """Borrow conn from pool → run query → return → sleep. Oscillates active↔idle."""
    last_slow = time.time() + random.uniform(0, SLOW_EVERY)
    while not stop_event.is_set():
        conn = None
        try:
            conn = the_pool.getconn()
            conn.autocommit = False
            now = time.time()
            if now - last_slow >= SLOW_EVERY:
                s = round(random.uniform(2, 5), 1)
                conn.cursor().execute(random.choice(SLOW).format(s=s))
                last_slow = now
            else:
                cur = conn.cursor()
                cur.execute(random.choice(FAST))
                cur.fetchall()
            conn.commit()
            with _lock:
                stats["queries"] += 1
        except pg_pool.PoolError:
            pass   # pool exhausted, skip this tick
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
        # quick live count from postgres
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
        print(
            f"\r[{elapsed:4d}s]  {counts}   queries={q} errors={e}   ",
            end="", file=sys.stderr, flush=True,
        )


def handle_signal(sig, frame):
    print("\n[pgload] stopping…", file=sys.stderr)
    stop_event.set()


# ── main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    signal.signal(signal.SIGINT,  handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    total = IDLE_CONNS + IDLE_IN_TX + ACTIVE_WORKERS
    print(textwrap.dedent(f"""
        [pgload] DSN            : {DSN}
        [pgload] idle holders   : {IDLE_CONNS:>3}  (state = idle)
        [pgload] idle-in-tx     : {IDLE_IN_TX:>3}  (state = idle in transaction)
        [pgload] active workers : {ACTIVE_WORKERS:>3}  (state oscillates active ↔ idle)
        [pgload] pool           : min={POOL_MIN} max={POOL_MAX}
        [pgload] total conns    : ~{total}
        [pgload] Ctrl-C to stop
    """), file=sys.stderr)

    setup()

    # test connection + print current state before starting
    c = psycopg2.connect(DSN)
    cur = c.cursor()
    cur.execute("SELECT coalesce(state,'bg'), count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() GROUP BY 1")
    print("[pgload] pg_stat_activity before load:", dict(cur.fetchall()), file=sys.stderr)
    c.close()

    the_pool = pg_pool.ThreadedConnectionPool(POOL_MIN, POOL_MAX, DSN,
                                              application_name="pgload-pool")

    threads = []
    for i in range(IDLE_CONNS):
        t = threading.Thread(target=idle_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(IDLE_IN_TX):
        t = threading.Thread(target=idle_in_tx_holder, args=(i,), daemon=True)
        t.start(); threads.append(t)

    for i in range(ACTIVE_WORKERS):
        t = threading.Thread(target=active_worker, args=(i, the_pool), daemon=True)
        t.start(); threads.append(t)

    time.sleep(1)   # let connections settle

    c2 = psycopg2.connect(DSN)
    cur2 = c2.cursor()
    cur2.execute("SELECT coalesce(state,'bg'), count(*) FROM pg_stat_activity WHERE pid<>pg_backend_pid() GROUP BY 1")
    print("[pgload] pg_stat_activity after  load:", dict(cur2.fetchall()), file=sys.stderr)
    c2.close()

    threading.Thread(target=printer, daemon=True).start()

    stop_event.wait()
    for t in threads:
        t.join(timeout=3)

    the_pool.closeall()
    teardown()
    print("\n[pgload] done.", file=sys.stderr)
