#[cfg(test)]
#[allow(clippy::panic)]
mod test_harness {
    use super::super::format::{
        count_sessions, filter_activity_sessions, format_activity_query, sort_statement_rows,
        sorted_activity_sessions,
    };
    use super::super::{
        ActivityChartMetric, ActivitySession, ActivitySubview, App, CapabilityStatus,
        ConnectionStatus, InputMode, OfflineState, PendingRequest, PendingRequestKind,
        QueryDetailSource, QueryDetailState, QueryStats, RefreshPayload, Tab, clamp_selected_row,
        parse_rate_value,
    };
    use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
    use std::{
        fs,
        path::Path,
        sync::mpsc,
        time::{Duration, Instant},
    };

    fn instant_secs_ago(seconds: u64) -> Instant {
        Instant::now()
            .checked_sub(Duration::from_secs(seconds))
            .unwrap_or_else(Instant::now)
    }

    fn themed_config() -> crate::config::Config {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!("pgmon-theme-{unique}.yaml"));
        let write_result = fs::write(
            &path,
            "theme: sky\nthemes:\n  sky:\n    ui:\n      header_border_color: \"#8fa1b3\"\n      footer_border_color: \"#93a8a3\"\n  mint:\n    ui:\n      header_border_color: \"#9db39d\"\n      footer_border_color: \"#9eb1ac\"\n",
        );
        assert!(write_result.is_ok());

        let config_result = crate::config::Config::load(Some(&path));
        let _ = fs::remove_file(&path);
        assert!(config_result.is_ok());
        let Ok(config) = config_result else {
            panic!("theme config should load");
        };
        config
    }

    #[test]
    fn test_clamp_selected_row_handles_shorter_data() {
        assert_eq!(clamp_selected_row(Some(10), 5), Some(4));
        assert_eq!(clamp_selected_row(Some(2), 5), Some(2));
        assert_eq!(clamp_selected_row(Some(0), 0), None);
    }

    #[test]
    fn test_count_sessions_tracks_waiting_and_idle_states() {
        let sessions = vec![
            create_test_session("1", "active", ""),
            create_test_session("2", "idle", ""),
            create_test_session("3", "active", "Lock: relation"),
        ];
        let counts = count_sessions(&sessions);
        assert_eq!(counts.total, 3);
        assert_eq!(counts.active, 2);
        assert_eq!(counts.idle, 1);
        assert_eq!(counts.waiting, 1);
    }

    #[test]
    fn test_count_sessions_does_not_count_blockers_as_waiting() {
        let mut blocker = create_test_session("1", "active", "");
        blocker.blocked_count = 2;
        let mut waiter = create_test_session("2", "active", "");
        waiter.blocked_by_count = 1;

        let counts = count_sessions(&[blocker, waiter]);

        assert_eq!(counts.waiting, 1);
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_filter_activity_sessions_active_includes_only_active_rows() {
        let sessions = vec![
            create_test_session("1", "active", ""),
            create_test_session("2", "idle", ""),
        ];
        let filtered = filter_activity_sessions(&sessions, ActivitySubview::Active);
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.first().unwrap().pid, "1");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_sorted_activity_sessions_prefers_blocker_count_in_blocking_view() {
        let mut s1 = create_test_session("1", "active", "");
        s1.blocked_count = 5;
        let mut s2 = create_test_session("2", "active", "");
        s2.blocked_count = 10;

        let sorted = sorted_activity_sessions(vec![s1, s2], ActivitySubview::Blocking);
        assert_eq!(sorted.first().unwrap().pid, "2");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_sort_statement_rows_by_calls_desc() {
        let rows = vec![
            vec!["q1".into(), "10".into(), "1".into(), "100".into()],
            vec!["q2".into(), "20".into(), "2".into(), "200".into()],
        ];
        let sorted = sort_statement_rows(rows, "calls");
        assert_eq!(sorted.first().unwrap().first().unwrap(), "q2");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_sort_statement_rows_by_total_time_desc() {
        let rows = vec![
            vec!["q1".into(), "10".into(), "5".into(), "100".into()],
            vec!["q2".into(), "20".into(), "2".into(), "50".into()],
        ];
        let sorted = sort_statement_rows(rows, "total_time");
        assert_eq!(sorted.first().unwrap().first().unwrap(), "q2");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_sort_statement_rows_by_mean_time_desc() {
        let rows = vec![
            vec!["q1".into(), "10".into(), "5".into(), "100".into()],
            vec!["q2".into(), "20".into(), "2".into(), "200".into()],
        ];
        let sorted = sort_statement_rows(rows, "mean_time");
        assert_eq!(sorted.first().unwrap().first().unwrap(), "q1");
    }

    #[test]
    fn test_format_activity_query_returns_replica_slot_label_for_walsender() {
        let mut session = create_test_session("1", "active", "");
        session.backend_type = "walsender".to_string();
        session.query = "START_REPLICATION SLOT \"replica_a\" 0/0 TIMELINE 1".to_string();

        assert_eq!(format_activity_query(&session), "replica replica_a");
    }

    #[test]
    fn test_format_activity_query_preserves_regular_queries() {
        let session = create_test_session("1", "active", "");

        assert_eq!(format_activity_query(&session), "SELECT 1");
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_prepare_table_data_statements_sorting() {
        use super::super::App;
        let app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "statements",
            "calls",
            crate::config::Config::default(),
        );
        let data = vec![
            vec!["q1".into(), "10".into(), "1".into(), "100".into()],
            vec!["q2".into(), "20".into(), "2".into(), "200".into()],
        ];
        let processed = app.prepare_table_data(data);
        assert_eq!(processed.first().unwrap().first().unwrap(), "q2");
    }

    #[test]
    fn test_prepare_table_data_handles_all_tabs() {
        use super::super::App;
        let app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "activity",
            "",
            crate::config::Config::default(),
        );

        let data = vec![vec!["row".into()]];
        let processed = app.prepare_table_data(data.clone());
        assert_eq!(processed.len(), 1);
    }

    #[test]
    fn test_can_export_current_view_only_for_activity_and_statements() {
        let activity_app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "activity",
            "",
            crate::config::Config::default(),
        );
        assert!(activity_app.can_export_current_view());

        let statements_app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "statements",
            "",
            crate::config::Config::default(),
        );
        assert!(statements_app.can_export_current_view());

        let mut database_app = App::new(
            String::new(),
            0,
            None,
            1000,
            10,
            "activity",
            "",
            crate::config::Config::default(),
        );
        database_app.set_tab(Tab::Database);
        assert!(!database_app.can_export_current_view());
    }

    fn create_test_session(pid: &str, state: &str, wait_info: &str) -> ActivitySession {
        ActivitySession {
            pid: pid.to_string(),
            backend_type: "client backend".to_string(),
            xmin: String::new(),
            database: "db".to_string(),
            application: "app".to_string(),
            user: "user".to_string(),
            client: "127.0.0.1".to_string(),
            duration_seconds: 10,
            wait_info: wait_info.to_string(),
            state: state.to_string(),
            query: "SELECT 1".to_string(),
            blocked_by_count: 0,
            blocked_count: 0,
        }
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_query_output_path_uses_tab_name_and_sql_extension() {
        use super::super::query_output_path;
        let path = query_output_path(Tab::Statements, Path::new("/tmp")).unwrap();
        assert!(path.to_string_lossy().contains("statements"));
        assert!(path.to_string_lossy().ends_with(".sql"));
    }

    #[test]
    #[allow(clippy::unwrap_used)]
    fn test_save_query_to_directory_writes_query() {
        use super::super::save_query_to_directory;
        let dir = std::env::temp_dir();
        let query = "SELECT * FROM test;";
        let path = save_query_to_directory(Tab::Activity, query, &dir).unwrap();
        assert!(path.exists());
        let content = std::fs::read_to_string(&path).unwrap();
        assert_eq!(content, format!("{query}\n"));
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn test_handle_key_event_does_not_explain_statement_query() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "statements",
            "total_time",
            crate::config::Config::default(),
        );

        app.query_detail = Some(QueryDetailState {
            query: "SELECT * FROM accounts WHERE id = $1".to_string(),
            database: "postgres".to_string(),
            source: QueryDetailSource::Statements,
            stats: Some(QueryStats {
                total_time: "1".to_string(),
                mean_time: "1".to_string(),
                calls: "1".to_string(),
                read_time: "0".to_string(),
                write_time: "0".to_string(),
            }),
            activity_detail: None,
        });

        app.handle_key_event(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));

        assert!(app.notice_state.is_some());
        assert!(app.query_detail.is_some());
        assert!(app.loading_state.is_none());
        assert!(app.pending_request.is_none());
        assert!(app.explain_plan.is_none());
    }

    #[test]
    fn test_handle_key_event_does_not_explain_normalized_activity_query() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        app.query_detail = Some(QueryDetailState {
            query: "SELECT * FROM accounts WHERE id = $1".to_string(),
            database: "postgres".to_string(),
            source: QueryDetailSource::Activity,
            stats: None,
            activity_detail: None,
        });

        app.handle_key_event(KeyEvent::new(KeyCode::Char('x'), KeyModifiers::NONE));

        assert!(app.notice_state.is_some());
        assert!(app.query_detail.is_some());
        assert!(app.loading_state.is_none());
        assert!(app.pending_request.is_none());
        assert!(app.explain_plan.is_none());
    }

    #[test]
    fn test_handle_refresh_result_keeps_last_data_when_refresh_fails_after_connect() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "statements",
            "total_time",
            crate::config::Config::default(),
        );
        app.has_loaded_once = true;
        app.last_refresh = instant_secs_ago(3);
        app.data = vec![vec!["existing".to_string()]];

        let (tx, rx) = mpsc::channel();
        app.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: Instant::now(),
        });
        let send_result = tx.send(Err(anyhow::anyhow!(
            "connection lost while refreshing activity"
        )));
        assert!(send_result.is_ok());

        app.handle_refresh_result();

        assert!(app.error_state.is_none());
        assert!(app.is_offline());
        assert_eq!(app.data, vec![vec!["existing".to_string()]]);
        assert!(app.pending_request.is_none());
    }

    #[test]
    fn test_handle_refresh_result_startup_failure_remains_blocking() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        let (tx, rx) = mpsc::channel();
        app.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: Instant::now(),
        });
        let send_result = tx.send(Err(anyhow::anyhow!("startup connect failed")));
        assert!(send_result.is_ok());

        app.handle_refresh_result();

        assert!(app.error_state.is_some());
        assert!(!app.is_offline());
    }

    #[test]
    fn test_handle_refresh_result_success_clears_offline_state() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.has_loaded_once = true;
        app.connection_status = ConnectionStatus::Offline(OfflineState {
            last_error: "connection lost".to_string(),
            failed_attempts: 2,
            next_retry_at: Instant::now() + Duration::from_secs(5),
            last_successful_refresh_at: Some(instant_secs_ago(10)),
        });

        let (tx, rx) = mpsc::channel();
        app.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: instant_secs_ago(2),
        });
        let send_result = tx.send(Ok(RefreshPayload::Explain(vec!["Seq Scan".to_string()])));
        assert!(send_result.is_ok());

        app.handle_refresh_result();

        assert!(!app.is_offline());
        assert!(app.error_state.is_none());
        assert!(app.explain_plan.is_some());
    }

    #[test]
    fn test_handle_refresh_result_disconnected_refresh_sets_offline() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.has_loaded_once = true;
        app.last_refresh = instant_secs_ago(2);

        let (tx, rx) = mpsc::channel::<anyhow::Result<RefreshPayload>>();
        drop(tx);
        app.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: Instant::now(),
        });

        app.handle_refresh_result();

        assert!(app.is_offline());
        assert!(app.error_state.is_none());
    }

    #[test]
    fn test_handle_key_event_manual_reconnect_starts_request() {
        let mut app = App::new(
            "not a dsn".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.has_loaded_once = true;
        app.connection_status = ConnectionStatus::Offline(OfflineState {
            last_error: "connection lost".to_string(),
            failed_attempts: 1,
            next_retry_at: Instant::now() + Duration::from_secs(30),
            last_successful_refresh_at: Some(instant_secs_ago(5)),
        });

        app.handle_key_event(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE));

        assert!(app.pending_request.is_some());
    }

    #[test]
    fn test_handle_key_event_opens_refresh_modal_with_restored_options() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));

        let Some(modal) = app.refresh_interval_modal.as_ref() else {
            panic!("refresh modal should open");
        };
        assert_eq!(
            modal.options,
            vec![500, 1000, 2000, 3000, 4000, 5000, 10000]
        );
        assert_eq!(modal.selected_index, 1);
    }

    #[test]
    fn test_handle_key_event_refresh_modal_preselects_and_applies_500ms() {
        let mut app = App::new(
            "not a dsn".to_string(),
            3000,
            None,
            500,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('r'), KeyModifiers::NONE));

        let Some(modal) = app.refresh_interval_modal.as_ref() else {
            panic!("refresh modal should open");
        };
        assert_eq!(modal.selected_index, 0);

        app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert_eq!(app.refresh_ms, 500);
        assert!(app.refresh_interval_modal.is_none());
    }

    #[test]
    fn test_handle_key_event_cycles_activity_chart_metric() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        assert_eq!(app.activity_chart_metric, ActivityChartMetric::Connections);

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(app.activity_chart_metric, ActivityChartMetric::Tps);

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(app.activity_chart_metric, ActivityChartMetric::Dml);

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(
            app.activity_chart_metric,
            ActivityChartMetric::TempBytesPerSec
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(
            app.activity_chart_metric,
            ActivityChartMetric::GrowthBytesPerSec
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));
        assert_eq!(app.activity_chart_metric, ActivityChartMetric::Connections);
    }

    #[test]
    fn test_handle_key_event_does_not_cycle_metric_outside_activity() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.set_tab(Tab::Statements);

        app.handle_key_event(KeyEvent::new(KeyCode::Char('m'), KeyModifiers::NONE));

        assert_eq!(app.activity_chart_metric, ActivityChartMetric::Connections);
    }

    #[test]
    fn test_parse_rate_value_defaults_invalid_values_to_zero() {
        assert!(parse_rate_value("-").abs() < f64::EPSILON);
        assert!(parse_rate_value("").abs() < f64::EPSILON);
        assert!(parse_rate_value("bad").abs() < f64::EPSILON);
        assert!((parse_rate_value("12.5") - 12.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_push_activity_chart_history_parses_summary_rates() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.dashboard.summary.session_counts.active = 3;
        app.dashboard.summary.session_counts.idle = 5;
        app.dashboard.summary.session_counts.total = 8;
        app.dashboard.summary.rates.tps = "12.5".to_string();
        app.dashboard.summary.rates.inserts_per_sec = "1.5".to_string();
        app.dashboard.summary.rates.updates_per_sec = "2.5".to_string();
        app.dashboard.summary.rates.deletes_per_sec = "3.5".to_string();
        app.dashboard.summary.rates.temp_bytes_per_sec = "-".to_string();
        app.dashboard.summary.rates.growth_bytes_per_sec = "2048".to_string();

        app.push_activity_chart_history();

        assert_eq!(
            app.dashboard.chart_history.connections.back(),
            Some(&(3, 5, 8))
        );
        assert_eq!(app.dashboard.chart_history.tps.back(), Some(&12.5));
        assert_eq!(
            app.dashboard.chart_history.inserts_per_sec.back(),
            Some(&1.5)
        );
        assert_eq!(
            app.dashboard.chart_history.updates_per_sec.back(),
            Some(&2.5)
        );
        assert_eq!(
            app.dashboard.chart_history.deletes_per_sec.back(),
            Some(&3.5)
        );
        assert_eq!(
            app.dashboard.chart_history.temp_bytes_per_sec.back(),
            Some(&0.0)
        );
        assert_eq!(
            app.dashboard.chart_history.growth_bytes_per_sec.back(),
            Some(&2048.0)
        );
    }

    #[test]
    fn test_handle_key_event_opens_theme_modal_when_themes_exist() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            themed_config(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT));

        assert!(app.theme_modal.is_some());
    }

    #[test]
    fn test_handle_key_event_opens_theme_modal_with_builtin_themes() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT));

        assert!(app.theme_modal.is_some());
    }

    #[test]
    fn test_handle_key_event_applies_selected_theme() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            themed_config(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('T'), KeyModifiers::SHIFT));
        let Some(modal) = app.theme_modal.as_mut() else {
            panic!("theme modal should open");
        };
        modal.selected_index = 0;
        if modal.options.first().is_some_and(|name| name == "mint") {
            modal.selected_index = 0;
        } else {
            modal.selected_index = 1;
        }

        app.handle_key_event(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));

        assert!(app.theme_modal.is_none());
        assert_eq!(app.active_theme_name(), Some("mint"));
        assert_eq!(app.config.ui.header_border_color, "#9db39d");
        assert!(app.notice_state.is_none());
    }

    #[test]
    fn test_handle_key_event_opens_contextual_help_modal() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));

        let Some(help) = app.help_modal.as_ref() else {
            panic!("help modal should open");
        };
        assert!(help.title.contains("Activity"));
        assert!(
            help.sections
                .iter()
                .any(|section| { section.lines.iter().any(|line| line.contains("DML/s")) })
        );
    }

    #[test]
    fn test_handle_key_event_closes_help_modal() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));

        app.handle_key_event(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));

        assert!(app.help_modal.is_none());
    }

    #[test]
    fn test_handle_key_event_opens_statements_help_with_explain_note() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "statements",
            "total_time",
            crate::config::Config::default(),
        );

        app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));

        let Some(help) = app.help_modal.as_ref() else {
            panic!("help modal should open");
        };
        assert!(help.sections.iter().any(|section| {
            section
                .lines
                .iter()
                .any(|line| line.contains("Explain is intentionally unavailable"))
        }));
    }

    #[test]
    fn test_handle_key_event_help_modal_includes_capability_reason() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "statements",
            "total_time",
            crate::config::Config::default(),
        );
        app.capabilities.statements =
            CapabilityStatus::Unavailable("pg_stat_statements extension is missing.".to_string());

        app.handle_key_event(KeyEvent::new(KeyCode::Char('?'), KeyModifiers::NONE));

        let Some(help) = app.help_modal.as_ref() else {
            panic!("help modal should open");
        };
        assert!(help.sections.iter().any(|section| {
            section
                .lines
                .iter()
                .any(|line| line.contains("pg_stat_statements extension is missing"))
        }));
    }

    #[test]
    fn test_handle_request_failure_refresh_tracks_offline_backoff() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.has_loaded_once = true;
        app.last_refresh = instant_secs_ago(4);

        app.handle_request_failure(
            PendingRequestKind::Refresh,
            "connection lost\nwith extra detail".to_string(),
        );

        let Some(offline) = app.offline_state() else {
            panic!("refresh failure should set offline state");
        };
        assert_eq!(offline.last_error, "connection lost");
        assert_eq!(offline.failed_attempts, 1);
        assert!(offline.next_retry_at > Instant::now());
        assert!(app.connection_health.last_error.is_some());
        assert!(app.pg_client.is_none());
    }

    #[test]
    fn test_can_manual_reconnect_requires_normal_mode_and_no_pending_request() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.connection_status = ConnectionStatus::Offline(OfflineState {
            last_error: "lost".to_string(),
            failed_attempts: 1,
            next_retry_at: Instant::now() + Duration::from_secs(1),
            last_successful_refresh_at: Some(instant_secs_ago(1)),
        });
        assert!(app.can_manual_reconnect());

        app.input_mode = InputMode::Search;
        assert!(!app.can_manual_reconnect());

        app.input_mode = InputMode::Normal;
        let (_tx, rx) = mpsc::channel();
        app.pending_request = Some(PendingRequest {
            kind: PendingRequestKind::Refresh,
            rx,
            started_at: Instant::now(),
        });
        assert!(!app.can_manual_reconnect());
    }

    #[test]
    fn test_current_view_capability_returns_statements_status() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "statements",
            "total_time",
            crate::config::Config::default(),
        );
        app.capabilities.statements =
            CapabilityStatus::Unavailable("pg_stat_statements is not installed.".to_string());

        let capability = app.current_view_capability();

        assert!(matches!(
            capability,
            Some(CapabilityStatus::Unavailable(reason))
                if reason == "pg_stat_statements is not installed."
        ));
    }

    #[test]
    fn test_effective_refresh_interval_uses_observed_latency() {
        let mut app = App::new(
            "postgres://localhost/postgres".to_string(),
            3000,
            None,
            1000,
            10,
            "activity",
            "total_time",
            crate::config::Config::default(),
        );
        app.connection_health.last_refresh_duration = Some(Duration::from_millis(2400));

        assert!(app.high_latency_detected());
        assert_eq!(
            app.effective_refresh_interval(),
            Duration::from_millis(2400)
        );
    }
}
