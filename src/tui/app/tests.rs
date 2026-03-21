#[cfg(test)]
mod test_harness {
    use super::super::format::{
        count_sessions, filter_activity_sessions, sort_statement_rows, sorted_activity_sessions,
    };
    use super::super::{ActivitySession, ActivitySubview, Tab, clamp_selected_row};
    use std::path::Path;

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
    fn test_prepare_table_data_statements_sorting() {
        use super::super::App;
        let app = App::new(String::new(), 0, None, 1000, 10, "statements", "calls");
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
        let app = App::new(String::new(), 0, None, 1000, 10, "activity", "");

        let data = vec![vec!["row".into()]];
        let processed = app.prepare_table_data(data.clone());
        assert_eq!(processed.len(), 1);
    }

    fn create_test_session(pid: &str, state: &str, wait_info: &str) -> ActivitySession {
        ActivitySession {
            pid: pid.to_string(),
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
}
