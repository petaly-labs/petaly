from petaly.core.load_summary import LoadSummary


class TestLoadSummary:
    def test_add_entry_aggregates_repeated_object_rows(self):
        summary = LoadSummary()

        summary.add_entry(
            source_connection='postgres_source',
            source_object='name_basics_incr',
            target_connection='mysql_target',
            target_object='name_basics_incr',
            recreated=False,
            rows_loaded=10000,
            duration_sec=0.45,
            status='success',
            start_time=100.0,
            end_time=100.45,
        )
        summary.add_entry(
            source_connection='postgres_source',
            source_object='name_basics_incr',
            target_connection='mysql_target',
            target_object='name_basics_incr',
            recreated=False,
            rows_loaded=2403,
            duration_sec=0.22,
            status='success',
            start_time=100.46,
            end_time=100.68,
        )

        assert len(summary.summary_list) == 1
        entry = summary.summary_list[0]
        assert entry['rows_loaded'] == 12403
        assert entry['duration_sec'] == 0.67
        assert entry['start_time'] == 100.0
        assert entry['end_time'] == 100.68
        assert entry['status'] == 'success'
