# Copyright © 2024-2026 Pavel Rabaev
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)

import logging
from datetime import datetime
logger = logging.getLogger(__name__)


class LoadSummary:
    """
    Manages and displays load summary information for pipeline execution.
    
    This class handles:
    - Collecting summary data for each loaded table
    - Displaying a formatted summary table at the end of loading
    
    Summary includes:
    - Source connection and object
    - Target connection and object
    - Recreated status
    - Number of rows loaded
    - Duration
    - Status (success/failed)
    """
    
    def __init__(self):
        """Initialize the LoadSummary instance."""
        self.summary_list = []

    def _make_entry_key(self, source_connection, source_object, target_connection, target_object):
        return (source_connection, source_object, target_connection, target_object)

    def _find_existing_entry(self, source_connection, source_object, target_connection, target_object):
        entry_key = self._make_entry_key(source_connection, source_object, target_connection, target_object)
        for summary in self.summary_list:
            if summary.get('_entry_key') == entry_key:
                return summary
        return None
    
    def add_entry(self, source_connection, source_object, target_connection, target_object,
                   recreated, rows_loaded, duration_sec, status, start_time, end_time):
        """
        Adds an entry to the summary list.
        
        Args:
            source_connection: Source connection name (or connector_type if inline)
            source_object: Source object name
            target_connection: Target connection name (or connector_type if inline)
            target_object: Target object name (schema.table)
            recreated: Boolean indicating if table was recreated
            rows_loaded: Number of rows loaded (or None if failed/unavailable)
            duration_sec: Duration in seconds
            status: 'success' or 'failed'
            start_time: Start time timestamp (float or datetime)
            end_time: End time timestamp (float or datetime)
        """
        existing_entry = self._find_existing_entry(
            source_connection, source_object, target_connection, target_object
        )

        if existing_entry is None:
            self.summary_list.append({
                '_entry_key': self._make_entry_key(source_connection, source_object, target_connection, target_object),
                'source_connection': source_connection,
                'source_object': source_object,
                'target_connection': target_connection,
                'target_object': target_object,
                'recreated': recreated,
                'rows_loaded': rows_loaded,
                'duration_sec': duration_sec,
                'status': status,
                'start_time': start_time,
                'end_time': end_time
            })
            return

        existing_entry['recreated'] = bool(existing_entry.get('recreated')) or bool(recreated)

        existing_rows = existing_entry.get('rows_loaded')
        if existing_rows is None:
            existing_entry['rows_loaded'] = rows_loaded
        elif rows_loaded is not None:
            existing_entry['rows_loaded'] = existing_rows + rows_loaded

        existing_entry['duration_sec'] = round(
            float(existing_entry.get('duration_sec', 0) or 0) + float(duration_sec or 0), 2
        )

        if existing_entry.get('status') != 'failed' and status == 'failed':
            existing_entry['status'] = 'failed'

        if existing_entry.get('start_time') is None or (start_time is not None and start_time < existing_entry.get('start_time')):
            existing_entry['start_time'] = start_time

        if existing_entry.get('end_time') is None or (end_time is not None and end_time > existing_entry.get('end_time')):
            existing_entry['end_time'] = end_time
    
    def display(self):
        """
        Displays the load summary table.
        
        Shows all loaded tables with their metrics in a formatted table.
        """
        if not self.summary_list:
            return
        
        # Calculate total width: 25 + 32 + 25 + 30 + 12 + 12 + 15 + 10 + 20 + 20 = 201
        total_width = 201
        logger.info("")
        logger.info("=" * total_width)
        logger.info("LOAD SUMMARY")
        logger.info("=" * total_width)
        logger.info(f"{'source(connection)':<25} {'source-object':<32} {'target(connection)':<25} {'target-object':<30} {'recreated':<12} {'rows':<12} {'duration (sec)':<15} {'status':<10} {'start-time':<20} {'end-time':<20}")
        logger.info("-" * total_width)
        
        for summary in self.summary_list:
            source_connection = summary.get('source_connection', 'N/A')
            source_object = summary.get('source_object', 'N/A')
            target_connection = summary.get('target_connection', 'N/A')
            target_object = summary.get('target_object', 'N/A')
            recreated = 'true' if summary.get('recreated') else 'false'
            rows_loaded = summary.get('rows_loaded')
            rows_str = str(rows_loaded) if rows_loaded is not None else 'N/A'
            duration_sec = summary.get('duration_sec', 0)
            status = summary.get('status', 'unknown')
            
            # Format start_time and end_time
            start_time = summary.get('start_time')
            end_time = summary.get('end_time')
            
            if start_time:
                if isinstance(start_time, float):
                    start_time_str = datetime.fromtimestamp(start_time).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    start_time_str = str(start_time)
            else:
                start_time_str = 'N/A'
            
            if end_time:
                if isinstance(end_time, float):
                    end_time_str = datetime.fromtimestamp(end_time).strftime('%Y-%m-%d %H:%M:%S')
                else:
                    end_time_str = str(end_time)
            else:
                end_time_str = 'N/A'
            
            logger.info(f"{source_connection:<25} {source_object:<32} {target_connection:<25} {target_object:<30} {recreated:<12} {rows_str:<12} {duration_sec:<15} {status:<10} {start_time_str:<20} {end_time_str:<20}")
        
        logger.info("=" * total_width)
        logger.info("")
