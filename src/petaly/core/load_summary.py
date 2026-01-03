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
        self.summary_list.append({
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
    
    def display(self):
        """
        Displays the load summary table.
        
        Shows all loaded tables with their metrics in a formatted table.
        """
        if not self.summary_list:
            return
        
        logger.info("")
        logger.info("=" * 200)
        logger.info("LOAD SUMMARY")
        logger.info("=" * 200)
        logger.info(f"{'source(connection)':<25} {'source-object':<25} {'target(connection)':<25} {'target-object':<30} {'recreated':<12} {'rows':<12} {'duration (sec)':<15} {'status':<10} {'start-time':<20} {'end-time':<20}")
        logger.info("-" * 200)
        
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
            
            logger.info(f"{source_connection:<25} {source_object:<25} {target_connection:<25} {target_object:<30} {recreated:<12} {rows_str:<12} {duration_sec:<15} {status:<10} {start_time_str:<20} {end_time_str:<20}")
        
        logger.info("=" * 200)
        logger.info("")

