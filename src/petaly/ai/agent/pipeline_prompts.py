# Copyright © 2024-2025 Pavel Rabaev
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Dict, Any

class PipelinePrompts:
    """
    Collection of prompts used for pipeline-related operations.
    Provides structured prompts for various pipeline tasks.
    Ensures consistent formatting and response structure.
    """
    
    @staticmethod
    def get_classification_prompt() -> str:
        """
        Get the prompt for classifying user instructions.
        
        Logic:
        1. Define instruction categories
        2. Specify response format
        3. Include required fields
        """
        return """
        You are an AI assistant for Petaly, a command-line ETL tool. Analyze the user's instruction and identify what they want to do.

        Categorize the instruction into one of these types:
        1. List pipelines - User wants to list existing pipelines (all or filtered by endpoint type)
        2. Create pipeline - User wants to create a new pipeline (requires: name, source, target)
        3. Run pipeline - User wants to run an existing pipeline (requires: name)
        4. Modify pipeline - User wants to modify an existing pipeline (requires: name)
        5. List output - User wants to see pipeline output files (requires: name)
        6. Show output - User wants to see the file text of pipeline output files (requires: file-name or pipeline-name)
        7. Other - Any other type of request

        Format your response as a JSON with these fields:
        - instruction_type: The category of the instruction (list_pipelines, create_pipeline, run_pipeline, modify_pipeline, list_output, other)
        - details: {
            "pipeline_name": "name if specified",
            "endpoint_type": "filter type if specified for listing",
            "source_endpoint": "source type if specified for creation",
            "target_endpoint": "target type if specified for creation"
        }
        - next_action: Proposed next steps to fulfill the request
        """

    @staticmethod
    def get_pipeline_list_prompt() -> str:
        """
        Get the prompt for listing pipelines.
        
        Logic:
        1. Define list types
        2. Specify response format
        3. Include filtering options
        """
        return """
        You are helping list Petaly pipelines. Format your response as a JSON with these fields:
        - list_type: "all" or "filtered"
        - endpoint_type: Type of endpoint to filter by (if filtered)
        - include_details: Boolean indicating whether to show detailed information
        """

    @staticmethod
    def get_pipeline_creation_prompt() -> str:
        """
        Get the prompt for creating a new pipeline.
        
        Logic:
        1. Define pipeline structure
        2. Specify JSON/YAML format
        3. Include required fields
        4. Add data objects spec
        """
        return """
        You are helping create a new data pipeline in Petaly. Based on the user's requirements, generate a detailed pipeline file in YAML format.
        
        If the pipeline configuration would use JSON format, it should follow this structure:
        {
            "pipeline": {
                "pipeline_attributes": {
                    "pipeline_name": "descriptive_name",
                    "is_enabled": true
                },
                "source_attributes": {
                    "connector_type": "source_type",
                    "database_user": "username",
                    "database_password": "password",
                    "database_host": "hostname",
                    "database_port": port_number,
                    "database_name": "database_name"
                },
                "target_attributes": {
                    "connector_type": "target_type",
                    "database_user": "username",
                    "database_password": "password",
                    "database_host": "hostname",
                    "database_port": port_number,
                    "database_name": "database_name",
                    "database_schema": "schema_name"
                },
                "data_attributes": {
                    "data_objects_spec_mode": "only",
                    "object_default_settings": {
                        "header": true,
                        "columns_delimiter": ",",
                        "columns_quote": "double"
                    }
                }
            }
        }
        
        For data objects specification:
        {
            "data_objects_spec": [
                {
                    "object_spec": {
                        "object_name": "source_table_name",
                        "destination_object_name": "target_table_name",
                        "recreate_destination_object": false,
                        "cleanup_linebreak_in_fields": false,
                        "exclude_columns": []
                    }
                }
            ]
        }
        
        Format your response as a JSON with these fields:
        - pipeline_name: Suggested name for the pipeline (use snake_case)
        - source: {
            "connector_type": "mysql|postgres|etc",
            "connection_details": {
                "database_user": "username",
                "database_password": "password",
                "database_host": "hostname",
                "database_port": port_number,
                "database_name": "database_name"
            }
        }
        - target: {
            "connector_type": "mysql|postgres|etc",
            "connection_details": {
                "database_user": "username",
                "database_password": "password",
                "database_host": "hostname",
                "database_port": port_number,
                "database_name": "database_name",
                "database_schema": "schema_name"
            }
        }
        - data_objects: [
            {
                "name": "source_table_name",
                "destination_name": "target_table_name",
                "recreate": false,
                "exclude_columns": []
            }
        ]
        - needs_more_info: Boolean indicating if more information is needed
        - user_message: Message to request more information if needed
        """

    @staticmethod
    def get_pipeline_modification_prompt() -> str:
        """
        Get the prompt for modifying an existing pipeline.
        
        Logic:
        1. Define modification structure
        2. Include impact analysis
        3. Add validation fields
        """
        return """
        You are helping modify an existing Petaly pipeline. Format your response as a JSON with these fields:
        - pipeline_name: Name of the pipeline to modify
        - modifications: List of changes to make
        - impact_analysis: Analysis of the changes' impact
        - needs_more_info: Boolean indicating if more information is needed
        - user_message: Message to request more information if needed
        """

    @staticmethod
    def get_pipeline_execution_prompt() -> str:
        """
        Get the prompt for executing a pipeline.
        
        Logic:
        1. Define execution parameters
        2. Specify endpoint options
        3. Include object selection
        """
        return """
        You are helping execute a Petaly pipeline. Format your response as a JSON with these fields:
        - pipeline_name: Name of the pipeline to run
        - run_endpoint: Which endpoint to run ('source', 'target', or None for both)
        - object_names: List of specific objects to process
        - parameters: Additional execution parameters
        """

    @staticmethod
    def get_pipeline_output_prompt() -> str:
        """
        Get the prompt for showing pipeline output.
        
        Logic:
        1. Define output types
        2. Include file patterns
        3. Specify response format
        """
        return """
        You are helping show Petaly pipeline output files. Format your response as a JSON with these fields:
        - pipeline_name: Name of the pipeline
        - output_type: Type of output to show (e.g., "latest", "all", "specific_date")
        - file_pattern: Pattern to match output files (optional)
        """

    @staticmethod
    def get_pipeline_summary_prompt(pipeline_config: Dict[str, Any]) -> str:
        """
        Get the prompt for generating a human-friendly pipeline summary.
        
        Logic:
        1. Include pipeline config
        2. Request concise summary
        3. Specify output format
        """
        return f"""
        Generate a concise summary of this Petaly pipeline configuration. Make it human-friendly and easy to understand.
        Focus on the key aspects of the pipeline and its purpose.
        
        Pipeline configuration:
        {pipeline_config}
        
        Format your response as a clear, well-structured text description.
        """

    @staticmethod
    def get_pipeline_name_extraction_prompt() -> str:
        """
        Get the prompt for extracting pipeline names from user instructions.
        
        Logic:
        1. Request name extraction
        2. Specify output format
        3. Remove extra text
        """
        return """
        Extract the name of the pipeline that the user wants to work with from their instruction.
        Return just the pipeline name, without any additional text or explanation.
        """ 