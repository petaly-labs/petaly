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

PETALY_TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "list_pipelines",
            "description": "List all available data pipelines in Petaly",
            "parameters": {
                "type": "object",
                "properties": {},
                "required": []
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "create_pipeline",
            "description": "Create a new data pipeline for moving data between systems",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "description": "Name for the new pipeline"
                    },
                    "source_type": {
                        "type": "string",
                        "description": "Type of source connector (postgres, mysql, csv, etc.)"
                    },
                    "source_config": {
                        "type": "object",
                        "description": "Configuration details for the source connector"
                    },
                    "target_type": {
                        "type": "string",
                        "description": "Type of target connector (postgres, mysql, csv, etc.)"
                    },
                    "target_config": {
                        "type": "object",
                        "description": "Configuration details for the target connector"
                    },
                    "objects": {
                        "type": "array",
                        "description": "List of data objects to transfer",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["pipeline_name", "source_type", "target_type"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "run_pipeline",
            "description": "Execute a specific pipeline to transfer data",
            "parameters": {
                "type": "object",
                "properties": {
                    "pipeline_name": {
                        "type": "string",
                        "description": "Name of the pipeline to run"
                    },
                    "endpoint": {
                        "type": "string",
                        "description": "Run only source, only target, or both (leave empty for both)",
                        "enum": ["source", "target", ""]
                    },
                    "objects": {
                        "type": "array",
                        "description": "List of specific objects to process (optional)",
                        "items": {
                            "type": "string"
                        }
                    }
                },
                "required": ["pipeline_name"]
            }
        }
    }
]