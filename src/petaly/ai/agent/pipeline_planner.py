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

import logging
import json
import os
import re
from typing import Dict, List, Any, Optional, Union, TypedDict

from petaly.ai.agent.llm_connector import LLMConnector
from petaly.utils.file_handler import FileHandler

logger = logging.getLogger(__name__)

# Constants for configuration keys
class ConfigKeys:
    PIPELINE = "pipeline"
    PIPELINE_ATTRIBUTES = "pipeline_attributes"
    SOURCE_ATTRIBUTES = "source_attributes"
    TARGET_ATTRIBUTES = "target_attributes"
    DATA_ATTRIBUTES = "data_attributes"
    DATA_OBJECTS_SPEC = "data_objects_spec"
    OBJECT_SPEC = "object_spec"
    CONNECTORS = "connectors"
    PLATFORM_SPECIFIC = "platform_specific"
    CONNECTOR_TYPE = "connector_type"
    DETAILS = "details"
    PIPELINE_NAME = "pipeline_name"
    IS_ENABLED = "is_enabled"
    DATA_OBJECTS_SPEC_MODE = "data_objects_spec_mode"
    OBJECT_DEFAULT_SETTINGS = "object_default_settings"

class PlatformConfigs:
    GCP = {
        "gcp_project_id": "gcp_project_id",
        "gcp_region": "gcp_region",
        "gcp_bucket_name": "gcp_bucket_name",
        "bucket_pipeline_prefix": "bucket_pipeline_prefix"
    }
    
    AWS = {
        "aws_account": "aws_account",
        "aws_bucket_name": "aws_bucket_name",
        "bucket_pipeline_prefix": "bucket_pipeline_prefix",
        "aws_iam_role": "aws_iam_role",
        "aws_profile_name": "aws_profile_name",
        "aws_access_key_id": "aws_access_key_id",
        "aws_secret_access_key": "aws_secret_access_key",
        "aws_region": "aws_region"
    }

class RedshiftConfigs:
    CONNECTION_METHOD = "connection_method"
    IS_SERVERLESS = "is_serverless"
    CLUSTER_IDENTIFIER = "cluster_identifier"
    WORKGROUP_NAME = "workgroup_name"

class PipelinePlan(TypedDict):
    pipeline_name: str
    source: Dict[str, Any]
    target: Dict[str, Any]
    data_objects: List[Union[str, Dict[str, Any]]]
    transformations: Optional[List[str]]
    needs_more_info: bool
    user_message: Optional[str]

class PipelinePlanner:
    """Generates plans and configurations for Petaly pipelines based on natural language instructions."""
    
    def __init__(self, llm: LLMConnector, main_config: Dict[str, Any]):
        """
        Initialize the pipeline planner.
        
        Args:
            llm: LLM connector for natural language processing
            main_config: Petaly's main configuration
        """
        self.llm = llm
        self.m_conf = main_config
        self.file_handler = FileHandler()
        self.connector_configs = self._load_connector_configs()
        
    def _load_connector_configs(self) -> Dict[str, Any]:
        """Load available connector configurations from the system."""
        return self.file_handler.load_json(self.m_conf.class_sysconfig_fpath)
    
    def create_pipeline_plan(self, instruction: str, system_prompt: Optional[str] = None) -> Dict[str, Any]:
        """
        Create a plan for a new pipeline based on natural language instruction.
        
        Args:
            instruction: User's natural language instruction
            system_prompt: Optional custom system prompt to override the default
            
        Returns:
            Dictionary containing the pipeline plan
        """
        # Generate prompt for pipeline planning
        available_connectors = list(self.connector_configs.get("connectors", {}).keys())
        
        if system_prompt is None:
            system_prompt = f"""
            You are an AI assistant for Petaly, an ETL tool. You'll create a pipeline plan from the user's instruction.
            
            Available connectors: {', '.join(available_connectors)}
            
            Analyze the instruction and extract:
            1. Pipeline name (create a short, descriptive name if not specified)
            2. Source system (type and details)
            3. Target system (type and details)
            4. Data objects to transfer (tables, files, etc.)
            5. Any transformation requirements
            
            Format your response as a JSON with these fields:
            - pipeline_name: A short descriptive name for the pipeline (use snake_case)
            - source: {{
                "connector_type": One of the available connectors,
                "details": Extracted connection details
            }}
            - target: {{
                "connector_type": One of the available connectors,
                "details": Extracted connection details
            }}
            - data_objects: Array of data objects to transfer
            - transformations: Any transformations needed
            - needs_more_info: true/false if more information is needed
            - user_message: Message asking for more information if needed
            
            If the information provided is insufficient, set needs_more_info to true and include what information is missing.
            """
        
        # Get plan from LLM
        plan_result = self.llm.generate_completion(
            prompt=instruction,
            system_prompt=system_prompt,
            max_tokens=1000
        )
        
        try:
            plan = json.loads(plan_result)
            return plan
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing pipeline plan: {e}")
            logger.debug(f"Raw plan result: {plan_result}")
            # Return a default plan with error
            return {
                "needs_more_info": True,
                "user_message": "I'm having trouble understanding your request. Could you provide more specific details about the source and target systems for your pipeline?"
            }
    
    def generate_pipeline_config(self, source_config: Dict[str, Any], target_config: Dict[str, Any], 
                               objects: List[str] = None) -> Dict[str, Any]:
        """Generate a pipeline configuration from source and target configs."""
        # First, validate the plan
        if source_config.get("needs_more_info", False) or target_config.get("needs_more_info", False):
            raise ValueError("Cannot generate configuration: more information needed")
        
        # Get pipeline metadata structure template
        pipeline_meta = self.file_handler.load_json(self.m_conf.pipeline_meta_config_fpath)
        
        # Extract source and target details
        source_details = source_config.get("details", {})
        target_details = target_config.get("details", {})
        
        # Build source attributes with platform-specific details
        source_attributes = self._build_connector_attributes(
            source_details,
            source_config.get("connector_type", ""),
            "source"
        )
        
        # Add platform-specific source attributes
        if "platform_specific" in source_details:
            source_attributes.update(source_details["platform_specific"])
        
        # Build target attributes with platform-specific details
        target_attributes = self._build_connector_attributes(
            target_details,
            target_config.get("connector_type", ""),
            "target"
        )
        
        # Add platform-specific target attributes
        if "platform_specific" in target_details:
            target_attributes.update(target_details["platform_specific"])
        
        # Handle platform-specific configurations
        platform_type = target_attributes.get("platform_type")
        platform_configs = {
            "gcp": {
                "gcp_project_id": target_details.get("gcp_project_id"),
                "gcp_region": target_details.get("gcp_region"),
                "gcp_bucket_name": target_details.get("gcp_bucket_name"),
                "bucket_pipeline_prefix": target_details.get("bucket_pipeline_prefix")
            },
            "aws": {
                "aws_account": target_details.get("aws_account"),
                "aws_bucket_name": target_details.get("aws_bucket_name"),
                "bucket_pipeline_prefix": target_details.get("bucket_pipeline_prefix"),
                "aws_iam_role": target_details.get("aws_iam_role"),
                "aws_profile_name": target_details.get("aws_profile_name"),
                "aws_access_key_id": target_details.get("aws_access_key_id"),
                "aws_secret_access_key": target_details.get("aws_secret_access_key"),
                "aws_region": target_details.get("aws_region")
            }
        }
        
        # Add platform-specific configurations
        if platform_type in platform_configs:
            for key, value in platform_configs[platform_type].items():
                if value is not None:
                    target_attributes[key] = value
        
        # Handle Redshift specific configurations
        if target_attributes.get("connector_type") == "redshift":
            redshift_configs = {
                "connection_method": target_details.get("connection_method"),
                "is_serverless": target_details.get("is_serverless"),
                "cluster_identifier": target_details.get("cluster_identifier"),
                "workgroup_name": target_details.get("workgroup_name")
            }
            for key, value in redshift_configs.items():
                if value is not None:
                    target_attributes[key] = value
        
        # Build data objects specification
        data_objects_spec = []
        for obj in objects or []:
            if isinstance(obj, str):
                # Simple object name
                spec = {
                    "object_spec": {
                        "object_name": obj,
                        "recreate_destination_object": False,
                        "cleanup_linebreak_in_fields": False,
                        "exclude_columns": [None]
                    }
                }
            else:
                # Object with details
                object_attributes = {
                    "object_name": obj.get("name"),
                    "recreate_destination_object": obj.get("recreate", False),
                    "cleanup_linebreak_in_fields": obj.get("cleanup_linebreak", False),
                    "exclude_columns": obj.get("exclude_columns", [None]),
                    "destination_object_name": obj.get("destination_name"),
                    "object_source_dir": obj.get("source_dir"),
                    "file_names": obj.get("file_names"),
                    "load_mode": obj.get("load_mode"),
                    "load_batch_size": obj.get("load_batch_size"),
                    "column_for_incremental_load": obj.get("column_for_incremental_load")
                }
                
                # Remove None values
                object_attributes = {k: v for k, v in object_attributes.items() if v is not None}
                
                spec = {"object_spec": object_attributes}
            
            data_objects_spec.append(spec)
        
        # Construct the pipeline configuration
        pipeline_config = [
            {
                "pipeline": {
                    "pipeline_attributes": {
                        "pipeline_name": source_config.get("pipeline_name", "new_pipeline"),
                        "is_enabled": True
                    },
                    "source_attributes": source_attributes,
                    "target_attributes": target_attributes,
                    "data_attributes": {
                        "data_objects_spec_mode": "only",
                        "object_default_settings": self._build_default_settings(source_config)
                    }
                }
            },
            {
                "data_objects_spec": data_objects_spec
            }
        ]
        
        return pipeline_config
    
    def _build_connector_attributes(self, details: Dict[str, Any], connector_type: str, connector_category: str) -> Dict[str, Any]:
        """
        Build connector attributes based on type and category.
        All parameters are optional and can be empty strings, except for essential decision-making parameters.
        Always includes connector_type and other essential attributes.
        
        Args:
            details: Connector details from the plan
            connector_type: Type of connector (e.g., 'mysql', 'postgres')
            connector_category: Category of connector (e.g., 'database', 'file')
            
        Returns:
            Dict of connector attributes
        """
        attributes = {}
        
        # Essential parameters that must be specified
        if not connector_type:
            raise ValueError("connector_type is required")
            
        # Always include connector type and platform type
        attributes["connector_type"] = connector_type
        
        # Set platform type based on connector
        if connector_type in ["bigquery", "gs"]:
            attributes["platform_type"] = "gcp"
            attributes.update(self._build_gcp_attributes(details))
        elif connector_type in ["redshift", "s3"]:
            attributes["platform_type"] = "aws"
            attributes.update(self._build_aws_attributes(details))
        elif connector_type in ["mysql", "postgres"]:
            attributes["platform_type"] = "local"
            # Always include database attributes for local databases
            attributes.update(self._build_database_attributes(details, connector_type))
            
        # Handle connector-specific attributes
        if connector_type == "redshift":
            attributes.update(self._build_redshift_attributes(details))
        elif connector_type == "bigquery":
            attributes.update(self._build_bigquery_attributes(details))
        elif connector_type == "csv":
            attributes.update(self._build_csv_attributes(details))
        elif connector_category == "database" and connector_type not in ["bigquery", "redshift"]:
            # Database attributes are already included above for local databases
            pass
            
        return attributes
        
    def _build_gcp_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build GCP-specific attributes. All parameters are optional."""
        return {
            "gcp_project_id": details.get("gcp_project_id", ""),
            "gcp_region": details.get("gcp_region", ""),
            "gcp_bucket_name": details.get("gcp_bucket_name", ""),
            "bucket_pipeline_prefix": details.get("bucket_pipeline_prefix", "")
        }
        
    def _build_aws_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build AWS-specific attributes. All parameters are optional."""
        return {
            "aws_region": details.get("aws_region", ""),
            "aws_bucket_name": details.get("aws_bucket_name", ""),
            "bucket_pipeline_prefix": details.get("bucket_pipeline_prefix", ""),
            "aws_iam_role": details.get("aws_iam_role", ""),
            "aws_profile_name": details.get("aws_profile_name", ""),
            "aws_access_key_id": details.get("aws_access_key_id", ""),
            "aws_secret_access_key": details.get("aws_secret_access_key", "")
        }
        
    def _build_redshift_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build Redshift-specific attributes. All parameters are optional except connection_method and is_serverless."""
        attributes = {
            "connection_method": details.get("connection_method", "iam"),
            "is_serverless": details.get("is_serverless", False)
        }
        
        if attributes["is_serverless"]:
            attributes["workgroup_name"] = details.get("workgroup_name", "")
        else:
            attributes["cluster_identifier"] = details.get("cluster_identifier", "")
            
        return attributes
        
    def _build_bigquery_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build BigQuery-specific attributes. All parameters are optional."""
        return {
            "database_schema": details.get("database_schema", "")
        }
        
    def _build_csv_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build CSV-specific attributes. Only destination_dir is required for target."""
        role = details.get("role", "").lower()
        if "source" in role:
            return {
                "object_source_dir": details.get("object_source_dir", ""),
                "file_names": details.get("file_names", [])
            }
        elif "target" in role:
            return {"destination_dir": details.get("destination_dir", "")}
        return {}
        
    def _build_database_attributes(self, details: Dict[str, Any], connector_type: str = "") -> Dict[str, Any]:
        """
        Build common database attributes. All parameters are optional.
        For MySQL, database_schema is not included.
        
        Args:
            details: Database connection details
            connector_type: Type of database connector (e.g., 'mysql', 'postgres')
            
        Returns:
            Dict of database attributes
        """
        # Always include all database parameters with empty defaults
        attributes = {
            "database_host": details.get("database_host", ""),
            "database_port": details.get("database_port", ""),
            "database_name": details.get("database_name", ""),
            "database_user": details.get("database_user", ""),
            "database_password": details.get("database_password", ""),
            "database_schema": details.get("database_schema", "")
        }
        
        # Remove schema for MySQL
        if connector_type == "mysql":
            attributes.pop("database_schema", None)
            
        return attributes

    def _load_connector_specific_attributes(self, connector_type: str, details: Dict[str, Any]) -> Dict[str, Any]:
        """Load and process connector-specific attributes from connector_attributes.json."""
        attributes = {}
        
        # Get connector configuration
        connector_config = self.connector_configs.get("connectors", {}).get(connector_type, {})
        connector_dpath = connector_config.get("connector_dpath", "").replace(".", "/")
        
        # Construct path to connector_attributes.json
        connector_attr_path = os.path.join(
            self.m_conf.base_dpath, 
            connector_dpath, 
            "config", 
            "connector_attributes.json"
        )
        
        if os.path.exists(connector_attr_path):
            try:
                connector_attributes = self.file_handler.load_json(connector_attr_path)
                
                # Process main attributes
                for key, attr_config in connector_attributes.items():
                    if key == "object_connector_settings":
                        continue
                        
                    # Skip attributes where in_use is false
                    if not attr_config.get("in_use", False):
                        continue
                        
                    # Check dependencies
                    if self._check_dependencies(attr_config.get("dependency"), details):
                        # Get value from details if present, otherwise use default
                        value = details.get(key)
                        if value is None and attr_config.get("default_value") is not None:
                            value = attr_config.get("default_value")
                        
                        if value is not None:
                            # Convert value type if needed
                            value = self._convert_value_type(value, attr_config.get("key_type"))
                            attributes[key] = value
                
                # Process object connector settings
                if "object_connector_settings" in connector_attributes:
                    object_settings = {}
                    for key, attr_config in connector_attributes["object_connector_settings"].items():
                        # Skip attributes where in_use is false
                        if not attr_config.get("in_use", False):
                            continue
                            
                        # Check dependencies
                        if self._check_dependencies(attr_config.get("dependency"), details):
                            # Get value from details if present, otherwise use default
                            value = details.get(key)
                            if value is None and attr_config.get("default_value") is not None:
                                value = attr_config.get("default_value")
                            
                            if value is not None:
                                # Convert value type if needed
                                value = self._convert_value_type(value, attr_config.get("key_type"))
                                object_settings[key] = value
                    
                    if object_settings:
                        attributes["object_connector_settings"] = object_settings
                        
            except Exception as e:
                logger.warning(f"Failed to load connector attributes from {connector_attr_path}: {e}")
        
        return attributes

    def _check_dependencies(self, dependency: Optional[Dict[str, str]], details: Dict[str, Any]) -> bool:
        """Check if all dependencies are satisfied."""
        if not dependency:
            return True
            
        for key, value in dependency.items():
            if details.get(key) != value:
                return False
                
        return True

    def _convert_value_type(self, value: Any, value_type: Optional[str]) -> Any:
        """Convert value to the specified type."""
        if not value_type:
            return value
            
        try:
            if value_type.lower() == "string":
                return str(value)
            elif value_type.lower() == "integer":
                return int(value)
            elif value_type.lower() == "boolean":
                if isinstance(value, str):
                    return value.lower() == "true"
                return bool(value)
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert value {value} to type {value_type}")
            return value
            
        return value

    def _build_default_database_attributes(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build attributes for default database connector."""
        attributes = {
            "database_user": details.get("database_user", ""),
            "database_password": details.get("database_password", ""),
            "database_name": details.get("database_name", ""),
            "database_schema": details.get("database_schema", "")
        }
        
        # Add host and port only if specified
        if "database_host" in details:
            attributes["database_host"] = details["database_host"]
        if "database_port" in details:
            attributes["database_port"] = details["database_port"]
        
        return attributes
    
    def _get_default_port(self, connector_type: str) -> int:
        """Get default port for database connector type."""
        default_ports = {
            "mysql": 3306,
            "postgres": 5432,
            "redshift": 5439,
            "mssql": 1433,
            "oracle": 1521
        }
        return default_ports.get(connector_type, 0)
    
    def _build_default_settings(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        """Build default settings for data objects based on the plan."""
        settings = {
            "header": True,
            "columns_delimiter": ",",
            "columns_quote": "double"
        }
        
        # Override with settings from plan if they exist
        if "settings" in plan:
            settings.update(plan.get("settings", {}))
            
        return settings
    
    def _build_data_objects_spec(self, data_objects: List[Union[str, Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Build data objects specification from the plan."""
        data_objects_spec = []
        for obj in data_objects:
            if isinstance(obj, str):
                # Simple object name
                spec = self._build_simple_object_spec(obj)
            else:
                # Object with details
                spec = self._build_detailed_object_spec(obj)
            
            data_objects_spec.append(spec)
        
        return data_objects_spec

    def _build_simple_object_spec(self, object_name: str) -> Dict[str, Any]:
        """Build specification for a simple object with just a name."""
        return {
            "object_spec": {
                "object_name": object_name,
                "recreate_destination_object": False,
                "cleanup_linebreak_in_fields": False,
                "exclude_columns": [None]
            }
        }

    def _build_detailed_object_spec(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        """Build specification for an object with detailed attributes."""
        object_attributes = {
            "object_name": obj.get("name"),
            "recreate_destination_object": obj.get("recreate", False),
            "cleanup_linebreak_in_fields": obj.get("cleanup_linebreak", False),
            "exclude_columns": obj.get("exclude_columns", [None]),
            "destination_object_name": obj.get("destination_name"),
            "object_source_dir": obj.get("source_dir"),
            "file_names": obj.get("file_names"),
            "load_mode": obj.get("load_mode"),
            "load_batch_size": obj.get("load_batch_size"),
            "column_for_incremental_load": obj.get("column_for_incremental_load")
        }
        
        # Remove None values
        object_attributes = {k: v for k, v in object_attributes.items() if v is not None}
        
        return {"object_spec": object_attributes}

    def _build_pipeline_config_from_plan(self, plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Build a complete pipeline configuration directly from a plan without requiring LLM.
        This is a more direct version of generate_pipeline_config.

        Args:
            plan: Pipeline plan dictionary with source, target, and object details

        Returns:
            List containing pipeline configuration documents
        """
        # Get pipeline metadata structure template
        pipeline_meta = self.file_handler.load_json(self.m_conf.pipeline_meta_config_fpath)
        
        # Ensure output directory exists
        pipeline_name = plan.get("pipeline_name", "new_pipeline")
        output_dir = os.path.join(self.m_conf.output_base_dpath, pipeline_name)
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract source and target details
        source_details = plan.get("source", {}).get("details", {})
        target_details = plan.get("target", {}).get("details", {})
        
        # Build source attributes with platform-specific details
        source_attributes = self._build_connector_attributes(
            source_details,
            plan.get("source", {}).get("connector_type", ""),
            "source"
        )
        
        # Add platform-specific source attributes
        if "platform_specific" in source_details:
            source_attributes.update(source_details["platform_specific"])
        
        # Build target attributes with platform-specific details
        target_attributes = self._build_connector_attributes(
            target_details,
            plan.get("target", {}).get("connector_type", ""),
            "target"
        )
        
        # Add platform-specific target attributes
        if "platform_specific" in target_details:
            target_attributes.update(target_details["platform_specific"])
        
        # Handle platform-specific configurations
        platform_type = target_attributes.get("platform_type")
        platform_configs = self._build_platform_configs(target_details)
        
        # Add platform-specific configurations
        if platform_type in platform_configs:
            for key, value in platform_configs[platform_type].items():
                if value is not None:
                    target_attributes[key] = value
        
        # Handle Redshift specific configurations
        if target_attributes.get("connector_type") == "redshift":
            redshift_configs = self._build_redshift_configs(target_details)
            for key, value in redshift_configs.items():
                if value is not None:
                    target_attributes[key] = value
        
        # Build data objects specification
        data_objects_spec = self._build_data_objects_spec(plan.get("data_objects", []))
        
        # Construct the pipeline configuration
        pipeline_config = [
            {
                "pipeline": {
                    "pipeline_attributes": {
                        "pipeline_name": pipeline_name,
                        "is_enabled": True
                    },
                    "source_attributes": source_attributes,
                    "target_attributes": target_attributes,
                    "data_attributes": {
                        "data_objects_spec_mode": "only",
                        "object_default_settings": self._build_default_settings(plan)
                    }
                }
            },
            {
                "data_objects_spec": data_objects_spec
            }
        ]

        return pipeline_config

    def _build_platform_configs(self, details: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
        """Build platform-specific configurations for GCP and AWS."""
        return {
            "gcp": {
                "gcp_project_id": details.get("gcp_project_id"),
                "gcp_region": details.get("gcp_region"),
                "gcp_bucket_name": details.get("gcp_bucket_name"),
                "bucket_pipeline_prefix": details.get("bucket_pipeline_prefix")
            },
            "aws": {
                "aws_account": details.get("aws_account"),
                "aws_bucket_name": details.get("aws_bucket_name"),
                "bucket_pipeline_prefix": details.get("bucket_pipeline_prefix"),
                "aws_iam_role": details.get("aws_iam_role"),
                "aws_profile_name": details.get("aws_profile_name"),
                "aws_access_key_id": details.get("aws_access_key_id"),
                "aws_secret_access_key": details.get("aws_secret_access_key"),
                "aws_region": details.get("aws_region")
            }
        }

    def _build_redshift_configs(self, details: Dict[str, Any]) -> Dict[str, Any]:
        """Build Redshift-specific configurations."""
        return {
            "connection_method": details.get("connection_method"),
            "is_serverless": details.get("is_serverless"),
            "cluster_identifier": details.get("cluster_identifier"),
            "workgroup_name": details.get("workgroup_name")
        }

    def create_full_pipeline_config(self, instruction: str, system_prompt: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Create a complete pipeline configuration based on natural language instruction.
        This method handles the full pipeline structure including platform-specific configurations.

        Args:
            instruction: User's natural language instruction
            system_prompt: Optional custom system prompt to override the default

        Returns:
            List containing complete pipeline configuration documents
        """
        # Generate prompt for pipeline planning
        available_connectors = list(self.connector_configs.get("connectors", {}).keys())
        available_platforms = list(self.connector_configs.get("platforms", {}).keys())
        
        if system_prompt is None:
            system_prompt = f"""
            You are an AI assistant for Petaly, an ETL tool. You'll create a complete pipeline configuration from the user's instruction.
            
            Available connectors: {', '.join(available_connectors)}
            Available platforms: {', '.join(available_platforms)}
            
            Analyze the instruction and extract:
            1. Pipeline name (create a short, descriptive name if not specified)
            2. Source system (type, platform, and all connection details)
            3. Target system (type, platform, and all connection details)
            4. Data objects to transfer (tables, files, etc.)
            5. Any transformation requirements
            6. Platform-specific configurations (GCP, AWS, etc.)
            
            Format your response as a JSON with these fields:
            - pipeline_name: A short descriptive name for the pipeline (use snake_case)
            - source: {{
                "connector_type": One of the available connectors,
                "details": {{
                    "platform_type": One of the available platforms,
                    "connection_details": All required connection details,
                    "platform_specific": Platform-specific configuration
                }}
            }}
            - target: {{
                "connector_type": One of the available connectors,
                "details": {{
                    "platform_type": One of the available platforms,
                    "connection_details": All required connection details,
                    "platform_specific": Platform-specific configuration
                }}
            }}
            - data_objects: Array of data objects to transfer
            - transformations: Any transformations needed
            - needs_more_info: true/false if more information is needed
            - user_message: Message asking for more information if needed
            
            If the information provided is insufficient, set needs_more_info to true and include what information is missing.
            """
        
        # Get plan from LLM
        plan_result = self.llm.generate_completion(
            prompt=instruction,
            system_prompt=system_prompt,
            max_tokens=1500
        )
        
        try:
            plan = json.loads(plan_result)
            if plan.get("needs_more_info", False):
                raise ValueError(plan.get("user_message", "More information needed"))
            
            return self._build_pipeline_config_from_plan(plan)
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing pipeline plan: {e}")
            logger.debug(f"Raw plan result: {plan_result}")
            raise ValueError("Failed to parse pipeline configuration. Please try again with more specific details.")
    
    def create_modification_plan(self, instruction: str, existing_config: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Create a plan for modifying an existing pipeline based on a natural language instruction.
        
        Args:
            instruction: User's natural language instruction
            existing_config: The existing pipeline configuration
            
        Returns:
            Dictionary containing the modification plan
        """
        # Serialize the existing config for the prompt
        existing_config_str = json.dumps(existing_config, indent=2)
        
        system_prompt = f"""
        You are an AI assistant for Petaly, an ETL tool. You'll create a plan to modify an existing pipeline based on the user's instruction.
        
        Here is the existing pipeline configuration:
        ```
        {existing_config_str}
        ```
        
        Analyze the instruction and determine what modifications are needed. Focus on:
        1. Changes to source or target configurations
        2. Adding, removing, or modifying data objects
        3. Changes to transformation logic
        
        Format your response as a JSON with these fields:
        - modification_type: Type of modification (source_config, target_config, data_objects, etc.)
        - changes: Specific changes to make
        - needs_more_info: true/false if more information is needed
        - user_message: Message asking for more information if needed
        
        If the information provided is insufficient, set needs_more_info to true and include what information is missing.
        """
        
        # Get plan from LLM
        plan_result = self.llm.generate_completion(
            prompt=instruction,
            system_prompt=system_prompt,
            max_tokens=1500
        )
        
        try:
            plan = json.loads(plan_result)
            return plan
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing modification plan: {e}")
            logger.debug(f"Raw plan result: {plan_result}")
            # Return a default plan with error
            return {
                "needs_more_info": True,
                "user_message": "I'm having trouble understanding what changes you'd like to make to the pipeline. Could you please be more specific?"
            }
    
    def apply_modifications(self, existing_config: List[Dict[str, Any]], modification_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Apply modifications to an existing pipeline configuration based on a modification plan.
        
        Args:
            existing_config: The existing pipeline configuration
            modification_plan: The plan for modifications
            
        Returns:
            Updated pipeline configuration
        """
        # Make a deep copy of the existing config to avoid modifying the original
        updated_config = json.loads(json.dumps(existing_config))
        
        mod_type = modification_plan.get("modification_type")
        changes = modification_plan.get("changes", {})
        
        if mod_type == "source_config":
            # Update source configuration
            if "source_attributes" in changes:
                updated_config[0]["pipeline"]["source_attributes"].update(changes["source_attributes"])
                
        elif mod_type == "target_config":
            # Update target configuration
            if "target_attributes" in changes:
                updated_config[0]["pipeline"]["target_attributes"].update(changes["target_attributes"])
                
        elif mod_type == "data_objects":
            # Handle data object modifications
            if "add" in changes:
                # Add new data objects
                for obj in changes["add"]:
                    spec = {
                        "object_spec": obj
                    }
                    updated_config[1]["data_objects_spec"].append(spec)
                    
            if "remove" in changes:
                # Remove data objects
                remove_names = changes["remove"]
                updated_config[1]["data_objects_spec"] = [
                    spec for spec in updated_config[1]["data_objects_spec"]
                    if spec["object_spec"]["object_name"] not in remove_names
                ]
                
            if "modify" in changes:
                # Modify existing data objects
                for mod in changes["modify"]:
                    obj_name = mod.get("object_name")
                    for i, spec in enumerate(updated_config[1]["data_objects_spec"]):
                        if spec["object_spec"]["object_name"] == obj_name:
                            # Update the object spec with new values
                            for key, value in mod.items():
                                if key != "object_name":
                                    updated_config[1]["data_objects_spec"][i]["object_spec"][key] = value
                            break
                            
        elif mod_type == "settings":
            # Update object default settings
            if "object_default_settings" in changes:
                updated_config[0]["pipeline"]["data_attributes"]["object_default_settings"].update(
                    changes["object_default_settings"]
                )
                
        elif mod_type == "pipeline_attributes":
            # Update pipeline attributes
            if "pipeline_attributes" in changes:
                updated_config[0]["pipeline"]["pipeline_attributes"].update(changes["pipeline_attributes"])
        
        return updated_config