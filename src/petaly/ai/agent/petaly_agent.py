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
import asyncio
import sys
from typing import Dict, List, Any, Optional, Union, Tuple, Callable
from enum import Enum, auto
from functools import wraps

from petaly.ai.agent.llm_connector import LLMConnector, get_llm_connector
from petaly.ai.agent.pipeline_planner import PipelinePlanner
from petaly.utils.file_handler import FileHandler
from petaly.core.pipeline import Pipeline
from petaly.ai.agent.conversation_store import ConversationStore
from petaly.ai.agent.pipeline_prompts import PipelinePrompts

logger = logging.getLogger(__name__)

def handle_errors(func: Callable) -> Callable:
    """Decorator to handle errors in handler methods."""
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        try:
            return await func(self, *args, **kwargs)
        except Exception as e:
            error_msg = f"Error in {func.__name__}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            # Create a more user-friendly error message
            if isinstance(e, AttributeError):
                if "'OpenAIConnector' object has no attribute 'generate'" in str(e):
                    return (
                        "I encountered an error while trying to process your request. "
                        "It seems there's a mismatch in the LLM connector implementation. "
                        "Please check your configuration and try again.\n\n"
                        "Technical details: The LLM connector is missing the required 'generate' method."
                    )
                else:
                    return (
                        "I encountered an error while trying to process your request. "
                        "It seems there's an issue with the LLM connector configuration. "
                        "Please check your configuration and try again.\n\n"
                        f"Technical details: {str(e)}"
                    )
            elif isinstance(e, json.JSONDecodeError):
                return (
                    "I encountered an error while trying to process the LLM response. "
                    "The response wasn't in the expected format. Please try again.\n\n"
                    f"Technical details: {str(e)}"
                )
            elif isinstance(e, ValueError) and "API key is required" in str(e):
                provider = "OpenAI" if "OpenAI" in str(e) else "Anthropic"
                return (
                    f"The {provider} API key is not configured. To fix this:\n\n"
                    "1. Set the API key in your configuration file under 'ai_settings.ai_agent_api_key', or\n"
                    "2. Set the AI_AGENT_API_KEY environment variable\n\n"
                    f"Example configuration:\n"
                    f"ai_settings:\n"
                    f"  ai_agent_api_key: your-api-key-here\n"
                    f"  llm_provider: {provider.lower()}\n"
                    f"  llm_model: gpt-4  # or your preferred model\n\n"
                    f"Or set the environment variable:\n"
                    f"export AI_AGENT_API_KEY=your-api-key-here"
                )
            else:
                return (
                    "I encountered an error while processing your request. "
                    "Please try again or rephrase your request.\n\n"
                    f"Technical details: {str(e)}"
                )
    return wrapper

class InstructionType(Enum):
    """Types of instructions that the agent can handle."""
    LIST_PIPELINES = auto()
    CREATE_PIPELINE = auto()
    RUN_PIPELINE = auto()
    MODIFY_PIPELINE = auto()
    LIST_OUTPUT = auto()
    OTHER = auto()

class PetalyAgent:
    """
    AI Agent for Petaly ETL operations.
    Handles natural language instructions for pipeline management,
    including creation, modification, execution, and querying.
    Manages conversation history and context for improved interactions.
    """
    
    def __init__(
        self, 
        main_config
    ):
        """
        Initializes the Petaly Agent.
        
        Logic:
        1. Store main configuration
        2. Initialize LLM connector with API settings
        3. Initialize pipeline planner
        4. Set up conversation store for history
        5. Initialize pipeline prompts
        """
        self.m_conf = main_config
        self.file_handler = FileHandler()

        # Initialize LLM connector
        llm_config = {"ai_agent_api_key": self.m_conf.ai_settings.get('ai_agent_api_key'),
                      "llm_model": self.m_conf.ai_settings.get('llm_model'),
                      "llm_provider": self.m_conf.ai_settings.get('llm_provider'),
                      "agent_memory_file": self.m_conf.ai_settings.get('agent_memory_file')
                      }

        # Validate LLM configuration
        if not llm_config["llm_provider"]:
            raise RuntimeError(
                "LLM provider is not configured. Please set 'llm_provider' in your configuration file under 'ai_settings'."
            )

        # Check for API key
        if not llm_config["ai_agent_api_key"] and not os.environ.get("AI_AGENT_API_KEY"):
            provider = llm_config["llm_provider"].capitalize()
            raise RuntimeError(
                f"{provider} API key is not configured. To fix this:\n\n"
                "1. Set the API key in your configuration file under 'ai_settings.ai_agent_api_key', or\n"
                "2. Set the AI_AGENT_API_KEY environment variable\n\n"
                f"Example configuration:\n"
                f"ai_settings:\n"
                f"  ai_agent_api_key: your-api-key-here\n"
                f"  llm_provider: {llm_config['llm_provider'].lower()}\n"
                f"  llm_model: {llm_config.get('llm_model', 'gpt-4')}  # or your preferred model\n\n"
                f"Or set the environment variable:\n"
                f"export AI_AGENT_API_KEY=your-api-key-here"
            )
        
        try:
            self.llm = get_llm_connector(llm_config)
        except Exception as e:
            logger.error(f"Failed to initialize LLM connector: {e}", exc_info=True)
            raise RuntimeError(
                f"Failed to initialize LLM connector with provider '{llm_config['llm_provider']}'. "
                f"Please check your configuration and API keys. Error: {str(e)}"
            )
        
        # Initialize planner
        self.planner = PipelinePlanner(self.llm, self.m_conf)
        
        agent_memory_file = llm_config.get('agent_memory_file')
        # Initialize conversation store
        if agent_memory_file is not None:
            # Use default path from config or fallback to ~/.petaly/agent_memory.json
            agent_memory_file = os.path.expanduser(agent_memory_file)
        try:
            self.conversation_store = ConversationStore(storage_path=agent_memory_file)
        except Exception as e:
            logger.error(f"Failed to initialize conversation store: {e}", exc_info=True)
            raise RuntimeError(
                f"Failed to initialize conversation store at '{agent_memory_file}'. "
                f"Please check the file path and permissions. Error: {str(e)}"
            )
        
        # Initialize prompts
        self.prompts = PipelinePrompts()
        
        logger.info(f"Initialized PetalyAgent with memory file: {agent_memory_file}")

    async def process_instruction(self, instruction: str) -> str:
        """
        Process a user instruction and return a response.
        
        Logic:
        1. Validate instruction format
        2. Add instruction to conversation history
        3. Get recent conversation context
        4. Process instruction with context
        5. Store response in history
        """
        try:
            if not isinstance(instruction, str) or not instruction.strip():
                return "Please provide a valid instruction. The instruction cannot be empty."
            
            # Add user instruction to conversation history
            self.conversation_store.add_entry(
                role="user",
                content=instruction,
                metadata={"timestamp": asyncio.get_event_loop().time()}
            )
            
            # Get recent conversation history for context
            recent_history = self.conversation_store.get_recent_entries(count=5)
            
            # Process the instruction
            response = await self._process_instruction_with_context(instruction, recent_history)
            
            # Add agent response to conversation history
            self.conversation_store.add_entry(
                role="assistant",
                content=response,
                metadata={"timestamp": asyncio.get_event_loop().time()}
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Error processing instruction: {e}", exc_info=True)
            return (
                "I encountered an error while processing your request. "
                "Please try again or rephrase your request.\n\n"
                f"Technical details: {str(e)}"
            )
    
    async def _process_instruction_with_context(
        self,
        instruction: str,
        recent_history: List[Dict[str, Any]]
    ) -> str:
        """
        Process an instruction with conversation context.
        
        Logic:
        1. Classify instruction type
        2. Route to appropriate handler based on type
        3. Handle errors with decorator
        """
        try:
            # Classify the instruction
            classification = await self._classify_instruction(instruction)
            instruction_type = self._get_instruction_type(classification["instruction_type"])
            
            # Process based on instruction type
            if instruction_type == InstructionType.LIST_PIPELINES:
                return await self._handle_list_pipelines(instruction, classification)
            elif instruction_type == InstructionType.CREATE_PIPELINE:
                return await self._handle_create_pipeline(instruction, classification)
            elif instruction_type == InstructionType.RUN_PIPELINE:
                return await self._handle_run_pipeline(instruction, classification)
            elif instruction_type == InstructionType.MODIFY_PIPELINE:
                return await self._handle_modify_pipeline(instruction, classification)
            elif instruction_type == InstructionType.LIST_OUTPUT:
                return await self._handle_list_output(instruction, classification)
            else:
                return await self._handle_other_request(instruction, classification)
        except Exception as e:
            logger.error(f"Error in instruction processing: {e}", exc_info=True)
            return (
                "I encountered an error while processing your instruction. "
                "Please try again or rephrase your request.\n\n"
                f"Technical details: {str(e)}"
            )
    
    async def _classify_instruction(self, instruction: str) -> Dict[str, Any]:
        """
        Classify the user's instruction using the LLM.
        
        Logic:
        1. Get classification prompt
        2. Send instruction to LLM
        3. Parse and validate response
        4. Return classification details
        """
        prompt = PipelinePrompts.get_classification_prompt()
        
        # Add instruction to the prompt
        full_prompt = f"{prompt}\n\nUser instruction: {instruction}"
        
        # Get classification from LLM (no await needed as it's not async)
        classification_result = self.llm.generate_completion(
            prompt=full_prompt,
            max_tokens=500
        )
        
        # Log the raw classification result
        logger.debug(f"Raw classification result: {classification_result}")
        
        # Parse the classification
        try:
            classification = self._parse_classification(classification_result)
            logger.debug(f"Parsed classification: {classification}")
            return classification
        except Exception as e:
            logger.error(f"Error parsing classification: {str(e)}")
            return {
                "instruction_type": "other",
                "details": {},
                "next_action": "I couldn't understand your request. Please try rephrasing it."
            }
    
    def _parse_classification(self, classification_result: str) -> Dict[str, Any]:
        """
        Parse the classification result from the LLM.
        
        Logic:
        1. Extract JSON from response
        2. Validate required fields
        3. Return structured classification
        """
        try:
            # Log the raw result for debugging
            logger.debug(f"Raw classification result: {classification_result}")
            
            # Try to find JSON in the response
            start_idx = classification_result.find('{')
            end_idx = classification_result.rfind('}') + 1
            
            if start_idx == -1 or end_idx == 0:
                logger.error("No JSON object found in classification result")
                return {
                    "instruction_type": "other",
                    "details": {},
                    "next_action": "I couldn't understand your request. Please try rephrasing it."
                }
            
            json_str = classification_result[start_idx:end_idx]
            logger.debug(f"Extracted JSON string: {json_str}")
            
            classification = json.loads(json_str)
            logger.debug(f"Parsed JSON: {classification}")
            
            # Ensure instruction_type is lowercase
            if "instruction_type" in classification:
                classification["instruction_type"] = classification["instruction_type"].lower()
            
            return classification
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON: {str(e)}")
            logger.error(f"Failed JSON string: {json_str}")
            return {
                "instruction_type": "other",
                "details": {},
                "next_action": "I couldn't understand your request. Please try rephrasing it."
            }
        except Exception as e:
            logger.error(f"Unexpected error in classification parsing: {str(e)}")
            return {
                "instruction_type": "other",
                "details": {},
                "next_action": "I couldn't understand your request. Please try rephrasing it."
            }
    
    def _get_instruction_type(self, instruction_type_str: str) -> InstructionType:
        """
        Convert instruction type string to enum.
        
        Logic:
        1. Map string to InstructionType enum
        2. Default to OTHER if unknown
        """
        try:
            # Normalize the instruction type string
            normalized_type = instruction_type_str.upper()
            
            # Map any variations to LIST_OUTPUT
            if normalized_type in ('LIST_FILES', 'SHOW_FILES', 'SHOW_OUTPUT', 'LIST_OUTPUT'):
                return InstructionType.LIST_OUTPUT
            
            return InstructionType[normalized_type]
        except KeyError:
            logger.warning(f"Unknown instruction type: {instruction_type_str}")
            return InstructionType.OTHER

    @handle_errors
    async def _handle_create_pipeline(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle pipeline creation requests.
        
        Logic:
        1. Extract pipeline details from classification
        2. Use planner to create pipeline
        3. Return creation status
        """
        # Get details from the planner
        pipeline_plan = self.planner.create_pipeline_plan(instruction)
        
        # Check if we have enough information to proceed
        if pipeline_plan.get("needs_more_info"):
            return pipeline_plan.get("user_message", "I need more information to create this pipeline. Could you provide details about the source and target systems?")
        
        # Generate pipeline configuration from the plan
        try:
            # Use synchronous generate_pipeline_config since it's not an async method
            pipeline_config = self.planner.generate_pipeline_config(pipeline_plan)
            pipeline_name = pipeline_config[0]['pipeline']['pipeline_attributes'].get('pipeline_name')
            
            # Save the configuration
            pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
            if not self.file_handler.is_dir(pipeline_dpath):
                self.file_handler.make_dirs(pipeline_dpath)
                
            pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
            self.file_handler.save_dict_to_yaml(pipeline_fpath, pipeline_config, dump_all=True)
            
            return f"I've created a new pipeline named '{pipeline_name}'. The configuration has been saved to {pipeline_fpath}. Would you like to review it or make any changes?"
            
        except Exception as e:
            logger.error(f"Error creating pipeline: {e}")
            return f"I encountered an error while trying to create the pipeline: {str(e)}"

    @handle_errors
    async def _handle_modify_pipeline(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle pipeline modification requests.
        
        Logic:
        1. Extract modification details
        2. Load existing pipeline
        3. Apply modifications
        4. Save updated pipeline
        """
        details = classification.get("details", {})
        pipeline_name = details.get("pipeline_name")
        
        if not pipeline_name:
            # Try to extract pipeline name from instruction
            pipeline_name = await self.llm.generate_completion(
                prompt=instruction,
                system_prompt=PipelinePrompts.get_pipeline_name_extraction_prompt(),
                max_tokens=50
            ).strip()
        
        # Check if pipeline exists
        pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
        if not self.file_handler.is_dir(pipeline_dpath):
            return f"I couldn't find a pipeline named '{pipeline_name}'. Please check the name and try again."
        
        # Load the existing pipeline
        pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
        existing_config = self.file_handler.load_yaml_all(pipeline_fpath)
        
        # Generate modification plan
        modification_plan = await self.planner.create_modification_plan(
            instruction,
            existing_config,
            system_prompt=PipelinePrompts.get_pipeline_modification_prompt()
        )
        
        if modification_plan.get("needs_more_info"):
            return modification_plan.get("user_message", "I need more specific information about what you'd like to modify in this pipeline.")
        
        # Apply modifications
        try:
            updated_config = await self.planner.apply_modifications(existing_config, modification_plan)
            
            # Backup the original config
            self.file_handler.backup_file(pipeline_fpath)
            
            # Save the updated configuration
            self.file_handler.save_dict_to_yaml(pipeline_fpath, updated_config, dump_all=True)
            
            return f"I've updated the pipeline '{pipeline_name}' with your requested changes. The original configuration has been backed up. Would you like to review the changes?"
            
        except Exception as e:
            logger.error(f"Error modifying pipeline: {e}")
            return f"I encountered an error while trying to modify the pipeline: {str(e)}"

    @handle_errors
    async def _handle_run_pipeline(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle pipeline execution requests.
        
        Logic:
        1. Extract pipeline details
        2. Load pipeline configuration
        3. Execute pipeline
        4. Return execution status
        """
        details = classification.get("details", {})
        pipeline_name = details.get("pipeline_name")
        run_endpoint = details.get("run_endpoint")  # "source", "target", or None for both
        object_names = details.get("object_names")  # List of specific objects or None for all
        
        if not pipeline_name:
            # Try to extract pipeline name from instruction
            pipeline_name = await self.llm.generate_completion(
                prompt=instruction,
                system_prompt=PipelinePrompts.get_pipeline_name_extraction_prompt(),
                max_tokens=50
            ).strip()
        
        # Check if pipeline exists
        pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
        if not self.file_handler.is_dir(pipeline_dpath):
            return f"I couldn't find a pipeline named '{pipeline_name}'. Please check the name and try again."
        
        try:
            # Initialize pipeline
            pipeline = Pipeline(pipeline_name, self.m_conf)
            
            from petaly.core.main_ctl import MainCtl
            main_ctl = MainCtl(self.m_conf)
            
            # Convert object_names list to comma-separated string if needed
            object_names_str = None
            if object_names:
                object_names_str = ",".join(object_names)
            
            # Run the pipeline
            #await main_ctl.run_pipeline(pipeline, run_endpoint, object_names_str)
            main_ctl.run_pipeline(pipeline, run_endpoint, object_names_str)

            return f"I've successfully executed the pipeline '{pipeline_name}'."
            
        except Exception as e:
            logger.error(f"Error running pipeline: {e}")
            return f"I encountered an error while trying to run the pipeline: {str(e)}"

    @handle_errors
    async def _handle_query_data(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle data querying requests.
        
        Logic:
        1. Extract query parameters
        2. Execute query on pipeline
        3. Format and return results
        """
        # Analyze the query
        query_analysis = await self.llm.generate_completion(
            prompt=instruction,
            system_prompt=PipelinePrompts.get_pipeline_query_prompt(),
            max_tokens=300
        )
        
        try:
            analysis = json.loads(query_analysis)
            query_type = analysis.get("query_type")
            parameters = analysis.get("parameters", {})
            
            if query_type == "pipeline_list":
                # List all pipelines
                pipelines = self.file_handler.get_all_dir_names(self.m_conf.pipeline_base_dpath)
                if not pipelines:
                    return "You don't have any pipelines set up yet. Would you like me to help you create one?"
                
                pipeline_list = "\n".join(f"- {p}" for p in pipelines)
                return f"Here are your existing pipelines:\n{pipeline_list}"
                
            elif query_type == "pipeline_details":
                # Get details of a specific pipeline
                pipeline_name = parameters.get("pipeline_name")
                if not pipeline_name:
                    return "Which pipeline would you like to know about?"
                
                pipeline_dpath = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
                if not self.file_handler.is_dir(pipeline_dpath):
                    return f"I couldn't find a pipeline named '{pipeline_name}'."
                
                # Load the pipeline config
                pipeline_fpath = os.path.join(pipeline_dpath, self.m_conf.pipeline_fname)
                pipeline_config = self.file_handler.load_yaml_all(pipeline_fpath)
                
                # Generate a human-friendly summary
                summary = await self.llm.generate_completion(
                    prompt=PipelinePrompts.get_pipeline_summary_prompt(pipeline_config),
                    max_tokens=1000
                )
                
                return summary
                
            else:
                return "I'm not sure how to answer that query about your data or pipelines. Could you please be more specific?"
                
        except json.JSONDecodeError:
            return "I'm having trouble understanding your query. Could you please rephrase it?"

    @handle_errors
    async def _handle_other_request(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle general or unrecognized requests.
        
        Logic:
        1. Extract request details
        2. Generate appropriate response
        3. Provide helpful suggestions
        """
        # Generate a general response
        system_prompt = """
        You are an AI assistant for Petaly, an ETL (Extract, Transform, Load) tool that helps users create data pipelines 
        between different systems. Respond to the user's message in a helpful way, focusing on how Petaly might help them.
        If their request is unrelated to data pipelines or ETL processes, gently steer the conversation back to how you can 
        help them with Petaly.
        """
        
        # Use synchronous generate_completion since it's not an async method
        response = self.llm.generate_completion(
            prompt=instruction,
            system_prompt=system_prompt,
            max_tokens=500
        )
        
        return response

    @handle_errors
    async def _handle_list_pipelines(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle pipeline listing requests.
        
        Logic:
        1. Get pipeline directory
        2. List available pipelines
        3. Format and return list
        """
        details = classification.get("details", {})
        endpoint_type = details.get("endpoint_type")
        
        # Get all pipeline directories
        pipelines = self.file_handler.get_all_dir_names(self.m_conf.pipeline_base_dpath)
        
        if not pipelines:
            return "No pipelines found. Would you like to create one?"
        
        # If endpoint type specified, filter pipelines
        if endpoint_type:
            filtered_pipelines = []
            for pipeline_name in pipelines:
                pipeline_path = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name, self.m_conf.pipeline_fname)
                if self.file_handler.is_file(pipeline_path):
                    config = self.file_handler.load_yaml_all(pipeline_path)
                    if config[0]['pipeline']['source_attributes']['connector_type'] == endpoint_type or \
                       config[0]['pipeline']['target_attributes']['connector_type'] == endpoint_type:
                        filtered_pipelines.append(pipeline_name)
            pipelines = filtered_pipelines
        
        # Format response
        if endpoint_type:
            header = f"Pipelines with {endpoint_type} endpoint:"
        else:
            header = "All pipelines:"
        
        if not pipelines:
            return f"No pipelines found{' with ' + endpoint_type + ' endpoint' if endpoint_type else ''}."
        
        pipeline_list = "\n".join(f"- {p}" for p in pipelines)
        return f"{header}\n{pipeline_list}"

    @handle_errors
    async def _handle_list_output(self, instruction: str, classification: Dict[str, Any]) -> str:
        """
        Handle output listing requests.
        
        Logic:
        1. Get output directory
        2. List available outputs
        3. Format and return list
        """
        details = classification.get("details", {})
        pipeline_name = details.get("pipeline_name")
        
        if not pipeline_name:
            return "Please specify which pipeline's output files you want to list."
        
        # Check if pipeline exists
        pipeline_dir = os.path.join(self.m_conf.pipeline_base_dpath, pipeline_name)
        if not self.file_handler.is_dir(pipeline_dir):
            return f"Pipeline '{pipeline_name}' not found."
        
        # Get output directory for this pipeline
        output_dir = os.path.join(self.m_conf.output_base_dpath, pipeline_name)
        if not self.file_handler.is_dir(output_dir):
            return f"No output files found for pipeline '{pipeline_name}'."
        
        # List output files
        output_data_dir = os.path.join(output_dir, '**', 'data', '*.*')
        output_files = self.file_handler.get_all_files_from_dir(output_data_dir)
        if not output_files:
            return f"No output files found for pipeline '{pipeline_name}' in {output_data_dir}:"
        
        # Format response
        files_list = "\n".join(f"- {f}" for f in output_files)
        return f"Output files for pipeline '{pipeline_name}':\n{files_list}"
