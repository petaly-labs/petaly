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
import os
import json
import requests
from abc import ABC, abstractmethod
from typing import Dict, List, Any, Optional, Union

logger = logging.getLogger(__name__)

class LLMConnector(ABC):
    """
    Base abstract class for LLM integrations.
    Defines interface for text completion and embedding generation.
    Supports multiple LLM providers through concrete implementations.
    """
    
    @abstractmethod
    def generate_completion(self, prompt: str, **kwargs) -> str:
        """
        Generate a text completion for the given prompt.
        
        Logic:
        1. Process input prompt
        2. Call LLM API
        3. Parse and return response
        """
        pass
    
    @abstractmethod
    def generate_embeddings(self, text: Union[str, List[str]]) -> List[List[float]]:
        """
        Generate embeddings for the given text(s).
        
        Logic:
        1. Process input text(s)
        2. Call embeddings API
        3. Return vector embeddings
        """
        pass


class AnthropicConnector(LLMConnector):
    """
    Connector for Anthropic Claude models.
    Handles API communication and response parsing for Claude.
    Supports JSON response extraction and validation.
    """
    
    def __init__(self, llm_config: Dict[str, Any]):
        """
        Initialize Anthropic connector.
        
        Logic:
        1. Get API key from config or environment
        2. Set model and API URL
        3. Validate configuration
        """
        self.api_key = llm_config.get('ai_agent_api_key') or os.environ.get("AI_AGENT_API_KEY")
        if not self.api_key:
            raise ValueError("Anthropic API key is required")
        
        self.model = llm_config.get('llm_model') or "claude-3-opus-20240229"
        self.api_url = "https://api.anthropic.com/v1/messages"

    def generate_completion(self, prompt: str, **kwargs) -> str:
        """
        Generate a completion using Anthropic Claude.
        
        Logic:
        1. Prepare API request with headers
        2. Format prompt and parameters
        3. Make API call
        4. Extract and validate JSON response
        5. Return parsed result
        """
        headers = {
            "x-api-key": self.api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json"
        }

        max_tokens = kwargs.get("max_tokens", 1024)
        system_prompt = kwargs.get("system_prompt", "")
        temperature = kwargs.get("temperature", 0.7)

        # Make the system prompt more explicit about JSON formatting
        if system_prompt:
            system_prompt += "\n\nIMPORTANT: Your response must be a valid JSON object. Do not include any text before or after the JSON."

        payload = {
            "model": self.model,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature
        }

        if system_prompt:
            payload["system"] = system_prompt

        try:
            response = requests.post(self.api_url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()

            # Extract the text response
            raw_text = result["content"][0]["text"]

            # Try several methods to extract valid JSON

            # Method 1: Try to find JSON between triple backticks
            import re
            json_match = re.search(r'```(?:json)?\s*([\s\S]*?)\s*```', raw_text)
            if json_match:
                try:
                    json_str = json_match.group(1).strip()
                    json.loads(json_str)  # Test if it's valid JSON
                    return json_str
                except json.JSONDecodeError:
                    pass  # Try next method

            # Method 2: Try to find the outermost JSON object
            json_match = re.search(r'({[\s\S]*})', raw_text)
            if json_match:
                try:
                    json_str = json_match.group(1).strip()
                    json.loads(json_str)  # Test if it's valid JSON
                    return json_str
                except json.JSONDecodeError:
                    pass  # Try next method

            # Method 3: Try to extract JSON by removing common prefixes
            cleaned_text = re.sub(r'^.*?({[\s\S]*}).*?$', r'\1', raw_text)
            try:
                json.loads(cleaned_text)  # Test if it's valid JSON
                return cleaned_text
            except json.JSONDecodeError:
                pass  # Try next method

            # Method 4: Try with a more aggressive approach - find anything that looks like JSON
            potential_json = raw_text.strip()
            # Remove common prefixes
            for prefix in ["Here is the response in JSON format:", "Here's the JSON:", "JSON response:"]:
                if potential_json.startswith(prefix):
                    potential_json = potential_json[len(prefix):].strip()

            # Try to find the first { and last }
            start_idx = potential_json.find('{')
            end_idx = potential_json.rfind('}')

            if start_idx != -1 and end_idx != -1:
                try:
                    json_str = potential_json[start_idx:end_idx + 1].strip()
                    json.loads(json_str)  # Test if it's valid JSON
                    return json_str
                except json.JSONDecodeError:
                    pass  # Fall through to returning raw text

            # If all else fails, return the raw text
            logger.warning("Failed to extract valid JSON from the response")
            return raw_text

        except Exception as e:
            logger.error(f"Error generating completion from Anthropic: {e}")
            if 'response' in locals():
                logger.error(f"Response status: {response.status_code}")
                logger.error(f"Response text: {response.text}")
            raise


    def generate_embeddings(self, text: Union[str, List[str]]) -> List[List[float]]:
        """
        Generate embeddings using Anthropic's API.
        
        Logic:
        1. Placeholder for future implementation
        2. Raise NotImplementedError
        """
        # Note: You would need to implement this when Anthropic releases their embeddings API
        # This is a placeholder implementation
        raise NotImplementedError("Anthropic embeddings API not implemented yet")


class OpenAIConnector(LLMConnector):
    """
    Connector for OpenAI models.
    Handles API communication for GPT models and embeddings.
    Supports chat completions and text embeddings.
    """
    
    def __init__(self, llm_config: Dict[str, Any]):
        """
        Initialize OpenAI connector.
        
        Logic:
        1. Get API key from config or environment
        2. Set model and API endpoints
        3. Configure embedding model
        """
        self.api_key = llm_config.get('ai_agent_api_key') or os.environ.get("AI_AGENT_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key is required")
        
        self.model = llm_config.get('llm_model') or "gpt-4"
        self.api_base = "https://api.openai.com/v1"
        self.completion_url = f"{self.api_base}/chat/completions"
        self.embedding_url = f"{self.api_base}/embeddings"
        self.embedding_model = "text-embedding-3-large"
        
    def generate_completion(self, prompt: str, **kwargs) -> str:
        """
        Generate a completion using OpenAI.
        
        Logic:
        1. Prepare API request with headers
        2. Format messages with system prompt
        3. Make API call
        4. Extract and return response
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        max_tokens = kwargs.get("max_tokens", 1024)
        system_prompt = kwargs.get("system_prompt", "")
        temperature = kwargs.get("temperature", 0.7)
        
        messages = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": prompt})
        
        payload = {
            "model": self.model,
            "messages": messages,
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        
        try:
            response = requests.post(self.completion_url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return result["choices"][0]["message"]["content"]

        except requests.exceptions.RequestException as e:
            if e.response is not None:
                logger.error(f"OpenAI API error {e.response.status_code}: {e.response.text}")
            else:
                logger.error(f"Error connecting to OpenAI API: {e}")
            raise
    
    def generate_embeddings(self, text: Union[str, List[str]]) -> List[List[float]]:
        """
        Generate embeddings using OpenAI's embedding model.
        
        Logic:
        1. Prepare API request
        2. Handle single/multiple text inputs
        3. Make API call
        4. Extract embeddings from response
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }
        
        # Handle single string or list of strings
        input_text = [text] if isinstance(text, str) else text
        
        payload = {
            "model": self.embedding_model,
            "input": input_text
        }
        
        try:
            response = requests.post(self.embedding_url, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            return [item["embedding"] for item in result["data"]]
        except Exception as e:
            logger.error(f"Error generating embeddings from OpenAI: {e}")
            raise


def get_llm_connector(llm_config: Dict[str, Any]) -> LLMConnector:
    """
    Factory function to get the appropriate LLM connector.
    
    Logic:
    1. Check provider in config
    2. Return corresponding connector instance
    3. Raise error for unsupported providers
    """
    if str(llm_config.get('llm_provider')).lower() == "anthropic":
        return AnthropicConnector(llm_config)
    elif str(llm_config.get('llm_provider')).lower() == "openai":
        return OpenAIConnector(llm_config)
    else:
        raise ValueError(f"Unsupported LLM provider: {str(llm_config.get('llm_provider'))}")
