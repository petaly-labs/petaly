import pytest
import os
import tempfile
from unittest.mock import Mock, patch, AsyncMock
from typing import Dict, Any

from petaly.ai.agent.petaly_agent import PetalyAgent, InstructionType
from petaly.ai.agent.llm_connector import LLMConnector, AnthropicConnector

class TestLLMConnector:
    @pytest.fixture
    def llm_config(self):
        return {
            'ai_agent_api_key': 'test_key',
            'llm_model': 'test_model'
        }
    
    @patch('requests.post')
    def test_anthropic_connector_generate_completion(self, mock_post, llm_config):
        # Setup mock response
        mock_response = Mock()
        mock_response.json.return_value = {
            'content': [{'text': 'Test response'}]
        }
        mock_post.return_value = mock_response
        
        # Test the connector
        connector = AnthropicConnector(llm_config)
        response = connector.generate_completion('Test prompt')
        
        assert response == 'Test response'
        mock_post.assert_called_once()

class TestPetalyAgent:
    @pytest.fixture
    def temp_dir(self):
        """Create a temporary directory for test files."""
        with tempfile.TemporaryDirectory() as temp_dir:
            yield temp_dir
    
    @pytest.fixture
    def main_config(self, temp_dir):
        # Create a mock config object with the required attributes
        config = Mock()
        config.ai_settings = Mock()
        
        # Create a proper path for the memory file
        memory_file = os.path.join(temp_dir, 'test_memory.json')
        
        config.ai_settings.get = lambda key: {
            'ai_agent_api_key': 'test_key',
            'llm_model': 'test_model',
            'llm_provider': 'anthropic',
            'agent_memory_file': memory_file
        }.get(key)
        return config
    
    @pytest.fixture
    def agent(self, main_config):
        with patch('petaly.ai.agent.petaly_agent.get_llm_connector') as mock_get_llm:
            # Mock the LLM connector
            mock_llm = Mock()
            mock_llm.generate_completion.return_value = '{"instruction_type": "CREATE_PIPELINE"}'
            mock_get_llm.return_value = mock_llm
            
            # Create and return the agent
            return PetalyAgent(main_config)
    
    def test_instruction_type_enum(self):
        """Test that instruction types are properly defined."""
        assert InstructionType.LIST_PIPELINES.name == 'LIST_PIPELINES'
        assert InstructionType.CREATE_PIPELINE.name == 'CREATE_PIPELINE'
        assert InstructionType.RUN_PIPELINE.name == 'RUN_PIPELINE'
    
    @pytest.mark.asyncio
    @patch('petaly.ai.agent.petaly_agent.PetalyAgent._classify_instruction')
    async def test_process_instruction(self, mock_classify, agent):
        # Setup mock
        mock_classify.return_value = {
            'instruction_type': 'CREATE_PIPELINE',
            'pipeline_name': 'test_pipeline'
        }
        
        # Test the method
        result = await agent.process_instruction('Create a new pipeline')
        
        # Verify the mock was called
        mock_classify.assert_called_once()
        assert isinstance(result, str)
    
    def test_parse_classification(self, agent):
        """Test parsing of classification results."""
        test_input = '{"instruction_type": "CREATE_PIPELINE", "pipeline_name": "test"}'
        result = agent._parse_classification(test_input)
        
        assert result['instruction_type'] == 'create_pipeline'
        assert result['pipeline_name'] == 'test'
    
    def test_get_instruction_type(self, agent):
        """Test instruction type conversion."""
        result = agent._get_instruction_type('CREATE_PIPELINE')
        assert result == InstructionType.CREATE_PIPELINE
        
        result = agent._get_instruction_type('INVALID_TYPE')
        assert result == InstructionType.OTHER 