# AI Agent Mode Guide

This guide explains how to use Petaly's AI Agent mode, which allows you to control Petaly using natural language commands.

## Installation

### Basic Installation
```bash
# Install with AI support
python3 -m pip install petaly[ai]
```

### Full Installation (includes AI)
```bash
# Install all features including AI
python3 -m pip install petaly[all]
```

## Configuration

### 1. Set up API Key
You can set your API key in two ways:

1. **Environment Variable**:
```bash
export AI_AGENT_API_KEY=your-api-key
```

2. **Configuration File**:
Edit `petaly.ini`:
```ini
[ai_settings]
llm_provider = openai  # or anthropic
llm_model = gpt-4     # or claude-3-opus-20240229
ai_agent_api_key = your-api-key
```

### 2. Configure Memory (Optional)
```ini
[ai_settings]
agent_memory_file = /path/to/memory.json
```

## Usage

### Basic Commands

1. **Initialize Pipeline**:
```bash
python3 -m petaly -c /path/to/petaly.ini init -p my_pipeline --ai
```

2. **Run Pipeline**:
```bash
python3 -m petaly -c /path/to/petaly.ini run -p my_pipeline --ai
```

### Example Interactions

1. **Create Pipeline**:
```
User: Create a pipeline to load data from CSV to PostgreSQL
AI: I'll help you create a pipeline. First, let's set up the source...
```

2. **Modify Pipeline**:
```
User: Add a new table to the pipeline
AI: I'll help you add the table. What's the table name and structure?
```

3. **Troubleshoot Issues**:
```
User: The pipeline failed with error X
AI: Let me help you diagnose the issue. First, let's check...
```

## Features

### Natural Language Processing
- Create and modify pipelines using natural language
- Get explanations of pipeline configurations
- Receive suggestions for optimization

### Context Awareness
- Remembers previous interactions
- Maintains conversation context
- Learns from your preferences

### Intelligent Assistance
- Suggests best practices
- Helps with troubleshooting
- Provides optimization recommendations

## Best Practices

1. **Clear Communication**:
   - Be specific in your requests
   - Provide necessary context
   - Ask for clarification when needed

2. **Security**:
   - Never share API keys in conversations
   - Use environment variables for sensitive data
   - Regularly rotate API keys

3. **Performance**:
   - Use specific commands for better results
   - Provide relevant context
   - Break complex tasks into steps

## Troubleshooting

### Common Issues

1. **API Key Problems**:
   - Verify API key is set correctly
   - Check provider-specific requirements
   - Ensure sufficient quota

2. **Memory Issues**:
   - Check memory file permissions
   - Verify file path is correct
   - Ensure sufficient disk space

3. **Response Quality**:
   - Provide more context
   - Be more specific in requests
   - Use appropriate technical terms

## Advanced Usage

### Custom Prompts
You can customize the AI's behavior by modifying the prompt templates in your configuration.

### Memory Management
The AI agent can maintain context across sessions using the memory file.

### Integration
The AI agent can be integrated with other tools and workflows through the API.

## Getting Help

If you encounter issues:

1. Check the [Troubleshooting Guide](troubleshooting.md)
2. Review the [Documentation](https://github.com/petaly-labs/petaly/docs/index.md)
3. Join our [Community](https://github.com/petaly-labs/petaly/discussions)
4. Submit a new issue with:
   - Error message
   - Configuration details
   - Steps to reproduce
   - System information

## Related Topics

- [Pipeline Configuration](pipeline_examples.md)
- [Configuration Guide](petaly_ini.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Source and Target Attributes](source_target_attributes.md) 