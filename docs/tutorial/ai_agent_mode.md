# Petaly AI Agent Mode Configuration Guide

## Overview
Petaly's AI Agent mode allows you to interact with the ETL tool using natural language instructions. This guide explains how to configure and use the AGENT mode effectively.

## Configuration

### 1. Setting up petaly.ini

The main configuration file (`petaly.ini`) needs to be properly configured to use the AI Agent mode. Here's a complete example:

```ini
# This is the main configuration file for the Petaly tool.
# It includes the workspace_config and global_settings sections.
[workspace_config]

# The following parameter specifies the absolute path to the pipeline directory.
# All pipelines created by init are persisted in this directory.
pipeline_dir_path=your-path-to-pipeline-dir

# The following parameter specifies the absolute path to the log directory.
logs_dir_path=your-path-to-log-dir

# The following parameter specifies the absolute path to the output directory.
# Data output directory is temporary directory used as transition space between source and destination.
output_dir_path=your-path-to-output-dir

[global_settings]
# The logging mode has two settings: INFO and DEBUG.
# By default, it is set to INFO, which generates minimal log output.
# If an issue occurs, switch to DEBUG for more detailed output that can assist in troubleshooting.
logging_mode=INFO

# This is main AI settings section
[ai_settings]
# In case app_mode=AGENT
# Select the LLM provider to use openai or anthropic. (default: openai)
llm_provider=openai

# Select the LLM model to use. Tested with openai (gpt-4o), anthropic (claude-3-opus-20240229
llm_model=gpt-4

# Path to the memory file. Default is ~/.petaly/agent_memory.json
agent_memory_file=~/.petaly/agent_memory.json

# Provide AI_AGENT_API_KEY as an environment variable: export AI_AGENT_API_KEY="YOUR-AI-AGENT-API-KEY-OR-TOKEN"
# Alternative and for the test purpose you can set it directly here
ai_agent_api_key=you-api-key

```

### 2. Environment Variables

For security best practices, you can set the API key as an environment variable instead of in the config file:

```bash
export AI_AGENT_API_KEY="your-api-key-here"
```

## Using the AI Agent Mode

### 1. Use Petaly Agent interactive
```bash
(.aivenv) petaly $ petaly interactive                                                                  
2025-04-29 12:05:57,489 - petaly.ai.agent.petaly_agent - INFO - Initialized PetalyAgent with memory file: ~/.petaly/agent_memory.json

Petaly AI Agent
Type 'exit' or 'quit' to end the session.

PROMPT > Create pipeline: pipeline-name ai2pipe; source-systems endpoint-type mysql (database_host='127.0.0.1', database_port=3306, database_user=root, database_name=xour-database, database_password=); target-systems endpoint-type postgres (database_host=lo
calhost, database_port=5432, database_user=postgres, database_name=your-databse, database_schema=your-schema, database_password=); data_objects_spec object_name=stocks

Agent
I've created a new pipeline named 'ai2pipe'. The configuration has been saved to /Users/pavel/Home/PycharmProjects/petaly-pipes/end2end/ai2pipe/pipeline.yaml. Would you like to review it or make any changes?                                                 

PROMPT > Run pipeline: pipeline-name ai2pipe;
Agent
I've successfully executed the pipeline 'ai2pipe'. 
```

### 2. Basic Commands

The AI Agent understands natural language commands for various ETL operations:

```bash
# List existing pipelines
python -m petaly command --instruction "Show me all available pipelines"
python -m petaly  command --instruction "Show me all available pipelines where source postgres"

```

### 3. Pipeline Creation Examples

The AI Agent can help you create complex pipelines with detailed specifications:

```bash
# Create a pipeline with specific source and target

# Example: MySQL -> BigQuery
python -m petaly command --instruction "Create pipeline: pipeline-name ai1pipe; source-systems endpoint-type mysql (database_host=127.0.0.1, database_port=3306, database_user=root, database_name=your-mysql-database, database_password=); target-systems endpoint-type bigquery (platform_type=gcp, database_schema=your-bigquery-dataset, gcp_project_id='your-gcp-project', gcp_region=EU, gcp_bucket_name='your-gcp-bucket', bucket_pipeline_prefix=petaly/{pipeline_name}); data_objects_spec object_name=your-table-name"

# run pipeline
python -m petaly command --instruction "Run pipeline: pipeline-name ai1pipe;"

# Example: MySQL -> Postgres
python -m petaly command --instruction "Create pipeline: pipeline-name ai2pipe; source-systems endpoint-type mysql (database_host='127.0.0.1', database_port=3306, database_user=root, database_name=petaly_tutorial, database_password=); target-systems endpoint-type postgres (database_host=localhost, database_port=5432, database_user=postgres, database_name=petaly_db, database_schema=petaly_tutorial, database_password=); data_objects_spec object_name=stocks"


python -m petaly command --instruction "Run pipeline: pipeline-name ai2pipe;"

# Example: Postgres -> Redshift
python -m petaly command --instruction "Create pipeline: pipeline-name ai3pipe; source-systems endpoint-type postgres (database_host=localhost, database_port=5432, database_user=postgres, database_name=petaly_db, database_schema=petaly_tutorial, database_password=); target-systems  endpoint-type: redshift (connection_method=iam, is_serverless=true, cluster_identifier='default-workgroup', database_user='your-redshift-database-user', database_name=dev database_schema=your-schema-name, workgroup_name='default-workgroup', platform_type=aws, aws_region: 'eu-north-1', aws_bucket_name='s3-bucket-name', bucket_pipeline_prefix=petaly/{pipeline_name}, aws_iam_role='arn:aws:iam::your-account:role/RedshiftS3Access-Role', aws_profile_name='your-aws-profile', aws_access_key_id=, aws_secret_access_key=); data_objects_spec object_name=your-table-name"

python -m petaly command --instruction "Run pipeline: pipeline-name ai3pipe;"

```

## Supported Features

### 1. Data Sources
- Relational databases (MySQL, PostgreSQL, SQL Server, etc.)
- File-based sources (CSV, JSON, etc.)
- Cloud storage (AWS S3, GCP Cloud Storage)
- Data warehouses (Snowflake, BigQuery, Redshift)


### 3. Platform Support
- Local execution
- Cloud platforms (AWS, GCP)
- Hybrid configurations

## Best Practices

1. **Be Specific**: Provide as much detail as possible in your instructions
2. **Use Clear Names**: Use descriptive names for pipelines and objects
3. **Verify Configurations**: Always review the generated pipeline configuration
4. **Test**: Start with small pipelines and expand gradually
5. **Monitor Logs**: Check logs for any issues or warnings

## Troubleshooting

### Common Issues

1. **API Key Errors**
   - Verify the API key is correctly set in environment or config
   - Check if the key has sufficient permissions

2. **Directory Access**
   - Ensure all configured directories exist and are writable
   - Check directory permissions

3. **Pipeline Generation**
   - If the agent needs more information, it will ask for clarification
   - Review the generated configuration carefully

### Getting Help

For additional support:
- Check the application logs in the configured logs directory
- Review the generated pipeline configurations
- Consult the Petaly documentation for specific connector requirements

## Security Considerations

1. **API Keys**
   - Never commit API keys to version control
   - Use environment variables for sensitive information
   - Rotate keys regularly

2. **Database Credentials**
   - Use secure methods to store database credentials
   - Consider using connection strings or credential managers

3. **Output Data**
   - Ensure output directories have appropriate access controls
   - Regularly clean up temporary files

## Limitations

1. The AI Agent may need clarification for complex transformations
2. Some advanced features may require manual configuration
3. Performance depends on the chosen LLM provider and model
4. Large datasets may require specific optimization instructions

## Future Enhancements

1. Support for more data sources and targets
2. Enhanced transformation capabilities
3. Improved error handling and recovery
4. Additional cloud platform integrations 