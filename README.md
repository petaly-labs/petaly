![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_logo_transparent.png)

![](https://raw.githubusercontent.com/petaly-labs/petaly/main/docs/tutorial/recording/petaly_run_pipe.gif)

## Overview

Petaly is an open-source ETL/ELT (Extract, Load, "Transform") tool, created by and for data professionals! Our mission is to simplify data movement across different platforms with a tool that truly understands the needs of the data community.

### Key Features

- **Multiple Data Sources**: Support for various endpoints:
  - PostgreSQL
  - MySQL
  - BigQuery
  - Redshift
  - Google Cloud Storage (GCS Bucket)
  - S3 Bucket
  - Local CSV files

- **Features**:
  - Source to target schema evaluation and mapping
  - CSV file load with column-type recognition
  - Target table structure generation
  - Configurable type mapping between different databases
  - Full table unload/load in CSV format

- **User-Friendly**: No programming knowledge required
- **YAML/JSON Configuration**: Easy pipeline setup
- **Cloud Ready**: Full support for AWS and GCP


**[EXPERIMENTAL]**:

Petaly went agentic!<br>
The AI Agent can create and run pipeline using natural language prompts.<br>
If you're interested in exploring, check out the experimental branch: [petaly-ai-agent](https://github.com/petaly-labs/petaly/tree/petaly-ai-agent)<br>

Feedback is welcome! 

## Quick Start

1. **[Installation](#installation)**
2. **[Configuration](#configuration)**
3. **[Create Pipeline](#create-pipeline)**
4. **[Run Pipeline](#run-pipeline)**

## Requirements

### System Requirements
- Python 3.10 - 3.12
- Operating System:
  - Linux
  - MacOS

*Note: Petaly may work on other operating systems and Python versions, but these haven't been tested yet.*

## Installation

### Basic Installation
```bash
# Create and activate virtual environment
mkdir petaly
cd petaly
python3 -m venv .venv
source .venv/bin/activate

# Install Petaly
python3 -m pip install petaly
```

### Cloud Provider Support

#### GCP Support
```bash
# Install with GCP support
python3 -m pip install petaly[gcp]
```

**Prerequisites**:
1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
2. Configure access to your Google Project
3. Set up service account authentication

#### AWS Support
```bash
# Install with AWS support
python3 -m pip install petaly[aws]
```

**Prerequisites**:
1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
2. Configure AWS credentials


### Full Installation
```bash
# Install all features including AWS, GCP
python3 -m pip install petaly[all]
```

### From Source
```bash
# Clone the repository
git clone https://github.com/petaly-labs/petaly.git
cd petaly

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install development dependencies
pip3 install -r requirements.txt

# Install in editable mode (recommended)
pip install -e .

# Alternative: Add src to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
```

## Configuration

### 1. Initialize Configuration
```bash
# Create petaly.ini in default location (~/.petaly/petaly.ini)
python3 -m petaly init

# Or specify custom location
python3 -m petaly -c /absolute-path-to-your-config-dir/petaly.ini init
```

### 2. Set Environment Variable (Optional)
```bash
# Set the environment variable if the folder differs from the default location
export PETALY_CONFIG_DIR=/absolute-path-to-your-config-dir

# Alternative run command using the main config parameter: -c /absolute-path-to-your-config-dir/petaly.ini
python3 -m petaly -c /absolute-path-to-your-config-dir/petaly.ini [command]
```

### 3. Initialize Workspace
1. Configure `petaly.ini`:
```ini
[workspace_config]
pipeline_dir_path=/home/user/petaly/pipelines
logs_dir_path=/home/user/petaly/logs
output_dir_path=/home/user/petaly/output

[global_settings]
logging_mode=INFO
pipeline_format=yaml

```

2. Create workspace:
```bash
python3 -m petaly init --workspace
```

## Create Pipeline

Initialize a new pipeline:
```bash
python3 -m petaly init -p my_pipeline
```

Follow the wizard to configure your pipeline. For detailed configuration options, see [Pipeline Configuration Guide](docs/pipeline_examples.md).

## Run Pipeline

Execute your pipeline:
```bash
python3 -m petaly run -p my_pipeline
```

### Run Specific Operations
```bash
# Extract data from source only
python3 -m petaly run -p my_pipeline --source_only

# Load data to target only
python3 -m petaly run -p my_pipeline --target_only

# Run specific objects
python3 -m petaly run -p my_pipeline -o object1,object2
```

## Tutorial: CSV to PostgreSQL

### Prerequisites
- Petaly installed and workspace initialized
- PostgreSQL server running

### Steps

1. **Initialize Pipeline**
```bash
python3 -m petaly init -p csv_to_postgres
```

2. **Download Test Data**
```bash
# Download and extract test files
gunzip options.csv.gz
gunzip stocks.csv.gz
```

3. **Configure Pipeline**
- Use `csv` as source
- Use `postgres` as target
- Configure database connection details

4. **Run Pipeline**
```bash
python3 -m petaly run -p csv_to_postgres
```

### Example Configuration
```yaml
pipeline:
  pipeline_attributes:
    pipeline_name: csv_to_postgres
    is_enabled: true
  source_attributes:
    connector_type: csv
  target_attributes:
    connector_type: postgres
    database_user: root
    database_password: db-password
    database_host: localhost
    database_port: 5432
    database_name: petalydb
    database_schema: petaly_tutorial
  data_attributes:
    use_data_objects_spec: only
    object_default_settings:
      header: true
      columns_delimiter: ","
      columns_quote: none
```

## Documentation

- [Pipeline Configuration Guide](docs/pipeline_examples.md)
- [Cloud Platform Guide](docs/cloud_platforms.md)
- [Troubleshooting Guide](docs/troubleshooting.md)
- [Video Tutorials](docs/recording/)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Petaly is licensed under the Apache License 2.0. See the [LICENSE](LICENSE.md) file for details.