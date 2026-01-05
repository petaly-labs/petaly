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
  - Parquet files
  - JSON files

- **Features**:
  - Source to target schema evaluation and mapping
  - CSV/Parquet/JSON file load with column-type recognition
  - Target table structure generation
  - Configurable type mapping between different databases
  - Full table unload/load in CSV, Parquet, or JSON format
  - Direct format conversion (CSV ↔ Parquet ↔ JSON)
  - Direct export/import for BigQuery and Redshift (Parquet/JSON)

- **User-Friendly**: No programming knowledge required
- **YAML/JSON Configuration**: Easy pipeline setup
- **Cloud Ready**: Full support for AWS and GCP




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
python3 -m petaly init -p csv2psql
```

2. **Configure Connections** (if using connection names)
   - Set up `csv_local` connection in `connections.yaml`
   - Set up `my_postgres` connection in `connections.yaml`
   - See [Connections Template](docs/connections.yaml-template) for details

3. **Configure Pipeline**
   - Use `csv` as source with `connection_name: csv_local`
   - Use `postgres` as target with `connection_name: my_postgres`
   - Set `type_autodetection: true` for automatic type detection
   - Configure `object_source_dir` for each data object

4. **Run Pipeline**
```bash
python3 -m petaly run -p csv2psql
```

### Example Configuration

**Using Connection Names (Recommended):**
```yaml
pipeline:
  pipeline_name: psql2bq
  source_attributes:
    connection_name: my_postgres
    database_schema: petaly_tutorial
  target_attributes:
    connection_name: my_bigquery
    database_schema: petaly_tutorial
    bucket_pipeline_prefix: petaly/{pipeline_name}
  data_attributes:
    include_data_objects: spec
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double
data_objects_spec:
- object_spec:
    object_name: new_stocks
    destination_object_name: new_stocks
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    - adjust_close
```

**CSV to PostgreSQL Example:**
```yaml
pipeline:
  pipeline_name: csv2psql
  source_attributes:
    connection_name: csv_local
    source_dir: /path/to/csv/folder
    type_autodetection: true
  target_attributes:
    connection_name: my_postgres
    database_schema: petaly_tutorial
  data_attributes:
    include_data_objects: spec
    csv_default_settings:
      header: true
      columns_delimiter: ','
      columns_quote: double
data_objects_spec:
- object_spec:
    object_name: stocks
    destination_object_name: null
    recreate_destination_object: true
    cleanup_linebreak_in_fields: false
    exclude_columns:
    - null
    object_source_dir: stocks/
    file_names:
    -
```

For more examples, see [Pipeline Configuration Guide](docs/pipeline_examples.md) and [Connections Template](docs/connections.yaml-template).

## Documentation

- [Pipeline Configuration Guide](docs/pipeline_examples.md)
- [Source and Target Attributes](docs/source_target_attributes.md)
- [Connections Template](docs/connections.yaml-template)
- [CLI Reference](docs/cli_reference.md)
- [Cloud Platform Guide](docs/cloud_platforms.md)
- [Troubleshooting Guide](docs/troubleshooting.md)
- [Video Tutorials](docs/recording/)

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

Petaly is licensed under the Apache License 2.0. See the [LICENSE](LICENSE.md) file for details.