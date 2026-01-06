# Installation Guide

## System Requirements

- Python 3.10 - 3.12
- Operating System:
  - Linux
  - MacOS

*Note: Petaly may work on other operating systems and Python versions, but these haven't been tested yet.*

## Installation Methods

### 1. Using pip (Recommended for Users)

#### Basic Installation
```bash
# Create and activate virtual environment
mkdir petaly
cd petaly
python3 -m venv .venv
source .venv/bin/activate

# Install Petaly
python3 -m pip install petaly
```

#### Cloud Provider Support

##### GCP Support
```bash
# Install with GCP support
python3 -m pip install petaly[gcp]
```

**Prerequisites**:
1. Install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install-sdk)
2. Configure access to your Google Project
3. Set up service account authentication

##### AWS Support
```bash
# Install with AWS support
python3 -m pip install petaly[aws]
```

**Prerequisites**:
1. Install [AWS CLI](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-prereqs.html)
2. Configure AWS credentials


#### Full Installation
```bash
# Install all features including AWS and GCP
python3 -m pip install petaly[all]
```

### 2. From Source (Recommended for Developers)

#### Method 1: Editable Installation (Recommended)
```bash
# Clone the repository
git clone https://github.com/petaly-labs/petaly.git
cd petaly

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install development dependencies
pip3 install -r requirements.txt

# Install in editable mode
pip install -e .
```

This method:
- Makes the package available system-wide
- Allows you to modify the code without reinstalling
- Maintains proper Python package structure
- Works with all Python tools and IDEs

#### Method 2: Using PYTHONPATH
```bash
# Clone the repository
git clone https://github.com/petaly-labs/petaly.git
cd petaly

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install development dependencies
pip3 install -r requirements.txt

# Add src to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:$(pwd)/src
```

This method:
- Makes the package available in the current shell
- Requires setting PYTHONPATH in each new shell
- Useful for quick testing or development
- Can be added to your shell profile for persistence

To make the PYTHONPATH setting permanent, add it to your shell profile:
```bash
# For bash
echo 'export PYTHONPATH=$PYTHONPATH:/path/to/petaly/src' >> ~/.bashrc

# For zsh
echo 'export PYTHONPATH=$PYTHONPATH:/path/to/petaly/src' >> ~/.zshrc
```

## Verifying Installation

After installation, verify that Petaly is properly installed:

```bash
# Check version
python3 -m petaly --version

# Run initialization
python3 -m petaly init
```

## Workspace Configuration

### 1. Initialize Configuration

Petaly looks for the configuration file in the following order:
1. User's home directory (`~/.petaly/petaly.ini`) - **Recommended**
2. Path specified in `PETALY_CONFIG_DIR` environment variable
3. Path provided with `-c` option (e.g., `-c /path/to/petaly.ini`)

#### Using Default Location
```bash
# This will create ~/.petaly/petaly.ini
python3 -m petaly init
```

#### Using Custom Location
```bash
# Set environment variable
export PETALY_CONFIG_DIR=/path/to/your/config/directory

# Or specify directly
python3 -m petaly -c /path/to/your/petaly.ini init
```

### 2. Configure Workspace

The default `petaly.ini` will contain:
```ini
[workspace_config]
# Directory for storing pipeline configurations
pipeline_dir_path=/home/username/petaly/pipelines

# Directory for storing log files
logs_dir_path=/home/username/petaly/logs

# Directory for temporary data storage during pipeline execution
output_dir_path=/home/username/petaly/output

# Connections file path (optional)
# If not specified, defaults to: pipeline_dir_path/connections.yaml
# Example: /home/username/petaly/connections.yaml
connections_file_path=

[global_settings]
# Logging level: INFO or DEBUG
logging_mode=INFO

# Pipeline configuration format: yaml or json
pipeline_format=yaml

```

### 3. Initialize Workspace

After configuring `petaly.ini`, create the workspace structure:
```bash
# Create workspace directories and structure
python3 -m petaly init --workspace
```

This command will:
- Create all necessary directories specified in `petaly.ini`
- Set up the initial workspace structure
- Verify directory permissions
- Create default templates and configurations
- Create `connections.yaml` file (at `pipeline_dir_path/connections.yaml` or custom `connections_file_path` if specified)

## Next Steps

1. [Configure Petaly](petaly_ini.md)
2. [Create Your First Pipeline](pipeline_examples.md)
