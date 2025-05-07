# Installation Guide

This guide provides detailed instructions for installing Petaly and its various components.

## System Requirements

### Python Version
- Python 3.10 - 3.12
- pip (Python package installer)

### Operating Systems
- Linux
- MacOS

*Note: Petaly may work on other operating systems and Python versions, but these haven't been tested yet.*

## Basic Installation

### 1. Create Virtual Environment
```bash
# Create a new directory for your project
mkdir petaly
cd petaly

# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate
```

### 2. Install Petaly
```bash
# Basic installation
python3 -m pip install petaly
```

## Optional Components

### AI Agent Support
```bash
# Install with AI support
python3 -m pip install petaly[ai]

# Or install all features including AI
python3 -m pip install petaly[all]
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
# Install all features
python3 -m pip install petaly[all]
```

## Installation from Source

### 1. Clone Repository
```bash
git clone https://github.com/petaly-labs/petaly.git
cd petaly
```

### 2. Set Up Environment
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip3 install -r requirements.txt
```

### 3. Choose Your Approach

#### Option 1: Run Directly from Source (Quick Start)
```bash
# Navigate to src directory
cd src

# Run Petaly
python3 -m petaly init
```

#### Option 2: Install as a Package (Recommended)
```bash
# Install Petaly in development mode
pip3 install -e .

# Now you can run Petaly from any directory
python3 -m petaly init
```

*Note: Option 2 (installing as a package) is recommended because:*
- *You can run Petaly from any directory*
- *The package is properly integrated with Python's module system*
- *It's easier to manage dependencies*
- *It follows Python packaging best practices*

## Post-Installation Setup

### 1. Initialize Configuration

Petaly looks for the configuration file in the following order:
1. User's home directory (`~/.petaly/petaly.ini`) - **Recommended**
2. Path specified in `PETALY_CONFIG_DIR` environment variable
3. Path provided with `-c` option (e.g., `-c /path/to/petaly.ini`)

#### Recommended: Use Default Location
The recommended approach is to use the default location in your home directory:
```bash
# This will create ~/.petaly/petaly.ini
python3 -m petaly init
```

#### Alternative 1: Set Environment Variable
```bash
# Add to your shell profile (~/.bashrc, ~/.zshrc, etc.)
export PETALY_CONFIG_DIR=/path/to/your/config/directory
```

#### Alternative 2: Specify Custom Path
```bash
# Use a custom location for petaly.ini
python3 -m petaly init -c /path/to/your/petaly.ini
```

The default `petaly.ini` will contain:
```ini
[workspace_config]
pipeline_dir_path=/home/username/petaly/pipelines
logs_dir_path=/home/username/petaly/logs
output_dir_path=/home/username/petaly/output

[global_settings]
logging_mode=INFO
pipeline_format=yaml

[ai_settings]
llm_provider=openai
llm_model=gpt-4
agent_memory_file=~/.petaly/agent_memory.json
```

### 2. Initialize Workspace
```