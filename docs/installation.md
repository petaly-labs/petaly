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
cd src/
```

## Post-Installation Setup

### 1. Initialize Configuration
```bash
# Create petaly.ini
python3 -m petaly -c /absolute-path-to-your-config-dir/petaly.ini init
```

### 2. Set Environment Variable (Optional)
```bash
export PETALY_CONFIG_DIR=/absolute-path-to-your-config-dir
```

### 3. Initialize Workspace
1. Configure `petaly.ini`:
```ini
[workspace_config]
pipeline_dir_path=/absolute-path-to-pipelines-dir
logs_dir_path=/absolute-path-to-logs-dir
output_dir_path=/absolute-path-to-output-dir
```

2. Create workspace:
```bash
python3 -m petaly -c /path_to_config_dir/petaly.ini init --workspace
```

## Verification

### 1. Check Installation
```bash
# Verify Petaly is installed
python3 -m petaly --version
```

### 2. Test Basic Functionality
```bash
# Create a test pipeline
python3 -m petaly -c /path_to_config_dir/petaly.ini init -p test_pipeline
```

## Common Issues

### Python Version Issues
**Issue**: Installation fails with Python version error
**Solution**: Ensure you're using Python 3.10 - 3.12
```bash
python3 --version
```

### Virtual Environment Issues
**Issue**: Package not found after installation
**Solution**: Ensure virtual environment is activated
```bash
source .venv/bin/activate
```

### Permission Issues
**Issue**: Permission denied during installation
**Solution**: Use appropriate permissions or virtual environment
```bash
# Create virtual environment in user space
python3 -m venv ~/.venv/petaly
source ~/.venv/petaly/bin/activate
```

## Best Practices

1. **Virtual Environment**
   - Always use a virtual environment
   - Keep dependencies isolated
   - Easy to manage different versions

2. **Configuration**
   - Use absolute paths
   - Store sensitive data in environment variables
   - Keep configuration files in version control

3. **Workspace Organization**
   - Separate directories for different environments
   - Clear naming conventions
   - Regular cleanup of temporary files

## Related Topics

- [Configuration Guide](petaly_ini.md)
- [Pipeline Configuration](pipeline_examples.md)
- [Source and Target Attributes](source_target_attributes.md)
- [Troubleshooting Guide](troubleshooting.md) 