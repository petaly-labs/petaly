# 🌸 Welcome to Petaly!
Petaly is an open-source ETL (Extract, Transform, Load) tool designed by data engineers, for data engineers. Our mission is to simplify data movement and transformation across different data platforms with a tool that truly understands the needs of data professionals.

## Why Petaly?
Petaly is designed for seamless data exchange, starting with support for PostgreSQL, MySQL, and CSV formats. Our goal is to expand and integrate additional open-source technologies, making it easier to connect and transfer data across various systems effortlessly. Petaly is user-friendly and does not require any programming knowledge; you can set up data pipelines simply by configuring them in YAML format, making it ready to use right after installation.

## Important
This is an Alpha version of the Petaly project!

## Getting Started
- **Explore the Documentation:** Check out our documentation to get started with installation, configuration, and best practices for using Petaly.
- **Join the Community:** Connect with fellow contributors, share your experiences, and get support in our community channels.
- **Contribute:** We’re continuously improving Petaly, and your feedback and contributions are invaluable. Check out our Contributing Guide to see how you can get involved.

## Tool Features
In the current version Petaly provides extract and load data between following endpoints:

- CSV
- MySQL (tested version 8.0)
- PostgresQL (tested version 16)

## Requirements:
Petaly was developed on: 
- Python 3.11
- Python 3.10 also supported

## Tested on
Petaly was tested on: 

OS: 
- MacOS 14.6
- Ubuntu 22.04.3 LTS

It's possible that the tool will work with other operating systems and other databases and python versions. It just hasn't been tested yet.

## Installation
### 1. Install with pip

```
pip3 install petaly

```
### 1.a Alternative download and install from GitHub

```
$ git clone git@github.com:petaly-labs/petaly.git

$ cd petaly

# Install venv
$ python3 -m venv .venv

$ source .venv/bin/activate

$ pip3 install "psycopg[binary]"
$ pip3 install PyYAML
$ pip3 install PyMySQL
$ pip3 install cryptography
$ pip3 install pyarrow
$ pip3 install mysql-connector-python
$ pip3 install rich

$ cd src/

# Installation step completed
```

### 2. Init config file and workspace
```
$ python3 -m petaly --help

# init petaly config file
$ python -m petaly init -c /YOUR_PATH_TO_PETALY_CONFIG_PATH/petaly.ini

# After file get created use it with [-c]:
$ python -m petaly init -c /YOUR_PATH_TO_PETALY_CONFIG_PATH/petaly.ini

# Alternative you can specify environment variable PETALY_CONFIG_PATH= and skip [-c] argument
$ export PETALY_CONFIG_PATH=/YOUR_PATH_TO_PETALY_CONFIG_PATH/petaly.ini


# edit petaly.ini file and define three different directories for: pipelines, output and logs
$ vi petaly.ini
[workspace_config]
pipeline_base_dir_path= /YOUR_FULL_PATH/petaly_pipelines
logs_base_dir_path= /YOUR_FULL_PATH/petaly_logs
output_base_dir_path= /YOUR_FULL_PATH/petaly_output
...

# init workspace
$ python3 -m petaly init --workspace
```

### 3. Init first pipeline
```
# run following command to initialize a pipeline my_pipeline and follow steps
$ python3 -m petaly init -p my_pipeline
```

### 4. Run pipeline
```
# following command will execute pipeline my_pipeline and makes changes in target endpoint (database or folder)
$ python3 -m petaly run -p my_pipeline
```
