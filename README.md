![](https://raw.githubusercontent.com/petaly-labs/petaly/main/images/logo/petaly_logo_transparent.png)

![](https://raw.githubusercontent.com/petaly-labs/petaly/main/docs/tutorial/recording/petaly_run_pipe.gif)

## Welcome to Petaly!

Petaly is an open-source ETL (Extract, Transform, Load) tool created by and for data professionals! 
Our mission is to simplify data movement and transformation across different platforms with a tool that truly understands the needs of the data community.

Designed for seamless data exchange, Petaly supports PostgreSQL, MySQL and CSV formats, with plans to expand and integrate additional open-source technologies. 
It makes connecting and transferring data across various systems effortless. Petaly is user-friendly and requires no programming knowledge. 
Data pipelines can be easily configured using the YAML format, making the tool ready to use immediately after installation.

## Important
This is an Alpha version of the Petaly project!

## Getting Started

- **Explore the Documentation:** Check out our documentation to get started with: 
  - **[installation](https://github.com/petaly-labs/petaly/blob/main/docs/tutorial/petaly_install.md)**
  - **[configuration](https://github.com/petaly-labs/petaly/blob/main/docs/tutorial/petaly_init_workspace.md)**
  - **[How to set up your first pipeline](https://github.com/petaly-labs/petaly/blob/main/docs/tutorial/pipeline_explained.md)**
- **Contribute:** We’re continuously improving Petaly, and your feedback and contributions are invaluable. Check out our Contributing Guide to see how you can get involved.
- **Join the Community:** Connect with fellow contributors, share your experiences, and get support in our community channels.

## Tool Features
In the current version Petaly provides extract and load data between following endpoints:

- CSV
- MySQL (tested version 8.0+)
- PostgresQL (tested version 16+)

## Requirements:
- Python 3.10 - 3.12

## Tested on
Petaly was tested on: 

OS: 
- MacOS 14.6
- Ubuntu 22.04.3 LTS

It's possible that the tool will work with other operating systems and other databases and python versions. It just hasn't been tested yet.

## Installation
### 1. Install with pip

```
$ mkdir petaly
$ cd petaly
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install petaly

```
### 1.a Alternatively, download and install from GitHub

```
$ git clone https://github.com/petaly-labs/petaly.git
$ cd petaly
$ python3 -m venv .venv
$ source .venv/bin/activate
$ pip3 install -r requirements.txt
$ cd src/

# Installation step completed
```

### 2. Init config file and workspace

```
# Check petaly installation
$ python3 -m petaly --help

# Init petaly config file
$ python -m petaly init -c /YOUR_PATH_TO_PETALY_CONFIG_DIR/petaly.ini

# After the config file is created use it with [-c] argument
$ python -m petaly init -c /YOUR_PATH_TO_PETALY_CONFIG_DIR/petaly.ini

# To skip the [-c] argument, set the environment variable PETALY_CONFIG_DIR
$ export PETALY_CONFIG_DIR=/YOUR_PATH_TO_PETALY_CONFIG_DIR

$ vi petaly.ini
# Edit the petaly.ini file and define three different folders for: pipelines, logs and output
[workspace_config]
pipeline_dir_path= /YOUR_FULL_PATH_FOR_PIPELINES
logs_dir_path= /YOUR_FULL_PATH_FOR_LOGS
output_dir_path= /YOUR_FULL_PATH_FOR_OUTPUT
...

# Init workspace
$ python3 -m petaly init --workspace -c /YOUR_PATH_TO_PETALY_CONFIG_DIR/petaly.ini
```

### 3. Init a pipeline

```
# Run the following command and follow the wizard steps to initialize a pipeline my_pipeline
# No changes will be made to the target endpoint at this point.
$ python3 -m petaly init -p my_pipeline
```

### 4. Run the pipeline

```
# Now you can run the pipeline my_pipeline and load data from the specified source. 
# Note that it will make changes, re/create tables in the target endpoint (database or folders)
$ python3 -m petaly run -p my_pipeline
```

To learn more about pipeline configuration parameters, use the following tutorial: **[How to set up your first pipeline](https://github.com/petaly-labs/petaly/blob/main/docs/tutorial/pipeline_explained.md)**

