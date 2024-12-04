# Step 2: Initialize config File and Workspace
This tutorial explains the `petaly.ini` configuration file and how to create a workspace.

## Check petaly installation
If petaly was installed with: 

`$ pip install petaly`

run:

`$ python3 -m petaly --help` 

If the repo was cloned from the repository https://github.com/petaly-labs/petaly navigate to the src folder 
<br>`$ cd petaly/scr` and execute the command above

## Init config file

To create petaly.ini file run following step once:

`$ python3 -m petaly init -c /absolute-path-to-your-config-dir/petaly.ini`

After the configuration file has been created, either use it always with the [-c] argument

`$ python -m petaly init -c /absolute-path-to-your-config-dir/petaly.ini`

or to skip the [-c] argument, set the environment variable `PETALY_CONFIG_DIR`

`$ export PETALY_CONFIG_DIR=/absolute-path-to-your-config-dir`

## Init workspace

This step must be performed only once after petaly installation. 

If the workspace is already initialised, you can skip the following step and start configuring pipelines. If the workspace hasn't been initialised yet, first define following three parameters in petaly.ini

```
pipeline_dir_path=/absolute-path-to-pipelines-dir
logs_dir_path=/absolute-path-to-logs-dir
output_dir_path=/absolute-path-to-output-dir
```
And then initialize the workspace with following command. This command will simply create all these 3 paths defined above in the petaly.ini file.

`$ python3 -m petaly init --workspace -c /path_to_config_dir/petaly.ini` 

