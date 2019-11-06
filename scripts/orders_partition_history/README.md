## Usage

### Requirements
Install dependencies before running script.   
`pip install -r requirements.txt`  or    
`python -m pip install -r requirements.txt`

### Environment vars
Create a file named `.env` in the folder where you will execute script.  
In this file, set the following vars:  
- SCRIPT_S3_URI   
    - S3 URI where partition script is saved
    - ex: SCRIPT_S3_URI=s3://bucket/path/to/script.py
- LOG_DEST_S3_URI   
    - S3 URI where you want to save your logs
    - ex: LOG_DEST_S3_URI=s3://bucket/path/to/logs/

### Args
You can pass some args to execute script

- --config-path   
    - required: **NOT** 
    - in AWS EMR, you can set some custom configuration to initiate your cluster. This configurations can be passed in a json file. So, you can pass the path to a json file with some configurations to initiate your cluster.
