You can trigger the partition historic orders scripts flow on your machine using the `submit_cluster_with_spark_partition_jobs.py` script. It will request EC2 instances, set up machines and run the orders partition script. For that, you only need to run the script `submit_cluster_with_spark_partition_jobs.py` on your local machine. It will only be needed to have aws cli installed.
 
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
