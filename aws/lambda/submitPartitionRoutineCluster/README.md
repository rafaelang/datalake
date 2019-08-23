### Description

This function must be triggered hourly by an CloudWatch Rules and has the aim of create a cluster and submit a pyspark task.
This pyspark task is intended to partition stream data by creation or update date.


### Execution Role

There is an role called VtexDatalakeLambdaEC2Access with the following polices:
    - AmazonEC2FullAccess
    - AWS managed policy
    - Log policies


### Env vars

This lambda needs these vars: 

    - LOG_DEST_S3_URI
        - s3 uri where save logs 
        - ex: s3://aws-logs-282989224251-us-east-2/elasticmapreduce/partition_routine/clusters/  
        
    - SCRIPT_S3_URI  
        - s3 uri where is located the script to be submitted 
        - ex: s3://vtex.datalake/scripts/script.py  

    - REQUIREMENTS_SCRIPT_S3_URI
        - s3 uri where is located script to install dependencies to run partition script
        - ex: s3://vtex.datalake/scripts/partition_routine/install_requirements.sh

    - DESTINATION_S3_URI_PREFIX
        - s3 uri where processed data will be stored
        - ex: s3://vtex.datalake/consumable_tables/

    - DATASRC_S3_PREFIX
        - s3 uri for dataset to read
        - ex: s3://vtex.datalake/stage/checkout_data/
