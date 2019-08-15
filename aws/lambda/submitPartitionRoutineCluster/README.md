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