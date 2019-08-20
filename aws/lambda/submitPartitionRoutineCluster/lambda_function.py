import logging
import boto3
import os
import time

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))

# AWS Client
emr_client = boto3.client('emr', region_name='us-east-2')

# ENV variables
SCRIPT_S3_URI = os.getenv('SCRIPT_S3_URI') 
LOG_DEST_S3_URI = os.getenv('LOG_DEST_S3_URI')
REQUIREMENTS_SCRIPT_S3_URI =  os.getenv('REQUIREMENTS_SCRIPT_S3_URI')

SCRIPT_NAME = SCRIPT_S3_URI.split('/')[-1]
HOME_HADOOP = '/home/hadoop/'


def lambda_handler(event, context):
    cluster_id = emr_client.run_job_flow(
        Name='PartitionRoutine | ' + time.strftime("%Y/%m/%d %H:%M:%S"),
        ReleaseLabel='emr-5.18.0',
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'ganglia'
            },
        ],        
        Instances={
            'InstanceGroups': [
                {
                    'Name': "Master nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm4.2xlarge',
                    'InstanceCount': 1,
                },
                {
                    'Name': "Slave nodes",
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'r5.xlarge',
                    'InstanceCount': 1,
                },         
            ],
            'Ec2KeyName': 'dev_datalake',
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False,
            'Ec2SubnetId': 'subnet-11e84e5d',
        },
        BootstrapActions=[
            {
                'Name': 'Get install requirements script',
                'ScriptBootstrapAction': {
                    'Path': REQUIREMENTS_SCRIPT_S3_URI,
                }
            },
        ],        
        Steps=[
            {
                'Name': 'Get pyspark script',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['aws', 's3', 'cp', SCRIPT_S3_URI, HOME_HADOOP]
                }
            },           
            {
                'Name': 'Run pyspark script',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', HOME_HADOOP + SCRIPT_NAME]
                }
            }
        ],            
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=LOG_DEST_S3_URI,
    )
    
    logger.info('Cluster {} created with the step.'.format(cluster_id['JobFlowId']))