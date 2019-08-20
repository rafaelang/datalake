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
SCRIPT_S3_URI = os.getenv(
    'SCRIPT_S3_URI',
    's3://vtex.datalake/scripts/partition_routine/partition.py'
) 
LOG_DEST_S3_URI = os.getenv(
    'LOG_DEST_S3_URI',
    's3://aws-logs-282989224251-us-east-2/elasticmapreduce/partition_routine/clusters/'
)
REQUIREMENTS_SCRIPT_S3_URI =  os.getenv(
    'REQUIREMENTS_SCRIPT_S3_URI',
    's3://vtex.datalake/scripts/partition_routine/install_requirements.sh'
)
DESTINATION_S3_URI_PREFIX = os.getenv(
    'DESTINATION_S3_URI_PREFIX',
    's3://vtex.datalake/consumable_tables/'
)
DATASRC_S3_PREFIX = os.getenv(
    'DATASRC_S3_PREFIX',
    's3://vtex.datalake/stage/checkout_data/'
)

SCRIPT_NAME = SCRIPT_S3_URI.split('/')[-1]
HOME_HADOOP = '/home/hadoop/'
CHECKOUT = 'checkout'
FULFILLMENT = 'fulfillment'


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
                'Name': 'Run pyspark script for CHECKOUT',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', HOME_HADOOP + SCRIPT_NAME,
                        '--destination-path', DESTINATION_S3_URI_PREFIX + CHECKOUT + '/',
                        '--datasrc-s3', DATASRC_S3_PREFIX + CHECKOUT + "order/"]
                }
            },
            {
                'Name': 'Run pyspark script for FULFILLMENT',
                'ActionOnFailure': 'CONTINUE',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': ['spark-submit', HOME_HADOOP + SCRIPT_NAME,
                        '--destination-path', DESTINATION_S3_URI_PREFIX + FULFILLMENT + '/',
                        '--datasrc-s3', DATASRC_S3_PREFIX + FULFILLMENT + "order/"]
                }
            }            
        ],            
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=LOG_DEST_S3_URI,
    )
    
    logger.info('Cluster {} created with the step.'.format(cluster_id['JobFlowId']))