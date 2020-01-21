import logging
import boto3
import os
import time
import json
import argparse

from dotenv import load_dotenv


# Load env vars
load_dotenv('.env')
SCRIPT_S3_URI = os.getenv('SCRIPT_S3_URI')
LOG_DEST_S3_URI = os.getenv('LOG_DEST_S3_URI')
REQUIREMENTS_SCRIPT_S3_URI =  os.getenv(
    'REQUIREMENTS_SCRIPT_S3_URI',
    's3://vtex.datalake/scripts/struct_partition_history_orders/install_requirements.sh'
)


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


# AWS Client
emr_client = boto3.client('emr', region_name='us-east-2')


SCRIPT_NAME = SCRIPT_S3_URI.split('/')[-1]
HOME_HADOOP = '/home/hadoop/'


def generate_steps(folder_prefix, destination_path):
    """
        Create definitions for EMR steps.
        Args:
            - folder_prefix (str): a char among the following 0123456789abcdef. This represents a prefix of a set of folders on s3 bucket.
            - destination_path (str): where to save partitioned data on s3 data lake bucket.
    """    

    copy_script_step = {
        'Name': 'Get partition spark script',
        'ActionOnFailure': 'TERMINATE_CLUSTER',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', SCRIPT_S3_URI, HOME_HADOOP]
        }
    }
    spark_jobs_steps = [
        {
            'Name': 'Run pyspark script for ' + folder_prefix + hexa_prefix,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 
                    '--deploy-mode', 'client',
                    HOME_HADOOP + SCRIPT_NAME,
                    '--folder-prefix-filter', str(folder_prefix + hexa_prefix),
                    '--destination-path', destination_path
                ]
            }
        } for hexa_prefix in '0123456789abcdef'
    ]
    steps = [copy_script_step] + spark_jobs_steps
    return steps


def get_instances_descriptions():
    """Returns the specifications of instances to be required on EC2.""" 

    return {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'c5.4xlarge',
                'InstanceCount': 1,
            },  
            {
                'Name': "Slave nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'c5.4xlarge',
                'InstanceCount': 2,
            }               
        ],
        'Ec2KeyName': 'dev_datalake',
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2SubnetId': 'subnet-11e84e5d',
    }
 

def submit_cluster(cluster_seq, config_from_json, destination_path):
    """
        Create definitions for EMR steps.
        Args:
            - cluster_seq (str): Each cluster started on EMR is responsible for process folders, 
                on data lake s3 bucket, under a prefix (cluster seq) among the following 0123456789abcdef.
            - config_from_json (str): string json with configuration of spark application.
            - destination_path (str): where to save partitioned data on s3 data lake bucket.
    """

    configurations = config_from_json or []

    cluster_id = emr_client.run_job_flow(
        Name='Partition History Orders --- ' + cluster_seq,
        ReleaseLabel='emr-5.26.0',
        Applications=[
            {
                'Name': 'Spark'
            },
            {
                'Name': 'ganglia'
            },
        ],
        Configurations=configurations,
        Instances=get_instances_descriptions(),     
        BootstrapActions=[
            {
                'Name': 'Get install requirements script',
                'ScriptBootstrapAction': {
                    'Path': REQUIREMENTS_SCRIPT_S3_URI,
                }
            },
        ],
        Steps=generate_steps(cluster_seq, destination_path),            
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=LOG_DEST_S3_URI,
    )

    logger.info('Cluster {} created for folder {}*.'.format(cluster_id['JobFlowId'], cluster_seq))


def _print_config():
    print("Running with following config: \n\
        SCRIPT_NAME:{}, \n\
        LOG_DEST_S3_URI:{}, \n"
        .format(SCRIPT_NAME, LOG_DEST_S3_URI))


def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--config-path', 
        help='Local path for json with cluster configuration',
        required=False
    )  
    parser.add_argument(
        '--destination-path',
        help='Where the data to be written. For example: consumable_tables/checkout',
        required=True
    )        
    args=parser.parse_args()
    return args.config_path, args.destination_path


if __name__ == "__main__":
    _print_config()
    
    config_path, destination_path = _read_args()
    
    config = None
    if config_path:
        with open(config_path, 'r') as conf_file:
            config = json.load(conf_file)
    
    for cluster_seq in '0123456789abcdef':
        submit_cluster(cluster_seq, config, destination_path)
