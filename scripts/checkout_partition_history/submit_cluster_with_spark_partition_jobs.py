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


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(getattr(logging, os.getenv('LOG_LEVEL', 'INFO')))


# AWS Client
emr_client = boto3.client('emr', region_name='us-east-2')


SCRIPT_NAME = SCRIPT_S3_URI.split('/')[-1]
HOME_HADOOP = '/home/hadoop/'
CHECKOUT = 'checkout'
FULFILLMENT = 'fulfillment'


def generate_steps(folder_prefix):
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
            'Name': 'Run pyspark script for ' + folder_prefix + hexa_prefix + ' CHECKOUT',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 
                    '--deploy-mode', 'cluster',
                    HOME_HADOOP + SCRIPT_NAME,
                    '--prefix-path', str(folder_prefix + hexa_prefix)]
            }
        } for hexa_prefix in '0123456789ABCDEF'
    ]
    steps = [copy_script_step] + spark_jobs_steps
    return steps


def get_instances_descriptions():
    ## TODO: move to envs the last ones
    return {
        'InstanceGroups': [
            {
                'Name': "Master nodes",
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'r5.2xlarge',
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
 

def submit_cluster(cluster_seq, config_from_json):
    configurations = config_from_json or []

    cluster_id = emr_client.run_job_flow(
        Name='Partition History --- ' + cluster_seq + "*_CheckoutOrder",
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
        Steps=generate_steps(cluster_seq),            
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=LOG_DEST_S3_URI,
    )

    logger.info('Cluster {} created for folder {}*_Checkout.'.format(cluster_id['JobFlowId'], cluster_seq))


def _print_config():
    print("Running with following config: \n\
        SCRIPT_NAME:{}, \n\
        LOG_DEST_S3_URI:{}, \n"
        .format(SCRIPT_NAME, LOG_DEST_S3_URI))


def _read_args():
    parser=argparse.ArgumentParser()
    parser.add_argument(
        '--config-path', 
        help='Local path for json with cluster configuration'
    )   
    args=parser.parse_args()
    return args.config_path


if __name__ == "__main__":
    _print_config()
    
    config_path = _read_args()
    
    config = None
    if config_path:
        with open(config_path, 'r') as conf_file:
            config = json.load(conf_file)
    
    for cluster_seq in '46BEF':
        submit_cluster(cluster_seq, config)
