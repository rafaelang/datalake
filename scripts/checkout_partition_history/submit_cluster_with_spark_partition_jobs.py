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


def generate_steps(folder_prefix, path_to_be_read, destination_path):
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
            'Name': 'Run pyspark script for ' + folder_prefix + hexa_prefix + path_to_be_read,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': ['spark-submit', 
                    '--deploy-mode', 'cluster',
                    HOME_HADOOP + SCRIPT_NAME,
                    '--directory-path', str(folder_prefix + hexa_prefix + path_to_be_read),
                    '--destination-path', destination_path
                ]
            }
        } for hexa_prefix in '0123456789ABCDEF'
    ]
    steps = [copy_script_step] + spark_jobs_steps
    return steps


def get_instances_descriptions():
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
 

def submit_cluster(cluster_seq, data_src_type, config_from_json, path_to_be_read, destination_path):
    configurations = config_from_json or []

    cluster_id = emr_client.run_job_flow(
        Name='Partition History --- ' + cluster_seq + " " + data_src_type,
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
        Steps=generate_steps(cluster_seq, path_to_be_read, destination_path),            
        JobFlowRole='EMR_EC2_DefaultRole',
        ServiceRole='EMR_DefaultRole',
        VisibleToAllUsers=True,
        LogUri=LOG_DEST_S3_URI,
    )

    logger.info('Cluster {} created for folder {}*_{}.'.format(cluster_id['JobFlowId'], cluster_seq, data_src_type))


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
        '--path-to-be-read', 
        help='Path to be read the data. For example: _CheckoutOrder',
        required=True,
        choices=('_FulfillmentOrder', '_CheckoutOrder')
    )   
    parser.add_argument(
        '--destination-path',
        help='Where the data to be written. For example: consumable_tables/checkout',
        required=True
    )   
    parser.add_argument(
        '--data-src-type', 
        help='If fulfillment or checkout.',
        required=True,
        choices=('checkout', 'fulfillment')
    )       
    args=parser.parse_args()
    return args.config_path, args.data_src_type.lower(), args.path_to_be_read, args.destination_path


if __name__ == "__main__":
    _print_config()
    
    config_path, data_src_type, path_to_be_read, destination_path = _read_args()
    
    config = None
    if config_path:
        with open(config_path, 'r') as conf_file:
            config = json.load(conf_file)
    
    for cluster_seq in '0123456789ABCDEF':
        submit_cluster(cluster_seq, data_src_type, config, path_to_be_read, destination_path)
