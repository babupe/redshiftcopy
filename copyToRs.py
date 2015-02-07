#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging
import threading
import time
import os

def show_usage(exit_code):
    print 'Usage: ./copyToRs.py -d <DynamoDB table with config file name>'
    sys.exit(exit_code)

def validate_args(argv,dynamodb_conn):
    if len(argv) == 0:
        show_usage(1)
    try:
        opts,args = getopt.getopt(argv,"hd:",["help","ddb_table_name="])
    except getopt.GetoptError:
        show_usage(1)
    input_params = {}
    param_count = 0
    for opt,arg in opts:
        if opt == '-h':
            show_usage(1)
        elif opt in ("-d","--ddb_table_name"):
          check_ddb_tbl_exists(dynamodb_conn,arg,'N')
          input_params['ddb_table_name'] = arg
          param_count += 1 

    if param_count != 1:
        show_usage(1)
    return input_params

def main():
    if int(os.popen('ps -ef|grep copyToRs.py|grep -v grep|wc -l').read()) == 1:
        global logger
        logger = logging.getLogger('copyToRs')
        logger.setLevel(logging.INFO)
        # create the logging file handler
        fh = logging.FileHandler("/tmp/copyToRs.log")

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setFormatter(formatter)
        # add handler to logger object
        logger.addHandler(fh)
        logger.addHandler(consoleHandler)
        dynamodb_conn = conn_to_dynamodb()
        sqs_conn = conn_to_sqs()
        s3_conn = conn_to_s3()
        rs_conn = conn_to_rs()
        input_params = validate_args(sys.argv[1:],dynamodb_conn)
        quitLoop = 0
        awscreds = getInstanceCredentials()
        while quitLoop == 0:
            try:
                currItem = return_value_from_dynamodb(dynamodb_conn,input_params['ddb_table_name'],1)
                quitLoop = 1
            except boto.dynamodb.exceptions.DynamoDBKeyNotFoundError:
                logger.warn('No Config File found! Publish the config file in your S3 Config directory!!')
                sleep(30)
        currConfigFile = currItem['current_version_file']
        logger.info('Config to use '+currConfigFile)
        configValues=getS3KeyContents(s3_conn,currConfigFile)
        logger.info('Config values extracted!!')
        if getRedshiftClusterState(rs_conn,configValues['rsClusterId']).upper() != 'AVAILABLE':
            logger.info('Redshift cluster not yet available!')
        else:
            if sqs_conn.get_queue(configValues['rsJobQueueName']):
                sqs_queue = sqs_conn.get_queue(configValues['rsJobQueueName'])
                counter = 1
                while counter < 10:
                    msg = sqs_queue.read()
                    if msg is not None:
                        manifestKey = msg.get_body()
                        copyCommandStaging = "copy "+configValues['stagingTableName']+" from '"+manifestKey+"' with credentials as 'aws_access_key_id="+awscreds['AccessKeyId']+";aws_secret_access_key="+awscreds['SecretAccessKey']+";token="+awscreds['Token']+"' manifest csv"
                        clusterEndPoint = rs_conn.describe_clusters(cluster_identifier=configValues['rsClusterId'])['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['Endpoint']['Address']
                        connString = "host='"+clusterEndPoint+"' dbname='"+configValues['dbName']+"' user='"+configValues['masterUserName']+"' password='"+configValues['masterPassword']+"' port="+configValues['dbPort']
                        copytoRS(copyCommandStaging,connString,configValues['tableName'],configValues['columnMapping'],configValues['stagingTableName'])
                        sqs_queue.delete_message(msg)
                    else:
                        logger.info('No Manifest file to load into Redshift!')
                    counter += 1

if __name__ == "__main__":
  main()
