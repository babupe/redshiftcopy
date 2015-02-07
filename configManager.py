#!/usr/bin/python
import sys,getopt
from generic_functions import *
import logging
import os

def show_usage(exit_code):
    print 'Usage: ./configManager.py -d <DynamoDB table with config file name> -q <SQS queue name>'
    sys.exit(exit_code)

def validate_args(argv,dynamodb_conn,sqs_conn):
    if len(argv) == 0:
        show_usage(1)
    try:
        opts,args = getopt.getopt(argv,"hd:q:",["help","ddb_table_name=","queue_name="])
    except getopt.GetoptError:
        show_usage(1)
    input_params = {}
    param_count = 0
    for opt,arg in opts:
        if opt == '-h':
            show_usage(1)
        elif opt in ("-q","--queue_name"):
          check_sqs_queue_exists(sqs_conn,arg)
          input_params['queue_name'] = arg
          param_count += 1 
        elif opt in ("-d","--ddb_table_name"):
          check_ddb_tbl_exists(dynamodb_conn,arg,'Y')
          input_params['ddb_table_name'] = arg
          param_count += 1 

    if param_count != 2:
        show_usage(1)
    return input_params

def main():
  if int(os.popen('ps -ef|grep configManager.py|grep -v grep|wc -l').read()) == 1:
    global logger
    logger = logging.getLogger('configManager')
    logger.setLevel(logging.INFO)
 
    # create the logging file handler
    fh = logging.FileHandler("/tmp/configManager.log")
 
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setFormatter(formatter)
 
    # add handler to logger object
    logger.addHandler(fh)
    logger.addHandler(consoleHandler)
    dynamodb_conn = conn_to_dynamodb()
    sqs_conn = conn_to_sqs()
    input_params = validate_args(sys.argv[1:],dynamodb_conn,sqs_conn)
    s3Queue = getQueue(sqs_conn,input_params['queue_name'])
    myMessage = get_sqs_message(s3Queue,10)
    if len(myMessage) > 0:
      s3Key = getS3Key(myMessage)
      if len(s3Key) == 0:
        logger.info('No Action necessary!')
      else:
        if len(s3Key) == 4:
          s3Key['versionId'] = ''
        if int(s3Key['size']) != 0:
          logger.info('New S3 key '+s3Key['Key']+' found. Processing!!')
          is_processed = processMessage(dynamodb_conn, input_params['ddb_table_name'], s3Key)
        else:
          is_processed = 0
        if is_processed == 0:
          logger.info(s3Key['Key']+' has been processed.')
          logger.warn('Deleting Message from Queue!!')
          try:
            delete_message(myMessage[0])
          except:
            logger.warn('Message could not be deleted. But, Do not worry since the configManager is idempotent!!')
        else:
          logger.warn(s3Key['Key']+' could not be processed.')
          logger.info('Message will be retried after the visibility timeout when it appears again in the queue!!')
    logger.info('Sleeping for 30 secs before checking again for messages!!!')
if __name__ == "__main__":
  main()