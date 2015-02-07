#!/usr/bin/python
import boto
import boto.kinesis
import boto.utils
import boto.sqs
from boto.utils import get_instance_metadata
from boto.ec2.cloudwatch import CloudWatchConnection
from boto.dynamodb.condition import *
from boto.dynamodb2.table import Table
from boto.sqs.message import Message
from boto.s3.key import Key
import boto.emr
import boto.redshift
import psycopg2
from boto.emr.step import StreamingStep
import math
import time
import logging
import inspect
import sys,ast
from datetime import datetime

global logger
logger = logging.getLogger('generic_functions')
logger.setLevel(logging.INFO)
fh = logging.FileHandler("/tmp/"+inspect.stack()[1][1][2:][:-3]+".log")
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(formatter)
logger.addHandler(fh)
logger.addHandler(consoleHandler)

def sleep(time_to_sleep):
    time.sleep(time_to_sleep)

def get_region():
    metadata = get_instance_metadata()
    return metadata['placement']['availability-zone'][:-1]

def getInstanceCredentials():
    awscreds = {}
    metadata = get_instance_metadata()
    awscreds['AccessKeyId']  = metadata['iam']['security-credentials']['AppRole']['AccessKeyId']
    awscreds['SecretAccessKey']  = metadata['iam']['security-credentials']['AppRole']['SecretAccessKey']
    awscreds['Token']  = metadata['iam']['security-credentials']['AppRole']['Token']
    return awscreds

def conn_to_dynamodb():
    return boto.connect_dynamodb(get_region())

def conn_to_sqs():
    return boto.sqs.connect_to_region(get_region())

def conn_to_s3():
    return boto.connect_s3(get_region())

def conn_to_emr():
    return boto.emr.connect_to_region(get_region())

def conn_to_rs():
    return boto.redshift.connect_to_region(get_region())

def getQueue(sqs_conn,queue_name):
    return sqs_conn.get_queue(queue_name)

def get_sqs_message(s3queue,waitTime):
    return s3queue.get_messages(num_messages = 1,wait_time_seconds = waitTime)

def get_sqs_messages(s3queue,waitTime,numMessages):
    return s3queue.get_messages(num_messages = numMessages,wait_time_seconds = waitTime)

def generateManifest(s3KeyList):
    fileListArr = s3KeyList.split(',')
    currTime = str(datetime.now())
    arrcnt = 0
    fname =  '/tmp/'+currTime[:4]+currTime[5:7]+currTime[8:10]+currTime[11:13]+currTime[14:16]+'.mft'
    mftfh = open(fname,'a')
    mftfh.write('{\n')
    mftfh.write('  "entries": [\n')
    for fileName in fileListArr:
        if arrcnt == len(fileListArr)-1:
             mftfh.write('    {"url":"'+fileName+'", "mandatory":true}\n')
        else:
            mftfh.write('    {"url":"'+fileName+'", "mandatory":true},\n')
        arrcnt += 1
    mftfh.write('  ]\n')
    mftfh.write('}')
    mftfh.close()
    return fname

def uploadManifesttoS3(s3conn,s3Bucket,mnftFileName):
    bucket = s3conn.get_bucket(s3Bucket)
    getFile = Key(bucket)
    getFile.key = mnftFileName.split('/')[-1]
    getFile.set_contents_from_filename(mnftFileName)
    return 's3://'+s3Bucket+'/'+mnftFileName.split('/')[-1]

def createRsJob(s3conn,s3Bucket,s3KeyList,sqs_conn,rsjobqueue):
    logger.info('Preparing the manifest now!')
    mnftFileName = generateManifest(s3KeyList)
    logger.info('Manifest prepared. Uploading to S3 now!')
    try:
        s3ManifestKey = uploadManifesttoS3(s3conn,s3Bucket,mnftFileName)
    except:
        logger.error('Manifest upload to S3 failed! Quitting!!')
        sys.exit(1)
    logger.info('Manifest uploaded to S3!!')
    logger.info('Publishing the manifest file to SQS for copy to Redshift')
    publishRsJob(s3ManifestKey,rsjobqueue,sqs_conn)
    logger.info('Copy job published to SQS')

def publishRsJob(s3ManifestKey,rsjobqueue,sqs_conn):
    if sqs_conn.get_queue(rsjobqueue):
        rsjqueue = sqs_conn.get_queue(rsjobqueue)
    else:
        rsjqueue = sqs_conn.create_queue(rsjobqueue,300)
    msg = Message()
    msg.set_body(s3ManifestKey)
    rsjqueue.write(msg)

def copytoRS(copyjobcode,connString,tableName,columnList,stagingTableName):
    rsConn = psycopg2.connect(connString)
    logger.info('Starting truncate and copy of data to Redshift!')
    try:
        executeCopy(rsConn,'truncate table '+stagingTableName)
        executeCopy(rsConn,copyjobcode)
    except:
        logger.error('Copy Failed!!!')
        sys.exit(1)
    logger.info('Copy succeeded!!') 
    insertFromStaging = "insert into "+tableName+" select "
    for columnTuple in columnList.split(','):
        if columnTuple.split(':')[1] == 'varchar':
            insertFromStaging = insertFromStaging+" substring("+columnTuple.split(':')[0]+",charindex(':',"+columnTuple.split(':')[0]+")+1,length("+columnTuple.split(':')[0]+")-charindex(':',"+columnTuple.split(':')[0]+")),"
        elif columnTuple.split(':')[1] == 'timestamp':
            insertFromStaging = insertFromStaging+" CAST(substring("+columnTuple.split(':')[0]+",charindex(':',"+columnTuple.split(':')[0]+")+1,length("+columnTuple.split(':')[0]+")-charindex(':',"+columnTuple.split(':')[0]+")) AS TIMESTAMP),"
        elif columnTuple.split(':')[1] == 'int':
            insertFromStaging = insertFromStaging+" int(substring("+columnTuple.split(':')[0]+",charindex(':',"+columnTuple.split(':')[0]+")+1,length("+columnTuple.split(':')[0]+")-charindex(':',"+columnTuple.split(':')[0]+"))),"
    insertFromStaging = insertFromStaging[:-1]
    insertFromStaging = insertFromStaging + " from "+stagingTableName
    logger.info('Starting loading of data into main table from Staging!')
    try:
        executeCopy(rsConn,insertFromStaging)
    except:
        logger.info('Load into main table failed!')
        sys.exit(1)
    logger.info('Transformed and written to Redshift main table!!!')

def executeCopy(rsConn,copyjobcode):
    cursor = rsConn.cursor()
    cursor.execute('commit')
    cursor.execute(copyjobcode)
    cursor.execute('commit')
     

def getS3KeyContents(s3_conn,currConfigFile):
    bucketName = str(currConfigFile.split('/')[2])
    keyName = '/'.join(currConfigFile.split('/')[3:])
    bucket = s3_conn.get_bucket(bucketName)
    getFile = Key(bucket)
    getFile.key = keyName
    configValues = getFile.get_contents_as_string()
    configValues = configValues.split('\n')
    configValDict = {}
    for confval in configValues:
        if confval != '':
            configValDict[str(confval.split('=')[0]).strip()] = str(confval.split('=')[1]).strip()
    return configValDict

def getS3Key(myMessage):
    s3key = {}
    msg_json = ast.literal_eval(myMessage[0].get_body())
    try:
        s3key['Key'] = str(msg_json['Records'][0]['s3']['object']['key'])
        s3key['eventTime'] = str(msg_json['Records'][0]['eventTime'])
        s3key['size'] = str(msg_json['Records'][0]['s3']['object']['size'])
        s3key['bucket'] = str(msg_json['Records'][0]['s3']['bucket']['name'])
        s3key['versionId'] = str(msg_json['Records'][0]['s3']['object']['versionId'])
        return s3key
    except KeyError:
        return s3key

def getS3Keys(myMessage):
    msg_json = ast.literal_eval(myMessage.get_body())
    if int(msg_json['Records'][0]['s3']['object']['size']) > 0:
        return str(msg_json['Records'][0]['s3']['object']['key'])
    else:
        return ''

def delete_message(myMessage):
    return myMessage.delete()

def delete_messages(myMessage):
    for msg in myMessage:
        msg.delete()

def processMessage(dynamodb_conn,tablename,s3Key):
    table = Table(tablename)
    try:
        currItem = table.get_item(version_id = 1)
    except boto.dynamodb2.exceptions.ItemNotFound:
        try:
            table.put_item(data = {
                'version_id' : 1,
                'latest_version_id' : 1,
                'current_version_file' : 's3://'+s3Key['bucket']+'/'+s3Key['Key'],
                'current_file_version_id' : s3Key['versionId'],
                'current_file_event_timestamp' : s3Key['eventTime']
                })
            return 0
        except:
            return 1
    except:
        return 1
    currItemTime = datetime.strptime(currItem['current_file_event_timestamp'],'%Y-%m-%dT%H:%M:%S.%fZ')
    newItemTime = datetime.strptime(s3Key['eventTime'],'%Y-%m-%dT%H:%M:%S.%fZ')
    if currItem['current_version_file'] != s3Key['Key'] or currItem['current_file_version_id'] != s3Key['versionId']:
        if currItemTime < newItemTime:
            try:
                table.put_item(data = {
                'version_id' : currItem['latest_version_id']+1,
                'latest_version_id' : currItem['latest_version_id']+1,
                'current_version_file' : currItem['current_version_file'],
                'current_file_version_id' : currItem['current_file_version_id'],
                'current_file_event_timestamp' : currItem['current_file_event_timestamp']
                })
            except boto.dynamodb2.exceptions.ConditionalCheckFailedException:
                logger.warn('Old config file key already backed up.. Continuing!!')
            except:
                logger.error('Error backing up old config key. Quitting!!')
                return 1
            try:
                currItem['latest_version_id'] = currItem['latest_version_id'] + 1
                currItem['current_version_file'] = 's3://'+s3Key['bucket']+'/'+s3Key['Key']
                currItem['current_file_version_id'] = s3Key['versionId']
                currItem['current_file_event_timestamp'] = s3Key['eventTime']
                currItem.save(overwrite=True)
                return 0
            except:
                logger.error('Latest config file key not updated. Exitting script and will retry later!!')
                return 1
        else:
            logger.warn('This config file does not contain newer version of config. Discarding!!')
            return 0
def createNewStreamingJob(dynamodb_conn,configValues):
    table = Table(configValues['ddbTableNameForState'])
    emr_conn = conn_to_emr()
    if str(configValues['vpcSubnetId']) != '':
        dict_subnet = {"Instances.Ec2SubnetId":configValues['vpcSubnetId']}
    else:
        dict_subnet = {}
    try:
        jobid = emr_conn.run_jobflow(name=configValues['jobflowName'],log_uri=configValues['logS3Uri'],steps=[],action_on_failure='CANCEL_AND_WAIT',master_instance_type=configValues['masterInstanceType'], slave_instance_type=configValues['slaveInstanceType'],num_instances=int(configValues['numInstances']),ami_version=configValues['amiVersion'],keep_alive=True,job_flow_role=configValues['jobFlowRole'],service_role=configValues['serviceRole'],ec2_keyname=configValues['ec2KeyName'],api_params = dict_subnet,visible_to_all_users=True)
    except:
        return 2
    emr_conn.set_termination_protection(jobid,True)
    state = check_cluster_running(emr_conn,jobid)
    try:
        jobflowId = table.get_item(jobid = 1)
        jobflowId['jobflowid'] = jobid
        jobflowId['state'] = state
        jobflowId['numinstances'] = configValues['numInstances']
        jobflowId['terminationprotect'] = configValues['terminationProtect']
        jobflowId.save(overwrite=True) 
    except boto.dynamodb2.exceptions.ItemNotFound:
        try:
            table.put_item(data = {
                'jobid' : 1,
                'jobflowid' : jobid,
                'state' : state,
                'numinstances' : configValues['numInstances'],
                'terminationprotect' : configValues['terminationProtect']
                })
            return 0
        except:
            return 1
    except:
        return 1

def check_cluster_running(emr_conn,jobid):
    status = emr_conn.describe_jobflow(jobid)
    while str(status.__dict__['state']) not in ('RUNNING','WAITING'):
        logger.info('Cluster not yet ready for use!! Sleeping to recheck again in a few!!!')
        sleep(20)
        status = emr_conn.describe_jobflow(jobid)
    logger.info('Cluster with job Id '+jobid+' ready for use!!!')
    return str(status.__dict__['state'])

def check_emr_jobflow(jobflowId):
    emr_conn = conn_to_emr()
    status = emr_conn.describe_jobflow(jobflowId)
    return str(status.__dict__['state'])

def increaseInstances(emr_conn,jobflowId,numInstances,dynamodb_conn,tblName):
    table = Table(tblName)
    ig = emr_conn.list_instance_groups(jobflowId)
    if str(ig.__dict__['instancegroups'][0].__dict__['instancegrouptype']) == 'CORE':
        igCoreId = str(ig.__dict__['instancegroups'][0].__dict__['id'])
    else:
        igCoreId = str(ig.__dict__['instancegroups'][1].__dict__['id'])
    emr_conn.modify_instance_groups(igCoreId,numInstances)
    jobflowInfo = table.get_item(jobid = 1)
    jobflowInfo['numinstances'] = numInstances
    jobflowInfo.save(overwrite=True) 

def toggleTermProtect(emr_conn,jobflowId,terminationProtect,dynamodb_conn,tblName):
    table = Table(tblName)
    if terminationProtect.upper() == 'TRUE':
        protection = bool(True)
    elif terminationProtect.upper() == 'FALSE':
        protection = bool(False)
    else:
        logger.error('Termination protection should be either True or False!!!')
        sys.exit(1)
    emr_conn.set_termination_protection(jobflowId,protection)
    jobflowInfo = table.get_item(jobid = 1)
    jobflowInfo['terminationprotect'] = str(terminationProtect)
    jobflowInfo.save(overwrite=True) 

def getKeystoProcess(sqs_conn,sqsQueueName):
    s3Queue = getQueue(sqs_conn,sqsQueueName)
    retryCount = 0
    s3Keys = []
    myMessage = ''
    while retryCount < 3:
        myMessage = get_sqs_messages(s3Queue,10,10)
        if len(myMessage) > 0:
            retryCount = 3
        else:
            retryCount += 1
            sleep(10)
    return myMessage

def createNewStreamingStep(stepName,fileList,outputDirectory,mapperKey,mapperLocation,reducerLocation):
    if reducerLocation != 'aggregate':
        args = ['-files',mapperLocation+','+reducerLocation]
        reducerKey = reducerLocation.split('/')[-1]
    else:
        args = ['-files',mapperLocation]
        reducerKey = reducerLocation
    return StreamingStep(name=stepName,mapper=mapperKey,reducer=reducerKey,input=fileList, output=outputDirectory,step_args=args)

def addSteptoJobFlow(emr_conn,jobflowId,step):
    emr_conn.add_jobflow_steps(jobflowId,[step])

def getFileList(myMessage,inputS3Bucket):
    s3Keys = []
    for msg in myMessage:
        if len(getS3Keys(msg)) > 0:
            s3Keys.append('s3://'+inputS3Bucket+'/'+getS3Keys(msg))
    s3KeyList = ','.join(s3Keys)
    return s3KeyList

def getCurrentStep(emr_conn,jobflowId):
    return emr_conn.list_steps(jobflowId).__dict__['steps'][0].__dict__['id']

def trackStep(sqs_conn,myMessage,emr_conn,jobflowId,stepId):
    stepState = emr_conn.describe_step(jobflowId,stepId).__dict__['status'].__dict__['state']
    while stepState != 'COMPLETED':
        if stepState == 'FAILED':
            logger.error('Step with stepId '+stepId+' failed!!')
            break
        sleep(30)
        stepState = emr_conn.describe_step(jobflowId,stepId).__dict__['status'].__dict__['state']
    if stepState == 'COMPLETED':
        logger.info('Step with stepId '+stepId+' completed!')
        logger.info('Deleting SQS messages!')
        delete_messages(myMessage)

def getOutputDirectory():
    currTime = str(datetime.now())
    return '/'+currTime[:4]+'/'+currTime[5:7]+'/'+currTime[8:10]+'/'+currTime[11:13]+'/'+currTime[14:16]+'/'

def conn_to_cw():
    return CloudWatchConnection(get_region())

def check_ddb_tbl_exists(dynamodb_conn,table_name,create_if_not_exists):
    try:
        dynamodb_conn.describe_table(table_name)
    except boto.exception.DynamoDBResponseError:
        if create_if_not_exists == 'Y':
            logger.warn('DynamoDB Table '+table_name+' does not exist.. Creating!!')
            try:
                create_ddb_table(dynamodb_conn,table_name,'version_id',long,'','',2,1)
                logger.info('DynamoDB Table '+table_name+' created!!')
            except:
                logger.error('DynamoDB table creation failed.. Quitting!!')
                sys.exit(1)
        else:
            logger.error('DynamoDB Table '+table_name+' does not exist.. Quitting!!')
            sys.exit(1)

def getRedshiftClusterState(rs_conn,clusterId):
    try:
        rs_conn.describe_clusters(cluster_identifier=clusterId)['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['ClusterStatus']
        return rs_conn.describe_clusters(cluster_identifier=clusterId)['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['ClusterStatus']
    except boto.redshift.exceptions.ClusterNotFound:
        return 'NOCLUSTER'

def creatersCluster(rs_conn,rsClusterId,nodeType,dbName,dbPort,masterUserName,masterPassword,vpcSecurityGroupIds,clusterSubnetGroupName,subnetId,numberOfNodes,encrypted):
    vpcsubGrpIds = vpcSecurityGroupIds.split(',')
    if encrypted.upper() == 'TRUE':
        encryptionFlag = bool(True)
    else:
        encryptionFlag = bool(False)
    try:
        rs_conn.describe_cluster_subnet_groups(cluster_subnet_group_name=clusterSubnetGroupName)
    except:
        rs_conn.create_cluster_subnet_group(clusterSubnetGroupName,rsClusterId,subnetId)
        logger.info('Sleeping for 10 secs!')
        sleep(10)
    rs_conn.create_cluster(rsClusterId,nodeType,masterUserName,masterPassword,db_name=dbName,vpc_security_group_ids=vpcsubGrpIds,cluster_subnet_group_name=clusterSubnetGroupName,port=dbPort,number_of_nodes=numberOfNodes,publicly_accessible=True,encrypted=encryptionFlag)
    while getRedshiftClusterState(rs_conn,rsClusterId).upper() != 'AVAILABLE':
        logger.info('Cluster creation in progress! Sleeping to check again to make sure cluster is available!!!')
        sleep(30)
    logger.info('Cluster creation complete!!!')
    return rs_conn.describe_clusters(cluster_identifier=rsClusterId)['DescribeClustersResponse']['DescribeClustersResult']['Clusters'][0]['Endpoint']['Address']

def executeSQL(rsConn,tableName,createTableStmt):
    cursor = rsConn.cursor()
    try:
        cursor.execute('commit')
        cursor.execute(createTableStmt)
    except:
        logger.error('Create Table Statement Failed!!!')
        sys.exit(1)
    logger.info('Table with name '+tableName+' was created!!')

def createTableIfNotExists(connString,tableName,stagingTableName,createTableNameStmt,createStagingTableNameStmt):
    rsConn = psycopg2.connect(connString)
    executeSQL(rsConn,tableName,createTableNameStmt)
    executeSQL(rsConn,stagingTableName,createStagingTableNameStmt)

def build_ddb_schema(dynamodb_conn,hashkey,hk_type,rangekey,rk_type):
    return dynamodb_conn.create_schema(hash_key_name = hashkey,hash_key_proto_value = hk_type,range_key_name = rangekey,range_key_proto_value = rk_type)

def create_ddb_table(dynamodb_conn,tablename,hashkey,hk_type,rangekey,rk_type,runits,wunits):
    schema = build_ddb_schema(dynamodb_conn,hashkey,hk_type,rangekey,rk_type)
    try:
        dynamodb_conn.create_table(name = tablename,schema = schema,read_units = runits,write_units = wunits)
    except:
        logger.error('DynamoDB table '+tablename+' could not be created.. Quitting!!')
        sys.exit(1)

def check_sqs_queue_exists(sqs_conn,queue_name):
    qname = sqs_conn.get_queue(queue_name)
    if qname is None:
        logger.error('SQS Queue '+queue_name+' does not exist.. Quitting!!')
        sys.exit(1)

def return_value_from_dynamodb(dynamodb_conn,tablename,hashkey):
    KinesisLimits = dynamodb_conn.get_table(tablename)
    return KinesisLimits.get_item(hash_key=hashkey)

def conn_to_kinesis():
    return boto.kinesis.connect_to_region(get_region())

def calculate_shard_count(records_per_sec,record_size,no_of_consumers,dynamodb_conn):
    shardWriteParams = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','ShardWriteSize')
    ShardWriteSize = shardWriteParams['LimitValue']
    shardWriteUnit = shardWriteParams['LimitUnit']
    shardWriteVelocity = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','ShardWritePerSec')['LimitValue']
    maxNoOfShards = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','MaxNoOfShards')['LimitValue']
    shardCountUsingVelocity = int(math.ceil(int(records_per_sec)/int(shardWriteVelocity)))+1

    writeSizeMB = math.ceil(int(records_per_sec)*int(record_size)/1024)
    if shardWriteUnit.upper() == 'MB':
        shardCountUsingSize = int(math.ceil(writeSizeMB/ShardWriteSize))+1

    shardeReadParams = return_value_from_dynamodb(dynamodb_conn,'KinesisLimits','ShardReadSize')
    ShardReadSize = shardeReadParams['LimitValue']
    readSizeMB = math.ceil(int(records_per_sec)*int(record_size)/2048)
    shardReadCount = int(math.ceil(float(readSizeMB)*float(no_of_consumers))) + 1

    numberOfShardsVelocity = shardCountUsingVelocity
    numberOfShardsSize = shardCountUsingSize
    numerOfShardsReadSize = shardReadCount


    if max(numberOfShardsVelocity,numberOfShardsSize,numerOfShardsReadSize) > int(maxNoOfShards):
        logger.warn('Your current limit on Max number of shards is not enough to process the input!')
        logger.info('Proceeding with Stream creation with the max no of shards='+str(maxNoOfShards)+'!!!')
        return maxNoOfShards
    else:
        return max(numberOfShardsVelocity,numberOfShardsSize,numerOfShardsReadSize)

def get_stream_status(stream_name,kinesis_conn):
    return str(kinesis_conn.describe_stream(stream_name)['StreamDescription']['StreamStatus']).upper()

def check_stream_active(stream_name,kinesis_conn):
    while get_stream_status(stream_name,kinesis_conn) != 'ACTIVE':
        logger.info('Stream not yet Active.. Sleeping!!')
        sleep(5)
    if get_stream_status(stream_name,kinesis_conn) == 'ACTIVE':
        logger.info('Stream '+stream_name+' created and is Active!!')
    else:
        logger.error('Issue with creating the Stream '+stream_name+'. Please retry later!!!')
        sys.exit(1)

def create_stream(stream_name,kinesis_conn,records_per_sec,record_size,no_of_consumers,dynamodb_conn):
    cwc = conn_to_cw()
    shard_count = calculate_shard_count(records_per_sec,record_size,no_of_consumers,dynamodb_conn)
    try:
        logger.info('Creating Stream '+stream_name+' with '+str(shard_count)+' shard(s) now!')
        kinesis_conn.create_stream(stream_name,shard_count)
        cwc.put_metric_data(namespace="Kinesis_Stream",name="streamCreated",value=1,dimensions={'streamName':stream_name})
        logger.info('Submitted Stream Creation.. Will check back again to make sure it is created!')
        sleep(5)
        check_stream_active(stream_name,kinesis_conn)
    except:
        logger.error('Error while trying to create your Stream. Please retry later!!!')
        sys.exit(1)

def validate_stream(stream_name,create_if_not_exists,records_per_sec,record_size,no_of_consumers,kinesis_conn,dynamodb_conn):
    try:
        check_complete = 0
        desc_response_json = kinesis_conn.describe_stream(stream_name)
    except boto.kinesis.exceptions.ResourceNotFoundException:
        if create_if_not_exists.upper() == 'Y':
            logger.info('Stream '+stream_name+' does not exist!!')
            create_stream(stream_name,kinesis_conn,records_per_sec,record_size,no_of_consumers,dynamodb_conn)
            check_complete = 1
        else:
            logger.error('Stream '+stream_name+' not found. Quitting program')
            sys.exit(1)
    if check_complete == 0:
        check_stream_active(stream_name,kinesis_conn)