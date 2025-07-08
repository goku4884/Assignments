import pandas as pd
from urllib.parse import quote
import pymysql
import requests
from time import time
import boto3
from botocore.exceptions import ClientError
import json
from io import BytesIO 
import gzip
import logging

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  

sns_client = boto3.client('sns')

"""Sends out notification through AWS SNS."""
def send_notification(message):
    config = get_secret('prod_bpo_adobe_secrets')
    sns_topic_arn = config['sns_arn']
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )

"""Fetch secret from AWS Secrets Manager."""
def get_secret(secret_name):
    EXPECTED_SECRET_NAME = 'prod_bpo_adobe_secrets'

    if secret_name != EXPECTED_SECRET_NAME:
        logger.error(f"Invalid secret name: '{secret_name}'. Expected: '{EXPECTED_SECRET_NAME}'")
        raise ValueError(f"Secret name must be exactly '{EXPECTED_SECRET_NAME}', but got '{secret_name}'")

    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        logger.info(f"Secret '{secret_name}' fetched successfully.")
        return secret
    except ClientError as e:
        logger.error(f"Error retrieving secret '{secret_name}': {e}")
        raise

config = get_secret('prod_bpo_adobe_secrets')

#TOKEN
issuer= config['issuer']
sub= config['sub']
audience= config['audience']
metascopes = config['metascopes']
private_key_file_name = config['private_key_file_name']

#CLIENT_SECRETS
client_id = config['client_id']
client_secret = config['client_secret']
x_api_key= config['x_api_key']

#ENCRYPTION
algocode= config['algocode']

#FILE
job_name = config['job_name']

#URLS
URL_FOR_BULK_API_CALL= config['URL_FOR_BULK_API_CALL']
URL_FOR_GETTING_ACCESS_TOKEN = config['URL_FOR_GETTING_ACCESS_TOKEN']
URL_TO_VALIDATE_FILE= config['URL_TO_VALIDATE_FILE']
validate_request = config['validate_request']
upload_to_bulk_api = config['upload_to_bulk_api']

#DATABASE
config = get_secret('aws_secrets_name')
hostname = config['hostname']
database = config['databasename']
user = config['user']
password = config['password']

s3_bucket_name = "Bucket_name_of_error_files_being_stored"
s3_client = boto3.client("s3")
s3_error_filepath = "failed_adobe_uploads_folder"

data_extract_query = r"SELECT id, DATE_FORMAT(NOW(),'%m/%d/%Y') as Date, CONCAT(DATE,'T',TIME,'+05:30') as timestamp,`cs_uri`as pageName,REPLACE(SUBSTRING_INDEX(SUBSTRING_INDEX(cs_referrer, 'referrer=', -1),'&',1), '\"', '') as referrer,SUBSTRING_INDEX(cs_uri, '/', -1) as prop16,SUBSTRING_INDEX(cs_uri, '/', -1) as eVar26,`company_name_info` as eVar21,`company_segment_info` as eVar22,'event24' as events,cs_uri as eVar7,cs_uri as prop7,cs_ip as ipAddress,`time_taken` as timetaken FROM `direct_download_logs` WHERE `adobe_cloud_upload_status` = 0 AND `demandbase_lookup_status` = 1 AND `demandbase_lookup_error`=0 AND `adobe_lock_for_upload` = 0 AND `visitor_id` IS NULL and date>='2024-07-30' ORDER BY ID LIMIT 1;"
lock_for_adobe_udpate_query = "UPDATE `direct_download_logs` SET `adobe_lock_for_upload` = 1 WHERE id IN "
unlock_rows_due_to_error = "UPDATE `direct_download_logs` SET `adobe_lock_for_upload` = 0 WHERE id IN "

try:
    def get_auth_token():
        url = URL_FOR_GETTING_ACCESS_TOKEN
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials',
            'scope': 'openid,AdobeID,additional_info.projectedProductContext'
        }
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(url, headers=headers, data=payload).json()
        return response.get('access_token', '')

    """Establishes a connection to the MySQL database.Uses predefined credentials and returns a connection object."""
    def database_connect():
        return pymysql.connect(host=hostname, user=user, password=password, db=database, port=3307, charset='utf8', autocommit=True)

    """Executes the provided SQL query and retrieves data from the database.Returns the result as a Pandas DataFrame."""
    def read_from_database(query):
        logger.info('Entered read_from_database')
        db_connection = database_connect()
        df = pd.read_sql(query, con=db_connection)
        logger.info('Exited read_from_database')
        return df

    """Locks the specified records in the database for Adobe upload.Updates the database using the given query and a list of IDs from the dataframe.Returns the formatted list of locked IDs or 0 if there are no IDs to lock."""
    def adobe_lock_for_upload_in_db(query, dataframe):
        logger.info('Entered adobe_lock_for_upload function')
        db_connection = database_connect()
        cursor_object = db_connection.cursor()
        df_id_list = dataframe["id"].values.tolist()
        if df_id_list:
            updateStatement = query + '(' + ','.join(map(str, df_id_list)) + ')'
            cursor_object.execute(updateStatement)
            cursor_object.close()
            logger.info('Exited adobe_lock_for_upload function')
            return '(' + ','.join(map(str, df_id_list)) + ')'
        return 0

    """Updates the 'job_name' column in the database for each record in the given dataframe.Appends the given file name for tracking purposes."""
    def update_job_name_in_db(dataframe, file_name):
        db_connection = database_connect()
        cursor_object = db_connection.cursor()
        id_list = dataframe.index.tolist() 
        for index in id_list:
            try:
                query = f"UPDATE direct_download_logs SET job_name = CONCAT(job_name, ', {file_name}') WHERE id = {index};"
                cursor_object.execute(query)
            except Exception as e:
                logger.error(f"Error updating job_name for id {index}: {e}")
        cursor_object.close()
        db_connection.close()

    """Converts a Pandas DataFrame into a compressed GZIP CSV file.Uploads the compressed file to an S3 bucket and returns the S3 key and the generated file name."""
    def convert_dataframe_to_gzip(dataframe):
        logger.info('Entered convert_dataframe_to_gzip')
        # Ensure reportSuiteID is correct
        expected_rsid = 'infosys-bpo'
        dataframe['reportSuiteID'] = expected_rsid
        if not all(dataframe['reportSuiteID'] == expected_rsid):
            logger.error("Mismatch in reportSuiteID values")
            raise ValueError("Mismatch in reportSuiteID values")
    
        milliseconds = int(time() * 1000)
        file_name = job_name + str(milliseconds) + ".csv.gz"
        s3_key = f"Infy-BPO/valid_adobe_gzip_files/{file_name}"
        
        dataframe.drop('id', inplace=True, axis=1)
        dataframe['userAgent'] = 'NA'
    
        gzipped_buffer = BytesIO()
        with gzip.GzipFile(fileobj=gzipped_buffer, mode='wb') as gz_file:
            dataframe.to_csv(gz_file, index=False, encoding='utf-8')
    
        gzipped_buffer.seek(0)
        s3_client.put_object(Bucket=s3_bucket_name, Key=s3_key, Body=gzipped_buffer.getvalue())
        update_job_name_in_db(dataframe, file_name)
        logger.info(f'Exited convert_dataframe_to_gzip, s3_key: {s3_key}')
        return s3_key, file_name

    """Downloads the specified file from S3 and validates it using an external API.Returns 1 if successful, otherwise returns 0."""
    def validate_file(s3_key, access_token):
        logger.info('Entered validate_file function')
        headers = {
            'Authorization': f'Bearer {access_token}',
            'x-api-key': x_api_key}
        try:
            logger.info("Downloading file from S3...")
            s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
            file_content = s3_object['Body'].read()
            files = {"file": ("file.gz", BytesIO(file_content), "application/gzip")}
            response = requests.post(URL_TO_VALIDATE_FILE, files=files, headers=headers).json()
            logger.info(f"Response: {response}")
            logger.info('Exited validate_file function')
            return 1 if 'success' in response.keys() else 0
        except Exception as e:
            logger.error(f"Error in validate_file: {e}")
            return 0

    """Uploads the validated file from S3 to the bulk API.Returns 1 if successfully uploaded, otherwise returns 0."""
    def bulk_api_upload(s3_key, file_name, access_token):
        logger.info('Entered bulk_api_upload')
        headers = {'Authorization': 'Bearer ' + access_token, 'x-adobe-vgid': str(file_name), 'x-api-key': x_api_key}
        try:
            logger.info("Downloading file from S3...")
            s3_object = s3_client.get_object(Bucket=s3_bucket_name, Key=s3_key)
            file_content = s3_object['Body'].read()
            files = {"file": (f"{file_name}", BytesIO(file_content), "application/gzip")}
            response = requests.post(URL_FOR_BULK_API_CALL, files=files, headers=headers).json()
            logger.info(f"Response: {response}")
            logger.info(f"Response status: {response.get('status_code')}")
            logger.info('Exited bulk_api_upload')
            return 1 if response.get('status_code') == 'UPLOADED' else 0
        except Exception as e:
            logger.error(f"Error in bulk_api_upload: {e}")
            return 0
        
    """Executes the given SQL query to update the Adobe upload status in the database."""
    def update_adobeupload_status_in_db(query):
        db_connection = database_connect()
        cursor_object = db_connection.cursor()
        cursor_object.execute(query)
        cursor_object.close()

    def move_s3_file(source_bucket, source_key, dest_bucket, dest_key):
        try:
            s3 = boto3.client('s3')
            dest_key = dest_key + source_key

            # Copy the object to the destination

            copy_source = {'Bucket': source_bucket,'Key': source_key}

            s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)

            # Delete the original object
            s3.delete_object(Bucket=source_bucket, Key=source_key)

            return True
        except Exception as e:

            print(f"Error moving S3 file: {e}")
            return False

    def delete_s3_file(S3_file):
        """Delete the processed file from S3."""
        try:
            s3_client.delete_object(Bucket=s3_bucket_name, Key=S3_file)
            logger.info(f"Deleted {S3_file} from S3 bucket.")
        except Exception as e:
            logger.error(f"Error deleting file {S3_file} from S3: {e}")

    def lambda_handler(event,context):
        access_token = get_auth_token()
        #print(access_token)
        dataframe = read_from_database(data_extract_query)
        #print(dataframe)
        global df_id_str_list
        global s3_key
        global file_name
        logger.info(f"Dataframe: {dataframe}")
        df_id_str_list = adobe_lock_for_upload_in_db(lock_for_adobe_udpate_query, dataframe)
        if df_id_str_list != 0:
            s3_key, file_name = convert_dataframe_to_gzip(dataframe)
            access_token = get_auth_token()
            file_valid_status = validate_file(s3_key, access_token)
            logger.info(f"File Valid Status: {file_valid_status}")
            if file_valid_status == 1:
                logger.info('Valid file')
                print('Valid file')
                #I have commented this part as this part is to upload files into the adobe dashboard .
                #bulk_api_upload_status = bulk_api_upload(s3_key, file_name, access_token)
                #if bulk_api_upload_status == 1:
                    #print('status 1')
                    #update_adobeupload_status_in_db(f"UPDATE `direct_download_logs` SET `adobe_cloud_upload_status` = 1, `adobe_cloud_upload_timestamp`=NOW(), `job_name` = '{file_name}' WHERE id IN {df_id_str_list}")
                    #delete_s3_file(s3_key)
                    #send_notification("Process Completed for BPO Adobe Upload suuccessfully")
                    #print("Process Completed for BPO Adobe Upload")
                #else:
                    #update_adobeupload_status_in_db(unlock_rows_due_to_error + df_id_str_list)
                    #end_notification("Process incomplete for BPO Adobe Upload")
                    #print("Process incomplete for BPO Adobe Upload")
            else:
                update_adobeupload_status_in_db(unlock_rows_due_to_error + df_id_str_list)
                move_s3_file(s3_bucket_name, s3_key, s3_bucket_name,s3_error_filepath)
                logger.info("File not valid for BPO Adobe Upload")
                send_notification("File not valid for BPO Adobe Upload")
        else:
            logger.info("No records to upload to adobe.")
        logger.info("Process Completed!")

except Exception as e:
    logger.error(f"An error occurred in lambda_handler: {e}")
    logger.error(f"df_id_str_list: {df_id_str_list}")
    if df_id_str_list != 0:
        update_adobeupload_status_in_db(unlock_rows_due_to_error+df_id_str_list)
    logging.exception("Something awful happened! "+str(e))
    send_notification("Something awful happened! "+str(e))
