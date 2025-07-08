from sqlalchemy import create_engine, Column, Integer, String, Boolean, text
from sqlalchemy.orm import sessionmaker, declarative_base
import urllib3
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import json
from datetime import datetime , timedelta
from sqlalchemy.engine.url import URL
import boto3
import logging
import numpy as np

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)  # Or another level like logging.DEBUG



def get_secret(secret_name):
    """Fetch secret from AWS Secrets Manager."""
    session = boto3.session.Session()
    client = session.client(service_name='secretsmanager')
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except ClientError as e:
        logger.error(f"Error retrieving secret: {e}")
        raise

# Retrieve database credentials from AWS Secrets Manager
config = get_secret('aws_secrets_name')
host = config['hostname']
database = config['databasename']
user = config['database_user']
password = config['database_password']

sns_client = boto3.client('sns')

def send_notification(message):
    """Send a notification using SNS."""
    config = get_secret('aws_secrets_name')
    sns_topic_arn = config['sns_arn']
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )


# Define the database connection URI
DATABASE_URI = URL.create(
    drivername="mysql+pymysql",
    username=user,
    password=password,
    host=host,
    port=3307,
    database=database
)

# Create database engine and session factory
engine = create_engine(DATABASE_URI)
SessionLocal = sessionmaker(bind=engine)

# API Details
DEMANDBASE_API_URL = config["DEMANDBASE_API_URL"]
DEMANDBASE_API_KEY = config["DEMANDBASE_API_KEY"]
THREAD_COUNT = 2

# Define ORM model for direct_download_logs table
Base = declarative_base()

class DirectDownloadLog(Base):
    """ORM model representing the direct_download_logs table."""
    __tablename__ = "direct_download_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    cs_ip = Column(String(45))
    company_name = Column(String(255), default="Unknown")
    industry = Column(String(255), default="Unknown")
    revenue_range = Column(String(255), default="Unknown")
    employee_range = Column(String(255), default="Unknown")
    demandbase_lookup_status = Column(Boolean, default=False)

def fetch_records():
    """
    Fetch records from the database where demandbase_lookup_status is 0.
    Limits the query to 8000 records.
    """
    session = SessionLocal()
    query = text("SELECT id, cs_ip FROM direct_download_logs WHERE id >= 59556553 and demandbase_lookup_status = 0 LIMIT 1 ;")
    records = session.execute(query).fetchall()
    session.close()
    logger.info(f"Fetched {len(records)} records from the database.")
    return records

def update_failed_record(record_id, error_message):
    """
    Update a record in the database to indicate a failed lookup attempt.
    
    Parameters:
    - record_id (int): ID of the record to update.
    - error_message (str): Error message describing the failure.
    """
    session = SessionLocal()
    update_query = text("""
        UPDATE direct_download_logs 
        SET demandbase_lookup_status = 0,
            demandbase_lookup_error = :error_message
        WHERE id = :id
    """)
    session.execute(update_query, {"id": record_id, "error_message": error_message})
    session.commit()
    session.close()
    logger.error(f"Failed to process record {record_id}: {error_message}")

def replace_none_with_null_string(data):
    if isinstance(data, dict):
        return {k: replace_none_with_null_string(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [replace_none_with_null_string(item) for item in data]
    elif data is None:
        return "null"
    return data


def fetch_demandbase_data(record_id, cs_ip):
    """
    Fetch company details from Demandbase API using an IP address.
    Updates the database with the retrieved company information.
    
    Parameters:
    - record_id (int): ID of the record being processed.
    - cs_ip (str): IP address to lookup in Demandbase API.
    """
    http = urllib3.PoolManager()
    try:
        url = f"{DEMANDBASE_API_URL}?key={DEMANDBASE_API_KEY}&query={cs_ip}"
        response = http.request('GET', url)

        if response.status != 200:
            update_failed_record(record_id, f"HTTP {response.status} - {response.data.decode('utf-8')[:255]}")
            return None

        data = json.loads(response.data.decode('utf-8'))
        data = replace_none_with_null_string(data)
        lookup_timestamp = datetime.utcnow() + timedelta(hours=5,minutes=30)

        def segment_filter(value,default = "Not In List"):
            return default if value in ["null",None] else str(value)
        def audience_filter(value,default = "ISP Visitor"):
            return default if value in ["null",None,""] else str(value)
        #Company Name
        company_id = "ISP Visitor"; company_name = "ISP Visitor"; industry = "ISP Visitor"; sub_industry = "ISP Visitor"; employee_range = "ISP Visitor"; revenue_range = "ISP Visitor"; audience = "ISP Visitor"; audience_segment = "ISP Visitor"

        #company Segment
        marketing_alias = "Not In List"; city = "Not In List"; state = "Not In List"; country_name = "Not In List"; fortune_1000 = "Not In List"; placeholder_1 = str(np.nan) ; placeholder_2 = str(np.nan) ; isp_service = "Not In List"

        # Extract data with default values
        information_level = str(data.get("information_level", "basic").lower())
        if information_level == "detailed":
            company_id = str(data.get("company_id", "ISP Visitor"))
            company_name = str(data.get("company_name", "ISP Visitor"))
            industry = str(data.get("industry", "ISP Visitor"))
            sub_industry = str(data.get("sub_industry", "ISP Visitor"))
            employee_range = str(data.get("employee_range", "ISP Visitor"))
            revenue_range = str(data.get("revenue_range", "ISP Visitor"))
            audience = str(data.get("audience", "ISP Visitor"))
            audience_segment = str(data.get("audience_segment", "ISP Visitor"))

            # Additional extracted details
            marketing_alias = segment_filter(str(data.get("marketing_alias", "Not In List")))
            city = segment_filter(str(data.get("city", "Not In List")))
            state = segment_filter(str(data.get("state", "Not In List")))
            country_name = segment_filter(str(data.get("country_name", "Not In List")))
            fortune_1000 = segment_filter(str(data.get("fortune_1000", "Not In List")))
            
        # Handle information level
        is_isp = False # Initialize is_isp
        if information_level == "basic":
            isp_service = "ISP Visitor"
            audience = str(data.get("audience","ISP Visitor"))
            if str(data.get("audience_segment")) !="null" and str(data.get("audience_segment")) != "" and str(data.get("audience_segment")) != None:
                audience_segment = audience_filter(str(data.get("audience_segment","ISP Visitor")))
            is_isp = True
            
        #demandbase_sid
        fields = [company_id, company_name, industry, sub_industry, employee_range, revenue_range, audience, audience_segment]
        company_name_info = ":".join(fields)
        company_segment_info = ":".join([marketing_alias, city, state, country_name, fortune_1000, placeholder_1, placeholder_2])
        company_segment_info += f":{isp_service}"
        demandbase_data_dump = json.dumps(data)


        # Update database with Demandbase data
        session = SessionLocal()
        update_query = text("""
            UPDATE direct_download_logs
            SET company_name_info = :company_name_info,
                company_segment_info = :company_segment_info,
                isp = :is_isp,
                demandbase_data_dump = :demandbase_data_dump,
                demandbase_lookup_timestamp = :lookup_timestamp,
                demandbase_lookup_status = 1
            WHERE id = :id
        """)
        session.execute(update_query, {
            "id": record_id,
            "company_name_info": company_name_info.replace("'", ""),
            "company_segment_info": company_segment_info.replace("'", ""),
            "is_isp": is_isp,
            "demandbase_data_dump": demandbase_data_dump.replace("'", ""),
            "lookup_timestamp": lookup_timestamp
        })
        session.commit()
        session.close()
        logger.info(f"Successfully processed record {record_id}")

    except Exception as e:
        update_failed_record(record_id, str(e)[:255])

def process_all_records(records):
    """
    Process all fetched records concurrently using multiple threads.
    
    Parameters:
    - records (list): List of tuples containing record_id and cs_ip.
    """
    if not records:
        logger.info("No records found in BPO database for demandbase enrichment.")
        send_notification("No records found in BPO database for demandbase enrichment.")
        return

    logger.info(f"Processing {len(records)} records concurrently with {THREAD_COUNT} threads...")

    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        future_to_record = {executor.submit(fetch_demandbase_data, record_id, cs_ip): record_id for record_id, cs_ip in records}

        for future in as_completed(future_to_record):
            record_id = future_to_record[future]
            try:
                future.result()
            except Exception as exc:
                logger.error(f"Record {record_id} generated an exception: {exc}")

        send_notification("Processing complete with BPO demandbase information .")



def lambda_handler(event, context):
    try:
        records = fetch_records()
        process_all_records(records)
    except Exception as e:
        logger.error(f"Lambda function encountered an error: {str(e)}", exc_info=True)
        send_notification("Error updating database with BPO demandbase data ..")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Internal Server Error", "message": str(e)})
        }

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Processing completed successfully."})
    }
