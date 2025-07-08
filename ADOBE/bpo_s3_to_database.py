import re
import gzip
from io import BytesIO, TextIOWrapper
from sqlalchemy import create_engine, Column, Integer, String, Text, Date, Time, Boolean, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.engine.url import URL
from datetime import datetime
import pymysql
import boto3
import hashlib
import itertools
import logging
from botocore.exceptions import ClientError
import json

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

config = get_secret('aws_secrets_name')
host = config['hostname']
database = config['databasename']
user = config['user']
password = config['password']

DATABASE_URI = URL.create(
    drivername="mysql+pymysql",
    username=user,
    password=password,
    host=host,
    port=3307,
    database=database
)

# Initialize SQLAlchemy ORM
Base = declarative_base()

class DirectDownloadLog(Base):
    __tablename__ = "direct_download_logs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date, nullable=False)
    time = Column(Time, nullable=False)
    cs_ip = Column(String(16), nullable=False)
    cs_method = Column(String(10), nullable=False)
    cs_uri = Column(Text, nullable=False)
    sc_status = Column(Integer, nullable=False)
    sc_bytes = Column(Integer, nullable=False)
    time_taken = Column(Integer, nullable=False)
    cs_referrer = Column(Text, nullable=False)
    cs_user_agent = Column(Text, nullable=False)
    cs_cookie = Column(Text, nullable=False)
    visitor_id = Column(String(100))
    hash = Column(String(32))
    demandbase_lookup_status = Column(Boolean, default=False)
    demandbase_lookup_error = Column(Boolean, default=False)
    demandbase_lookup_timestamp = Column(DateTime)
    job_name = Column(String(500))  # Store job_name (processed file name)

# Create database connection
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

# Regex patterns
LOG_FILTER_MASK = r"^[^\t]+\t{1}[^\t]+\t{1}[^\t]+\t{1}[^\t]+\t{1}/www.infosysbpm.com/[^\t]+\.(pdf|ppt|doc|xls|xlsx|rtf|csv|docx|pptx)(\?[^\t]+)?\t{1}[^\t]+\t{1}[^\t]+\t{1}[^\t]+\t{1}(?!\"http(s)?://(www.login|login|www).infosysbpm.com.*\"|\"www.infosysbpm.com\")[^\t]+\t{1}[^\t]+\t{1}[^\t]+$"
BOT_IGNORE_MASK = r"(Googlebot|AskJeeves|Digger|Lycos|msnbot|Inktomi Slurp|Nutch|bingbot|BingPreview|Mediapartners-Google|proximic|AhrefsBot|AdsBot-Google|Ezooms|AddThis.com|facebookexternalhit|MetaURI|Feedfetcher-Google|PaperLiBot|TweetmemeBot|Sogou web spider|GoogleProducer|RockmeltEmbedder|ShareThisFetcher|YandexBot|rogerbot-crawler|ShowyouBot|Baiduspider|Sosospider|Exabot|YandexMobileBot|MJ12bot|SimplePie|Yahoo! Slurp|Googlebot-Mobile|AdsBot-Google-Mobile|SiteLockSpider|OkHttp|Curl|ips-agent|Googlebot-Image|BLEXBot|YandexBot|ScoutJet|DotBot|MS Search 6.0 Robot|MS Search 4.0 Robot|MS Search 5.0 Robot|MS Search 2.0 Robot|CCBot|Yeti/1.1|OpenSearchServer_Bot|LinkedInBot|Slackbot|2112DiscoveryBot|TurnitinBot|linkdexbot|SeznamBot|spyglassbot|SemrushBot|Twitterbot|istellabot|ZoomBot|Go-http-client/1\.1|Blackboard Safeassign|Semrush ContentAnalyzer bot)"
VALID_HTTP_STATUS_MASK = r"(200|201|202|203|204|205|206|207|208|209|304)"

# AWS S3 Configuration
S3_BUCKET_NAME = "Bucket_name_of_log_files"
S3_DEST_BUCKET = "Bucket_name_of_error_files_being_stored"
S3_ERROR_FILEPATH = "error_files_folder_name"

# Initialize S3 client
s3_client = boto3.client("s3")
sns_client = boto3.client('sns')

def send_notification(message):
    """Send a notification using SNS."""
    config = get_secret('aws_secrets_name')
    sns_topic_arn = config['sns_arn']
    sns_client.publish(
        TopicArn=sns_topic_arn,
        Message=message
    )



def move_s3_file(source_bucket, source_key, dest_bucket, dest_key):
    try:
        s3 = boto3.client('s3')

        dest_key = dest_key + source_key

        # Copy the object to the destination
        copy_source = {
            'Bucket': source_bucket,
            'Key': source_key
        }

        s3.copy_object(CopySource=copy_source, Bucket=dest_bucket, Key=dest_key)

        # Delete the original object
        s3.delete_object(Bucket=source_bucket, Key=source_key)

        return True

    except Exception as e:
        print(f"Error moving S3 file: {e}")
        return False


def list_s3_gz_files():
    """List all .gz files in the S3 logs folder that match the specific pattern."""
    gz_files = []
    response = s3_client.list_objects_v2(Bucket=S3_BUCKET_NAME)

    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.gz'):
                if re.match(r"^infosysbpo_[0-9]{5}\.[a-zA-Z0-9]{5}_S\.[0-9]{12}-[0-9]{4}-[0-9]{1}\.gz$", obj['Key']):
                    gz_files.append(obj['Key'])
                else:
                    logger.error(f"File naming convention not in the right format:{obj['Key']}.")
                    move_s3_file(S3_BUCKET_NAME, obj['Key'], S3_DEST_BUCKET,S3_ERROR_FILEPATH)
            else:
                logger.error(f"File type not in the right Gzip format:{obj['Key']}.")
                move_s3_file(S3_BUCKET_NAME, obj['Key'], S3_DEST_BUCKET,S3_ERROR_FILEPATH)


    return gz_files

def stream_s3_gz_file(s3_key):
    """Stream a gzipped log file from S3 without downloading it to disk."""
    try:
        response = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        return gzip.GzipFile(fileobj=BytesIO(response['Body'].read()))
    except Exception as e:
        logger.error(f"Error streaming log file {s3_key}: {e}")
        return None


def process_gz_log_file(log_stream, job_name):
    """Process a compressed log file in-memory and extract valid records."""
    valid_records = []
    try:
        with TextIOWrapper(log_stream, encoding="utf-8", errors="ignore") as file:
            for line in file:
                fields = line.strip().split("\t")
                if len(fields) < 11:
                    continue
                if re.match(LOG_FILTER_MASK, line) and not re.search(BOT_IGNORE_MASK, fields[9]) and re.search(VALID_HTTP_STATUS_MASK, fields[5]):
                    if not re.search(r"\.aspx\?", fields[4]):
                        valid_records.append(fields)
    except Exception as e:
        logger.error(f"Error processing log file {job_name}: {e}")
        move_s3_file(S3_BUCKET_NAME, job_name, S3_DEST_BUCKET,S3_ERROR_FILEPATH)
    return valid_records

def calculate_hash(record):
    """Calculate a unique hash for a record."""
    record_str = ''.join(map(str, record))
    return hashlib.md5(record_str.encode()).hexdigest()

def extract_svid(cookie_string):
    """Extract svid from cookie string."""
    svid_match = re.search(r"svid=([^;]+)", cookie_string)
    if svid_match:
        return svid_match.group(1)
    return None


def delete_s3_file(s3_key):
    """Delete the processed file from S3."""
    try:
        s3_client.delete_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
        logger.info(f"Deleted {s3_key} from S3 bucket.")
    except Exception as e:
        logger.error(f"Error deleting file {s3_key} from S3: {e}")


def insert_into_db(records, job_name):
    """Insert valid, unique records into the database."""
    session = Session()
    try:
        # Sort the records to ensure 'groupby' works correctly
        records.sort()

        # Remove duplicate records using itertools.groupby
        unique_records = [key for key, _ in itertools.groupby(records)]

        data_to_insert = []
        for record in unique_records:
            try:
                visitor_id = extract_svid(record[10])
                record_hash = calculate_hash(record)
                data_to_insert.append(DirectDownloadLog(
                    date=datetime.strptime(record[0], "%Y-%m-%d").date(),
                    time=datetime.strptime(record[1], "%H:%M:%S").time(),
                    cs_ip=record[2],
                    cs_method=record[3],
                    cs_uri=record[4],
                    sc_status=int(record[5]),
                    sc_bytes=int(record[6]),
                    time_taken=int(record[7]),
                    cs_referrer=record[8],
                    cs_user_agent=record[9],
                    cs_cookie=record[10],
                    visitor_id=visitor_id,
                    hash=record_hash,
                    job_name=job_name  # Store processed file name
                ))
            except ValueError:
                continue  # Skip records with incorrect formatting

        if data_to_insert:
            session.bulk_save_objects(data_to_insert)
            session.commit()
            logger.info(f"Inserted {len(data_to_insert)} unique records into the database for job {job_name}.")
            print(f"Inserted {len(data_to_insert)} unique records into the database for job {job_name}.")
            #delete_s3_file(job_name)

        else:
            logger.info(f"No valid records to insert for {job_name}.")
    except Exception as e:
        session.rollback()
        logger.error(f"Error inserting records: {e}")
    finally:
        session.close()


def lambda_handler(event, context):
    # Fetch gz files from S3 and process them
    s3_gz_files = list_s3_gz_files()

    if not s3_gz_files:
        logger.info("No compressed log files found in the S3 logs folder.")
        #send_notification("No BPO log files in S3 bucket.")
        print("No BPO log files in S3 bucket.")
    else:
        for s3_file in s3_gz_files:
            logger.info(f"Processing S3 log file: {s3_file}")

            log_stream = stream_s3_gz_file(s3_file)
            if log_stream:
                valid_records = process_gz_log_file(log_stream, job_name=s3_file)
                if valid_records:
                    insert_into_db(valid_records, job_name=s3_file)
                else:
                    logger.info(f"No valid records found in {s3_file}.")
            else:
                logger.info(f"Skipping {s3_file} due to streaming error.")

        logger.info("Processing complete for all S3 log files.")
        #send_notification("Processing complete for all S3 BPO log files.")
        print("Processing complete for all S3 BPO log files.")
