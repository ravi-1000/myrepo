import base64
import json
from typing import Dict, Union
import pandas as pd
import sys
import pymysql
import logging
import boto3
import os


# rds settings
ENDPOINT = "mysqldb.123456789012.us-east-1.rds.amazonaws.com"
PORT = "3306"
USER = "jane_doe"
REGION = "us-west-1"
DBNAME = "mydb"
os.environ['LIBMYSQL_ENABLE_CLEARTEXT_PLUGIN'] = '1'

logger = logging.getLogger()
logger.setLevel(logging.INFO)
session = boto3.Session(profile_name='default')
client = session.client('rds')
token = client.generate_db_auth_token(DBHostname=ENDPOINT, Port=PORT, DBUsername=USER, Region=REGION)


# create the database connection outside of the handler to allow connections to be
# re-used by subsequent function invocations.
try:
    conn = pymysql.connect(host=ENDPOINT, user=USER, passwd=token, port=PORT, database=DBNAME,ssl_ca='SSLCERTIFICATE')
except pymysql.MySQLError as e:
    logger.error("ERROR: Unexpected error: Could not connect to MySQL instance.")
    logger.error(e)
    sys.exit()

logger.info("SUCCESS: Connection to RDS MySQL instance succeeded")


def _parse_record(record: dict) -> dict:
    """Parses the event record into the relevant data.

    Parameters
    ----------
    record: dict
        Raw event record.

    Returns
    -------
    Parsed event record as a dictionary.
    """
    base64_message = record["kinesis"]["data"]
    base64_bytes = base64_message.encode("ascii")
    message_bytes = base64.b64decode(base64_bytes)
    message = message_bytes.decode("ascii")
    decoded_payload = json.loads(message)
    return decoded_payload


def lambda_handler(event, context):
    """Lambda event handler. Pulls events from kinesis stream."""
    for record in event["Records"]:
        event_dict = _parse_record(record)
        try:
            main(event_dict)
        except Exception as e:
            pass


def save_error(machine, payload):
    try:
        s3 = boto3.resource('s3')
        object = s3.Object('my_error_bucket', 'my/key/including/filename.txt')
        object.put(Body=machine+" "+payload)
    except Exception as e:
        logging.error(str(e))


def main(event: dict):
    """Main function for event processing. User code for processing goes here.

    Parameters
    ----------
    event: dict
        Event record as a dictionary.

    Returns
    -------
    Does not return anything.
    """
    machine: str = event["meta"]["type"]  # equipment type (I.e. StackingMachine, FillingMachine)
    payload: Dict[str, Union[str, float, int]] = event["payload"]  # variable schema for each equipment (will stay constant over time)
    try:
        df = pd.DataFrame(payload)
        df.to_sql(machine, conn, None, if_exists='append')
    except Exception as e:
        save_error(machine, payload)