import base64
import json
from typing import Dict, Union
import sys
import logging
import pymysql
import boto3
import os
import uuid

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


def table_exists(key):
    query = "SELECT EXISTS( SELECT TABLE_NAME FROM information_schema.TABLE WHERE TABLE_SCHEMA LIKE 'Challenge' AND TABLE_TYPE LIKE 'BASE TABLE' AND TABLE_NAME = %s".format(key);
    try:
        cur = conn.cursor()
        cur.execute(query)
        query_results = cur.fetchall()
        if query_results == '1':
            return True
        else:
            return False
    except Exception as e:
        logger.error("Database connection failed due to {}".format(e))


def create_table(table,value):
    i = 0
    buffer = ""
    buffer.append("create table if not exists "+table)
    for x in value:
        col = "col_" + i
        if type(x) == int:
            buffer.append(col +" int NOT NULL")
        if type(x) == str:
            buffer.append(col +" varchar(64) NOT NULL")
        if type(x) == float:
            buffer.append(col +" float NOT NULL")
        i = i+1
    buffer.append(" primary key UUID")
    return buffer


def insert_row(table, value):
    buffer = ""
    buffer.append("insert into " + table+ " values (")
    for x in value:
        if type(x) == str:
            buffer.append("'"+x+"',")
        else:
            buffer.append(x+',')
    buffer.append(str(uuid.uuid4()+')'))
    return buffer


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
    try:
        machine: str = event["meta"]["type"]  # equipment type (I.e. StackingMachine, FillingMachine)
        payload: Dict[str, Union[str, float, int]] = event["payload"]  # variable schema for each equipment (will stay constant over time)
        stmt = ""
        for key, value in payload:
            if not table_exists(key):
                stmt = create_table(key, value)
            else:
                stmt = insert_row(key, value)
                with conn.cursor() as cur:
                    cur.execute(stmt)
        conn.commit()
    except Exception as e:
        logger.error("query failed due to {}".format(e))

