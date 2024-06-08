from io import StringIO
from typing import Optional
import boto3
import pandas as pd
from db import mysql_engine
from data_cleaner import clean_data
from util import get_csv_file_from_s3

s3_resource = boto3.resource("s3")
DATA_TYPE = ["signup", "exams", "eval", "survey"]


def lambda_handler(event, context):
    """Lambda triggered on s3 upload"""
    records = event["Records"]
    for item in records:
        data_type, df = get_csv_file_from_s3(
            bucket=item["s3"]["bucket"]["name"],
            key=item["s3"]["object"]["key"],
        )
        if df is not None:
            df = clean_data(df=df, type=data_type)
            df.to_sql(name=data_type, con=mysql_engine, if_exists="append")


def get_data_type_from_file_name(file_path: str) -> Optional[str]:
    for data_type in DATA_TYPE:
        if data_type in file_path.lower():
            return data_type
    return None
