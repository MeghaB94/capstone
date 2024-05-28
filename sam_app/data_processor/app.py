from io import StringIO
from typing import Optional
from urllib.parse import unquote_plus
import boto3
import pandas as pd
from db import mysql_engine
from data_cleaner import clean_data

s3_resource = boto3.resource("s3")
DATA_TYPE = ["signup", "exams", "eval", "survey"]


def lambda_handler(event, context):
    """Lambda triggered on s3 upload"""
    records = event["Records"]
    for item in records:
        key: str = unquote_plus(item["s3"]["object"]["key"])
        bucket: str = item["s3"]["bucket"]["name"]
        data_type = get_data_type_from_file_name(key)
        print(f"data_type: {data_type}")
        if data_type and key.endswith(".csv"):
            print(f"getting {key} object from {bucket} bucket")
            s3_object = s3_resource.Object(bucket, key)
            file_body = s3_object.get()["Body"]
            encoding = "cp1252" if data_type == "signup" else "utf-8"
            print(f"update encoding {encoding}")
            csv_string = file_body.read().decode(encoding)
            df = pd.read_csv(StringIO(csv_string), encoding=encoding)
            df = clean_data(df=df, type=data_type)
            df.to_sql(name=data_type, con=mysql_engine, if_exists="append")


def get_data_type_from_file_name(file_path: str) -> Optional[str]:
    for data_type in DATA_TYPE:
        if data_type in file_path.lower():
            return data_type
    return None
