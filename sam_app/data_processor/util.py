from io import StringIO
from typing import Optional, Tuple
from urllib.parse import unquote_plus
import boto3
import pandas as pd


from config import ANSI_ENCODING

s3_resource = boto3.resource("s3")
DATA_TYPE = ["signup", "exams", "eval", "survey"]


def get_data_type_from_file_name(file_path: str) -> Optional[str]:
    for data_type in DATA_TYPE:
        if data_type in file_path.lower():
            return data_type
    return None


def get_csv_file_from_s3(
    bucket: str, key: str
) -> Tuple[Optional[str], Optional[pd.DataFrame]]:
    key = unquote_plus(key)
    data_type = get_data_type_from_file_name(key)
    print(f"data_type: {data_type}")
    if data_type and key.endswith(".csv"):
        print(f"getting {key} object from {bucket} bucket")
        s3_object = s3_resource.Object(bucket, key)
        file_body = s3_object.get()["Body"]
        encoding = ANSI_ENCODING if data_type == "signup" else "utf-8"
        print(f"update encoding {encoding}")
        string_content = file_body.read().decode(encoding)
        df = pd.read_csv(StringIO(string_content), encoding=encoding)
        # remove trailing spaces from columns
        df.rename(columns=lambda x: x.strip(), inplace=True)
        return data_type, df
    return None, None
