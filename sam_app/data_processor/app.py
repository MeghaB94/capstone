from db import save_df_to_sql
from data_cleaner import clean_data
from util import get_csv_file_from_s3


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
            save_df_to_sql(data_type, df)
