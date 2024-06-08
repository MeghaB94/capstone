import os
import pandas as pd
from sam_app.data_processor.data_cleaner import clean_data
from sam_app.data_processor.db import mysql_engine
from dotenv import load_dotenv
from sam_app.data_processor.util import get_csv_file_from_s3

load_dotenv()

directory = "ASCEND Dataset"
keywords = ["signup", "exams", "eval", "survey"]

BUCKET = "data-processor-databucket-xsxgjcwmwdxa"
keys = [
    "ASCEND Dataset/ASCEND Data Jan - Mar 2024/IECBC-SDE-User Signup Report20240401070006658Immigrant Employment Council of BC (IECBC)_8a2f7fa4-7920-4802-aafe-334c465610d1.csv",
    "ASCEND Dataset/ASCEND Data Jan - Mar 2024/IECBC-Assessments Report Exams Only -20240401070410195.csv",
    "ASCEND Dataset/ASCEND Data Jan - Mar 2024/IECBC-Assessments Report-Survey20240401070113110Immigrant Employment Council of BC (IECBC)_8a2f7fa4-7920-4802-aafe-334c465610d1.csv",
    "ASCEND Dataset/ASCEND Data Jan - Mar 2024/IECBC-Evaluation-Report-20240401070018872.csv",
]


def read_files_to_separate_dataframes(directory, keywords):
    excel_data = {keyword: pd.DataFrame() for keyword in keywords}
    # Walk through the directory

    for root, dirs, files in os.walk(directory):
        for file in files:
            for keyword in keywords:
                if keyword in file.lower() and file.lower().endswith(".csv"):
                    file_path = os.path.join(root, file)
                    try:
                        csv_data = pd.read_csv(
                            file_path,
                            encoding=(
                                "latin1" if "signup" in file.lower() else "utf-8"
                            ),
                        )
                        csv_data.rename(columns=lambda x: x.strip(), inplace=True)
                        excel_data[keyword] = pd.concat(
                            [excel_data[keyword], csv_data], ignore_index=True
                        )
                    except Exception as e:
                        print(f"Error reading {file_path} with : {e}")
    return excel_data


def read_files_from_s3():
    excel_data = {keyword: pd.DataFrame() for keyword in keywords}
    for key in keys:
        data_type, csv_data = get_csv_file_from_s3(
            bucket=BUCKET,
            key=key,
        )
        if csv_data is not None:
            excel_data[data_type] = pd.concat(
                [excel_data[data_type], csv_data], ignore_index=True
            )
    return excel_data


def save_to_sql():
    # dataframes = read_files_from_s3()
    dataframes = read_files_to_separate_dataframes(directory, keywords)

    for keyword, df in dataframes.items():
        if type(df) == pd.DataFrame:
            print(f"cleaning data for {keyword}")
            dataframes[keyword] = clean_data(df, keyword)

    print("data cleaned and ready to write to database")
    for table_name, df in dataframes.items():
        result = df.to_sql(name=table_name, con=mysql_engine, if_exists="replace")
        print(result)


save_to_sql()
