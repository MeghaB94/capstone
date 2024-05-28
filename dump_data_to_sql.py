import os
import pandas as pd
from sam_app.data_processor.data_cleaner import clean_data
from sam_app.data_processor.db import mysql_engine
from dotenv import load_dotenv

load_dotenv()

directory = "ASCEND Dataset"
keywords = ["signup", "exams", "eval", "survey"]


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
                        excel_data[keyword] = pd.concat(
                            [excel_data[keyword], csv_data], ignore_index=True
                        )
                    except Exception as e:
                        print(f"Error reading {file_path} with : {e}")
    return excel_data


dataframes = read_files_to_separate_dataframes(directory, keywords)

for keyword, df in dataframes.items():
    if type(df) == pd.DataFrame:
        print(f"cleaning data for {keyword}")
        dataframes[keyword] = clean_data(df, keyword)

print("data cleaned and ready to write to database")
for table_name, df in dataframes.items():
    df.to_sql(name=table_name, con=mysql_engine, if_exists="replace")
