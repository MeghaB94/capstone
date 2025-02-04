import os
from pandas import DataFrame, read_csv, concat
from .local_translator import translate_columns, translate_rows
from data_cleaner import clean_data
from db import mysql_engine
from dotenv import load_dotenv

load_dotenv()

translated_directory = "translated_data"
directory = "ASCEND Dataset"
keywords = ["signup", "exams", "eval", "survey"]

ANSI_ENCODING = os.getenv("ANSI_ENCODING")


def read_files_to_separate_dataframes(directory, keywords):
    excel_data = {keyword: DataFrame() for keyword in keywords}
    # Walk through the directory

    for root, dirs, files in os.walk(directory):
        for file in files:
            for keyword in keywords:
                if keyword in file.lower() and file.lower().endswith(".csv"):
                    file_path = os.path.join(root, file)
                    try:
                        csv_data = read_csv(
                            file_path,
                            dtype=str,
                            encoding=(
                                ANSI_ENCODING if "signup" in file.lower() else "utf-8"
                            ),
                        )
                        csv_data.rename(columns=lambda x: x.strip(), inplace=True)
                        csv_data = csv_data.apply(
                            lambda x: x.str.strip() if x.dtype == "object" else x
                        )
                        excel_data[keyword] = concat(
                            [excel_data[keyword], csv_data], ignore_index=True
                        )
                    except Exception as e:
                        print(f"Error reading {file_path} with : {e}")
    return excel_data


def _clean_and_dump_to_db(dataframes: dict):
    for keyword, df in dataframes.items():
        if type(df) == DataFrame:
            print(f"cleaning data for {keyword}")
            dataframes[keyword] = df = clean_data(df, keyword)
            print(f"data cleaned and ready to write to database for {keyword}")
            result = df.to_sql(
                name=keyword, con=mysql_engine, index=False, if_exists="replace"
            )
            print(result)


def save_to_sql():
    dataframes = read_files_to_separate_dataframes(directory, keywords)

    for keyword, df in dataframes.items():
        if type(df) == DataFrame:
            print(f"cleaning data for {keyword}")
            df = df[
                ~df["Username"].str.contains(
                    "test|proton|demo|@ascend", case=False, na=False
                )
            ]
            df = df[
                ~df["Program Version"].str.contains("Facilitator", case=False, na=False)
            ]
            if "DepartmentName" in df:
                df = df[
                    ~df["DepartmentName"].str.contains(
                        "Facilitator", case=False, na=False
                    )
                ]
            df.reset_index(drop=True, inplace=True)
            df = translate_columns(df)
            df = translate_rows(df)
            df.to_csv(f"{translated_directory}/{keyword}.csv", index=False)
            dataframes[keyword] = df
    _clean_and_dump_to_db(dataframes)


def dump_translated_to_sql():
    dataframes = read_files_to_separate_dataframes(translated_directory, keywords)
    _clean_and_dump_to_db(dataframes)
