from functools import lru_cache
from pandas import DataFrame
import numpy as np
import pandas as pd
import re

from dateutil import parser


def clean_data(df: DataFrame, type: str):
    df = fill_nas(df)
    if type == "signup":
        result_df = clean_signup(df)
    elif type == "exams":
        result_df = clean_exams(df)
    elif type == "eval":
        result_df = clean_eval(df)
    elif type == "survey":
        result_df = clean_survey(df)
    return result_df


def fill_nas(df: DataFrame):
    for colum in df.columns.to_list():
        if colum == "CompletionDate":
            df[colum].fillna("0", inplace=True)
        elif colum == "Eval Response":
            df[colum].fillna("NA", inplace=True)
            df[colum] = df[colum].replace(" ", np.nan).fillna("NA").replace(" ", "NA")
        elif colum == "UserAnswer":
            df[colum].fillna("NA", inplace=True)
        else:
            df[colum].fillna("Prefer not to disclose", inplace=True)
    return df


# Function to extract the year from various date formats
def extract_year(date_str):
    if pd.isna(date_str):
        return None  # Handle NaN values
    elif str(date_str).strip() == "0":  # Explicitly check for '0' as a string
        return "0"  # Return '0' as a string to keep the format consistent

    try:
        # Convert to string to handle any non-string types
        date_str = str(date_str)
        # Mapping for French month names to English
        french_to_english = {
            "Janvier": "January",
            "Février": "February",
            "Mars": "March",
            "Avril": "April",
            "Mai": "May",
            "Juin": "June",
            "Juillet": "July",
            "Août": "August",
            "Septembre": "September",
            "Octobre": "October",
            "Novembre": "November",
            "Décembre": "December",
        }
        # Replace French month names if present
        for fr, en in french_to_english.items():
            date_str = date_str.replace(fr, en)
        # Parse the date and extract the year
        return str(parser.parse(date_str).year)
    except ValueError:
        return None  # Returns None if the date string is invalid


def looks_like_date(value):
    date_patterns = [
        r"\d{1,2}-[A-Za-z]{3}",  # e.g., 12-Sep
        r"[A-Za-z]{3} \d{1,2} \d{4}",  # e.g., OCT 11 1973
        r"[A-Za-z]{4,9}-\d{2}-\d{4}",  # e.g., june-03-1991
        r"\d{2}/\d{2}/\d{4}",  # e.g., 24/12/1994
        r"\d{1,2}/\d{1,2}/\d{4}",  # e.g., 1/1/1993
        r"\d{4}/[A-Za-z]{4,9} \d{1,2}",  # e.g., 1967/september 24
        r"\d{1,2}/\d{1,2}/\d{4}",  # e.g., 9/9/1990
        r"[A-Za-z]{3,9}-\d{1,2}-\d{4}",  # e.g., june-03-1991
    ]
    return any(re.match(pattern, str(value)) for pattern in date_patterns)


def clean_signup(df: DataFrame):
    # Apply the function to identify date-like entries
    is_date = df["Country of birth"].apply(looks_like_date)

    # Replace date-like entries with the corresponding values from "Country you have_will immigrate from"
    df.loc[is_date, "Country of birth"] = df.loc[
        is_date, "Country you have_will immigrate from"
    ]

    # Update the 'Country of birth' and 'Country you have_will immigrate from' columns
    df.loc[
        df["Username"] == "fessehad27@gmail.com",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "Euthiopia"
    df.loc[
        df["Username"] == "hong008sec@hotmail.com",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "China"
    df.loc[
        df["Username"] == "chudasama.h@northeastern.edu",
        ["Country you have_will immigrate from"],
    ] = "Canada"
    df.loc[
        df["Username"] == "howladerony@gmail.com",
        ["Country you have_will immigrate from"],
    ] = "Bangladesh"
    df.loc[
        df["Username"] == "liujun02122023@163.com",
        ["Country you have_will immigrate from"],
    ] = "China"
    df.loc[
        df["Username"] == "danitacarrasco@hotmail.com",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "Chile"
    df.loc[
        df["Username"] == "feng.xiaom@northeastern.edu",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "China"
    df.loc[
        df["Username"] == "Rajindersingh090990@gmail.com",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "India"
    df.loc[
        df["Username"] == "howladerony@gmail.com",
        ["Country of birth", "Country you have_will immigrate from"],
    ] = "Bangladesh"

    #     usernames_to_display = [
    #     'fessehad27@gmail.com',
    #     'hong008sec@hotmail.com',
    #     'chudasama.h@northeastern.edu',
    #     'howladerony@gmail.com',
    #     'liujun02122023@163.com'
    # ]

    # Filter the DataFrame to show only the specified usernames
    # filtered_df = df[df['Username'].isin(usernames_to_display)]

    usernames_to_update = [
        "feng.chenc@northeastern.edu",
        "pink_gricel95@hotmail.com",
        "wu.hao11@northeastern.edu",
        "zeng.zhix@northeastern.edu",
        "obaidamourad357@gmail.com",
    ]

    # Update the 'Country you have_will immigrate from' to match 'Country of birth' for the specified usernames
    df.loc[
        df["Username"].isin(usernames_to_update), "Country you have_will immigrate from"
    ] = df.loc[df["Username"].isin(usernames_to_update), "Country of birth"]

    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("Brasil", "Brazil")

    # Update 'Country of birth' column from 'Brasil' to 'Brazil'
    df["Country of birth"] = df["Country of birth"].replace("Brasil", "Brazil")

    # List of variations to be replaced with 'China'
    variations_to_replace = [
        "china",
        "CN",
        "China, PeopleÂ’s Republic of",
        "Chinia",
        "Hong Kong",
        "Hong Kong, China",
        "China, PeopleÂ’s Republic of ",
        "China, PeopleÂ’s Republic of",
    ]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(variations_to_replace, "China")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        variations_to_replace, "China"
    )

    # Correct the replacement of 'PerÃº' to 'Peru' considering special characters
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].str.replace("China, PeopleÂ’s Republic of ", "China", regex=False)
    df["Country of birth"] = df["Country of birth"].str.replace(
        "China, PeopleÂ’s Republic of ", "China", regex=False
    )

    # Optionally, normalize the text to handle any special characters
    df["Country you have_will immigrate from"] = (
        df["Country you have_will immigrate from"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df["Country of birth"] = (
        df["Country of birth"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    # Update 'Country you have_will immigrate from' column from 'Brasil' to 'Brazil'
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("Euthiopia", "Ethiopia")

    # Update 'Country of birth' column from 'Brasil' to 'Brazil'
    df["Country of birth"] = df["Country of birth"].replace("Euthiopia", "Ethiopia")

    df["Country of birth"] = df["Country of birth"].replace("Hyderabad ", "India")
    korea_variations = [
        "Korea",
        "South Korea",
        "Korea, Republic of",
        "Republic of Korea",
        "Korea South",
    ]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(korea_variations, "South Korea")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        korea_variations, "South Korea"
    )
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("MÃ©xico", "Mexico")

    # Update 'Country of birth' column from 'Brasil' to 'Brazil'
    df["Country of birth"] = df["Country of birth"].replace("MÃ©xico", "Mexico")
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].str.replace("MÃ©xico", "Mexico", regex=False)
    df["Country of birth"] = df["Country of birth"].str.replace(
        "MÃ©xico", "Mexico", regex=False
    )

    # Optionally, normalize the text to handle any special characters
    df["Country you have_will immigrate from"] = (
        df["Country you have_will immigrate from"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df["Country of birth"] = (
        df["Country of birth"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("PerÃº", "Peru")

    df["Country of birth"] = df["Country of birth"].replace("PerÃº", "Peru")

    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].str.replace("PerÃº", "Peru", regex=False)
    df["Country of birth"] = df["Country of birth"].str.replace(
        "PerÃº", "Peru", regex=False
    )

    # Optionally, normalize the text to handle any special characters
    df["Country you have_will immigrate from"] = (
        df["Country you have_will immigrate from"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df["Country of birth"] = (
        df["Country of birth"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    # List of variations to be replaced with 'South Korea'
    Ukraine_variations = [
        "Ukraine ",
        "Ukrainian",
        "Kyiv",
        "Ukrain",
        "Ikey",
        "Ukrainian",
        "Ukrainian ",
        "Ukraine/Canada",
    ]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(Ukraine_variations, "Ukraine")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        Ukraine_variations, "Ukraine"
    )

    # Update 'Country you have_will immigrate from' column from 'Brasil' to 'Brazil'
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("Viet Nam", "Vietnam")

    # Update 'Country of birth' column from 'Brasil' to 'Brazil'
    df["Country of birth"] = df["Country of birth"].replace("Viet Nam", "Vietnam")

    # List of variations to be replaced with 'South Korea'
    Canada_variations = [
        "British Columbia",
        "British Colombia canada",
        "British Colombia canada ",
        "CanadÃ¡",
    ]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(Canada_variations, "Canada")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(Canada_variations, "Canada")

    # Correct the replacement of 'PerÃº' to 'Peru' considering special characters
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].str.replace("CanadÃ¡", "Canada", regex=False)
    df["Country of birth"] = df["Country of birth"].str.replace(
        "CanadÃ¡", "Canada", regex=False
    )

    # Optionally, normalize the text to handle any special characters
    df["Country you have_will immigrate from"] = (
        df["Country you have_will immigrate from"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )
    df["Country of birth"] = (
        df["Country of birth"]
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
    )

    # List of variations to be replaced with 'South Korea'
    Pakistan_variations = ["PAKISTAN ", "Pakistani", "pakistan ", "Pakistani "]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(Pakistan_variations, "Pakistan")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        Pakistan_variations, "Pakistan"
    )

    # List of variations to be replaced with 'South Korea'
    Nigeria_variations = ["NIGERIA", "lagos", "Nigerian"]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(Nigeria_variations, "Nigeria")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        Nigeria_variations, "Nigeria"
    )
    # Update 'Country you have_will immigrate from' column from 'Brasil' to 'Brazil'
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("North", "Iran")
    # Update 'Country you have_will immigrate from' column from 'Brasil' to 'Brazil'
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("Rwanada", "Rwanda")
    # List of variations to be replaced with 'South Korea'
    Turkey_variations = ["turky", "turkiy"]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(Turkey_variations, "Turkey")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(Turkey_variations, "Turkey")
    # List of variations to be replaced with 'South Korea'
    UAE_variations = [
        "UEA",
        "UAE",
        "United Arab Emirates",
        "United Arab Emirates  ",
        "United Arab Emarites",
        "United Arab Emarites ",
    ]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(UAE_variations, "United Arab Emirates")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        Nigeria_variations, "United Arab Emirates"
    )

    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace("US", "USA")
    # List of variations to be replaced with 'South Korea'
    afghanistan_variations = ["Afghanistan", "Afganistan", "Afghanistan"]

    # Update 'Country you have_will immigrate from' column
    df["Country you have_will immigrate from"] = df[
        "Country you have_will immigrate from"
    ].replace(afghanistan_variations, "Afghanistan")

    # Update 'Country of birth' column
    df["Country of birth"] = df["Country of birth"].replace(
        afghanistan_variations, "Afghanistan"
    )
    # Update 'Country you have_will immigrate from' column from 'Brasil' to 'Brazil'
    df["What is your current annual employment salary"] = df[
        "What is your current annual employment salary"
    ].replace("3) $25,000 Â– $50,000", "3) $25,000 – $50,000")
    df["Organization"] = df["Organization"].fillna("Prefer not to disclose")
    df["Age"] = df["Age"].fillna("Prefer not to disclose")
    return df


def clean_exams(df: DataFrame):
    # df["Situation maritale"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Genre"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Âge"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Statut au Canada"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    df["Age"] = df["Age"].fillna("Prefer not to disclose")
    # df["Pays de naissance"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # country of birth
    # df["Pays d_où vous avez immigré_immigrerez"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # immigration country

    # df["Plus haut niveau de formation"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # Highest education
    # df["Quelle est votre situation professionnelle actuelle"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # current employment
    # df["Dans quel secteur dactivite ou industrie travaillez_vous"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # industry

    # df["Est_ce votre secteur ou industrie de prédilection"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # is this ur prefered sector
    # df["Quel est votre salaire annuel actuel"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )
    # # Applying the function to the date column
    # df["Année d_arrivée au Canada _saisir 0 si non applicable_"] = df[
    #     "Année d_arrivée au Canada _saisir 0 si non applicable_"
    # ].apply(extract_year)
    df.loc[df["Username"] == "contact.vsophie@gmail.com", "Pays de naissance"] = (
        "France"
    )
    # df["Année d_arrivée au Canada _saisir 0 si non applicable_"] = df[
    #     "Année d_arrivée au Canada _saisir 0 si non applicable_"
    # ].replace("  ", pd.NA)

    # Find the most frequent value in the column (excluding NaN)
    # most_frequent_value = df[
    #     "Année d_arrivée au Canada _saisir 0 si non applicable_"
    # ].mode(dropna=True)[0]
    # # Fill NaN values with the most frequent value
    # df["Année d_arrivée au Canada _saisir 0 si non applicable_"] = df[
    #     "Année d_arrivée au Canada _saisir 0 si non applicable_"
    # ].fillna(most_frequent_value)

    # English cat
    df["CompletionDate"].fillna("0", inplace=True)
    df.loc[
        df["Username"] == "pink_gricel95@hotmail.com",
        "Country you have_will immigrate from",
    ] = "Mexico"
    df.loc[
        df["Username"] == "zeng.zhix@northeastern.edu",
        "Country you have_will immigrate from",
    ] = "China"
    df.loc[
        df["Username"] == "wu.hao11@northeastern.edu",
        "Country you have_will immigrate from",
    ] = "China"
    df.loc[df["Username"] == "anxu825@gmail.com", "Country of birth"] = "China"
    df.loc[df["Username"] == "raulcan333@gmail.com", "Country of birth"] = "Colombia"
    df.loc[df["Username"] == "niu.me@northeastern.edu", "Country of birth"] = "China"
    df.loc[df["Username"] == "danitacarrasco@hotmail.com", "Country of birth"] = "Chile"
    df.loc[df["Username"] == "fessehad27@gmail.com", "Country of birth"] = "Ethiopia"
    df.loc[
        df["Username"] == "fessehad27@gmail.com", "Country you have_will immigrate from"
    ] = "Ethiopia"
    df["What is your current employment status"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["What industry or sector are you working in"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["What is your current annual employment salary"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["Is this your preferred industry or sector"].fillna(
        "Prefer not to disclose", inplace=True
    )
    return df


def clean_eval(df: DataFrame):
    df["What is your current employment status"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["What industry or sector are you working in"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["What is your current annual employment salary"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df["Is this your preferred industry or sector"].fillna(
        "Prefer not to disclose", inplace=True
    )
    df.loc[
        df["Username"] == "pink_gricel95@hotmail.com",
        "Country you have_will immigrate from",
    ] = "Mexico"
    df.loc[
        df["Username"] == "zeng.zhix@northeastern.edu",
        "Country you have_will immigrate from",
    ] = "China"
    df.loc[
        df["Username"] == "wu.hao11@northeastern.edu",
        "Country you have_will immigrate from",
    ] = "China"
    df.loc[df["Username"] == "anxu825@gmail.com", "Country of birth"] = "China"

    df.loc[df["Username"] == "raulcan333@gmail.com", "Country of birth"] = "Colombia"

    df.loc[df["Username"] == "niu.me@northeastern.edu", "Country of birth"] = "China"

    df.loc[df["Username"] == "danitacarrasco@hotmail.com", "Country of birth"] = "Chile"
    df["Age"] = df["Age"].fillna("Prefer not to disclose")
    return df


def clean_survey(df: DataFrame):
    df = df.drop_duplicates()
    df.reset_index(drop=True, inplace=True)
    df.drop(4446, inplace=True)  # Entry mistake
    # df["Situation maritale"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Statut au Canada"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Genre"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    df["Age"] = df["Age"].fillna("Prefer not to disclose")
    # df["Âge"].fillna("PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True)
    # df["Pays de naissance"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # country of birth
    # df["Pays d_où vous avez immigré_immigrerez"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # immigration country
    # df["Plus haut niveau de formation"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # Highest education
    # df["Quelle est votre situation professionnelle actuelle"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # current employment
    # df["Dans quel secteur dactivite ou industrie travaillez_vous"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # industry
    # df["Est_ce votre secteur ou industrie de prédilection"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # is this ur prefered sector
    # df["Quel est votre salaire annuel actuel"].fillna(
    #     "PrÃ©fÃ¨re ne pas rÃ©pondre", inplace=True
    # )  # annual sal

    # # Applying the function to the date column
    # df["Année d_arrivée au Canada _saisir 0 si non applicable_"] = df[
    #     "Année d_arrivée au Canada _saisir 0 si non applicable_"
    # ].apply(extract_year)
    return df
