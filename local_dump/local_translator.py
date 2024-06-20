from pandas import DataFrame, concat, read_csv
from functools import lru_cache
import time
from googletrans import Translator as GoogleTranslator
from translation_config import (
    fix_column_translate,
    fix_duplicate_columns,
    columns_to_be_translated,
)

translator = GoogleTranslator()
translator.raise_Exception = True
TRANSLATION_FILE = "local_dump/translations.csv"


@lru_cache
def translate(text):
    if not text or type(text) != str or not text.strip():
        return text
    try:
        translation = translator.translate(text, dest="en")
    except Exception as e:
        print(e)
        if e.args[0] == "Unexpected status code \"429\" from ('translate.google.com',)":
            time.sleep(60)
            return translate(text)
        raise e
    return translation.text


class Translator:
    df: DataFrame

    def __init__(self) -> None:

        self.df = DataFrame(
            [
                {"input": "Homme", "output": "Male"},
                {"input": "Femme", "output": "Female"},
                {"input": "Oui", "output": "Yes"},
                {"input": "Non", "output": "No"},
                {"input": "Genre", "output": "Gender"},
                {"input": "Statut au Canada", "output": "Status in Canada"},
            ]
        )
        # if translations cached locally load csv from there else create csv
        self._get_df_from_csv()

    def _save_df_to_csv(self):
        self.df.to_csv(TRANSLATION_FILE, index=False)

    def _get_df_from_csv(self):
        try:
            self.df = read_csv(
                TRANSLATION_FILE,
                encoding="utf-8",
            )
        except FileNotFoundError as e:
            self._save_df_to_csv()

    # @lru_cache
    def fetch_translation(self, input):
        if not input or type(input) != str:
            return input
        result = self.df.loc[self.df["input"] == input]
        if result.values.size > 0:
            return result["output"][result.index[0]]
        self.df = concat(
            [self.df, DataFrame([{"input": input, "output": translate(input)}])],
            ignore_index=True,
        )
        self._save_df_to_csv()
        return self.fetch_translation(input)


def _copy_values_between_cols(df: DataFrame, drop_col: str, keep_col: str):
    if drop_col not in df:
        return False
    if keep_col not in df:
        df.loc[:, keep_col] = df[drop_col]
    df.loc[:, keep_col] = df[keep_col].fillna(df[drop_col])
    df.loc[:, drop_col] = df[drop_col].fillna(df[keep_col])
    df.loc[:, keep_col] = df[drop_col]
    return True


def translate_columns(df: DataFrame):
    translator_obj = Translator()
    french_cols = {}
    for col in df.columns.to_list():
        translated_text = translator_obj.fetch_translation(col)
        if translated_text != col:
            french_cols.update({col: translated_text})
    columns_to_drop = []
    for french_col, english_col in french_cols.items():
        new_col_name = fix_column_translate.get(english_col, english_col)
        if _copy_values_between_cols(df, drop_col=french_col, keep_col=new_col_name):
            columns_to_drop.append(french_col)
    for drop_col, keep_col in fix_duplicate_columns.items():
        if _copy_values_between_cols(df, drop_col=drop_col, keep_col=keep_col):
            columns_to_drop.append(drop_col)
    new_columns = [col for col in df.columns if col not in columns_to_drop]
    df = df[new_columns]
    return df


def translate_rows(df: DataFrame):
    translator_obj = Translator()
    print("start translation of each row")
    for index, row in df.iterrows():
        for column in columns_to_be_translated:
            if column in row:
                df.at[index, column] = translator_obj.fetch_translation(
                    df.at[index, column]
                )
    print("all row translation complete")
    return df
