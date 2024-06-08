from pandas import DataFrame, concat, read_sql
from functools import lru_cache
import time
from googletrans import Translator as GoogleTranslator
from db import check_table_exists, mysql_engine

translator = GoogleTranslator()
translator.raise_Exception = True


@lru_cache
def translate(text):
    if not text or type(text) != str:
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
    table_name: str = "translations"

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
        self._load_cached_translations()

    def _save_df_to_sql(self):
        self.df.to_sql(
            self.table_name, con=mysql_engine, index="index", if_exists="replace"
        )

    def fetch_translation(self, input):
        result = self.df.loc[self.df["input"] == input]
        if result.values.size > 0:
            return result["output"][result.index[0]]
        self.df = concat(
            [self.df, DataFrame([{"input": input, "output": translate(input)}])],
            ignore_index=True,
        )
        self._save_df_to_sql()
        return self.fetch_translation(input)

    def _load_cached_translations(self):
        if check_table_exists(self.table_name):
            self.df = read_sql(
                f"select * from {self.table_name}", con=mysql_engine, index_col="index"
            )
        else:
            self._save_df_to_sql()
