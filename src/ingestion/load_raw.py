import pandas as pd
from src.config.settings import RAW_PATH

def load_raw(filename: str = "sample_2000.txt") -> pd.DataFrame:
    path = f"{RAW_PATH}/{filename}"

    return pd.read_csv(
        path,
        sep=";",
        encoding="latin1",   # Maybe change to utf-8
        dtype=str
    )
