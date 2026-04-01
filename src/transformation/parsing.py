import pandas as pd

def parse_br_float(series):
  return pd.to_numeric(
    series.astype(str)
      .str.replace(".", "", regex=False)
      .str.replace(",", ".", regex=False),
    errors="coerce"
  )

def parse_int(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")

def parse_bool(series):
  return series.astype(str).map({"1": True, "0": False}).astype("boolean")

def parse_yyyymm(series):
  return pd.to_datetime(series.astype(str), format="%Y%m", errors="coerce")
