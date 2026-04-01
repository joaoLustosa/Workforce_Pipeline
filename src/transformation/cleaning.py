import unicodedata

def normalize_columns(df):
    df.columns = [
        unicodedata.normalize("NFKD", col)
        .encode("ascii", "ignore")
        .decode("ascii")
        .lower()
        for col in df.columns
    ]
    return df
