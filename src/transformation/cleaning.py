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

def normalize_column_name(col: str) -> str:
    # remove accents
    col = unicodedata.normalize("NFKD", col).encode("ascii", "ignore").decode("ascii")

    # lowercase
    col = col.lower()

    # remove spaces
    col = col.replace(" ", "")

    return col
