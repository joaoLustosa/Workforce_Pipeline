import pandas.pandas as pd
from pathlib import Path

DATA_PATH = Path("data/raw/sample_2000.txt")


def load_data(encoding: str):
    try:
        df = pd.read_csv(
            DATA_PATH,
            sep=";",
            encoding=encoding,
            dtype=str  # force everything as string initially
        )
        print(f"\nSUCCESS with encoding: {encoding}")
        return df
    except Exception as e:
        print(f"\nFAILED with encoding: {encoding}")
        print(e)
        return None


def inspect_dataframe(df: pd.DataFrame):
    print("\n=== BASIC INFO ===")
    print(df.shape)

    print("\n=== COLUMNS ===")
    print(df.columns.tolist())

    print("\n=== DTYPES ===")
    print(df.dtypes)

    print("\n=== NULL COUNTS ===")
    print(df.isnull().sum())

    print("\n=== SAMPLE ROWS ===")
    print(df.head(20))


def main():
    df = load_data("utf-8")
    if df is not None:
        inspect_dataframe(df)
    else:
        raise ValueError("Could not load file.")


if __name__ == "__main__":
    main()
