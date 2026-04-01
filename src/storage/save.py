from pathlib import Path

def save_parquet(df, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
