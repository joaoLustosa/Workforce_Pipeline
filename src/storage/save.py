from pathlib import Path
import shutil

def save_parquet_partitioned(df, base_path: str):
    path = Path(base_path)

    # extract partition values from dataframe
    partitions = df["competencia_mov_partition"].dropna().unique()

    for value in partitions:
        partition_path = path / f"competencia_mov_partition={value}"

        if partition_path.exists():
            shutil.rmtree(partition_path)

    path.mkdir(parents=True, exist_ok=True)

    df.to_parquet(
        path,
        partition_cols=["competencia_mov_partition"],
        engine="pyarrow",
        index=False
    )
