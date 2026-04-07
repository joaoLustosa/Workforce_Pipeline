from src.ingestion.load_raw import load_raw
from src.transformation.transform_caged import transform
from src.curation.aggregate_caged import aggregate_employment_by_uf
from src.storage.save import save_parquet_partitioned
from src.config.settings import STAGING_PATH, CURATED_PATH

def main():
    df = load_raw()
    print(f"[INFO] Rows loaded: {len(df)}")

    df = transform(df)
    print(f"[INFO] Rows after transform: {len(df)}")

    df_curated = aggregate_employment_by_uf(df)

    save_parquet_partitioned(df, f"{STAGING_PATH}/caged")

    save_parquet_partitioned(
    df_curated,
    f"{CURATED_PATH}/caged_employment_by_uf"
    )

    print(df.columns.tolist())

if __name__ == "__main__":
    main()
