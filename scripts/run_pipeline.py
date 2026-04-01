from src.ingestion.load_raw import load_raw
from src.transformation.transform_caged import transform
from src.storage.save import save_parquet

def main():
    df = load_raw()
    print(f"[INFO] Rows loaded: {len(df)}")

    df = transform(df)
    print(f"[INFO] Rows after transform: {len(df)}")

    save_parquet(df, f"{STAGING_PATH}/caged.parquet")
    print("[INFO] Data saved to staging")

    print(df.columns.tolist())

if __name__ == "__main__":
    main()
