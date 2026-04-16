from pathlib import Path

def save_rejects(df_rejects, base_path: str):
    if df_rejects is None or df_rejects.empty:
        return

    path = Path(base_path)
    path.mkdir(parents=True, exist_ok=True)

    output_file = path / "rejects.parquet"

    df_rejects.to_parquet(output_file, index=False)

    print(f"[INFO] Rejects saved to {output_file}")
