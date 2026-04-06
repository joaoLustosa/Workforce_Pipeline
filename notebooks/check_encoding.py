import pandas as pd

encodings = ["latin-1", "ISO-8859-1", "cp1252", "utf-8"]

for enc in encodings:
    try:
        df = pd.read_csv("data/raw/sample_2000.txt", sep=";", encoding=enc, nrows=5)
        print(f"\nSUCCESS: {enc}")
        print(df.columns.tolist())
    except Exception as e:
        print(f"\nFAILED: {enc}")
