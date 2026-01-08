# spark_streaming_feeder.py
import pandas as pd, time, os

SRC = "udemy_data.csv"
DEST_DIR = "spark_demo/input"
BATCH_ROWS = 2000   # koliko redova po fajlu (podesi po želji)
DELAY_SEC = 3       # pauza između fajlova

os.makedirs(DEST_DIR, exist_ok=True)

df = pd.read_csv(SRC)     # čita tvoj veliki CSV
print("source rows:", len(df))

start = 0
part = 0
while start < len(df):
    chunk = df.iloc[start:start+BATCH_ROWS]
    out = os.path.join(DEST_DIR, f"part_{part:03d}.csv")
    # BEZ headera, da streaming čita čist sadržaj po zadatoj šemi
    chunk.to_csv(out, index=False, header=False)
    print("wrote", out, "rows:", len(chunk))
    start += BATCH_ROWS
    part += 1
    time.sleep(DELAY_SEC)

print("feeder done.")


