import os
import requests
import pandas as pd

# URL CSV dari repo kamu atau sumber eksternal
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"

OUTPUT_DIR = "data_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Mengunduh CSV dari:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()

# Simpan sementara
with open("player_profiles.csv", "wb") as f:
    f.write(response.content)

print("✅ CSV berhasil diunduh.")

# Baca CSV dengan pandas
df = pd.read_csv("player_profiles.csv")

# Pastikan kolom 'player_id' dan 'name' tersedia
if "player_id" not in df.columns or "name" not in df.columns:
    raise ValueError("Kolom 'player_id' dan 'name' harus ada di CSV!")

print(f"📊 Memproses {len(df)} pemain...")

# Loop setiap pemain
for _, row in df.iterrows():
    player_id = str(row["player_id"])
    player_name = str(row["name"])

    # Path file JSON output per pemain
    json_path = os.path.join(OUTPUT_DIR, f"{player_id}.json")

    # Konversi baris ke dict
    player_data = row.to_dict()

    # Simpan ke JSON
    pd.Series(player_data).to_json(json_path, force_ascii=False, indent=2)

print("✅ Semua data pemain berhasil disimpan ke folder:", OUTPUT_DIR)
