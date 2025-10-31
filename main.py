import pandas as pd
import os
import json

# URL CSV dari repo hasil fork kamu
CSV_URL = "https://raw.githubusercontent.com/rzee-Jpn/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data/processed/clubs/"

# Pastikan folder ada
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Mengunduh data CSV...")
df = pd.read_csv(CSV_URL)

for _, row in df.iterrows():
    club = str(row.get("club_name", "Unknown")).strip().replace("/", "-")
    player_name = str(row.get("player_name", "Unknown")).strip().replace(" ", "_")

    club_dir = os.path.join(OUTPUT_DIR, club)
    os.makedirs(club_dir, exist_ok=True)

    file_path = os.path.join(club_dir, f"{player_name}.json")

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(row.to_dict(), f, indent=2, ensure_ascii=False)

print("✅ Semua data pemain berhasil disimpan per klub!")
