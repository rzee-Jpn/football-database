import os
import csv
import json
import requests

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data_output"

# ===== BUAT FOLDER OUTPUT =====
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== DOWNLOAD CSV =====
print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()
csv_lines = response.text.splitlines()

# ===== PARSE CSV =====
reader = csv.DictReader(csv_lines)
print(f"📊 Processing {len(list(reader))} rows...")

# reset reader
reader = csv.DictReader(csv_lines)

# ===== BUAT JSON PER PEMAIN =====
count_new = 0
count_updated = 0

for row in reader:
    player_id = str(row.get("player_id", "unknown"))
    player_name = row.get("player_name", f"player_{player_id}").replace(" ", "_").lower()

    # File path per pemain
    json_path = os.path.join(OUTPUT_DIR, f"{player_id}.json")

    # Merge data lama jika file sudah ada
    if os.path.exists(json_path):
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
            except json.JSONDecodeError:
                old_data = {}
        old_data.update(row)
        merged_data = old_data
        count_updated += 1
    else:
        merged_data = row
        count_new += 1

    # Simpan JSON
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)

print(f"✅ JSON created: {count_new}, updated: {count_updated}")
print("All JSON files saved in:", OUTPUT_DIR)
