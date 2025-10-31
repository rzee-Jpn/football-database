import csv
import json
import os
import requests

# 🔗 URL CSV kamu (bisa ganti ke repo fork kamu)
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/refs/heads/main/datalake/transfermarkt/player_profiles/player_profiles.csv"

# 📁 Folder output
OUTPUT_DIR = "data_output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Mengunduh data CSV dari Transfermarkt dataset...")
response = requests.get(CSV_URL)
response.raise_for_status()

# 💾 Baca CSV baris demi baris
lines = response.text.splitlines()
reader = csv.DictReader(lines)

count_new = 0
count_updated = 0

for row in reader:
    player_id = row.get("player_id", "").strip() or "unknown"
    output_path = os.path.join(OUTPUT_DIR, f"{player_id}.json")

    # Jika file sudah ada, baca data lama
    if os.path.exists(output_path):
        with open(output_path, "r", encoding="utf-8") as f:
            try:
                old_data = json.load(f)
            except json.JSONDecodeError:
                old_data = {}
        merged_data = {**old_data, **row}  # merge data lama dan baru
        action = "updated"
        count_updated += 1
    else:
        merged_data = row
        action = "created"
        count_new += 1

    # Simpan hasil ke JSON
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(merged_data, f, ensure_ascii=False, indent=2)

    print(f"✅ {action.capitalize()} JSON: {output_path}")

print("\n📊 Ringkasan:")
print(f"🆕 File baru: {count_new}")
print(f"♻️  File diperbarui: {count_updated}")
print("✅ Proses selesai.")
