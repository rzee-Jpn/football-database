import os
import csv
import json
import requests
from collections import defaultdict
import shutil

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data_output"

# ===== CLEANUP =====
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()

reader = csv.DictReader(response.text.splitlines())
leagues = defaultdict(list)

# ===== GROUP PER LIGA =====
for row in reader:
    league = row.get("league_name", "Unknown League") or "Unknown League"
    leagues[league].append({
        "id": row.get("player_id"),
        "name": row.get("player_name"),
        "club": row.get("club_name"),
        "age": row.get("age"),
        "nationality": row.get("country_of_citizenship"),
        "position": row.get("position"),
        "market_value": row.get("market_value_in_eur")
    })

# ===== SIMPAN JSON PER LIGA =====
for league_name, players in leagues.items():
    safe_name = (
        league_name.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
    )
    path = os.path.join(OUTPUT_DIR, f"{safe_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump({
            "league": league_name,
            "player_count": len(players),
            "players": players
        }, f, ensure_ascii=False, indent=2)
    print(f"✅ Saved: {path} ({len(players)} players)")

print(f"\n🎉 DONE! Total leagues: {len(leagues)}")
print(f"All JSON saved in: {OUTPUT_DIR}")