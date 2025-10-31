import os
import csv
import json
import requests
from collections import defaultdict

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data_output"

# ===== BUAT FOLDER OUTPUT =====
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()
csv_lines = response.text.splitlines()
reader = csv.DictReader(csv_lines)

# ===== BUAT STRUKTUR PENYIMPANAN BERDASARKAN LIGA =====
leagues = defaultdict(lambda: {
    "league": "",
    "season": "2024/2025",
    "players": {}
})

def format_player(row):
    """Bentuk struktur JSON lengkap per pemain."""
    return {
        "personal_info": {
            "full_name": row.get("player_name", ""),
            "known_as": row.get("player_name", ""),
            "birth_date": row.get("date_of_birth", ""),
            "age": row.get("age", ""),
            "birth_place": {
                "city": row.get("place_of_birth", ""),
                "country": row.get("country_of_birth", "")
            },
            "nationality": [row.get("citizenship", "")],
            "height_cm": row.get("height", ""),
            "weight_kg": "",
            "main_position": row.get("position", ""),
            "other_positions": [],
            "foot": row.get("foot", ""),
            "player_status": row.get("player_status", ""),
            "current_club": row.get("current_club", ""),
            "shirt_number": row.get("shirt_number", ""),
            "joined_date": row.get("joined_date", ""),
            "contract_duration": {
                "from": row.get("contract_start", ""),
                "to": row.get("contract_expiry", "")
            },
            "agent": row.get("agent", ""),
            "social_media": {
                "instagram": "",
                "twitter": ""
            }
        },
        "market_value": {
            "current_value_eur": row.get("market_value_in_eur", ""),
            "last_update": row.get("last_market_value_update", ""),
            "highest_value_eur": "",
            "value_history": []
        },
        "club_contract": {
            "club": row.get("current_club", ""),
            "league": row.get("league_name", ""),
            "contract_period": {
                "from": row.get("contract_start", ""),
                "to": row.get("contract_expiry", "")
            },
            "status": "Active",
            "release_clause_eur": "",
            "salary": {
                "weekly_eur": "",
                "annual_eur": ""
            }
        },
        "transfer_history": [],
        "injury_history": [],
        "performance_stats": [],
        "international_career": {
            "country": "",
            "levels": []
        },
        "achievements": {
            "team_titles": [],
            "individual_awards": [],
            "total_trophies": ""
        },
        "tactical_stats": {
            "preferred_formation": "",
            "most_played_position": row.get("position", ""),
            "heatmap_contribution": ""
        },
        "career_trends": {
            "career_timeline": [],
            "position_changes": [],
            "market_value_progression": [],
            "performance_trend": []
        }
    }

# ===== PROSES DATA =====
count_players = 0
for row in reader:
    league_name = row.get("league_name", "Unknown League") or "Unknown League"
    player_id = str(row.get("player_id", "unknown"))
    player_name = row.get("player_name", f"player_{player_id}").replace(" ", "_").lower()

    leagues[league_name]["league"] = league_name
    leagues[league_name]["players"][player_name] = format_player(row)
    count_players += 1

# ===== SIMPAN FILE PER LIGA =====
for league_name, league_data in leagues.items():
    safe_name = league_name.lower().replace(" ", "_").replace("/", "_")
    json_path = os.path.join(OUTPUT_DIR, f"{safe_name}.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(league_data, f, ensure_ascii=False, indent=2)

print(f"✅ Processed {count_players} players into {len(leagues)} league files")
print("📂 All JSON saved in:", OUTPUT_DIR)