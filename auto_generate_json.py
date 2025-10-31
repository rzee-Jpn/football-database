import os
import csv
import json
import requests
from collections import defaultdict

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800  # batas aman per file

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()
csv_lines = response.text.splitlines()
reader = csv.DictReader(csv_lines)

# ===== STRUKTUR DATA =====
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)

# ===== FORMAT PEMAIN SESUAI STRUKTUR JSON YANG KAMU MAU =====
def format_player(row):
    return {
        "identitas_dasar": {
            "nama_lengkap": row.get("player_name", ""),
            "nama_panggilan": row.get("player_name", ""),
            "tanggal_lahir": row.get("date_of_birth", ""),
            "usia": row.get("age", ""),
            "tempat_lahir": {
                "kota": row.get("place_of_birth", ""),
                "negara": row.get("country_of_birth", "")
            },
            "kewarganegaraan": [row.get("citizenship", "")],
            "tinggi_badan_cm": row.get("height", ""),
            "posisi_utama": row.get("position", ""),
            "kaki_dominan": row.get("foot", ""),
            "klub_saat_ini": row.get("current_club", "")
        },
        "nilai_pasar": {
            "nilai_terkini_eur": row.get("market_value_in_eur", ""),
            "update_terakhir": row.get("last_market_value_update", "")
        },
        "kontrak": {
            "klub": row.get("current_club", ""),
            "mulai": row.get("contract_start", ""),
            "berakhir": row.get("contract_expiry", "")
        },
        "statistik_performa": {
            "musim": row.get("season", ""),
            "main": row.get("appearances_overall", ""),
            "gol": row.get("goals_overall", ""),
            "assist": row.get("assists_overall", "")
        }
    }

count_players = 0
for row in reader:
    league = row.get("league_name", "").strip() or None
    country = row.get("citizenship", "").strip() or None
    position = row.get("position", "").strip() or "Unknown Position"
    player = format_player(row)

    # Prioritas 1: Liga
    if league and league.lower() != "unknown":
        leagues[league].append(player)
    # Prioritas 2: Negara (jika liga unknown)
    elif country and country.lower() != "unknown":
        countries[country].append(player)
    # Prioritas 3: Posisi (fallback terakhir)
    else:
        positions[position].append(player)

    count_players += 1

print(f"✅ Total pemain: {count_players}")
print(f"📊 Liga terdeteksi: {len(leagues)}, Negara fallback: {len(countries)}, Posisi fallback: {len(positions)}")

# ===== FUNCTION SIMPAN =====
def save_json_group(name, group_data, folder):
    os.makedirs(folder, exist_ok=True)
    for group_name, players in group_data.items():
        safe_name = group_name.lower().replace(" ", "_").replace("/", "_")
        for i in range(0, len(players), MAX_PLAYERS_PER_FILE):
            part = i // MAX_PLAYERS_PER_FILE + 1
            subset = players[i:i + MAX_PLAYERS_PER_FILE]
            data = {
                "kategori": name,
                "nama_group": group_name,
                "total_pemain": len(subset),
                "pemain": subset
            }
            filename = (
                f"{safe_name}_part{part}.json"
                if len(players) > MAX_PLAYERS_PER_FILE
                else f"{safe_name}.json"
            )
            path = os.path.join(folder, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"💾 Saved {name}/{filename} ({len(subset)} pemain)")

# ===== SIMPAN SEMUA =====
save_json_group("liga", leagues, os.path.join(OUTPUT_DIR, "leagues"))
save_json_group("negara", countries, os.path.join(OUTPUT_DIR, "countries"))
save_json_group("posisi", positions, os.path.join(OUTPUT_DIR, "positions"))

print("🎉 Semua file selesai dibuat & sudah otomatis dibagi per kategori fallback.")