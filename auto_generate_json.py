import os
import csv
import json
import requests
from collections import defaultdict

# ==============================
# CONFIG
# ==============================
CSV_URL = https://raw.githubusercontent.com/rzee-Jpn/football-datasets/refs/heads/main/datalake/transfermarkt/player_injuries/player_injuries.csv"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800

# ==============================
# PREPARE FOLDER
# ==============================
folders = [
    OUTPUT_DIR,
    os.path.join(OUTPUT_DIR, "leagues"),
    os.path.join(OUTPUT_DIR, "countries"),
    os.path.join(OUTPUT_DIR, "positions"),
    os.path.join(OUTPUT_DIR, "index")
]
for f in folders:
    os.makedirs(f, exist_ok=True)

print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()

csv_lines = response.text.splitlines()
reader = csv.DictReader(csv_lines)

leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)

# ==============================
# FORMAT PEMAIN (10 PANEL)
# ==============================
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
            "berat_badan_kg": "",
            "posisi_utama": row.get("position", ""),
            "posisi_alternatif": "",
            "kaki_dominan": row.get("foot", ""),
            "status_pemain": "Aktif",
            "klub_saat_ini": row.get("current_club", ""),
            "nomor_punggung": "",
            "tanggal_gabung": "",
            "kontrak_mulai": row.get("contract_start", ""),
            "kontrak_berakhir": row.get("contract_expiry", ""),
            "agen": "",
            "media_sosial": []
        },
        "nilai_pasar": {
            "nilai_terkini_eur": row.get("market_value_in_eur", ""),
            "update_terakhir": row.get("last_market_value_update", ""),
            "riwayat_nilai": [],
            "nilai_tertinggi_eur": ""
        },
        "kontrak": {
            "klub": row.get("current_club", ""),
            "liga": row.get("league_name", ""),
            "mulai": row.get("contract_start", ""),
            "berakhir": row.get("contract_expiry", ""),
            "status": "",
            "klausul_rilis": "",
            "gaji_per_musim": ""
        },
        "transfer": [],
        "cedera": [],
        "statistik_performa": {
            "musim": row.get("season", ""),
            "main": row.get("appearances_overall", ""),
            "starter": "",
            "substitusi": "",
            "menit_bermain": "",
            "gol": row.get("goals_overall", ""),
            "assist": row.get("assists_overall", ""),
            "kuning": "",
            "merah": "",
            "clean_sheet": "",
            "kebobolan": "",
            "rasio_gol_per_menit": "",
            "kontribusi_gol_total": "",
            "per_posisi": {}
        },
        "karier_internasional": {
            "negara": row.get("citizenship", ""),
            "level": "",
            "caps": "",
            "gol": "",
            "debut": "",
            "turnamen": []
        },
        "prestasi": [],
        "taktikal": {
            "formasi": "",
            "posisi": "",
            "kontribusi_formasi": ""
        },
        "tren_karier": [],
        "sumber_data": {
            "transfermarkt_profile_url": row.get("url", ""),
            "data_asal": "transfermarkt",
            "update_terakhir": row.get("last_market_value_update", "")
        }
    }

# ==============================
# PROSES CSV
# ==============================
count_players = 0
for row in reader:
    league = row.get("league_name", "").strip() or None
    country = row.get("citizenship", "").strip() or None
    position = row.get("position", "").strip() or "Unknown Position"
    player = format_player(row)

    if league and league.lower() != "unknown":
        leagues[league].append(player)
    elif country and country.lower() != "unknown":
        countries[country].append(player)
    else:
        positions[position].append(player)

    count_players += 1

print(f"✅ Total pemain: {count_players}")
print(f"📊 Liga: {len(leagues)}, Negara: {len(countries)}, Posisi: {len(positions)}")

# ==============================
# SAVE FUNCTION
# ==============================
def save_json_group(name, group_data, folder):
    file_list = []
    for group_name, players in group_data.items():
        safe_name = (
            group_name.lower()
            .replace(" ", "_")
            .replace("/", "_")
            .replace("\\", "_")
            .replace("'", "")
        )
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
            file_list.append({
                "kategori": name,
                "nama_group": group_name,
                "file": filename,
                "jumlah_pemain": len(subset)
            })
            print(f"💾 Saved {name}/{filename} ({len(subset)} pemain)")
    return file_list

# ==============================
# SAVE ALL
# ==============================
index_data = []
index_data += save_json_group("liga", leagues, os.path.join(OUTPUT_DIR, "leagues"))
index_data += save_json_group("negara", countries, os.path.join(OUTPUT_DIR, "countries"))
index_data += save_json_group("posisi", positions, os.path.join(OUTPUT_DIR, "positions"))

index_path = os.path.join(OUTPUT_DIR, "index", "index.json")
with open(index_path, "w", encoding="utf-8") as f:
    json.dump({"data": index_data}, f, ensure_ascii=False, indent=2)
print("🗂️ Index written to data_output/index/index.json")

print("🎉 Semua file JSON selesai dibuat & siap dipanggil dari Blogger!")