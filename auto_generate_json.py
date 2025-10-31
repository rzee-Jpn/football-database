#!/usr/bin/env python3
import os
import csv
import json
import requests
from collections import defaultdict
from datetime import datetime
import re

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/salimt/football-datasets/main/datalake/transfermarkt/player_profiles/player_profiles.csv"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800  # batas aman per file
INDEX_FOLDER = os.path.join(OUTPUT_DIR, "index")
TIMESTAMP = datetime.utcnow().isoformat() + "Z"

os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(INDEX_FOLDER, exist_ok=True)

# ===== HELPERS =====
def slugify(s: str) -> str:
    if not s:
        return "unknown"
    s = s.strip().lower()
    s = re.sub(r"[\/\\\s]+", "_", s)  # spaces/slashes => underscore
    s = re.sub(r"[^\w\-]", "", s)     # keep alnum, underscore, dash
    s = re.sub(r"_+", "_", s)
    return s.strip("_") or "unknown"

def safe_list(v):
    if v is None or v == "":
        return []
    if isinstance(v, list):
        return v
    # try comma separated values
    return [x.strip() for x in str(v).split(",") if x.strip()]

# ===== DOWNLOAD CSV =====
print("📥 Downloading CSV from:", CSV_URL)
resp = requests.get(CSV_URL, timeout=60)
resp.raise_for_status()
lines = resp.text.splitlines()
reader = csv.DictReader(lines)

# ===== COLLECTIONS =====
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)
clubs = defaultdict(list)

# For index building later
index = {
    "generated_at": TIMESTAMP,
    "leagues": {},
    "clubs": {},
    "countries": {},
    "positions": {}
}

# ===== FORMAT PEMAIN: FULL 10-PANEL (isi kosong jika tidak tersedia) =====
def format_player_full(row):
    # helper to read multiple possible column names
    def any_of(*keys):
        for k in keys:
            if k in row and row[k] not in (None, ""):
                return row[k]
        return ""

    # build full structure as requested
    player = {
        # 1. Identitas & Informasi Pribadi
        "identitas_dasar": {
            "nama_lengkap": any_of("player_name", "full_name"),
            "nama_panggilan": any_of("known_as", "nickname", "common_name"),
            "tanggal_lahir": any_of("date_of_birth", "birth_date"),
            "usia": any_of("age"),
            "tempat_lahir": {
                "kota": any_of("place_of_birth", "birth_place_city"),
                "negara": any_of("country_of_birth", "birth_place_country")
            },
            "kewarganegaraan": safe_list(any_of("citizenship", "nationality")),
            "tinggi_badan_cm": any_of("height", "height_in_cm"),
            "berat_badan_kg": any_of("weight", "weight_kg"),
            "posisi_utama": any_of("position", "main_position"),
            "posisi_alternatif": safe_list(any_of("other_positions", "positions")),
            "kaki_dominan": any_of("foot", "preferred_foot"),
            "status_pemain": any_of("player_status", "status"),
            "klub_saat_ini": any_of("current_club", "club"),
            "nomor_punggung": any_of("shirt_number", "squad_number"),
            "tanggal_gabung": any_of("joined_date", "joined"),
            "kontrak_mulai": any_of("contract_start", "contract_from"),
            "kontrak_berakhir": any_of("contract_expiry", "contract_to"),
            "agen": any_of("agent"),
            "media_sosial": {
                "instagram": any_of("instagram"),
                "twitter": any_of("twitter"),
                "other": safe_list(any_of("social_links", "social_media"))
            }
        },

        # 2. Nilai Pasar
        "nilai_pasar": {
            "nilai_terkini_eur": any_of("market_value_in_eur", "market_value"),
            "update_terakhir": any_of("last_market_value_update", "market_value_date"),
            "riwayat_nilai": [],           # kosong — but ready to be filled
            "nilai_tertinggi_eur": any_of("market_value_highest")
        },

        # 3. Data Klub & Kontrak
        "kontrak": {
            "klub": any_of("current_club", "club"),
            "liga": any_of("league_name", "league"),
            "nomor_kontrak_mulai": any_of("contract_start"),
            "nomor_kontrak_berakhir": any_of("contract_expiry"),
            "status": any_of("contract_status", ""),
            "klausul_rilis": any_of("release_clause"),
            "gaji_per_minggu_eur": any_of("wage_weekly", "salary_weekly"),
            "gaji_per_tahun_eur": any_of("wage_annual", "salary_annual")
        },

        # 4. Riwayat Transfer
        "transfer_history": [],

        # 5. Data Cedera (Injury History)
        "injury_history": [],

        # 6. Statistik Performa (list per season/competition)
        "performance_stats": [],

        # 7. Karier Internasional
        "international_career": {
            "negara": any_of("citizenship"),
            "levels": []  # ready to fill with objects {level, caps, goals, debut, tournaments}
        },

        # 8. Prestasi & Penghargaan
        "achievements": {
            "team_titles": [],
            "individual_awards": [],
            "total_trophies": ""
        },

        # 9. Statistik Taktikal
        "tactical_stats": {
            "preferred_formation": any_of("preferred_formation"),
            "most_played_position": any_of("position"),
            "heatmap_contribution": ""
        },

        # 10. Tren Karier
        "career_trends": {
            "career_timeline": [],
            "position_changes": [],
            "market_value_progression": [],
            "performance_trend": []
        },

        # metadata / source
        "sumber_data": {
            "origin_csv_fields": {k: v for k, v in row.items() if v not in (None, "")},
            "generated_at": TIMESTAMP
        }
    }

    # attempt to auto-fill some lists if CSV has common columns
    # transfer columns could be like transfer_date_1, transfer_fee_1, ...
    # but CSV likely doesn't have; left as empty for now.

    return player

# ===== PARSING CSV & GROUPING LOGIC (prioritas klub > liga > negara > posisi) =====
total = 0
for row in reader:
    player = format_player_full(row)

    club = (row.get("current_club") or "").strip()
    league = (row.get("league_name") or "").strip()
    country = (row.get("citizenship") or "").strip()
    position = (row.get("position") or "").strip() or "Unknown"

    # Preference: club if present, else league, else country, else position
    if club and club.lower() not in ("", "unknown", "n/a", "none"):
        clubs[club].append(player)
    elif league and league.lower() not in ("", "unknown", "n/a", "none"):
        leagues[league].append(player)
    elif country and country.lower() not in ("", "unknown", "n/a", "none"):
        countries[country].append(player)
    else:
        positions[position].append(player)

    total += 1

print(f"✅ Total processed players: {total}")
print(f"📊 counts -> clubs: {len(clubs)}, leagues: {len(leagues)}, countries: {len(countries)}, positions: {len(positions)}")

# ===== SAVE UTIL =====
def save_group_to_folder(group_name, group_dict, target_folder):
    os.makedirs(target_folder, exist_ok=True)
    file_list = []
    for key, players in group_dict.items():
        slug = slugify(key)
        # split big lists into parts
        for i in range(0, len(players), MAX_PLAYERS_PER_FILE):
            part_idx = i // MAX_PLAYERS_PER_FILE + 1
            subset = players[i:i+MAX_PLAYERS_PER_FILE]
            filename = f"{slug}.json" if len(players) <= MAX_PLAYERS_PER_FILE else f"{slug}_part{part_idx}.json"
            path = os.path.join(target_folder, filename)
            payload = {
                "kategori": group_name,
                "nama_group": key,
                "total_pemain_in_file": len(subset),
                "players_count_group_total": len(players),
                "players": subset,
                "generated_at": TIMESTAMP
            }
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            file_list.append(filename)
            print(f"💾 Saved {group_name}/{filename} ({len(subset)} players)")
    return file_list

# ===== WRITE FILES & BUILD INDEX =====
leagues_folder = os.path.join(OUTPUT_DIR, "leagues")
clubs_folder = os.path.join(OUTPUT_DIR, "clubs")
countries_folder = os.path.join(OUTPUT_DIR, "countries")
positions_folder = os.path.join(OUTPUT_DIR, "positions")

index["leagues"] = {k: save_group_to_folder("league", {k:v}, leagues_folder) for k,v in leagues.items()}
index["clubs"] = {k: save_group_to_folder("club", {k:v}, clubs_folder) for k,v in clubs.items()}
index["countries"] = {k: save_group_to_folder("country", {k:v}, countries_folder) for k,v in countries.items()}
index["positions"] = {k: save_group_to_folder("position", {k:v}, positions_folder) for k,v in positions.items()}

# write top-level index files
with open(os.path.join(INDEX_FOLDER, "index.json"), "w", encoding="utf-8") as fh:
    json.dump(index, fh, ensure_ascii=False, indent=2)
print(f"🗂️ Index written to {os.path.join(INDEX_FOLDER, 'index.json')}")

# also create a simple master file listing all files (easy for frontend)
master_list = {
    "generated_at": TIMESTAMP,
    "files": {
        "leagues": {},
        "clubs": {},
        "countries": {},
        "positions": {}
    }
}
# populate master list with filenames (slugs)
for folder_name, folder_path in (("leagues", leagues_folder), ("clubs", clubs_folder),
                                 ("countries", countries_folder), ("positions", positions_folder)):
    if not os.path.exists(folder_path):
        continue
    for fname in sorted(os.listdir(folder_path)):
        master_list["files"].setdefault(folder_name, []).append(fname)

with open(os.path.join(INDEX_FOLDER, "master_files.json"), "w", encoding="utf-8") as fh:
    json.dump(master_list, fh, ensure_ascii=False, indent=2)
print(f"🗂️ Master files list written to {os.path.join(INDEX_FOLDER, 'master_files.json')}")

print("🎉 Semua file selesai dibuat. Struktur lengkap 10-panel tersedia per pemain dalam masing-masing group.")