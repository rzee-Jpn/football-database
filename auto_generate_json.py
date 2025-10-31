import os
import csv
import json
import requests
from collections import defaultdict

# ===== CONFIG =====
BASE_URL = "https://raw.githubusercontent.com/rzee-Jpn/football-datasets/refs/heads/main/datalake/transfermarkt"
CSV_URLS = {
    "players": f"{BASE_URL}/player_profiles/player_profiles.csv",
    "teams": f"{BASE_URL}/team_details/team_details.csv",
    "competitions": f"{BASE_URL}/team_competitions_seasons/team_competitions_seasons.csv",
    "injuries": f"{BASE_URL}/player_injuries/player_injuries.csv"
}
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800

os.makedirs(OUTPUT_DIR, exist_ok=True)

def download_csv(url):
    print(f"📥 Downloading: {url}")
    r = requests.get(url)
    r.raise_for_status()
    lines = r.text.splitlines()
    return list(csv.DictReader(lines))

# ===== LOAD ALL DATA =====
players_data = download_csv(CSV_URLS["players"])
teams_data = download_csv(CSV_URLS["teams"])
competitions_data = download_csv(CSV_URLS["competitions"])
injuries_data = download_csv(CSV_URLS["injuries"])

print(f"✅ Loaded datasets:")
print(f"   Players: {len(players_data)} | Teams: {len(teams_data)} | Competitions: {len(competitions_data)} | Injuries: {len(injuries_data)}")

# ===== BUILD INDEX (by player_name / club_id) =====
injuries_by_player = defaultdict(list)
for inj in injuries_data:
    pname = inj.get("player_name", "").strip()
    if pname:
        injuries_by_player[pname].append({
            "cedera": inj.get("injury", ""),
            "mulai": inj.get("injury_start", ""),
            "selesai": inj.get("injury_end", ""),
            "absen_hari": inj.get("days", ""),
            "absen_pertandingan": inj.get("games_missed", ""),
            "sumber": inj.get("url", "")
        })

team_index = {t.get("club_name", "").strip(): t for t in teams_data}

# ===== GROUP STRUCTURE =====
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)
clubs = defaultdict(list)

# ===== PLAYER STRUCTURE (10 PANEL) =====
def format_player(row):
    player_name = row.get("player_name", "")
    team_name = row.get("current_club", "")
    injuries = injuries_by_player.get(player_name, [])

    return {
        "identitas_dasar": {
            "nama_lengkap": player_name,
            "nama_panggilan": player_name,
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
            "klub_saat_ini": team_name,
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
            "klub": team_name,
            "liga": row.get("league_name", ""),
            "mulai": row.get("contract_start", ""),
            "berakhir": row.get("contract_expiry", ""),
            "status": "",
            "klausul_rilis": "",
            "gaji_per_musim": ""
        },
        "transfer": [],
        "cedera": injuries,
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

# ===== GROUPING PLAYERS =====
for row in players_data:
    league = row.get("league_name", "").strip() or None
    country = row.get("citizenship", "").strip() or None
    position = row.get("position", "").strip() or "Unknown Position"
    club_name = row.get("current_club", "").strip() or "Unknown Club"

    player = format_player(row)

    if league and league.lower() != "unknown":
        leagues[league].append(player)
    elif country and country.lower() != "unknown":
        countries[country].append(player)
    else:
        positions[position].append(player)

    clubs[club_name].append(player)

print(f"✅ Total pemain: {len(players_data)}")

# ===== SIMPAN JSON =====
def save_json_group(name, group_data, folder):
    os.makedirs(folder, exist_ok=True)
    for group_name, items in group_data.items():
        safe_name = group_name.lower().replace(" ", "_").replace("/", "_")
        for i in range(0, len(items), MAX_PLAYERS_PER_FILE):
            part = i // MAX_PLAYERS_PER_FILE + 1
            subset = items[i:i + MAX_PLAYERS_PER_FILE]
            data = {
                "kategori": name,
                "nama_group": group_name,
                "total_item": len(subset),
                "data": subset
            }
            filename = (
                f"{safe_name}_part{part}.json"
                if len(items) > MAX_PLAYERS_PER_FILE
                else f"{safe_name}.json"
            )
            with open(os.path.join(folder, filename), "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"💾 Saved {name}/{filename} ({len(subset)} items)")

save_json_group("liga", leagues, os.path.join(OUTPUT_DIR, "leagues"))
save_json_group("negara", countries, os.path.join(OUTPUT_DIR, "countries"))
save_json_group("posisi", positions, os.path.join(OUTPUT_DIR, "positions"))
save_json_group("klub", clubs, os.path.join(OUTPUT_DIR, "clubs"))

# ===== SIMPAN DATA TAMBAHAN =====
with open(os.path.join(OUTPUT_DIR, "team_details.json"), "w", encoding="utf-8") as f:
    json.dump(teams_data, f, ensure_ascii=False, indent=2)

with open(os.path.join(OUTPUT_DIR, "competitions.json"), "w", encoding="utf-8") as f:
    json.dump(competitions_data, f, ensure_ascii=False, indent=2)

with open(os.path.join(OUTPUT_DIR, "injuries.json"), "w", encoding="utf-8") as f:
    json.dump(injuries_data, f, ensure_ascii=False, indent=2)

print("🎯 Semua dataset selesai dikonversi ke JSON modular (versi 3.0 lengkap).")