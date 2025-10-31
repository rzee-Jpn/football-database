import os
import csv
import json
import requests
from collections import defaultdict

# ===== CONFIG =====
CSV_URL = "https://raw.githubusercontent.com/rzee-Jpn/football-datasets/refs/heads/main/datalake/transfermarkt/player_latest_market_value/player_latest_market_value.csv"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800  # batas aman per file

os.makedirs(OUTPUT_DIR, exist_ok=True)

print("📥 Downloading CSV from:", CSV_URL)
response = requests.get(CSV_URL)
response.raise_for_status()
csv_lines = response.text.splitlines()
reader = csv.DictReader(csv_lines)

# ===== STRUKTUR KELOMPOK =====
clubs = defaultdict(list)
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)

# ===== SAFE GETTER =====
def safe_get(row, key, default=""):
    return row.get(key, default) if key in row else default

# ===== FORMAT PEMAIN SESUAI STRUKTUR LENGKAP (10 PANEL) =====
def format_player(row):
    return {
        "identitas_dasar": {
            "nama_lengkap": safe_get(row, "player_name"),
            "nama_panggilan": safe_get(row, "player_name"),
            "tanggal_lahir": safe_get(row, "date_of_birth"),
            "usia": safe_get(row, "age"),
            "tempat_lahir": {
                "kota": safe_get(row, "place_of_birth"),
                "negara": safe_get(row, "country_of_birth")
            },
            "kewarganegaraan": [safe_get(row, "citizenship")],
            "tinggi_badan_cm": safe_get(row, "height"),
            "berat_badan_kg": safe_get(row, "weight"),
            "posisi_utama": safe_get(row, "position"),
            "posisi_alternatif": "",
            "kaki_dominan": safe_get(row, "foot"),
            "status_pemain": "Aktif",
            "klub_saat_ini": safe_get(row, "current_club"),
            "nomor_punggung": safe_get(row, "shirt_number"),
            "tanggal_gabung": safe_get(row, "joined_date"),
            "kontrak_mulai": safe_get(row, "contract_start"),
            "kontrak_berakhir": safe_get(row, "contract_expiry"),
            "agen": safe_get(row, "agent"),
            "media_sosial": []
        },
        "nilai_pasar": {
            "nilai_terkini_eur": safe_get(row, "market_value_in_eur"),
            "update_terakhir": safe_get(row, "last_market_value_update"),
            "riwayat_nilai": [],
            "nilai_tertinggi_eur": ""
        },
        "kontrak": {
            "klub": safe_get(row, "current_club"),
            "liga": safe_get(row, "league_name"),
            "mulai": safe_get(row, "contract_start"),
            "berakhir": safe_get(row, "contract_expiry"),
            "status": safe_get(row, "loan_status"),
            "klausul_rilis": "",
            "gaji_per_musim": safe_get(row, "salary_per_year")
        },
        "transfer": [],
        "cedera": [],
        "statistik_performa": {
            "musim": safe_get(row, "season"),
            "main": safe_get(row, "appearances_overall"),
            "starter": "",
            "substitusi": "",
            "menit_bermain": "",
            "gol": safe_get(row, "goals_overall"),
            "assist": safe_get(row, "assists_overall"),
            "kuning": "",
            "merah": "",
            "clean_sheet": "",
            "kebobolan": "",
            "rasio_gol_per_menit": "",
            "kontribusi_gol_total": "",
            "per_posisi": {}
        },
        "karier_internasional": {
            "negara": safe_get(row, "citizenship"),
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
            "transfermarkt_profile_url": safe_get(row, "url"),
            "data_asal": "transfermarkt",
            "update_terakhir": safe_get(row, "last_market_value_update")
        }
    }

# ===== PROSES PEMAIN =====
count_players = 0
for row in reader:
    club = safe_get(row, "current_club").strip()
    league = safe_get(row, "league_name").strip()
    country = safe_get(row, "citizenship").strip()
    position = safe_get(row, "position").strip() or "Unknown Position"
    player = format_player(row)

    # Prioritas 1️⃣: Klub
    if club and club.lower() != "unknown":
        clubs[club].append(player)
    # Prioritas 2️⃣: Liga
    elif league and league.lower() != "unknown":
        leagues[league].append(player)
    # Prioritas 3️⃣: Negara
    elif country and country.lower() != "unknown":
        countries[country].append(player)
    # Prioritas 4️⃣: Posisi
    else:
        positions[position].append(player)

    count_players += 1

print(f"✅ Total pemain: {count_players}")
print(f"🏟️ Klub: {len(clubs)} | 🏆 Liga: {len(leagues)} | 🌍 Negara: {len(countries)} | ⚽ Posisi: {len(positions)}")

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
save_json_group("klub", clubs, os.path.join(OUTPUT_DIR, "clubs"))
save_json_group("liga", leagues, os.path.join(OUTPUT_DIR, "leagues"))
save_json_group("negara", countries, os.path.join(OUTPUT_DIR, "countries"))
save_json_group("posisi", positions, os.path.join(OUTPUT_DIR, "positions"))

print("🎉 Semua file selesai dibuat (struktur lengkap 10 panel, adaptif terhadap CSV apapun).")