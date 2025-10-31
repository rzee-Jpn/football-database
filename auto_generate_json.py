import os
import csv
import json
from collections import defaultdict
from rapidfuzz import process  # pip install rapidfuzz

# ===== CONFIG =====
DATA_DIR = "datalake"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800  # batas aman per file

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===== STRUKTUR DATA =====
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)
clubs = defaultdict(list)
players_index = {}  # key: player_name, value: JSON object

# ===== BACA SEMUA FILE CSV DI DALAM FOLDER DATALAKE =====
def read_csv(file_path):
    with open(file_path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)

# ===== FORMAT PLAYER SESUAI 10 PANEL =====
def format_player(row):
    name = row.get("player_name", "")
    player = {
        "identitas_dasar": {
            "nama_lengkap": name,
            "nama_panggilan": name,
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
    return player

# ===== MERGE DATA DARI FILE BARU =====
def merge_player(player):
    name = player["identitas_dasar"]["nama_lengkap"]
    # fuzzy matching untuk nama pemain yang hampir sama
    existing_names = list(players_index.keys())
    match = process.extractOne(name, existing_names, score_cutoff=90)
    if match:
        existing = players_index[match[0]]
        # update semua field panel
        for k, v in player.items():
            if isinstance(v, dict):
                existing[k].update(v)
            elif isinstance(v, list):
                existing[k].extend(v)
            else:
                existing[k] = v
        return existing
    else:
        players_index[name] = player
        return player

# ===== PROSES SEMUA FILE DI DATALAKE =====
for file in os.listdir(DATA_DIR):
    if file.endswith(".csv"):
        path = os.path.join(DATA_DIR, file)
        rows = read_csv(path)
        for row in rows:
            player = format_player(row)
            player = merge_player(player)

            # Kelompok prioritas
            league = row.get("league_name", "").strip() or None
            country = row.get("citizenship", "").strip() or None
            position = row.get("position", "").strip() or "Unknown Position"
            club = row.get("current_club", "").strip() or "Unknown Club"

            if league and league.lower() != "unknown":
                leagues[league].append(player)
            elif country and country.lower() != "unknown":
                countries[country].append(player)
            else:
                positions[position].append(player)
            clubs[club].append(player)

# ===== SIMPAN JSON =====
def save_group(name, group_data, folder):
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
            filename = f"{safe_name}_part{part}.json" if len(players) > MAX_PLAYERS_PER_FILE else f"{safe_name}.json"
            path = os.path.join(folder, filename)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"💾 Saved {name}/{filename} ({len(subset)} pemain)")

save_group("liga", leagues, os.path.join(OUTPUT_DIR, "leagues"))
save_group("negara", countries, os.path.join(OUTPUT_DIR, "countries"))
save_group("posisi", positions, os.path.join(OUTPUT_DIR, "positions"))
save_group("klub", clubs, os.path.join(OUTPUT_DIR, "clubs"))

print("🎉 Semua file selesai dibuat. Struktur 10 panel lengkap, siap auto-merge untuk file baru.")