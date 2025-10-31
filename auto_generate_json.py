#!/usr/bin/env python3
"""
auto_generate_json_v3.1_smart_mapper.py

- Scan datalake/transfermarkt/ for CSVs
- Auto-detect file type (players/clubs/competitions/injuries/other)
- Merge data into canonical 10-panel player structure
- Enrich players with club details from team_details if available
- Save modular JSON outputs (leagues, clubs, players, positions, countries, index, injuries, competitions)
- Matching strategy: player_id if available -> normalized exact name -> token overlap -> levenshtein ratio
"""

import os
import csv
import json
import math
from collections import defaultdict
from datetime import datetime
import re

# -----------------------
# CONFIG
# -----------------------
DATA_LAKE = "datalake/transfermarkt"
OUTPUT_DIR = "data_output"
MAX_PLAYERS_PER_FILE = 800
TIMESTAMP = datetime.utcnow().isoformat() + "Z"

# create output folders
os.makedirs(OUTPUT_DIR, exist_ok=True)
for sub in ("leagues", "clubs", "players", "positions", "countries", "index", "injuries", "competitions"):
    os.makedirs(os.path.join(OUTPUT_DIR, sub), exist_ok=True)

# -----------------------
# UTILITIES
# -----------------------
def read_csv(path):
    with open(path, newline='', encoding='utf-8') as fh:
        return list(csv.DictReader(fh))

def safe_get(d, k):
    return d.get(k, "") if d else ""

def normalize(s):
    if not s:
        return ""
    s = str(s).lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^\w\s]", "", s)  # remove punctuation
    return s

def tokens(s):
    return [t for t in normalize(s).split() if t]

def token_overlap_score(a, b):
    sa = set(tokens(a))
    sb = set(tokens(b))
    if not sa and not sb:
        return 0.0
    return len(sa & sb) / max(1, len(sa | sb))

def levenshtein_ratio(a, b):
    # simple Levenshtein distance ratio
    a = normalize(a); b = normalize(b)
    if a == b:
        return 1.0
    la, lb = len(a), len(b)
    if la == 0 or lb == 0:
        return 0.0
    # dp O(la*lb)
    dp = [[0]*(lb+1) for _ in range(la+1)]
    for i in range(la+1):
        dp[i][0] = i
    for j in range(lb+1):
        dp[0][j] = j
    for i in range(1, la+1):
        for j in range(1, lb+1):
            cost = 0 if a[i-1] == b[j-1] else 1
            dp[i][j] = min(dp[i-1][j]+1, dp[i][j-1]+1, dp[i-1][j-1]+cost)
    dist = dp[la][lb]
    ratio = 1 - dist / max(la, lb)
    return ratio

def best_name_match(name, candidates, token_thresh=0.6, lev_thresh=0.78):
    """
    candidates: iterable of (key, canonical_name)
    returns best_key or None
    """
    n = normalize(name)
    if not n:
        return None
    # exact normalized match
    for key, canon in candidates.items():
        if normalize(canon) == n:
            return key
    # token overlap
    best = None
    best_score = 0.0
    for key, canon in candidates.items():
        tscore = token_overlap_score(n, canon)
        if tscore > best_score:
            best_score = tscore
            best = key
    if best_score >= token_thresh:
        return best
    # levenshtein ratio fallback
    best = None
    best_ratio = 0.0
    for key, canon in candidates.items():
        lr = levenshtein_ratio(n, canon)
        if lr > best_ratio:
            best_ratio = lr
            best = key
    if best_ratio >= lev_thresh:
        return best
    return None

# -----------------------
# SCAN DATALAKE CSV FILES
# -----------------------
csv_files = []
for root, dirs, files in os.walk(DATA_LAKE):
    for f in files:
        if f.lower().endswith(".csv"):
            csv_files.append(os.path.join(root, f))

if not csv_files:
    print("⚠️ No CSV files found under", DATA_LAKE)
else:
    print(f"Found {len(csv_files)} CSV files.")

# -----------------------
# LOAD & CLASSIFY FILES
# -----------------------
# heuristics: check filename and headers for keywords
files_by_type = defaultdict(list)  # types: players, teams, competitions, injuries, unknown
for path in csv_files:
    name = os.path.basename(path).lower()
    try:
        with open(path, newline='', encoding='utf-8') as fh:
            reader = csv.reader(fh)
            headers = next(reader, [])
            headers_l = [h.lower() for h in headers]
    except Exception as e:
        headers = []
        headers_l = []
    typ = "unknown"
    if any(k in name for k in ("player", "players", "player_profiles")) or any("player" in h for h in headers_l):
        typ = "players"
    if any(k in name for k in ("club", "team_details", "teams", "club_profiles")) or any("club" in h or "team" in h for h in headers_l):
        # team files often include club_id, club_name
        # if both "player" and "club" present prefer club if filename indicates team_details
        if "team_details" in name or "club" in name or "team" in name:
            typ = "teams"
    if any(k in name for k in ("competition", "competitions", "team_competitions")) or any("competition" in h for h in headers_l):
        typ = "competitions"
    if any(k in name for k in ("injury", "injuries", "player_injuries")) or any("injury" in h for h in headers_l):
        typ = "injuries"
    # if headers suggest players strongly override teams
    if any("player_id" in h or "player_name" in h for h in headers_l) and "team" not in name:
        typ = "players"
    files_by_type[typ].append(path)

print("File classification:")
for k,v in files_by_type.items():
    print(" -", k, len(v))

# -----------------------
# LOAD DATASETS
# -----------------------
players_rows = []
teams_rows = []
competitions_rows = []
injuries_rows = []

for p in files_by_type.get("players", []):
    try:
        players_rows.extend(read_csv(p))
        print("Loaded players CSV:", p)
    except Exception as e:
        print("Failed to read", p, e)

for p in files_by_type.get("teams", []):
    try:
        teams_rows.extend(read_csv(p))
        print("Loaded teams CSV:", p)
    except Exception as e:
        print("Failed to read", p, e)

for p in files_by_type.get("competitions", []):
    try:
        competitions_rows.extend(read_csv(p))
        print("Loaded competitions CSV:", p)
    except Exception as e:
        print("Failed to read", p, e)

for p in files_by_type.get("injuries", []):
    try:
        injuries_rows.extend(read_csv(p))
        print("Loaded injuries CSV:", p)
    except Exception as e:
        print("Failed to read", p, e)

# -----------------------
# BUILD INDEX MAPS
# -----------------------
# clubs by id and name
clubs_by_id = {}
clubs_by_name = {}
for r in teams_rows:
    cid = r.get("club_id") or r.get("team_id") or r.get("id") or ""
    name = r.get("club_name") or r.get("team_name") or r.get("name") or ""
    if name:
        clubs_by_name[name.strip().lower()] = name.strip()
    if cid:
        clubs_by_id[cid] = r

# players by id & name (we'll create entries that will be enriched)
players_by_id = {}
players_by_name = {}  # name_lower -> canonical name or id
for r in players_rows:
    pid = r.get("player_id") or r.get("id") or ""
    pname = r.get("player_name") or r.get("name") or ""
    if pid:
        players_by_id[pid] = r
    if pname:
        players_by_name[pname.strip().lower()] = pname.strip()

# injuries map by player name (raw)
inj_by_player = defaultdict(list)
for r in injuries_rows:
    pname = r.get("player_name") or r.get("name") or ""
    if pname:
        inj_by_player[pname.strip().lower()].append(r)

# competitions map by club_id or club_name
comps_by_club = defaultdict(list)
for r in competitions_rows:
    # some files use club_id, some club_name
    cid = r.get("club_id") or r.get("team_id") or ""
    cname = r.get("club_name") or r.get("team_name") or ""
    key = cid or cname
    if key:
        comps_by_club[key.strip().lower()].append(r)

print("Indexes: clubs_by_name:", len(clubs_by_name), "players_by_id:", len(players_by_id), "players_by_name:", len(players_by_name))

# -----------------------
# CANONICAL PLAYER STRUCTURE (10 panels)
# -----------------------
def make_blank_player():
    return {
        "identitas_dasar": {
            "player_id": "",
            "nama_lengkap": "",
            "nama_panggilan": "",
            "tanggal_lahir": "",
            "usia": "",
            "tempat_lahir": {"kota": "", "negara": ""},
            "kewarganegaraan": [],
            "tinggi_badan_cm": "",
            "berat_badan_kg": "",
            "posisi_utama": "",
            "posisi_alternatif": [],
            "kaki_dominan": "",
            "status_pemain": "",
            "klub_saat_ini": "",
            "nomor_punggung": "",
            "tanggal_gabung": "",
            "kontrak_mulai": "",
            "kontrak_berakhir": "",
            "agen": "",
            "media_sosial": {}
        },
        "nilai_pasar": {
            "nilai_terkini_eur": "",
            "update_terakhir": "",
            "riwayat_nilai": [],
            "nilai_tertinggi_eur": ""
        },
        "kontrak": {
            "klub": "",
            "liga": "",
            "mulai": "",
            "berakhir": "",
            "status": "",
            "klausul_rilis": "",
            "gaji_per_musim": ""
        },
        "transfer": [],
        "cedera": [],
        "statistik_performa": [],
        "karier_internasional": {"negara": "", "levels": []},
        "prestasi": [],
        "taktikal": {"formasi": "", "posisi": "", "kontribusi_formasi": ""},
        "tren_karier": [],
        "sumber_data": {"origins": {}}
    }

# master players container (keyed by player_id if present else generated slug)
master_players = {}
name_to_master_key = {}  # normalized name -> master_key

def add_or_merge_player(row):
    # identify by id if present
    pid = row.get("player_id") or row.get("id") or ""
    pname = (row.get("player_name") or row.get("name") or "").strip()
    normalized = pname.lower().strip()
    key = None
    if pid and pid in master_players:
        key = pid
    elif pid and pid not in master_players:
        # create new by id key
        key = pid
        master_players[key] = make_blank_player()
        master_players[key]["identitas_dasar"]["player_id"] = pid
    else:
        # try name match in name_to_master_key
        if normalized in name_to_master_key:
            key = name_to_master_key[normalized]
        else:
            # try matching against existing names using heuristic
            match = best_name_match(pname, {k: v["identitas_dasar"].get("nama_lengkap","") for k,v in master_players.items()})
            if match:
                key = match
            else:
                # create new generated key (slug)
                slug = re.sub(r"[^\w]+", "_", normalized) or f"player_{len(master_players)+1}"
                # ensure unique
                i = 1
                base = slug
                while slug in master_players:
                    i += 1
                    slug = f"{base}_{i}"
                key = slug
                master_players[key] = make_blank_player()
                master_players[key]["identitas_dasar"]["player_id"] = pid or ""
    # now merge fields from row into master_players[key]
    target = master_players[key]
    # merge identitas_dasar common fields
    idd = target["identitas_dasar"]
    def set_if_present(k, dest, src_key=None):
        src_key = src_key or k
        v = row.get(src_key)
        if v is not None and v != "":
            dest[k] = v
    set_if_present("nama_lengkap", idd, "player_name")
    set_if_present("nama_panggilan", idd, "known_as")
    set_if_present("tanggal_lahir", idd, "date_of_birth")
    set_if_present("usia", idd, "age")
    set_if_present("tinggi_badan_cm", idd, "height")
    set_if_present("berat_badan_kg", idd, "weight")
    set_if_present("posisi_utama", idd, "position")
    set_if_present("kaki_dominan", idd, "foot")
    set_if_present("klub_saat_ini", idd, "current_club")
    set_if_present("nomor_punggung", idd, "shirt_number")
    set_if_present("kontrak_mulai", idd, "contract_start")
    set_if_present("kontrak_berakhir", idd, "contract_expiry")
    # nationality possibly comma separated
    nat = row.get("citizenship") or row.get("nationality")
    if nat:
        if isinstance(nat, list):
            idd["kewarganegaraan"] = nat
        else:
            idd["kewarganegaraan"] = [x.strip() for x in str(nat).split(",") if x.strip()]
    # nilai pasar
    mv = row.get("market_value_in_eur") or row.get("market_value")
    if mv:
        target["nilai_pasar"]["nilai_terkini_eur"] = mv
    lu = row.get("last_market_value_update")
    if lu:
        target["nilai_pasar"]["update_terakhir"] = lu
    # injuries: merged later by name/id
    # store origin raw row for traceability
    orig = target["sumber_data"].setdefault("origins", {})
    srcname = row.get("_source_file") or row.get("source_file") or "unknown"
    orig.setdefault(srcname, []).append({k:v for k,v in row.items() if v not in (None,"")})
    # maintain normalized name mapping
    if target["identitas_dasar"]["nama_lengkap"]:
        name_to_master_key[normalize(target["identitas_dasar"]["nama_lengkap"])] = key
    return key

# -----------------------
# INITIAL LOAD: create master entries from player rows
# -----------------------
for r in players_rows:
    # annotate source
    r["_source_file"] = r.get("_source_file") or "player_profiles"
    add_or_merge_player(r)

print("Initial master players created:", len(master_players))

# -----------------------
# ENRICH FROM TEAMS (team_details)
# -----------------------
# Build club canonical map (by id or name)
club_canon = {}
for r in teams_rows:
    cid = r.get("club_id") or r.get("id") or ""
    name = r.get("club_name") or r.get("team_name") or ""
    logo = r.get("logo_url") or r.get("crest_url") or ""
    country = r.get("country_name") or ""
    comp = r.get("competition_name") or ""
    rec = {"club_id": cid, "club_slug": r.get("club_slug",""), "club_name": name, "logo_url": logo, "country_name": country, "competition_name": comp, "raw": r}
    if cid:
        club_canon[cid] = rec
    if name:
        club_canon[name.strip().lower()] = rec

# Add club info into players if possible
for key, p in list(master_players.items()):
    club_name = p["identitas_dasar"].get("klub_saat_ini","") or p["kontrak"].get("klub","")
    if club_name:
        # try id or name
        club_key = None
        # try exact name key in club_canon
        if club_name.strip().lower() in club_canon:
            club_key = club_name.strip().lower()
            club_info = club_canon[club_key]
        else:
            # try token/lev match against club_canon names
            club_key = best_name_match(club_name, {k: v.get("club_name","") for k,v in club_canon.items()})
            club_info = club_canon.get(club_key) if club_key else None
        if club_info:
            # attach club details under sumber_data.club_info
            p["sumber_data"].setdefault("club_info", {}).update({
                "club_id": club_info.get("club_id"),
                "club_name": club_info.get("club_name"),
                "club_slug": club_info.get("club_slug"),
                "logo_url": club_info.get("logo_url"),
                "country_name": club_info.get("country_name"),
                "competition_name": club_info.get("competition_name")
            })

# -----------------------
# MERGE INJURIES (match by player_id or name heuristics)
# -----------------------
# Build candidate name map from master_players
candidates = {k: v["identitas_dasar"].get("nama_lengkap","") for k,v in master_players.items()}
for inj in injuries_rows:
    pid = inj.get("player_id") or ""
    pname = (inj.get("player_name") or inj.get("name") or "").strip()
    target_key = None
    if pid and pid in master_players:
        target_key = pid
    elif pname:
        # try normalized exact
        if normalize(pname) in name_to_master_key:
            target_key = name_to_master_key[normalize(pname)]
        else:
            # fuzzy match
            maybe = best_name_match(pname, candidates, token_thresh=0.6, lev_thresh=0.78)
            if maybe:
                target_key = maybe
    if target_key:
        master_players[target_key]["cedera"].append({
            "injury": inj.get("injury", inj.get("type", "")),
            "start": inj.get("injury_start", inj.get("start_date", "")),
            "end": inj.get("injury_end", inj.get("end_date", "")),
            "days": inj.get("days", ""),
            "games_missed": inj.get("games_missed", inj.get("matches_missed", "")),
            "club": inj.get("club", inj.get("team", "")),
            "source": inj.get("url", "")
        })
    else:
        # if no match, save to loose injuries file (we'll also write injuries.json global)
        pass

# -----------------------
# MERGE COMPETITIONS INTO CLUBS (by club_id or name)
# -----------------------
# We'll create a clubs data container enriched by competitions, team_details
clubs_master = {}
for r in teams_rows:
    cid = r.get("club_id") or r.get("id") or ""
    name = r.get("club_name") or r.get("team_name") or ""
    key = cid or name.strip().lower() or name
    clubs_master[key] = {
        "club_id": cid,
        "club_name": name,
        "club_slug": r.get("club_slug") or "",
        "logo_url": r.get("logo_url") or r.get("crest_url") or "",
        "country_name": r.get("country_name") or "",
        "website": r.get("source_url") or r.get("website") or "",
        "competitions": [],
        "raw": r
    }
# add competitions rows to clubs_master
for r in competitions_rows:
    # prefer club_id if present
    cid = (r.get("club_id") or r.get("team_id") or "").strip()
    cname = (r.get("club_name") or r.get("team_name") or "").strip()
    if cid and cid in clubs_master:
        clubs_master[cid]["competitions"].append(r)
    else:
        # try by name match
        matched = None
        for k,v in clubs_master.items():
            if v.get("club_name") and normalize(v["club_name"]) == normalize(cname):
                matched = k; break
        if matched:
            clubs_master[matched]["competitions"].append(r)
        else:
            # create new club entry from competition if not exist
            k = cid or cname.lower().replace(" ", "_")
            clubs_master.setdefault(k, {"club_id": cid, "club_name": cname, "club_slug": "", "logo_url":"", "country_name": r.get("country_name",""), "competitions":[r], "raw":{}})

# -----------------------
# FINALIZE: save master players, clubs, indices and per-group JSONs
# -----------------------
# Save each player as-is into data_output/players (could be heavy; we save grouped later)
all_players_list = []
for k,p in master_players.items():
    # ensure sumber_data.origins exist
    p["sumber_data"].setdefault("origins", {})
    all_players_list.append(p)

with open(os.path.join(OUTPUT_DIR, "players", "master_players.json"), "w", encoding="utf-8") as fh:
    json.dump(all_players_list, fh, ensure_ascii=False, indent=2)
print("💾 Saved master_players.json (players count):", len(all_players_list))

# create grouping by league / country / position / clubs
leagues = defaultdict(list)
countries = defaultdict(list)
positions = defaultdict(list)
clubs_out = defaultdict(list)

for p in all_players_list:
    league = p["kontrak"].get("liga") or p["sumber_data"].get("club_info", {}).get("competition_name","") or ""
    country = p["karier_internasional"].get("negara") or (p["sumber_data"].get("club_info",{}).get("country_name",""))
    pos = p["identitas_dasar"].get("posisi_utama") or "Unknown"
    clubname = p["identitas_dasar"].get("klub_saat_ini") or p["kontrak"].get("klub") or (p["sumber_data"].get("club_info",{}).get("club_name","Unknown Club"))
    if league:
        leagues[league].append(p)
    if country:
        countries[country].append(p)
    positions[pos].append(p)
    clubs_out[clubname].append(p)

def save_grouped(group_dict, folder, kind):
    os.makedirs(folder, exist_ok=True)
    idx = []
    for gname, items in group_dict.items():
        safe = re.sub(r"[^\w\-]+", "_", gname.strip().lower()) or "unknown"
        # split if too large
        for i in range(0, len(items), MAX_PLAYERS_PER_FILE):
            subset = items[i:i+MAX_PLAYERS_PER_FILE]
            part = i // MAX_PLAYERS_PER_FILE + 1
            filename = f"{safe}.json" if len(items) <= MAX_PLAYERS_PER_FILE else f"{safe}_part{part}.json"
            payload = {"kategori": kind, "nama_group": gname, "total": len(subset), "data": subset, "generated_at": TIMESTAMP}
            path = os.path.join(folder, filename)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(payload, fh, ensure_ascii=False, indent=2)
            idx.append({"group": gname, "file": filename, "count": len(subset)})
            print(f"💾 Saved {kind}/{filename} ({len(subset)})")
    return idx

# save group files and build index
index = {"generated_at": TIMESTAMP, "leagues": [], "countries": [], "positions": [], "clubs": [], "files": []}

index["leagues"] = save_grouped(leagues, os.path.join(OUTPUT_DIR, "leagues"), "league")
index["countries"] = save_grouped(countries, os.path.join(OUTPUT_DIR, "countries"), "country")
index["positions"] = save_grouped(positions, os.path.join(OUTPUT_DIR, "positions"), "position")
index["clubs"] = save_grouped(clubs_out, os.path.join(OUTPUT_DIR, "clubs"), "club")

# save clubs master details & competitions & injuries full dumps
with open(os.path.join(OUTPUT_DIR, "clubs", "clubs_master.json"), "w", encoding="utf-8") as fh:
    json.dump(list(clubs_master.values()), fh, ensure_ascii=False, indent=2)
print("💾 Saved clubs_master.json")

with open(os.path.join(OUTPUT_DIR, "competitions.json"), "w", encoding="utf-8") as fh:
    json.dump(competitions_rows, fh, ensure_ascii=False, indent=2)
print("💾 Saved competitions.json")

with open(os.path.join(OUTPUT_DIR, "injuries.json"), "w", encoding="utf-8") as fh:
    json.dump(injuries_rows, fh, ensure_ascii=False, indent=2)
print("💾 Saved injuries.json")

# write index
with open(os.path.join(OUTPUT_DIR, "index", "index.json"), "w", encoding="utf-8") as fh:
    json.dump(index, fh, ensure_ascii=False, indent=2)
print("🗂️ Index written to data_output/index/index.json")

print("🎉 v3.1 Smart mapper finished. Outputs in", OUTPUT_DIR)