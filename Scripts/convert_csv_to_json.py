import csv, json, os

CSV_FILE = "data/players.csv"
OUT_FOLDER = "players"

os.makedirs(OUT_FOLDER, exist_ok=True)

with open(CSV_FILE, encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for row in reader:
        player_id = row.get("player_id") or row.get("id") or "unknown"
        out_path = os.path.join(OUT_FOLDER, f"{player_id}.json")
        with open(out_path, "w", encoding="utf-8") as jf:
            json.dump(row, jf, indent=2, ensure_ascii=False)
        print(f"✅ Converted {player_id}")
