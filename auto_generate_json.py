import csv, json, os

output_folder = "data_output"
os.makedirs(output_folder, exist_ok=True)

def find_csv_files(base_folder="."):
    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.endswith(".csv"):
                yield os.path.join(root, file)

for csv_path in find_csv_files("datalake/transfermarkt/player_profiles"):
    print(f"📂 Membaca {csv_path}")
    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            club = row.get("current_club_name", "Unknown").replace(" ", "_") or "Unknown"
            club_dir = os.path.join(output_folder, club)
            os.makedirs(club_dir, exist_ok=True)
            
            name = row.get("player_name", "Unknown").replace(" ", "_")
            json_path = os.path.join(club_dir, f"{name}.json")
            
            with open(json_path, "w", encoding="utf-8") as jf:
                json.dump(row, jf, indent=2, ensure_ascii=False)

print("✅ Semua pemain berhasil dipisahkan ke JSON.")
