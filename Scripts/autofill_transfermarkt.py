import os, json, requests, shutil
from bs4 import BeautifulSoup

def get_transfermarkt_data(player_id):
    url = f"https://www.transfermarkt.com/-/profil/spieler/{player_id}"
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)
    if r.status_code != 200:
        print(f"⚠️ Player {player_id} not found ({r.status_code})")
        return None

    soup = BeautifulSoup(r.text, "html.parser")

    data = {}
    name = soup.find("h1", itemprop="name")
    if name:
        data["player_name"] = name.text.strip()

    dob = soup.find("span", itemprop="birthDate")
    if dob:
        data["date_of_birth"] = dob.text.strip()

    pob = soup.find("span", itemprop="birthPlace")
    if pob:
        data["place_of_birth"] = pob.text.strip()

    club = soup.find("a", href=lambda x: x and "/verein/" in x)
    if club:
        data["current_club_name"] = club.text.strip()
    else:
        data["current_club_name"] = "Without Club"

    img = soup.select_one("img[data-src]")
    if img:
        data["player_image_url"] = img["data-src"]

    return data

def save_to_club_folder(player, player_id):
    base_folder = "players"
    club = player.get("current_club_name", "Unknown Club").replace("/", "-").strip()
    club_folder = os.path.join(base_folder, club)
    os.makedirs(club_folder, exist_ok=True)

    out_path = os.path.join(club_folder, f"{player_id}.json")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(player, f, indent=2, ensure_ascii=False)

    print(f"✅ Saved {player.get('player_name', 'Unknown')} → {club_folder}/")

players_dir = "players"

for root, _, files in os.walk(players_dir):
    for file in files:
        if not file.endswith(".json"):
            continue

        path = os.path.join(root, file)
        with open(path, encoding="utf-8") as f:
            player = json.load(f)

        pid = player.get("player_id") or os.path.splitext(file)[0]
        tm_data = get_transfermarkt_data(pid)
        if not tm_data:
            continue

        player.update(tm_data)
        save_to_club_folder(player, pid)

        new_folder = os.path.join(players_dir, player["current_club_name"])
        if root != new_folder and os.path.exists(path):
            os.remove(path)
