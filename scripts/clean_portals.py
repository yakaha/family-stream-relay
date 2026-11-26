"""
Script de nettoyage et compactage de xtream_portals.json :
1. Supprime les entrées avec des credentials corrompus (username/password contenant &, password=, type=, etc.)
2. Supprime les fausses chaînes séparateurs (#####..., ---, ===, ***) sans tronquer les vraies chaînes
3. Corrige added_at si manquant ou "None" au format standard UTC "%Y-%m-%d %H:%M"
4. Réécrit en JSON compact (sans indent) dans xtream_portals.json et app/src/main/assets/xtream_portals.json
"""
import json
import os
from datetime import datetime, timezone

BASE_DIR = r"D:\alone\Family Stream final 2"
PORTALS_FILE = os.path.join(BASE_DIR, "xtream_portals.json")
ASSETS_FILE = os.path.join(BASE_DIR, "app", "src", "main", "assets", "xtream_portals.json")
NOW = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")


def clean_portals(path: str):
    if not os.path.exists(path):
        print(f"Absent: {path}")
        return None
    size_before = os.path.getsize(path)
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    cleaned = []
    removed = 0
    for idx, entry in enumerate(data):
        h = str(entry.get("host", "")).rstrip("/")
        u = str(entry.get("username", "")).strip()
        p = str(entry.get("password", "")).strip()
        if not h or not u or not p or "&" in u or "&" in p or "password=" in u.lower() or "type=" in u.lower():
            removed += 1
            continue

        new_cats = []
        total_sport = 0
        for cat in entry.get("categories", []):
            chs = []
            for ch in cat.get("channels", []):
                name = str(ch.get("name", "")).strip()
                if not name or name.startswith(("###", "---", "===", "***")):
                    continue
                item = {"id": str(ch.get("id", "")).strip(), "name": name}
                if ch.get("icon"):
                    item["icon"] = ch["icon"]
                chs.append(item)
            if chs:
                new_cats.append({
                    "category_id": str(cat.get("category_id", "")),
                    "name": str(cat.get("name", "")),
                    "channels": chs,
                })
                total_sport += len(chs)

        if not new_cats:
            removed += 1
            continue

        added_at = entry.get("added_at")
        if not added_at or str(added_at) == "None":
            added_at = NOW

        clean_host = h.replace("http://", "").replace("https://", "").split(":")[0]
        new_id = f"srv_{len(cleaned) + 1}"
        cleaned.append({
            "id": new_id,
            "name": f"Serveur #{len(cleaned) + 1} : {clean_host}",
            "host": h,
            "username": u,
            "password": p,
            "status": entry.get("status") or "Active",
            "expires_str": entry.get("expires_str") or "Actif (Certifié Vidéo)",
            "added_at": added_at,
            "max_connections": max(1, int(entry.get("max_connections") or 1)),
            "total_categories": len(new_cats),
            "total_sport_channels": total_sport,
            "categories": new_cats,
        })

    with open(path, "w", encoding="utf-8") as f:
        json.dump(cleaned, f, ensure_ascii=False, separators=(",", ":"))
    size_after = os.path.getsize(path)
    print(f"{os.path.relpath(path, BASE_DIR)}: {len(data)} -> {len(cleaned)} valides ({removed} supprimées) | {size_before/1024/1024:.2f} MB -> {size_after/1024/1024:.2f} MB")
    return cleaned


if __name__ == "__main__":
    result = clean_portals(PORTALS_FILE)
    if result is not None and os.path.exists(os.path.dirname(ASSETS_FILE)):
        with open(ASSETS_FILE, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, separators=(",", ":"))
        print(f"Assets synchronisé: {os.path.getsize(ASSETS_FILE)/1024/1024:.2f} MB")
