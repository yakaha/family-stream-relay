#!/usr/bin/env python3
"""
Family Stream - Vérificateur & Purge Auto des Portails Xtream
==============================================================
S'exécute via GitHub Actions (cron 6h ou manuel) ou en local :
1. Charge les portails existants (xtream_portals.json) et les sources autorisées
   fournies par l'administrateur (variable d'env AUTHORIZED_PORTALS ou fichiers
   locaux authorized_portals.txt / authorized_portals.json).
2. Rejette immédiatement tout identifiant corrompu (&password=, entités HTML, champs vides).
3. Teste en parallèle : authentification Xtream, expiration, catégories sportives
   et lecture réelle d'un flux vidéo MPEG-TS (.ts).
4. Purge les serveurs morts, expirés ou sans flux vidéo, en protégeant la liste
   existante contre les erreurs réseau transitoires.
5. Pré-package toutes les catégories sportives (AR & FR en tête) sans troncature
   des chaînes et sauvegarde en JSON compact.
"""

import html as html_module
import json
import os
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

PORTALS_FILE = "xtream_portals.json"
AUTHORIZED_TXT_FILE = "authorized_portals.txt"
AUTHORIZED_JSON_FILE = "authorized_portals.json"
MAX_WORKERS = 24
HTTP_TIMEOUT = 6.0
STREAM_TIMEOUT = 6.0

SPORT_POSITIVE = [
    "SPORT", "BEIN", "CANAL", "FOOT", "RMC", "DAZN", "EUROSPORT",
    "LALIGA", "CALCIO", "SKY SPORT", "TNT SPORT", "LIGUE 1", "MATCH", "TOD", "SSC"
]

NEGATIVE_KEYWORDS = [
    "CINEMA", "FILMS", "MOVIES", "SERIES", "ADULT", "XXX", "KIDS",
    "ANIMATION", "DOCUMENTAIRE", "INFO", "NEWS", "MUSIC", "RADIO", "VOD",
    "ENTERTAINMENT"
]

STREAM_USER_AGENTS = [
    "VLC/3.0.18 LibVLC/3.0.18",
    "IPTVSmartersPro",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
]


def is_sport(cname: str) -> bool:
    up = cname.upper()
    for neg in NEGATIVE_KEYWORDS:
        if neg in up and not any(pos in up for pos in ["SPORT", "FOOT", "CANAL+ SPORT"]):
            return False
    return any(pos in up for pos in SPORT_POSITIVE)


def is_garbage_channel(name: str) -> bool:
    s = name.strip()
    if not s or len(s) < 2:
        return True
    return s.startswith(("###", "---", "===", "***"))


def category_priority(cname: str):
    up = cname.upper()
    if ("AR" in up or "QATAR" in up or "ARAB" in up) and ("BEIN" in up or "TOD" in up):
        return (1, cname)
    if ("FR" in up or "FRANCE" in up) and ("BEIN" in up):
        return (2, cname)
    if ("FR" in up or "FRANCE" in up) and ("CANAL" in up or "FOOT" in up or "LIGUE 1" in up):
        return (3, cname)
    if ("FR" in up or "FRANCE" in up) and ("RMC" in up or "DAZN" in up or "EUROSPORT" in up):
        return (4, cname)
    if ("AR" in up or "ARAB" in up) and ("SSC" in up or "SPORT" in up):
        return (5, cname)
    if "FR" in up or "FRANCE" in up:
        return (6, cname)
    if "AR" in up or "ARAB" in up:
        return (7, cname)
    return (10, cname)


def has_valid_credentials(host: str, username: str, password: str) -> bool:
    if not host or not username or not password:
        return False
    if not host.startswith(("http://", "https://")):
        return False
    for val in (username, password):
        low = val.lower()
        if "&" in val or "password=" in low or "username=" in low or "type=" in low or " " in val:
            return False
    return len(username) >= 2 and len(password) >= 2


def parse_get_php_url(raw_url: str) -> dict | None:
    """Extrait host/username/password d'une URL get.php fournie par l'administrateur."""
    url = html_module.unescape(raw_url.strip())
    try:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        h = f"{parsed.scheme}://{parsed.netloc}".rstrip("/")
        u = (params.get("username") or params.get("user") or [None])[0]
        p = (params.get("password") or params.get("pass") or [None])[0]
        if h and u and p:
            u, p = u.strip(), p.strip()
            if has_valid_credentials(h, u, p):
                return {"host": h, "username": u, "password": p}
    except Exception:
        pass
    return None


def parse_authorized_text(content: str) -> list[dict]:
    """Parse une liste autorisée (URLs get.php, format host|user|pass ou blocs HOST/USER/PASS)."""
    results = []
    if not content:
        return results

    for raw_url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', content, re.I):
        cred = parse_get_php_url(raw_url)
        if cred:
            results.append(cred)

    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line and line.startswith(("http://", "https://")):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 3 and has_valid_credentials(parts[0].rstrip("/"), parts[1], parts[2]):
                results.append({"host": parts[0].rstrip("/"), "username": parts[1], "password": parts[2]})

    for block in re.split(r"━{4,}|={4,}|\n\s*\n", content):
        h, u, p = None, None, None
        for line in block.splitlines():
            line = line.strip()
            mh = re.search(r"(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)", line, re.I)
            if mh:
                h = mh.group(1).rstrip("/")
            mu = re.search(r"(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)", line, re.I)
            if mu:
                u = mu.group(1).strip()
            mp = re.search(r"(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)", line, re.I)
            if mp:
                p = mp.group(1).strip()
        if h and u and p and has_valid_credentials(h, u, p):
            results.append({"host": h, "username": u, "password": p})

    return results


def load_authorized_inputs() -> list[dict]:
    """Charge les serveurs fournis explicitement par l'admin (env AUTHORIZED_PORTALS ou fichiers locaux)."""
    candidates = []

    env_val = os.environ.get("AUTHORIZED_PORTALS", "").strip()
    if env_val:
        if env_val.startswith("["):
            try:
                items = json.loads(env_val)
                if isinstance(items, list):
                    for it in items:
                        if isinstance(it, dict):
                            h = str(it.get("host") or it.get("url") or "").rstrip("/")
                            u = str(it.get("username") or "").strip()
                            p = str(it.get("password") or "").strip()
                            if has_valid_credentials(h, u, p):
                                candidates.append({"host": h, "username": u, "password": p})
            except Exception as e:
                print(f"   ⚠️ AUTHORIZED_PORTALS JSON invalide: {e}", flush=True)
        else:
            candidates.extend(parse_authorized_text(env_val))

    if os.path.exists(AUTHORIZED_TXT_FILE):
        with open(AUTHORIZED_TXT_FILE, "r", encoding="utf-8", errors="ignore") as f:
            candidates.extend(parse_authorized_text(f.read()))

    if os.path.exists(AUTHORIZED_JSON_FILE):
        try:
            with open(AUTHORIZED_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, list):
                for it in data:
                    if isinstance(it, dict):
                        h = str(it.get("host") or it.get("url") or "").rstrip("/")
                        u = str(it.get("username") or "").strip()
                        p = str(it.get("password") or "").strip()
                        if has_valid_credentials(h, u, p):
                            candidates.append({"host": h, "username": u, "password": p})
        except Exception as e:
            print(f"   ⚠️ Erreur lecture {AUTHORIZED_JSON_FILE}: {e}", flush=True)

    return candidates


def verify_live_mpegts(stream_url: str) -> bool:
    """Vérifie qu'un flux .ts délivre réellement au moins 1 Ko de données vidéo."""
    for ua in STREAM_USER_AGENTS[:2]:
        try:
            with requests.get(
                stream_url,
                timeout=STREAM_TIMEOUT,
                headers={"User-Agent": ua},
                stream=True,
                verify=False,
            ) as resp:
                if resp.status_code != 200:
                    continue
                bytes_received = 0
                for chunk in resp.iter_content(chunk_size=4096):
                    if chunk:
                        bytes_received += len(chunk)
                        if bytes_received >= 2048:
                            break
                if bytes_received >= 1024:
                    return True
        except Exception:
            continue
    return False


def certify_and_extract_server(server: dict) -> dict | None:
    h = str(server.get("host", "")).rstrip("/")
    u = str(server.get("username", "")).strip()
    p = str(server.get("password", "")).strip()

    if not has_valid_credentials(h, u, p):
        return None

    clean_host = h.replace("http://", "").replace("https://", "").split(":")[0]
    if any(x in clean_host.lower() for x in ["adult", "porn", "xxx", "myultratv"]):
        return None

    base = f"{h}/player_api.php?username={urllib.parse.quote(u)}&password={urllib.parse.quote(p)}"
    headers = {"User-Agent": STREAM_USER_AGENTS[0]}

    try:
        # 1. Auth & expiration check
        rinfo = requests.get(base, timeout=HTTP_TIMEOUT, headers=headers, verify=False)
        if rinfo.status_code != 200:
            return None
        d = rinfo.json()
        if not isinstance(d, dict):
            return None
        uinfo = d.get("user_info", {})
        if not isinstance(uinfo, dict) or uinfo.get("auth") != 1 or str(uinfo.get("status", "")).lower() != "active":
            return None
        exp = uinfo.get("exp_date")
        if exp:
            try:
                if int(float(exp)) < int(time.time()):
                    return None
            except Exception:
                pass

        # 2. Sport categories check
        rcat = requests.get(f"{base}&action=get_live_categories", timeout=HTTP_TIMEOUT, headers=headers, verify=False)
        if rcat.status_code != 200:
            return None
        all_cats = rcat.json()
        if not isinstance(all_cats, list) or not all_cats:
            return None
        sport_cats = {}
        for c in all_cats:
            cid = str(c.get("category_id", "")).strip()
            cname = str(c.get("category_name", "")).strip()
            if cid and cname and is_sport(cname):
                sport_cats[cid] = cname
        if not sport_cats:
            return None

        # 3. Live streams check
        rstr = requests.get(f"{base}&action=get_live_streams", timeout=HTTP_TIMEOUT + 2.0, headers=headers, verify=False)
        if rstr.status_code != 200:
            return None
        all_streams = rstr.json()
        if not isinstance(all_streams, list) or not all_streams:
            return None

        cat_to_channels = {cid: [] for cid in sport_cats.keys()}
        sample_stream_ids = []

        for stream in all_streams:
            cid = str(stream.get("category_id", "")).strip()
            if cid not in cat_to_channels:
                continue
            sid = str(stream.get("stream_id", "")).strip()
            sname = str(stream.get("name", "")).strip()
            if not sid or is_garbage_channel(sname):
                continue
            if len(sample_stream_ids) < 2:
                sample_stream_ids.append(sid)
            cat_to_channels[cid].append({"id": sid, "name": sname})

        if not sample_stream_ids:
            return None

        # 4. Real video MPEG-TS test (on up to 2 sample channels in case 1st is temporarily off)
        video_ok = any(verify_live_mpegts(f"{h}/live/{u}/{p}/{sid}.ts") for sid in sample_stream_ids)
        if not video_ok:
            return None

        # 5. Build complete sport categories sorted with AR & FR on top (no channel truncation!)
        sorted_cids = sorted(sport_cats.keys(), key=lambda cid: category_priority(sport_cats[cid]))
        built_categories = []
        total_sport_ch = 0
        for cid in sorted_cids:
            chs = cat_to_channels.get(cid, [])
            if chs:
                built_categories.append({
                    "category_id": cid,
                    "name": sport_cats[cid],
                    "channels": chs,
                })
                total_sport_ch += len(chs)

        if not built_categories:
            return None

        max_conn = 1
        try:
            max_conn = max(1, int(uinfo.get("max_connections", 1) or 1))
        except Exception:
            max_conn = 1

        return {
            "host": h,
            "username": u,
            "password": p,
            "status": "Active",
            "expires_str": "Actif (Certifié Vidéo)",
            "max_connections": max_conn,
            "total_categories": len(built_categories),
            "total_sport_channels": total_sport_ch,
            "categories": built_categories,
        }
    except Exception:
        return None


def main():
    print("=== DÉMARRAGE DU VÉRIFICATEUR VIDÉO & PURGE AUTO ===", flush=True)
    t_start = time.time()

    current_portals = []
    if os.path.exists(PORTALS_FILE):
        try:
            with open(PORTALS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                current_portals = loaded
            print(f"[1/4] Serveurs actuellement enregistrés : {len(current_portals)}", flush=True)
        except Exception as e:
            print(f"Erreur lecture {PORTALS_FILE}: {e}", flush=True)

    existing_added_at = {}
    candidate_pool = []
    seen = set()

    for portal in current_portals:
        h = str(portal.get("host", "")).rstrip("/")
        u = str(portal.get("username", "")).strip()
        p_val = str(portal.get("password", "")).strip()
        if not has_valid_credentials(h, u, p_val):
            print(f"   [PURGÉ CORROMPU] {h} user={u[:30]}", flush=True)
            continue
        k = (h.lower(), u)
        if portal.get("added_at") and str(portal.get("added_at")) != "None":
            existing_added_at[k] = portal["added_at"]
        if k not in seen:
            seen.add(k)
            candidate_pool.append({"host": h, "username": u, "password": p_val})

    print("[2/4] Chargement des sources autorisées supplémentaires...", flush=True)
    auth_cands = load_authorized_inputs()
    auth_new = 0
    for c in auth_cands:
        k = (c["host"].rstrip("/").lower(), c["username"])
        if k not in seen:
            seen.add(k)
            candidate_pool.append(c)
            auth_new += 1
    print(f"   → {auth_new} nouveau(x) candidat(s) autorisé(s) • Total à vérifier: {len(candidate_pool)}", flush=True)

    if not candidate_pool:
        print("Aucun candidat à vérifier — fichier conservé tel quel.", flush=True)
        return

    print(f"[3/4] Certification vidéo en direct (test réel MPEG-TS, max {MAX_WORKERS} threads)...", flush=True)
    raw_certified = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(certify_and_extract_server, s): s for s in candidate_pool}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                raw_certified.append(res)

    # Prioriser les serveurs multi-connexions et les plus riches AVANT la limite de 2 par domaine
    raw_certified.sort(
        key=lambda s: (s["max_connections"], s["total_sport_channels"]),
        reverse=True,
    )

    certified = []
    seen_hosts = {}
    for res in raw_certified:
        clean_host = res["host"].replace("http://", "").replace("https://", "").split(":")[0].lower()
        host_count = seen_hosts.get(clean_host, 0)
        if host_count < 2:
            seen_hosts[clean_host] = host_count + 1
            certified.append(res)
            print(
                f"   [VÉRIFIÉ OK] {clean_host} ({res['total_sport_channels']} chaînes • max {res['max_connections']})",
                flush=True,
            )

    # Protection anti-effacement accidentel sur incident réseau global
    if not certified and len(current_portals) > 0:
        print(
            "⚠️ 0 serveur vérifié sur ce run alors que la liste contenait déjà des entrées — "
            "sauvegarde ignorée pour protéger xtream_portals.json contre un incident réseau.",
            flush=True,
        )
        sys.exit(1)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    final_list = []
    for idx, s in enumerate(certified, 1):
        clean_host = s["host"].replace("http://", "").replace("https://", "").split(":")[0]
        key = (s["host"].rstrip("/").lower(), s["username"])
        added_at = existing_added_at.get(key) or now_str
        final_list.append({
            "id": f"srv_{idx}",
            "name": f"Serveur #{idx} : {clean_host}",
            "host": s["host"],
            "username": s["username"],
            "password": s["password"],
            "status": s["status"],
            "expires_str": s["expires_str"],
            "added_at": added_at,
            "max_connections": s["max_connections"],
            "total_categories": s["total_categories"],
            "total_sport_channels": s["total_sport_channels"],
            "categories": s["categories"],
        })

    print(f"[4/4] Sauvegarde compacte dans {PORTALS_FILE}...", flush=True)
    with open(PORTALS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    assets_file = os.path.join("app", "src", "main", "assets", "xtream_portals.json")
    if os.path.exists(os.path.dirname(assets_file)):
        with open(assets_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    elapsed = time.time() - t_start
    size_kb = os.path.getsize(PORTALS_FILE) / 1024
    print(
        f"=== TERMINÉ EN {elapsed:.1f}s : {len(final_list)} SERVEURS CERTIFIÉS VIDÉO | Fichier: {size_kb:.0f} KB ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
