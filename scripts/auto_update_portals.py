#!/usr/bin/env python3
"""
Family Stream - Auto-Updater & Health Checker (Xtream Portals)
==============================================================
S'exécute automatiquement via GitHub Actions (cron toutes les 6h ou manuel) :
1. Teste tous les serveurs existants dans xtream_portals.json en parallèle (timeout 3.5s).
2. ÉLIMINE immédiatement les serveurs morts, inaccessibles ou expirés.
3. Scrape Telegram public pour découvrir de nouveaux serveurs frais.
4. Sauvegarde la liste propre et à jour dans xtream_portals.json.
"""

import json
import os
import sys
import time
import urllib.parse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

PORTALS_FILE = "xtream_portals.json"
MAX_WORKERS = 20
HTTP_TIMEOUT = 3.5

TELEGRAM_SOURCES = [
    "https://t.me/s/Xtream_Iptv_Code",
    "https://t.me/s/xtream_code_iptv",
    "https://t.me/s/FreeIPTVxtream"
]

SPORT_POSITIVE = [
    'SPORT', 'BEIN', 'CANAL', 'FOOT', 'RMC', 'DAZN', 'EUROSPORT', 
    'LALIGA', 'CALCIO', 'SKY SPORT', 'TNT SPORT', 'LIGUE 1', 'MATCH', 'TOD', 'SSC'
]

NEGATIVE_KEYWORDS = [
    'CINEMA', 'FILMS', 'MOVIES', 'SERIES', 'ADULT', 'XXX', 'KIDS', 
    'ANIMATION', 'DOCUMENTAIRE', 'INFO', 'NEWS', 'MUSIC', 'RADIO', 'VOD'
]

def is_sport(cname):
    up = cname.upper()
    for neg in NEGATIVE_KEYWORDS:
        if neg in up and not any(pos in up for pos in ['SPORT', 'FOOT', 'CANAL+ SPORT']):
            return False
    for pos in SPORT_POSITIVE:
        if pos in up:
            return True
    return False

def category_priority(cname):
    up = cname.upper()
    if ('AR' in up or 'QATAR' in up or 'ARAB' in up) and ('BEIN' in up or 'TOD' in up):
        return (1, cname)
    if ('FR' in up or 'FRANCE' in up) and ('BEIN' in up):
        return (2, cname)
    if ('FR' in up or 'FRANCE' in up) and ('CANAL' in up or 'FOOT' in up or 'LIGUE 1' in up):
        return (3, cname)
    if ('FR' in up or 'FRANCE' in up) and ('RMC' in up or 'DAZN' in up or 'EUROSPORT' in up):
        return (4, cname)
    if ('AR' in up or 'ARAB' in up) and ('SSC' in up or 'SPORT' in up):
        return (5, cname)
    if 'FR' in up or 'FRANCE' in up:
        return (6, cname)
    if 'AR' in up or 'ARAB' in up:
        return (7, cname)
    return (10, cname)

def check_portal_health(portal: dict):
    h = portal.get('host', '').rstrip('/')
    u = portal.get('username', '')
    p = portal.get('password', '')
    if not h or not u or not p:
        return None
    try:
        url = f"{h}/player_api.php?username={u}&password={p}"
        r = requests.get(url, timeout=HTTP_TIMEOUT, headers={'User-Agent': 'Mozilla/5.0'})
        if r.status_code == 200:
            d = r.json()
            uinfo = d.get('user_info', {})
            auth = uinfo.get('auth', 0)
            status = uinfo.get('status', '')
            if auth == 1 and status == 'Active':
                exp = uinfo.get('exp_date')
                if exp:
                    try:
                        if int(exp) < int(time.time()):
                            return None
                    except Exception:
                        pass
                return portal
    except Exception:
        pass
    return None

def extract_credentials_from_telegram_html(html: str) -> list[dict]:
    results = []
    messages = re.findall(r'<div class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    for m in messages:
        clean = re.sub(r'<[^>]+>', '\n', m)
        # get.php
        for url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', clean, re.IGNORECASE):
            try:
                parsed = urllib.parse.urlparse(url)
                params = urllib.parse.parse_qs(parsed.query)
                h = f"{parsed.scheme}://{parsed.netloc}".rstrip('/')
                u = (params.get('username') or params.get('user') or [None])[0]
                p = (params.get('password') or params.get('pass') or [None])[0]
                if h and u and p:
                    results.append({'host': h, 'username': u.strip(), 'password': p.strip()})
            except Exception:
                pass
        # lines
        h, u, p = None, None, None
        for line in clean.splitlines():
            line = line.strip()
            if not line:
                continue
            mh = re.search(r'(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)', line, re.I)
            if mh:
                h = mh.group(1).rstrip('/')
            mu = re.search(r'(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)', line, re.I)
            if mu:
                u = mu.group(1).strip()
            mp = re.search(r'(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)', line, re.I)
            if mp:
                p = mp.group(1).strip()
        if h and u and p:
            results.append({'host': h, 'username': u, 'password': p})
    return results

def process_new_candidate(cand: dict, server_id: str):
    h = cand['host'].rstrip('/')
    u = cand['username']
    p = cand['password']
    base = f"{h}/player_api.php?username={u}&password={p}"
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        rinfo = requests.get(base, timeout=HTTP_TIMEOUT, headers=headers)
        if rinfo.status_code != 200:
            return None
        d = rinfo.json()
        uinfo = d.get('user_info', {})
        if uinfo.get('auth') != 1 or uinfo.get('status') != 'Active':
            return None

        rcat = requests.get(f"{base}&action=get_live_categories", timeout=HTTP_TIMEOUT, headers=headers)
        if rcat.status_code != 200:
            return None
        all_cats = rcat.json()
        if not isinstance(all_cats, list) or not all_cats:
            return None

        sport_cats = {}
        for c in all_cats:
            cid = str(c.get('category_id', '')).strip()
            cname = c.get('category_name', '').strip()
            if cid and is_sport(cname):
                sport_cats[cid] = cname
        if not sport_cats:
            return None

        rstr = requests.get(f"{base}&action=get_live_streams", timeout=HTTP_TIMEOUT + 1.0, headers=headers)
        if rstr.status_code != 200:
            return None
        all_streams = rstr.json()
        if not isinstance(all_streams, list) or not all_streams:
            return None

        cat_streams = {cid: [] for cid in sport_cats.keys()}
        for s in all_streams:
            scat = str(s.get('category_id', '')).strip()
            if scat in cat_streams:
                s_id = str(s.get('stream_id', '')).strip()
                s_name = s.get('name', '').strip()
                s_icon = s.get('stream_icon', '')
                if s_id and s_name:
                    cat_streams[scat].append({"id": s_id, "name": s_name, "icon": s_icon})

        sorted_cids = sorted(sport_cats.keys(), key=lambda cid: category_priority(sport_cats[cid]))
        portal_categories = []
        total_sport_ch = 0
        for cid in sorted_cids:
            chs = cat_streams[cid]
            if chs:
                portal_categories.append({"categoryId": cid, "name": sport_cats[cid], "channels": chs})
                total_sport_ch += len(chs)

        if not portal_categories:
            return None

        clean_host = h.replace('http://', '').replace('https://', '').split(':')[0]
        return {
            "id": server_id,
            "name": f"Serveur Bot : {clean_host}",
            "host": h,
            "username": u,
            "password": p,
            "status": "Active",
            "expiresStr": "Actif",
            "maxConnections": int(uinfo.get('max_connections', 1) or 1),
            "totalCategories": len(portal_categories),
            "totalSportChannels": total_sport_ch,
            "categories": portal_categories
        }
    except Exception:
        return None

def main():
    print("=== DÉMARRAGE DU SCANNER ET PURGE AUTOMATIQUE ===", flush=True)
    t_start = time.time()

    # 1. Charger la liste actuelle
    current_portals = []
    if os.path.exists(PORTALS_FILE):
        try:
            with open(PORTALS_FILE, "r", encoding="utf-8") as f:
                current_portals = json.load(f)
            print(f"[1/4] Serveurs actuellement enregistrés : {len(current_portals)}", flush=True)
        except Exception as e:
            print(f"Erreur lecture {PORTALS_FILE}: {e}", flush=True)

    # 2. Vérification et purge des morts
    print("[2/4] Test de santé en parallèle de tous les serveurs...", flush=True)
    alive_portals = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(check_portal_health, p): p for p in current_portals}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                alive_portals.append(res)

    dead_count = len(current_portals) - len(alive_portals)
    print(f"   -> {len(alive_portals)} serveurs en ligne conservés.", flush=True)
    print(f"   -> {dead_count} serveurs morts ou expirés ÉLIMINÉS.", flush=True)

    # 3. Récolte des nouveaux serveurs depuis Telegram
    print("[3/4] Recherche de nouveaux serveurs sur Telegram...", flush=True)
    existing_keys = {(p['host'].rstrip('/'), p['username'], p['password']) for p in alive_portals}
    new_candidates = []
    for tg_url in TELEGRAM_SOURCES:
        try:
            r = requests.get(tg_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            if r.status_code == 200:
                cands = extract_credentials_from_telegram_html(r.text)
                for c in cands:
                    k = (c['host'].rstrip('/'), c['username'], c['password'])
                    if k not in existing_keys:
                        existing_keys.add(k)
                        new_candidates.append(c)
        except Exception as e:
            print(f"   (Telegram {tg_url} ignoré: {e})", flush=True)

    print(f"   -> {len(new_candidates)} nouveaux serveurs potentiels découverts.", flush=True)

    # 4. Traiter les nouveaux serveurs (limité aux 15 premiers pour rapidité)
    new_active = []
    if new_candidates:
        print("[4/4] Validation des nouveaux serveurs...", flush=True)
        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(process_new_candidate, c, f"srv_new_{i}") for i, c in enumerate(new_candidates[:15], 1)]
            for fut in as_completed(futures):
                res = fut.result()
                if res:
                    new_active.append(res)
                    print(f"   + Nouveau serveur valide ajouté: {res['name']} ({res['totalSportChannels']} chaînes)", flush=True)

    # 5. Fusion et indexation
    final_list = new_active + alive_portals
    for idx, p in enumerate(final_list, 1):
        p['id'] = f"srv_{idx}"
        clean_host = p['host'].replace('http://', '').replace('https://', '').split(':')[0]
        p['name'] = f"Serveur #{idx} : {clean_host}"

    with open(PORTALS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)

    assets_file = os.path.join("app", "src", "main", "assets", "xtream_portals.json")
    if os.path.exists(os.path.dirname(assets_file)):
        with open(assets_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t_start
    print(f"=== TERMINÉ EN {elapsed:.1f}s : {len(final_list)} SERVEURS ACTIFS SAUVEGARDÉS ===", flush=True)

if __name__ == "__main__":
    main()
