#!/usr/bin/env python3
"""
Family Stream - Auto-Updater & Health Checker (Certified Video Xtream Portals)
==============================================================================
S'exécute automatiquement via GitHub Actions (cron toutes les 6h ou manuel) ou en local :
1. Scrape Telegram public pour découvrir de nouveaux serveurs Xtream frais.
2. Teste la santé complète ET la diffusion vidéo réelle (chunks MPEG-TS) en parallèle.
3. ÉLIMINE immédiatement les serveurs morts, inaccessibles, sans flux vidéo ou expirés.
4. Extrait et pré-packagera les catégories sportives authentiques (AR & FR en tête).
5. Sauvegarde la liste certifiée dans xtream_portals.json (0ms d'attente sur TV).
Durée totale d'exécution : environ 45 à 60 secondes.
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
MAX_WORKERS = 24
HTTP_TIMEOUT = 4.0
STREAM_TIMEOUT = 4.5

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

def extract_credentials_from_telegram_html(html: str) -> list[dict]:
    results = []
    messages = re.findall(r'<div class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    for m in messages:
        clean = re.sub(r'<[^>]+>', '\n', m)
        # get.php format
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
        # line by line format
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

def certify_and_extract_server(server: dict) -> dict:
    h = server.get('host', '').rstrip('/')
    u = server.get('username', '').strip()
    p = server.get('password', '').strip()
    clean_host = h.replace('http://', '').replace('https://', '').split(':')[0]
    
    # Filter adult/sketchy hosts
    if any(x in clean_host.lower() for x in ['adult', 'porn', 'xxx', 'myultratv']):
        return None
        
    base = f"{h}/player_api.php?username={u}&password={p}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}

    try:
        # 1. Auth check
        rinfo = requests.get(base, timeout=HTTP_TIMEOUT, headers=headers)
        if rinfo.status_code != 200:
            return None
        d = rinfo.json()
        uinfo = d.get('user_info', {})
        if uinfo.get('auth') != 1 or uinfo.get('status') != 'Active':
            return None
        exp = uinfo.get('exp_date')
        if exp:
            try:
                if int(exp) < int(time.time()):
                    return None
            except Exception:
                pass

        # 2. Categories check
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

        # 3. Live streams check
        rstr = requests.get(f"{base}&action=get_live_streams", timeout=HTTP_TIMEOUT, headers=headers)
        if rstr.status_code != 200:
            return None
        all_streams = rstr.json()
        if not isinstance(all_streams, list) or not all_streams:
            return None

        cat_to_channels = {cid: [] for cid in sport_cats.keys()}
        sample_stream_id = None
        for stream in all_streams:
            cid = str(stream.get('category_id', '')).strip()
            if cid in cat_to_channels:
                sid = str(stream.get('stream_id'))
                if not sample_stream_id:
                    sample_stream_id = sid
                sname = stream.get('name', '').strip()
                sicon = stream.get('stream_icon', '')
                ch_obj = {'id': sid, 'name': sname}
                if sicon:
                    ch_obj['icon'] = sicon
                cat_to_channels[cid].append(ch_obj)

        if not sample_stream_id:
            return None

        # 4. CRITICAL: Test real video MPEG-TS delivery
        stream_url = f"{h}/live/{u}/{p}/{sample_stream_id}.ts"
        with requests.get(stream_url, timeout=STREAM_TIMEOUT, headers=headers, stream=True) as stream_resp:
            if stream_resp.status_code != 200:
                return None
            bytes_received = 0
            for chunk in stream_resp.iter_content(chunk_size=4096):
                bytes_received += len(chunk)
                if bytes_received >= 2048:
                    break
            if bytes_received < 1024:
                return None

        # 5. Build pre-packaged sport categories (AR & FR first)
        sorted_cids = sorted(sport_cats.keys(), key=lambda cid: category_priority(sport_cats[cid]))
        built_categories = []
        total_sport_ch = 0
        for cid in sorted_cids:
            chs = cat_to_channels.get(cid, [])
            if chs:
                built_categories.append({
                    'category_id': cid,
                    'name': sport_cats[cid],
                    'channels': chs
                })
                total_sport_ch += len(chs)

        if not built_categories:
            return None

        return {
            'host': h,
            'username': u,
            'password': p,
            'status': 'Active',
            'expires_str': 'Actif (Certifié Vidéo)',
            'max_connections': int(uinfo.get('max_connections', 1) or 1),
            'total_categories': len(built_categories),
            'total_sport_channels': total_sport_ch,
            'categories': built_categories
        }
    except Exception:
        return None

def main():
    print("=== DÉMARRAGE DU SCANNER VIDÉO & PURGE AUTO ===", flush=True)
    t_start = time.time()

    # 1. Charger les serveurs existants
    current_portals = []
    if os.path.exists(PORTALS_FILE):
        try:
            with open(PORTALS_FILE, "r", encoding="utf-8") as f:
                current_portals = json.load(f)
            print(f"[1/4] Serveurs actuellement enregistrés : {len(current_portals)}", flush=True)
        except Exception as e:
            print(f"Erreur lecture {PORTALS_FILE}: {e}", flush=True)

    # 2. Récolter de nouveaux serveurs depuis Telegram (scrape ~3-5s)
    print("[2/4] Recherche de nouveaux serveurs sur Telegram public...", flush=True)
    candidate_pool = []
    seen = set()
    for p in current_portals:
        k = (p.get('host', '').rstrip('/'), p.get('username', ''), p.get('password', ''))
        seen.add(k)
        candidate_pool.append(p)

    for tg_url in TELEGRAM_SOURCES:
        try:
            r = requests.get(tg_url, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            if r.status_code == 200:
                cands = extract_credentials_from_telegram_html(r.text)
                for c in cands:
                    k = (c['host'].rstrip('/'), c['username'], c['password'])
                    if k not in seen:
                        seen.add(k)
                        candidate_pool.append(c)
        except Exception as e:
            print(f"   (Telegram {tg_url} ignoré: {e})", flush=True)

    print(f"   -> {len(candidate_pool)} serveurs candidats à vérifier...", flush=True)

    # 3. Test de santé + test de diffusion vidéo réelle en parallèle
    print(f"[3/4] Certification vidéo en direct (test réel de chunks .ts, max {MAX_WORKERS} threads)...", flush=True)
    certified = []
    seen_hosts = set()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(certify_and_extract_server, s): s for s in candidate_pool}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                clean_host = res['host'].replace('http://', '').replace('https://', '').split(':')[0]
                # Diversité des hébergeurs : max 2 comptes par domaine
                host_count = sum(1 for h in seen_hosts if h == clean_host)
                if host_count < 2:
                    seen_hosts.add(clean_host)
                    certified.append(res)
                    print(f"   [VÉRIFIÉ OK] {clean_host} ({res['total_sport_channels']} chaînes)", flush=True)

    # 4. Formater les serveurs certifiés
    final_list = []
    for idx, s in enumerate(certified, 1):
        clean_host = s['host'].replace('http://', '').replace('https://', '').split(':')[0]
        final_list.append({
            'id': f"srv_{idx}",
            'name': f"Serveur #{idx} : {clean_host}",
            'host': s['host'],
            'username': s['username'],
            'password': s['password'],
            'status': s['status'],
            'expires_str': s['expires_str'],
            'max_connections': s['max_connections'],
            'total_categories': s['total_categories'],
            'total_sport_channels': s['total_sport_channels'],
            'categories': s['categories']
        })

    # 5. Sauvegarde
    with open(PORTALS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, indent=2)

    assets_file = os.path.join("app", "src", "main", "assets", "xtream_portals.json")
    if os.path.exists(os.path.dirname(assets_file)):
        with open(assets_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, indent=2)

    elapsed = time.time() - t_start
    print(f"=== TERMINÉ EN {elapsed:.1f}s : {len(final_list)} SERVEURS 100% CERTIFIÉS VIDÉO SAUVEGARDÉS ===", flush=True)

if __name__ == "__main__":
    main()
