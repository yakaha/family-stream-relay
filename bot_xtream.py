#!/usr/bin/env python3
"""
Family Stream - Bot Telegram Xtream VIP Scanner (Texte + Fichiers)
==================================================================
Scrape:
  1. https://t.me/Xtream_Iptv_Code
  2. M3U & Xtream [VIP GROUP] (texte + derniers fichiers .txt / .m3u)
  3. t.me/xtream_code_iptv
  4. t.me/FreeIPTVxtream

Caractéristiques :
  - Téléchargement sécurisé des fichiers joints (.txt, .m3u) avec timeout 12s
  - Test multi-threadé en parallèle (15 workers, timeout 5s)
  - Détection automatique de beIN Sports (France & Arabe) et Canal+ / RMC
  - Enregistrement immédiat dans 'xtream_pool.json' et 'resultats_scan.txt'
  - Statut consultable en direct via l'API /api/bot/status
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import threading
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ─── Telethon (Telegram) ───────────────────────────────────────────────────────
from telethon import TelegramClient, events

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

API_ID   = 39352626
API_HASH = "938a87cb1245b0a95c1d85827082fecc"
SESSION  = "xtream_bot_session"

# Sources cibles prioritaires
TARGET_SOURCES = [
    "https://t.me/Xtream_Iptv_Code",
    "M3U & Xtream [VIP GROUP]",
    "https://t.me/xtream_code_iptv",
    "https://t.me/FreeIPTVxtream",
]

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE_DIR, "xtream_pool.json")
REPORT_FILE = os.path.join(BASE_DIR, "resultats_scan.txt")
POOL_LOCK = threading.RLock()

MESSAGES_LIMIT = 200        # Messages à inspecter par source
MAX_FILES_PER_SOURCE = 6    # Maximum de fichiers .txt/.m3u à télécharger par groupe
MAX_FILE_SIZE = 2 * 1024 * 1024  # 2 Mo max par fichier
FILE_DOWNLOAD_TIMEOUT = 12  # Timeout 12s par téléchargement
TEST_WORKERS = 15           # Tests simultanés en parallèle
HTTP_TIMEOUT = 5            # Timeout 5s par test Xtream
SERVER_TTL = 12 * 3600      # Durée de vie d'un serveur (12h)
SESSION_TTL = 4 * 3600

# Patterns sport
BEIN_AR_PATTERNS = [
    (r"\b(bein\s*(?:sport|sports)?\s*1\b|bein\s*1\b|ar\s*[:|\-]?\s*bein\s*1\b)", "bein_1"),
    (r"\b(bein\s*(?:sport|sports)?\s*2\b|bein\s*2\b|ar\s*[:|\-]?\s*bein\s*2\b)", "bein_2"),
    (r"\b(bein\s*(?:sport|sports)?\s*3\b|bein\s*3\b|ar\s*[:|\-]?\s*bein\s*3\b)", "bein_3"),
    (r"\b(bein\s*(?:sport|sports)?\s*4\b|bein\s*4\b|ar\s*[:|\-]?\s*bein\s*4\b)", "bein_4"),
    (r"\b(bein\s*(?:sport|sports)?\s*5\b|bein\s*5\b|ar\s*[:|\-]?\s*bein\s*5\b)", "bein_5"),
    (r"\b(bein\s*(?:sport|sports)?\s*6\b|bein\s*6\b|ar\s*[:|\-]?\s*bein\s*6\b)", "bein_6"),
    (r"\b(bein\s*(?:sport|sports)?\s*7\b|bein\s*7\b|ar\s*[:|\-]?\s*bein\s*7\b)", "bein_7"),
    (r"\b(bein\s*(?:sport|sports)?\s*8\b|bein\s*8\b|ar\s*[:|\-]?\s*bein\s*8\b)", "bein_8"),
    (r"\b(bein\s*(?:sport|sports)?\s*9\b|bein\s*9\b|ar\s*[:|\-]?\s*bein\s*9\b)", "bein_9"),
    (r"\b(bein\s*(?:sport|sports)?\s*4k\b)", "bein_4k"),
]

BEIN_FR_PATTERNS = [
    (r"\b(bein\s*(?:sports?|sport)?\s*1\s*fr\b|fr\s*[:|\-]?\s*bein\s*1\b)", "bein_1_fr"),
    (r"\b(bein\s*(?:sports?|sport)?\s*2\s*fr\b|fr\s*[:|\-]?\s*bein\s*2\b)", "bein_2_fr"),
    (r"\b(bein\s*(?:sports?|sport)?\s*3\s*fr\b|fr\s*[:|\-]?\s*bein\s*3\b)", "bein_3_fr"),
]

CANAL_PATTERNS = [
    (r"\b(canal\+?\s*sport\b|canal\s*plus\s*sport)", "canal_sport"),
    (r"\b(canal\+?\s*foot\b|canal\s*plus\s*foot)", "canal_foot"),
    (r"\b(canal\+?\s*live\b)", "canal_live"),
    (r"\b(rmc\s*sport\s*1\b)", "rmc_sport_1"),
    (r"\b(rmc\s*sport\s*2\b)", "rmc_sport_2"),
    (r"\b(eurosport\s*1\b)", "eurosport_1"),
    (r"\b(eurosport\s*2\b)", "eurosport_2"),
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "bot_xtream.log"), encoding="utf-8"),
    ]
)
log = logging.getLogger("xtream_bot")

def _load_pool() -> dict:
    with POOL_LOCK:
        if not os.path.exists(POOL_FILE):
            return {}
        try:
            with open(POOL_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}

def _save_pool(pool: dict):
    with POOL_LOCK:
        try:
            with open(POOL_FILE, "w", encoding="utf-8") as f:
                json.dump(pool, f, ensure_ascii=False, indent=2)
        except Exception as e:
            log.error(f"Erreur sauvegarde pool: {e}")

def server_doc_id(host: str, username: str) -> str:
    clean = re.sub(r'[^a-zA-Z0-9]', '_', f"{host}_{username}")
    return clean[:100]

def save_server_to_pool(server_info: dict):
    doc_id = server_doc_id(server_info["host"], server_info["username"])
    pool = _load_pool()
    existing = pool.get(doc_id)
    if existing and existing.get("assigned_to"):
        server_info["assigned_to"] = existing["assigned_to"]
        server_info["assigned_channel"] = existing.get("assigned_channel")
        server_info["assigned_at"] = existing.get("assigned_at")
        server_info["status"] = existing.get("status", "available")
    pool[doc_id] = server_info
    _save_pool(pool)

# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACTION DES CODES XTREAM
# ══════════════════════════════════════════════════════════════════════════════

def extract_xtream_codes(text: str) -> list[dict]:
    results = []
    if not text:
        return results

    # 1. URL get.php
    pat_m3u = re.findall(r'https?://[^\s"\'<>]+/get\.php\?[^\s"\'<>]+', text, re.IGNORECASE)
    for url in pat_m3u:
        try:
            parsed = urllib.parse.urlparse(url)
            params = urllib.parse.parse_qs(parsed.query)
            host = f"{parsed.scheme}://{parsed.netloc}".rstrip('/')
            user = (params.get("username") or params.get("user") or [None])[0]
            pwd  = (params.get("password") or params.get("pass") or [None])[0]
            if host and user and pwd:
                results.append({"host": host, "username": user.strip(), "password": pwd.strip()})
        except Exception:
            pass

    # 2. Format blocs : PORTAL / HOST ... USER ... PASS ...
    block_patterns = [
        r'(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)[\s\S]*?(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)[\s\S]*?(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)',
        r'(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)[\s\S]*?(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)[\s\S]*?(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)',
    ]
    for pat in block_patterns:
        for b in re.findall(pat, text, re.IGNORECASE):
            if pat.startswith('(?:USER'):
                u, p, h = b[0].strip(), b[1].strip(), b[2].strip().rstrip('/')
            else:
                h, u, p = b[0].strip().rstrip('/'), b[1].strip(), b[2].strip()
            if h and u and p and len(u) >= 2 and len(p) >= 2:
                results.append({"host": h, "username": u, "password": p})

    # 3. Format triplet ligne : http://host:port user pass
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r'^(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@!#$%&*]+)$', line)
        if m:
            results.append({
                "host": m.group(1).rstrip('/'),
                "username": m.group(2).strip(),
                "password": m.group(3).strip()
            })

    # Déduplication
    seen = set()
    unique = []
    for r in results:
        key = (r['host'], r['username'], r['password'])
        if key not in seen:
            seen.add(key)
            unique.append(r)
    return unique

# ══════════════════════════════════════════════════════════════════════════════
#  TEST XTREAM SERVER
# ══════════════════════════════════════════════════════════════════════════════

def test_xtream_server(host: str, username: str, password: str, timeout: int = HTTP_TIMEOUT) -> tuple[dict | None, str]:
    host = host.rstrip('/')
    api_url = f"{host}/player_api.php?username={username}&password={password}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }

    try:
        r = requests.get(api_url, timeout=timeout, headers=headers, verify=False)
        if r.status_code != 200:
            return None, f"HTTP {r.status_code}"
        data = r.json()
    except requests.exceptions.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, f"Connexion ({type(e).__name__})"

    if not isinstance(data, dict) or "user_info" not in data:
        return None, "Non-Xtream"

    user_info = data.get("user_info", {})
    status = str(user_info.get("status", "")).lower()
    if status not in ("active", "1"):
        return None, f"Inactif ({status})"

    exp_ts = user_info.get("exp_date")
    exp_dt = None
    exp_str = "Illimité"
    try:
        if exp_ts:
            exp_dt = datetime.fromtimestamp(int(exp_ts), tz=timezone.utc)
            if exp_dt < datetime.now(tz=timezone.utc):
                return None, f"Expiré ({exp_dt.strftime('%Y-%m-%d')})"
            exp_str = exp_dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass

    max_conn = int(user_info.get("max_connections", 1) or 1)
    active_conn = int(user_info.get("active_connections", 0) or 0)

    # Récupération chaînes sport
    streams_url = f"{host}/player_api.php?username={username}&password={password}&action=get_live_streams"
    bein_channels = {}
    bein_fr_channels = {}
    canal_channels = {}

    try:
        rc = requests.get(streams_url, timeout=timeout + 3, headers=headers, verify=False)
        if rc.status_code == 200:
            channels = rc.json()
            if isinstance(channels, list):
                for ch in channels:
                    ch_name = str(ch.get("name", "")).strip()
                    ch_lower = ch_name.lower()
                    stream_id = str(ch.get("stream_id", "")).strip()
                    if not stream_id:
                        continue

                    # beIN France
                    matched = False
                    for pat, key in BEIN_FR_PATTERNS:
                        if re.search(pat, ch_lower):
                            bein_fr_channels[key] = {"stream_id": stream_id, "name": ch_name}
                            matched = True
                            break
                    if matched:
                        continue

                    # beIN Arabe
                    for pat, key in BEIN_AR_PATTERNS:
                        if re.search(pat, ch_lower):
                            bein_channels[key] = {"stream_id": stream_id, "name": ch_name}
                            matched = True
                            break
                    if matched:
                        continue

                    # Canal+
                    for pat, key in CANAL_PATTERNS:
                        if re.search(pat, ch_lower):
                            canal_channels[key] = {"stream_id": stream_id, "name": ch_name}
                            break
    except Exception:
        pass

    total_sport = len(bein_channels) + len(bein_fr_channels) + len(canal_channels)
    if total_sport == 0:
        return None, "0 sport"

    return {
        "host": host,
        "username": username,
        "password": password,
        "max_connections": max_conn,
        "active_connections": active_conn,
        "expires_at": exp_dt.isoformat() if exp_dt else None,
        "expires_str": exp_str,
        "bein_channels": bein_channels,
        "bein_fr_channels": bein_fr_channels,
        "canal_channels": canal_channels,
        "total_sport_channels": total_sport,
        "status": "available",
        "assigned_to": None,
        "assigned_channel": None,
        "assigned_at": None,
        "added_at": datetime.now(tz=timezone.utc).isoformat(),
        "last_verified": datetime.now(tz=timezone.utc).isoformat(),
    }, "OK"

# ══════════════════════════════════════════════════════════════════════════════
#  RÉSOLUTION DES ENTITÉS TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

async def resolve_target(client, target: str):
    target = target.strip()
    if target.startswith("https://t.me/") or target.startswith("http://t.me/") or target.startswith("@"):
        try:
            entity = await client.get_entity(target)
            return entity, getattr(entity, "title", target)
        except Exception:
            pass

    target_clean = re.sub(r'[^\w\s]', '', target).lower().strip()
    target_words = [w for w in target_clean.split() if len(w) >= 2]
    best_match = None
    best_title = None

    async for dialog in client.iter_dialogs():
        d_name = dialog.name or ""
        d_clean = re.sub(r'[^\w\s]', '', d_name).lower().strip()
        if target.lower() in d_name.lower() or target_clean in d_clean:
            return dialog.entity, d_name
        if target_words and all(w in d_clean for w in target_words):
            best_match = dialog.entity
            best_title = d_name

    if best_match:
        return best_match, best_title

    try:
        entity = await client.get_entity(target)
        return entity, getattr(entity, "title", target)
    except Exception:
        return None, target

# ══════════════════════════════════════════════════════════════════════════════
#  SCAN INITIAL COMPLET
# ══════════════════════════════════════════════════════════════════════════════

async def run_fast_scan(client, targets: list[str], limit: int = MESSAGES_LIMIT) -> list[dict]:
    log.info("\n" + "=" * 70)
    log.info("🔍 SCAN AUTOMATIQUE DES GROUPES & CANAUX TELEGRAM")
    log.info("=" * 70)

    all_raw_codes = []
    seen_keys = set()

    for target in targets:
        entity, title = await resolve_target(client, target)
        if not entity:
            continue

        log.info(f"📡 Analyse des messages récents de : '{title}'...")
        count_from_text = 0
        count_from_files = 0
        file_count = 0

        try:
            async for msg in client.iter_messages(entity, limit=limit):
                # 1. Texte du message
                if msg.text:
                    codes = extract_xtream_codes(msg.text)
                    for c in codes:
                        k = (c['host'], c['username'], c['password'])
                        if k not in seen_keys:
                            seen_keys.add(k)
                            c['found_in'] = title
                            all_raw_codes.append(c)
                            count_from_text += 1

                # 2. Pièces jointes .txt ou .m3u
                if file_count < MAX_FILES_PER_SOURCE and msg.document and msg.file and msg.file.name:
                    fname = msg.file.name.lower()
                    if any(fname.endswith(ext) for ext in ['.txt', '.m3u', '.m3u8']):
                        fsize = getattr(msg.file, 'size', 0)
                        if fsize and fsize < MAX_FILE_SIZE:
                            file_count += 1
                            log.info(f"   📥 Téléchargement de la liste : {msg.file.name} ({msg.file.size} octets)...")
                            try:
                                b = await asyncio.wait_for(client.download_media(msg, bytes), timeout=FILE_DOWNLOAD_TIMEOUT)
                                if b:
                                    content_str = b.decode('utf-8', errors='ignore')
                                    file_codes = extract_xtream_codes(content_str)
                                    for c in file_codes:
                                        k = (c['host'], c['username'], c['password'])
                                        if k not in seen_keys:
                                            seen_keys.add(k)
                                            c['found_in'] = f"{title} [{msg.file.name}]"
                                            all_raw_codes.append(c)
                                            count_from_files += 1
                                    log.info(f"      ↳ {len(file_codes)} codes extraits de {msg.file.name}")
                            except asyncio.TimeoutError:
                                log.warning(f"      ⚠️ Timeout téléchargement {msg.file.name} (> {FILE_DOWNLOAD_TIMEOUT}s), ignoré.")
                            except Exception as fe:
                                log.warning(f"      ⚠️ Erreur téléchargement {msg.file.name}: {fe}")

            log.info(f"   ↳ Bilan '{title}' : {count_from_text} codes dans texte + {count_from_files} codes dans {file_count} fichier(s)")
        except Exception as e:
            log.error(f"   ⚠️ Erreur scan '{title}': {e}")

    log.info(f"\n📊 Total de codes uniques collectés : {len(all_raw_codes)}")
    if not all_raw_codes:
        log.warning("Aucun code Xtream à tester.")
        output_report([], 0, 0, {})
        return []

    # Test multi-threadé
    log.info(f"⚡ Lancement des vérifications avec {TEST_WORKERS} threads en parallèle (timeout={HTTP_TIMEOUT}s)...")
    valid_servers = []
    fail_reasons = {}
    tested = 0
    total = len(all_raw_codes)

    def _worker(c):
        res, reason = test_xtream_server(c['host'], c['username'], c['password'])
        if res:
            res['found_in'] = c.get('found_in', 'Telegram')
        return res, reason

    with ThreadPoolExecutor(max_workers=TEST_WORKERS) as executor:
        future_map = {executor.submit(_worker, c): c for c in all_raw_codes}
        for future in as_completed(future_map):
            tested += 1
            try:
                res, reason = future.result()
                if res:
                    valid_servers.append(res)
                    save_server_to_pool(res)
                    log.info(f"  [+] VALIDE ({len(valid_servers)} trouvés) : {res['host']} | {res['username']} | {res['total_sport_channels']} chaînes sport | Exp: {res['expires_str']}")
                else:
                    fail_reasons[reason] = fail_reasons.get(reason, 0) + 1
                    if tested % 25 == 0 or tested == total:
                        log.info(f"  Progression : {tested}/{total} testés ({len(valid_servers)} valides)")
            except Exception:
                pass

    output_report(valid_servers, total, tested, fail_reasons)
    return valid_servers

# ══════════════════════════════════════════════════════════════════════════════
#  RAPPORT FINAL
# ══════════════════════════════════════════════════════════════════════════════

def output_report(valid_servers: list[dict], total_codes: int, tested_codes: int, fail_reasons: dict):
    lines = []
    lines.append("=" * 78)
    lines.append("🏆 RAPPORT FINAL - SERVEURS XTREAM DISPONIBLES ET FONCTIONNELS")
    lines.append("=" * 78)
    lines.append(f"Date du scan       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Codes uniques vus  : {total_codes}")
    lines.append(f"Codes testés       : {tested_codes}")
    lines.append(f"Serveurs VALIDES   : {len(valid_servers)}")

    if fail_reasons:
        lines.append("\n📊 Détail des tests négatifs :")
        for reason, count in sorted(fail_reasons.items(), key=lambda x: -x[1])[:6]:
            lines.append(f"   • {reason} : {count} serveurs")

    lines.append("-" * 78)

    if not valid_servers:
        lines.append("❌ Aucun nouveau serveur fonctionnel trouvé dans les derniers messages.")
    else:
        for idx, s in enumerate(valid_servers, 1):
            lines.append(f"\n[{idx}] SERVEUR : {s['host']}")
            lines.append(f"    👤 Utilisateur   : {s['username']}")
            lines.append(f"    🔑 Mot de passe  : {s['password']}")
            lines.append(f"    📅 Expiration    : {s.get('expires_str', 'Inconnue')}")
            lines.append(f"    👥 Connexions    : {s['active_connections']} / {s['max_connections']} max")
            lines.append(f"    📡 Trouvé dans   : {s.get('found_in', 'Telegram')}")
            lines.append(f"    ⚽ Chaînes Sport ({s['total_sport_channels']} chaînes) :")

            ar_keys = list(s.get("bein_channels", {}).keys())
            fr_keys = list(s.get("bein_fr_channels", {}).keys())
            canal_keys = list(s.get("canal_channels", {}).keys())

            if ar_keys:
                lines.append(f"       • beIN Arabe : {len(ar_keys)} flux ({', '.join(ar_keys[:8])})")
            if fr_keys:
                lines.append(f"       • beIN France: {len(fr_keys)} flux ({', '.join(fr_keys)})")
            if canal_keys:
                lines.append(f"       • Canal+/RMC : {len(canal_keys)} flux ({', '.join(canal_keys)})")

    lines.append("\n" + "=" * 78)
    lines.append(f"💾 Pool global sauvegardé dans : {POOL_FILE}")
    lines.append(f"📄 Ce rapport est enregistré dans: {REPORT_FILE}")
    lines.append("=" * 78)

    report_text = "\n".join(lines)
    print("\n" + report_text + "\n")

    try:
        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(report_text)
    except Exception as e:
        log.error(f"Erreur écriture rapport: {e}")

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("🚀 Family Stream - Bot Xtream VIP Scanner (Texte + Fichiers)")
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    log.info("✅ Telegram connecté avec succès !")

    await run_fast_scan(client, TARGET_SOURCES, limit=MESSAGES_LIMIT)
    await client.disconnect()
    log.info("Scan terminé. Déconnexion effectuée.")

if __name__ == "__main__":
    asyncio.run(main())
