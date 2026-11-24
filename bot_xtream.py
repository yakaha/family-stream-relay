#!/usr/bin/env python3
"""
Family Stream - Bot Telegram Xtream Pool Scanner & Watcher
===========================================================
Scrape:
  1. https://t.me/Xtream_Iptv_Code
  2. Groupe "M3U & Xtream [VIP GROUP]" (résolution auto dans vos dialogues Telegram)

Fonctionnalités :
  - Recherche automatique des groupes par URL ou nom de dialogue
  - Extraction multi-formats robuste des codes Xtream (get.php, triplet, PORTAL/USER/PASS)
  - Test multi-threadé ULTRA-RAPIDE (10-15 workers concurrents)
  - Détection précise des chaînes beIN Sports (France & Arabe) et Canal+
  - Affichage direct du rapport de serveurs DISPONIBLES dans le terminal
  - Export dans 'resultats_scan.txt' et mise à jour de 'xtream_pool.json'
  - Surveillance temps réel permanente après le scan initial

Usage:
  python3 bot_xtream.py             # Scan complet + veille temps réel
  python3 bot_xtream.py --scan-only # Scan complet + rapport + arrêt
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

# Sources cibles demandées par l'utilisateur
TARGET_SOURCES = [
    "https://t.me/Xtream_Iptv_Code",
    "M3U & Xtream [VIP GROUP]",
]

# Canaux additionnels optionnels
EXTRA_CHANNELS = [
    "https://t.me/xtream_code_iptv",
    "https://t.me/FreeIPTVxtream",
]

# Fichiers de stockage
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
POOL_FILE = os.path.join(BASE_DIR, "xtream_pool.json")
REPORT_FILE = os.path.join(BASE_DIR, "resultats_scan.txt")
POOL_LOCK = threading.RLock()

# Limites & Délais
MESSAGES_LIMIT = 250        # Nombre de messages récents à analyser par source
TEST_WORKERS = 12           # Tests simultanés en parallèle
HTTP_TIMEOUT = 7            # Timeout par requête HTTP
SERVER_TTL = 6 * 3600       # Durée de vie d'un serveur dans le pool (6h)
SESSION_TTL = 4 * 3600      # Durée de session utilisateur (4h)

# Patterns sports
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

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(BASE_DIR, "bot_xtream.log"), encoding="utf-8"),
    ]
)
log = logging.getLogger("xtream_bot")

# ══════════════════════════════════════════════════════════════════════════════
#  STOCKAGE JSON LOCAL
# ══════════════════════════════════════════════════════════════════════════════

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

def cleanup_expired_servers():
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(seconds=SERVER_TTL)
    pool = _load_pool()
    to_delete = []
    for doc_id, d in pool.items():
        try:
            added = datetime.fromisoformat(d.get("added_at", ""))
            if added < cutoff:
                to_delete.append(doc_id)
                continue
        except Exception:
            pass
        if d.get("assigned_at"):
            try:
                assigned = datetime.fromisoformat(d["assigned_at"])
                if (now - assigned).total_seconds() > SESSION_TTL:
                    d["status"] = "available"
                    d["assigned_to"] = None
                    d["assigned_channel"] = None
                    d["assigned_at"] = None
            except Exception:
                pass
    for doc_id in to_delete:
        del pool[doc_id]
    _save_pool(pool)

# ══════════════════════════════════════════════════════════════════════════════
#  EXTRACTION DES CODES XTREAM MULTI-FORMATS
# ══════════════════════════════════════════════════════════════════════════════

def extract_xtream_codes(text: str) -> list[dict]:
    results = []
    if not text:
        return results

    # 1. URL get.php
    pat_m3u = re.findall(r'https?://[^\s"\'<>]+/get\.php\?[^\s"\'<>]+', text, re.IGNORECASE)
    for url in pat_m3u:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        host = f"{parsed.scheme}://{parsed.netloc}".rstrip('/')
        user = (params.get("username") or params.get("user") or [None])[0]
        pwd  = (params.get("password") or params.get("pass") or [None])[0]
        if host and user and pwd:
            results.append({"host": host, "username": user.strip(), "password": pwd.strip(), "source": "m3u_url"})

    # 2. Format blocs : HOST/PORTAL ... USER ... PASS ...
    block_patterns = [
        r'(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n]+)[\s\S]*?(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n]+)[\s\S]*?(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n]+)',
        r'(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n]+)[\s\S]*?(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n]+)[\s\S]*?(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n]+)',
    ]
    for b in re.findall(block_patterns[0], text, re.IGNORECASE):
        h = b[0].strip().rstrip('/')
        u = b[1].strip()
        p = b[2].strip()
        if h and u and p and len(u) >= 2 and len(p) >= 2:
            results.append({"host": h, "username": u, "password": p, "source": "block_portal"})

    for b in re.findall(block_patterns[1], text, re.IGNORECASE):
        u = b[0].strip()
        p = b[1].strip()
        h = b[2].strip().rstrip('/')
        if h and u and p and len(u) >= 2 and len(p) >= 2:
            results.append({"host": h, "username": u, "password": p, "source": "block_user_first"})

    # 3. Format triplet ligne : http://host:port user pass
    for line in text.splitlines():
        line = line.strip()
        m = re.match(r'^(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@!#$%&*]+)$', line)
        if m:
            results.append({
                "host": m.group(1).rstrip('/'),
                "username": m.group(2).strip(),
                "password": m.group(3).strip(),
                "source": "triplet"
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
#  TEST RAPIDE D'UN SERVEUR XTREAM
# ══════════════════════════════════════════════════════════════════════════════

def test_xtream_server(host: str, username: str, password: str, timeout: int = HTTP_TIMEOUT) -> dict | None:
    host = host.rstrip('/')
    api_url = f"{host}/player_api.php?username={username}&password={password}"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "*/*"
    }

    try:
        r = requests.get(api_url, timeout=timeout, headers=headers, verify=False)
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception:
        return None

    if not isinstance(data, dict) or "user_info" not in data:
        return None

    user_info = data.get("user_info", {})
    status = str(user_info.get("status", "")).lower()
    if status not in ("active", "1"):
        return None

    # Expiration
    exp_ts = user_info.get("exp_date")
    exp_dt = None
    exp_str = "Illimité"
    try:
        if exp_ts:
            exp_dt = datetime.fromtimestamp(int(exp_ts), tz=timezone.utc)
            if exp_dt < datetime.now(tz=timezone.utc):
                return None  # Expiré
            exp_str = exp_dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        pass

    max_conn = int(user_info.get("max_connections", 1) or 1)
    active_conn = int(user_info.get("active_connections", 0) or 0)

    # Récupération des chaînes live
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

                    # beIN Sports France
                    matched_fr = False
                    for pat, key in BEIN_FR_PATTERNS:
                        if re.search(pat, ch_lower):
                            bein_fr_channels[key] = {
                                "stream_id": stream_id,
                                "name": ch_name,
                                "category": "🇫🇷 beIN Sports (France)"
                            }
                            matched_fr = True
                            break
                    if matched_fr:
                        continue

                    # beIN Sports Arabe
                    matched_ar = False
                    for pat, key in BEIN_AR_PATTERNS:
                        if re.search(pat, ch_lower):
                            bein_channels[key] = {
                                "stream_id": stream_id,
                                "name": ch_name,
                                "category": "🇸🇦 beIN Sports (Arabe)"
                            }
                            matched_ar = True
                            break
                    if matched_ar:
                        continue

                    # Canal+ & Sports FR
                    for pat, key in CANAL_PATTERNS:
                        if re.search(pat, ch_lower):
                            canal_channels[key] = {
                                "stream_id": stream_id,
                                "name": ch_name,
                                "category": "🇫🇷 Canal+ & Sports France"
                            }
                            break
    except Exception:
        pass

    total_sport = len(bein_channels) + len(bein_fr_channels) + len(canal_channels)
    if total_sport == 0:
        return None

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
    }

# ══════════════════════════════════════════════════════════════════════════════
#  RÉSOLUTION DES ENTITÉS TELEGRAM (CANAUX & GROUPES)
# ══════════════════════════════════════════════════════════════════════════════

async def resolve_target(client, target: str):
    """
    Résout un canal ou groupe Telegram :
      - Par URL (ex: https://t.me/Xtream_Iptv_Code)
      - Par @username
      - Par nom de dialogue dans le compte (ex: 'M3U & Xtream [VIP GROUP]')
    """
    target = target.strip()

    # 1. URL ou @username direct
    if target.startswith("https://t.me/") or target.startswith("http://t.me/") or target.startswith("@"):
        try:
            entity = await client.get_entity(target)
            title = getattr(entity, "title", target)
            log.info(f"  🎯 Canal résolu par lien : {title} ({target})")
            return entity, title
        except Exception as e:
            log.warning(f"  ⚠️ get_entity direct échoué pour {target}: {e}")

    # 2. Recherche dans les dialogues du compte connecté
    target_clean = re.sub(r'[^\w\s]', '', target).lower().strip()
    target_words = [w for w in target_clean.split() if len(w) >= 2]

    best_match = None
    best_title = None

    async for dialog in client.iter_dialogs():
        d_name = dialog.name or ""
        d_clean = re.sub(r'[^\w\s]', '', d_name).lower().strip()

        # Correspondance exacte ou inclusion
        if target.lower() in d_name.lower() or target_clean in d_clean:
            log.info(f"  🎯 Groupe/Canal trouvé dans vos dialogues Telegram : '{d_name}'")
            return dialog.entity, d_name

        # Correspondance par mots clés (ex: 'm3u', 'xtream', 'vip')
        if target_words and all(w in d_clean for w in target_words):
            best_match = dialog.entity
            best_title = d_name

    if best_match:
        log.info(f"  🎯 Correspondance par mots-clés dans vos dialogues : '{best_title}'")
        return best_match, best_title

    # Fallback get_entity
    try:
        entity = await client.get_entity(target)
        title = getattr(entity, "title", target)
        return entity, title
    except Exception as e:
        log.error(f"  ❌ Source introuvable sur votre compte Telegram : '{target}' ({e})")
        return None, target

# ══════════════════════════════════════════════════════════════════════════════
#  SCAN INITIAL PARALLÉLISÉ & RAPIDE
# ══════════════════════════════════════════════════════════════════════════════

async def run_fast_scan(client, targets: list[str], limit: int = MESSAGES_LIMIT) -> list[dict]:
    log.info("\n" + "=" * 70)
    log.info("🔍 DÉMARRAGE DU SCAN DES CANAUX & GROUPES DEMANDÉS")
    log.info("=" * 70)

    all_raw_codes = []
    seen_keys = set()
    active_entities = []

    # 1. Résolution des cibles et collecte des messages
    for target in targets:
        entity, title = await resolve_target(client, target)
        if not entity:
            continue
        active_entities.append((entity, title))

        log.info(f"📡 Analyse des {limit} derniers messages de : '{title}'...")
        count_found = 0
        try:
            async for msg in client.iter_messages(entity, limit=limit):
                if msg.text:
                    codes = extract_xtream_codes(msg.text)
                    for c in codes:
                        k = (c['host'], c['username'], c['password'])
                        if k not in seen_keys:
                            seen_keys.add(k)
                            c['channel_title'] = title
                            all_raw_codes.append(c)
                            count_found += 1
            log.info(f"   ↳ {count_found} code(s) Xtream uniques extrait(s) de '{title}'")
        except Exception as e:
            log.error(f"   ⚠️ Erreur lecture messages de '{title}': {e}")

    log.info(f"\n📊 Total de codes uniques à tester : {len(all_raw_codes)}")
    if not all_raw_codes:
        log.warning("Aucun code trouvé à tester.")
        return []

    # 2. Test multi-threadé rapide
    log.info(f"⚡ Lancement des tests avec {TEST_WORKERS} threads en parallèle (timeout={HTTP_TIMEOUT}s)...")
    valid_servers = []
    tested = 0
    total = len(all_raw_codes)

    def _worker(c):
        res = test_xtream_server(c['host'], c['username'], c['password'])
        if res:
            res['found_in'] = c.get('channel_title', 'Telegram')
        return res

    with ThreadPoolExecutor(max_workers=TEST_WORKERS) as executor:
        future_map = {executor.submit(_worker, c): c for c in all_raw_codes}
        for future in as_completed(future_map):
            tested += 1
            try:
                res = future.result()
                if res:
                    valid_servers.append(res)
                    save_server_to_pool(res)
                    log.info(f"  [+] VALIDE ({len(valid_servers)} trouvés) : {res['host']} ({res['username']}) | {res['total_sport_channels']} chaînes sport | Exp: {res['expires_str']}")
                else:
                    if tested % 15 == 0 or tested == total:
                        log.info(f"  Progression : {tested}/{total} testés ({len(valid_servers)} valides)")
            except Exception:
                pass

    # 3. Génération du Rapport Final
    output_report(valid_servers, total, tested)
    return valid_servers

# ══════════════════════════════════════════════════════════════════════════════
#  AFFICHAGE DU RAPPORT
# ══════════════════════════════════════════════════════════════════════════════

def output_report(valid_servers: list[dict], total_codes: int, tested_codes: int):
    lines = []
    lines.append("=" * 78)
    lines.append("🏆 RAPPORT FINAL - SERVEURS XTREAM DISPONIBLES ET OPÉRATIONNELS")
    lines.append("=" * 78)
    lines.append(f"Date du scan       : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"Codes uniques vus  : {total_codes}")
    lines.append(f"Codes testés       : {tested_codes}")
    lines.append(f"Serveurs VALIDES   : {len(valid_servers)}")
    lines.append("-" * 78)

    if not valid_servers:
        lines.append("❌ Aucun serveur fonctionnel n'a été trouvé lors de ce scan.")
    else:
        for idx, s in enumerate(valid_servers, 1):
            lines.append(f"\n[{idx}] SERVEUR : {s['host']}")
            lines.append(f"    👤 Utilisateur   : {s['username']}")
            lines.append(f"    🔑 Mot de passe  : {s['password']}")
            lines.append(f"    📅 Expiration    : {s.get('expires_str', 'Inconnue')}")
            lines.append(f"    👥 Connexions    : {s['active_connections']} / {s['max_connections']} max")
            lines.append(f"    📡 Source        : {s.get('found_in', 'Telegram')}")
            lines.append(f"    ⚽ Couverture Sport ({s['total_sport_channels']} chaînes) :")
            
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
#  SURVEILLANCE TEMPS RÉEL (TEMPS RÉEL APRÈS SCAN)
# ══════════════════════════════════════════════════════════════════════════════

async def watch_target_sources(client, targets: list[str]):
    for target in targets:
        entity, title = await resolve_target(client, target)
        if not entity:
            continue

        @client.on(events.NewMessage(chats=entity))
        async def handler(event, src_title=title):
            if not event.message.text:
                return
            codes = extract_xtream_codes(event.message.text)
            if not codes:
                return
            log.info(f"\n⚡ NOUVEAU MESSAGE sur '{src_title}' : {len(codes)} code(s) détecté(s) !")
            for c in codes:
                res = test_xtream_server(c['host'], c['username'], c['password'])
                if res:
                    res['found_in'] = src_title
                    save_server_to_pool(res)
                    log.info(f"  🎉 NOUVEAU SERVEUR AJOUTÉ AU POOL : {res['host']} | {res['total_sport_channels']} chaînes sport | Exp: {res['expires_str']}")
                else:
                    log.info(f"  ❌ Code invalide/inactif : {c['host']} ({c['username']})")

        log.info(f"👁️ Surveillance temps réel active sur : '{title}'")

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN ENTRYPOINT
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("🚀 Family Stream - Bot Xtream VIP Scanner")
    cleanup_expired_servers()

    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    log.info("✅ Telegram connecté avec succès !")

    # 1. Scan initial des cibles spécifiées par l'utilisateur
    await run_fast_scan(client, TARGET_SOURCES, limit=MESSAGES_LIMIT)

    # Si mode --scan-only, on quitte directement
    if "--scan-only" in sys.argv:
        log.info("Option --scan-only spécifiée. Fin du traitement.")
        await client.disconnect()
        return

    # 2. Activation de la veille permanente en temps réel
    log.info("\n🔔 Passage en mode SURVEILLANCE TEMPS RÉEL...")
    await watch_target_sources(client, TARGET_SOURCES)

    pool = _load_pool()
    log.info(f"✅ Bot opérationnel - Total serveurs dans le pool: {len(pool)}")
    log.info("⏳ En attente de nouveaux codes en temps réel... (Ctrl+C pour quitter)\n")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
