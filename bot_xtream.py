#!/usr/bin/env python3
"""
Family Stream - Bot Telegram Xtream Pool
=========================================
Scrape t.me/Xtream_Iptv_Code (et autres canaux configurés),
teste chaque Xtream Code, garde les serveurs valides avec beIN Sports
dans un pool Firebase Firestore avec système d'assignation exclusive.

Usage: python bot_xtream.py
Dépendances: pip install telethon requests firebase-admin
"""

import asyncio
import json
import logging
import os
import re
import sys
import time
import threading
import requests
import urllib.parse
from datetime import datetime, timezone, timedelta

# ─── Telethon (Telegram) ───────────────────────────────────────────────────────
from telethon import TelegramClient, events
from telethon.tl.types import MessageEntityUrl, MessageEntityTextUrl

# ─── Firebase Admin ────────────────────────────────────────────────────────────
import firebase_admin
from firebase_admin import credentials, firestore

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

API_ID   = 39352626
API_HASH = "938a87cb1245b0a95c1d85827082fecc"
SESSION  = "xtream_bot_session"  # Fichier .session sauvegardé localement

# Canaux Telegram à scraper (ajoute-en autant que tu veux)
TELEGRAM_CHANNELS = [
    "https://t.me/Xtream_Iptv_Code",
    "https://t.me/xtream_code_iptv",
    "https://t.me/FreeIPTVxtream",
]

# Firebase - chemin vers la clé de service
FIREBASE_CRED_PATH = os.path.join(os.path.dirname(__file__), "firebase_service_key.json")
FIREBASE_PROJECT   = "familysteam-09"

# Collection Firestore
POOL_COLLECTION = "xtream_pool"

# Mots-clés pour détecter les chaînes beIN Sports
BEIN_KEYWORDS = [
    "bein sport", "beinsport", "bein sports", "bein 1", "bein 2", "bein 3",
    "bein 4", "bein 5", "bein 6", "bein 7", "bein ar", "bein hd",
    "ar: bein", "ar - bein", "ar| bein",
]
CANAL_KEYWORDS = ["canal+", "canal plus", "canal sport", "rmc sport", "eurosport"]
BEIN_FR_KEYWORDS = ["bein sports fr", "bein fr", "fr: bein", "fr - bein"]

# Durée de vie d'un serveur en pool (secondes) - 6 heures
SERVER_TTL = 6 * 3600
# Durée max d'une session utilisateur (secondes) - 4 heures
SESSION_TTL = 4 * 3600
# Intervalle de vérification des serveurs (secondes)
CHECK_INTERVAL = 300  # 5 minutes

# ─── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("bot_xtream.log"),
    ]
)
log = logging.getLogger("xtream_bot")

# ══════════════════════════════════════════════════════════════════════════════
#  FIREBASE
# ══════════════════════════════════════════════════════════════════════════════

db = None

def init_firebase():
    global db
    if not firebase_admin._apps:
        if os.path.exists(FIREBASE_CRED_PATH):
            cred = credentials.Certificate(FIREBASE_CRED_PATH)
        else:
            # Sur Codespace : utilise les Application Default Credentials
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT})
    db = firestore.client()
    log.info("✅ Firebase connecté")

# ══════════════════════════════════════════════════════════════════════════════
#  PARSING DES XTREAM CODES
# ══════════════════════════════════════════════════════════════════════════════

def extract_xtream_codes(text: str) -> list[dict]:
    """
    Extrait tous les Xtream Codes d'un message Telegram.
    Supporte les formats:
      - http://host:port/get.php?username=X&password=Y&type=m3u
      - http://host:port username password
      - URL: http://... User: ... Pass: ...
      - M3U: http://host:port/get.php?username=...
    """
    results = []
    text = text.strip()

    # Pattern 1: URL complète type m3u/m3u_plus
    pat_m3u = re.findall(
        r'(https?://[^\s]+/get\.php\?[^\s]+)',
        text, re.IGNORECASE
    )
    for url in pat_m3u:
        parsed = urllib.parse.urlparse(url)
        params = urllib.parse.parse_qs(parsed.query)
        host = f"{parsed.scheme}://{parsed.netloc}"
        user = params.get("username", params.get("user", [None]))[0]
        pwd  = params.get("password", params.get("pass", [None]))[0]
        if host and user and pwd:
            results.append({"host": host, "username": user, "password": pwd, "source": "m3u_url"})

    # Pattern 2: http://host:port username password (3 mots)
    pat_triplet = re.findall(
        r'(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@]+)',
        text
    )
    for host, user, pwd in pat_triplet:
        if len(user) >= 3 and len(pwd) >= 3:
            results.append({"host": host, "username": user, "password": pwd, "source": "triplet"})

    # Pattern 3: clé:valeur (URL/Host/Server + Username/User + Password/Pass)
    url_match  = re.search(r'(?:url|host|server|portal)[:\s]+([^\s\n]+)', text, re.IGNORECASE)
    user_match = re.search(r'(?:username|user|usr)[:\s]+([^\s\n]+)', text, re.IGNORECASE)
    pass_match = re.search(r'(?:password|pass|pwd)[:\s]+([^\s\n]+)', text, re.IGNORECASE)
    if url_match and user_match and pass_match:
        h = url_match.group(1).strip().rstrip('/')
        if not h.startswith("http"):
            h = "http://" + h
        results.append({
            "host": h,
            "username": user_match.group(1).strip(),
            "password": pass_match.group(1).strip(),
            "source": "keyvalue"
        })

    # Déduplique
    seen = set()
    unique = []
    for r in results:
        key = f"{r['host']}|{r['username']}|{r['password']}"
        if key not in seen:
            seen.add(key)
            unique.append(r)

    return unique

# ══════════════════════════════════════════════════════════════════════════════
#  VÉRIFICATION DES SERVEURS
# ══════════════════════════════════════════════════════════════════════════════

def test_xtream_server(host: str, username: str, password: str, timeout=8) -> dict | None:
    """
    Teste un Xtream Code via /player_api.php.
    Retourne un dict avec les infos du serveur si valide, None sinon.
    """
    host = host.rstrip('/')
    url  = f"{host}/player_api.php?username={username}&password={password}"

    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent": "FamilyStream/2.0"})
        if r.status_code != 200:
            return None
        data = r.json()
    except Exception as e:
        log.debug(f"  ❌ {host} → {e}")
        return None

    # Vérifie que c'est un vrai Xtream
    if "user_info" not in data:
        return None

    user_info = data.get("user_info", {})
    # Vérifie que le compte est actif
    status = str(user_info.get("status", "")).lower()
    if status not in ("active", "1"):
        log.debug(f"  ⏸ {host} compte inactif (status={status})")
        return None

    max_conn = int(user_info.get("max_connections", 1))
    active_conn = int(user_info.get("active_connections", 0))

    # Charge la liste des chaînes live
    channels_url = f"{host}/player_api.php?username={username}&password={password}&action=get_live_streams"
    bein_channels = {}
    canal_channels = {}
    bein_fr_channels = {}

    try:
        rc = requests.get(channels_url, timeout=15, headers={"User-Agent": "FamilyStream/2.0"})
        if rc.status_code == 200:
            channels = rc.json()
            for ch in channels:
                name = str(ch.get("name", "")).lower()
                stream_id = str(ch.get("stream_id", ""))
                if not stream_id:
                    continue

                # beIN Sports France
                if any(k in name for k in BEIN_FR_KEYWORDS):
                    num = extract_channel_number(name, "bein")
                    key = f"bein_{num}_fr" if num else f"bein_fr_{stream_id}"
                    bein_fr_channels[key] = {
                        "stream_id": stream_id,
                        "name": ch.get("name", ""),
                        "category": "🇫🇷 beIN Sports (France)"
                    }
                # beIN Sports Arabe
                elif any(k in name for k in BEIN_KEYWORDS):
                    num = extract_channel_number(name, "bein")
                    key = f"bein_{num}" if num else f"bein_{stream_id}"
                    bein_channels[key] = {
                        "stream_id": stream_id,
                        "name": ch.get("name", ""),
                        "category": "🇸🇦 beIN Sports (Arabe)"
                    }
                # Canal+ / RMC
                elif any(k in name for k in CANAL_KEYWORDS):
                    num = extract_channel_number(name, "canal")
                    key = f"canal_{num}" if num else f"canal_{stream_id}"
                    canal_channels[key] = {
                        "stream_id": stream_id,
                        "name": ch.get("name", ""),
                        "category": "🇫🇷 Canal+ & Sports France"
                    }
    except Exception as e:
        log.debug(f"  ⚠️ Chargement chaînes échoué: {e}")

    total_sport = len(bein_channels) + len(bein_fr_channels) + len(canal_channels)
    if total_sport == 0:
        log.debug(f"  ❌ {host} → aucune chaîne beIN/Canal trouvée")
        return None

    exp_ts = user_info.get("exp_date")
    try:
        exp_dt = datetime.fromtimestamp(int(exp_ts), tz=timezone.utc) if exp_ts else None
        if exp_dt and exp_dt < datetime.now(tz=timezone.utc):
            log.debug(f"  ⌛ {host} → compte expiré ({exp_dt})")
            return None
    except Exception:
        exp_dt = None

    log.info(f"  ✅ {host} | {username} → {len(bein_channels)} beIN-AR, {len(bein_fr_channels)} beIN-FR, {len(canal_channels)} Canal+")

    return {
        "host": host,
        "username": username,
        "password": password,
        "max_connections": max_conn,
        "active_connections": active_conn,
        "expires_at": exp_dt.isoformat() if exp_dt else None,
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

def extract_channel_number(name: str, prefix: str) -> str | None:
    """Extrait le numéro d'une chaîne (ex: 'bein sports 3 hd' → '3')"""
    m = re.search(r'\b(\d+)\b', name)
    if m:
        return m.group(1)
    return None

# ══════════════════════════════════════════════════════════════════════════════
#  FIRESTORE - POOL MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def server_doc_id(host: str, username: str) -> str:
    """ID unique du document Firestore pour un serveur"""
    clean = re.sub(r'[^a-zA-Z0-9]', '_', f"{host}_{username}")
    return clean[:100]

def save_server_to_pool(server_info: dict):
    """Sauvegarde ou met à jour un serveur dans Firestore"""
    if not db:
        return
    doc_id = server_doc_id(server_info["host"], server_info["username"])
    ref = db.collection(POOL_COLLECTION).document(doc_id)
    # Ne pas écraser l'assignation en cours
    existing = ref.get()
    if existing.exists:
        old = existing.to_dict()
        if old.get("assigned_to"):
            # Mise à jour partielle (pas toucher à l'assignation)
            server_info["assigned_to"] = old["assigned_to"]
            server_info["assigned_channel"] = old.get("assigned_channel")
            server_info["assigned_at"] = old.get("assigned_at")
            server_info["status"] = old.get("status", "available")
    ref.set(server_info)
    log.info(f"  💾 Sauvegardé: {doc_id}")

def cleanup_expired_servers():
    """Supprime les serveurs expirés ou invalides du pool"""
    if not db:
        return
    now = datetime.now(tz=timezone.utc)
    cutoff = now - timedelta(seconds=SERVER_TTL)
    docs = db.collection(POOL_COLLECTION).stream()
    removed = 0
    for doc in docs:
        d = doc.to_dict()
        # Vérifie TTL
        try:
            added = datetime.fromisoformat(d.get("added_at", ""))
            if added < cutoff:
                doc.reference.delete()
                removed += 1
                continue
        except Exception:
            pass
        # Libère les sessions expirées
        if d.get("assigned_at"):
            try:
                assigned = datetime.fromisoformat(d["assigned_at"])
                if (now - assigned).total_seconds() > SESSION_TTL:
                    doc.reference.update({
                        "status": "available",
                        "assigned_to": None,
                        "assigned_channel": None,
                        "assigned_at": None,
                    })
                    log.info(f"  🔓 Session expirée libérée: {doc.id}")
            except Exception:
                pass
    if removed:
        log.info(f"  🗑️ {removed} serveurs expirés supprimés")

# ══════════════════════════════════════════════════════════════════════════════
#  VÉRIFICATION PÉRIODIQUE DES SERVEURS EXISTANTS
# ══════════════════════════════════════════════════════════════════════════════

def reverify_pool():
    """Re-teste tous les serveurs du pool pour supprimer ceux qui ne marchent plus"""
    if not db:
        return
    docs = list(db.collection(POOL_COLLECTION).stream())
    log.info(f"🔄 Re-vérification de {len(docs)} serveurs du pool...")
    for doc in docs:
        d = doc.to_dict()
        host = d.get("host", "")
        user = d.get("username", "")
        pwd  = d.get("password", "")
        if not host or not user or not pwd:
            doc.reference.delete()
            continue
        result = test_xtream_server(host, user, pwd)
        if result:
            # Mise à jour sans toucher à l'assignation
            update = {
                "bein_channels": result["bein_channels"],
                "bein_fr_channels": result["bein_fr_channels"],
                "canal_channels": result["canal_channels"],
                "total_sport_channels": result["total_sport_channels"],
                "max_connections": result["max_connections"],
                "last_verified": datetime.now(tz=timezone.utc).isoformat(),
            }
            doc.reference.update(update)
        else:
            # Serveur mort : supprime seulement s'il n'est pas en cours d'utilisation
            if not d.get("assigned_to"):
                doc.reference.delete()
                log.info(f"  🗑️ Serveur mort supprimé: {doc.id}")

# ══════════════════════════════════════════════════════════════════════════════
#  BACKGROUND WORKER: SCRAPING CONTINU
# ══════════════════════════════════════════════════════════════════════════════

processed_messages = set()

async def process_message(text: str, source: str):
    """Traite un message Telegram pour en extraire et tester les Xtream Codes"""
    codes = extract_xtream_codes(text)
    if not codes:
        return
    log.info(f"📩 {source} → {len(codes)} code(s) trouvé(s)")
    for code in codes:
        log.info(f"  🧪 Test: {code['host']} / {code['username']}")
        result = test_xtream_server(code["host"], code["username"], code["password"])
        if result:
            save_server_to_pool(result)

async def scrape_history(client, channel_url: str, limit: int = 200):
    """Charge l'historique récent d'un canal"""
    try:
        entity = await client.get_entity(channel_url)
        log.info(f"📡 Scrape historique: {channel_url} (derniers {limit} messages)")
        async for msg in client.iter_messages(entity, limit=limit):
            if msg.id in processed_messages:
                continue
            processed_messages.add(msg.id)
            if msg.text:
                await process_message(msg.text, channel_url)
    except Exception as e:
        log.warning(f"⚠️ Erreur scrape {channel_url}: {e}")

async def watch_channels(client):
    """Écoute les nouveaux messages en temps réel"""
    for ch_url in TELEGRAM_CHANNELS:
        try:
            entity = await client.get_entity(ch_url)
            @client.on(events.NewMessage(chats=entity))
            async def handler(event, src=ch_url):
                if event.message.text:
                    await process_message(event.message.text, src)
            log.info(f"👁️ Surveillance active: {ch_url}")
        except Exception as e:
            log.warning(f"⚠️ Impossible de surveiller {ch_url}: {e}")

async def periodic_maintenance():
    """Nettoyage et re-vérification périodique"""
    while True:
        await asyncio.sleep(CHECK_INTERVAL)
        log.info("⏰ Maintenance périodique...")
        cleanup_expired_servers()
        reverify_pool()
        # Re-scrape l'historique récent
        # (sera fait dans main loop)

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("🚀 Family Stream - Bot Xtream Pool démarré")

    # Init Firebase
    init_firebase()

    # Nettoyage initial
    cleanup_expired_servers()

    # Démarrage client Telegram
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    log.info("✅ Telegram connecté")

    # Scrape l'historique récent de tous les canaux
    for ch in TELEGRAM_CHANNELS:
        await scrape_history(client, ch, limit=300)

    # Active la surveillance temps réel
    await watch_channels(client)

    # Lance la maintenance périodique en parallèle
    asyncio.create_task(periodic_maintenance())

    # Re-scrape toutes les heures
    async def hourly_rescrape():
        while True:
            await asyncio.sleep(3600)
            log.info("🔄 Re-scrape horaire...")
            for ch in TELEGRAM_CHANNELS:
                await scrape_history(client, ch, limit=50)

    asyncio.create_task(hourly_rescrape())

    log.info("✅ Bot actif - En attente de nouveaux codes Xtream...")
    await client.run_until_disconnected()

if __name__ == "__main__":
    asyncio.run(main())
