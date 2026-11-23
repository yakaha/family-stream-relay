#!/usr/bin/env python3
"""
Family Stream - Xtream Pool Assignment API
==========================================
Endpoints HTTP pour l'assignation exclusive de serveurs Xtream aux utilisateurs.
Stockage : fichier JSON local xtream_pool.json (pas besoin de Firebase)

Routes:
  GET  /api/pool/assign?pin=USER_PIN&channel=bein_1   → assigne un serveur exclusif
  GET  /api/pool/release?pin=USER_PIN                 → libère le serveur
  GET  /api/pool/swap?pin=USER_PIN&channel=bein_1     → change de serveur
  GET  /api/pool/status?pin=USER_PIN                  → état de la session
  GET  /api/pool/stats                                 → stats du pool (admin)
"""

import json
import logging
import os
import re
import threading
from datetime import datetime, timezone, timedelta

log = logging.getLogger("xtream_assign")

POOL_FILE   = os.path.join(os.path.dirname(__file__), "xtream_pool.json")
POOL_LOCK   = threading.Lock()
SESSION_TTL = 4 * 3600  # 4 heures max par session

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
        with open(POOL_FILE, "w", encoding="utf-8") as f:
            json.dump(pool, f, ensure_ascii=False, indent=2)

# ══════════════════════════════════════════════════════════════════════════════
#  API PUBLIQUE
# ══════════════════════════════════════════════════════════════════════════════

def assign_stream(user_pin: str, channel_key: str) -> dict:
    """
    Assigne exclusivement un serveur à un utilisateur pour une chaîne donnée.
    Retourne: {"success": True, "stream_url": "...", "server_id": "..."}
    """
    now = datetime.now(tz=timezone.utc)
    pool = _load_pool()

    if not pool:
        return {"success": False, "error": "Pool vide - Le bot n'a pas encore trouvé de serveurs", "pool_size": 0}

    # 1. Vérifie si l'utilisateur a déjà un serveur assigné
    for doc_id, d in pool.items():
        if d.get("assigned_to") == user_pin:
            # Vérifie que la session n'est pas expirée
            try:
                assigned_at = datetime.fromisoformat(d["assigned_at"])
                if (now - assigned_at).total_seconds() < SESSION_TTL:
                    # Même chaîne → retourne l'URL en cours
                    if d.get("assigned_channel") == channel_key:
                        return {
                            "success": True,
                            "stream_url": _build_stream_url(d, channel_key),
                            "server_id": doc_id,
                            "already_assigned": True,
                            "channel_name": _get_channel_name(d, channel_key),
                            "expires_in": _remaining_ttl(d),
                        }
                    else:
                        # Chaîne différente → libère
                        _release_in_pool(pool, doc_id)
                        break
            except Exception:
                _release_in_pool(pool, doc_id)
                break

    # 2. Cherche un serveur disponible avec cette chaîne
    cat_field = _channel_key_to_field(channel_key)
    if not cat_field:
        return {"success": False, "error": f"Chaîne inconnue: {channel_key}"}

    candidates = []
    for doc_id, d in pool.items():
        if d.get("status") != "available":
            continue
        channels_dict = d.get(cat_field, {})
        if channel_key in channels_dict or _fuzzy_match_channel(channels_dict, channel_key):
            candidates.append((doc_id, d))

    if not candidates:
        available_total = sum(1 for d in pool.values() if d.get("status") == "available")
        return {
            "success": False,
            "error": f"Aucun serveur disponible pour {channel_key}",
            "pool_size": len(pool),
            "available_servers": available_total,
        }

    # 3. Prend le meilleur candidat (moins de connexions actives)
    candidates.sort(key=lambda x: x[1].get("active_connections", 0))
    doc_id, data = candidates[0]

    # 4. Assignation
    pool[doc_id]["status"] = "in_use"
    pool[doc_id]["assigned_to"] = user_pin
    pool[doc_id]["assigned_channel"] = channel_key
    pool[doc_id]["assigned_at"] = now.isoformat()
    _save_pool(pool)

    stream_url = _build_stream_url(pool[doc_id], channel_key)
    log.info(f"✅ Assigné: pin={user_pin} → {channel_key} @ {data['host']}")

    return {
        "success": True,
        "stream_url": stream_url,
        "server_id": doc_id,
        "server_name": f"{data['host']} ({data['username']})",
        "channel_name": _get_channel_name(data, channel_key),
        "expires_in": SESSION_TTL,
        "max_connections": data.get("max_connections", 1),
    }


def release_stream(user_pin: str) -> dict:
    """Libère le serveur assigné à un utilisateur"""
    pool = _load_pool()
    released = False
    for doc_id, d in pool.items():
        if d.get("assigned_to") == user_pin:
            _release_in_pool(pool, doc_id)
            released = True
            break
    if released:
        _save_pool(pool)
        log.info(f"🔓 Libéré: pin={user_pin}")
    return {"success": True, "message": "Session libérée" if released else "Aucune session active"}


def swap_stream(user_pin: str, channel_key: str) -> dict:
    """Change de serveur pour la même chaîne. Libère l'actuel et en assigne un nouveau."""
    pool = _load_pool()
    current_server_id = None

    # Trouve et libère le serveur actuel
    for doc_id, d in pool.items():
        if d.get("assigned_to") == user_pin:
            current_server_id = doc_id
            _release_in_pool(pool, doc_id)
            break
    _save_pool(pool)

    # Assigne un nouveau (différent du précédent si possible)
    result = assign_stream(user_pin, channel_key)

    # Si même serveur et qu'il y en a d'autres, essaie le suivant
    if result.get("success") and result.get("server_id") == current_server_id:
        pool2 = _load_pool()
        # Libère le même serveur et cherche le suivant
        _release_in_pool(pool2, current_server_id)
        _save_pool(pool2)

        # Essaie les autres candidats
        cat_field = _channel_key_to_field(channel_key)
        candidates = [
            (did, d) for did, d in pool2.items()
            if d.get("status") == "available"
            and did != current_server_id
            and (channel_key in d.get(cat_field, {}) or _fuzzy_match_channel(d.get(cat_field, {}), channel_key))
        ]
        if candidates:
            result = assign_stream(user_pin, channel_key)
        else:
            # Pas d'autre choix, reprend le même
            result = assign_stream(user_pin, channel_key)
            if result.get("success"):
                result["message"] = "Aucun autre serveur disponible - même source"

    return result


def get_session_status(user_pin: str) -> dict:
    """Retourne l'état de la session d'un utilisateur"""
    pool = _load_pool()
    for doc_id, d in pool.items():
        if d.get("assigned_to") == user_pin:
            channel = d.get("assigned_channel", "")
            return {
                "success": True,
                "has_session": True,
                "server_id": doc_id,
                "channel": channel,
                "stream_url": _build_stream_url(d, channel),
                "expires_in": _remaining_ttl(d),
            }
    return {"success": True, "has_session": False}


def get_pool_stats() -> dict:
    """Statistiques du pool"""
    pool = _load_pool()
    total = len(pool)
    available = sum(1 for d in pool.values() if d.get("status") == "available")
    in_use = sum(1 for d in pool.values() if d.get("status") == "in_use")

    bein_coverage = {}
    for d in pool.values():
        for key in d.get("bein_channels", {}):
            bein_coverage[key] = bein_coverage.get(key, 0) + 1

    return {
        "total_servers": total,
        "available": available,
        "in_use": in_use,
        "bein_channels_coverage": bein_coverage,
        "last_check": datetime.now(tz=timezone.utc).isoformat(),
        "pool_file": POOL_FILE,
    }

# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS INTERNES
# ══════════════════════════════════════════════════════════════════════════════

def _release_in_pool(pool: dict, doc_id: str):
    """Libère un serveur dans le pool (en mémoire, appeler _save_pool ensuite)"""
    if doc_id in pool:
        pool[doc_id]["status"] = "available"
        pool[doc_id]["assigned_to"] = None
        pool[doc_id]["assigned_channel"] = None
        pool[doc_id]["assigned_at"] = None


def _channel_key_to_field(channel_key: str) -> str | None:
    if channel_key.endswith("_fr"):
        return "bein_fr_channels"
    if channel_key.startswith("bein_"):
        return "bein_channels"
    if channel_key.startswith("canal_") or channel_key.startswith("rmc_"):
        return "canal_channels"
    return None


def _fuzzy_match_channel(channels_dict: dict, channel_key: str) -> bool:
    num = re.search(r'\d+', channel_key)
    if not num:
        return False
    n = num.group(0)
    for key in channels_dict:
        if re.search(r'\b' + n + r'\b', key):
            return True
    return False


def _build_stream_url(server_data: dict, channel_key: str) -> str:
    host = server_data.get("host", "").rstrip('/')
    user = server_data.get("username", "")
    pwd  = server_data.get("password", "")

    all_channels = {}
    all_channels.update(server_data.get("bein_channels", {}))
    all_channels.update(server_data.get("bein_fr_channels", {}))
    all_channels.update(server_data.get("canal_channels", {}))

    ch_info = all_channels.get(channel_key)
    if not ch_info:
        num = re.search(r'\d+', channel_key)
        if num:
            for k, v in all_channels.items():
                if re.search(r'\b' + num.group(0) + r'\b', k):
                    ch_info = v
                    break

    if ch_info:
        stream_id = ch_info.get("stream_id", "")
        if stream_id:
            return f"{host}/live/{user}/{pwd}/{stream_id}.m3u8"

    return f"{host}/live/{user}/{pwd}/1.m3u8"


def _get_channel_name(server_data: dict, channel_key: str) -> str:
    for field in ["bein_channels", "bein_fr_channels", "canal_channels"]:
        ch = server_data.get(field, {}).get(channel_key)
        if ch:
            return ch.get("name", channel_key)
    return channel_key


def _remaining_ttl(data: dict) -> int:
    try:
        assigned_at = datetime.fromisoformat(data["assigned_at"])
        elapsed = (datetime.now(tz=timezone.utc) - assigned_at).total_seconds()
        return max(0, int(SESSION_TTL - elapsed))
    except Exception:
        return SESSION_TTL
