#!/usr/bin/env python3
"""
Family Stream - Xtream Pool Assignment API
==========================================
Endpoints HTTP pour l'assignation exclusive de serveurs Xtream aux utilisateurs.
Intégré dans server.py sur le Codespace.

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
import sys
import time
import urllib.request
from datetime import datetime, timezone, timedelta

try:
    import firebase_admin
    from firebase_admin import credentials, firestore
    FIREBASE_OK = True
except ImportError:
    FIREBASE_OK = False

log = logging.getLogger("xtream_assign")

FIREBASE_PROJECT = "familysteam-09"
POOL_COLLECTION  = "xtream_pool"
SESSION_TTL      = 4 * 3600  # 4 heures max par session
_db = None

def _get_db():
    global _db
    if _db:
        return _db
    if not FIREBASE_OK:
        return None
    if not firebase_admin._apps:
        cred_path = os.path.join(os.path.dirname(__file__), "firebase_service_key.json")
        if os.path.exists(cred_path):
            cred = credentials.Certificate(cred_path)
        else:
            cred = credentials.ApplicationDefault()
        firebase_admin.initialize_app(cred, {"projectId": FIREBASE_PROJECT})
    _db = firestore.client()
    return _db

# ──────────────────────────────────────────────────────────────────────────────

def assign_stream(user_pin: str, channel_key: str) -> dict:
    """
    Assigne exclusivement un serveur à un utilisateur pour une chaîne donnée.
    
    - channel_key: ex. "bein_1", "bein_2_fr", "canal_sport"
    - Retourne: {"success": True, "stream_url": "...", "server_id": "..."}
    """
    db = _get_db()
    if not db:
        return {"success": False, "error": "Firebase non configuré"}

    now = datetime.now(tz=timezone.utc)
    
    # 1. Vérifie si l'utilisateur a déjà un serveur assigné
    existing = _find_user_session(db, user_pin)
    if existing:
        doc, data = existing
        # Même chaîne → retourne l'URL en cours
        if data.get("assigned_channel") == channel_key:
            url = _build_stream_url(data, channel_key)
            return {
                "success": True,
                "stream_url": url,
                "server_id": doc.id,
                "already_assigned": True,
                "expires_in": _remaining_ttl(data)
            }
        else:
            # Chaîne différente → libère d'abord
            _release_doc(doc, data)

    # 2. Cherche un serveur disponible avec cette chaîne
    cat_field = _channel_key_to_field(channel_key)
    if not cat_field:
        return {"success": False, "error": f"Clé de chaîne inconnue: {channel_key}"}

    # Requête Firestore : serveurs disponibles ayant la chaîne demandée
    docs = list(db.collection(POOL_COLLECTION)
                  .where("status", "==", "available")
                  .stream())

    candidates = []
    for doc in docs:
        d = doc.to_dict()
        channels_dict = d.get(cat_field, {})
        if channel_key in channels_dict or _fuzzy_match_channel(channels_dict, channel_key):
            candidates.append((doc, d))

    if not candidates:
        return {
            "success": False,
            "error": f"Aucun serveur disponible pour {channel_key}",
            "pool_size": len(docs)
        }

    # 3. Prend le meilleur candidat (moins de connexions actives)
    candidates.sort(key=lambda x: x[1].get("active_connections", 0))
    doc, data = candidates[0]

    # 4. Assignation atomique via transaction
    @firestore.transactional
    def _do_assign(transaction, ref):
        snap = ref.get(transaction=transaction)
        if not snap.exists:
            raise Exception("Document supprimé")
        d = snap.to_dict()
        if d.get("status") != "available":
            raise Exception("Déjà assigné")
        transaction.update(ref, {
            "status": "in_use",
            "assigned_to": user_pin,
            "assigned_channel": channel_key,
            "assigned_at": now.isoformat(),
        })

    transaction = db.transaction()
    try:
        _do_assign(transaction, doc.reference)
    except Exception as e:
        # Race condition → cherche un autre serveur
        for doc2, data2 in candidates[1:]:
            try:
                transaction2 = db.transaction()
                _do_assign(transaction2, doc2.reference)
                doc, data = doc2, data2
                break
            except Exception:
                continue
        else:
            return {"success": False, "error": "Tous les serveurs disponibles ont été assignés simultanément"}

    stream_url = _build_stream_url(data, channel_key)
    log.info(f"✅ Assigné: pin={user_pin} → {channel_key} @ {data['host']}")

    return {
        "success": True,
        "stream_url": stream_url,
        "server_id": doc.id,
        "server_name": f"{data['host']} ({data['username']})",
        "channel_name": _get_channel_name(data, channel_key),
        "expires_in": SESSION_TTL,
        "max_connections": data.get("max_connections", 1),
    }


def release_stream(user_pin: str) -> dict:
    """Libère le serveur assigné à un utilisateur"""
    db = _get_db()
    if not db:
        return {"success": False, "error": "Firebase non configuré"}

    existing = _find_user_session(db, user_pin)
    if not existing:
        return {"success": True, "message": "Aucune session active"}

    doc, data = existing
    _release_doc(doc, data)
    log.info(f"🔓 Libéré: pin={user_pin}")
    return {"success": True, "message": "Session libérée"}


def swap_stream(user_pin: str, channel_key: str) -> dict:
    """
    Change de serveur pour la même chaîne (ou une autre).
    Libère le serveur actuel et en assigne un nouveau différent.
    """
    db = _get_db()
    if not db:
        return {"success": False, "error": "Firebase non configuré"}

    # Mémorise le serveur actuel pour l'exclure
    current_server_id = None
    existing = _find_user_session(db, user_pin)
    if existing:
        doc, data = existing
        current_server_id = doc.id
        _release_doc(doc, data)

    # Assigne un nouveau serveur
    result = assign_stream(user_pin, channel_key)
    if result.get("success") and result.get("server_id") == current_server_id:
        # Même serveur → libère et cherche le suivant
        release_stream(user_pin)
        result = assign_stream(user_pin, channel_key)

    return result


def get_session_status(user_pin: str) -> dict:
    """Retourne l'état de la session d'un utilisateur"""
    db = _get_db()
    if not db:
        return {"success": False, "error": "Firebase non configuré"}

    existing = _find_user_session(db, user_pin)
    if not existing:
        return {"success": True, "has_session": False}

    doc, data = existing
    return {
        "success": True,
        "has_session": True,
        "server_id": doc.id,
        "channel": data.get("assigned_channel"),
        "stream_url": _build_stream_url(data, data.get("assigned_channel", "")),
        "expires_in": _remaining_ttl(data),
    }


def get_pool_stats() -> dict:
    """Statistiques du pool (admin)"""
    db = _get_db()
    if not db:
        return {"error": "Firebase non configuré"}

    docs = list(db.collection(POOL_COLLECTION).stream())
    total = len(docs)
    available = sum(1 for d in docs if d.to_dict().get("status") == "available")
    in_use = sum(1 for d in docs if d.to_dict().get("status") == "in_use")

    bein_coverage = {}
    for doc in docs:
        d = doc.to_dict()
        for key in d.get("bein_channels", {}):
            bein_coverage[key] = bein_coverage.get(key, 0) + 1

    return {
        "total_servers": total,
        "available": available,
        "in_use": in_use,
        "bein_channels_coverage": bein_coverage,
        "last_check": datetime.now(tz=timezone.utc).isoformat(),
    }

# ──────────────────────────────────────────────────────────────────────────────
#  HELPERS INTERNES
# ──────────────────────────────────────────────────────────────────────────────

def _find_user_session(db, user_pin: str):
    """Cherche si un utilisateur a une session active"""
    docs = list(db.collection(POOL_COLLECTION)
                  .where("assigned_to", "==", user_pin)
                  .limit(1)
                  .stream())
    if docs:
        d = docs[0].to_dict()
        # Vérifie que la session n'est pas expirée
        if d.get("assigned_at"):
            try:
                assigned_at = datetime.fromisoformat(d["assigned_at"])
                now = datetime.now(tz=timezone.utc)
                if (now - assigned_at).total_seconds() > SESSION_TTL:
                    _release_doc(docs[0], d)
                    return None
            except Exception:
                pass
        return docs[0], d
    return None


def _release_doc(doc, data):
    """Libère un document d'assignation"""
    try:
        doc.reference.update({
            "status": "available",
            "assigned_to": None,
            "assigned_channel": None,
            "assigned_at": None,
        })
    except Exception as e:
        log.warning(f"Erreur libération: {e}")


def _channel_key_to_field(channel_key: str) -> str | None:
    """Retourne le champ Firestore correspondant à une clé de chaîne"""
    if channel_key.endswith("_fr"):
        return "bein_fr_channels"
    if channel_key.startswith("bein_"):
        return "bein_channels"
    if channel_key.startswith("canal_") or channel_key.startswith("rmc_"):
        return "canal_channels"
    return None


def _fuzzy_match_channel(channels_dict: dict, channel_key: str) -> bool:
    """Correspondance approximative si la clé exacte n'existe pas"""
    num = re.search(r'\d+', channel_key)
    if not num:
        return False
    n = num.group(0)
    for key in channels_dict:
        if re.search(r'\b' + n + r'\b', key):
            return True
    return False


def _build_stream_url(server_data: dict, channel_key: str) -> str:
    """Construit l'URL de flux HLS directe pour une chaîne"""
    host = server_data.get("host", "").rstrip('/')
    user = server_data.get("username", "")
    pwd  = server_data.get("password", "")

    # Cherche le stream_id dans les dictionnaires
    all_channels = {}
    all_channels.update(server_data.get("bein_channels", {}))
    all_channels.update(server_data.get("bein_fr_channels", {}))
    all_channels.update(server_data.get("canal_channels", {}))

    ch_info = all_channels.get(channel_key)
    if not ch_info:
        # Fuzzy match
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

    # Fallback: URL générique
    return f"{host}/live/{user}/{pwd}/1.m3u8"


def _get_channel_name(server_data: dict, channel_key: str) -> str:
    """Retourne le nom lisible d'une chaîne"""
    for field in ["bein_channels", "bein_fr_channels", "canal_channels"]:
        ch = server_data.get(field, {}).get(channel_key)
        if ch:
            return ch.get("name", channel_key)
    return channel_key


def _remaining_ttl(data: dict) -> int:
    """Secondes restantes avant expiration de session"""
    try:
        assigned_at = datetime.fromisoformat(data["assigned_at"])
        elapsed = (datetime.now(tz=timezone.utc) - assigned_at).total_seconds()
        return max(0, int(SESSION_TTL - elapsed))
    except Exception:
        return SESSION_TTL
