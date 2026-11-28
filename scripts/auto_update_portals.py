#!/usr/bin/env python3
"""
Family Stream - Découverte Multi-Sources (Telegram, Sites Web, Fichiers),
Vérificateur Vidéo Xtream (prioritaire) & MAC Portal, et Purge Auto
==========================================================================
1. Lit les portails existants (xtream_portals.json), les sources autorisées locales/env,
   et la config Admin Firestore (`settings/bot_sources` : sites personnalisés,
   candidats extraits de fichiers Xtream/MAC, priorité & mode auto/manuel).
2. Découvre de nouveaux serveurs depuis Telegram (`DEFAULT_TELEGRAM_SOURCES` +
   `TELEGRAM_SOURCES`), les sites web fournis et les fichiers/listes importés.
3. Vérifie en priorité les serveurs Xtream :
   - Identifiants propres, auth multi User-Agent, statut Active, expiration
   - Catégories & chaînes sportives non vides
   - Lecture vidéo réelle MPEG-TS (.ts) vérifiant le cadrage binaire 0x47
4. Vérifie ensuite les éventuels portails MAC (Stalker handshake, profil, chaînes, flux).
5. Publie dans `xtream_portals.json` selon `publishMode` (`auto` ou `manual`) tout en
   purgeant les anciens serveurs morts, et envoie le rapport détaillé (OK / échecs)
   vers Firestore `settings/bot_scan_report` pour la sélection dans l'Admin Web.
"""

import hashlib
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
FIRESTORE_BASE_URL = (
    os.environ.get("FIRESTORE_SETTINGS_URL", "").strip().rstrip("/")
    or "https://firestore.googleapis.com/v1/projects/familysteam-09/databases/(default)/documents"
)

MAX_WORKERS = 12
MAC_WORKERS = 6
HTTP_TIMEOUT = 7.0
STREAMS_TIMEOUT = 12.0
STREAM_TIMEOUT = 7.0
TELEGRAM_HTTP_TIMEOUT = 8.0
SITE_HTTP_TIMEOUT = 10.0
TELEGRAM_MAX_PAGES = 3
MAX_PER_DOMAIN = 2
MAX_REPORT_ITEMS = 220

DEFAULT_TELEGRAM_SOURCES = [
    "https://t.me/s/Xtream_Iptv_Code",
    "https://t.me/s/VORTEXIPTVSUITE",
    "https://t.me/s/xtream_code_iptv",
    "https://t.me/s/FreeIPTVxtream",
]

SPORT_POSITIVE = [
    "SPORT", "BEIN", "CANAL", "FOOT", "RMC", "DAZN", "EUROSPORT",
    "LALIGA", "CALCIO", "SKY SPORT", "TNT SPORT", "LIGUE 1", "MATCH", "TOD", "SSC",
]

NEGATIVE_KEYWORDS = [
    "CINEMA", "FILMS", "MOVIES", "SERIES", "ADULT", "XXX", "KIDS",
    "ANIMATION", "DOCUMENTAIRE", "INFO", "NEWS", "MUSIC", "RADIO", "VOD",
    "ENTERTAINMENT",
]

STREAM_USER_AGENTS = [
    "VLC/3.0.18 LibVLC/3.0.18",
    "IPTVSmartersPro",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
]

BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    )
}

MAG_USER_AGENT = (
    "Mozilla/5.0 (QtEmbedded; U; Linux; C) "
    "AppleWebKit/533.3 (KHTML, like Gecko) MAG200 stbapp ver: 2 rev: 250 Safari/533.3"
)

MAC_RE = re.compile(r"\b([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})\b")


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
    """Extrait host/username/password d'une URL get.php avec décodage HTML."""
    url = html_module.unescape(raw_url.strip())
    url = re.sub(r'[),.;\]]+$', '', url)
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


def _normalize_portal_url(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not u.startswith(("http://", "https://")):
        u = "http://" + u
    return u.rstrip("/")


def extract_mixed_from_text(content: str, source_label: str = "") -> dict:
    """Extrait à la fois les candidats Xtream et les portails MAC depuis du texte ou HTML."""
    xtream = []
    macs = []
    seen_x = set()
    seen_m = set()

    if not content:
        return {"xtream": xtream, "mac": macs}

    decoded = html_module.unescape(content)

    def _add_xtream(h: str, u: str, p: str):
        hc = h.rstrip("/")
        uc, pc = u.strip(), p.strip()
        if not has_valid_credentials(hc, uc, pc):
            return
        k = (hc.lower(), uc)
        if k not in seen_x:
            seen_x.add(k)
            xtream.append({"host": hc, "username": uc, "password": pc, "source": source_label})

    def _add_mac(url: str, mac: str, exp: str = ""):
        nu = _normalize_portal_url(url)
        m_match = MAC_RE.search(mac or "")
        if not nu or not m_match:
            return
        nm = m_match.group(1).upper()
        k = (nu.lower(), nm)
        if k not in seen_m:
            seen_m.add(k)
            macs.append({"url": nu, "mac": nm, "expiration": exp.strip(), "source": source_label})

    # 1. Liens get.php
    for raw_url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', decoded, re.I):
        cred = parse_get_php_url(raw_url)
        if cred:
            _add_xtream(cred["host"], cred["username"], cred["password"])

    # 2. Lignes pipe, triplets ou couples URL + MAC sur la même ligne
    for line in decoded.splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        if "|" in s and s.startswith(("http://", "https://")):
            parts = [p.strip() for p in s.split("|")]
            if len(parts) >= 2 and MAC_RE.fullmatch(parts[1]):
                exp = parts[3] if len(parts) >= 4 else ""
                _add_mac(parts[0], parts[1], exp)
                continue
            if len(parts) >= 3:
                _add_xtream(parts[0], parts[1], parts[2])
                continue
        m_triplet = re.match(
            r'^(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@!#$%*]+)$',
            s,
        )
        if m_triplet:
            _add_xtream(m_triplet.group(1), m_triplet.group(2), m_triplet.group(3))
            continue
        m_inline_mac = re.search(r'(https?://[^\s"\'<>|]+)', s, re.I)
        m_mac = MAC_RE.search(s)
        if m_inline_mac and m_mac and "get.php" not in s.lower():
            _add_mac(m_inline_mac.group(1), m_mac.group(1))

    # 3. Blocs multi-lignes (MAC+URL ou HOST+USER+PASS), avec flush auto si blocs adjacents
    def _flush_block(h_val, u_val, p_val, mac_v, exp_v):
        if h_val and mac_v:
            _add_mac(h_val, mac_v, exp_v)
        elif h_val and u_val and p_val:
            _add_xtream(h_val, u_val, p_val)

    for block in re.split(r"━{4,}|={4,}|\n\s*\n", decoded):
        h, u, p, mac_val, exp_val = None, None, None, None, ""
        for line in block.splitlines():
            ls = line.strip()
            mh = re.search(r"(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)", ls, re.I)
            if mh:
                if h and (mac_val or (u and p)):
                    _flush_block(h, u, p, mac_val, exp_val)
                    h, u, p, mac_val, exp_val = None, None, None, None, ""
                h = mh.group(1).rstrip("/")
                continue
            mu = re.search(r"(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)", ls, re.I)
            if mu:
                if u and h and p:
                    _flush_block(h, u, p, mac_val, exp_val)
                    h, u, p, mac_val, exp_val = None, None, None, None, ""
                u = mu.group(1).strip()
                continue
            mp = re.search(r"(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)", ls, re.I)
            if mp:
                p = mp.group(1).strip()
                continue
            mm = re.search(r"MAC\s*[:|=]\s*([0-9A-Fa-f]{2}(?::[0-9A-Fa-f]{2}){5})", ls, re.I)
            if mm:
                if mac_val and h:
                    _flush_block(h, u, p, mac_val, exp_val)
                    h, u, p, mac_val, exp_val = None, None, None, None, ""
                mac_val = mm.group(1).upper()
                continue
            me = re.search(r"(?:EXP|EXPIRATION|EXPIRE)\s*[:|=]\s*(.+)$", ls, re.I)
            if me:
                exp_val = me.group(1).strip()
        _flush_block(h, u, p, mac_val, exp_val)

    return {"xtream": xtream, "mac": macs}


def parse_authorized_text(content: str) -> list[dict]:
    """Parse une liste texte/HTML Xtream (compatibilité avec les appels existants)."""
    return extract_mixed_from_text(content).get("xtream", [])


def _normalize_telegram_source(src: str) -> str | None:
    s = (src or "").strip()
    if not s:
        return None
    if s.startswith("@"):
        s = "https://t.me/s/" + s[1:].strip("/")
    elif re.match(r"^https?://t\.me/[^/]+/?$", s, re.I):
        slug = s.rstrip("/").split("/")[-1]
        s = f"https://t.me/s/{slug}"
    elif re.match(r"^t\.me/s/[^/?#]+", s, re.I) or re.match(r"^t\.me/[^/?#]+", s, re.I):
        slug = s.rstrip("/").split("/")[-1]
        s = f"https://t.me/s/{slug}"
    if not s.startswith(("http://", "https://")):
        return None
    return s.split("?")[0].rstrip("/")


def get_telegram_sources() -> list[str]:
    raw_list = list(DEFAULT_TELEGRAM_SOURCES)
    extra = os.environ.get("TELEGRAM_SOURCES", "").strip()
    if extra:
        for token in re.split(r"[\n,;]+", extra):
            if token.strip():
                raw_list.append(token.strip())

    normalized = []
    seen_urls = set()
    for item in raw_list:
        u = _normalize_telegram_source(item)
        if u and u.lower() not in seen_urls:
            seen_urls.add(u.lower())
            normalized.append(u)
    return normalized


def parse_telegram_html(html_text: str, source_label: str = "Telegram") -> list[dict]:
    """Extrait les identifiants Xtream depuis une page t.me/s/... (compatibilité directe)."""
    return parse_telegram_html_mixed(html_text, source_label=source_label).get("xtream", [])


def parse_telegram_html_mixed(html_text: str, source_label: str = "Telegram") -> dict:
    if not html_text:
        return {"xtream": [], "mac": []}
    xtream = []
    macs = []
    seen_x = set()
    seen_m = set()

    def _merge(bundle: dict):
        for it in bundle.get("xtream", []):
            k = (it["host"].rstrip("/").lower(), it["username"])
            if k not in seen_x:
                seen_x.add(k)
                xtream.append(it)
        for it in bundle.get("mac", []):
            k = (it["url"].rstrip("/").lower(), it["mac"].upper())
            if k not in seen_m:
                seen_m.add(k)
                macs.append(it)

    for raw_url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', html_text, re.I):
        cred = parse_get_php_url(raw_url)
        if cred:
            cred["source"] = source_label
            _merge({"xtream": [cred], "mac": []})

    messages = re.findall(
        r'<div class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        html_text,
        re.DOTALL,
    )
    for msg_html in messages:
        cleaned = re.sub(r"<br\s*/?>", "\n", msg_html, flags=re.I)
        cleaned = re.sub(r"<[^>]+>", "\n", cleaned)
        _merge(extract_mixed_from_text(cleaned, source_label=source_label))

    return {"xtream": xtream, "mac": macs}


def _scrape_telegram_url(base_url: str) -> dict:
    xtream = []
    macs = []
    seen_x = set()
    seen_m = set()
    url = base_url
    visited_before = set()

    for page_idx in range(TELEGRAM_MAX_PAGES):
        try:
            resp = requests.get(url, timeout=TELEGRAM_HTTP_TIMEOUT, headers=BROWSER_HEADERS)
            if resp.status_code != 200 or not resp.text:
                break
            bundle = parse_telegram_html_mixed(resp.text, source_label=base_url)
            for c in bundle["xtream"]:
                k = (c["host"].rstrip("/").lower(), c["username"])
                if k not in seen_x:
                    seen_x.add(k)
                    xtream.append(c)
            for c in bundle["mac"]:
                k = (c["url"].rstrip("/").lower(), c["mac"])
                if k not in seen_m:
                    seen_m.add(k)
                    macs.append(c)

            before_ids = [int(x) for x in re.findall(r"\?before=(\d+)", resp.text)]
            if not before_ids:
                break
            next_before = min(before_ids)
            if next_before in visited_before or next_before <= 1:
                break
            visited_before.add(next_before)
            url = f"{base_url}?before={next_before}"
        except Exception as e:
            print(f"   ⚠️ Telegram {base_url} page {page_idx + 1}: {e}", flush=True)
            break

    return {"xtream": xtream, "mac": macs}


def load_telegram_inputs() -> list[dict]:
    discovered = []
    seen = set()
    for base_url in get_telegram_sources():
        res = _scrape_telegram_url(base_url)
        cnt = 0
        for c in res["xtream"]:
            k = (c["host"].rstrip("/").lower(), c["username"])
            if k not in seen:
                seen.add(k)
                discovered.append(c)
                cnt += 1
        print(f"   • Telegram {base_url} → {cnt} candidat(s) unique(s)", flush=True)
    return discovered


def load_custom_sites_inputs(custom_sites: list[str]) -> dict:
    """Explore les sites Web et liens Telegram personnalisés fournis depuis l'Admin Web."""
    xtream = []
    macs = []
    seen_x = set()
    seen_m = set()

    for raw_site in custom_sites:
        site = str(raw_site or "").strip()
        if not site:
            continue
        if "t.me/" in site.lower() or site.startswith("@"):
            norm_tg = _normalize_telegram_source(site)
            if not norm_tg:
                continue
            bundle = _scrape_telegram_url(norm_tg)
        else:
            if not site.startswith(("http://", "https://")):
                site = "https://" + site
            try:
                resp = requests.get(site, timeout=SITE_HTTP_TIMEOUT, headers=BROWSER_HEADERS, verify=False)
                if resp.status_code != 200 or not resp.text:
                    print(f"   ⚠️ Site {site} → HTTP {resp.status_code}", flush=True)
                    continue
                raw_html = resp.text
                hrefs = "\n".join(re.findall(r'href=["\']([^"\']+)["\']', raw_html, re.I))
                plain = re.sub(r"<(script|style)[^>]*>[\s\S]*?</\1>", "\n", raw_html, flags=re.I)
                plain = re.sub(r"<br\s*/?>", "\n", plain, flags=re.I)
                plain = re.sub(r"<[^>]+>", "\n", plain)
                bundle = extract_mixed_from_text(hrefs + "\n" + plain, source_label=site)
            except Exception as e:
                print(f"   ⚠️ Site {site} → erreur: {e}", flush=True)
                continue

        x_new, m_new = 0, 0
        for c in bundle.get("xtream", []):
            k = (c["host"].rstrip("/").lower(), c["username"])
            if k not in seen_x:
                seen_x.add(k)
                xtream.append(c)
                x_new += 1
        for c in bundle.get("mac", []):
            k = (c["url"].rstrip("/").lower(), c["mac"])
            if k not in seen_m:
                seen_m.add(k)
                macs.append(c)
                m_new += 1
        print(f"   • Site {site} → {x_new} Xtream + {m_new} MAC extraits", flush=True)

    return {"xtream": xtream, "mac": macs}


def load_authorized_inputs() -> list[dict]:
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
                                candidates.append({"host": h, "username": u, "password": p, "source": "authorized"})
            except Exception as e:
                print(f"   ⚠️ AUTHORIZED_PORTALS JSON invalide: {e}", flush=True)
        else:
            for c in parse_authorized_text(env_val):
                c["source"] = "authorized"
                candidates.append(c)

    if os.path.exists(AUTHORIZED_TXT_FILE):
        with open(AUTHORIZED_TXT_FILE, "r", encoding="utf-8", errors="ignore") as f:
            for c in parse_authorized_text(f.read()):
                c["source"] = AUTHORIZED_TXT_FILE
                candidates.append(c)

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
                            candidates.append({"host": h, "username": u, "password": p, "source": AUTHORIZED_JSON_FILE})
        except Exception as e:
            print(f"   ⚠️ Erreur lecture {AUTHORIZED_JSON_FILE}: {e}", flush=True)

    return candidates


def _fs_decode_val(val: dict):
    if not isinstance(val, dict):
        return None
    if "stringValue" in val:
        return val["stringValue"]
    if "booleanValue" in val:
        return bool(val["booleanValue"])
    if "integerValue" in val:
        return int(val["integerValue"])
    if "doubleValue" in val:
        return float(val["doubleValue"])
    if "nullValue" in val:
        return None
    if "arrayValue" in val:
        return [_fs_decode_val(x) for x in val["arrayValue"].get("values", [])]
    if "mapValue" in val:
        fields = val["mapValue"].get("fields", {})
        return {k: _fs_decode_val(v) for k, v in fields.items()}
    return None


def _fs_encode_val(val) -> dict:
    if val is None:
        return {"nullValue": None}
    if isinstance(val, bool):
        return {"booleanValue": val}
    if isinstance(val, int):
        return {"integerValue": str(val)}
    if isinstance(val, float):
        return {"doubleValue": val}
    if isinstance(val, str):
        return {"stringValue": val}
    if isinstance(val, list):
        return {"arrayValue": {"values": [_fs_encode_val(x) for x in val]}}
    if isinstance(val, dict):
        return {"mapValue": {"fields": {str(k): _fs_encode_val(v) for k, v in val.items()}}}
    return {"stringValue": str(val)}


def load_firestore_bot_sources() -> dict:
    default_cfg = {
        "customSites": [],
        "uploadedCandidates": [],
        "publishMode": "auto",
        "scanPriority": "xtream_first",
    }
    try:
        r = requests.get(f"{FIRESTORE_BASE_URL}/settings/bot_sources", timeout=8.0)
        if r.status_code != 200:
            return default_cfg
        fields = (r.json() or {}).get("fields", {})
        decoded = {k: _fs_decode_val(v) for k, v in fields.items()}
        return {
            "customSites": decoded.get("customSites") if isinstance(decoded.get("customSites"), list) else [],
            "uploadedCandidates": decoded.get("uploadedCandidates") if isinstance(decoded.get("uploadedCandidates"), list) else [],
            "publishMode": str(decoded.get("publishMode") or "auto").lower(),
            "scanPriority": str(decoded.get("scanPriority") or "xtream_first").lower(),
        }
    except Exception as e:
        print(f"   ⚠️ Firestore bot_sources indisponible ({e}) — mode par défaut conservé.", flush=True)
        return default_cfg


def save_firestore_bot_report(report: dict):
    try:
        fields = {k: _fs_encode_val(v) for k, v in report.items()}
        r = requests.patch(
            f"{FIRESTORE_BASE_URL}/settings/bot_scan_report",
            json={"fields": fields},
            timeout=10.0,
        )
        if r.ok:
            print("   ✓ Rapport détaillé synchronisé dans Firestore (settings/bot_scan_report)", flush=True)
        else:
            print(f"   ⚠️ Échec écriture rapport Firestore: HTTP {r.status_code}", flush=True)
    except Exception as e:
        print(f"   ⚠️ Erreur écriture rapport Firestore: {e}", flush=True)


def looks_like_mpegts(buf: bytes) -> bool:
    """Vérifie la présence d'un vrai flux MPEG-TS (octet de synchro 0x47 espacé de 188 octets)."""
    n = len(buf)
    if n < 188:
        return False
    for i in range(n - 188):
        if buf[i] == 0x47 and buf[i + 188] == 0x47:
            return True
    return False


def verify_live_mpegts(stream_url: str, preferred_ua: str | None = None) -> bool:
    """Vérifie qu'un flux .ts délivre réellement >= 1 Ko de paquets vidéo MPEG-TS valides."""
    ua_list = []
    if preferred_ua:
        ua_list.append(preferred_ua)
    for ua in STREAM_USER_AGENTS:
        if ua not in ua_list:
            ua_list.append(ua)

    for ua in ua_list[:2]:
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
                ctype = (resp.headers.get("Content-Type") or "").lower()
                if any(bad in ctype for bad in ("text/html", "application/json", "text/plain", "application/xml")):
                    continue
                buf = bytearray()
                for chunk in resp.iter_content(chunk_size=4096):
                    if chunk:
                        buf.extend(chunk)
                        if len(buf) >= 4096:
                            break
                if len(buf) >= 1024 and looks_like_mpegts(bytes(buf)):
                    return True
        except Exception:
            continue
    return False


def _pick_sample_streams(sport_streams: list[dict], limit: int = 4) -> list[str]:
    prio_ids = []
    other_ids = []
    for s in sport_streams:
        sid = str(s.get("id", "")).strip()
        up = str(s.get("name", "")).upper()
        if not sid:
            continue
        if any(k in up for k in ("BEIN", "CANAL", "RMC", "DAZN", "EUROSPORT")):
            prio_ids.append(sid)
        else:
            other_ids.append(sid)
    return (prio_ids + other_ids)[:limit]


def _format_expiry_label(exp_raw) -> tuple[bool, str]:
    if exp_raw is None:
        return True, "Actif • Illimité (Certifié Vidéo)"
    s = str(exp_raw).strip()
    if not s or s.lower() in ("null", "none"):
        return True, "Actif • Illimité (Certifié Vidéo)"
    try:
        exp_ts = int(float(s))
        if exp_ts <= 0:
            return True, "Actif (Certifié Vidéo)"
        if exp_ts < int(time.time()):
            return False, "Expiré"
        dt_str = datetime.fromtimestamp(exp_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        return True, f"Actif • Exp {dt_str} (Certifié Vidéo)"
    except Exception:
        return True, "Actif (Certifié Vidéo)"


def certify_xtream_with_reason(server: dict) -> tuple[dict | None, str]:
    """Teste complètement un serveur Xtream et retourne (résultat | None, raison)."""
    h = str(server.get("host", "")).rstrip("/")
    u = str(server.get("username", "")).strip()
    p = str(server.get("password", "")).strip()

    if not has_valid_credentials(h, u, p):
        return None, "Identifiants invalides"

    clean_host = h.replace("http://", "").replace("https://", "").split(":")[0].lower()
    blocked_extra = {
        x.strip().lower()
        for x in re.split(r"[\n,;]+", os.environ.get("BLACKLISTED_HOSTS", ""))
        if x.strip()
    }
    if clean_host in blocked_extra or any(x in clean_host for x in ["adult", "porn", "xxx", "myultratv"]):
        return None, "Hôte filtré / blacklisté"

    base = f"{h}/player_api.php?username={urllib.parse.quote(u)}&password={urllib.parse.quote(p)}"

    try:
        # 1. Auth & expiration check (multi User-Agent)
        d = None
        working_ua = STREAM_USER_AGENTS[0]
        last_auth_err = "Connexion impossible"
        for ua in STREAM_USER_AGENTS:
            try:
                rinfo = requests.get(base, timeout=HTTP_TIMEOUT, headers={"User-Agent": ua}, verify=False)
                if rinfo.status_code != 200:
                    last_auth_err = f"HTTP {rinfo.status_code}"
                    continue
                parsed = rinfo.json()
                if isinstance(parsed, dict) and "user_info" in parsed:
                    d = parsed
                    working_ua = ua
                    break
                last_auth_err = "Réponse non-Xtream"
            except requests.exceptions.Timeout:
                last_auth_err = "Timeout API"
            except Exception as e:
                last_auth_err = type(e).__name__

        if not isinstance(d, dict):
            return None, last_auth_err

        uinfo = d.get("user_info", {})
        if not isinstance(uinfo, dict) or str(uinfo.get("auth", "")) != "1":
            return None, "Authentification refusée (auth=0)"
        st = str(uinfo.get("status", "")).lower()
        if st != "active":
            return None, f"Compte inactif ({st or 'unknown'})"

        exp_ok, expires_label = _format_expiry_label(uinfo.get("exp_date"))
        if not exp_ok:
            return None, "Compte expiré"

        # 2. Sport categories check
        rcat = requests.get(
            f"{base}&action=get_live_categories",
            timeout=HTTP_TIMEOUT,
            headers={"User-Agent": working_ua},
            verify=False,
        )
        if rcat.status_code != 200:
            return None, f"Catégories HTTP {rcat.status_code}"
        all_cats = rcat.json()
        if not isinstance(all_cats, list) or not all_cats:
            return None, "Aucune catégorie live"
        sport_cats = {}
        for c in all_cats:
            cid = str(c.get("category_id", "")).strip()
            cname = str(c.get("category_name", "")).strip()
            if cid and cname and is_sport(cname):
                sport_cats[cid] = cname
        if not sport_cats:
            return None, "0 catégorie sportive"

        # 3. Live streams check
        all_streams = None
        for ua_try in [working_ua] + [x for x in STREAM_USER_AGENTS if x != working_ua]:
            try:
                rstr = requests.get(
                    f"{base}&action=get_live_streams",
                    timeout=STREAMS_TIMEOUT,
                    headers={"User-Agent": ua_try},
                    verify=False,
                )
                if rstr.status_code != 200:
                    continue
                parsed_streams = rstr.json()
                if isinstance(parsed_streams, list) and parsed_streams:
                    all_streams = parsed_streams
                    working_ua = ua_try
                    break
            except Exception:
                continue
        if not isinstance(all_streams, list) or not all_streams:
            return None, "Catalogue chaînes live vide"

        cat_to_channels = {cid: [] for cid in sport_cats.keys()}
        sport_flat = []

        for stream in all_streams:
            cid = str(stream.get("category_id", "")).strip()
            if cid not in cat_to_channels:
                continue
            sid = str(stream.get("stream_id", "")).strip()
            sname = str(stream.get("name", "")).strip()
            if not sid or is_garbage_channel(sname):
                continue
            ch_entry = {"id": sid, "name": sname}
            sport_flat.append(ch_entry)
            cat_to_channels[cid].append(ch_entry)

        sample_stream_ids = _pick_sample_streams(sport_flat, limit=4)
        if not sample_stream_ids:
            return None, "0 chaîne sportive valide"

        # 4. Real video MPEG-TS test before publication
        video_ok = any(
            verify_live_mpegts(f"{h}/live/{u}/{p}/{sid}.ts", preferred_ua=working_ua)
            for sid in sample_stream_ids
        )
        if not video_ok:
            return None, "Échec lecture flux vidéo MPEG-TS"

        # 5. Build complete sport categories sorted with AR & FR on top
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
            return None, "0 catégorie sportive exploitable"

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
            "expires_str": expires_label,
            "max_connections": max_conn,
            "total_categories": len(built_categories),
            "total_sport_channels": total_sport_ch,
            "categories": built_categories,
        }, "OK (Flux MPEG-TS certifié)"
    except requests.exceptions.Timeout:
        return None, "Timeout"
    except Exception as e:
        return None, type(e).__name__


def certify_and_extract_server(server: dict) -> dict | None:
    """Compatibilité ascendante : retourne uniquement le dict certifié ou None."""
    res, _ = certify_xtream_with_reason(server)
    return res


def _build_stalker_candidates(portal_url: str) -> list[str]:
    u = _normalize_portal_url(portal_url)
    if not u:
        return []
    p = urllib.parse.urlparse(u)
    origin = f"{p.scheme}://{p.netloc}"
    path = (p.path or "").rstrip("/")
    if path.endswith(".php"):
        return [u]
    cands = []
    if "/stalker_portal" in path:
        cands.append(f"{origin}/stalker_portal/server/load.php")
        cands.append(f"{origin}/portal.php")
    else:
        cands.append(f"{origin}/portal.php")
        cands.append(f"{origin}/stalker_portal/server/load.php")
    return cands


def _parse_json_loose(resp) -> dict | None:
    try:
        return resp.json()
    except Exception:
        txt = (resp.text or "").strip()
        idx = txt.find("{")
        if idx >= 0:
            try:
                return json.loads(txt[idx:])
            except Exception:
                return None
    return None


def verify_mac_portal_with_reason(portal_url: str, mac: str) -> tuple[bool, str, dict]:
    """Vérifie un portail MAC Stalker (handshake, profil, chaînes, lien flux réel)."""
    meta = {"channels": 0, "expiration": ""}
    nm = (mac or "").strip().upper()
    cands = _build_stalker_candidates(portal_url)
    if not cands or not MAC_RE.fullmatch(nm):
        return False, "URL ou MAC invalide", meta

    sn = hashlib.md5(nm.encode()).hexdigest().upper()[:13]
    dev_id = hashlib.sha256(sn.encode()).hexdigest().upper()
    dev_id2 = hashlib.sha256(nm.encode()).hexdigest().upper()
    hw2 = hashlib.sha1(nm.encode()).hexdigest()

    cookies = {
        "mac": nm,
        "sn": sn,
        "device_id": dev_id,
        "device_id2": dev_id2,
        "hw_version": "1.7-BD-00",
        "stb_lang": "en",
        "timezone": "Europe/Paris",
        "adid": hw2,
    }
    headers = {
        "User-Agent": MAG_USER_AGENT,
        "X-User-Agent": "Model: MAG250; Link: WiFi",
        "Accept": "*/*",
    }

    sess = requests.Session()
    endpoint = ""
    token = ""
    rand_val = None
    last_err = "Handshake échoué"

    for cand in cands:
        origin_root = f"{urllib.parse.urlparse(cand).scheme}://{urllib.parse.urlparse(cand).netloc}"
        headers["Referer"] = f"{origin_root}/c/"
        try:
            r = sess.get(f"{cand}?type=stb&action=handshake&token=&JsHttpRequest=1-xml", cookies=cookies, headers=headers, timeout=6.0, verify=False)
            if r.status_code != 200:
                last_err = f"Handshake HTTP {r.status_code}"
                continue
            j = _parse_json_loose(r)
            js = (j or {}).get("js") if isinstance(j, dict) else None
            tok = str((js or {}).get("token") or "").strip()
            if tok:
                endpoint = cand
                token = tok
                rand_val = (js or {}).get("random")
                break
            last_err = "Pas de token STB"
        except requests.exceptions.Timeout:
            last_err = "Timeout handshake"
        except Exception as e:
            last_err = type(e).__name__

    if not endpoint or not token:
        return False, last_err, meta

    cookies["token"] = token
    headers["Authorization"] = f"Bearer {token}"
    if rand_val:
        headers["X-Random"] = str(rand_val)

    try:
        rp = sess.get(
            f"{endpoint}?type=stb&action=get_profile&hd=1&auth_second_step=1&JsHttpRequest=1-xml",
            cookies=cookies,
            headers=headers,
            timeout=6.0,
            verify=False,
        )
        if rp.status_code == 200:
            pj = _parse_json_loose(rp)
            if isinstance(pj, dict) and pj.get("error"):
                return False, f"Profil bloqué: {str(pj.get('error'))[:40]}", meta
            pjs = (pj or {}).get("js") if isinstance(pj, dict) else {}
            if isinstance(pjs, dict):
                if str(pjs.get("blocked") or "") == "1":
                    return False, "Compte MAC bloqué (blocked=1)", meta
                exp = str(pjs.get("expire_billing_date") or pjs.get("expire_date") or pjs.get("end_date") or "").strip()
                if exp and not exp.startswith("0000-00-00"):
                    meta["expiration"] = exp
    except Exception:
        pass

    channels = []
    try:
        rc = sess.get(f"{endpoint}?type=itv&action=get_all_channels&JsHttpRequest=1-xml", cookies=cookies, headers=headers, timeout=8.0, verify=False)
        cj = _parse_json_loose(rc) if rc.status_code == 200 else None
        data_list = ((cj or {}).get("js") or {}).get("data") if isinstance(cj, dict) else None
        if isinstance(data_list, list):
            channels = data_list
    except Exception:
        pass

    if not channels:
        try:
            rc2 = sess.get(f"{endpoint}?type=itv&action=get_ordered_list&genre=*&p=1&JsHttpRequest=1-xml", cookies=cookies, headers=headers, timeout=8.0, verify=False)
            cj2 = _parse_json_loose(rc2) if rc2.status_code == 200 else None
            data2 = ((cj2 or {}).get("js") or {}).get("data") if isinstance(cj2, dict) else None
            if isinstance(data2, list):
                channels = data2
        except Exception:
            pass

    if not channels:
        return False, "0 chaîne retournée", meta
    meta["channels"] = len(channels)

    # Probe stream link on up to 3 channels
    for ch in channels[:3]:
        cmd = str(ch.get("cmd") or "").strip()
        if not cmd:
            continue
        stream_url = ""
        if cmd.startswith(("http://", "https://")) and "/ch/" not in cmd and "localhost" not in cmd:
            stream_url = cmd
        else:
            try:
                rl = sess.get(
                    f"{endpoint}?type=itv&action=create_link&cmd={urllib.parse.quote(cmd, safe='')}&series=&forced_storage=false&disable_ad=false&download=false&JsHttpRequest=1-xml",
                    cookies=cookies,
                    headers=headers,
                    timeout=6.0,
                    verify=False,
                )
                lj = _parse_json_loose(rl) if rl.status_code == 200 else None
                raw_cmd = str(((lj or {}).get("js") or {}).get("cmd") or "").strip()
                m_u = re.search(r"https?://[^\s]+", raw_cmd)
                if m_u:
                    stream_url = m_u.group(0)
            except Exception:
                continue
        if not stream_url or "stream=&" in stream_url:
            continue
        try:
            with sess.get(stream_url, cookies=cookies, headers=headers, timeout=6.0, stream=True, verify=False) as rs:
                if rs.status_code != 200:
                    continue
                ctype = (rs.headers.get("Content-Type") or "").lower()
                if "text/html" in ctype or "application/json" in ctype:
                    continue
                buf = bytearray()
                for chunk in rs.iter_content(chunk_size=4096):
                    if chunk:
                        buf.extend(chunk)
                        if len(buf) >= 2048:
                            break
                if len(buf) >= 512 and (looks_like_mpegts(bytes(buf)) or b"#EXTM3U" in bytes(buf)):
                    return True, f"OK ({len(channels)} ch • flux vidéo valide)", meta
        except Exception:
            continue

    return False, f"Auth OK ({len(channels)} ch) mais flux vidéo KO", meta


def _parse_added_at_ts(added_at: str) -> float:
    if not added_at:
        return 0.0
    s = str(added_at).strip().rstrip("Z")
    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc).timestamp()
        except Exception:
            continue
    return 0.0


def key_is_new(existing_added_at: dict, server: dict) -> bool:
    key = (server["host"].rstrip("/").lower(), server["username"])
    return key not in existing_added_at


def main():
    print("=== DÉMARRAGE DU SCANNER MULTI-SOURCES & VÉRIFICATEUR XTREAM / MAC ===", flush=True)
    t_start = time.time()

    current_portals = []
    if os.path.exists(PORTALS_FILE):
        try:
            with open(PORTALS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                current_portals = loaded
            print(f"[1/6] Serveurs actuellement enregistrés : {len(current_portals)}", flush=True)
        except Exception as e:
            print(f"Erreur lecture {PORTALS_FILE}: {e}", flush=True)

    existing_added_at = {}
    existing_keys = set()
    authorized_keys = set()
    xtream_pool = []
    seen_x = set()
    mac_pool = []
    seen_m = set()

    def _push_x(item: dict, mark_auth: bool = False):
        h = str(item.get("host", "")).rstrip("/")
        u = str(item.get("username", "")).strip()
        p = str(item.get("password", "")).strip()
        if not has_valid_credentials(h, u, p):
            return
        k = (h.lower(), u)
        if mark_auth:
            authorized_keys.add(k)
        if k not in seen_x:
            seen_x.add(k)
            xtream_pool.append({
                "host": h,
                "username": u,
                "password": p,
                "source": item.get("source") or "Inconnu",
            })

    def _push_m(item: dict):
        u = _normalize_portal_url(item.get("url", ""))
        m = str(item.get("mac", "")).strip().upper()
        if not u or not MAC_RE.fullmatch(m):
            return
        k = (u.lower(), m)
        if k not in seen_m:
            seen_m.add(k)
            mac_pool.append({
                "url": u,
                "mac": m,
                "expiration": str(item.get("expiration") or "").strip(),
                "source": item.get("source") or "Fichier / Site",
            })

    for portal in current_portals:
        h = str(portal.get("host", "")).rstrip("/")
        u = str(portal.get("username", "")).strip()
        p_val = str(portal.get("password", "")).strip()
        if not has_valid_credentials(h, u, p_val):
            print(f"   [PURGÉ CORROMPU] {h} user={u[:30]}", flush=True)
            continue
        k = (h.lower(), u)
        existing_keys.add(k)
        if portal.get("added_at") and str(portal.get("added_at")) != "None":
            existing_added_at[k] = portal["added_at"]
        _push_x({"host": h, "username": u, "password": p_val, "source": "Pool existant"})

    print("[2/6] Chargement config Admin Firestore & sources autorisées...", flush=True)
    bot_cfg = load_firestore_bot_sources()
    publish_mode = bot_cfg["publishMode"] if bot_cfg["publishMode"] in ("auto", "manual") else "auto"
    scan_priority = bot_cfg["scanPriority"] if bot_cfg["scanPriority"] in ("xtream_first", "xtream_only", "all") else "xtream_first"
    print(f"   → Mode publication : {publish_mode} | Priorité : {scan_priority} | Sites: {len(bot_cfg['customSites'])} | Fichiers/extraits: {len(bot_cfg['uploadedCandidates'])}", flush=True)

    for c in load_authorized_inputs():
        _push_x(c, mark_auth=True)

    for up in bot_cfg["uploadedCandidates"]:
        if not isinstance(up, dict):
            continue
        kind = str(up.get("kind") or "").lower()
        if kind == "mac":
            _push_m({"url": up.get("url", ""), "mac": up.get("mac", ""), "expiration": up.get("expiration", ""), "source": "Fichier Admin"})
        else:
            _push_x({"host": up.get("host") or up.get("url") or "", "username": up.get("username", ""), "password": up.get("password", ""), "source": "Fichier Admin"})

    print("[3/6] Découverte Telegram & Sites Web personnalisés...", flush=True)
    for c in load_telegram_inputs():
        _push_x(c)
    if bot_cfg["customSites"]:
        site_res = load_custom_sites_inputs(bot_cfg["customSites"])
        for c in site_res["xtream"]:
            _push_x(c)
        for c in site_res["mac"]:
            _push_m(c)

    print(f"   → Total candidats à vérifier : {len(xtream_pool)} Xtream (prioritaire) + {len(mac_pool)} MAC Portal", flush=True)

    # [4/6] Vérification prioritaire Xtream
    print(f"[4/6] Vérification Xtream (auth + sport + vidéo MPEG-TS, max {MAX_WORKERS} threads)...", flush=True)
    raw_certified = []
    xtream_report_items = []

    if xtream_pool:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            futures = {ex.submit(certify_xtream_with_reason, s): s for s in xtream_pool}
            for fut in as_completed(futures):
                cand = futures[fut]
                res, reason = fut.result()
                k = (cand["host"].rstrip("/").lower(), cand["username"])
                is_existing = k in existing_keys
                clean_host = cand["host"].replace("http://", "").replace("https://", "").split(":")[0]
                if res:
                    raw_certified.append(res)
                    if not is_existing:
                        xtream_report_items.append({
                            "kind": "xtream",
                            "ok": True,
                            "reason": reason,
                            "name": clean_host,
                            "url": res["host"],
                            "username": res["username"],
                            "password": res["password"],
                            "mac": "",
                            "expiration": res["expires_str"],
                            "channels": res["total_sport_channels"],
                            "maxConnections": res["max_connections"],
                            "source": cand.get("source") or "Bot",
                        })
                else:
                    if not is_existing:
                        xtream_report_items.append({
                            "kind": "xtream",
                            "ok": False,
                            "reason": reason,
                            "name": clean_host,
                            "url": cand["host"],
                            "username": cand["username"],
                            "password": cand["password"],
                            "mac": "",
                            "expiration": "",
                            "channels": 0,
                            "maxConnections": 1,
                            "source": cand.get("source") or "Bot",
                        })

    # Décider quels Xtream sont publiés dans xtream_portals.json (auto vs manual)
    publishable_raw = []
    for res in raw_certified:
        k = (res["host"].rstrip("/").lower(), res["username"])
        if publish_mode == "auto" or k in existing_keys or k in authorized_keys:
            publishable_raw.append(res)

    publishable_raw.sort(
        key=lambda s: (s["max_connections"], s["total_sport_channels"]),
        reverse=True,
    )

    certified = []
    seen_hosts = {}
    for res in publishable_raw:
        clean_host = res["host"].replace("http://", "").replace("https://", "").split(":")[0].lower()
        host_count = seen_hosts.get(clean_host, 0)
        if host_count < MAX_PER_DOMAIN:
            seen_hosts[clean_host] = host_count + 1
            certified.append(res)

    if not certified and len(current_portals) > 0:
        print(
            "⚠️ 0 serveur Xtream vérifié alors que la liste contenait déjà des entrées — "
            "sauvegarde ignorée pour protéger xtream_portals.json.",
            flush=True,
        )
        sys.exit(1)

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M")
    enriched = []
    new_added_count = 0
    for s in certified:
        key = (s["host"].rstrip("/").lower(), s["username"])
        prev_added = existing_added_at.get(key)
        if not prev_added:
            new_added_count += 1
        added_at = prev_added or now_str
        enriched.append((s, added_at))

    enriched.sort(
        key=lambda item: (
            _parse_added_at_ts(item[1]),
            item[0]["max_connections"],
            item[0]["total_sport_channels"],
        ),
        reverse=True,
    )

    final_list = []
    for idx, (s, added_at) in enumerate(enriched, 1):
        clean_host = s["host"].replace("http://", "").replace("https://", "").split(":")[0]
        tag = "NOUVEAU" if key_is_new(existing_added_at, s) else "EXISTANT"
        print(
            f"   [PUBLIÉ {tag}] #{idx} {clean_host} ({s['total_sport_channels']} ch • max {s['max_connections']} • ajouté {added_at})",
            flush=True,
        )
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

    # [5/6] Vérification secondaire MAC Portal (si demandé et si candidats présents)
    mac_report_items = []
    if mac_pool and scan_priority != "xtream_only":
        print(f"[5/6] Vérification MAC Portals ({len(mac_pool)} candidats, {MAC_WORKERS} threads)...", flush=True)
        with ThreadPoolExecutor(max_workers=MAC_WORKERS) as ex:
            mfutures = {ex.submit(verify_mac_portal_with_reason, m["url"], m["mac"]): m for m in mac_pool}
            for fut in as_completed(mfutures):
                m = mfutures[fut]
                ok, reason, meta = fut.result()
                clean_host = m["url"].replace("http://", "").replace("https://", "").split("/")[0].split(":")[0]
                mac_report_items.append({
                    "kind": "mac",
                    "ok": bool(ok),
                    "reason": reason,
                    "name": clean_host,
                    "url": m["url"],
                    "username": "",
                    "password": "",
                    "mac": m["mac"],
                    "expiration": meta.get("expiration") or m.get("expiration") or "",
                    "channels": int(meta.get("channels") or 0),
                    "maxConnections": 1,
                    "source": m.get("source") or "Bot",
                })
    else:
        print("[5/6] Vérification MAC Portals ignorée (priorité Xtream seul ou 0 candidat MAC).", flush=True)

    # Priorité Xtream OK -> MAC OK -> Xtream KO -> MAC KO
    all_report_items = xtream_report_items + mac_report_items
    all_report_items.sort(
        key=lambda x: (
            1 if x["kind"] == "xtream" else 0,
            1 if x["ok"] else 0,
            int(x.get("channels") or 0),
        ),
        reverse=True,
    )

    report_payload = {
        "generatedAt": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "publishMode": publish_mode,
        "scanPriority": scan_priority,
        "xtreamOk": sum(1 for x in xtream_report_items if x["ok"]),
        "xtreamFail": sum(1 for x in xtream_report_items if not x["ok"]),
        "macOk": sum(1 for x in mac_report_items if x["ok"]),
        "macFail": sum(1 for x in mac_report_items if not x["ok"]),
        "items": all_report_items[:MAX_REPORT_ITEMS],
    }
    save_firestore_bot_report(report_payload)

    print(f"[6/6] Sauvegarde compacte dans {PORTALS_FILE}...", flush=True)
    with open(PORTALS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    assets_file = os.path.join("app", "src", "main", "assets", "xtream_portals.json")
    if os.path.exists(os.path.dirname(assets_file)):
        with open(assets_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    elapsed = time.time() - t_start
    size_kb = os.path.getsize(PORTALS_FILE) / 1024
    print(
        f"=== TERMINÉ EN {elapsed:.1f}s : {len(final_list)} XTREAM PUBLIÉS ({new_added_count} nouveau(x)) | "
        f"Rapport: {report_payload['xtreamOk']} Xtream OK, {report_payload['macOk']} MAC OK | Fichier: {size_kb:.0f} KB ===",
        flush=True,
    )


if __name__ == "__main__":
    main()
