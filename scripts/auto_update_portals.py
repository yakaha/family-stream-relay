#!/usr/bin/env python3
"""
Family Stream - Découverte Telegram, Vérificateur Vidéo & Purge Auto Xtream
============================================================================
S'exécute via GitHub Actions (cron 6h ou bouton Admin Web) ou en local :
1. Charge les portails existants (xtream_portals.json) et les sources autorisées
   fournies par l'administrateur (AUTHORIZED_PORTALS / authorized_portals.txt/.json).
2. Découvre de nouveaux serveurs sur les chaînes Telegram publiques (DEFAULT_TELEGRAM_SOURCES
   + variable optionnelle TELEGRAM_SOURCES) avec décodage HTML propre (html.unescape)
   pour éviter toute corruption &amp; / &password=.
3. Vérifie réellement chaque serveur avant publication :
   - Identifiants propres et hôtes autorisés
   - Authentification API Xtream multi User-Agent + statut Active + non expiré
   - Présence de catégories et chaînes sportives
   - Lecture vidéo réelle MPEG-TS (.ts) vérifiant le cadrage 0x47 (pas de page HTML)
4. Purge les serveurs morts, expirés ou sans flux vidéo, en protégeant la liste
   existante contre les pannes réseau globales.
5. Trie avec les nouveaux serveurs certifiés en tête et sauvegarde en JSON compact.
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
MAX_WORKERS = 12
HTTP_TIMEOUT = 7.0
STREAMS_TIMEOUT = 12.0
STREAM_TIMEOUT = 7.0
TELEGRAM_HTTP_TIMEOUT = 8.0
TELEGRAM_MAX_PAGES = 3
MAX_PER_DOMAIN = 2

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


def parse_authorized_text(content: str) -> list[dict]:
    """Parse une liste texte/HTML (get.php, host|user|pass, blocs HOST/USER/PASS ou triplets)."""
    results = []
    if not content:
        return results

    decoded = html_module.unescape(content)

    for raw_url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', decoded, re.I):
        cred = parse_get_php_url(raw_url)
        if cred:
            results.append(cred)

    for line in decoded.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "|" in line and line.startswith(("http://", "https://")):
            parts = [p.strip() for p in line.split("|")]
            if len(parts) >= 3 and has_valid_credentials(parts[0].rstrip("/"), parts[1], parts[2]):
                results.append({"host": parts[0].rstrip("/"), "username": parts[1], "password": parts[2]})
            continue
        m_triplet = re.match(
            r'^(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@!#$%*]+)$',
            line,
        )
        if m_triplet:
            h, u, p = m_triplet.group(1).rstrip("/"), m_triplet.group(2).strip(), m_triplet.group(3).strip()
            if has_valid_credentials(h, u, p):
                results.append({"host": h, "username": u, "password": p})

    for block in re.split(r"━{4,}|={4,}|\n\s*\n", decoded):
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
    """Construit la liste des canaux Telegram publics (défauts + env TELEGRAM_SOURCES)."""
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


def parse_telegram_html(html_text: str) -> list[dict]:
    """Extrait les identifiants depuis une page t.me/s/... (liens href + texte messages)."""
    if not html_text:
        return []
    candidates = []
    seen_local = set()

    def _add(items: list[dict]):
        for it in items:
            k = (it["host"].rstrip("/").lower(), it["username"])
            if k not in seen_local:
                seen_local.add(k)
                candidates.append(it)

    # 1. Liens get.php éventuellement présents directement dans les balises <a>
    for raw_url in re.findall(r'https?://[^\s"\'<>\n]+/get\.php\?[^\s"\'<>\n]+', html_text, re.I):
        cred = parse_get_php_url(raw_url)
        if cred:
            _add([cred])

    # 2. Corps de chaque message Telegram (avec conversion <br> et décodage entités HTML)
    messages = re.findall(
        r'<div class="[^"]*tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>',
        html_text,
        re.DOTALL,
    )
    for msg_html in messages:
        cleaned = re.sub(r"<br\s*/?>", "\n", msg_html, flags=re.I)
        cleaned = re.sub(r"<[^>]+>", "\n", cleaned)
        _add(parse_authorized_text(cleaned))

    return candidates


def load_telegram_inputs() -> list[dict]:
    """Scrape les canaux Telegram publics configurés (jusqu'à TELEGRAM_MAX_PAGES pages)."""
    sources = get_telegram_sources()
    discovered = []
    seen = set()

    for base_url in sources:
        url = base_url
        visited_before = set()
        src_count = 0

        for page_idx in range(TELEGRAM_MAX_PAGES):
            try:
                resp = requests.get(
                    url,
                    timeout=TELEGRAM_HTTP_TIMEOUT,
                    headers=BROWSER_HEADERS,
                )
                if resp.status_code != 200 or not resp.text:
                    break
                page_cands = parse_telegram_html(resp.text)
                for c in page_cands:
                    k = (c["host"].rstrip("/").lower(), c["username"])
                    if k not in seen:
                        seen.add(k)
                        discovered.append(c)
                        src_count += 1

                # Pagination vers les messages juste précédents (?before=...)
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

        print(f"   • Telegram {base_url} → {src_count} candidat(s) unique(s)", flush=True)

    return discovered


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
    """Sélectionne des chaînes de test fiables (beIN/Canal/FR/AR d'abord) pour éviter les faux négatifs."""
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
    """Retourne (is_valid, label_fr) depuis exp_date Xtream."""
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


def certify_and_extract_server(server: dict) -> dict | None:
    """Teste complètement un serveur Xtream (auth, expiration, sport, lecture vidéo MPEG-TS réelle)."""
    h = str(server.get("host", "")).rstrip("/")
    u = str(server.get("username", "")).strip()
    p = str(server.get("password", "")).strip()

    if not has_valid_credentials(h, u, p):
        return None

    clean_host = h.replace("http://", "").replace("https://", "").split(":")[0].lower()
    blocked_extra = {
        x.strip().lower()
        for x in re.split(r"[\n,;]+", os.environ.get("BLACKLISTED_HOSTS", ""))
        if x.strip()
    }
    if clean_host in blocked_extra or any(x in clean_host for x in ["adult", "porn", "xxx", "myultratv"]):
        return None

    base = f"{h}/player_api.php?username={urllib.parse.quote(u)}&password={urllib.parse.quote(p)}"

    try:
        # 1. Auth & expiration check (multi User-Agent)
        d = None
        working_ua = STREAM_USER_AGENTS[0]
        for ua in STREAM_USER_AGENTS:
            try:
                rinfo = requests.get(base, timeout=HTTP_TIMEOUT, headers={"User-Agent": ua}, verify=False)
                if rinfo.status_code != 200:
                    continue
                parsed = rinfo.json()
                if isinstance(parsed, dict) and "user_info" in parsed:
                    d = parsed
                    working_ua = ua
                    break
            except Exception:
                continue

        if not isinstance(d, dict):
            return None

        uinfo = d.get("user_info", {})
        if (
            not isinstance(uinfo, dict)
            or str(uinfo.get("auth", "")) != "1"
            or str(uinfo.get("status", "")).lower() != "active"
        ):
            return None

        exp_ok, expires_label = _format_expiry_label(uinfo.get("exp_date"))
        if not exp_ok:
            return None

        headers = {"User-Agent": working_ua}

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

        # 3. Live streams check (retry with alternate UA on empty response / transient timeout)
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
            return None

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
            return None

        # 4. Real video MPEG-TS test before publication
        video_ok = any(
            verify_live_mpegts(f"{h}/live/{u}/{p}/{sid}.ts", preferred_ua=working_ua)
            for sid in sample_stream_ids
        )
        if not video_ok:
            return None

        # 5. Build complete sport categories sorted with AR & FR on top (no channel truncation)
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
            "expires_str": expires_label,
            "max_connections": max_conn,
            "total_categories": len(built_categories),
            "total_sport_channels": total_sport_ch,
            "categories": built_categories,
        }
    except Exception:
        return None


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


def main():
    print("=== DÉMARRAGE DU SCANNER TELEGRAM & VÉRIFICATEUR VIDÉO XTREAM ===", flush=True)
    t_start = time.time()

    current_portals = []
    if os.path.exists(PORTALS_FILE):
        try:
            with open(PORTALS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                current_portals = loaded
            print(f"[1/5] Serveurs actuellement enregistrés : {len(current_portals)}", flush=True)
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

    print("[2/5] Chargement des sources autorisées supplémentaires...", flush=True)
    auth_cands = load_authorized_inputs()
    auth_new = 0
    for c in auth_cands:
        k = (c["host"].rstrip("/").lower(), c["username"])
        if k not in seen:
            seen.add(k)
            candidate_pool.append(c)
            auth_new += 1
    print(f"   → {auth_new} candidat(s) autorisé(s) ajouté(s)", flush=True)

    print("[3/5] Découverte automatique sur les chaînes Telegram publiques...", flush=True)
    tg_cands = load_telegram_inputs()
    tg_new = 0
    for c in tg_cands:
        k = (c["host"].rstrip("/").lower(), c["username"])
        if k not in seen:
            seen.add(k)
            candidate_pool.append(c)
            tg_new += 1
    print(f"   → {tg_new} nouveau(x) candidat(s) Telegram • Total à vérifier : {len(candidate_pool)}", flush=True)

    if not candidate_pool:
        print("Aucun candidat à vérifier — fichier conservé tel quel.", flush=True)
        return

    print(f"[4/5] Vérification complète avant publication (auth + sport + flux vidéo MPEG-TS, max {MAX_WORKERS} threads)...", flush=True)
    raw_certified = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(certify_and_extract_server, s): s for s in candidate_pool}
        for fut in as_completed(futures):
            res = fut.result()
            if res:
                raw_certified.append(res)

    # Prioriser les serveurs multi-connexions et les plus riches AVANT la limite par domaine
    raw_certified.sort(
        key=lambda s: (s["max_connections"], s["total_sport_channels"]),
        reverse=True,
    )

    certified = []
    seen_hosts = {}
    for res in raw_certified:
        clean_host = res["host"].replace("http://", "").replace("https://", "").split(":")[0].lower()
        host_count = seen_hosts.get(clean_host, 0)
        if host_count < MAX_PER_DOMAIN:
            seen_hosts[clean_host] = host_count + 1
            certified.append(res)

    # Protection anti-effacement accidentel sur incident réseau global
    if not certified and len(current_portals) > 0:
        print(
            "⚠️ 0 serveur vérifié sur ce run alors que la liste contenait déjà des entrées — "
            "sauvegarde ignorée pour protéger xtream_portals.json contre un incident réseau.",
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

    # Trier les nouveaux serveurs vérifiés en tête, puis connexions et richesse sport
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

    print(f"[5/5] Publication JSON compacte dans {PORTALS_FILE}...", flush=True)
    with open(PORTALS_FILE, "w", encoding="utf-8") as f:
        json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    assets_file = os.path.join("app", "src", "main", "assets", "xtream_portals.json")
    if os.path.exists(os.path.dirname(assets_file)):
        with open(assets_file, "w", encoding="utf-8") as f:
            json.dump(final_list, f, ensure_ascii=False, separators=(",", ":"))

    elapsed = time.time() - t_start
    size_kb = os.path.getsize(PORTALS_FILE) / 1024
    print(
        f"=== TERMINÉ EN {elapsed:.1f}s : {len(final_list)} SERVEURS CERTIFIÉS VIDÉO ({new_added_count} nouveau(x)) | Fichier: {size_kb:.0f} KB ===",
        flush=True,
    )


def key_is_new(existing_added_at: dict, server: dict) -> bool:
    key = (server["host"].rstrip("/").lower(), server["username"])
    return key not in existing_added_at


if __name__ == "__main__":
    main()
