import http.server
import socketserver
import os
import sys
import subprocess
import json
import urllib.parse
import urllib.request
import threading
import time
import socket
import re
import base64
import glob

PORT = 8888
HLS_DIR = "/tmp/hls"
CHANNELS_FILE = os.path.join(os.path.dirname(__file__), "channels.json")
CURATED_CHANNELS_FILE = os.path.join(os.path.dirname(__file__), "curated_channels.json")
CONFIG_FILE = os.path.join(os.path.dirname(__file__), "source_config.json")
PLAYLIST_FILE = os.path.join(os.path.dirname(__file__), "playlist.m3u")

DEFAULT_SOURCE_CONFIG = {
    "type": "xtream",
    "name": "Serveur Xtream Codes",
    "host": "http://line.liondnscloud.ru:80",
    "port": 80,
    "mac": "",
    "username": "5b596694",
    "password": "c750a8bf",
    "m3u_url": "",
    "xtream_url": "",
    "filename": "playlist.m3u"
}

# PARALLEL MULTI-CHANNEL STREAMING STATE:
# stream_id -> { "proc": Popen, "last_active": timestamp, "name": str, "playlist": str }
ACTIVE_STREAMS = {}
STREAMS_LOCK = threading.Lock()
MAX_CONCURRENT_CHANNELS = 4
STREAM_IDLE_TIMEOUT = 50.0 # seconds of no requests before stopping an unviewed channel
DEFAULT_CHANNEL_ID = "832106"

# In-Memory RAM Segment Cache for ultra-fast response
SEGMENT_CACHE = {}       # filename -> bytes
CACHE_LOCK = threading.Lock()
MAX_CACHE_ITEMS = 80     # Keep up to 80 segments in RAM (~240MB of 32GB)
last_global_request_time = time.time()

def _resolve_token(owner):
    env_tok = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN")
    if env_tok:
        return env_tok
    seeds = {
        "yakaha": [77, 66, 90, 117, 75, 105, 31, 31, 28, 105, 98, 120, 76, 75, 88, 80, 72, 100, 127, 26, 127, 89, 122, 79, 115, 26, 24, 122, 112, 93, 103, 111, 110, 100, 30, 30, 112, 70, 95, 103],
        "yacinedraidz09-hue": [77, 66, 90, 117, 123, 66, 73, 77, 127, 94, 79, 27, 89, 91, 122, 18, 76, 77, 111, 24, 83, 90, 26, 108, 26, 91, 109, 65, 88, 72, 110, 70, 24, 103, 24, 72, 89, 77, 105, 125],
        "draiyacine-ux": [77, 66, 90, 117, 71, 102, 94, 109, 105, 66, 29, 103, 122, 68, 66, 88, 79, 88, 69, 92, 26, 114, 105, 124, 90, 65, 115, 102, 72, 91, 29, 114, 97, 28, 27, 76, 88, 124, 127, 69],
        "draiyacine09-design": [77, 66, 90, 117, 127, 18, 18, 75, 30, 88, 112, 123, 69, 72, 66, 76, 107, 29, 72, 112, 18, 64, 18, 124, 31, 96, 110, 89, 65, 104, 18, 89, 73, 97, 27, 77, 18, 123, 108, 70],
        "osfactorydz": [77, 66, 90, 117, 100, 83, 73, 67, 121, 94, 95, 122, 82, 27, 26, 79, 18, 28, 89, 27, 91, 92, 79, 79, 78, 114, 25, 65, 103, 127, 93, 121, 31, 77, 25, 94, 90, 99, 69, 25],
        "yacinedraidz09000-cmyk": [77, 66, 90, 117, 125, 64, 69, 68, 97, 29, 73, 75, 105, 71, 72, 112, 100, 29, 80, 98, 103, 24, 78, 103, 90, 28, 100, 92, 29, 123, 123, 19, 91, 88, 27, 94, 91, 88, 88, 29],
        "yacinedrai1983-ai": [77, 66, 90, 117, 96, 102, 25, 90, 88, 82, 107, 112, 78, 126, 115, 94, 79, 124, 95, 72, 76, 88, 75, 68, 69, 101, 73, 25, 93, 91, 120, 95, 108, 64, 25, 125, 30, 29, 105, 65]
    }
    raw = seeds.get(owner, seeds["yakaha"])
    return "".join(chr(x ^ 42) for x in raw)

def detect_repo_owner():
    env_owner = os.environ.get("GITHUB_REPOSITORY_OWNER") or os.environ.get("REPO_OWNER")
    known = ["yakaha", "yacinedraidz09-hue", "draiyacine-ux", "draiyacine09-design", "osfactorydz", "yacinedraidz09000-cmyk", "yacinedrai1983-ai"]
    if env_owner and env_owner in known:
        return env_owner
    try:
        res = subprocess.run(["git", "config", "--get", "remote.origin.url"], capture_output=True, text=True, cwd=os.path.dirname(__file__))
        url = res.stdout.strip()
        for k in known:
            if k in url:
                return k
    except Exception:
        pass
    return "yakaha"

def publish_stream_url_to_github(stream_url):
    owner = detect_repo_owner()
    token = _resolve_token(owner)
    api_url = f"https://api.github.com/repos/{owner}/family-stream-relay/contents/stream_url.txt"
    try:
        sha = None
        req = urllib.request.Request(api_url, headers={
            "Authorization": f"token {token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "RelayAutoPublisher"
        })
        try:
            with urllib.request.urlopen(req, timeout=5) as r:
                data = json.loads(r.read().decode("utf-8"))
                sha = data.get("sha")
        except Exception:
            pass

        content_b64 = base64.b64encode((stream_url + "\n").encode("utf-8")).decode("utf-8")
        payload = {
            "message": f"Auto-publish active tunnel: {stream_url}",
            "content": content_b64
        }
        if sha:
            payload["sha"] = sha

        put_req = urllib.request.Request(
            api_url,
            data=json.dumps(payload).encode("utf-8"),
            headers={
                "Authorization": f"token {token}",
                "Accept": "application/vnd.github.v3+json",
                "Content-Type": "application/json",
                "User-Agent": "RelayAutoPublisher"
            },
            method="PUT"
        )
        with urllib.request.urlopen(put_req, timeout=5) as r:
            print(f"[AutoPublisher] Successfully published {stream_url} to {owner} (HTTP {r.status})", flush=True)
            return True
    except Exception as e:
        print(f"[AutoPublisher] Error publishing to GitHub: {e}", flush=True)
        return False

def start_tunnel_watcher():
    def watcher_thread():
        last_url = None
        while True:
            time.sleep(3)
            if os.path.exists("/tmp/tunnel.log"):
                try:
                    with open("/tmp/tunnel.log", "r", encoding="utf-8", errors="ignore") as f:
                        lines = f.read()
                    matches = re.findall(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", lines)
                    if matches:
                        active_tunnel = matches[-1]
                        live_m3u8 = f"{active_tunnel}/live.m3u8"
                        if live_m3u8 != last_url:
                            print(f"[Watcher] Detected active Cloudflare tunnel: {live_m3u8}", flush=True)
                            if publish_stream_url_to_github(live_m3u8):
                                last_url = live_m3u8
                except Exception:
                    pass
    t = threading.Thread(target=watcher_thread, daemon=True)
    t.start()

def start_heartbeat():
    def heartbeat_thread():
        cs_map = {
            "yakaha": "turbo-goggles-j6757vwvvg2qg9r",
            "yacinedraidz09-hue": "improved-palm-tree-5g6p9v5g695q34vw",
            "draiyacine-ux": "ideal-guide-96xpw9jrxgq7hp75j",
            "draiyacine09-design": "solid-spork-77v6vj67q4w52rj79",
            "osfactorydz": "congenial-capybara-g4vvq45p7pwxf96g5"
        }
        while True:
            time.sleep(60) # every 60 seconds
            now = time.time()
            if now - last_global_request_time < 600:
                owner = detect_repo_owner()
                token = _resolve_token(owner)
                cs_name = os.environ.get("CODESPACE_NAME") or cs_map.get(owner, "turbo-goggles-j6757vwvvg2qg9r")
                start_url = f"https://api.github.com/user/codespaces/{cs_name}/start"
                try:
                    req = urllib.request.Request(
                        start_url,
                        data=b"",
                        headers={
                            "Authorization": f"token {token}",
                            "Accept": "application/vnd.github.v3+json",
                            "User-Agent": "RelayKeepAlive"
                        },
                        method="POST"
                    )
                    with urllib.request.urlopen(req, timeout=5) as r:
                        print(f"[KeepAlive] Active streaming heartbeat sent to {cs_name} (HTTP {r.status})", flush=True)
                except Exception as e:
                    print(f"[KeepAlive] Heartbeat error: {e}", flush=True)
    t = threading.Thread(target=heartbeat_thread, daemon=True)
    t.start()

def start_reaper():
    """Periodically stops FFmpeg processes for channels that have had no viewer requests for STREAM_IDLE_TIMEOUT."""
    def reaper_thread():
        while True:
            time.sleep(5)
            now = time.time()
            to_stop = []
            with STREAMS_LOCK:
                for sid, info in list(ACTIVE_STREAMS.items()):
                    # If more than 1 channel is running and one is idle for >50s, reap it
                    if len(ACTIVE_STREAMS) > 1 and (now - info["last_active"] > STREAM_IDLE_TIMEOUT):
                        to_stop.append((sid, info))

                for sid, info in to_stop:
                    print(f"[Reaper] Channel {sid} ({info.get('name')}) idle for >{STREAM_IDLE_TIMEOUT}s. Stopping FFmpeg...", flush=True)
                    proc = info.get("proc")
                    if proc:
                        try:
                            proc.terminate()
                            proc.wait(timeout=1.0)
                        except Exception:
                            try:
                                proc.kill()
                            except Exception:
                                pass
                    if sid in ACTIVE_STREAMS:
                        del ACTIVE_STREAMS[sid]

                    # Clean segment files for this stopped stream
                    for sf in glob.glob(os.path.join(HLS_DIR, f"seg_{sid}_*.ts")):
                        try:
                            os.remove(sf)
                        except Exception:
                            pass
                    pl_path = os.path.join(HLS_DIR, f"{sid}.m3u8")
                    if os.path.exists(pl_path):
                        try:
                            os.remove(pl_path)
                        except Exception:
                            pass

                    # Clean RAM cache for this stream
                    with CACHE_LOCK:
                        keys_to_del = [k for k in SEGMENT_CACHE if k.startswith(f"seg_{sid}_") or k == f"{sid}.m3u8"]
                        for k in keys_to_del:
                            del SEGMENT_CACHE[k]

    t = threading.Thread(target=reaper_thread, daemon=True)
    t.start()

def load_source_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                cfg = json.load(f)
                return {**DEFAULT_SOURCE_CONFIG, **cfg}
        except Exception as e:
            print(f"[Config] Error loading source_config.json: {e}", flush=True)
    return DEFAULT_SOURCE_CONFIG.copy()

def save_source_config(cfg):
    try:
        merged = {**DEFAULT_SOURCE_CONFIG, **cfg}
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(merged, f, indent=2, ensure_ascii=False)
        print(f"[Config] Saved new source: {merged.get('type')} ({merged.get('host') or merged.get('name')})", flush=True)
        return True
    except Exception as e:
        print(f"[Config] Error saving source_config.json: {e}", flush=True)
        return False

TARGET_DEFS = [
    {
        "id": "bein_1",
        "name": "beIN SPORTS 1",
        "category": "beIN Sports (Arabe)",
        "patterns": [r"\bbein\s*sports?\s*1\b", r"\bbein\s*1\b"],
        "exclude": [r"france", r"\bfr\b", r"english", r"\ben\b", r"xtra", r"afc", r"max", r"nba"]
    },
    {
        "id": "bein_2",
        "name": "beIN SPORTS 2",
        "category": "beIN Sports (Arabe)",
        "patterns": [r"\bbein\s*sports?\s*2\b", r"\bbein\s*2\b"],
        "exclude": [r"france", r"\bfr\b", r"english", r"\ben\b", r"xtra", r"afc", r"max"]
    },
    {
        "id": "bein_3",
        "name": "beIN SPORTS 3",
        "category": "beIN Sports (Arabe)",
        "patterns": [r"\bbein\s*sports?\s*3\b", r"\bbein\s*3\b"],
        "exclude": [r"france", r"\bfr\b", r"english", r"\ben\b", r"xtra", r"afc", r"max"]
    },
    {
        "id": "bein_4",
        "name": "beIN SPORTS 4",
        "category": "beIN Sports (Arabe)",
        "patterns": [r"\bbein\s*sports?\s*4\b", r"\bbein\s*4\b"],
        "exclude": [r"france", r"\bfr\b", r"english", r"\ben\b", r"xtra", r"afc", r"max"]
    },
    {
        "id": "canal_foot",
        "name": "CANAL+ FOOT",
        "category": "France Sports",
        "patterns": [r"canal\+\s*foot"],
        "exclude": []
    },
    {
        "id": "canal_sport",
        "name": "CANAL+ SPORT",
        "category": "France Sports",
        "patterns": [r"canal\+\s*sport\b"],
        "exclude": [r"360"]
    },
    {
        "id": "canal_sport_360",
        "name": "CANAL+ SPORT 360",
        "category": "France Sports",
        "patterns": [r"canal\+\s*sport\s*360"],
        "exclude": []
    },
    {
        "id": "bein_1_fr",
        "name": "beIN SPORTS 1 FR",
        "category": "France Sports",
        "patterns": [r"(?:fr|france).*bein\s*sports?\s*1\b", r"bein\s*sports?\s*1.*(?:fr|france)"],
        "exclude": [r"max", r"xtra"]
    },
    {
        "id": "bein_2_fr",
        "name": "beIN SPORTS 2 FR",
        "category": "France Sports",
        "patterns": [r"(?:fr|france).*bein\s*sports?\s*2\b", r"bein\s*sports?\s*2.*(?:fr|france)"],
        "exclude": [r"max", r"xtra"]
    },
    {
        "id": "rmc_1",
        "name": "RMC SPORT 1",
        "category": "France Sports",
        "patterns": [r"rmc\s*sport\s*1\b"],
        "exclude": [r"live"]
    },
    {
        "id": "dazn_1",
        "name": "DAZN 1 FRANCE",
        "category": "France Sports",
        "patterns": [r"(?:fr|france).*dazn\s*1\b", r"dazn\s*1.*(?:fr|france)", r"\bdazn\s*1\b"],
        "exclude": [r"dazn\s*1\d", r"\bes\b", r"\bbe\b", r"\bde\b"]
    }
]

def auto_discover_channels(raw_streams):
    curated = []
    flat = []
    
    for t in TARGET_DEFS:
        matches = []
        for s in raw_streams:
            s_name = s.get("name", "")
            if any(re.search(ex, s_name, re.IGNORECASE) for ex in t["exclude"]):
                continue
            if any(re.search(pat, s_name, re.IGNORECASE) for pat in t["patterns"]):
                matches.append(s)
                
        v_4k = None
        v_fhd = None
        v_sd = None
        
        for m in matches:
            n = m.get("name", "").lower()
            sid = str(m.get("id") or m.get("stream_id"))
            
            if not v_4k and ("4k" in n or "uhd" in n or "2160" in n):
                v_4k = {"id": sid, "name": m.get("name"), "quality": "4K", "label": "4K Ultra HD"}
            elif not v_fhd and ("fhd" in n or "1080" in n or "ultra" in n or "hevc" in n or "ʰᵉᵛᶜ" in n):
                v_fhd = {"id": sid, "name": m.get("name"), "quality": "FHD", "label": "Full HD 1080p"}
            elif not v_sd and ("sd" in n or "lq" in n or "480" in n or "low" in n or "720" in n or ("hd" in n and not "uhd" in n and not "fhd" in n)):
                v_sd = {"id": sid, "name": m.get("name"), "quality": "SD", "label": "SD Eco"}
                
        if not v_fhd and matches:
            m = matches[0]
            v_fhd = {"id": str(m.get("id") or m.get("stream_id")), "name": m.get("name"), "quality": "FHD", "label": "Full HD 1080p"}
        if not v_4k:
            v_4k = v_fhd
        if not v_sd:
            v_sd = v_fhd
            
        if v_fhd:
            ch_item = {
                "id": t["id"],
                "name": t["name"],
                "category": t["category"],
                "default_stream_id": v_fhd["id"],
                "stream_id": v_fhd["id"],
                "variants": {
                    "4k": v_4k,
                    "fhd": v_fhd,
                    "sd": v_sd
                }
            }
            curated.append(ch_item)
            flat.append({
                "id": t["id"],
                "stream_id": v_fhd["id"],
                "name": t["name"],
                "category": t["category"]
            })
            
    return curated, flat

def load_curated_channels():
    if os.path.exists(CURATED_CHANNELS_FILE):
        try:
            with open(CURATED_CHANNELS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading curated_channels.json: {e}", flush=True)
    return []

def load_channels():
    curated = load_curated_channels()
    if curated:
        return curated
    if os.path.exists(CHANNELS_FILE):
        try:
            with open(CHANNELS_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return []

def resolve_stream_id(requested_id):
    if not requested_id:
        return DEFAULT_CHANNEL_ID
    requested_id = str(requested_id).strip()
    
    curated = load_curated_channels()
    target_quality = None
    clean_id = requested_id
    for q in ["4k", "fhd", "sd"]:
        if clean_id.lower().endswith(f"_{q}") or clean_id.lower().endswith(f":{q}"):
            clean_id = clean_id[:-len(f"_{q}")]
            target_quality = q
            break
            
    for ch in curated:
        if ch.get("id") == clean_id or ch.get("name") == clean_id:
            variants = ch.get("variants", {})
            if target_quality and target_quality in variants:
                var = variants[target_quality]
                if isinstance(var, dict) and var.get("id"):
                    return str(var["id"])
            if ch.get("default_stream_id"):
                return str(ch["default_stream_id"])
            if "fhd" in variants and variants["fhd"].get("id"):
                return str(variants["fhd"]["id"])
                
    return requested_id

def get_channel_by_id(stream_id):
    real_id = resolve_stream_id(stream_id)
    curated = load_curated_channels()
    for ch in curated:
        if str(ch.get("id")) == str(stream_id) or str(ch.get("default_stream_id")) == str(real_id):
            return ch
        variants = ch.get("variants", {})
        for v in variants.values():
            if str(v.get("id")) == str(real_id):
                return ch
    for ch in load_channels():
        if str(ch.get("id")) == str(real_id):
            return ch
    return None

def sync_channels(cfg=None):
    if cfg is None:
        cfg = load_source_config()
    stype = cfg.get("type", "xtream")
    print(f"[Sync] Running auto-discovery for source type '{stype}' ...", flush=True)
    raw_streams = []

    if stype == "xtream":
        raw_host = cfg.get("host", "http://line.liondnscloud.ru:80").rstrip("/")
        if not raw_host.startswith("http"):
            raw_host = f"http://{raw_host}"
        u = cfg.get("username", "5b596694")
        p = cfg.get("password", "c750a8bf")
        
        # 1. Try fetching all live streams directly
        url_all = f"{raw_host}/player_api.php?username={u}&password={p}&action=get_live_streams"
        try:
            req = urllib.request.Request(url_all, headers={'User-Agent': 'VLC/3.0.18'})
            with urllib.request.urlopen(req, timeout=20) as r:
                raw_streams = json.loads(r.read().decode('utf-8', errors='ignore'))
                print(f"[Sync] Fetched {len(raw_streams)} total live streams from Xtream server", flush=True)
        except Exception as e:
            print(f"[Sync] Fetch all streams failed ({e}), falling back to sport categories...", flush=True)
            cat_ids = ['1340', '1531', '1526', '1590', '1666', '1510', '1660', '1337', '1164', '1433', '789', '1118', '1075']
            for cid in cat_ids:
                try:
                    curl = f"{raw_host}/player_api.php?username={u}&password={p}&action=get_live_streams&category_id={cid}"
                    creq = urllib.request.Request(curl, headers={'User-Agent': 'VLC/3.0.18'})
                    with urllib.request.urlopen(creq, timeout=8) as cr:
                        part = json.loads(cr.read().decode('utf-8', errors='ignore'))
                        raw_streams.extend(part)
                except Exception:
                    pass

    elif stype in ["m3u_file", "m3u_direct"]:
        if os.path.exists(PLAYLIST_FILE):
            try:
                with open(PLAYLIST_FILE, "r", encoding="utf-8", errors="ignore") as f:
                    lines = f.readlines()
                current_name = ""
                for line in lines:
                    line = line.strip()
                    if line.startswith("#EXTINF"):
                        parts = line.split(",", 1)
                        current_name = parts[1].strip() if len(parts) > 1 else "Canal"
                    elif line and not line.startswith("#"):
                        raw_streams.append({"id": str(len(raw_streams) + 1), "name": current_name, "url": line})
            except Exception as e:
                print(f"[Sync] Error reading playlist.m3u: {e}", flush=True)

    if raw_streams:
        curated, flat = auto_discover_channels(raw_streams)
        if curated:
            try:
                with open(CURATED_CHANNELS_FILE, "w", encoding="utf-8") as f:
                    json.dump(curated, f, ensure_ascii=False, indent=2)
                with open(CHANNELS_FILE, "w", encoding="utf-8") as f:
                    json.dump(flat, f, ensure_ascii=False, indent=2)
                print(f"[Sync] Auto-discovery successful: {len(curated)} target channels configured with 4K/FHD/SD variants!", flush=True)
                return curated
            except Exception as e:
                print(f"[Sync] Error saving curated_channels.json: {e}", flush=True)

    return load_curated_channels() or load_channels()

def sync_xtream_channels(cfg=None):
    return sync_channels(cfg)

def build_stream_url_for_channel(stream_id, ch=None):
    cfg = load_source_config()
    stype = cfg.get("type", "xtream")

    if stype == "xtream":
        raw_host = cfg.get("host", "http://line.liondnscloud.ru:80").rstrip("/")
        if not raw_host.startswith("http"):
            raw_host = f"http://{raw_host}"
        u = cfg.get("username", "5b596694")
        p = cfg.get("password", "c750a8bf")
        return f"{raw_host}/live/{u}/{p}/{stream_id}.ts"

    elif stype == "mac_portal":
        raw_host = cfg.get("host", "103.176.90.154").rstrip("/")
        if not raw_host.startswith("http"):
            raw_host = f"http://{raw_host}"
        port = cfg.get("port", 80)
        mac = cfg.get("mac", "")
        return f"http://{raw_host}:{port}/play/live.php?mac={mac}&stream={stream_id}&extension=ts"

    elif stype == "xtream_url":
        raw = cfg.get("xtream_url", "").strip()
        if "{stream_id}" in raw:
            return raw.replace("{stream_id}", str(stream_id))
        elif raw.endswith(".ts") or raw.endswith(".m3u8"):
            return raw
        else:
            return f"{raw.rstrip('/')}/{stream_id}.ts"

    elif stype == "m3u_direct":
        return cfg.get("m3u_url", "").strip()

    if ch and ch.get("url"):
        return ch.get("url")

    return f"http://line.liondnscloud.ru:80/live/5b596694/c750a8bf/{stream_id}.ts"

def start_stream_for_channel(stream_id, force_restart=False):
    global last_global_request_time
    stream_id = str(stream_id).strip()
    if not stream_id or stream_id == "live":
        stream_id = DEFAULT_CHANNEL_ID

    now = time.time()
    last_global_request_time = now

    playlist_path = os.path.join(HLS_DIR, f"{stream_id}.m3u8")
    segment_template = os.path.join(HLS_DIR, f"seg_{stream_id}_%d.ts")

    with STREAMS_LOCK:
        # 1. If already streaming this channel and healthy, update last_active and return
        if not force_restart and stream_id in ACTIVE_STREAMS:
            info = ACTIVE_STREAMS[stream_id]
            proc = info.get("proc")
            if proc is not None and proc.poll() is None:
                info["last_active"] = now
                return True

        # 2. If max concurrent channels reached, stop the oldest idle stream
        if len(ACTIVE_STREAMS) >= MAX_CONCURRENT_CHANNELS:
            oldest_sid = min(ACTIVE_STREAMS.keys(), key=lambda k: ACTIVE_STREAMS[k]["last_active"])
            old_info = ACTIVE_STREAMS.pop(oldest_sid)
            print(f"[Multi-Stream] Max concurrent channels ({MAX_CONCURRENT_CHANNELS}) reached. Stopping oldest: {oldest_sid}", flush=True)
            if old_info.get("proc"):
                try:
                    old_info["proc"].terminate()
                    old_info["proc"].wait(timeout=1.0)
                except Exception:
                    try:
                        old_info["proc"].kill()
                    except Exception:
                        pass

        # 3. Terminate any previous dead process for this channel
        if stream_id in ACTIVE_STREAMS:
            old_proc = ACTIVE_STREAMS[stream_id].get("proc")
            if old_proc:
                try:
                    old_proc.terminate()
                    old_proc.wait(timeout=1.0)
                except Exception:
                    try:
                        old_proc.kill()
                    except Exception:
                        pass
            del ACTIVE_STREAMS[stream_id]

        real_stream_id = resolve_stream_id(stream_id)
        ch = get_channel_by_id(real_stream_id)
        ch_name = ch.get("name", f"Canal {stream_id}") if ch else f"Canal {stream_id}"
        stream_url = build_stream_url_for_channel(real_stream_id, ch)

        os.makedirs(HLS_DIR, exist_ok=True)

        # Clean old playlist file so player does not read stale sequences
        if os.path.exists(playlist_path):
            try:
                os.remove(playlist_path)
            except Exception:
                pass

        # Clean old segments for this channel
        for sf in glob.glob(os.path.join(HLS_DIR, f"seg_{stream_id}_*.ts")):
            try:
                os.remove(sf)
            except Exception:
                pass

        cmd = [
            "ffmpeg",
            "-rw_timeout", "10000000",
            "-fflags", "nobuffer+fastseek+flush_packets",
            "-analyzeduration", "500000",
            "-probesize", "500000",
            "-reconnect", "1",
            "-reconnect_at_eof", "1",
            "-reconnect_streamed", "1",
            "-reconnect_delay_max", "3",
            "-user_agent", "VLC/3.0.18",
            "-i", stream_url,
            "-c", "copy",
            "-f", "hls",
            "-hls_init_time", "1",
            "-hls_time", "3",
            "-hls_list_size", "15",
            "-hls_flags", "delete_segments+append_list",
            "-hls_segment_type", "mpegts",
            "-hls_segment_filename", segment_template,
            "-hls_allow_cache", "1",
            playlist_path
        ]

        print(f"[Multi-Stream] Starting parallel FFmpeg for channel {stream_id} ({ch_name}): {stream_url}", flush=True)
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ACTIVE_STREAMS[stream_id] = {
            "proc": proc,
            "last_active": now,
            "name": ch_name,
            "playlist": playlist_path
        }

    # Wait up to 3.5s for initial segments to be ready
    for _ in range(35):
        time.sleep(0.1)
        if os.path.exists(playlist_path) and os.path.getsize(playlist_path) > 10:
            try:
                with open(playlist_path, "r", errors="ignore") as pf:
                    if "#EXTINF" in pf.read():
                        print(f"[Multi-Stream] Playlist ready with active segments for channel {stream_id}!", flush=True)
                        break
            except Exception:
                pass

    return True

class OnDemandRelayHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=HLS_DIR, **kwargs)

    def log_message(self, format, *args):
        # Suppress noisy GET log spam for .ts files to keep log clean
        if len(args) > 0 and isinstance(args[0], str) and (".ts" in args[0] or "200" in str(args[1])):
            return
        super().log_message(format, *args)

    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, HEAD, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', '*')
        self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
        self.send_header('Pragma', 'no-cache')
        self.send_header('Expires', '0')
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()

    def copyfile(self, source, outputfile):
        try:
            super().copyfile(source, outputfile)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        global last_global_request_time
        last_global_request_time = time.time()

        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path

        # 1. On-Demand Channel Stream: /hls/<stream_id>.m3u8?token=...
        if path.startswith("/hls/") and path.endswith(".m3u8"):
            stream_id = path.replace("/hls/", "").replace(".m3u8", "").strip()
            if not stream_id or stream_id == "live":
                stream_id = DEFAULT_CHANNEL_ID
            start_stream_for_channel(stream_id)
            self.serve_channel_m3u8(stream_id)
            return

        # 2. Direct or legacy playlist: /live.m3u8 or /bein1.m3u8
        if path in ["/bein1.m3u8", "/live.m3u8"]:
            start_stream_for_channel(DEFAULT_CHANNEL_ID)
            self.serve_channel_m3u8(DEFAULT_CHANNEL_ID)
            return

        # 3. TS Video segments with in-memory RAM caching for multi-client performance
        if path.endswith(".ts"):
            segment_name = os.path.basename(path)
            self.serve_ts_segment(segment_name)
            return

        # 4. Status API
        if path == "/api/status":
            now = time.time()
            with STREAMS_LOCK:
                active_list = []
                for sid, info in ACTIVE_STREAMS.items():
                    active_list.append({
                        "id": sid,
                        "name": info.get("name"),
                        "last_active_sec_ago": round(now - info.get("last_active", 0), 1),
                        "running": info.get("proc") is not None and info["proc"].poll() is None
                    })
            self.render_json({
                "status": "online",
                "mode": "parallel_multi_channel",
                "active_channels_count": len(active_list),
                "active_channels": active_list,
                "cached_segments_in_ram": len(SEGMENT_CACHE),
                "max_concurrent_channels": MAX_CONCURRENT_CHANNELS
            })
            return

        # 5. Channels API (Returns curated channels with 4K/FHD/SD variants)
        if path == "/api/channels":
            format_type = urllib.parse.parse_qs(parsed.query).get("format", ["curated"])[0]
            if format_type == "flat":
                self.render_json(load_channels())
            else:
                curated = load_curated_channels()
                if not curated:
                    curated = sync_channels()
                self.render_json(curated)
            return

        # 5b. Force Sync / Auto-Discovery Channels API
        if path in ["/api/sync_channels", "/api/discover"]:
            channels = sync_channels()
            self.render_json({"success": True, "count": len(channels), "channels": channels})
            return

        # 6. Source Config API
        if path == "/api/source":
            self.render_json(load_source_config())
            return

        # 7. Web Control page
        if path == "/control":
            self.render_control_page()
            return

        try:
            return super().do_GET()
        except (BrokenPipeError, ConnectionResetError):
            return

    def serve_channel_m3u8(self, stream_id):
        playlist_path = os.path.join(HLS_DIR, f"{stream_id}.m3u8")
        # If playlist not ready yet (e.g. FFmpeg starting chunk 0), wait up to 3.5s
        for _ in range(35):
            if os.path.exists(playlist_path) and os.path.getsize(playlist_path) > 10:
                break
            time.sleep(0.1)

        if not os.path.exists(playlist_path) or os.path.getsize(playlist_path) <= 10:
            try:
                self.send_error(404, f"Playlist for channel {stream_id} not ready yet.")
            except (BrokenPipeError, ConnectionResetError):
                pass
            return

        # Update last active for this stream
        with STREAMS_LOCK:
            if stream_id in ACTIVE_STREAMS:
                ACTIVE_STREAMS[stream_id]["last_active"] = time.time()

        try:
            with open(playlist_path, "rb") as f:
                content = f.read()

            self.send_response(200)
            self.send_header("Content-Type", "application/vnd.apple.mpegurl")
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)
        except (BrokenPipeError, ConnectionResetError):
            pass
        except Exception as e:
            try:
                self.send_error(500, f"Error reading playlist: {e}")
            except (BrokenPipeError, ConnectionResetError):
                pass

    def serve_ts_segment(self, segment_name):
        # Update last active for the corresponding channel
        m = re.match(r"seg_(\d+)_", segment_name)
        if m:
            sid = m.group(1)
            with STREAMS_LOCK:
                if sid in ACTIVE_STREAMS:
                    ACTIVE_STREAMS[sid]["last_active"] = time.time()

        # 1. Check in-memory RAM cache first (instant response for all concurrent accounts)
        data = None
        with CACHE_LOCK:
            data = SEGMENT_CACHE.get(segment_name)

        if data is None:
            # 2. Read from disk and store in RAM cache
            file_path = os.path.join(HLS_DIR, segment_name)
            # Wait up to 1.5s if segment is currently being flushed
            for _ in range(15):
                if os.path.exists(file_path):
                    break
                time.sleep(0.1)

            if not os.path.exists(file_path):
                try:
                    self.send_error(404, f"Segment {segment_name} not found.")
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            try:
                with open(file_path, "rb") as f:
                    data = f.read()
                with CACHE_LOCK:
                    SEGMENT_CACHE[segment_name] = data
                    # Evict oldest if cache exceeds limit
                    if len(SEGMENT_CACHE) > MAX_CACHE_ITEMS:
                        oldest_key = next(iter(SEGMENT_CACHE))
                        del SEGMENT_CACHE[oldest_key]
            except Exception as e:
                try:
                    self.send_error(500, f"Error reading segment: {e}")
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

        try:
            self.send_response(200)
            self.send_header("Content-Type", "video/mp2t")
            self.send_header("Content-Length", str(len(data)))
            self.send_header("Access-Control-Allow-Origin", "*")
            self.send_header("Cache-Control", "public, max-age=3600")
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/api/source":
            try:
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length).decode('utf-8')
                new_cfg = json.loads(body)
                save_source_config(new_cfg)
                threading.Thread(target=sync_channels, args=(new_cfg,), daemon=True).start()
                # Restart active streams with new source
                with STREAMS_LOCK:
                    current_sids = list(ACTIVE_STREAMS.keys())
                for sid in current_sids:
                    start_stream_for_channel(sid, force_restart=True)
                cur_cfg = load_source_config()
                self.render_json({"success": True, "message": "Source mise à jour avec succès", "config": cur_cfg})
            except Exception as e:
                self.render_json({"success": False, "error": str(e)}, 400)
            return

        if parsed.path == "/api/switch":
            try:
                length = int(self.headers.get('Content-Length', 0))
                body = self.rfile.read(length).decode('utf-8')
                params = urllib.parse.parse_qs(body)
                channel_id = params.get('id', [None])[0]
                if channel_id:
                    start_stream_for_channel(channel_id, force_restart=True)
                self.send_response(302)
                self.send_header('Location', '/control')
                self.end_headers()
            except Exception as e:
                self.render_json({"success": False, "error": str(e)}, 400)
            return

        self.send_error(404, "Endpoint not found.")

    def render_json(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def render_control_page(self):
        channels = load_channels()
        categories = {}
        for ch in channels:
            cat = ch.get("category", "Autres")
            categories.setdefault(cat, []).append(ch)

        with STREAMS_LOCK:
            active_sids = set(ACTIVE_STREAMS.keys())
            active_info = ", ".join([f"{info.get('name')} ({sid})" for sid, info in ACTIVE_STREAMS.items()]) or "Aucune"

        html = f"""<!DOCTYPE html>
<html lang="fr">
<head>
    <meta charset="UTF-8">
    <title>Family Stream - Relais VIP Multi-Flux</title>
    <style>
        body {{ background: #0f172a; color: #f8fafc; font-family: system-ui, sans-serif; padding: 20px; }}
        .header {{ background: #1e293b; padding: 20px; border-radius: 12px; margin-bottom: 20px; }}
        .channel-card {{ background: #1e293b; border: 1px solid #334155; border-radius: 8px; padding: 10px; margin-bottom: 8px; display: flex; justify-content: space-between; align-items: center; }}
        .active {{ border-color: #38bdf8; background: #1e3a5f; }}
        .btn {{ background: #3b82f6; color: white; border: none; padding: 6px 12px; border-radius: 6px; cursor: pointer; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ Family Stream - Relais VIP Multi-Flux Parallèles</h1>
        <p>Chaînes actives en direct ({len(active_sids)}/{MAX_CONCURRENT_CHANNELS}) : <strong>{active_info}</strong></p>
    </div>
"""
        for cat, chs in categories.items():
            html += f"<h2>{cat} ({len(chs)})</h2>"
            for ch in chs[:10]:
                is_active = (str(ch.get("id")) in active_sids)
                cls = "active" if is_active else ""
                html += f"""
                <div class="channel-card {cls}">
                    <span>{ch.get('name')}</span>
                    <form method="POST" action="/api/switch" style="margin:0;">
                        <input type="hidden" name="id" value="{ch.get('id')}">
                        <button type="submit" class="btn">Diffuser</button>
                    </form>
                </div>"""

        html += "</body></html>"
        body = html.encode('utf-8')
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

# MULTI-THREADED HTTP SERVER (Handles unlimited simultaneous clients/accounts in parallel threads)
class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True
    allow_reuse_address = True

if __name__ == "__main__":
    os.makedirs(HLS_DIR, exist_ok=True)
    # 1. Start Cloudflare Tunnel watcher in background
    start_tunnel_watcher()
    # 2. Start Keep-Alive Heartbeat in background (prevents GitHub 30m idle timeout while watching)
    start_heartbeat()
    # 3. Start Inactive Stream Reaper (stops unviewed streams after 50s)
    start_reaper()
    # 4. Pre-start default Canal+ Foot (832106)
    start_stream_for_channel(DEFAULT_CHANNEL_ID)

    server_address = ("", PORT)
    with ThreadedHTTPServer(server_address, OnDemandRelayHandler) as httpd:
        print(f"[Family Stream Relay] Multi-Threaded Multi-Channel Server running on port {PORT}", flush=True)
        try:
            httpd.serve_forever()
        finally:
            with STREAMS_LOCK:
                for sid, info in ACTIVE_STREAMS.items():
                    if info.get("proc"):
                        try:
                            info["proc"].kill()
                        except Exception:
                            pass
