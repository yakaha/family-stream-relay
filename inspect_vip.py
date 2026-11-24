import asyncio
import os
import re
import json
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from telethon import TelegramClient
import requests
import urllib3
urllib3.disable_warnings()

API_ID   = 39352626
API_HASH = "938a87cb1245b0a95c1d85827082fecc"
SESSION  = "xtream_bot_session"

def extract_codes(text):
    results = []
    # 1. get.php
    for u in re.findall(r'https?://[^\s"\'<>]+/get\.php\?[^\s"\'<>]+', text, re.I):
        try:
            p = urllib.parse.urlparse(u)
            q = urllib.parse.parse_qs(p.query)
            user = (q.get("username") or q.get("user") or [None])[0]
            pwd = (q.get("password") or q.get("pass") or [None])[0]
            if user and pwd:
                results.append((f"{p.scheme}://{p.netloc}".rstrip('/'), user.strip(), pwd.strip()))
        except Exception:
            pass
    # 2. blocks
    for b in re.findall(r'(?:PORTAL|HOST|SERVER|URL)\s*[:|=]\s*(https?://[^\s\n\r]+)[\s\S]*?(?:USER|USERNAME|USR)\s*[:|=]\s*([^\s\n\r]+)[\s\S]*?(?:PASS|PASSWORD|PWD)\s*[:|=]\s*([^\s\n\r]+)', text, re.I):
        h, u, p = b[0].strip().rstrip('/'), b[1].strip(), b[2].strip()
        if len(u) >= 2 and len(p) >= 2:
            results.append((h, u, p))
    # 3. triplets
    for line in text.splitlines():
        m = re.match(r'^(https?://[\w.\-]+(?::\d+)?)\s+([A-Za-z0-9_.\-@]+)\s+([A-Za-z0-9_.\-@!#$%&*]+)$', line.strip())
        if m:
            results.append((m.group(1).rstrip('/'), m.group(2).strip(), m.group(3).strip()))
    return list(set(results))

def test_server(h, u, p):
    url = f"{h}/player_api.php?username={u}&password={p}"
    try:
        r = requests.get(url, timeout=5, verify=False, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, dict) and "user_info" in data:
                info = data["user_info"]
                status = str(info.get("status", "")).lower()
                if status in ("active", "1"):
                    # Check channels
                    rc = requests.get(f"{url}&action=get_live_streams", timeout=7, verify=False)
                    ch_count = len(rc.json()) if rc.status_code == 200 and isinstance(rc.json(), list) else 0
                    return True, h, u, p, info.get("exp_date"), info.get("max_connections"), ch_count
    except Exception:
        pass
    return False, h, u, p, None, None, 0

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    print("Telegram connected!")

    # Find M3U & Xtream [VIP GROUP]
    target_entity = None
    async for dialog in client.iter_dialogs():
        if "vip group" in (dialog.name or "").lower():
            target_entity = dialog.entity
            break

    if not target_entity:
        print("VIP Group not found!")
        await client.disconnect()
        return

    print("Fetching recent files from M3U & Xtream [VIP GROUP]...")
    all_codes = []
    async for msg in client.iter_messages(target_entity, limit=30):
        if msg.document and msg.file and msg.file.name and msg.file.name.lower().endswith('.txt'):
            print(f"Downloading file: {msg.file.name} ({msg.file.size} bytes)...")
            try:
                b = await asyncio.wait_for(client.download_media(msg, bytes), timeout=20)
                if b:
                    text = b.decode('utf-8', errors='ignore')
                    codes = extract_codes(text)
                    print(f"  -> Extracted {len(codes)} codes from {msg.file.name}")
                    all_codes.extend(codes)
            except Exception as e:
                print(f"  -> Error: {e}")

    await client.disconnect()

    unique_codes = list(set(all_codes))
    print(f"\nTotal unique codes collected from VIP files: {len(unique_codes)}")

    print(f"Testing {len(unique_codes)} servers in parallel (15 workers)...")
    valid = []
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = [ex.submit(test_server, h, u, p) for h, u, p in unique_codes]
        for fut in as_completed(futs):
            ok, h, u, p, exp, mc, chs = fut.result()
            if ok:
                print(f"  [VALIDE] {h} | User: {u} | MaxConn: {mc} | Chaînes: {chs}")
                valid.append((h, u, p, exp, mc, chs))

    print(f"\n==========================================")
    print(f"TOTAL SERVEURS VALIDES TROUVES: {len(valid)}")
    print(f"==========================================")
    for v in valid:
        print(f"Host: {v[0]} | User: {v[1]} | Pass: {v[2]} | MaxConn: {v[4]} | Total chaînes: {v[5]}")

if __name__ == "__main__":
    asyncio.run(main())
