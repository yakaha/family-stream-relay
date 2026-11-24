import asyncio
import os
import re
import json
from telethon import TelegramClient

API_ID   = 39352626
API_HASH = "938a87cb1245b0a95c1d85827082fecc"
SESSION  = "xtream_bot_session"

async def main():
    client = TelegramClient(SESSION, API_ID, API_HASH)
    await client.start()
    print("Telegram connected!")

    # Find M3U & Xtream [VIP GROUP]
    target_entity = None
    target_title = None
    async for dialog in client.iter_dialogs():
        d_name = dialog.name or ""
        if "m3u" in d_name.lower() or "xtream" in d_name.lower() or "vip" in d_name.lower():
            print(f"Found candidate dialog: '{d_name}' (ID: {dialog.id})")
            if "vip group" in d_name.lower() or "m3u & xtream" in d_name.lower():
                target_entity = dialog.entity
                target_title = d_name

    if not target_entity:
        print("M3U & Xtream [VIP GROUP] not matched specifically, printing top 20 dialogs:")
        async for dialog in client.iter_dialogs(limit=20):
            print(f"  • {dialog.name}")
        await client.disconnect()
        return

    print(f"\nInspecting last 20 messages of: '{target_title}'...")
    with open("inspect_vip.txt", "w", encoding="utf-8") as out:
        out.write(f"=== Messages of {target_title} ===\n\n")
        async for msg in client.iter_messages(target_entity, limit=20):
            out.write(f"--- Message ID: {msg.id} | Date: {msg.date} ---\n")
            if msg.media:
                out.write(f"Media type: {type(msg.media).__name__}\n")
                if hasattr(msg, 'file') and msg.file:
                    out.write(f"File name: {msg.file.name} | Size: {msg.file.size} bytes\n")
            text = msg.message or msg.text or ""
            out.write(f"Text length: {len(text)}\n")
            out.write(f"Text sample:\n{text[:500]}\n\n")

    print("Inspection written to inspect_vip.txt")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())
