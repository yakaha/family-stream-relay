#!/usr/bin/env bash
echo "=== Starting On-Demand HLS Relay ==="
cd /workspaces/family-stream-relay 2>/dev/null || cd "$(dirname "$0")"

# ALWAYS auto-sync latest code from GitHub main
git fetch origin main 2>/dev/null && git reset --hard origin/main 2>/dev/null || true

mkdir -p /tmp/hls
pkill -9 -f ffmpeg || true
pkill -9 -f cloudflared || true
pkill -9 -f server.py || true
pkill -9 -f "python3.*server.py" || true
rm -rf /tmp/hls/*
sleep 1

# 1. Start Python On-Demand Server & Stream Manager
nohup python3 server.py > /tmp/http.log 2>&1 &
echo "On-Demand Server running on port 8888"

# 2. Start Cloudflare Tunnel
nohup cloudflared tunnel --url http://127.0.0.1:8888 > /tmp/tunnel.log 2>&1 &
echo "Cloudflare Tunnel started"

# 3. Wait for tunnel URL and first playlist
TUNNEL_URL=""
for i in {1..40}; do
  sleep 1
  if [ -z "$TUNNEL_URL" ]; then
    TUNNEL_URL=$(grep -o 'https://[a-zA-Z0-9-]*\.trycloudflare\.com' /tmp/tunnel.log 2>/dev/null | head -1)
  fi
  if [ -n "$TUNNEL_URL" ] && [ -f /tmp/hls/live.m3u8 ]; then
    echo "Found tunnel: $TUNNEL_URL and playlist exists!"
    break
  fi
done

echo "Active Tunnel: $TUNNEL_URL"

if [ -n "$TUNNEL_URL" ]; then
  STREAM_URL="$TUNNEL_URL/live.m3u8"
  echo "STREAM READY: $STREAM_URL"
  echo "$STREAM_URL" > stream_url.txt
  git config user.name "FamilyStreamBot"
  git config user.email "bot@familystream.local"
  git add stream_url.txt
  git commit -m "Live stream URL: $STREAM_URL" || true
  git push origin main || true
fi
