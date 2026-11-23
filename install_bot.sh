#!/bin/bash
# ============================================================
# Family Stream - Install Bot Xtream Pool sur Codespace
# Exécute ce script une seule fois sur le Codespace yakaha
# ============================================================

set -e
REPO_DIR="/workspaces/family-stream-relay"
cd "$REPO_DIR"

echo "📦 Installation des dépendances Python..."
pip install telethon firebase-admin requests --quiet

echo "✅ Dépendances installées"

# Vérifie que firebase_service_key.json est présent
if [ ! -f "$REPO_DIR/firebase_service_key.json" ]; then
    echo "⚠️  ATTENTION: firebase_service_key.json manquant !"
    echo "   → Place ton fichier de clé Firebase à: $REPO_DIR/firebase_service_key.json"
    echo "   → Télécharge-le depuis: Firebase Console → Paramètres → Comptes de service"
fi

# Démarre le bot en arrière-plan avec nohup
echo "🚀 Démarrage du bot Xtream..."
nohup python bot_xtream.py > bot_xtream.log 2>&1 &
BOT_PID=$!
echo "✅ Bot démarré (PID: $BOT_PID)"
echo "$BOT_PID" > bot_xtream.pid

echo ""
echo "🔍 Logs: tail -f $REPO_DIR/bot_xtream.log"
echo "🛑 Stop: kill \$(cat $REPO_DIR/bot_xtream.pid)"
