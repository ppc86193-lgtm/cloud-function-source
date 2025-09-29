#!/usr/bin/env bash
set -euo pipefail
BOT_TOKEN="${BOT_TOKEN:-}"
CHAT_ID="${CHAT_ID:-}"
if [[ -z "${BOT_TOKEN}" || -z "${CHAT_ID}" ]]; then
  exit 0
fi
TEXT="$1"
# 纯文本，避免Markdown/HTML解析错误
curl -sS -X POST "https://api.telegram.org/bot${BOT_TOKEN}/sendMessage" \
  -d "chat_id=${CHAT_ID}" \
  --data-urlencode "text=${TEXT}" \
  -d "disable_web_page_preview=true" \
  -d "disable_notification=false" >/dev/null || true
