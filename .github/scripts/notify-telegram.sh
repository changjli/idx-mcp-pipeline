#!/usr/bin/env bash
# Push the daily-flow digest to Telegram.
# Sends a truncated text headline, then the full digest as a document.
# Env: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID. Reads ./digest.md and ./flow.err.
set -uo pipefail

ts="$(date -u +'%Y-%m-%d %H:%M UTC')"

if [[ -z "${TELEGRAM_BOT_TOKEN:-}" || -z "${TELEGRAM_CHAT_ID:-}" ]]; then
  echo "TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID unset — skipping notification."
  exit 0
fi

api="https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}"

if [[ -s digest.md ]]; then
  # Telegram text messages cap at 4096 chars; send a headline, then the file.
  text="$(head -c 3800 digest.md)"
  msg="IDX daily flow — ${ts}

${text}"
else
  msg="IDX daily flow — ${ts}

No digest produced. Last errors:
$(tail -n 15 flow.err 2>/dev/null || echo '(no log)')"
fi

curl -sS -X POST "${api}/sendMessage" \
  --data-urlencode "chat_id=${TELEGRAM_CHAT_ID}" \
  --data-urlencode "text=${msg}" \
  --data-urlencode "disable_web_page_preview=true" \
  > /dev/null || echo "sendMessage failed"

if [[ -s digest.md ]]; then
  curl -sS -X POST "${api}/sendDocument" \
    -F "chat_id=${TELEGRAM_CHAT_ID}" \
    -F "document=@digest.md;filename=idx-daily-flow-$(date -u +%F).md" \
    > /dev/null || echo "sendDocument failed"
fi
