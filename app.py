import os
import asyncio
import logging
import logging.handlers
import threading
import requests as http_requests
from flask import Flask, request, jsonify
from telegram import Update

# ── Logging setup with rotation ──
# Console: INFO level (normal operation)
# File: DEBUG level, auto-rotates at 5MB, keeps 2 backups only
# This prevents log files from growing indefinitely during long sessions
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_file_handler = logging.handlers.RotatingFileHandler(
    "bot.log", maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s"))

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[_console_handler, _file_handler]
)
logger = logging.getLogger(__name__)

# Suppress noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app      = Flask(__name__)
bot_app  = None
bot_loop = None


# 🌐 Health check
@app.route("/")
def home():
    return "✅ Bot is running"


# 📨 Telegram webhook
@app.route("/webhook", methods=["POST"])
def webhook():
    global bot_app, bot_loop
    if bot_app is None or bot_loop is None:
        return jsonify({"status": "error", "detail": "bot not ready"}), 500
    try:
        data   = request.get_json(force=True)
        update = Update.de_json(data, bot_app.bot)
        # Fire-and-forget — do NOT block on future.result().
        # Blocking here causes a deadlock/timeout when the handler itself
        # needs to send messages (e.g. upgrade_request_yes notifying admins),
        # which freezes the entire bot for all users until redeploy.
        asyncio.run_coroutine_threadsafe(
            bot_app.process_update(update), bot_loop
        )
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"Telegram webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


# ─── Bot setup ───
async def run_bot_setup(render_url):
    global bot_app
    from bot import start_bot

    webhook_url = f"{render_url}/webhook"
    logger.info(f"Setting webhook: {webhook_url}")

    bot = start_bot()
    await bot.initialize()

    # python-telegram-bot never calls post_init automatically when running
    # Flask + a manual webhook, so call it explicitly. post_init (bot.py)
    # starts the plan-expiry watchdog, upgrade notifier, market collectors
    # and registers the command menus (user + admin scope).
    await bot.post_init(bot)
    logger.info("✅ post_init called — background tasks started")

    await bot.bot.set_webhook(url=webhook_url)
    bot_app = bot
    logger.info("✅ Bot ready")


def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            ip = http_requests.get(svc, timeout=5).text.strip()
            if ip:
                logger.info("=" * 55)
                logger.info(f"  🌍 PUBLIC IP: {ip}")
                logger.info(f"  👉 Whitelist this IP on your Bybit API keys")
                logger.info("=" * 55)
                break
        except Exception:
            continue

    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        logger.error("❌ RENDER_EXTERNAL_URL not set")
        raise SystemExit(1)

    # Initialise persistent disk database
    try:
        import db
        db._init_dirs()
        logger.info(f"✅ Disk DB initialised at {os.getenv('DISK_PATH', '/data')}")
    except Exception as e:
        logger.warning(f"⚠️ Disk DB init warning: {e} — will retry on first use")


    bot_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(bot_loop,), daemon=False)
    t.start()
    logger.info("✅ Persistent event loop started")

    future = asyncio.run_coroutine_threadsafe(run_bot_setup(render_url), bot_loop)
    try:
        future.result(timeout=30)
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
