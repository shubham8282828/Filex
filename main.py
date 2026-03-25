"""
Telegram File Sharing + Streaming Bot
Production-ready | Webhook Mode | Flask + MongoDB + Pixeldrain
"""

# ─────────────────────────────────────────────────────────────
# 1. IMPORTS
# ─────────────────────────────────────────────────────────────
import asyncio
import base64
import hashlib
import hmac
import io
import logging
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional

import certifi
import requests
from dotenv import load_dotenv
from flask import Flask, Response, redirect, render_template_string, request, stream_with_context
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from telegram import (
    Bot, InlineKeyboardButton, InlineKeyboardMarkup,
    Update, BotCommand,
)
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ContextTypes, MessageHandler, filters,
)

load_dotenv()

# ─────────────────────────────────────────────────────────────
# 2. LOGGING SETUP
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("FileBot")
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)

# ─────────────────────────────────────────────────────────────
# 3. ENVIRONMENT VARIABLES
# ─────────────────────────────────────────────────────────────
BOT_TOKEN            = os.environ["BOT_TOKEN"]
MONGO_URI            = os.environ["MONGO_URI"]
PIXELDRAIN_API_KEY   = os.environ["PIXELDRAIN_API_KEY"]
ADMIN_IDS            = [int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()]
BOT_USERNAME         = os.getenv("BOT_USERNAME", "MyFileBot")
BASE_URL             = os.getenv("BASE_URL", "https://yourapp.onrender.com").rstrip("/")
UPI_ID               = os.getenv("UPI_ID", "example@upi")
UPI_QR_URL           = os.getenv("UPI_QR_URL", "")
FLASK_SECRET         = os.getenv("FLASK_SECRET", "changeme_32_chars_random_string_!")
PORT                 = int(os.getenv("PORT", 8080))
TOKEN_VALIDITY_HOURS = int(os.getenv("TOKEN_VALIDITY_HOURS", 24))
SHORTENER_API_KEY    = os.getenv("SHORTENER_API_KEY", "")
SHORTENER_DOMAIN     = os.getenv("SHORTENER_DOMAIN", "api.shrtco.de")

# Multi-bot
SERVER_ID  = int(os.getenv("SERVER_ID", 1))
MAX_USERS  = int(os.getenv("MAX_USERS", 500))
NEXT_BOT   = os.getenv("NEXT_BOT", "")          # "username|https://t.me/username"
ALL_BOTS   = os.getenv("ALL_BOTS", "")           # "bot1|https://url1,bot2|https://url2"

# ─────────────────────────────────────────────────────────────
# 4. MULTI-BOT CONFIG + ACTIVE USER TRACKING
# ─────────────────────────────────────────────────────────────
_active_users: dict[int, float] = {}   # user_id → last_seen timestamp
_active_lock = threading.Lock()
ACTIVE_WINDOW_SECONDS = 300            # 5 minutes

def track_active_user(user_id: int) -> None:
    with _active_lock:
        _active_users[user_id] = time.time()

def get_concurrent_users() -> int:
    cutoff = time.time() - ACTIVE_WINDOW_SECONDS
    with _active_lock:
        return sum(1 for ts in _active_users.values() if ts >= cutoff)

def parse_all_bots() -> list[dict]:
    """Return list of {username, url} dicts from ALL_BOTS env."""
    bots = []
    for entry in ALL_BOTS.split(","):
        entry = entry.strip()
        if "|" in entry:
            parts = entry.split("|", 1)
            bots.append({"username": parts[0].strip(), "url": parts[1].strip()})
    return bots

def parse_next_bot() -> dict | None:
    if NEXT_BOT and "|" in NEXT_BOT:
        parts = NEXT_BOT.split("|", 1)
        return {"username": parts[0].strip(), "url": parts[1].strip()}
    return None

# ─────────────────────────────────────────────────────────────
# 5. MONGODB SETUP
# ─────────────────────────────────────────────────────────────
_mongo_client = MongoClient(
    MONGO_URI,
    maxPoolSize=50,
    minPoolSize=5,
    retryWrites=True,
    retryReads=True,
    tlsCAFile=certifi.where(),
)
db = _mongo_client.get_default_database("filebotdb")

# ─────────────────────────────────────────────────────────────
# 6. MONGODB COLLECTIONS
# ─────────────────────────────────────────────────────────────
users_col    = db[f"users_bot{SERVER_ID}"]   # per-bot user collection
files_col    = db["files"]                    # shared
payments_col = db["payments"]                 # shared
referrals_col= db["referrals"]                # shared

# ─────────────────────────────────────────────────────────────
# 7. SETUP INDEXES
# ─────────────────────────────────────────────────────────────
def setup_indexes() -> None:
    try:
        users_col.create_index([("user_id", ASCENDING)], unique=True)
        files_col.create_index([("file_unique_id", ASCENDING)], unique=True)
        payments_col.create_index([("utr", ASCENDING)], sparse=True)
        payments_col.create_index([("user_id", ASCENDING)])
        referrals_col.create_index(
            [("referrer_id", ASCENDING), ("referred_user_id", ASCENDING)],
            unique=True,
        )
        referrals_col.create_index([("referred_user_id", ASCENDING)], unique=True)
        logger.info("MongoDB indexes ready.")
    except Exception as exc:
        logger.warning("Index setup: %s", exc)

# ─────────────────────────────────────────────────────────────
# 8. DB HELPER FUNCTIONS
# ─────────────────────────────────────────────────────────────
def get_or_create_user(user_id: int) -> dict:
    user = users_col.find_one({"user_id": user_id})
    if not user:
        user = {
            "user_id": user_id,
            "token_expiry": None,
            "premium_expiry": None,
            "referrals_count": 0,
            "referred_by": None,
            "joined_at": datetime.now(timezone.utc),
        }
        try:
            users_col.insert_one(user)
        except DuplicateKeyError:
            user = users_col.find_one({"user_id": user_id})
    return user

def is_premium(user_id: int) -> bool:
    user = users_col.find_one({"user_id": user_id}, {"premium_expiry": 1})
    if not user or not user.get("premium_expiry"):
        return False
    return user["premium_expiry"] > datetime.now(timezone.utc)

def has_valid_token(user_id: int) -> bool:
    """Check cache first, then DB."""
    cached = _token_cache_get(user_id)
    if cached is not None:
        return cached
    user = users_col.find_one({"user_id": user_id}, {"token_expiry": 1, "premium_expiry": 1})
    if not user:
        _token_cache_set(user_id, False)
        return False
    now = datetime.now(timezone.utc)
    if user.get("premium_expiry") and user["premium_expiry"] > now:
        _token_cache_set(user_id, True)
        return True
    result = bool(user.get("token_expiry") and user["token_expiry"] > now)
    _token_cache_set(user_id, result)
    return result

def grant_token(user_id: int, hours: int = TOKEN_VALIDITY_HOURS) -> None:
    expiry = datetime.now(timezone.utc) + timedelta(hours=hours)
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"token_expiry": expiry}},
        upsert=True,
    )
    _token_cache_invalidate(user_id)

def grant_premium(user_id: int, days: int) -> None:
    user = users_col.find_one({"user_id": user_id}, {"premium_expiry": 1})
    now = datetime.now(timezone.utc)
    base = now
    if user and user.get("premium_expiry") and user["premium_expiry"] > now:
        base = user["premium_expiry"]   # stack on existing
    expiry = base + timedelta(days=days)
    users_col.update_one(
        {"user_id": user_id},
        {"$set": {"premium_expiry": expiry}},
        upsert=True,
    )
    _token_cache_invalidate(user_id)

def revoke_premium(user_id: int) -> None:
    users_col.update_one({"user_id": user_id}, {"$set": {"premium_expiry": None}})
    _token_cache_invalidate(user_id)

def is_bot_full() -> bool:
    return users_col.count_documents({}) >= MAX_USERS

def get_file_by_unique_id(file_unique_id: str) -> dict | None:
    return files_col.find_one({"file_unique_id": file_unique_id})

def save_file(doc: dict) -> None:
    try:
        files_col.insert_one(doc)
    except DuplicateKeyError:
        files_col.update_one(
            {"file_unique_id": doc["file_unique_id"]},
            {"$set": doc},
        )

def add_referral(referrer_id: int, referred_id: int) -> bool:
    """Returns True if referral was newly recorded."""
    if referrer_id == referred_id:
        return False
    try:
        referrals_col.insert_one({
            "referrer_id": referrer_id,
            "referred_user_id": referred_id,
            "timestamp": datetime.now(timezone.utc),
        })
        users_col.update_one(
            {"user_id": referrer_id},
            {"$inc": {"referrals_count": 1}},
            upsert=True,
        )
        return True
    except DuplicateKeyError:
        return False

PREMIUM_PLANS = {
    "1month":   {"days": 30,    "price": 49,  "label": "1 Month"},
    "3months":  {"days": 90,    "price": 129, "label": "3 Months"},
    "1year":    {"days": 365,   "price": 399, "label": "1 Year"},
    "lifetime": {"days": 36500, "price": 799, "label": "Lifetime"},
}

# ─────────────────────────────────────────────────────────────
# 9. IN-MEMORY TOKEN CACHE (thread-safe, 60s TTL, max 10 000)
# ─────────────────────────────────────────────────────────────
_cache: dict[int, tuple[bool, float]] = {}   # user_id → (valid, expires_at)
_cache_lock = threading.Lock()
_CACHE_TTL  = 60.0
_CACHE_MAX  = 10_000

def _token_cache_get(user_id: int) -> bool | None:
    with _cache_lock:
        entry = _cache.get(user_id)
        if entry is None:
            return None
        valid, exp = entry
        if time.time() > exp:
            del _cache[user_id]
            return None
        return valid

def _token_cache_set(user_id: int, valid: bool) -> None:
    with _cache_lock:
        if len(_cache) >= _CACHE_MAX:
            # evict oldest 10 %
            oldest = sorted(_cache.items(), key=lambda x: x[1][1])[:_CACHE_MAX // 10]
            for k, _ in oldest:
                del _cache[k]
        _cache[user_id] = (valid, time.time() + _CACHE_TTL)

def _token_cache_invalidate(user_id: int) -> None:
    with _cache_lock:
        _cache.pop(user_id, None)

# ─────────────────────────────────────────────────────────────
# 10. PIXELDRAIN FUNCTIONS
# ─────────────────────────────────────────────────────────────
PD_BASE = "https://pixeldrain.com/api"

def _pd_auth_header() -> dict:
    token = base64.b64encode(f":{PIXELDRAIN_API_KEY}".encode()).decode()
    return {"Authorization": f"Basic {token}"}

def pixeldrain_upload(file_bytes: bytes, filename: str) -> str | None:
    """Upload bytes to Pixeldrain. Returns file ID or None."""
    url = f"{PD_BASE}/file/{filename}"
    headers = _pd_auth_header()
    try:
        resp = requests.put(url, data=file_bytes, headers=headers, timeout=120)
        resp.raise_for_status()
        return resp.json().get("id")
    except Exception as exc:
        logger.error("Pixeldrain upload error: %s", exc)
        return None

def pixeldrain_check(pd_id: str) -> bool:
    """Returns True if file is accessible on Pixeldrain."""
    try:
        resp = requests.get(
            f"{PD_BASE}/file/{pd_id}/info",
            headers=_pd_auth_header(),
            timeout=10,
        )
        return resp.status_code == 200
    except Exception:
        return False

def pixeldrain_stream_generator(pd_id: str, range_header: str):
    """
    Generator that streams a Pixeldrain file in 256 KB chunks.
    Yields (chunk_generator, status_code, response_headers).
    This is a one-shot call – caller must iterate the inner generator.
    """
    url = f"{PD_BASE}/file/{pd_id}"
    req_headers = dict(_pd_auth_header())
    if range_header:
        req_headers["Range"] = range_header

    pd_resp = None
    try:
        pd_resp = requests.get(url, headers=req_headers, stream=True, timeout=30)
        status = pd_resp.status_code

        forward_headers = {}
        for h in ("Content-Type", "Content-Length", "Content-Range", "Accept-Ranges"):
            if h in pd_resp.headers:
                forward_headers[h] = pd_resp.headers[h]
        if "Content-Type" not in forward_headers:
            forward_headers["Content-Type"] = "video/mp4"

        def _gen():
            try:
                for chunk in pd_resp.iter_content(chunk_size=256 * 1024):
                    if chunk:
                        yield chunk
            finally:
                pd_resp.close()

        yield _gen(), status, forward_headers

    except Exception as exc:
        logger.error("Pixeldrain stream error: %s", exc)
        if pd_resp:
            pd_resp.close()
        yield None, 502, {}

# ─────────────────────────────────────────────────────────────
# 11. URL SHORTENER
# ─────────────────────────────────────────────────────────────
def shorten_url(long_url: str) -> str:
    """Return shortened URL or original on failure."""
    try:
        if SHORTENER_API_KEY and SHORTENER_DOMAIN != "api.shrtco.de":
            # Custom shortener (generic GET API)
            resp = requests.get(
                f"https://{SHORTENER_DOMAIN}/api",
                params={"api": SHORTENER_API_KEY, "url": long_url},
                timeout=8,
            )
            data = resp.json()
            return data.get("shortenedUrl") or data.get("short_url") or long_url
        else:
            # shrtco.de – no API key needed
            resp = requests.get(
                f"https://api.shrtco.de/v2/shorten",
                params={"url": long_url},
                timeout=8,
            )
            data = resp.json()
            if data.get("ok"):
                return data["result"]["full_short_link"]
    except Exception as exc:
        logger.warning("URL shortener error: %s", exc)
    return long_url

# ─────────────────────────────────────────────────────────────
# 12. ADMIN-ONLY DECORATOR
# ─────────────────────────────────────────────────────────────
def admin_only(func):
    @wraps(func)
    async def wrapper(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
        uid = update.effective_user.id if update.effective_user else None
        if uid not in ADMIN_IDS:
            await update.message.reply_text("⛔ Admin only.")
            return
        return await func(update, ctx)
    return wrapper

# ─────────────────────────────────────────────────────────────
# 13. TELEGRAM HANDLERS
# ─────────────────────────────────────────────────────────────

# ── Helpers ──────────────────────────────────────────────────

def make_stream_token(user_id: int, file_id: str) -> str:
    msg = f"{user_id}:{file_id}:{FLASK_SECRET}"
    return hmac.new(FLASK_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()[:16]

def build_watch_url(user_id: int, pd_id: str) -> str:
    t = make_stream_token(user_id, pd_id)
    return f"{BASE_URL}/watch?id={pd_id}&u={user_id}&t={t}"

def build_file_links(file_unique_id: str) -> list[str]:
    """Generate deep links for all bots + current bot."""
    bots = parse_all_bots()
    if not bots:
        bots = [{"username": BOT_USERNAME, "url": f"https://t.me/{BOT_USERNAME}"}]
    links = []
    for b in bots:
        links.append(f"https://t.me/{b['username']}?start=file_{file_unique_id}")
    return links

async def send_verification(bot: Bot, user_id: int) -> None:
    verify_url = f"{BASE_URL}/verify/{user_id}"
    short_url  = shorten_url(verify_url)
    msg = (
        "🔐 *Token Required*\n\n"
        "Your access token has expired or is missing.\n"
        "Click the button below to get a new 24-hour token:\n\n"
        f"[🔗 Get Access Token]({short_url})\n\n"
        "_After verification, send the file link again._"
    )
    await bot.send_message(user_id, msg, parse_mode="Markdown", disable_web_page_preview=True)

async def check_and_reupload(file_doc: dict, bot: Bot) -> str | None:
    """Verify Pixeldrain file; re-upload if broken. Returns current pd_id or None."""
    pd_id = file_doc.get("pixeldrain_id")
    last_checked = file_doc.get("last_checked")
    now = datetime.now(timezone.utc)

    # Check at most once per hour
    if last_checked and (now - last_checked).total_seconds() < 3600:
        return pd_id

    files_col.update_one(
        {"file_unique_id": file_doc["file_unique_id"]},
        {"$set": {"last_checked": now}},
    )

    if pd_id and pixeldrain_check(pd_id):
        return pd_id

    # Re-download from Telegram
    logger.info("Re-uploading file %s", file_doc["file_unique_id"])
    for attempt in range(3):
        try:
            tg_file = await bot.get_file(file_doc["telegram_file_id"])
            buf = io.BytesIO()
            await tg_file.download_to_memory(buf)
            buf.seek(0)
            new_id = pixeldrain_upload(buf.read(), file_doc.get("file_name", "file"))
            if new_id:
                files_col.update_one(
                    {"file_unique_id": file_doc["file_unique_id"]},
                    {"$set": {"pixeldrain_id": new_id, "last_checked": now}},
                )
                return new_id
        except Exception as exc:
            wait = 2 ** attempt
            logger.warning("Re-upload attempt %d failed: %s – retrying in %ds", attempt + 1, exc, wait)
            time.sleep(wait)
    return None

# ── /start ────────────────────────────────────────────────────

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user:
        return
    track_active_user(user.id)
    get_or_create_user(user.id)

    args = ctx.args or []
    param = args[0] if args else ""

    # ── Referral registration ─────────────────────────────────
    if param.startswith("ref_"):
        try:
            ref_id = int(param[4:])
            if add_referral(ref_id, user.id):
                ref_count = users_col.find_one({"user_id": ref_id}, {"referrals_count": 1})
                count = ref_count.get("referrals_count", 0) if ref_count else 0
                # Milestone rewards
                if count >= 10:
                    grant_premium(ref_id, 7)
                    try:
                        await ctx.bot.send_message(
                            ref_id,
                            "🎉 You earned *7-day Premium* for reaching 10 referrals!",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                elif count >= 5:
                    grant_token(ref_id, TOKEN_VALIDITY_HOURS * 3)
                    try:
                        await ctx.bot.send_message(
                            ref_id,
                            f"🎁 Bonus! You got *{TOKEN_VALIDITY_HOURS * 3}h* token for 5 referrals!",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
                else:
                    try:
                        await ctx.bot.send_message(
                            ref_id,
                            f"👤 New referral from *{user.full_name}*! Total: {count}",
                            parse_mode="Markdown",
                        )
                    except Exception:
                        pass
        except (ValueError, Exception) as e:
            logger.debug("Referral parse error: %s", e)

    # ── Token verification callback ───────────────────────────
    if param.startswith("verify_"):
        try:
            vid = int(param[7:])
            if vid == user.id:
                grant_token(user.id)
                await update.message.reply_text(
                    f"✅ *Token granted!* Valid for {TOKEN_VALIDITY_HOURS} hours.\n"
                    "You can now access files. Send the file link again.",
                    parse_mode="Markdown",
                )
                return
        except ValueError:
            pass

    # ── File request ─────────────────────────────────────────
    if param.startswith("file_"):
        file_unique_id = param[5:]

        # Multi-bot: redirect new users if full (but NOT for file requests)
        # We still allow file access — redirect only on plain /start
        # (spec: "File requests NEVER redirect")

        if not has_valid_token(user.id):
            await send_verification(ctx.bot, user.id)
            return

        file_doc = get_file_by_unique_id(file_unique_id)
        if not file_doc:
            await update.message.reply_text("❌ File not found.")
            return

        pd_id = await check_and_reupload(file_doc, ctx.bot)
        if not pd_id:
            await update.message.reply_text("❌ File unavailable. Please try again later.")
            return

        watch_url = build_watch_url(user.id, pd_id)
        kb = InlineKeyboardMarkup([[
            InlineKeyboardButton("▶️ Watch / Stream", url=watch_url),
        ]])
        await update.message.reply_text(
            f"🎬 *{file_doc.get('file_name', 'File')}*\n\n"
            "Click below to stream or watch:",
            reply_markup=kb,
            parse_mode="Markdown",
        )
        return

    # ── Plain /start — redirect if bot full ──────────────────
    if is_bot_full():
        next_bot = parse_next_bot()
        if next_bot:
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(f"Go to @{next_bot['username']}", url=next_bot["url"]),
            ]])
            await update.message.reply_text(
                "⚠️ This bot is currently full.\nPlease use the next available bot:",
                reply_markup=kb,
            )
            return

    await update.message.reply_text(
        f"👋 Welcome, *{user.first_name}*!\n\n"
        "I can share and stream files securely.\n"
        "Use /referral to earn free access tokens.\n"
        "Use /premium to unlock unlimited access.",
        parse_mode="Markdown",
    )

# ── /referral ─────────────────────────────────────────────────

async def cmd_referral(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    track_active_user(user.id)
    ref_link = f"https://t.me/{BOT_USERNAME}?start=ref_{user.id}"
    short    = shorten_url(ref_link)
    user_doc = users_col.find_one({"user_id": user.id}, {"referrals_count": 1}) or {}
    count    = user_doc.get("referrals_count", 0)
    await update.message.reply_text(
        f"🔗 *Your Referral Link*\n\n"
        f"`{short}`\n\n"
        f"You have *{count}* referral(s).\n\n"
        "Rewards:\n"
        "• 5 referrals → Bonus token hours\n"
        "• 10 referrals → 7-day Premium 🎉",
        parse_mode="Markdown",
    )

# ── /premium ──────────────────────────────────────────────────

async def cmd_premium(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    track_active_user(update.effective_user.id)
    buttons = []
    for key, plan in PREMIUM_PLANS.items():
        buttons.append([InlineKeyboardButton(
            f"{plan['label']} — ₹{plan['price']}",
            callback_data=f"buy_{key}",
        )])
    kb = InlineKeyboardMarkup(buttons)
    await update.message.reply_text(
        "💎 *Premium Plans*\n\nSelect a plan to get payment details:",
        reply_markup=kb,
        parse_mode="Markdown",
    )

async def cb_buy_plan(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()
    key = query.data[4:]
    plan = PREMIUM_PLANS.get(key)
    if not plan:
        return
    qr_text = f"\n[View QR Code]({UPI_QR_URL})" if UPI_QR_URL else ""
    msg = (
        f"💳 *{plan['label']} — ₹{plan['price']}*\n\n"
        f"UPI ID: `{UPI_ID}`{qr_text}\n\n"
        "After payment:\n"
        f"1. Send `/utr <12-digit-UTR>` to confirm.\n"
        "2. We'll verify and activate within minutes."
    )
    await query.edit_message_text(msg, parse_mode="Markdown")
    ctx.user_data["pending_plan"] = key

# ── /utr ─────────────────────────────────────────────────────

async def cmd_utr(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    track_active_user(user.id)
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: `/utr <UTR number>`", parse_mode="Markdown")
        return
    utr = args[0].strip()
    if not re.match(r"^\d{12}$", utr):
        await update.message.reply_text("❌ Invalid UTR. Must be 12 digits.")
        return
    if payments_col.find_one({"utr": utr}):
        await update.message.reply_text("❌ This UTR has already been submitted.")
        return
    plan_key = ctx.user_data.get("pending_plan", "1month")
    plan = PREMIUM_PLANS.get(plan_key, PREMIUM_PLANS["1month"])
    doc = {
        "user_id":    user.id,
        "username":   user.username or "",
        "utr":        utr,
        "plan":       plan_key,
        "price":      plan["price"],
        "status":     "pending",
        "created_at": datetime.now(timezone.utc),
    }
    payments_col.insert_one(doc)
    await update.message.reply_text(
        f"✅ UTR *{utr}* submitted for *{plan['label']}* plan.\n"
        "An admin will review your payment shortly.",
        parse_mode="Markdown",
    )
    # Notify admins
    for aid in ADMIN_IDS:
        try:
            await ctx.bot.send_message(
                aid,
                f"💰 New payment from @{user.username or user.id}\n"
                f"Plan: {plan['label']} | UTR: `{utr}`\n"
                f"Approve: `/approve {user.id}`",
                parse_mode="Markdown",
            )
        except Exception:
            pass

# ── Admin commands ────────────────────────────────────────────

@admin_only
async def cmd_approve(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: `/approve <user_id>`", parse_mode="Markdown")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    pmt = payments_col.find_one({"user_id": uid, "status": "pending"})
    if not pmt:
        await update.message.reply_text("No pending payment for this user.")
        return
    plan = PREMIUM_PLANS.get(pmt["plan"], PREMIUM_PLANS["1month"])
    grant_premium(uid, plan["days"])
    payments_col.update_one({"_id": pmt["_id"]}, {"$set": {"status": "approved"}})
    await update.message.reply_text(f"✅ Premium granted to {uid}.")
    try:
        await ctx.bot.send_message(
            uid,
            f"🎉 *Payment Approved!*\nYou now have *{plan['label']}* premium access.",
            parse_mode="Markdown",
        )
    except Exception:
        pass

@admin_only
async def cmd_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: `/reject <user_id>`", parse_mode="Markdown")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    payments_col.update_many({"user_id": uid, "status": "pending"}, {"$set": {"status": "rejected"}})
    await update.message.reply_text(f"❌ Payment rejected for {uid}.")
    try:
        await ctx.bot.send_message(uid, "❌ Your payment was rejected. Contact support.")
    except Exception:
        pass

@admin_only
async def cmd_addpremium(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = ctx.args or []
    if len(args) < 2:
        await update.message.reply_text("Usage: `/addpremium <user_id> <days>`", parse_mode="Markdown")
        return
    try:
        uid  = int(args[0])
        days = int(args[1])
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments.")
        return
    grant_premium(uid, days)
    await update.message.reply_text(f"✅ Granted {days} days premium to {uid}.")

@admin_only
async def cmd_removepremium(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    args = ctx.args or []
    if not args:
        await update.message.reply_text("Usage: `/removepremium <user_id>`", parse_mode="Markdown")
        return
    try:
        uid = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.")
        return
    revoke_premium(uid)
    await update.message.reply_text(f"✅ Premium revoked for {uid}.")

@admin_only
async def cmd_stats(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    total_users   = users_col.count_documents({})
    now           = datetime.now(timezone.utc)
    premium_users = users_col.count_documents({"premium_expiry": {"$gt": now}})
    total_files   = files_col.count_documents({})
    pending_pmts  = payments_col.count_documents({"status": "pending"})
    concurrent    = get_concurrent_users()
    load_pct      = min(int(total_users / max(MAX_USERS, 1) * 100), 100)
    bar_filled    = int(load_pct / 10)
    bar           = "█" * bar_filled + "░" * (10 - bar_filled)
    await update.message.reply_text(
        f"📊 *Bot Stats — Server {SERVER_ID}*\n\n"
        f"👥 Users: {total_users}/{MAX_USERS}\n"
        f"💎 Premium: {premium_users}\n"
        f"🎬 Files: {total_files}\n"
        f"💰 Pending payments: {pending_pmts}\n"
        f"⚡ Active (5m): {concurrent}\n\n"
        f"Load: [{bar}] {load_pct}%",
        parse_mode="Markdown",
    )

@admin_only
async def cmd_files(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    docs = list(files_col.find().sort("created_at", -1).limit(10))
    if not docs:
        await update.message.reply_text("No files uploaded yet.")
        return
    lines = ["📁 *Last 10 Files*\n"]
    for d in docs:
        fuid = d["file_unique_id"]
        links = build_file_links(fuid)
        link_str = " | ".join(f"[Bot{i+1}]({l})" for i, l in enumerate(links))
        lines.append(f"• `{d.get('file_name','?')}` — {link_str}")
    await update.message.reply_text("\n".join(lines), parse_mode="Markdown", disable_web_page_preview=True)

@admin_only
async def cmd_broadcast(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message.reply_to_message:
        await update.message.reply_text("Reply to a message to broadcast it.")
        return
    src = update.message.reply_to_message
    all_users = list(users_col.find({}, {"user_id": 1}))
    sent = failed = 0
    for u in all_users:
        try:
            await src.copy(u["user_id"])
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)
    await update.message.reply_text(f"📢 Broadcast done. ✅ {sent} | ❌ {failed}")

# ── File upload handler (admin) ───────────────────────────────

async def handle_file(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    user = update.effective_user
    if not user or user.id not in ADMIN_IDS:
        return
    track_active_user(user.id)
    msg = update.message

    tg_file   = None
    file_type = None
    file_name = "file"

    if msg.video:
        tg_file   = msg.video
        file_type = "video"
        file_name = msg.video.file_name or f"video_{tg_file.file_unique_id}.mp4"
    elif msg.document:
        tg_file   = msg.document
        file_type = "document"
        file_name = msg.document.file_name or f"doc_{tg_file.file_unique_id}"
    elif msg.audio:
        tg_file   = msg.audio
        file_type = "audio"
        file_name = msg.audio.file_name or f"audio_{tg_file.file_unique_id}.mp3"

    if not tg_file:
        return

    existing = get_file_by_unique_id(tg_file.file_unique_id)
    if existing:
        links = build_file_links(tg_file.file_unique_id)
        link_str = "\n".join(f"• [Bot {i+1}]({l})" for i, l in enumerate(links))
        await msg.reply_text(
            f"♻️ File already exists!\n\n{link_str}",
            parse_mode="Markdown",
            disable_web_page_preview=True,
        )
        return

    status_msg = await msg.reply_text("⬇️ Downloading from Telegram…")

    try:
        tg_file_obj = await ctx.bot.get_file(tg_file.file_id)
        buf = io.BytesIO()
        await tg_file_obj.download_to_memory(buf)
        buf.seek(0)
        file_bytes = buf.read()
    except Exception as exc:
        await status_msg.edit_text(f"❌ Download failed: {exc}")
        return

    await status_msg.edit_text("⬆️ Uploading to Pixeldrain…")
    pd_id = pixeldrain_upload(file_bytes, file_name)
    if not pd_id:
        await status_msg.edit_text("❌ Pixeldrain upload failed.")
        return

    doc = {
        "file_unique_id":  tg_file.file_unique_id,
        "telegram_file_id": tg_file.file_id,
        "pixeldrain_id":   pd_id,
        "file_type":       file_type,
        "file_name":       file_name,
        "created_at":      datetime.now(timezone.utc),
        "last_checked":    datetime.now(timezone.utc),
    }
    save_file(doc)

    links = build_file_links(tg_file.file_unique_id)
    link_str = "\n".join(f"• [Bot {i+1}]({l})" for i, l in enumerate(links))
    await status_msg.edit_text(
        f"✅ *Uploaded:* `{file_name}`\n\n"
        f"📎 File links:\n{link_str}",
        parse_mode="Markdown",
        disable_web_page_preview=True,
    )

# ─────────────────────────────────────────────────────────────
# 14. FLASK APP + HTML PLAYER TEMPLATE
# ─────────────────────────────────────────────────────────────
flask_app = Flask(__name__)
flask_app.secret_key = FLASK_SECRET

WATCH_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{{ filename }} — FileBot Player</title>
<style>
  @import url('https://fonts.googleapis.com/css2?family=Syne:wght@400;700&display=swap');
  :root {
    --bg: #0a0a0f;
    --surface: #13131a;
    --accent: #6c63ff;
    --text: #e8e8f0;
    --muted: #6b6b80;
  }
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg);
    color: var(--text);
    font-family: 'Syne', sans-serif;
    min-height: 100vh;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    padding: 1rem;
  }
  .wrapper {
    width: 100%;
    max-width: 900px;
  }
  h1 {
    font-size: clamp(1rem, 3vw, 1.4rem);
    font-weight: 700;
    margin-bottom: 0.75rem;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
  }
  .player-box {
    background: var(--surface);
    border-radius: 12px;
    overflow: hidden;
    box-shadow: 0 8px 48px rgba(108,99,255,0.15);
    border: 1px solid rgba(108,99,255,0.18);
  }
  video {
    width: 100%;
    display: block;
    background: #000;
    max-height: 70vh;
  }
  footer {
    margin-top: 1.2rem;
    font-size: 0.75rem;
    color: var(--muted);
    text-align: center;
    letter-spacing: 0.05em;
  }
  footer span { color: var(--accent); }
</style>
</head>
<body>
<div class="wrapper">
  <h1>▶ {{ filename }}</h1>
  <div class="player-box">
    <video controls autoplay preload="metadata">
      <source src="{{ stream_url }}" type="video/mp4">
      Your browser does not support HTML5 video.
    </video>
  </div>
  <footer>Powered by <span>FileBot</span></footer>
</div>
</body>
</html>"""

# ─────────────────────────────────────────────────────────────
# 15. VERIFY STREAM ACCESS (HMAC)
# ─────────────────────────────────────────────────────────────
def verify_stream_access(pd_id: str, user_id: str, token: str) -> bool:
    try:
        uid = int(user_id)
    except (ValueError, TypeError):
        return False
    expected = make_stream_token(uid, pd_id)
    return hmac.compare_digest(expected, token)

# ─────────────────────────────────────────────────────────────
# 16. /watch ROUTE
# ─────────────────────────────────────────────────────────────
@flask_app.route("/watch")
def route_watch():
    pd_id   = request.args.get("id", "")
    user_id = request.args.get("u", "")
    token   = request.args.get("t", "")

    if not verify_stream_access(pd_id, user_id, token):
        return "403 Forbidden", 403

    try:
        uid = int(user_id)
    except ValueError:
        return "400 Bad Request", 400

    if not has_valid_token(uid):
        return "Access denied. Token expired.", 403

    file_doc  = files_col.find_one({"pixeldrain_id": pd_id}, {"file_name": 1})
    filename  = file_doc["file_name"] if file_doc else "video.mp4"
    stream_url = f"{BASE_URL}/stream?id={pd_id}&u={user_id}&t={token}"
    return render_template_string(WATCH_HTML, filename=filename, stream_url=stream_url)

# ─────────────────────────────────────────────────────────────
# 17. /stream ROUTE — CRITICAL PROXY
# ─────────────────────────────────────────────────────────────
@flask_app.route("/stream")
def route_stream():
    pd_id   = request.args.get("id", "")
    user_id = request.args.get("u", "")
    token   = request.args.get("t", "")

    if not verify_stream_access(pd_id, user_id, token):
        return "403 Forbidden", 403

    try:
        uid = int(user_id)
    except ValueError:
        return "400 Bad Request", 400

    if not has_valid_token(uid):
        return "Access denied. Token expired.", 403

    range_header = request.headers.get("Range", "bytes=0-")

    # Single GET to Pixeldrain — no HEAD request
    result = list(pixeldrain_stream_generator(pd_id, range_header))
    gen, status_code, resp_headers = result[0]

    if gen is None:
        return "502 Bad Gateway", 502

    return Response(
        stream_with_context(gen),
        status=status_code,
        headers=resp_headers,
        direct_passthrough=True,
    )

# ─────────────────────────────────────────────────────────────
# 18. /health ROUTE
# ─────────────────────────────────────────────────────────────
@flask_app.route("/health")
def route_health():
    return {"status": "ok", "server": SERVER_ID, "bot": BOT_USERNAME}, 200

# ─────────────────────────────────────────────────────────────
# 19. /verify/{user_id} ROUTE (shortener redirect)
# ─────────────────────────────────────────────────────────────
@flask_app.route("/verify/<int:user_id>")
def route_verify(user_id: int):
    """
    Redirect users to Telegram deep link that grants token.
    Shortener → BASE_URL/verify/{uid} → t.me/bot?start=verify_{uid}
    """
    deep_link = f"https://t.me/{BOT_USERNAME}?start=verify_{user_id}"
    return redirect(deep_link)

# ─────────────────────────────────────────────────────────────
# 20. BUILD TELEGRAM APPLICATION
# ─────────────────────────────────────────────────────────────
def build_application() -> Application:
    app = Application.builder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start",         cmd_start))
    app.add_handler(CommandHandler("referral",      cmd_referral))
    app.add_handler(CommandHandler("premium",       cmd_premium))
    app.add_handler(CommandHandler("utr",           cmd_utr))
    app.add_handler(CommandHandler("approve",       cmd_approve))
    app.add_handler(CommandHandler("reject",        cmd_reject))
    app.add_handler(CommandHandler("addpremium",    cmd_addpremium))
    app.add_handler(CommandHandler("removepremium", cmd_removepremium))
    app.add_handler(CommandHandler("stats",         cmd_stats))
    app.add_handler(CommandHandler("files",         cmd_files))
    app.add_handler(CommandHandler("broadcast",     cmd_broadcast))
    app.add_handler(CallbackQueryHandler(cb_buy_plan, pattern=r"^buy_"))
    app.add_handler(MessageHandler(
        filters.VIDEO | filters.Document.ALL | filters.AUDIO,
        handle_file,
    ))
    return app

# ─────────────────────────────────────────────────────────────
# 21. WEBHOOK MODE FUNCTIONS
# ─────────────────────────────────────────────────────────────
_bot_loop: asyncio.AbstractEventLoop | None = None
_application: Application | None = None

def run_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    asyncio.set_event_loop(loop)
    loop.run_forever()

def init_bot() -> None:
    global _bot_loop, _application

    _bot_loop = asyncio.new_event_loop()
    loop_thread = threading.Thread(target=run_event_loop, args=(_bot_loop,), daemon=True)
    loop_thread.start()

    _application = build_application()

    async def _start_app():
        async with _application:
            await _application.initialize()
            await _application.start()
            # Set webhook
            webhook_url = f"{BASE_URL}/webhook"
            await _application.bot.set_webhook(
                url=webhook_url,
                allowed_updates=["message", "callback_query"],
                drop_pending_updates=True,
            )
            logger.info("Webhook set: %s", webhook_url)
            # Set bot commands
            await _application.bot.set_my_commands([
                BotCommand("start",         "Start the bot"),
                BotCommand("premium",       "Premium plans"),
                BotCommand("referral",      "Your referral link"),
                BotCommand("utr",           "Submit payment UTR"),
                BotCommand("stats",         "Bot statistics (admin)"),
                BotCommand("files",         "List files (admin)"),
                BotCommand("broadcast",     "Broadcast message (admin)"),
            ])
            # Keep running until loop stops
            while True:
                await asyncio.sleep(3600)

    asyncio.run_coroutine_threadsafe(_start_app(), _bot_loop)
    # Give the application a moment to initialize
    time.sleep(2)
    logger.info("Bot application started in background loop.")

def process_update(update_data: dict) -> None:
    """Deserialize and process a Telegram update in the bot's event loop."""
    async def _process():
        update = Update.de_json(update_data, _application.bot)
        await _application.process_update(update)

    future = asyncio.run_coroutine_threadsafe(_process(), _bot_loop)
    try:
        future.result(timeout=60)
    except Exception as exc:
        logger.error("Update processing error: %s", exc)

# ─────────────────────────────────────────────────────────────
# 22. WEBHOOK FLASK ROUTE
# ─────────────────────────────────────────────────────────────
@flask_app.route("/webhook", methods=["POST"])
def webhook_route():
    data = request.get_json(force=True, silent=True)
    if not data:
        return "Bad Request", 400
    process_update(data)
    return "OK", 200

# ─────────────────────────────────────────────────────────────
# 23. MAIN
# ─────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("=== FileBot Server %s starting ===", SERVER_ID)
    setup_indexes()
    init_bot()
    logger.info("Flask listening on port %d", PORT)
    flask_app.run(
        host="0.0.0.0",
        port=PORT,
        threaded=True,
        debug=False,
        use_reloader=False,
    )

if __name__ == "__main__":
    main()
