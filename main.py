"""
=============================================================================
TELEGRAM FILE SHARING BOT - PRODUCTION READY
=============================================================================
Author: Senior Python Backend Engineer
Stack: python-telegram-bot + MongoDB + Webhook Mode
Features: File Sharing, Token Verification, Referral, Premium, UPI Payments
=============================================================================
"""

import os
import asyncio
import logging
import hashlib
import secrets
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Optional

import httpx
from pymongo import MongoClient, ASCENDING
from pymongo.errors import DuplicateKeyError
from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup,
    BotCommand, InputFile
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, CallbackQueryHandler,
    ContextTypes, filters
)
from telegram.error import TelegramError
from flask import Flask, request as flask_request
import threading

# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION — Set via Environment Variables
# =============================================================================
class Config:
    # Bot & DB
    BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
    MONGO_URI: str = os.environ.get("MONGO_URI", "YOUR_MONGO_URI_HERE")
    BOT_USERNAME: str = os.environ.get("BOT_USERNAME", "YourBotUsername")

    # Admin IDs — comma-separated e.g. "123456,789012"
    ADMIN_IDS: list[int] = [
        int(x.strip())
        for x in os.environ.get("ADMIN_IDS", "0").split(",")
        if x.strip().isdigit()
    ]

    # Webhook
    WEBHOOK_URL: str = os.environ.get("WEBHOOK_URL", "https://your-app.onrender.com")
    PORT: int = int(os.environ.get("PORT", "8443"))

    # Token Settings
    TOKEN_VALIDITY_HOURS: int = int(os.environ.get("TOKEN_VALIDITY_HOURS", "24"))

    # URL Shortener (using is.gd free API — no key needed; swap for paid if needed)
    SHORTENER_API: str = os.environ.get("SHORTENER_API", "isgd")  # "isgd" or "tinyurl"
    SHORTENER_API_KEY: str = os.environ.get("SHORTENER_API_KEY", "")  # For paid APIs

    # UPI Payment Details
    UPI_ID: str = os.environ.get("UPI_ID", "yourname@upi")
    UPI_NAME: str = os.environ.get("UPI_NAME", "Bot Premium")

    # Premium Plans (days: price_inr)
    PREMIUM_PLANS: dict = {
        "7": {"days": 7,  "price": 49,  "label": "7 Days  — ₹49"},
        "30": {"days": 30, "price": 149, "label": "30 Days — ₹149"},
        "90": {"days": 90, "price": 399, "label": "90 Days — ₹399"},
    }

    # Referral reward (hours of token access per successful referral)
    REFERRAL_REWARD_HOURS: int = int(os.environ.get("REFERRAL_REWARD_HOURS", "6"))

    # Rate limiting (max requests per user per minute)
    RATE_LIMIT_PER_MIN: int = 10

# =============================================================================
# DATABASE LAYER
# =============================================================================
class Database:
    """All database operations — single source of truth."""

    def __init__(self):
        self.client = MongoClient(Config.MONGO_URI, serverSelectionTimeoutMS=5000)
        self.db = self.client["telegram_file_bot"]
        self._setup_collections()
        self._create_indexes()
        logger.info("✅ MongoDB connected and indexes created.")

    def _setup_collections(self):
        self.users     = self.db["users"]
        self.files     = self.db["files"]
        self.payments  = self.db["payments"]
        self.referrals = self.db["referrals"]
        self.rate_limit_store = self.db["rate_limits"]

    def _create_indexes(self):
        # Users
        self.users.create_index([("user_id", ASCENDING)], unique=True)
        self.users.create_index([("referred_by", ASCENDING)])

        # Files
        self.files.create_index([("file_unique_id", ASCENDING)], unique=True)
        self.files.create_index([("created_at", ASCENDING)])

        # Payments
        self.payments.create_index([("user_id", ASCENDING)])
        self.payments.create_index([("utr", ASCENDING)])
        self.payments.create_index([("status", ASCENDING)])

        # Referrals
        self.referrals.create_index([("referred_user_id", ASCENDING)], unique=True)
        self.referrals.create_index([("referrer_id", ASCENDING)])

        # Rate limits
        self.rate_limit_store.create_index([("user_id", ASCENDING)], unique=True)

    # -------------------------------------------------------------------------
    # USER OPERATIONS
    # -------------------------------------------------------------------------
    def get_user(self, user_id: int) -> Optional[dict]:
        return self.users.find_one({"user_id": user_id})

    def upsert_user(self, user_id: int, **kwargs) -> None:
        """Create user if not exists, or update fields."""
        self.users.update_one(
            {"user_id": user_id},
            {"$setOnInsert": {"user_id": user_id, "joined_at": datetime.now(timezone.utc),
                              "referrals_count": 0, "referred_by": None,
                              "token_expiry": None, "premium_expiry": None},
             "$set": kwargs},
            upsert=True
        )

    def set_token(self, user_id: int, hours: int = None) -> None:
        hours = hours or Config.TOKEN_VALIDITY_HOURS
        expiry = datetime.now(timezone.utc) + timedelta(hours=hours)
        self.users.update_one(
            {"user_id": user_id},
            {"$set": {"token_expiry": expiry}},
            upsert=True
        )

    def is_token_valid(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user:
            return False
        expiry = user.get("token_expiry")
        if not expiry:
            return False
        # Ensure timezone-aware comparison
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return expiry > datetime.now(timezone.utc)

    def is_premium(self, user_id: int) -> bool:
        user = self.get_user(user_id)
        if not user:
            return False
        expiry = user.get("premium_expiry")
        if not expiry:
            return False
        if expiry.tzinfo is None:
            expiry = expiry.replace(tzinfo=timezone.utc)
        return expiry > datetime.now(timezone.utc)

    def grant_premium(self, user_id: int, days: int) -> datetime:
        user = self.get_user(user_id)
        now = datetime.now(timezone.utc)
        current_expiry = user.get("premium_expiry") if user else None
        if current_expiry:
            if current_expiry.tzinfo is None:
                current_expiry = current_expiry.replace(tzinfo=timezone.utc)
            # Extend if already premium
            base = max(current_expiry, now)
        else:
            base = now
        new_expiry = base + timedelta(days=days)
        self.users.update_one(
            {"user_id": user_id},
            {"$set": {"premium_expiry": new_expiry}},
            upsert=True
        )
        return new_expiry

    def revoke_premium(self, user_id: int) -> None:
        self.users.update_one(
            {"user_id": user_id},
            {"$set": {"premium_expiry": None}}
        )

    def get_stats(self) -> dict:
        total_users    = self.users.count_documents({})
        premium_users  = self.users.count_documents(
            {"premium_expiry": {"$gt": datetime.now(timezone.utc)}}
        )
        total_files    = self.files.count_documents({})
        total_payments = self.payments.count_documents({"status": "approved"})
        return {
            "total_users": total_users,
            "premium_users": premium_users,
            "total_files": total_files,
            "approved_payments": total_payments,
        }

    # -------------------------------------------------------------------------
    # FILE OPERATIONS
    # -------------------------------------------------------------------------
    def save_file(self, file_unique_id: str, telegram_file_id: str,
                  file_name: str = "", file_type: str = "document") -> bool:
        try:
            self.files.insert_one({
                "file_unique_id": file_unique_id,
                "telegram_file_id": telegram_file_id,
                "file_name": file_name,
                "file_type": file_type,
                "created_at": datetime.now(timezone.utc),
            })
            return True
        except DuplicateKeyError:
            return False

    def get_file(self, file_unique_id: str) -> Optional[dict]:
        return self.files.find_one({"file_unique_id": file_unique_id})

    def get_all_files(self, limit: int = 20) -> list:
        return list(self.files.find({}, {"_id": 0}).sort("created_at", -1).limit(limit))

    # -------------------------------------------------------------------------
    # PAYMENT OPERATIONS
    # -------------------------------------------------------------------------
    def create_payment(self, user_id: int, utr: str, plan_days: int, amount: int) -> bool:
        # Check duplicate UTR
        if self.payments.find_one({"utr": utr}):
            return False
        self.payments.insert_one({
            "user_id": user_id,
            "utr": utr,
            "plan_days": plan_days,
            "amount": amount,
            "status": "pending",
            "created_at": datetime.now(timezone.utc),
        })
        return True

    def get_pending_payment(self, user_id: int) -> Optional[dict]:
        return self.payments.find_one({"user_id": user_id, "status": "pending"})

    def approve_payment(self, user_id: int) -> Optional[dict]:
        payment = self.get_pending_payment(user_id)
        if not payment:
            return None
        self.payments.update_one(
            {"_id": payment["_id"]},
            {"$set": {"status": "approved", "approved_at": datetime.now(timezone.utc)}}
        )
        return payment

    def reject_payment(self, user_id: int) -> Optional[dict]:
        payment = self.get_pending_payment(user_id)
        if not payment:
            return None
        self.payments.update_one(
            {"_id": payment["_id"]},
            {"$set": {"status": "rejected", "rejected_at": datetime.now(timezone.utc)}}
        )
        return payment

    # -------------------------------------------------------------------------
    # REFERRAL OPERATIONS
    # -------------------------------------------------------------------------
    def record_referral(self, referrer_id: int, referred_user_id: int) -> bool:
        """Returns True if referral recorded (first time), False if already exists."""
        try:
            self.referrals.insert_one({
                "referrer_id": referrer_id,
                "referred_user_id": referred_user_id,
                "timestamp": datetime.now(timezone.utc),
            })
            self.users.update_one(
                {"user_id": referrer_id},
                {"$inc": {"referrals_count": 1}}
            )
            return True
        except DuplicateKeyError:
            return False

    def get_referral_count(self, user_id: int) -> int:
        user = self.get_user(user_id)
        return user.get("referrals_count", 0) if user else 0

    # -------------------------------------------------------------------------
    # RATE LIMITING
    # -------------------------------------------------------------------------
    def check_rate_limit(self, user_id: int) -> bool:
        """Returns True if user is within limit, False if rate limited."""
        now = time.time()
        window_start = now - 60  # 1 minute window

        doc = self.rate_limit_store.find_one({"user_id": user_id})
        if not doc:
            self.rate_limit_store.update_one(
                {"user_id": user_id},
                {"$set": {"timestamps": [now]}},
                upsert=True
            )
            return True

        timestamps = [t for t in doc.get("timestamps", []) if t > window_start]
        if len(timestamps) >= Config.RATE_LIMIT_PER_MIN:
            return False

        timestamps.append(now)
        self.rate_limit_store.update_one(
            {"user_id": user_id},
            {"$set": {"timestamps": timestamps}}
        )
        return True


# =============================================================================
# URL SHORTENER
# =============================================================================
class URLShortener:
    """Supports multiple shortener backends with fallback."""

    @staticmethod
    async def shorten(long_url: str) -> str:
        """Returns shortened URL or original on failure."""
        try:
            if Config.SHORTENER_API == "isgd":
                return await URLShortener._isgd(long_url)
            elif Config.SHORTENER_API == "tinyurl":
                return await URLShortener._tinyurl(long_url)
            else:
                return long_url
        except Exception as e:
            logger.warning(f"URL Shortener failed: {e} — returning original URL")
            return long_url

    @staticmethod
    async def _isgd(long_url: str) -> str:
        """is.gd free shortener — no API key needed."""
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://is.gd/create.php",
                params={"format": "simple", "url": long_url}
            )
            resp.raise_for_status()
            shortened = resp.text.strip()
            if shortened.startswith("http"):
                return shortened
            raise ValueError(f"is.gd returned unexpected: {shortened}")

    @staticmethod
    async def _tinyurl(long_url: str) -> str:
        """TinyURL free shortener."""
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                "https://tinyurl.com/api-create.php",
                params={"url": long_url}
            )
            resp.raise_for_status()
            return resp.text.strip()


# =============================================================================
# HELPERS & DECORATORS
# =============================================================================
db = Database()

def admin_only(func):
    """Decorator: restrict command to admins only."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if user_id not in Config.ADMIN_IDS:
            await update.message.reply_text(
                "🚫 *Access Denied*\n\nThis command is for admins only.",
                parse_mode="Markdown"
            )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

def rate_limited(func):
    """Decorator: apply rate limiting per user."""
    @wraps(func)
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id
        if not db.check_rate_limit(user_id):
            await update.message.reply_text(
                "⚠️ *Slow down!*\n\nToo many requests. Please wait a moment.",
                parse_mode="Markdown"
            )
            return
        return await func(update, context, *args, **kwargs)
    return wrapper

def generate_file_id() -> str:
    """Generate a unique short ID for a file."""
    return secrets.token_urlsafe(8)

async def send_verification_link(update: Update, file_unique_id: str) -> None:
    """Generate and send the token verification link to user."""
    user_id = update.effective_user.id

    # The target URL after verification (deep link back to the file)
    callback_url = f"https://t.me/{Config.BOT_USERNAME}?start=verified_{user_id}_{file_unique_id}"

    # Shorten via URL shortener (this acts as the "token step")
    short_link = await URLShortener.shorten(callback_url)

    keyboard = [[InlineKeyboardButton("🔗 Complete Verification", url=short_link)]]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "🔐 *Verification Required*\n\n"
        "To access this file, complete a quick verification step:\n\n"
        "1️⃣ Tap the button below\n"
        "2️⃣ Visit the link that opens\n"
        "3️⃣ Come back and click *Get File* again\n\n"
        "⏱ Access is valid for *24 hours* after verification.",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )


# =============================================================================
# COMMAND HANDLERS
# =============================================================================

async def start_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle /start — entry point for all deep links."""
    user = update.effective_user
    user_id = user.id
    args = context.args  # List of arguments after /start

    # Ensure user exists in DB
    db.upsert_user(user_id)

    # ── DEEP LINK ROUTING ────────────────────────────────────────────────────
    if args:
        payload = args[0]

        # ── FILE ACCESS: start=file_<id> ──────────────────────────────────
        if payload.startswith("file_"):
            file_unique_id = payload[5:]
            await handle_file_access(update, context, file_unique_id)
            return

        # ── POST-VERIFICATION: start=verified_<uid>_<fid> ────────────────
        if payload.startswith("verified_"):
            parts = payload[9:].split("_", 1)
            if len(parts) == 2:
                verified_uid, file_unique_id = parts
                if str(user_id) == verified_uid:
                    # Grant token
                    db.set_token(user_id, Config.TOKEN_VALIDITY_HOURS)
                    await update.message.reply_text(
                        "✅ *Verification Successful!*\n\n"
                        "🎉 Access unlocked — enjoy your content!\n\n"
                        "📂 Fetching your file now...",
                        parse_mode="Markdown"
                    )
                    await asyncio.sleep(1)
                    await handle_file_access(update, context, file_unique_id, already_verified=True)
                    return
                else:
                    await update.message.reply_text(
                        "⚠️ This verification link belongs to a different account.",
                        parse_mode="Markdown"
                    )
                    return

        # ── REFERRAL: start=ref_<referrer_id> ────────────────────────────
        if payload.startswith("ref_"):
            referrer_id_str = payload[4:]
            if referrer_id_str.isdigit():
                referrer_id = int(referrer_id_str)

                # Anti-abuse: no self-referral
                if referrer_id == user_id:
                    await update.message.reply_text(
                        "😅 You can't refer yourself!\n"
                        "Share your link with friends to earn rewards.",
                        parse_mode="Markdown"
                    )
                else:
                    # Check if this user was already referred
                    existing = db.get_user(user_id)
                    if existing and existing.get("referred_by") is None:
                        # Record referral
                        recorded = db.record_referral(referrer_id, user_id)
                        if recorded:
                            # Update referred_by
                            db.upsert_user(user_id, referred_by=referrer_id)

                            # Reward referrer with bonus token hours
                            db.set_token(referrer_id, Config.REFERRAL_REWARD_HOURS)

                            # Notify referrer
                            try:
                                await context.bot.send_message(
                                    chat_id=referrer_id,
                                    text=(
                                        "🎉 *New user joined via your link!*\n\n"
                                        f"👤 Someone just signed up using your referral.\n"
                                        f"🔓 *Bonus access unlocked!* "
                                        f"+{Config.REFERRAL_REWARD_HOURS}h token added.\n\n"
                                        "Keep sharing to earn more! 🚀"
                                    ),
                                    parse_mode="Markdown"
                                )
                            except TelegramError:
                                pass  # Referrer may have blocked bot

    # ── DEFAULT WELCOME MESSAGE ───────────────────────────────────────────────
    keyboard = [
        [InlineKeyboardButton("📂 My Files", callback_data="my_files"),
         InlineKeyboardButton("💎 Get Premium", callback_data="premium_info")],
        [InlineKeyboardButton("👥 Referral", callback_data="referral"),
         InlineKeyboardButton("ℹ️ Help", callback_data="help")],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        f"👋 *Welcome, {user.first_name}!*\n\n"
        "🤖 I'm a secure file sharing bot.\n\n"
        "📌 *What I can do:*\n"
        "• Share files securely via links\n"
        "• Verify access with a quick token step\n"
        "• Offer premium for unlimited access\n"
        "• Reward you for referring friends\n\n"
        "👇 *Choose an option below to get started:*",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )


async def handle_file_access(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    file_unique_id: str,
    already_verified: bool = False
) -> None:
    """Core logic: check access rights and send file."""
    user_id = update.effective_user.id

    # Fetch file from DB
    file_doc = db.get_file(file_unique_id)
    if not file_doc:
        await update.message.reply_text(
            "❌ *File not found!*\n\n"
            "This file may have been removed or the link is invalid.",
            parse_mode="Markdown"
        )
        return

    # Check access: Premium → Token → Verification required
    if db.is_premium(user_id):
        pass  # Premium users always get access
    elif db.is_token_valid(user_id) or already_verified:
        pass  # Token valid
    else:
        # Need verification
        await send_verification_link(update, file_unique_id)
        return

    # ── SEND FILE ─────────────────────────────────────────────────────────────
    await update.message.reply_text(
        "📂 *Preparing your file...*\n"
        "⚡ Please wait a moment",
        parse_mode="Markdown"
    )

    try:
        telegram_file_id = file_doc["telegram_file_id"]
        file_type = file_doc.get("file_type", "document")
        file_name = file_doc.get("file_name", "file")

        if file_type == "video":
            await context.bot.send_video(
                chat_id=update.effective_chat.id,
                video=telegram_file_id,
                caption=f"📹 *{file_name}*\n\n_Enjoy your content!_ 🎬",
                parse_mode="Markdown"
            )
        elif file_type == "photo":
            await context.bot.send_photo(
                chat_id=update.effective_chat.id,
                photo=telegram_file_id,
                caption=f"🖼 *{file_name}*",
                parse_mode="Markdown"
            )
        elif file_type == "audio":
            await context.bot.send_audio(
                chat_id=update.effective_chat.id,
                audio=telegram_file_id,
                caption=f"🎵 *{file_name}*",
                parse_mode="Markdown"
            )
        else:
            await context.bot.send_document(
                chat_id=update.effective_chat.id,
                document=telegram_file_id,
                caption=f"📄 *{file_name}*\n\n✅ _Delivered securely!_",
                parse_mode="Markdown"
            )
    except TelegramError as e:
        logger.error(f"Error sending file {file_unique_id}: {e}")
        await update.message.reply_text(
            "⚠️ *Error delivering file.*\n"
            "Please try again or contact support.",
            parse_mode="Markdown"
        )


# =============================================================================
# ADMIN — FILE UPLOAD
# =============================================================================
async def handle_file_upload(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle file uploads — admin only."""
    user_id = update.effective_user.id

    # Only admins can upload
    if user_id not in Config.ADMIN_IDS:
        return  # Silently ignore non-admin uploads

    msg = update.message
    telegram_file_id = None
    file_name = "Untitled"
    file_type = "document"

    if msg.document:
        telegram_file_id = msg.document.file_id
        file_name = msg.document.file_name or "Document"
        file_type = "document"
    elif msg.video:
        telegram_file_id = msg.video.file_id
        file_name = msg.video.file_name or "Video"
        file_type = "video"
    elif msg.audio:
        telegram_file_id = msg.audio.file_id
        file_name = msg.audio.file_name or "Audio"
        file_type = "audio"
    elif msg.photo:
        telegram_file_id = msg.photo[-1].file_id  # Largest size
        file_name = "Photo"
        file_type = "photo"
    else:
        return  # Not a supported file type

    # Generate unique ID for file
    file_unique_id = generate_file_id()

    # Save to DB
    saved = db.save_file(file_unique_id, telegram_file_id, file_name, file_type)
    if not saved:
        await msg.reply_text("⚠️ File already exists in database!")
        return

    # Generate deep link
    deep_link = f"https://t.me/{Config.BOT_USERNAME}?start=file_{file_unique_id}"

    await msg.reply_text(
        f"✅ *File Saved Successfully!*\n\n"
        f"📄 *Name:* `{file_name}`\n"
        f"🆔 *ID:* `{file_unique_id}`\n"
        f"🔗 *Share Link:*\n`{deep_link}`\n\n"
        f"Share this link with users to give them access!",
        parse_mode="Markdown"
    )


# =============================================================================
# ADMIN COMMANDS
# =============================================================================

@admin_only
async def stats_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/stats — show bot statistics."""
    stats = db.get_stats()
    await update.message.reply_text(
        "📊 *Bot Statistics*\n\n"
        f"👥 Total Users: *{stats['total_users']}*\n"
        f"💎 Premium Users: *{stats['premium_users']}*\n"
        f"📂 Total Files: *{stats['total_files']}*\n"
        f"✅ Approved Payments: *{stats['approved_payments']}*",
        parse_mode="Markdown"
    )


@admin_only
async def files_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/files — list recent files."""
    files = db.get_all_files(limit=15)
    if not files:
        await update.message.reply_text("📂 No files uploaded yet.")
        return

    text = "📂 *Recent Files (last 15):*\n\n"
    for f in files:
        deep_link = f"https://t.me/{Config.BOT_USERNAME}?start=file_{f['file_unique_id']}"
        text += f"• `{f['file_name']}` — [Link]({deep_link})\n"

    await update.message.reply_text(text, parse_mode="Markdown", disable_web_page_preview=True)


@admin_only
async def add_premium_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/addpremium <user_id> <days>"""
    args = context.args
    if len(args) < 2 or not args[0].isdigit() or not args[1].isdigit():
        await update.message.reply_text(
            "⚠️ Usage: `/addpremium <user_id> <days>`", parse_mode="Markdown"
        )
        return

    target_uid = int(args[0])
    days = int(args[1])
    expiry = db.grant_premium(target_uid, days)

    await update.message.reply_text(
        f"💎 *Premium Granted!*\n\n"
        f"👤 User: `{target_uid}`\n"
        f"📅 Days Added: *{days}*\n"
        f"⏳ Expires: `{expiry.strftime('%Y-%m-%d %H:%M UTC')}`",
        parse_mode="Markdown"
    )

    # Notify the user
    try:
        await context.bot.send_message(
            chat_id=target_uid,
            text=(
                "💎 *Premium Activated!*\n\n"
                f"🚀 Enjoy fast & unlimited access for *{days} days*!\n"
                f"⏳ Expires: `{expiry.strftime('%Y-%m-%d %H:%M UTC')}`\n\n"
                "Thank you for being a premium member! 🙏"
            ),
            parse_mode="Markdown"
        )
    except TelegramError:
        pass


@admin_only
async def remove_premium_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/removepremium <user_id>"""
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text(
            "⚠️ Usage: `/removepremium <user_id>`", parse_mode="Markdown"
        )
        return

    target_uid = int(args[0])
    db.revoke_premium(target_uid)
    await update.message.reply_text(
        f"✅ Premium removed for user `{target_uid}`.", parse_mode="Markdown"
    )


@admin_only
async def approve_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/approve <user_id>"""
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text(
            "⚠️ Usage: `/approve <user_id>`", parse_mode="Markdown"
        )
        return

    target_uid = int(args[0])
    payment = db.approve_payment(target_uid)
    if not payment:
        await update.message.reply_text(f"❌ No pending payment found for `{target_uid}`.")
        return

    days = payment.get("plan_days", 30)
    expiry = db.grant_premium(target_uid, days)

    await update.message.reply_text(
        f"✅ *Payment Approved!*\n\n"
        f"👤 User: `{target_uid}`\n"
        f"💰 UTR: `{payment.get('utr', 'N/A')}`\n"
        f"📅 Premium for: *{days} days*",
        parse_mode="Markdown"
    )

    try:
        await context.bot.send_message(
            chat_id=target_uid,
            text=(
                "💎 *Premium Activated!*\n\n"
                "🚀 Enjoy fast & unlimited access!\n"
                f"⏳ Expires: `{expiry.strftime('%Y-%m-%d %H:%M UTC')}`\n\n"
                "Thank you for your payment! 🙏"
            ),
            parse_mode="Markdown"
        )
    except TelegramError:
        pass


@admin_only
async def reject_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/reject <user_id>"""
    args = context.args
    if not args or not args[0].isdigit():
        await update.message.reply_text(
            "⚠️ Usage: `/reject <user_id>`", parse_mode="Markdown"
        )
        return

    target_uid = int(args[0])
    payment = db.reject_payment(target_uid)
    if not payment:
        await update.message.reply_text(f"❌ No pending payment found for `{target_uid}`.")
        return

    await update.message.reply_text(
        f"❌ Payment rejected for `{target_uid}`.", parse_mode="Markdown"
    )

    try:
        await context.bot.send_message(
            chat_id=target_uid,
            text=(
                "❌ *Payment Rejected*\n\n"
                "Your payment could not be verified.\n"
                "Please resubmit with a valid UTR number or contact support."
            ),
            parse_mode="Markdown"
        )
    except TelegramError:
        pass


@admin_only
async def broadcast_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/broadcast <message> — send message to all users."""
    if not context.args:
        await update.message.reply_text(
            "⚠️ Usage: `/broadcast Your message here`", parse_mode="Markdown"
        )
        return

    broadcast_text = " ".join(context.args)
    all_users = list(db.users.find({}, {"user_id": 1}))
    total = len(all_users)
    sent = 0
    failed = 0

    status_msg = await update.message.reply_text(
        f"📡 Broadcasting to {total} users...", parse_mode="Markdown"
    )

    for user_doc in all_users:
        uid = user_doc["user_id"]
        try:
            await context.bot.send_message(
                chat_id=uid,
                text=f"📢 *Announcement*\n\n{broadcast_text}",
                parse_mode="Markdown"
            )
            sent += 1
        except TelegramError:
            failed += 1
        await asyncio.sleep(0.05)  # Throttle to avoid flood limits

    await status_msg.edit_text(
        f"📡 *Broadcast Complete!*\n\n"
        f"✅ Sent: *{sent}*\n"
        f"❌ Failed: *{failed}*\n"
        f"👥 Total: *{total}*",
        parse_mode="Markdown"
    )


# =============================================================================
# USER COMMANDS
# =============================================================================

@rate_limited
async def premium_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/premium — show premium plans and UPI payment info."""
    user_id = update.effective_user.id

    # Check if already premium
    if db.is_premium(user_id):
        user = db.get_user(user_id)
        expiry = user.get("premium_expiry")
        expiry_str = expiry.strftime('%Y-%m-%d %H:%M UTC') if expiry else "Unknown"
        await update.message.reply_text(
            f"💎 *You're already Premium!*\n\n"
            f"⏳ Your access expires: `{expiry_str}`\n\n"
            "Enjoy unlimited file access! 🚀",
            parse_mode="Markdown"
        )
        return

    # Build plans keyboard
    keyboard = []
    for plan_key, plan in Config.PREMIUM_PLANS.items():
        keyboard.append([
            InlineKeyboardButton(
                f"💎 {plan['label']}",
                callback_data=f"buy_plan_{plan_key}"
            )
        ])
    keyboard.append([InlineKeyboardButton("❌ Cancel", callback_data="cancel")])
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "💎 *Premium Plans*\n\n"
        "Get unlimited access with no verification steps!\n\n"
        "📌 *Benefits:*\n"
        "• ⚡ Skip all verification\n"
        "• 🔓 Instant file access\n"
        "• 📂 Unlimited downloads\n"
        "• 🎯 Priority support\n\n"
        "👇 Choose a plan:",
        parse_mode="Markdown",
        reply_markup=reply_markup
    )


@rate_limited
async def referral_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/referral — show user's referral link and stats."""
    user_id = update.effective_user.id
    ref_link = f"https://t.me/{Config.BOT_USERNAME}?start=ref_{user_id}"
    count = db.get_referral_count(user_id)

    await update.message.reply_text(
        "👥 *Your Referral Program*\n\n"
        f"🔗 *Your Link:*\n`{ref_link}`\n\n"
        f"👤 *Total Referrals:* {count}\n\n"
        "🎁 *Reward:* Each referral gives you "
        f"*+{Config.REFERRAL_REWARD_HOURS}h* of bonus access!\n\n"
        "💡 Share your link and earn free access every time someone joins!",
        parse_mode="Markdown",
        disable_web_page_preview=True
    )


@rate_limited
async def myaccount_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """/myaccount — show user's account status."""
    user_id = update.effective_user.id
    user = db.get_user(user_id)

    if not user:
        await update.message.reply_text("You don't have an account yet. Send /start to register.")
        return

    premium = db.is_premium(user_id)
    token_valid = db.is_token_valid(user_id)
    ref_count = user.get("referrals_count", 0)

    premium_expiry = user.get("premium_expiry")
    premium_str = premium_expiry.strftime('%Y-%m-%d') if premium_expiry and premium else "Not Active"

    token_expiry = user.get("token_expiry")
    token_str = token_expiry.strftime('%H:%M UTC') if token_expiry and token_valid else "Expired"

    joined = user.get("joined_at")
    joined_str = joined.strftime('%Y-%m-%d') if joined else "Unknown"

    await update.message.reply_text(
        f"👤 *My Account*\n\n"
        f"🆔 User ID: `{user_id}`\n"
        f"📅 Joined: `{joined_str}`\n\n"
        f"💎 Premium: {'✅ Active until ' + premium_str if premium else '❌ Not Active'}\n"
        f"🔑 Token: {'✅ Valid till ' + token_str if token_valid else '❌ Expired'}\n"
        f"👥 Referrals: *{ref_count}*",
        parse_mode="Markdown"
    )


# =============================================================================
# CALLBACK QUERY HANDLER (Inline Buttons)
# =============================================================================
async def callback_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle all inline keyboard button presses."""
    query = update.callback_query
    await query.answer()

    user_id = query.from_user.id
    data = query.data

    # ── PREMIUM INFO ──────────────────────────────────────────────────────────
    if data == "premium_info":
        keyboard = []
        for plan_key, plan in Config.PREMIUM_PLANS.items():
            keyboard.append([
                InlineKeyboardButton(f"💎 {plan['label']}", callback_data=f"buy_plan_{plan_key}")
            ])
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="main_menu")])
        await query.edit_message_text(
            "💎 *Premium Plans*\n\n"
            "Choose a plan to unlock unlimited access:\n",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── BUY PLAN ─────────────────────────────────────────────────────────────
    elif data.startswith("buy_plan_"):
        plan_key = data[9:]
        plan = Config.PREMIUM_PLANS.get(plan_key)
        if not plan:
            await query.edit_message_text("❌ Invalid plan selected.")
            return

        context.user_data["pending_plan"] = plan_key

        keyboard = [
            [InlineKeyboardButton("✅ I've Paid — Submit UTR", callback_data=f"submit_utr_{plan_key}")],
            [InlineKeyboardButton("🔙 Back", callback_data="premium_info")],
        ]

        await query.edit_message_text(
            f"💳 *Payment Instructions*\n\n"
            f"📦 Plan: *{plan['label']}*\n"
            f"💰 Amount: *₹{plan['price']}*\n\n"
            f"🏦 *UPI ID:* `{Config.UPI_ID}`\n"
            f"👤 *Pay to:* {Config.UPI_NAME}\n\n"
            f"📌 *Steps:*\n"
            f"1️⃣ Pay ₹{plan['price']} to the UPI ID above\n"
            f"2️⃣ Note your 12-digit UTR/Reference number\n"
            f"3️⃣ Click the button below to submit it\n\n"
            f"⚠️ _Approval is manual — usually within a few hours._",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── SUBMIT UTR ────────────────────────────────────────────────────────────
    elif data.startswith("submit_utr_"):
        plan_key = data[11:]
        context.user_data["awaiting_utr"] = plan_key
        await query.edit_message_text(
            "📝 *Enter your UTR Number*\n\n"
            "Please type your 12-digit UTR/Reference number from the payment.\n\n"
            "💡 _Example: 123456789012_",
            parse_mode="Markdown"
        )

    # ── REFERRAL ──────────────────────────────────────────────────────────────
    elif data == "referral":
        ref_link = f"https://t.me/{Config.BOT_USERNAME}?start=ref_{user_id}"
        count = db.get_referral_count(user_id)
        await query.edit_message_text(
            "👥 *Referral Program*\n\n"
            f"🔗 Your Link:\n`{ref_link}`\n\n"
            f"👤 Referrals: *{count}*\n"
            f"🎁 Reward: +{Config.REFERRAL_REWARD_HOURS}h per referral\n\n"
            "Share and earn free access!",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ]),
            disable_web_page_preview=True
        )

    # ── HELP ──────────────────────────────────────────────────────────────────
    elif data == "help":
        await query.edit_message_text(
            "ℹ️ *Help & Commands*\n\n"
            "/start — Start the bot\n"
            "/premium — View & buy premium plans\n"
            "/referral — Get your referral link\n"
            "/myaccount — View your account status\n\n"
            "📌 *How it works:*\n"
            "1. Click a file share link\n"
            "2. Complete verification (once per 24h)\n"
            "3. Get your file instantly!\n\n"
            "💎 Premium users skip verification entirely.",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ])
        )

    # ── MAIN MENU ─────────────────────────────────────────────────────────────
    elif data == "main_menu":
        keyboard = [
            [InlineKeyboardButton("📂 My Files", callback_data="my_files"),
             InlineKeyboardButton("💎 Get Premium", callback_data="premium_info")],
            [InlineKeyboardButton("👥 Referral", callback_data="referral"),
             InlineKeyboardButton("ℹ️ Help", callback_data="help")],
        ]
        await query.edit_message_text(
            "👋 *Main Menu*\n\nChoose an option:",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    # ── MY FILES ──────────────────────────────────────────────────────────────
    elif data == "my_files":
        await query.edit_message_text(
            "📂 *File Access*\n\n"
            "To access files, click a share link provided to you.\n\n"
            "Example link format:\n"
            f"`https://t.me/{Config.BOT_USERNAME}?start=file_XXXXXXXX`\n\n"
            "💡 _Links are shared by admins or content creators._",
            parse_mode="Markdown",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔙 Back", callback_data="main_menu")]
            ]),
            disable_web_page_preview=True
        )

    # ── CANCEL ────────────────────────────────────────────────────────────────
    elif data == "cancel":
        await query.edit_message_text("✅ Cancelled.")


# =============================================================================
# TEXT MESSAGE HANDLER (UTR Collection)
# =============================================================================
async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handle plain text messages — primarily for UTR collection."""
    user_id = update.effective_user.id
    text = update.message.text.strip()

    # ── UTR COLLECTION ────────────────────────────────────────────────────────
    if context.user_data.get("awaiting_utr"):
        plan_key = context.user_data.pop("awaiting_utr")
        plan = Config.PREMIUM_PLANS.get(plan_key)

        if not plan:
            await update.message.reply_text("❌ Invalid plan. Please try again with /premium")
            return

        # Validate UTR: 8-24 alphanumeric characters
        if not (8 <= len(text) <= 24 and text.isalnum()):
            await update.message.reply_text(
                "⚠️ *Invalid UTR format!*\n\n"
                "UTR should be 8-24 alphanumeric characters.\n"
                "Please check and try again.",
                parse_mode="Markdown"
            )
            context.user_data["awaiting_utr"] = plan_key  # Re-ask
            return

        # Check for duplicate UTR
        created = db.create_payment(user_id, text, plan["days"], plan["price"])
        if not created:
            await update.message.reply_text(
                "⚠️ This UTR has already been submitted.\n"
                "Please contact support if this is an error.",
                parse_mode="Markdown"
            )
            return

        # Notify admins
        for admin_id in Config.ADMIN_IDS:
            try:
                await context.bot.send_message(
                    chat_id=admin_id,
                    text=(
                        f"💰 *New Payment Submitted*\n\n"
                        f"👤 User ID: `{user_id}`\n"
                        f"💳 UTR: `{text}`\n"
                        f"📦 Plan: {plan['label']}\n"
                        f"💰 Amount: ₹{plan['price']}\n\n"
                        f"✅ Approve: `/approve {user_id}`\n"
                        f"❌ Reject: `/reject {user_id}`"
                    ),
                    parse_mode="Markdown"
                )
            except TelegramError:
                pass

        await update.message.reply_text(
            "📩 *Payment Received!*\n\n"
            f"💳 UTR: `{text}`\n"
            f"📦 Plan: {plan['label']}\n\n"
            "⏳ Waiting for approval — usually within a few hours.\n"
            "You'll be notified once approved! 🔔",
            parse_mode="Markdown"
        )
        return

    # ── FILE UPLOAD FROM ADMIN (not file object — ignore text) ───────────────
    if user_id in Config.ADMIN_IDS:
        # Admin sent text — could be a command, just ignore non-command text
        pass


# =============================================================================
# ERROR HANDLER
# =============================================================================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Log errors and notify admin."""
    logger.error(f"Update {update} caused error: {context.error}", exc_info=True)

    # Notify first admin
    if Config.ADMIN_IDS:
        try:
            await context.bot.send_message(
                chat_id=Config.ADMIN_IDS[0],
                text=f"⚠️ *Bot Error:*\n`{str(context.error)[:500]}`",
                parse_mode="Markdown"
            )
        except Exception:
            pass


# =============================================================================
# FLASK APP (Webhook Server)
# =============================================================================
flask_app = Flask(__name__)
application: Application = None  # Will be set in main()

@flask_app.route("/")
def index():
    return "🤖 Telegram File Sharing Bot is running!", 200

@flask_app.route(f"/webhook", methods=["POST"])
def webhook():
    """Receive updates from Telegram via webhook."""
    if application is None:
        return "Not ready", 503
    data = flask_request.get_json(force=True)
    asyncio.run_coroutine_threadsafe(
        application.update_queue.put(
            Update.de_json(data, application.bot)
        ),
        application.bot_data.get("loop")
    )
    return "OK", 200


# =============================================================================
# BOT SETUP & MAIN
# =============================================================================
async def setup_bot_commands(app: Application) -> None:
    """Register bot commands in Telegram menu."""
    commands = [
        BotCommand("start", "Start the bot"),
        BotCommand("premium", "View & buy premium plans"),
        BotCommand("referral", "Get your referral link"),
        BotCommand("myaccount", "View your account status"),
        BotCommand("stats", "Admin: Bot statistics"),
        BotCommand("files", "Admin: List uploaded files"),
        BotCommand("addpremium", "Admin: Grant premium"),
        BotCommand("removepremium", "Admin: Revoke premium"),
        BotCommand("approve", "Admin: Approve payment"),
        BotCommand("reject", "Admin: Reject payment"),
        BotCommand("broadcast", "Admin: Broadcast message"),
    ]
    await app.bot.set_my_commands(commands)


def build_application() -> Application:
    """Build and configure the telegram application."""
    app = (
        Application.builder()
        .token(Config.BOT_TOKEN)
        .build()
    )

    # ── Register Handlers ────────────────────────────────────────────────────
    # /start (covers all deep links)
    app.add_handler(CommandHandler("start", start_handler))

    # User Commands
    app.add_handler(CommandHandler("premium", premium_handler))
    app.add_handler(CommandHandler("referral", referral_handler))
    app.add_handler(CommandHandler("myaccount", myaccount_handler))

    # Admin Commands
    app.add_handler(CommandHandler("stats", stats_handler))
    app.add_handler(CommandHandler("files", files_handler))
    app.add_handler(CommandHandler("addpremium", add_premium_handler))
    app.add_handler(CommandHandler("removepremium", remove_premium_handler))
    app.add_handler(CommandHandler("approve", approve_handler))
    app.add_handler(CommandHandler("reject", reject_handler))
    app.add_handler(CommandHandler("broadcast", broadcast_handler))

    # File Uploads (admin only — all file types)
    app.add_handler(MessageHandler(
        filters.Document.ALL | filters.VIDEO | filters.AUDIO | filters.PHOTO,
        handle_file_upload
    ))

    # Text messages (UTR collection)
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_message_handler))

    # Callback queries (inline buttons)
    app.add_handler(CallbackQueryHandler(callback_handler))

    # Error handler
    app.add_error_handler(error_handler)

    return app


async def run_webhook_mode(app: Application) -> None:
    """Configure and start webhook."""
    webhook_path = "/webhook"
    webhook_full_url = f"{Config.WEBHOOK_URL.rstrip('/')}{webhook_path}"

    await app.initialize()
    await setup_bot_commands(app)

    # Set webhook with Telegram
    await app.bot.set_webhook(
        url=webhook_full_url,
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )
    logger.info(f"✅ Webhook set to: {webhook_full_url}")

    await app.start()
    logger.info("🤖 Bot started in webhook mode!")

    # Keep running
    await asyncio.Event().wait()


def main() -> None:
    """Entry point."""
    global application

    logger.info("🚀 Starting Telegram File Sharing Bot...")
    logger.info(f"👑 Admin IDs: {Config.ADMIN_IDS}")
    logger.info(f"🤖 Bot Username: @{Config.BOT_USERNAME}")

    application = build_application()

    # Store event loop reference for webhook
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    application.bot_data["loop"] = loop

    # Start Flask in a separate thread
    flask_thread = threading.Thread(
        target=lambda: flask_app.run(host="0.0.0.0", port=Config.PORT, use_reloader=False),
        daemon=True
    )
    flask_thread.start()
    logger.info(f"🌐 Flask server started on port {Config.PORT}")

    # Run bot webhook in main loop
    loop.run_until_complete(run_webhook_mode(application))


if __name__ == "__main__":
    main()
