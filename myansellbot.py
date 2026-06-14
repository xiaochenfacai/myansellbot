"""
PostBot MY — Myanmar (Burmese)
Admin → post → set price → publish to Group/Channel
Buyer → pay → proof + address → admin review
"""

import logging
import asyncio
import json
import os
import re
import socket
import sqlite3
import sys
import threading
import time
from datetime import datetime, timedelta

from flask import Flask, request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto, InputMediaVideo, Update
from telegram.error import Conflict, Forbidden
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    ChatMemberHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# ---------------------------------------------------------------------------
# config
# ---------------------------------------------------------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("postbot_my")

TOKEN = os.environ.get("POSTBOT_TOKEN", "8655662511:AAGM5EA_-EzKTD87oypmLhbbPkNp1jHHBwI")
MASTER_ID = int(os.environ.get("POSTBOT_MASTER", "8807178282"))
PORT = int(os.environ.get("PORT", 8080))
DB_PATH = os.environ.get("POSTBOT_DB", "postbot_my_data.db")
ADMIN_WEB_KEY = os.environ.get("ADMIN_WEB_KEY", "postbot_my2024")
WEB_BASE_URL = os.environ.get("WEB_BASE_URL", "https://myansell-888gg.onrender.com")

# payment settings
DEFAULT_PAY = {
    "usdt": os.environ.get("USDT_ADDRESS", "USDT လိပ်စာ ထည့်သွင်းပါ"),
    "kpay": os.environ.get("KPAY_PHONE", "KPay ဖုန်းနံပါတ် ထည့်ပါ"),
    "wavepay": os.environ.get("WAVEPAY_PHONE", "WavePay ဖုန်းနံပါတ် ထည့်ပါ"),
    "admin_username": os.environ.get("ADMIN_USERNAME", "Admin username ထည့်ပါ"),
    "usdt_rate": os.environ.get("USDT_RATE", "4200"),
}

flask_app = Flask(__name__)

LOCK_KEY = "postbot_my_polling"
LOCK_STALE_SEC = 90

# multi-image debounce
_content_tasks: dict[int, asyncio.Task] = {}


# ---------------------------------------------------------------------------
# database
# ---------------------------------------------------------------------------
def init_db():
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """CREATE TABLE IF NOT EXISTS targets (
                chat_id INTEGER PRIMARY KEY, title TEXT, chat_type TEXT, added_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS prefs (
                user_id INTEGER PRIMARY KEY, default_target INTEGER
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY, value TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS listings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                media_type TEXT, file_id TEXT, caption TEXT,
                price1 REAL, price2 REAL, price3 REAL,
                created_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                listing_id INTEGER, buyer_id INTEGER, buyer_name TEXT,
                qty INTEGER, price REAL, payment_method TEXT,
                proof_file_id TEXT, address TEXT, status TEXT,
                created_at TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS buyer_sessions (
                user_id INTEGER PRIMARY KEY, order_id INTEGER, step TEXT
            )"""
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS admin_drafts (
                user_id INTEGER PRIMARY KEY,
                step TEXT, media_type TEXT, file_id TEXT, caption TEXT,
                price1 REAL, price2 REAL, price3 REAL, no_price INTEGER DEFAULT 0
            )"""
        )
        for k, v in DEFAULT_PAY.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (k, v)
            )
        _migrate_db(conn)


def _migrate_db(conn):
    listing_cols = {r[1] for r in conn.execute("PRAGMA table_info(listings)").fetchall()}
    if "price_mode" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN price_mode TEXT DEFAULT 'qty'")
    if "prices_json" not in listing_cols:
        conn.execute("ALTER TABLE listings ADD COLUMN prices_json TEXT")
    draft_cols = {r[1] for r in conn.execute("PRAGMA table_info(admin_drafts)").fetchall()}
    if "price_mode" not in draft_cols:
        conn.execute("ALTER TABLE admin_drafts ADD COLUMN price_mode TEXT DEFAULT 'qty'")
    if "prices_json" not in draft_cols:
        conn.execute("ALTER TABLE admin_drafts ADD COLUMN prices_json TEXT")
    if "selected_targets" not in draft_cols:
        conn.execute("ALTER TABLE admin_drafts ADD COLUMN selected_targets TEXT")
    order_cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)").fetchall()}
    for col, typedef in [
        ("item_label", "TEXT"), ("source_chat_id", "INTEGER"), ("source_message_id", "INTEGER"),
        ("post_link", "TEXT"), ("buyer_phone", "TEXT"),
    ]:
        if col not in order_cols:
            conn.execute(f"ALTER TABLE orders ADD COLUMN {col} {typedef}")
    session_cols = {r[1] for r in conn.execute("PRAGMA table_info(buyer_sessions)").fetchall()}
    if "extra_json" not in session_cols:
        conn.execute("ALTER TABLE buyer_sessions ADD COLUMN extra_json TEXT")


def db_get_draft(user_id: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT step, media_type, file_id, caption, price1, price2, price3, no_price, "
            "price_mode, prices_json, selected_targets FROM admin_drafts WHERE user_id=?", (user_id,),
        ).fetchone()
    if not row:
        return None
    draft: dict = {"step": row[0]}
    if row[1]:
        draft["media_type"] = row[1]
    if row[2]:
        draft["file_id"] = row[2]
    if row[3]:
        draft["caption"] = row[3]
    if row[4] is not None:
        draft["price1"] = row[4]
    if row[5] is not None:
        draft["price2"] = row[5]
    if row[6] is not None:
        draft["price3"] = row[6]
    draft["no_price"] = bool(row[7])
    if len(row) > 8 and row[8]:
        draft["price_mode"] = row[8]
    if len(row) > 9 and row[9]:
        draft["prices_json"] = row[9]
        try:
            draft["work_prices"] = json.loads(row[9])
        except json.JSONDecodeError:
            draft["work_prices"] = []
    if len(row) > 10 and row[10]:
        try:
            draft["selected_targets"] = json.loads(row[10])
        except json.JSONDecodeError:
            draft["selected_targets"] = []
    if row[1] == "collecting" and row[2]:
        try:
            draft["items"] = json.loads(row[2])
        except json.JSONDecodeError:
            draft["items"] = []
    return draft


def db_save_draft(user_id: int, draft: dict):
    media_type = draft.get("media_type")
    file_id = draft.get("file_id")
    if "items" in draft:
        media_type = "collecting"
        file_id = json.dumps(draft["items"])
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """INSERT OR REPLACE INTO admin_drafts
               (user_id, step, media_type, file_id, caption, price1, price2, price3, no_price,
                price_mode, prices_json, selected_targets)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id, draft.get("step"), media_type, file_id,
                draft.get("caption"), draft.get("price1"), draft.get("price2"), draft.get("price3"),
                1 if draft.get("no_price") else 0,
                draft.get("price_mode", "qty"),
                draft.get("prices_json") or (
                    json.dumps(draft["work_prices"]) if draft.get("work_prices") else None
                ),
                json.dumps(draft["selected_targets"]) if draft.get("selected_targets") else None,
            ),
        )


def db_clear_draft(user_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM admin_drafts WHERE user_id=?", (user_id,))


def db_get_setting(key: str) -> str:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else DEFAULT_PAY.get(key, "")


def db_set_setting(key: str, value: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value)
        )


def db_add_target(chat_id: int, title: str, chat_type: str):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO targets VALUES (?, ?, ?, ?)",
            (chat_id, title, chat_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        )


def db_remove_target(chat_id: int):
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM targets WHERE chat_id=?", (chat_id,))


def db_list_targets() -> list[dict]:
    with sqlite3.connect(DB_PATH) as conn:
        rows = conn.execute(
            "SELECT chat_id, title, chat_type FROM targets ORDER BY added_at"
        ).fetchall()
    return [{"id": r[0], "title": r[1], "type": r[2]} for r in rows]


def db_set_default(user_id: int, chat_id: int | None):
    with sqlite3.connect(DB_PATH) as conn:
        if chat_id is None:
            conn.execute("DELETE FROM prefs WHERE user_id=?", (user_id,))
        else:
            conn.execute("INSERT OR REPLACE INTO prefs VALUES (?, ?)", (user_id, chat_id))


def db_get_default(user_id: int) -> int | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT default_target FROM prefs WHERE user_id=?", (user_id,)).fetchone()
    return row[0] if row else None


def db_create_listing(media_type, file_id, caption, p1, p2, p3,
                      price_mode="qty", prices_json=None) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            "INSERT INTO listings (media_type,file_id,caption,price1,price2,price3,created_at,"
            "price_mode,prices_json) VALUES (?,?,?,?,?,?,?,?,?)",
            (media_type, file_id, caption or "", p1, p2, p3,
             datetime.now().strftime("%Y-%m-%d %H:%M:%S"), price_mode, prices_json),
        )
        return cur.lastrowid


def db_get_listing(lid: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT * FROM listings WHERE id=?", (lid,)).fetchone()
        if not row:
            return None
        cols = [c[1] for c in conn.execute("PRAGMA table_info(listings)").fetchall()]
        listing = dict(zip(cols, row))
        if listing.get("prices_json"):
            try:
                listing["work_prices"] = json.loads(listing["prices_json"])
            except json.JSONDecodeError:
                listing["work_prices"] = []
        return listing


def db_create_order(listing_id, buyer_id, buyer_name, qty, price, **extra) -> int:
    fields = {
        "listing_id": listing_id, "buyer_id": buyer_id, "buyer_name": buyer_name,
        "qty": qty, "price": price, "status": "pending_pay",
        "created_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }
    fields.update(extra)
    cols = ", ".join(fields.keys())
    placeholders = ", ".join("?" * len(fields))
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.execute(
            f"INSERT INTO orders ({cols}) VALUES ({placeholders})",
            list(fields.values()),
        )
        return cur.lastrowid


def db_get_order(oid: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute("SELECT * FROM orders WHERE id=?", (oid,)).fetchone()
        if not row:
            return None
        cols = [c[1] for c in conn.execute("PRAGMA table_info(orders)").fetchall()]
        return dict(zip(cols, row))


def db_list_orders(date: str | None = None, status: str | None = None) -> list[dict]:
    sql = "SELECT * FROM orders WHERE 1=1"
    params: list = []
    if date:
        sql += " AND created_at LIKE ?"
        params.append(f"{date}%")
    if status:
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY id DESC"
    with sqlite3.connect(DB_PATH) as conn:
        cols = [c[1] for c in conn.execute("PRAGMA table_info(orders)").fetchall()]
        rows = conn.execute(sql, params).fetchall()
    return [dict(zip(cols, r)) for r in rows]


def db_daily_stats(date: str) -> dict:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            """SELECT COUNT(*), COALESCE(SUM(price), 0)
               FROM orders WHERE created_at LIKE ? AND status='success'""",
            (f"{date}%",),
        ).fetchone()
    return {"count": row[0], "total": row[1] or 0}


def db_update_order(oid: int, **fields):
    if not fields:
        return
    sets = ", ".join(f"{k}=?" for k in fields)
    vals = list(fields.values()) + [oid]
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(f"UPDATE orders SET {sets} WHERE id=?", vals)


def db_set_buyer_session(user_id: int, order_id: int | None, step: str | None, extra_json: str | None = None):
    with sqlite3.connect(DB_PATH) as conn:
        if step is None:
            conn.execute("DELETE FROM buyer_sessions WHERE user_id=?", (user_id,))
        else:
            conn.execute(
                "INSERT OR REPLACE INTO buyer_sessions (user_id, order_id, step, extra_json) VALUES (?,?,?,?)",
                (user_id, order_id or 0, step, extra_json),
            )


def db_get_buyer_session(user_id: int) -> dict | None:
    with sqlite3.connect(DB_PATH) as conn:
        row = conn.execute(
            "SELECT order_id, step, extra_json FROM buyer_sessions WHERE user_id=?", (user_id,),
        ).fetchone()
    if not row:
        return None
    sess = {"order_id": row[0], "step": row[1]}
    if row[2]:
        try:
            sess["extra"] = json.loads(row[2])
        except json.JSONDecodeError:
            sess["extra"] = {}
    return sess


# ---------------------------------------------------------------------------
# utils
# ---------------------------------------------------------------------------
def is_master(user_id: int | None) -> bool:
    if user_id is None:
        return False
    return int(user_id) == int(MASTER_ID)


async def reply(update: Update, text: str, **kwargs):
    msg = update.effective_message
    if msg:
        return await msg.reply_text(text, **kwargs)


def forward_chat(message):
    origin = getattr(message, "forward_origin", None)
    if origin and getattr(origin, "chat", None):
        return origin.chat
    return getattr(message, "forward_from_chat", None)


def parse_prices(text: str) -> tuple[float, float, float] | None:
    nums = parse_price_list(text, 3)
    if nums and len(nums) >= 3:
        return nums[0], nums[1], nums[2]
    return None


def parse_price_list(text: str, count: int | None = None) -> list[float] | None:
    nums = [float(x) for x in re.findall(r"\d+(?:\.\d+)?", text.replace(",", " "))]
    if count is not None:
        return nums if len(nums) == count else None
    return nums if nums else None


def listing_qty_price(listing: dict, qty: int) -> float | None:
    if listing.get("price_mode") == "works":
        prices = listing.get("work_prices") or []
        if not prices and listing.get("prices_json"):
            try:
                prices = json.loads(listing["prices_json"])
            except json.JSONDecodeError:
                prices = []
        if 1 <= qty <= len(prices):
            return prices[qty - 1]
        return None
    return {1: listing["price1"], 2: listing["price2"], 3: listing["price3"]}.get(qty)


def draft_work_count(draft: dict) -> int:
    if draft.get("media_type") == "album":
        try:
            return len(json.loads(draft.get("file_id") or "[]"))
        except json.JSONDecodeError:
            return 0
    return 1


def extract_media(message):
    if message.photo:
        return "photo", message.photo[-1].file_id
    if message.video:
        return "video", message.video.file_id
    if message.animation:
        return "animation", message.animation.file_id
    if message.document:
        mime = message.document.mime_type or ""
        if mime.startswith("image/"):
            return "photo", message.document.file_id
        if mime.startswith("video/"):
            return "video", message.document.file_id
    return None, None


def price_buttons(listing_id: int, p1: float, p2: float, p3: float) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"🛒 ၁ ခု — {p1:g}", callback_data=f"buy:{listing_id}:1")],
        [InlineKeyboardButton(f"🛒 ၂ ခု — {p2:g}", callback_data=f"buy:{listing_id}:2")],
        [InlineKeyboardButton(f"🛒 ၃ ခု — {p3:g}", callback_data=f"buy:{listing_id}:3")],
    ])


def work_buttons(listing_id: int, prices: list[float]) -> InlineKeyboardMarkup:
    rows = []
    for i, p in enumerate(prices, 1):
        rows.append([
            InlineKeyboardButton(
                f"🛒 {i} လက်ရာ — {p:g}",
                callback_data=f"buy:{listing_id}:{i}",
            )
        ])
    return InlineKeyboardMarkup(rows)


def listing_keyboard(listing: dict) -> InlineKeyboardMarkup | None:
    if listing.get("price_mode") == "works":
        prices = listing.get("work_prices") or []
        if not prices and listing.get("prices_json"):
            try:
                prices = json.loads(listing["prices_json"])
            except json.JSONDecodeError:
                prices = []
        if prices:
            return work_buttons(listing["id"], prices)
        return None
    return price_buttons(listing["id"], listing["price1"], listing["price2"], listing["price3"])


def pay_buttons(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💎 USDT", callback_data=f"pay:{order_id}:usdt")],
        [InlineKeyboardButton("📱 KPay", callback_data=f"pay:{order_id}:kpay")],
        [InlineKeyboardButton("📱 WavePay", callback_data=f"pay:{order_id}:wavepay")],
    ])


def review_buttons(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ ဝယ်ယူမှု အောင်မြင်", callback_data=f"review:{order_id}:ok"),
            InlineKeyboardButton("❌ ဝယ်ယူမှု မအောင်မြင်", callback_data=f"review:{order_id}:fail"),
        ]
    ])


def target_label(t: dict) -> str:
    kind = "Channel" if t["type"] == "channel" else "Group"
    return f"{t['title']} ({kind})"


def build_target_keyboard(targets: list[dict], prefix: str) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(target_label(t), callback_data=f"{prefix}:{t['id']}")] for t in targets]
    rows.append([InlineKeyboardButton("❌ ပယ်ဖျက်", callback_data=f"{prefix}:cancel")])
    return InlineKeyboardMarkup(rows)


def draft_selected_set(draft: dict) -> set[int]:
    return set(draft.get("selected_targets") or [])


def build_multi_target_keyboard(targets: list[dict], selected: set[int]) -> InlineKeyboardMarkup:
    rows = []
    for t in targets:
        mark = "✅" if t["id"] in selected else "⬜"
        rows.append([InlineKeyboardButton(
            f"{mark} {target_label(t)}", callback_data=f"ptog:{t['id']}",
        )])
    n = len(selected)
    rows.append([
        InlineKeyboardButton("☑️ အားလုံး", callback_data="ptog:all"),
        InlineKeyboardButton(f"🚀 ထုတ်ဝေမည်({n})", callback_data="pubgo:0"),
    ])
    rows.append([InlineKeyboardButton("❌ ပယ်ဖျက်", callback_data="pick:cancel")])
    return InlineKeyboardMarkup(rows)


def build_message_link(chat_id: int, message_id: int) -> str:
    cid = str(chat_id)
    if cid.startswith("-100"):
        return f"https://t.me/c/{cid[4:]}/{message_id}"
    return ""


def order_item_label(listing: dict | None, qty: int) -> str:
    if listing and listing.get("price_mode") == "works":
        return f"{qty} လက်ရာ"
    return f"{qty} ခု"


def listing_work_media(listing: dict, qty: int) -> tuple[str, str] | None:
    """preview media (media_type, file_id)"""
    mt = listing.get("media_type")
    fid = listing.get("file_id") or ""
    if mt == "album":
        try:
            items = json.loads(fid)
            idx = max(0, min(qty - 1, len(items) - 1))
            item = items[idx]
            return item["type"], item["file_id"]
        except (json.JSONDecodeError, IndexError, KeyError):
            return None
    if mt in ("photo", "video", "animation") and fid:
        return mt, fid
    if mt == "text":
        return "text", fid
    return None


async def prompt_pick_targets(update_or_query, draft: dict, price_summary: str = ""):
    targets = db_list_targets()
    text = "Group/Channel ရွေးပါ (များစွာ ရွေးနိုင်) —"
    if price_summary:
        text = f"ဈေးနှုန်း — {price_summary}\n\n{text}"
    kb = build_multi_target_keyboard(targets, draft_selected_set(draft))
    if hasattr(update_or_query, "edit_message_text"):
        await update_or_query.edit_message_text(text, reply_markup=kb)
    else:
        await update_or_query.message.reply_text(text, reply_markup=kb)


def get_usdt_rate() -> float:
    try:
        return float(db_get_setting("usdt_rate") or "4200")
    except ValueError:
        return 4200.0


def mmk_to_usdt(mmk: float) -> float:
    return round(mmk / get_usdt_rate(), 2)


def format_mmk(price: float) -> str:
    if price == int(price):
        return f"{int(price):,}"
    return f"{price:g}"


def format_pay_block(method: str, mmk_price: float) -> str:
    mmk_str = format_mmk(mmk_price)
    if method == "usdt":
        rate = get_usdt_rate()
        usdt = mmk_to_usdt(mmk_price)
        rate_str = f"{int(rate)}" if rate == int(rate) else f"{rate:g}"
        return (
            f"မူလ — <b>{mmk_str}</b> ကျပ်\n"
            f"ပြောင်း — {mmk_str} ÷ {rate_str} = <b>{usdt:.2f} USDT</b>\n\n"
            f"💰 ပေးချေပါ <b>{usdt:.2f} USDT</b>\n"
            f"⚠️ ပမာဏ မှန်ကန်စွာ ပေးချေပါ！"
        )
    return f"ပေးရမည့်ပမာဏ — <b>{mmk_str}</b> ကျပ်"


def pay_info(method: str) -> str:
    if method == "usdt":
        return f"💎 <b>USDT (TRC20)</b>\n<code>{db_get_setting('usdt')}</code>"
    if method == "kpay":
        return f"📱 <b>KPay</b>\n<code>{db_get_setting('kpay')}</code>"
    if method == "wavepay":
        return f"📱 <b>WavePay</b>\n<code>{db_get_setting('wavepay')}</code>"
    return ""


async def verify_and_bind(update: Update, context: ContextTypes.DEFAULT_TYPE,
                          chat_id: int, title: str, chat_type: str):
    try:
        me = await context.bot.get_me()
        member = await context.bot.get_chat_member(chat_id, me.id)
        if member.status not in ("administrator", "creator"):
            await reply(update, "❌ Bot ကို Admin အဖြစ် ထည့်ပါ။")
            return
        if chat_type == "channel":
            if not (getattr(member, "can_post_messages", False) or getattr(member, "can_edit_messages", False)):
                await reply(update, "❌ Channel တွင် message ပို့ permission လိုသည်။")
                return
        elif chat_type in ("group", "supergroup"):
            if getattr(member, "can_send_messages", True) is False:
                await reply(update, "❌ Group တွင် message ပို့ permission လိုသည်။")
                return
        db_add_target(chat_id, title, chat_type)
        await reply(update, f"✅ ချိတ်ဆက်ပြီး — {title}")
    except Exception as e:
        log.exception("bind error")
        await reply(update, f"❌ bind မအောင်မြင် — {e}")


async def send_listing_to_chat(context, chat_id: int, listing: dict) -> tuple[bool, int | None]:
    kb = listing_keyboard(listing)
    return await _send_media(
        context, chat_id, listing["media_type"], listing["file_id"],
        listing["caption"] or "🛍 ရွေးချယ်ထားသော လက်ရာများ", kb,
    )


async def send_draft_to_chat(context, chat_id: int, draft: dict) -> tuple[bool, int | None]:
    cap = draft.get("caption") or ""
    return await _send_media(context, chat_id, draft["media_type"], draft.get("file_id"), cap, None)


async def _send_media(context, chat_id, media_type, file_id, caption, reply_markup) -> tuple[bool, int | None]:
    msg_id = None
    try:
        if media_type == "text":
            msg = await context.bot.send_message(chat_id, caption or " ", reply_markup=reply_markup)
            msg_id = msg.message_id
        elif media_type == "album":
            items = json.loads(file_id)
            media = []
            for i, item in enumerate(items):
                cap = caption if i == 0 else None
                if item["type"] == "video":
                    media.append(InputMediaVideo(item["file_id"], caption=cap))
                else:
                    media.append(InputMediaPhoto(item["file_id"], caption=cap))
            await context.bot.send_media_group(chat_id, media)
            if reply_markup:
                msg = await context.bot.send_message(chat_id, "👇 ဝယ်ယူရန် နှိပ်ပါ", reply_markup=reply_markup)
                msg_id = msg.message_id
        elif media_type == "photo":
            msg = await context.bot.send_photo(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
            msg_id = msg.message_id
        elif media_type == "video":
            msg = await context.bot.send_video(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
            msg_id = msg.message_id
        else:
            msg = await context.bot.send_animation(chat_id, file_id, caption=caption or None, reply_markup=reply_markup)
            msg_id = msg.message_id
        return True, msg_id
    except Exception as e:
        log.error("publish error chat=%s err=%s", chat_id, e)
        return False, None


async def send_work_preview(context, chat_id: int, listing: dict, qty: int, caption: str,
                            reply_markup: InlineKeyboardMarkup | None = None):
    media = listing_work_media(listing, qty)
    if not media:
        await context.bot.send_message(chat_id, caption, parse_mode="HTML", reply_markup=reply_markup)
        return
    mt, fid = media
    if mt == "text":
        text = (listing.get("caption") or "") + "\n\n" + caption
        await context.bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=reply_markup)
    elif mt == "photo":
        await context.bot.send_photo(chat_id, fid, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
    elif mt == "video":
        await context.bot.send_video(chat_id, fid, caption=caption, parse_mode="HTML", reply_markup=reply_markup)
    else:
        await context.bot.send_animation(chat_id, fid, caption=caption, parse_mode="HTML", reply_markup=reply_markup)


def _get_draft_items(draft: dict) -> list:
    items = draft.get("items")
    if items is not None:
        return items
    if draft.get("media_type") == "collecting" and draft.get("file_id"):
        try:
            return json.loads(draft["file_id"])
        except json.JSONDecodeError:
            return []
    return []


async def _finalize_content(context, user_id: int, chat_id: int):
    _content_tasks.pop(user_id, None)
    draft = db_get_draft(user_id)
    if not draft or draft.get("step") != "await_content":
        return
    items = _get_draft_items(draft)
    if not items:
        return
    caption = draft.get("caption") or ""
    if len(items) == 1:
        draft.update({"media_type": items[0]["type"], "file_id": items[0]["file_id"], "price_mode": "qty"})
    else:
        draft.update({"media_type": "album", "file_id": json.dumps(items), "price_mode": "works"})
    draft["caption"] = caption
    draft["step"] = "await_prices"
    if "items" in draft:
        del draft["items"]
    db_save_draft(user_id, draft)
    n = len(items)
    if draft.get("price_mode") == "works":
        price_hint = (
            f"📸 လက်ရာ {n} ခု ရရှိပြီး！\n\n"
            f"လက်ရာ တစ်ခုချင်း ဈေးနှုန်း ({n} ခု၊ ကျပ်) — ဥပမာ —\n"
            f"<code>{', '.join(['400000'] * min(n, 3))}{'...' if n > 3 else ''}</code>\n\n"
            f"ခလုတ် — လက်ရာ ၁၊ ၂…\n"
            f"သို့မဟုတ် 「ဈေးမပါ」 —"
        )
    else:
        price_hint = (
            "📸 အကြောင်းအရာ ရရှိပြီး！\n\n"
            "ဈေးနှုန်း ၃ ခု (၁/၂/၃) — ဥပမာ —\n"
            "<code>400000, 750000, 1000000</code>\n\n"
            "သို့မဟုတ် 「ဈေးမပါ」 —"
        )
    await context.bot.send_message(
        chat_id, price_hint, parse_mode="HTML", reply_markup=price_prompt_keyboard(),
    )


async def _schedule_content_finalize(context, user_id: int, chat_id: int):
    old = _content_tasks.get(user_id)
    if old and not old.done():
        old.cancel()

    async def job():
        try:
            await asyncio.sleep(2.5)
            await _finalize_content(context, user_id, chat_id)
        except asyncio.CancelledError:
            pass

    _content_tasks[user_id] = asyncio.create_task(job())


def price_prompt_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("📝 ဈေးမပါ ထုတ်ဝေ", callback_data="post:noprice")],
        [InlineKeyboardButton("❌ ပယ်ဖျက်", callback_data="post:cancel")],
    ])


async def ask_prices(update: Update, draft: dict | None = None):
    if draft and draft.get("price_mode") == "works":
        n = draft_work_count(draft)
        text = (
            f"📸 လက်ရာ {n} ခု ရရှိပြီး！\n\n"
            f"လက်ရာ တစ်ခုချင်း ဈေးနှုန်း ({n} ခု) — ဥပမာ —\n"
            f"<code>{', '.join(['400000'] * min(n, 3))}</code>\n\n"
            f"သို့မဟုတ် 「ဈေးမပါ」 —"
        )
    else:
        text = (
            "📸 အကြောင်းအရာ ရရှိပြီး！\n\n"
            "ဈေးနှုန်း ၃ ခု (၁/၂/၃) — ဥပမာ —\n"
            "<code>400000, 750000, 1000000</code>\n\n"
            "သို့မဟုတ် 「ဈေးမပါ」 —"
        )
    await update.message.reply_text(text, parse_mode="HTML", reply_markup=price_prompt_keyboard())


async def finish_publish(context, uid: int, draft: dict) -> tuple[bool, str]:
    target_ids = draft.get("selected_targets") or []
    if not target_ids:
        return False, "❌ Group/Channel အနည်းဆုံး ၁ ခု"

    targets_map = {t["id"]: t for t in db_list_targets()}
    ok_count = 0
    fail_names = []

    if draft.get("no_price"):
        for tid in target_ids:
            ok, _ = await send_draft_to_chat(context, tid, draft)
            if ok:
                ok_count += 1
            else:
                fail_names.append(targets_map.get(tid, {}).get("title", str(tid)))
        db_clear_draft(uid)
        if ok_count == len(target_ids):
            return True, f"✅ ဈေးမပါ ထုတ်ဝေပြီး — {ok_count} နေရာ"
        return ok_count > 0, f"⚠️ ထုတ်ဝေ {ok_count}/{len(target_ids)} ခု\nမအောင်မြင် — {', '.join(fail_names)}"

    price_mode = draft.get("price_mode", "qty")
    if price_mode == "works":
        work_prices = draft.get("work_prices") or []
        if not work_prices and draft.get("prices_json"):
            try:
                work_prices = json.loads(draft["prices_json"])
            except json.JSONDecodeError:
                work_prices = []
        p1 = work_prices[0] if work_prices else 0
        p2 = work_prices[1] if len(work_prices) > 1 else 0
        p3 = work_prices[2] if len(work_prices) > 2 else 0
        prices_json = json.dumps(work_prices)
        lid = db_create_listing(
            draft["media_type"], draft.get("file_id", ""), draft.get("caption", ""),
            p1, p2, p3, price_mode="works", prices_json=prices_json,
        )
        price_str = " / ".join(f"{i}#{p:g}" for i, p in enumerate(work_prices, 1))
    else:
        lid = db_create_listing(
            draft["media_type"], draft.get("file_id", ""), draft.get("caption", ""),
            draft["price1"], draft["price2"], draft["price3"],
        )
        price_str = f"{draft['price1']}/{draft['price2']}/{draft['price3']}"

    listing = db_get_listing(lid)
    for tid in target_ids:
        ok, _ = await send_listing_to_chat(context, tid, listing)
        if ok:
            ok_count += 1
        else:
            fail_names.append(targets_map.get(tid, {}).get("title", str(tid)))

    db_clear_draft(uid)
    msg = (
        f"{'✅ ထုတ်ဝေပြီး' if ok_count == len(target_ids) else '⚠️ တစ်စိတ်တစ်ပိုင်း'}\n"
        f"အောင်မြင် — {ok_count}/{len(target_ids)} နေရာ\n"
        f"ပစ္စည်း ID — {lid}\n"
        f"ဈေးနှုန်း — {price_str}"
    )
    if fail_names:
        msg += f"\nမအောင်မြင် — {', '.join(fail_names)}"
    return ok_count > 0, msg


async def show_payment_menu(context, user_id: int, order_id: int):
    order = db_get_order(order_id)
    if not order:
        await context.bot.send_message(user_id, "အော်ဒါ မရှိ/သက်တမ်းကုန်။")
        return
    listing = db_get_listing(order["listing_id"])
    item_label = order.get("item_label") or order_item_label(listing, order["qty"])
    text = (
        f"🛍 <b>အော်ဒါ #{order_id}</b>\n"
        f"ပစ္စည်း — {item_label}\n"
        f"ပမာဏ — <b>{format_mmk(order['price'])}</b> ကျပ်\n"
        f"（USDT ÷{int(get_usdt_rate())}）\n\n"
        f"ငွေပေးချေနည်း ရွေးပါ —"
    )
    await context.bot.send_message(user_id, text, parse_mode="HTML", reply_markup=pay_buttons(order_id))


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------
HELP_ADMIN = (
    "📮 <b>ထုတ်ဝေ + ရောင်းချ Bot</b>\n\n"
    "<b>ထုတ်ဝေနည်း —</b>\n"
    "/post → ပုံ/ဗီဒီယို/စာသား → ဈေးနှုန်း သို့မဟုတ် 「ဈေးမပါ」\n"
    "（album ၁၀ ပုံအထိ）\n\n"
    "<b>ရောင်းချ —</b>\n"
    "ပုံ ၁ ပုံ — ၁/၂/၃ ခု ဈေးနှုန်း\n"
    "ပုံများစွာ — လက်ရာ တစ်ခုချင်း ဈေးနှုန်း\n\n"
    "<b>Commands —</b>\n"
    "/post — ထုတ်ဝေမည်\n"
    "/done — ပုံများ ပြီးပြီ\n"
    "/setpay — ငွေလက်ခံအချက်အလက်\n"
    "/targets — bind Group/Channel\n"
    "/default — default ထုတ်ဝေရာ\n"
    "/bind /unbind /ping /id\n"
    "/orders — Web အော်ဒါ Admin"
)

HELP_BUYER = (
    "👋 မင်္ဂလာပါ！\n\n"
    "Channel/Group ထဲက ဝယ်ယူ ခလုတ်မှ ဝယ်ယူပါ။\n"
    "မေးခွန်းရှိရင် ဆက်သွယ်ပါ @{admin}"
)

HELP_GROUP = "📮 Bot အဆင်သင့်\nGroup ID — <code>{cid}</code>\nAdmin — /bind ဖြင့် bind"


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not user:
        return

    if chat.type == "private":
        # deep link /start buy_LISTING_QTY
        if context.args and context.args[0].startswith("buy_"):
            parts = context.args[0].split("_")
            if len(parts) == 3:
                await start_buy_flow(context, user, int(parts[1]), int(parts[2]))
                return
        if context.args and context.args[0].startswith("pay_"):
            order_id = int(context.args[0].split("_")[1])
            await show_payment_menu(context, user.id, order_id)
            return

        if is_master(user.id):
            await reply(update, HELP_ADMIN, parse_mode="HTML")
        else:
            admin = db_get_setting("admin_username").lstrip("@")
            await reply(update, HELP_BUYER.format(admin=admin))
    else:
        await reply(update, HELP_GROUP.format(cid=chat.id), parse_mode="HTML")


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await cmd_start(update, context)


async def cmd_ping(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await reply(update, f"✅ Online\nID：<code>{update.effective_chat.id}</code>", parse_mode="HTML")


async def cmd_id(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    ok = is_master(user.id if user else None)
    await reply(
        update,
        f"သင့် ID — <code>{user.id if user else '?'}</code>\n"
        f"Admin — <code>{MASTER_ID}</code>\n"
        f"{'✅ Admin' if ok else '❌ Admin မဟုတ်'}",
        parse_mode="HTML",
    )


async def cmd_setpay(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    if len(context.args) < 2:
        await reply(
            update,
            "ငွေလက်ခံ Settings —\n\n"
            f"USDT：<code>{db_get_setting('usdt')}</code>\n"
            f"USDT rate — 1 USDT = {format_mmk(get_usdt_rate())} ကျပ်（ကျပ်÷{int(get_usdt_rate())}）\n"
            f"KPay：<code>{db_get_setting('kpay')}</code>\n"
            f"WavePay：<code>{db_get_setting('wavepay')}</code>\n"
            f"ဆက်သွယ်ရန် — @{db_get_setting('admin_username').lstrip('@')}\n\n"
            "ပြင်နည်း —\n"
            "/setpay usdt your address\n"
            "/setpay rate 4200\n"
            "/setpay kpay phone\n"
            "/setpay wavepay phone\n"
            "/setpay admin your username",
            parse_mode="HTML",
        )
        return
    key = context.args[0].lower()
    val = " ".join(context.args[1:])
    mapping = {"usdt": "usdt", "kpay": "kpay", "wavepay": "wavepay", "admin": "admin_username", "rate": "usdt_rate"}
    if key not in mapping:
        await reply(update, "ရွေးချယ် — usdt / rate / kpay / wavepay / admin")
        return
    if key == "rate":
        try:
            float(val)
        except ValueError:
            await reply(update, "rate ဂဏန်း — /setpay rate 4200")
            return
    db_set_setting(mapping[key], val.lstrip("@") if key == "admin" else val)
    await reply(update, f"✅ ပြင်ပြီး {key}")


async def cmd_bind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if chat.type == "private":
        if not is_master(user.id if user else None):
            await reply(update, "Admin သာ bind လုပ်နိုင်သည်။")
            return
        if context.args:
            try:
                t = await context.bot.get_chat(int(context.args[0]))
            except Exception as e:
                await reply(update, f"မတွေ့ — {e}")
                return
            await verify_and_bind(update, context, t.id, t.title or str(t.id), t.type)
            return
        await reply(update, "Group/Channel msg forward လုပ်ပါ သို့မဟုတ် /bind -100xxx")
        return
    if not is_master(user.id if user else None):
        await reply(update, "Admin သာ bind လုပ်နိုင်သည်။")
        return
    await verify_and_bind(update, context, chat.id, chat.title or str(chat.id), chat.type)


async def cmd_unbind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    if chat.type == "private" or not is_master(update.effective_user.id):
        return
    db_remove_target(chat.id)
    await reply(update, "✅ bind ဖြုတ်ပြီး။")


async def cmd_targets(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    targets = db_list_targets()
    if not targets:
        await reply(update, "bind မရှိသေးပါ။")
        return
    default = db_get_default(update.effective_user.id)
    lines = ["📋 ချိတ်ဆက်ပြီး — "]
    for i, t in enumerate(targets, 1):
        mark = " ⭐" if default == t["id"] else ""
        lines.append(f"{i}. {t['title']}{mark}\n   {t['id']}")
    await reply(update, "\n".join(lines))


async def cmd_orders(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    base = WEB_BASE_URL.rstrip("/") if WEB_BASE_URL else f"http://localhost:{PORT}"
    url = f"{base}/admin?key={ADMIN_WEB_KEY}"
    await reply(
        update,
        f"📊 <b>Web အော်ဒါ Admin</b>\n\n"
        f"Browser တွင် ဖွင့်ပါ —\n<code>{url}</code>\n\n"
        f"ဝယ်ယူသူ၊ လိပ်စာ၊ လက်ရာ၊ ပမာဏ ကြည့်နိုင်သည်။\n"
        f"（Render — WEB_BASE_URL ထည့်ပါ）",
        parse_mode="HTML",
    )


async def cmd_default(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    targets = db_list_targets()
    if not targets:
        await reply(update, "Group/Channel အရင် bind လုပ်ပါ။")
        return
    await reply(update, "Default ထုတ်ဝေရာ ရွေးပါ —", reply_markup=build_target_keyboard(targets, "def"))


async def cmd_post(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    db_save_draft(update.effective_user.id, {"step": "await_content"})
    await reply(
        update,
        "📝 <b>ထုတ်ဝေ Mode</b>\n\n"
        "ထုတ်ဝေမည့် အကြောင်းအရာ ပို့ပါ —\n"
        "• ပုံ / ဗီဒီယို (album ၁၀ ပုံအထိ)\n"
        "• သို့မဟုတ် စာသား\n\n"
        "album သို့ တစ်ပုံချင်း — ပြီးရင် /done\n\n"
        "ပြီးရင် ဈေးနှုန်း သို့မဟုတ် 「ဈေးမပါ」",
        parse_mode="HTML",
    )


async def cmd_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_master(update.effective_user.id):
        return
    uid = update.effective_user.id
    draft = db_get_draft(uid)
    if not draft or draft.get("step") != "await_content" or not _get_draft_items(draft):
        await reply(update, "ထုတ်ဝေရန် ပုံမရှိ — /post ဖြင့် စပါ။")
        return
    old = _content_tasks.pop(uid, None)
    if old and not old.done():
        old.cancel()
    await _finalize_content(context, uid, update.effective_chat.id)


# ---------------------------------------------------------------------------
# admin post flow
# ---------------------------------------------------------------------------
async def on_admin_private(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """admin private"""
    if update.effective_chat.type != "private":
        return
    user = update.effective_user
    if not is_master(user.id if user else None):
        return

    msg = update.message
    if not msg:
        return

    try:
        draft = db_get_draft(user.id)
        if not draft:
            source = forward_chat(msg)
            if source and source.type in ("channel", "group", "supergroup"):
                await verify_and_bind(update, context, source.id, source.title or str(source.id), source.type)
            return

        step = draft.get("step")
        log.info("post draft user=%s step=%s", user.id, step)

        if step == "await_content":
            media_type, file_id = extract_media(msg)
            if media_type:
                caption = msg.caption or ""
                src = forward_chat(msg)
                if not caption and src:
                    caption = src.title or ""
                items = _get_draft_items(draft)
                if not any(x["file_id"] == file_id for x in items):
                    items.append({"type": media_type, "file_id": file_id})
                draft["items"] = items
                if caption:
                    draft["caption"] = caption
                db_save_draft(user.id, draft)
                await _schedule_content_finalize(context, user.id, msg.chat_id)
                await msg.reply_text(
                    f"✅ ရရှိပြီး {len(items)} ပုံ\n"
                    f"ဆက်ပို့ပါ သို့မဟုတ် /done"
                )
                return
            elif msg.text and not msg.text.startswith("/"):
                draft.update({
                    "media_type": "text", "file_id": "", "caption": msg.text, "price_mode": "qty",
                })
            else:
                await msg.reply_text("ပုံ၊ ဗီဒီယို သို့မဟုတ် စာသား ပို့ပါ။")
                return
            draft["step"] = "await_prices"
            db_save_draft(user.id, draft)
            await ask_prices(update, draft)
            return

        if step == "await_prices" and msg.text and not msg.text.startswith("/"):
            await on_admin_prices(update, context)
            return

    except Exception as e:
        log.exception("admin error")
        await msg.reply_text(f"❌ error — {e}\n\n/post ဖြင့် ပြန်စပါ")
        db_clear_draft(user.id)


async def on_admin_prices(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or not is_master(update.effective_user.id):
        return
    uid = update.effective_user.id
    draft = db_get_draft(uid)
    if not draft or draft.get("step") != "await_prices":
        return

    price_mode = draft.get("price_mode", "qty")
    if price_mode == "works":
        n = draft_work_count(draft)
        prices = parse_price_list(update.message.text, n)
        if not prices:
            await update.message.reply_text(
                f"❌ format မမှန် — ဈေးနှုန်း {n} ခု —\n"
                f"<code>{', '.join(['400000'] * min(n, 3))}</code>\n"
                f"သို့မဟုတ် 「ဈေးမပါ」 ခလုတ်",
                parse_mode="HTML",
            )
            return
        draft["work_prices"] = prices
        draft["prices_json"] = json.dumps(prices)
        draft["price1"] = prices[0]
        draft["price2"] = prices[1] if len(prices) > 1 else 0
        draft["price3"] = prices[2] if len(prices) > 2 else 0
        price_summary = " / ".join(f"{i}#{p:g}" for i, p in enumerate(prices, 1))
    else:
        prices = parse_prices(update.message.text)
        if not prices:
            await update.message.reply_text(
                "❌ format မမှန် — 400000, 750000, 1000000\n"
                "သို့မဟုတ် 「ဈေးမပါ」 ခလုတ်",
            )
            return
        draft["price1"], draft["price2"], draft["price3"] = prices
        price_summary = f"{prices[0]} / {prices[1]} / {prices[2]}"

    draft["no_price"] = False
    draft["step"] = "pick_target"
    draft.setdefault("selected_targets", [])
    db_save_draft(uid, draft)

    targets = db_list_targets()
    if not targets:
        await update.message.reply_text("Group/Channel bind (/bind)")
        db_clear_draft(uid)
        return

    await prompt_pick_targets(update, draft, price_summary)


# ---------------------------------------------------------------------------
# buyer purchase flow
# ---------------------------------------------------------------------------
async def show_buy_preview(context, user, listing: dict, qty: int, price: float,
                           source_chat_id: int | None = None, source_message_id: int | None = None):
    label = order_item_label(listing, qty)
    caption = (
        f"🛍 <b>{label}</b>\n"
        f"ဈေးနှုန်း — <b>{format_mmk(price)}</b> ကျပ်\n\n"
        f"အထက်ပါ ပစ္စည်း မှန်ကြောင်း အတည်ပြုပြီး ငွေပေးပါ —"
    )
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ ဝယ်ယူမည် အတည်ပြု", callback_data=f"buyok:{listing['id']}:{qty}")],
        [InlineKeyboardButton("❌ ပယ်ဖျက်", callback_data="buycancel:0")],
    ])
    extra = {
        "listing_id": listing["id"], "qty": qty, "price": price,
        "source_chat_id": source_chat_id, "source_message_id": source_message_id,
        "item_label": label,
    }
    db_set_buyer_session(user.id, 0, "pending_confirm", json.dumps(extra))
    await send_work_preview(context, user.id, listing, qty, caption, kb)


async def start_buy_flow(context, user, listing_id: int, qty: int,
                         source_chat_id: int | None = None, source_message_id: int | None = None):
    listing = db_get_listing(listing_id)
    if not listing:
        await context.bot.send_message(user.id, "ပစ္စည်း မရှိ/ရပ်ဆိုင်း။")
        return
    price = listing_qty_price(listing, qty)
    if price is None:
        await context.bot.send_message(user.id, "ရွေးချယ်မှု မမှန်။")
        return
    await show_buy_preview(context, user, listing, qty, price, source_chat_id, source_message_id)


async def on_buy_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, lid, qty = query.data.split(":")
    listing_id, qty = int(lid), int(qty)
    buyer = query.from_user

    listing = db_get_listing(listing_id)
    if not listing:
        await query.answer("ပစ္စည်း ရပ်ဆိုင်း", show_alert=True)
        return

    price = listing_qty_price(listing, qty)
    if price is None:
        await query.answer("မမှန်သော ရွေးချယ်မှု", show_alert=True)
        return

    src_chat = query.message.chat_id if query.message else None
    src_msg = query.message.message_id if query.message else None

    try:
        await show_buy_preview(context, buyer, listing, qty, price, src_chat, src_msg)
        await query.answer("Private chat တွင် အတည်ပြုပါ 👉")
    except Forbidden:
        me = await context.bot.get_me()
        await query.answer(url=f"https://t.me/{me.username}?start=buy_{listing_id}_{qty}")


async def on_buy_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, lid, qty = query.data.split(":")
    listing_id, qty = int(lid), int(qty)
    buyer = query.from_user

    session = db_get_buyer_session(buyer.id)
    if not session or session.get("step") != "pending_confirm":
        await query.answer("Session ကုန် — ပြန်ဝယ်ပါ", show_alert=True)
        return

    extra = session.get("extra") or {}
    if extra.get("listing_id") != listing_id or extra.get("qty") != qty:
        await query.answer("အော်ဒါ မကိုက် — ပြန်ဝယ်ပါ", show_alert=True)
        return

    listing = db_get_listing(listing_id)
    price = extra.get("price") or listing_qty_price(listing, qty)
    label = extra.get("item_label") or order_item_label(listing, qty)
    src_chat = extra.get("source_chat_id")
    src_msg = extra.get("source_message_id")
    post_link = build_message_link(src_chat, src_msg) if src_chat and src_msg else ""

    name = buyer.full_name or buyer.username or str(buyer.id)
    oid = db_create_order(
        listing_id, buyer.id, name, qty, price,
        item_label=label, source_chat_id=src_chat, source_message_id=src_msg, post_link=post_link,
    )
    db_set_buyer_session(buyer.id, oid, "await_pay_choice")

    try:
        if query.message and query.message.photo:
            await query.edit_message_caption(caption=f"✅ အတည်ပြုပြီး — {label} — {format_mmk(price)} ကျပ်")
        elif query.message:
            await query.edit_message_text(f"✅ အတည်ပြုပြီး — {label} — {format_mmk(price)} ကျပ်")
    except Exception:
        pass

    await show_payment_menu(context, buyer.id, oid)
    await query.answer("ငွေပေးချေနည်း ရွေးပါ")


async def on_buy_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    db_set_buyer_session(query.from_user.id, None, None)
    try:
        await query.edit_message_text("ဝယ်ယူမှု ပယ်ဖျက်ပြီး။")
    except Exception:
        pass
    await query.answer()


async def on_pay_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    _, oid, method = query.data.split(":")
    order_id = int(oid)
    order = db_get_order(order_id)
    if not order or query.from_user.id != order["buyer_id"]:
        await query.answer("အော်ဒါ မမှန်", show_alert=True)
        return

    db_update_order(order_id, payment_method=method)
    db_set_buyer_session(query.from_user.id, order_id, "await_proof")

    info = pay_info(method)
    pay_block = format_pay_block(method, order["price"])
    text = (
        f"{info}\n\n"
        f"{pay_block}\n\n"
        f"📌 <b>လုပ်ဆောင်ရန် —</b>\n"
        f"1️⃣ ငွေပေးချေပါ\n"
        f"2️⃣ <b>screenshot</b> ပို့ပါ\n"
        f"3️⃣ <b>လိပ်စာ</b> (စာသား)\n"
        f"4️⃣ <b>ဖုန်းနံပါတ်</b> (စာသား)\n\n"
        f"⚠️ ငွေမှန်ကန်စွာ ပေးချေပါ။"
    )
    await query.edit_message_text(text, parse_mode="HTML")
    await query.answer()


async def on_buyer_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private" or is_master(update.effective_user.id):
        return

    session = db_get_buyer_session(update.effective_user.id)
    if not session:
        return

    order = db_get_order(session["order_id"])
    if not order:
        db_set_buyer_session(update.effective_user.id, None, None)
        return

    try:
        if session["step"] == "pending_confirm":
            await update.message.reply_text("preview တွင် 「အတည်ပြု」/「ပယ်ဖျက်」 နှိပ်ပါ။")
            return

        if session["step"] == "await_proof":
            proof_id = None
            if update.message.photo:
                proof_id = update.message.photo[-1].file_id
            elif update.message.document and (update.message.document.mime_type or "").startswith("image/"):
                proof_id = update.message.document.file_id

            if not proof_id:
                await update.message.reply_text("screenshot (ပုံ) ပို့ပါ။")
                return

            db_update_order(order["id"], proof_file_id=proof_id)
            db_set_buyer_session(update.effective_user.id, order["id"], "await_address")
            await update.message.reply_text("✅ screenshot ရပြီး။\n\nလိပ်စာ (စာသား) ပို့ပါ —")
            return

        if session["step"] == "await_address":
            address = update.message.text or update.message.caption
            if not address:
                await update.message.reply_text("လိပ်စာ စာသားဖြင့် ပို့ပါ။")
                return

            db_update_order(order["id"], address=address)
            db_set_buyer_session(update.effective_user.id, order["id"], "await_phone")
            await update.message.reply_text("✅ လိပ်စာ ရပြီး။\n\nဖုန်းနံပါတ် ပို့ပါ —")
            return

        if session["step"] == "await_phone":
            phone = (update.message.text or update.message.caption or "").strip()
            if not phone or not re.search(r"\d", phone):
                await update.message.reply_text("မှန်ကန်သော ဖုန်းနံပါတ် ပို့ပါ။")
                return

            db_update_order(order["id"], buyer_phone=phone, status="pending_review")
            db_set_buyer_session(update.effective_user.id, None, None)

            order = db_get_order(order["id"])
            proof_id = order.get("proof_file_id")
            address = order.get("address") or ""

            await update.message.reply_text("✅ တင်ပြီး！ Admin စစ်ဆေးချက် စောင့်ပါ။")

            method = order.get("payment_method") or "?"
            mmk = order["price"]
            item_label = order.get("item_label") or order_item_label(db_get_listing(order["listing_id"]), order["qty"])
            post_link = order.get("post_link") or ""
            amount_line = f"ပမာဏ — {format_mmk(mmk)} ကျပ်"
            if method == "usdt":
                amount_line += f"\nUSDT：{mmk_to_usdt(mmk):.2f} USDT（÷{int(get_usdt_rate())}）"
            link_line = f"လက်ရာ Link — <a href=\"{post_link}\">{post_link}</a>\n" if post_link else ""
            admin_text = (
                f"🔔 <b>အော်ဒါ အသစ် #{order['id']}</b>\n\n"
                f"လက်ရာ — <b>{item_label}</b>\n"
                f"{link_line}"
                f"ဝယ်ယူသူ — {order['buyer_name']} (<code>{order['buyer_id']}</code>)\n"
                f"ဖုန်း — {phone}\n"
                f"{amount_line}\n"
                f"ငွေပေးချေ — {method.upper()}\n"
                f"လိပ်စာ — {address}\n\n"
                f"screenshot စစ်ပြီး နှိပ်ပါ —"
            )
            await context.bot.send_message(
                MASTER_ID, admin_text, parse_mode="HTML", reply_markup=review_buttons(order["id"]),
            )
            if proof_id:
                cap = f"#{order['id']} | {item_label} | screenshot"
                if post_link:
                    cap += f"\n{post_link}"
                await context.bot.send_photo(MASTER_ID, proof_id, caption=cap)
            return
    except Exception as e:
        log.exception("buyer error")
        await update.message.reply_text(f"error — ပြန်ကြိုးစားပါ။({e})")


async def on_review_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_master(query.from_user.id):
        await query.answer("ခွင့်မရှိ", show_alert=True)
        return

    _, oid, result = query.data.split(":")
    order_id = int(oid)
    order = db_get_order(order_id)
    if not order:
        await query.answer("အော်ဒါ မရှိ", show_alert=True)
        return

    admin_user = db_get_setting("admin_username").lstrip("@")
    buyer_id = order["buyer_id"]

    if result == "ok":
        db_update_order(order_id, status="success")
        buyer_msg = (
            "🎉 <b>ဝယ်ယူမှု အောင်မြင်！</b>\n\n"
            "အော်ဒါ အတည်ပြုပြီး — <b>၇-၁၅ ရက်</b> အတွင်း ပို့ပေးမည်。\n"
            "ပစ္စည်းမရရင် Admin — "
            f" @{admin_user}"
        )
        await query.edit_message_text(f"✅ #{order_id} အောင်မြင်")
    else:
        db_update_order(order_id, status="failed")
        buyer_msg = (
            "❌ <b>ဝယ်ယူမှု မအောင်မြင်</b>\n\n"
            "screenshot မအောင်မြင် — ပြန်စစ်ပါ。\n"
            "မေးခွန်း —"
            f" @{admin_user}"
        )
        await query.edit_message_text(f"❌ #{order_id} ငြင်းပယ်")

    try:
        await context.bot.send_message(buyer_id, buyer_msg, parse_mode="HTML")
    except Exception as e:
        log.error("notify buyer failed: %s", e)

    await query.answer()


# ---------------------------------------------------------------------------
# admin callbacks
# ---------------------------------------------------------------------------
async def on_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not is_master(query.from_user.id):
        return

    action, _, value = query.data.partition(":")
    uid = query.from_user.id

    if action == "def":
        if value == "cancel":
            await query.edit_message_text("ပယ်ဖျက်ပြီး။")
        else:
            db_set_default(uid, int(value))
            targets = {t["id"]: t for t in db_list_targets()}
            await query.edit_message_text(f"⭐ Default — {targets.get(int(value), {}).get('title', value)}")
        await query.answer()
        return

    if action == "post":
        draft = db_get_draft(uid)
        if value == "cancel":
            db_clear_draft(uid)
            await query.edit_message_text("ထုတ်ဝေမှု ပယ်ဖျက်။")
        elif value == "noprice":
            if not draft or draft.get("step") != "await_prices":
                await query.answer("/post ဖြင့် အရင် စပါ", show_alert=True)
                return
            draft["no_price"] = True
            draft["step"] = "pick_target"
            draft.setdefault("selected_targets", [])
            db_save_draft(uid, draft)
            targets = db_list_targets()
            if not targets:
                db_clear_draft(uid)
                await query.edit_message_text("Group/Channel bind (/bind)")
            else:
                await query.edit_message_text(
                    "📝 ဈေးမပါ — Group/Channel (များစွာ) —",
                    reply_markup=build_multi_target_keyboard(targets, draft_selected_set(draft)),
                )
        await query.answer()
        return

    if action == "ptog":
        draft = db_get_draft(uid)
        if not draft:
            await query.answer("task မရှိ", show_alert=True)
            return
        targets = db_list_targets()
        selected = draft_selected_set(draft)
        if value == "all":
            selected = {t["id"] for t in targets}
        else:
            tid = int(value)
            if tid in selected:
                selected.discard(tid)
            else:
                selected.add(tid)
        draft["selected_targets"] = list(selected)
        db_save_draft(uid, draft)
        await query.edit_message_reply_markup(
            reply_markup=build_multi_target_keyboard(targets, selected),
        )
        await query.answer(f"ရွေးထား {len(selected)} ခု")
        return

    if action == "pubgo":
        draft = db_get_draft(uid)
        if not draft or not draft.get("selected_targets"):
            await query.answer("Group/Channel အနည်းဆုံး ၁ ခု", show_alert=True)
            return
        ok, msg = await finish_publish(context, uid, draft)
        await query.edit_message_text(msg)
        await query.answer()
        return

    if action == "pick":
        draft = db_get_draft(uid)
        if not draft or value == "cancel":
            db_clear_draft(uid)
            await query.edit_message_text("ထုတ်ဝေမှု ပယ်ဖျက်။")
            await query.answer()
            return
        await query.answer()
        return

    if action == "sale":
        draft = db_get_draft(uid)
        if not draft or value == "cancel":
            db_clear_draft(uid)
            await query.edit_message_text("ထုတ်ဝေမှု ပယ်ဖျက်။")
            await query.answer()
            return
        draft["selected_targets"] = [int(value)]
        db_save_draft(uid, draft)
        ok, msg = await finish_publish(context, uid, draft)
        await query.edit_message_text(msg)
        await query.answer()
        return


async def on_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    data = update.callback_query.data
    if data.startswith("buy:"):
        await on_buy_click(update, context)
    elif data.startswith("buyok:"):
        await on_buy_confirm(update, context)
    elif data.startswith("buycancel:"):
        await on_buy_cancel(update, context)
    elif data.startswith("pay:"):
        await on_pay_click(update, context)
    elif data.startswith("review:"):
        await on_review_click(update, context)
    elif data.startswith(("def:", "sale:", "pick:", "post:", "ptog:", "pubgo:")):
        await on_admin_callback(update, context)


async def on_bot_joined(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.my_chat_member
    if not m or m.new_chat_member.status not in ("administrator", "member"):
        return
    chat = m.chat
    if chat.type in ("group", "supergroup", "channel"):
        try:
            await context.bot.send_message(chat.id, HELP_GROUP.format(cid=chat.id), parse_mode="HTML")
        except Exception:
            pass


async def on_log(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user, msg = update.effective_chat, update.effective_user, update.effective_message
    log.info("chat=%s user=%s text=%s", getattr(chat, "id", "?"), getattr(user, "id", "?"),
             (msg.text[:50] if msg and msg.text else ""))


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE):
    err = context.error
    if isinstance(err, Conflict):
        log.error(
            "409 Conflict — Token တစ်ခုတည်း instance များစွာ polling။ postbot_my တစ်ခုတည်းသာ run ထားပါ။"
        )
        return
    log.exception("error", exc_info=err)


async def post_init(application: Application) -> None:
    await application.bot.delete_webhook(drop_pending_updates=True)
    me = await application.bot.get_me()
    wh = await application.bot.get_webhook_info()
    host = socket.gethostname()
    tok_hint = f"...{TOKEN[-6:]}" if len(TOKEN) > 6 else "?"
    log.info(
        "Bot MY အဆင်သင့် @%s | pid=%s host=%s | token=%s | webhook=%s",
        me.username, os.getpid(), host, tok_hint, wh.url or "(မရှိ)",
    )


# ---------------------------------------------------------------------------
# startup
# ---------------------------------------------------------------------------
@flask_app.route("/")
def health():
    return f"PostBot MY OK | master={MASTER_ID} | <a href='/admin?key={ADMIN_WEB_KEY}'>Web အော်ဒါ Admin</a>", 200


@flask_app.route("/admin")
def admin_orders_page():
    key = request.args.get("key", "")
    if key != ADMIN_WEB_KEY:
        return "Unauthorized — ADMIN_WEB_KEY ထည့်ပါ", 401

    date = request.args.get("date", datetime.now().strftime("%Y-%m-%d"))
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        date = datetime.now().strftime("%Y-%m-%d")

    orders = db_list_orders(date=date)
    stats = db_daily_stats(date)
    status_map = {
        "pending_pay": "ငွေမပေးရသေး", "pending_review": "စစ်ဆေးရန်",
        "success": "အောင်မြင်", "failed": "မအောင်မြင်",
    }

    rows_html = ""
    for o in orders:
        st = status_map.get(o.get("status"), o.get("status"))
        link = o.get("post_link") or ""
        item = o.get("item_label") or f"ပစ္စည်း#{o.get('listing_id')}"
        link_cell = f'<a href="{link}" target="_blank">လက်ရာ Link</a>' if link else item
        rows_html += (
            f"<tr><td>{o['id']}</td><td>{o.get('created_at','')}</td>"
            f"<td>{o.get('buyer_name','')}</td><td>{o.get('buyer_phone') or '-'}</td>"
            f"<td>{link_cell}</td><td>{o.get('address') or '-'}</td>"
            f"<td>{format_mmk(o.get('price', 0))}</td><td>{st}</td></tr>"
        )

    prev = (datetime.strptime(date, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    nxt = (datetime.strptime(date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>PostBot MY Orders</title>
<style>
body{{font-family:sans-serif;margin:16px;background:#f5f5f5}}
.card{{background:#fff;padding:16px;border-radius:8px;margin-bottom:16px;box-shadow:0 1px 3px #0002}}
table{{width:100%;border-collapse:collapse;font-size:14px}}
th,td{{border:1px solid #ddd;padding:8px;text-align:left}}
th{{background:#333;color:#fff}}
.stats{{display:flex;gap:24px;flex-wrap:wrap}}
.stat{{font-size:18px}} .stat b{{color:#007bff}}
nav a{{margin-right:12px;padding:6px 12px;background:#007bff;color:#fff;text-decoration:none;border-radius:4px}}
input[type=date]{{padding:6px;font-size:16px}}
</style></head><body>
<h1>📊 PostBot Web အော်ဒါ Admin</h1>
<div class="card stats">
  <div class="stat">ရက်စွဲ — <b>{date}</b></div>
  <div class="stat">ရောင်းချ — <b>{stats['count']}</b></div>
  <div class="stat">စုစုပေါင်း — <b>{format_mmk(stats['total'])}</b> ကျပ်</div>
</div>
<div class="card">
  <form method="get">
    <input type="hidden" name="key" value="{key}">
    <label>ရက်စွဲ — </label>
    <input type="date" name="date" value="{date}" onchange="this.form.submit()">
    <button type="submit">ရှာမည်</button>
  </form>
  <p style="margin-top:12px">
    <a href="/admin?key={key}&date={prev}">← မနေ့</a>
    <a href="/admin?key={key}&date={nxt}">နောက်နေ့ →</a>
    <a href="/admin?key={key}&date={datetime.now().strftime('%Y-%m-%d')}">ယနေ့</a>
  </p>
</div>
<div class="card" style="overflow-x:auto">
<table>
<tr><th>ID</th><th>အချိန်</th><th>ဝယ်ယူသူ</th><th>ဖုန်း</th><th>လက်ရာ</th><th>လိပ်စာ</th><th>ပမာဏ</th><th>အခြေအနေ</th></tr>
{rows_html if rows_html else '<tr><td colspan="8">အော်ဒါမရှိ</td></tr>'}
</table>
</div>
</body></html>"""


def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)


async def on_private_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_chat.type != "private":
        return
    user = update.effective_user
    if not user:
        return
    if is_master(user.id):
        await on_admin_private(update, context)
    else:
        await on_buyer_message(update, context)


def create_app() -> Application:
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_error_handler(on_error)
    app.add_handler(MessageHandler(filters.ALL, on_log), group=-1)
    app.add_handler(ChatMemberHandler(on_bot_joined, ChatMemberHandler.MY_CHAT_MEMBER))

    for cmd, handler in [
        ("start", cmd_start), ("help", cmd_help), ("ping", cmd_ping), ("id", cmd_id),
        ("post", cmd_post), ("done", cmd_done), ("bind", cmd_bind), ("unbind", cmd_unbind),
        ("targets", cmd_targets), ("default", cmd_default), ("setpay", cmd_setpay), ("orders", cmd_orders),
    ]:
        app.add_handler(CommandHandler(cmd, handler))
        app.add_handler(CommandHandler(cmd, handler, filters=filters.UpdateType.CHANNEL_POSTS))

    app.add_handler(CallbackQueryHandler(on_callback_router))
    app.add_handler(MessageHandler(filters.ChatType.PRIVATE & ~filters.COMMAND, on_private_router))
    return app


def _instance_holder() -> str:
    return f"{socket.gethostname()}:{os.getpid()}"


def acquire_instance_lock() -> bool:
    """single instance lock"""
    holder = _instance_holder()
    now = time.time()
    with sqlite3.connect(DB_PATH, timeout=5) as conn:
        conn.execute(
            "CREATE TABLE IF NOT EXISTS instance_lock (k TEXT PRIMARY KEY, holder TEXT, heartbeat REAL)"
        )
        row = conn.execute(
            "SELECT holder, heartbeat FROM instance_lock WHERE k=?", (LOCK_KEY,),
        ).fetchone()
        if row:
            old_holder, hb = row
            if old_holder != holder and now - (hb or 0) < LOCK_STALE_SEC:
                log.error(
                    "instance ရှိပြီး polling — %s（%ds  လွန်ခဲ့)။ process ထွက်မည်။",
                    old_holder, int(now - hb),
                )
                return False
        conn.execute(
            "INSERT OR REPLACE INTO instance_lock (k, holder, heartbeat) VALUES (?, ?, ?)",
            (LOCK_KEY, holder, now),
        )
        conn.commit()
    return True


def start_lock_heartbeat():
    holder = _instance_holder()

    def beat():
        while True:
            time.sleep(30)
            try:
                with sqlite3.connect(DB_PATH, timeout=5) as conn:
                    conn.execute(
                        "UPDATE instance_lock SET heartbeat=? WHERE k=? AND holder=?",
                        (time.time(), LOCK_KEY, holder),
                    )
                    conn.commit()
            except Exception:
                pass

    threading.Thread(target=beat, daemon=True).start()


def main():
    init_db()
    if not acquire_instance_lock():
        log.error(
            "409 — instance ထပ်နေ။ Instance=1၊ service Suspend၊ Token revoke/update POSTBOT_TOKEN။"
        )
        sys.exit(0)
    start_lock_heartbeat()
    threading.Thread(target=run_flask, daemon=True).start()
    log.info("PostBot MY စ port=%s master=%s holder=%s", PORT, MASTER_ID, _instance_holder())
    create_app().run_polling(drop_pending_updates=True, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
