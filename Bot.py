"""
Whatnot Claims Bot (discord.py 2.x) — FULL VERSION
Includes:
- .env token loading + config.json for IDs/names
- Dual categories (NSFW=N, SFW=S)
- Upload from 4 threads: Raw NSFW, WM NSFW, Raw SFW, WM SFW
- WM posted publicly to selection channels; RAW attached in claims thread posts
- !upload N|S|all [item] + aliases
- Item codes: N001 / S001 (shown as "Item N001" / "Item S001")
- Winner system with roles Winner X (discord users)
- Guests (non-discord) winners + claiming: !guestwinner / !guestclaim / !gueststatus
- Claim via reaction with multiple affirmation emojis
- Deletes claimed listing from selection channel
- !sort deletes bot claim posts and rebuilds thread grouped by user WITH RAW re-attached (fallback to RAW link)
- !endshow runs sort + locks selection channels + archives/locks thread + closes claims
- !swap / !unassign
- !random assigns a random remaining item (discord user)
- !randomguest assigns a random remaining item (guest)
- !export (current thread by default, or "!export all")

IMPORTANT SETUP:
1) Install Python 3.11
2) Create venv + install deps:
   py -3.11 -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install -U pip
   pip install discord.py aiohttp python-dotenv watchdog

3) Create .env:
   DISCORD_TOKEN=YOUR_TOKEN

4) Create config.json (example provided in chat)

5) FIRST TIME AFTER SCHEMA CHANGES:
   If you previously used a DB with user_id NOT NULL, delete the sqlite file:
   - stop bot
   - delete whatnot_claims.sqlite3
   - restart bot
"""

import asyncio
import csv
import io
import json
import os
import random
import sqlite3
import time
from datetime import datetime, timezone
from typing import Optional, Tuple, Dict, List

import aiohttp
import discord
from discord.ext import commands
from dotenv import load_dotenv


ATTACH_RAW_FILES = False

# ---------------------------
# Load .env + config.json
# ---------------------------
load_dotenv()

with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
if not DISCORD_TOKEN:
    raise RuntimeError("Missing DISCORD_TOKEN in .env")

UPLOAD_PARENT_CHANNEL_ID = int(CONFIG["upload_parent_channel_id"])
CLAIMED_PARENT_CHANNEL_ID = int(CONFIG["claimed_parent_channel_id"])

NSFW_SELECTION_CHANNEL_ID = int(CONFIG["selection_channels"]["N"])
SFW_SELECTION_CHANNEL_ID = int(CONFIG["selection_channels"]["S"])

THREAD_RAW_N = CONFIG["upload_threads"]["raw_nsfw"]
THREAD_WM_N = CONFIG["upload_threads"]["wm_nsfw"]
THREAD_RAW_S = CONFIG["upload_threads"]["raw_sfw"]
THREAD_WM_S = CONFIG["upload_threads"]["wm_sfw"]

VERIFY_CHANNEL_ID = int(CONFIG["verify_channel_id"])
UNVERIFIED_ROLE_ID = int(CONFIG["unverified_role_id"])
VERIFIED_ROLE_ID = int(CONFIG["verified_role_id"])


STAFF_ROLE_NAMES = set(CONFIG["staff_roles"])
WINNER_ROLE_PREFIX = CONFIG.get("winner_role_prefix", "Winner")

UPLOAD_HISTORY_LIMIT = int(CONFIG["upload"]["history_limit"])
UPLOAD_SEND_DELAY = float(CONFIG["upload"]["send_delay"])

CLAIM_EMOJIS = set(CONFIG["claim_emojis"])

# ---------------------------
# DB
# ---------------------------
DB_PATH = CONFIG.get("db_path", "whatnot_claims.sqlite3")

SCHEMA = """
CREATE TABLE IF NOT EXISTS items (
    item_code TEXT PRIMARY KEY,         -- e.g. N001, S042
    category TEXT NOT NULL,              -- 'N' or 'S'
    number INTEGER NOT NULL,             -- 1..n
    wm_filename TEXT NOT NULL,
    wm_url TEXT NOT NULL,
    raw_filename TEXT NOT NULL,
    raw_url TEXT NOT NULL,
    selection_message_id INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS winners (
    user_id INTEGER PRIMARY KEY,
    reason TEXT NOT NULL,
    remaining INTEGER NOT NULL
);

-- user_id is NULL for guest claims
CREATE TABLE IF NOT EXISTS claims (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    claimed_at TEXT NOT NULL,
    guild_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    user_id INTEGER,                 -- NULL for guest claims
    user_tag TEXT NOT NULL,          -- Discord tag or guest name
    reason TEXT NOT NULL,
    category TEXT NOT NULL,
    item_code TEXT NOT NULL,
    item_number INTEGER NOT NULL,
    wm_filename TEXT NOT NULL,
    raw_filename TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS guest_winners (
    guest_tag TEXT PRIMARY KEY,
    reason TEXT NOT NULL,
    remaining INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""

def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.executescript(SCHEMA)
    return conn

conn = db_connect()

CONFIRM_WINDOW_SECONDS = 60
_pending_confirms: Dict[Tuple[int, int, str], float] = {}
# key = (guild_id, user_id, action) -> expires_at (monotonic)

# ---------------------------
# Helpers
# ---------------------------
def _clean_whatnot_name(name: str) -> str:
    """
    Sanitizes and validates a Whatnot username for Discord nicknames.
    Raises ValueError if invalid.
    """
    if not name:
        raise ValueError("Empty name")

    # Normalize spaces
    name = " ".join(name.strip().split())

    # Block mass-mention abuse
    lowered = name.lower()
    if "@everyone" in lowered or "@here" in lowered:
        raise ValueError("Invalid name")

    # Discord nickname limits
    if len(name) < 2 or len(name) > 32:
        raise ValueError("Name must be 2–32 characters")

    return name

def _dbg_last_filenames(msgs: list, maxn: int = 15) -> str:
    names = []
    for m in msgs[-maxn:]:
        for a in getattr(m, "attachments", []):
            names.append(a.filename)
    return " | ".join(names[-maxn:]) if names else "(none)"

async def fetch_bytes(url: str) -> Optional[bytes]:
    global http_session
    if not url:
        return None
    if http_session is None:
        http_session = aiohttp.ClientSession()
    try:
        async with http_session.get(url) as resp:
            if resp.status != 200:
                return None
            return await resp.read()
    except Exception:
        return None

def _message_text_for_search(m: discord.Message) -> str:
    parts = []
    if m.content:
        parts.append(m.content)

    # Include embed text (your claim posts might be embeds)
    for e in (m.embeds or []):
        if e.title:
            parts.append(e.title)
        if e.description:
            parts.append(e.description)
        for f in (e.fields or []):
            if f.name:
                parts.append(f.name)
            if f.value:
                parts.append(f.value)

    return "\n".join(parts)


async def delete_claim_post_from_thread(
    guild: discord.Guild,
    thread_id: int,
    user_id: Optional[int],
    user_tag: str,
    item_code: str,
    lookback: int = 1200
) -> bool:
    """
    Deletes the bot's claim message in the active show thread for this user+item.
    Matches against message content + embeds.
    Returns True if deleted.
    """
    try:
        thread = guild.get_thread(thread_id) or await guild.fetch_channel(thread_id)
    except Exception:
        return False

    if not isinstance(thread, (discord.Thread, discord.TextChannel)):
        return False

    # Match loosely:
    # - must be bot-authored
    # - must contain the raw item_code (e.g., "N001")
    # - must contain the user mention OR user tag
    needle_item = item_code  # just "N001" or "S012"
    needle_user_mention = f"<@{user_id}>" if user_id else None

    try:
        async for m in thread.history(limit=lookback, oldest_first=False):
            if not m.author.bot:
                continue

            hay = _message_text_for_search(m)
            if needle_item not in hay:
                continue

            user_match = False
            if needle_user_mention and needle_user_mention in hay:
                user_match = True
            elif user_tag and user_tag in hay:
                user_match = True

            if not user_match:
                continue

            try:
                await m.delete()
                return True
            except Exception:
                return False
    except Exception:
        return False

    return False


async def ensure_help_message(guild: discord.Guild):
    """
    Ensures there is exactly ONE official help embed in HELP_CHANNEL_ID.
    Updates it on startup and deletes older duplicates so they don't stack.
    Never hard-fails if missing permissions.
    """
    if not HELP_CHANNEL_ID:
        return

    ch = guild.get_channel(HELP_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return

    embed = build_help_embed()

    # --- Fetch stored official message id (supports older key too) ---
    msg_id_str = get_setting("help_message_id") or get_setting("help_msg")
    official = None

    if msg_id_str and str(msg_id_str).isdigit():
        try:
            official = await ch.fetch_message(int(msg_id_str))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            official = None

    # --- Update existing help message if found ---
    if official:
        try:
            await official.edit(embed=embed)
        except (discord.Forbidden, discord.HTTPException):
            official = None

    # --- Otherwise create a new official help message ---
    if not official:
        try:
            official = await ch.send(embed=embed)
            set_setting("help_message_id", str(official.id))
        except discord.Forbidden:
            return  # can't access the channel

    # --- Optional pin (never crash) ---
    if HELP_AUTO_PIN and official:
        try:
            if not official.pinned:
                await official.pin(reason="Auto-pin bot help")
        except discord.Forbidden:
            pass

    # --- Cleanup duplicates (requires Read History + Manage Messages to delete) ---
    try:
        async for m in ch.history(limit=75, oldest_first=False):
            if not m.author.bot:
                continue
            if m.id == official.id:
                continue

            # Only delete messages that look like our help embed
            if m.embeds and m.embeds[0].title == embed.title:
                try:
                    await m.delete()
                    await asyncio.sleep(0.25)
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass
    except discord.Forbidden:
        # If we can't read history, we simply can't cleanup — but we still updated/posted the official message
        pass


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def is_staff(member: discord.Member) -> bool:
    return any(r.name in STAFF_ROLE_NAMES for r in member.roles)

def parse_item_number(filename: str) -> Optional[int]:
    """
    Supports:
      12-27-2025_1.png -> 1
      12-27-2025_12.jpg -> 12
      anything (3).png -> 3
    """
    name = filename.strip()
    dot = name.rfind(".")
    if dot == -1:
        return None
    base = name[:dot]

    if "_" in base:
        tail = base.rsplit("_", 1)[-1].strip()
        if tail.isdigit():
            n = int(tail)
            return n if n > 0 else None

    close_paren = base.rfind(")")
    open_paren = base.rfind("(", 0, close_paren)
    if open_paren != -1 and close_paren != -1 and close_paren > open_paren:
        inside = base[open_paren + 1:close_paren].strip()
        if inside.isdigit():
            n = int(inside)
            return n if n > 0 else None

    return None

def _needs_confirm(guild_id: int, user_id: int, action: str) -> bool:
    now = time.monotonic()
    key = (guild_id, user_id, action)
    exp = _pending_confirms.get(key, 0.0)

    # valid confirm exists
    if exp > now:
        del _pending_confirms[key]
        return False

    # set new confirm window
    _pending_confirms[key] = now + CONFIRM_WINDOW_SECONDS
    return True


def make_item_code(category: str, number: int) -> str:
    return f"{category.upper()}{str(number).zfill(3)}"

def parse_item_code(s: str) -> Optional[str]:
    t = s.strip().upper()
    if len(t) == 4 and t[0] in {"N", "S"} and t[1:].isdigit():
        return t
    return None

def category_name(category: str) -> str:
    return "NSFW" if category.upper() == "N" else "SFW"

def category_emoji(category: str) -> str:
    return "🔞" if category.upper() == "N" else "🟦"

def selection_channel_id_for(category: str) -> int:
    return NSFW_SELECTION_CHANNEL_ID if category.upper() == "N" else SFW_SELECTION_CHANNEL_ID

def set_setting(key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO settings(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
        (key, value)
    )
    conn.commit()

def get_setting(key: str) -> Optional[str]:
    row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
    return row[0] if row else None

def clear_setting(key: str) -> None:
    conn.execute("DELETE FROM settings WHERE key=?", (key,))
    conn.commit()

def get_active_thread_id() -> int:
    v = get_setting("active_claims_thread_id")
    return int(v) if v and v.isdigit() else 0

def get_winner_state(user_id: int) -> Optional[Tuple[str, int]]:
    row = conn.execute("SELECT reason, remaining FROM winners WHERE user_id=?", (user_id,)).fetchone()
    return (row[0], int(row[1])) if row else None

def get_guest_winner(guest_tag: str) -> Optional[Tuple[str, int]]:
    row = conn.execute("SELECT reason, remaining FROM guest_winners WHERE guest_tag=?", (guest_tag,)).fetchone()
    return (row[0], int(row[1])) if row else None

def upsert_guest_winner(guest_tag: str, reason: str, remaining: int) -> None:
    conn.execute(
        "INSERT INTO guest_winners(guest_tag, reason, remaining) VALUES(?,?,?) "
        "ON CONFLICT(guest_tag) DO UPDATE SET reason=excluded.reason, remaining=excluded.remaining",
        (guest_tag, reason, remaining)
    )
    conn.commit()

async def ensure_winner_role(guild: discord.Guild, n: int) -> discord.Role:
    name = f"{WINNER_ROLE_PREFIX} {n}"
    role = discord.utils.get(guild.roles, name=name)
    if role:
        return role
    return await guild.create_role(name=name, mentionable=False, reason="Auto-create winner role")

def get_current_winner_role(member: discord.Member) -> Optional[Tuple[discord.Role, int]]:
    for r in member.roles:
        if r.name.startswith(WINNER_ROLE_PREFIX + " "):
            try:
                n = int(r.name.split(" ", 1)[1].strip())
                return (r, n)
            except ValueError:
                continue
    return None

async def set_member_winner_remaining(member: discord.Member, reason: str, remaining: int) -> None:
    current = get_current_winner_role(member)
    if current:
        old_role, _ = current
        try:
            await member.remove_roles(old_role, reason="Updating winner remaining")
        except discord.Forbidden:
            pass

    if remaining > 0:
        role = await ensure_winner_role(member.guild, remaining)
        await member.add_roles(role, reason="Assigning winner role")

    conn.execute(
        "INSERT INTO winners(user_id, reason, remaining) VALUES(?,?,?) "
        "ON CONFLICT(user_id) DO UPDATE SET reason=excluded.reason, remaining=excluded.remaining",
        (member.id, reason, remaining)
    )
    conn.commit()

def upsert_item(item_code: str, category: str, number: int,
                wm_filename: str, wm_url: str, raw_filename: str, raw_url: str,
                selection_message_id: int) -> None:
    conn.execute(
        "INSERT INTO items(item_code, category, number, wm_filename, wm_url, raw_filename, raw_url, selection_message_id) "
        "VALUES(?,?,?,?,?,?,?,?) "
        "ON CONFLICT(item_code) DO UPDATE SET "
        "category=excluded.category, number=excluded.number, "
        "wm_filename=excluded.wm_filename, wm_url=excluded.wm_url, "
        "raw_filename=excluded.raw_filename, raw_url=excluded.raw_url, "
        "selection_message_id=excluded.selection_message_id",
        (item_code, category, number, wm_filename, wm_url, raw_filename, raw_url, selection_message_id)
    )
    conn.commit()

def update_item_selection_message_id(item_code: str, message_id: int) -> None:
    conn.execute("UPDATE items SET selection_message_id=? WHERE item_code=?", (message_id, item_code.upper()))
    conn.commit()

def get_item(item_code: str):
    return conn.execute(
        "SELECT item_code, category, number, wm_filename, wm_url, raw_filename, raw_url, selection_message_id "
        "FROM items WHERE item_code=?",
        (item_code.upper(),)
    ).fetchone()

def get_item_by_selection_message(selection_message_id: int):
    return conn.execute(
        "SELECT item_code, category, number, wm_filename, wm_url, raw_filename, raw_url, selection_message_id "
        "FROM items WHERE selection_message_id=?",
        (selection_message_id,)
    ).fetchone()

def get_raw_url_for_item(item_code: str) -> Optional[str]:
    row = conn.execute("SELECT raw_url FROM items WHERE item_code=?", (item_code.upper(),)).fetchone()
    return row[0] if row else None

def is_item_claimed(item_code: str, thread_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM claims WHERE item_code=? AND thread_id=? LIMIT 1",
        (item_code.upper(), thread_id)
    ).fetchone()
    return row is not None

def add_claim(guild_id: int, thread_id: int, user_id: Optional[int], user_tag: str,
              reason: str, category: str, item_code: str, item_number: int, wm_filename: str, raw_filename: str) -> str:
    claimed_at = now_utc_iso()
    conn.execute(
        "INSERT INTO claims(claimed_at, guild_id, thread_id, user_id, user_tag, reason, category, item_code, item_number, wm_filename, raw_filename) "
        "VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (claimed_at, guild_id, thread_id, user_id, user_tag, reason, category.upper(), item_code.upper(), item_number, wm_filename, raw_filename)
    )
    conn.commit()
    return claimed_at

def get_claim_for_item(thread_id: int, item_code: str):
    return conn.execute(
        "SELECT id, claimed_at, user_id, user_tag, reason, category, item_code, item_number, wm_filename, raw_filename "
        "FROM claims WHERE thread_id=? AND item_code=? ORDER BY id DESC LIMIT 1",
        (thread_id, item_code.upper())
    ).fetchone()

def delete_claim_by_id(claim_id: int) -> None:
    conn.execute("DELETE FROM claims WHERE id=?", (claim_id,))
    conn.commit()

def get_claims_for_thread(thread_id: int) -> List[tuple]:
    return conn.execute(
        "SELECT user_tag, COALESCE(user_id, 0) as user_id, category, item_code, item_number, wm_filename, raw_filename, claimed_at, reason "
        "FROM claims WHERE thread_id=? ORDER BY user_tag COLLATE NOCASE, item_number ASC",
        (thread_id,)
    ).fetchall()

def list_unclaimed_items(thread_id: int, category: str) -> List[str]:
    rows = conn.execute(
        "SELECT item_code FROM items WHERE category=? ORDER BY number ASC",
        (category.upper(),)
    ).fetchall()
    out = []
    for (code,) in rows:
        if not is_item_claimed(code, thread_id):
            out.append(code)
    return out

async def dm_user_safe(user: discord.abc.User, text: str) -> None:
    try:
        await user.send(text)
    except Exception:
        pass

async def remove_user_reaction_safe(guild: discord.Guild, channel_id: int, message_id: int,
                                    emoji: discord.PartialEmoji, user_id: int) -> None:
    try:
        ch = guild.get_channel(channel_id) or await guild.fetch_channel(channel_id)
        msg = await ch.fetch_message(message_id)
        u = guild.get_member(user_id) or await guild.fetch_member(user_id)
        await msg.remove_reaction(emoji, u)
    except Exception:
        pass

async def download_to_discord_file(session: aiohttp.ClientSession, url: str, filename: str) -> Optional[discord.File]:
    try:
        async with session.get(url) as resp:
            if resp.status != 200:
                return None
            data = await resp.read()
        bio = io.BytesIO(data)
        bio.seek(0)
        return discord.File(bio, filename=filename)
    except Exception:
        return None

AUDIT_CHANNEL_ID = int(CONFIG.get("audit_channel_id", 0))

async def audit_log(guild: discord.Guild, text: str):
    if not AUDIT_CHANNEL_ID:
        return
    ch = guild.get_channel(AUDIT_CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        try:
            await ch.send(text)
        except Exception:
            pass

def set_panic_mode(enabled: bool):
    set_setting("panic_mode", "1" if enabled else "0")

def is_panic_mode() -> bool:
    return get_setting("panic_mode") == "1"

def build_help_embed() -> discord.Embed:
    e = discord.Embed(
        title="📌 Whatnot Claims Bot — Command Guide",
        description=(
            "🔒 **Staff-only commands require a Staff role**\n"
            "🎯 **Users claim cards by reacting to listings** (no commands)\n\n"
            "⚠️ Commands marked **CONFIRM** require typing the command twice to proceed."
        ),
        color=0x000000
    )

    e.add_field(
        name="🩺 System / Show Control",
        value=(
            "`!health` — bot uptime + system status\n"
            "`!newshow` — start a new show (**CONFIRM**)\n"
            "`!endshow` — end show, auto-sort, lock channels (**CONFIRM**)\n"
            "`!sort` — rebuild claims thread grouped by user (RAW re-attached)\n"
            "`!export` — export current show claims (CSV)\n"
            "`!export all` — export all claims across shows"
        ),
        inline=False
    )

    e.add_field(
        name="🚨 Emergency Controls",
        value=(
            "`!panic` — pause claims + lock selection channels (**CONFIRM**)\n"
            "`!unpanic` — resume claims + unlock selection channels\n"
            "`!panicstatus` — show panic state + who/when toggled"
        ),
        inline=False
    )

    e.add_field(
        name="📤 Uploading Cards",
        value=(
            "`!upload N | NSFW` — upload NSFW cards\n"
            "`!upload S | SFW` — upload SFW cards\n"
            "`!upload all` — upload both categories\n"
            "`!upload N 12` — upload only Item N012\n"
            "`!upload S007` — upload only Item S007\n\n"
            "📁 Upload threads required:\n"
            f"• {THREAD_RAW_N} / {THREAD_WM_N}\n"
            f"• {THREAD_RAW_S} / {THREAD_WM_S}"
        ),
        inline=False
    )

    e.add_field(
        name="🏆 Winners (Discord Users)",
        value=(
            "`!winner @user <amount> <reason>` — assign claim picks\n"
            "`!status @user` — show picks remaining + reason\n"
            "`!random @user N|S` — randomly assign remaining card (consumes pick)"
        ),
        inline=False
    )

    e.add_field(
        name="👤 Guests (Not in Discord)",
        value=(
            "`!guestwinner \"name\" <amount> <reason>` — assign guest picks\n"
            "`!gueststatus \"name\"` — show guest picks remaining\n"
            "`!guestclaim \"name\" N042` — claim specific item for guest\n"
            "`!randomguest \"name\" N|S` — random remaining card for guest"
        ),
        inline=False
    )

    e.add_field(
        name="🔄 Corrections / Admin Fixes",
        value=(
            "`!swap @user N012 N019` — swap claimed cards (same category)\n"
            "`!unassign @user N012` — remove claim, refund 1 pick, repost listing\n"
            "`!wipe` — reset all data (disabled during active show) (**CONFIRM**)"
        ),
        inline=False
    )

    e.add_field(
        name="🎯 Claiming Cards (Users)",
        value=(
            "Users must have a **Winner X** role.\n"
            "React to a card listing with any approved emoji:\n"
            f"{', '.join(sorted(CLAIM_EMOJIS))}\n\n"
            "On claim:\n"
            "• Listing is deleted\n"
            "• RAW image is attached in claims thread\n"
            "• Winner picks decrement\n"
            "• User receives DM confirmation"
        ),
        inline=False
    )

    e.add_field(
        name="🧠 Important Notes",
        value=(
            "• `!wipe` is blocked during active shows\n"
            "• `!panic` pauses claims without ending the show\n"
            "• `!sort` re-posts claims (timestamps preserved in text)\n"
            "• Claims thread is the source of truth"
        ),
        inline=False
    )

    return e

HELP_CHANNEL_ID = int(CONFIG.get("help_channel_id", 0))
HELP_AUTO_PIN = bool(CONFIG.get("help_auto_pin", True))

async def ensure_help_message(guild: discord.Guild):
    """Create or update the single official help embed message."""
    if not HELP_CHANNEL_ID:
        return

    ch = guild.get_channel(HELP_CHANNEL_ID)
    if not isinstance(ch, discord.TextChannel):
        return

    embed = build_help_embed()

    # Stored in DB so it persists across restarts
    msg_id_str = get_setting("help_message_id")
    msg = None

    if msg_id_str and msg_id_str.isdigit():
        try:
            msg = await ch.fetch_message(int(msg_id_str))
        except Exception:
            msg = None

    if msg:
        try:
            await msg.edit(embed=embed)
            if HELP_AUTO_PIN and not msg.pinned:
                await msg.pin(reason="Auto-pin help message")
        except Exception:
            pass
    else:
        try:
            new_msg = await ch.send(embed=embed)
            set_setting("help_message_id", str(new_msg.id))
            if HELP_AUTO_PIN:
                await new_msg.pin(reason="Auto-pin help message")
        except Exception:
            pass

def set_panic_meta(enabled: bool, actor: str):
    set_setting("panic_mode", "1" if enabled else "0")
    set_setting("panic_actor", actor)
    set_setting("panic_at", now_utc_iso())

def get_panic_meta() -> Tuple[bool, str, str]:
    enabled = get_setting("panic_mode") == "1"
    actor = get_setting("panic_actor") or "Unknown"
    at = get_setting("panic_at") or "Unknown"
    return enabled, actor, at

# ---------------------------
# Bot setup
# ---------------------------
intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)
BOT_START_TIME = time.time()
http_session: Optional[aiohttp.ClientSession] = None

@bot.event
async def on_ready():
    print(f"Logged in as {bot.user} ({bot.user.id})")

    for g in bot.guilds:
        try:
            await ensure_help_message(g)
        except Exception:
            pass


# ---------------------------
# Upload thread discovery
# ---------------------------
async def find_thread_by_name(parent: discord.TextChannel, name: str) -> Optional[discord.Thread]:
    for t in parent.threads:
        if t.name == name:
            return t
    try:
        async for t in parent.archived_threads(limit=50):
            if t.name == name:
                return t
    except Exception:
        pass
    return None

async def collect_attachments_from_thread(thread: discord.Thread, limit: int) -> Dict[int, discord.Attachment]:
    found: Dict[int, discord.Attachment] = {}
    async for msg in thread.history(limit=limit, oldest_first=True):
        for att in msg.attachments:
            n = parse_item_number(att.filename)
            if not n:
                continue
            found[n] = att
    return found

def parse_upload_scope(arg: Optional[str]) -> str:
    if not arg:
        return "ALL"
    a = arg.strip().lower()
    if a in {"all"}:
        return "ALL"
    if a in {"n", "nsfw"}:
        return "N"
    if a in {"s", "sfw"}:
        return "S"
    return "ALL"

# ---------------------------
# Sorting helpers
# ---------------------------
def is_bot_claim_message(msg: discord.Message) -> bool:
    return msg.author.bot and msg.content.startswith("✅ **Card Claimed")

# ---------------------------
# Internal operations
# ---------------------------
async def repost_listing(guild: discord.Guild, item_code: str) -> bool:
    item = get_item(item_code)
    if not item:
        return False
    item_code_db, cat, num, wm_fn, wm_url, raw_fn, raw_url, old_msg_id = item
    selection_ch = guild.get_channel(selection_channel_id_for(cat))
    if not isinstance(selection_ch, discord.TextChannel):
        return False
    embed = discord.Embed(description=f"Item {item_code_db}", color=0x000000)
    embed.set_image(url=wm_url)
    sent = await selection_ch.send(embed=embed)
    update_item_selection_message_id(item_code_db, sent.id)
    return True

# ---------------------------
# Commands
# ---------------------------
@bot.command(name="helprefresh", aliases=["refreshhelp"])
@commands.guild_only()
async def helprefresh_cmd(ctx: commands.Context):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    await ensure_help_message(ctx.guild)
    await ctx.send("✅ Help message refreshed.")

@bot.command(name="bothelp", aliases=["helpbot", "commands"])
@commands.guild_only()
async def bothelp_cmd(ctx: commands.Context):
    await ctx.send(embed=build_help_embed())

    # Public-safe help (does not reveal IDs or token info)
    e = discord.Embed(
        title="📌 Whatnot Claims Bot — Commands",
        description="Staff commands require a Staff role. Users claim by reacting to card posts.",
        color=0x000000
    )

    e.add_field(
        name="🩺 System / Show",
        value=(
            "`!health` — bot status + active show info\n"
            "`!newshow` — start new show (⚠️ confirm required)\n"
            "`!endshow` — end show + auto-sort + lock channels (⚠️ confirm required)\n"
            "`!sort` — rebuild claims thread grouped by user (RAW re-attached)\n"
            "`!export` — export current show claims CSV\n"
            "`!export all` — export all claims CSV"
        ),
        inline=False
    )

    e.add_field(
        name="📤 Uploads",
        value=(
            "`!upload N|NSFW` — upload NSFW WM listings\n"
            "`!upload S|SFW` — upload SFW WM listings\n"
            "`!upload all` — upload both categories\n"
            "`!upload N 12` — upload only Item N012\n"
            "`!upload S007` — upload only Item S007"
        ),
        inline=False
    )

    e.add_field(
        name="🏆 Winners (Discord users)",
        value=(
            "`!winner @user <amount> <reason>` — add picks (adds to existing)\n"
            "`!status @user` — show picks remaining + reason\n"
            "`!random @user N|S` — assign a random remaining card (consumes pick)"
        ),
        inline=False
    )

    e.add_field(
        name="👤 Guests (not in Discord)",
        value=(
            "`!guestwinner \"name\" <amount> <reason>` — add picks to guest\n"
            "`!gueststatus \"name\"` — show guest picks remaining\n"
            "`!guestclaim \"name\" N042` — claim specific item for guest\n"
            "`!randomguest \"name\" N|S` — random remaining item for guest (consumes pick)"
        ),
        inline=False
    )

    e.add_field(
        name="🔄 Fixes",
        value=(
            "`!swap @user N012 N019` — swap claimed card (no pick change)\n"
            "`!unassign @user N012` — remove claim + refund 1 pick + repost listing"
        ),
        inline=False
    )

    e.add_field(
        name="✅ User Claiming",
        value=(
            "Users claim by reacting to a card listing with an allowed emoji.\n"
            f"Allowed: {', '.join(sorted(CLAIM_EMOJIS))}\n"
            "Bot deletes the listing after claim and posts the claim + RAW attachment in the claims thread."
        ),
        inline=False
    )

    await ctx.send(embed=e)

@bot.command(name="panic")
@commands.guild_only()
async def panic_cmd(ctx: commands.Context, confirm: Optional[str] = None):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    if (confirm or "").upper() != "CONFIRM":
        if _needs_confirm(ctx.guild.id, ctx.author.id, "panic"):
            return await ctx.send(
                "🚨 **Confirm PANIC MODE**\n"
                "- Claims will be **paused** (reactions won’t claim).\n"
                "- Selection channels will be **locked** (no reactions/messages).\n"
                f"Type: `!panic CONFIRM` within {CONFIRM_WINDOW_SECONDS} seconds to proceed."
            )

    set_panic_meta(True, str(ctx.author))

    # Lock selection channels (stop new claims + reduce chaos)
    locked = []
    for ch_id in (NSFW_SELECTION_CHANNEL_ID, SFW_SELECTION_CHANNEL_ID):
        ch = ctx.guild.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                ow = ch.overwrites_for(ctx.guild.default_role)
                ow.send_messages = False
                ow.add_reactions = False
                await ch.set_permissions(ctx.guild.default_role, overwrite=ow, reason="PANIC MODE ON")
                locked.append(ch.mention)
            except discord.Forbidden:
                pass

    msg = (
        f"🚨 **PANIC MODE ENABLED** by {ctx.author.mention}\n"
        f"- Claims paused ✅\n"
        f"- Locked: {' '.join(locked) if locked else '(could not lock channels)'}\n"
        "Use `!unpanic` to resume."
    )
    await ctx.send(msg)

    # Post in active claims thread (do NOT lock/archive)
    tid = get_active_thread_id()
    if tid:
        try:
            thread = ctx.guild.get_thread(tid) or await ctx.guild.fetch_channel(tid)
            await thread.send(msg)
        except Exception:
            pass

    await audit_log(ctx.guild, f"🚨 PANIC ON by {ctx.author} in #{ctx.channel} at {now_utc_iso()} UTC")

@bot.command(name="panicstatus", aliases=["ps"])
@commands.guild_only()
async def panicstatus_cmd(ctx: commands.Context):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    enabled, actor, at = get_panic_meta()
    status = "🚨 ON" if enabled else "🟢 OFF"

    await ctx.send(
        f"**Panic Mode:** {status}\n"
        f"- Last toggled by: `{actor}`\n"
        f"- Last toggled at (UTC): `{at}`"
    )


@bot.command(name="unpanic")
@commands.guild_only()
async def unpanic_cmd(ctx: commands.Context):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    set_panic_meta(False, str(ctx.author))

    # Unlock selection channels by resetting overrides back to default
    unlocked = []
    for ch_id in (NSFW_SELECTION_CHANNEL_ID, SFW_SELECTION_CHANNEL_ID):
        ch = ctx.guild.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                ow = ch.overwrites_for(ctx.guild.default_role)
                ow.send_messages = None
                ow.add_reactions = None
                await ch.set_permissions(ctx.guild.default_role, overwrite=ow, reason="PANIC MODE OFF")
                unlocked.append(ch.mention)
            except discord.Forbidden:
                pass

    msg = (
        f"🟢 **PANIC MODE DISABLED** by {ctx.author.mention}\n"
        f"- Claims resumed ✅\n"
        f"- Unlocked: {' '.join(unlocked) if unlocked else '(could not unlock channels)'}"
    )
    await ctx.send(msg)

    tid = get_active_thread_id()
    if tid:
        try:
            thread = ctx.guild.get_thread(tid) or await ctx.guild.fetch_channel(tid)
            await thread.send(msg)
        except Exception:
            pass

    await audit_log(ctx.guild, f"🟢 PANIC OFF by {ctx.author} in #{ctx.channel} at {now_utc_iso()} UTC")

@bot.command(name="health")
@commands.guild_only()
async def health_cmd(ctx: commands.Context):
    uptime = int(time.time() - BOT_START_TIME)
    h, rem = divmod(uptime, 3600)
    m, s = divmod(rem, 60)
    tid = get_active_thread_id()
    await ctx.send(
        "🫀 **Health Check**\n"
        f"- Uptime: {h}h {m}m {s}s\n"
        f"- Active claims thread: {tid if tid else 'None'}\n"
        f"- Items mapped: {conn.execute('SELECT COUNT(*) FROM items').fetchone()[0]}\n"
        f"- Active winners: {conn.execute('SELECT COUNT(*) FROM winners').fetchone()[0]}\n"
        f"- Active guests: {conn.execute('SELECT COUNT(*) FROM guest_winners').fetchone()[0]}\n"
        f"- Claims in DB: {conn.execute('SELECT COUNT(*) FROM claims').fetchone()[0]}"
    )

@bot.command(name="wipe")
@commands.guild_only()
async def wipe_cmd(ctx: commands.Context, confirm: Optional[str] = None):   
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
   
    if get_active_thread_id():
        return await ctx.send("❌ Wipe is disabled while a show is active. Use `!endshow` first.")

    if (confirm or "").upper() != "CONFIRM":
        if _needs_confirm(ctx.guild.id, ctx.author.id, "wipe"):
            return await ctx.send(
                "⚠️ **Confirm Wipe**\n"
                "- This will clear items, winners, guests, claims, and active thread pointer.\n"
                f"Type: `!wipe CONFIRM` within {CONFIRM_WINDOW_SECONDS} seconds to proceed."
            )

    conn.execute("DELETE FROM items")
    conn.execute("DELETE FROM winners")
    conn.execute("DELETE FROM guest_winners")
    conn.execute("DELETE FROM claims")
    conn.execute("DELETE FROM settings")
    conn.commit()

    await ctx.send("✅ Wiped all stored show data.")

@bot.command(name="newshow")
@commands.guild_only()
async def newshow_cmd(ctx: commands.Context, confirm: Optional[str] = None):

    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
    
    if (confirm or "").upper() != "CONFIRM":
        if _needs_confirm(ctx.guild.id, ctx.author.id, "newshow"):
            return await ctx.send(
                "⚠️ **Confirm New Show**\n"
                "- This will **wipe** items, winners, guests, and claims.\n"
                f"Type: `!newshow CONFIRM` within {CONFIRM_WINDOW_SECONDS} seconds to proceed."
            )

    parent = ctx.guild.get_channel(CLAIMED_PARENT_CHANNEL_ID)
    if not isinstance(parent, discord.TextChannel):
        return await ctx.send("❌ claimed_parent_channel_id must be a normal text channel.")

    # wipe show state
    conn.execute("DELETE FROM items")
    conn.execute("DELETE FROM winners")
    conn.execute("DELETE FROM guest_winners")
    conn.execute("DELETE FROM claims")
    conn.execute("DELETE FROM settings")
    conn.commit()

    today_str = datetime.now().strftime("%Y-%m-%d")
    starter = await parent.send(f"📅 **{today_str} — Start of Claims**")
    thread = await starter.create_thread(name=f"{today_str} Claims", auto_archive_duration=1440)

    await thread.send("**start of claims**")
    set_setting("active_claims_thread_id", str(thread.id))

    await ctx.send(f"✅ New show started: {thread.mention}")

@bot.command(name="winner")
@commands.guild_only()
async def winner_cmd(ctx: commands.Context, member: discord.Member, amount: int, *, reason: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
    if amount <= 0:
        return await ctx.send("❌ amount must be positive.")

    st = get_winner_state(member.id)
    if st:
        _, old = st
        amount = old + amount

    await set_member_winner_remaining(member, reason, amount)
    await ctx.send(f"✅ {member.mention} now has **{amount}** picks. Reason: {reason}")

@bot.command(name="status")
@commands.guild_only()
async def status_cmd(ctx: commands.Context, member: discord.Member):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
    st = get_winner_state(member.id)
    if not st:
        return await ctx.send(f"ℹ️ {member.mention} has no active Winner tag.")
    reason, rem = st
    await ctx.send(f"✅ {member.mention} picks remaining: **{rem}** | Reason: {reason}")

@bot.command(name="guestwinner")
@commands.guild_only()
async def guestwinner_cmd(ctx: commands.Context, guest_tag: str, amount: int, *, reason: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
    if amount <= 0:
        return await ctx.send("❌ amount must be positive.")

    guest_tag = guest_tag.strip()
    st = get_guest_winner(guest_tag)
    if st:
        _, old = st
        amount = old + amount

    upsert_guest_winner(guest_tag, reason, amount)
    await ctx.send(f"✅ Guest **{guest_tag}** now has **{amount}** picks. Reason: {reason}")

@bot.command(name="gueststatus")
@commands.guild_only()
async def gueststatus_cmd(ctx: commands.Context, guest_tag: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")
    guest_tag = guest_tag.strip()
    st = get_guest_winner(guest_tag)
    if not st:
        return await ctx.send(f"ℹ️ Guest **{guest_tag}** has no active picks.")
    reason, rem = st
    await ctx.send(f"✅ Guest **{guest_tag}** picks remaining: **{rem}** | Reason: {reason}")

@bot.command(name="upload", aliases=["u"])
@commands.guild_only()
async def upload_cmd(ctx: commands.Context, scope: Optional[str] = None, item: Optional[str] = None):
    """
    Usage:
      !upload N
      !upload S
      !upload all
      !upload N 12
      !upload S 7
      !upload N042
      !upload S012
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    upload_parent = ctx.guild.get_channel(UPLOAD_PARENT_CHANNEL_ID)
    if not isinstance(upload_parent, discord.TextChannel):
        return await ctx.send("❌ upload_parent_channel_id must be a normal text channel with threads.")

    sc = parse_upload_scope(scope)

    t_raw_n = await find_thread_by_name(upload_parent, THREAD_RAW_N)
    t_wm_n = await find_thread_by_name(upload_parent, THREAD_WM_N)
    t_raw_s = await find_thread_by_name(upload_parent, THREAD_RAW_S)
    t_wm_s = await find_thread_by_name(upload_parent, THREAD_WM_S)

    if sc in {"N", "ALL"} and (t_raw_n is None or t_wm_n is None):
        return await ctx.send(f"❌ Missing NSFW threads: '{THREAD_RAW_N}' and/or '{THREAD_WM_N}'")
    if sc in {"S", "ALL"} and (t_raw_s is None or t_wm_s is None):
        return await ctx.send(f"❌ Missing SFW threads: '{THREAD_RAW_S}' and/or '{THREAD_WM_S}'")

    item_code_filter: Optional[str] = None
    item_num_filter: Optional[int] = None

    if item:
        ic = parse_item_code(item)
        if ic:
            item_code_filter = ic
        elif item.strip().isdigit():
            item_num_filter = int(item.strip())

    if scope:
        ic = parse_item_code(scope)
        if ic:
            item_code_filter = ic
            sc = "ALL"

    async def do_upload_category(category: str, t_raw: discord.Thread, t_wm: discord.Thread) -> int:
        print(f"[UPLOAD] {category}: starting upload scan")
        raw_map = await collect_attachments_from_thread(t_raw, UPLOAD_HISTORY_LIMIT)
        print(f"[UPLOAD] {category}: RAW mapped -> {len(raw_map)} items")
        wm_map = await collect_attachments_from_thread(t_wm, UPLOAD_HISTORY_LIMIT)
        print(f"[UPLOAD] {category}: WM mapped -> {len(wm_map)} items")
        nums = sorted(set(raw_map.keys()) & set(wm_map.keys()))
        print(f"[UPLOAD] {category}: matched items -> {len(nums)}")
        if not nums:
            return 0

        target_nums = nums
        if item_code_filter:
            if item_code_filter[0] != category:
                return 0
            n = int(item_code_filter[1:])
            target_nums = [n] if n in nums else []
        elif item_num_filter is not None:
            target_nums = [item_num_filter] if item_num_filter in nums else []

        if not target_nums:
            return 0

        selection_ch = ctx.guild.get_channel(selection_channel_id_for(category))
        if not isinstance(selection_ch, discord.TextChannel):
            raise RuntimeError("Selection channel not found / not a text channel.")

        posted = 0
        print(f"[UPLOAD] {category}: posting items…")
        for n in target_nums:
            item_code = make_item_code(category, n)
            raw_att = raw_map[n]
            wm_att = wm_map[n]

            embed = discord.Embed(description=f"Item #{item_code}", color=0x000000)
            embed.set_image(url=wm_att.url)

            sent = await selection_ch.send(embed=embed)

            upsert_item(
                item_code=item_code,
                category=category,
                number=n,
                wm_filename=wm_att.filename,
                wm_url=wm_att.url,
                raw_filename=raw_att.filename,
                raw_url=raw_att.url,
                selection_message_id=sent.id
            )

            posted += 1
            print(f"[UPLOAD] {category}: posted {posted}/{len(target_nums)} (Item {make_item_code(category, n)})")
            await asyncio.sleep(UPLOAD_SEND_DELAY)

        return posted

    total = 0
    try:
        if sc in {"N", "ALL"} and t_raw_n and t_wm_n:
            total += await do_upload_category("N", t_raw_n, t_wm_n)
        if sc in {"S", "ALL"} and t_raw_s and t_wm_s:
            total += await do_upload_category("S", t_raw_s, t_wm_s)
    except Exception as e:
        return await ctx.send(f"❌ Upload failed: {type(e).__name__}: {e}")

    await ctx.send(f"✅ Uploaded {total} item(s).")

@bot.command(name="assign")
@commands.guild_only()
async def assign_cmd(ctx: commands.Context, member: discord.Member, item_code: str, *, reason_override: Optional[str] = None):
    """
    Staff-only: claim an item for a Discord user (no reaction required).
    Usage:
      !assign @user N042
      !assign @user S012 optional reason override
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active claims thread. Run `!newshow` first.")

    item_code = parse_item_code(item_code) or item_code.strip().upper()
    if not item_code or item_code[0] not in ("N", "S"):
        return await ctx.send("❌ Invalid item code. Example: `N042` or `S012`.")

    # Winner check
    st = get_winner_state(member.id)  # expected (reason, remaining)
    if not st:
        return await ctx.send("❌ That user is not a winner / has no picks.")
    reason_live, remaining = st[0], int(st[1])
    if remaining <= 0:
        return await ctx.send("❌ That user has 0 picks remaining.")

    reason = reason_override.strip() if reason_override else reason_live

    # Item lookup
    row = conn.execute(
        "SELECT category, number, wm_filename, raw_filename, selection_message_id FROM items WHERE item_code=?",
        (item_code,)
    ).fetchone()
    if not row:
        return await ctx.send("❌ Item not found in DB. Did you `!upload` this show?")
    category, number, wm_filename, raw_filename, selection_message_id = row


    # Already claimed?
    exists = conn.execute(
        "SELECT 1 FROM claims WHERE thread_id=? AND item_code=? LIMIT 1",
        (tid, item_code)
    ).fetchone()
    if exists:
        return await ctx.send("❌ That item is already claimed.")

    # Delete listing from selection channel (best effort)
    try:
        sel_ch = ctx.guild.get_channel(selection_channel_id_for(category))
        if isinstance(sel_ch, discord.TextChannel) and selection_message_id:
            try:
                msg = await sel_ch.fetch_message(int(selection_message_id))
                await msg.delete()
            except discord.NotFound:
                pass
    except Exception:
        pass

    # Insert claim row
    claimed_at = add_claim(
        guild_id=ctx.guild.id,
        thread_id=tid,
        user_id=member.id,
        user_tag=str(member),
        reason=reason,
        category=category,
        item_code=item_code,
        item_number=int(number),
        wm_filename=wm_filename,
        raw_filename=raw_filename
    )

    # Decrement picks (DB) — mirrors the common pattern used elsewhere
    conn.execute("UPDATE winners SET remaining = remaining - 1 WHERE user_id=?", (member.id,))
    conn.execute("DELETE FROM winners WHERE user_id=? AND remaining <= 0", (member.id,))
    conn.commit()
    # Sync Winner role ladder to remaining picks (safe, mirrors existing behavior)
    try:
        row2 = conn.execute(
            "SELECT remaining FROM winners WHERE user_id=?",
            (member.id,)
        ).fetchone()

        current = get_current_winner_role(member)
        if current:
            old_role, _ = current
            try:
                await member.remove_roles(old_role, reason="Winner picks updated (assign)")
            except discord.Forbidden:
                pass

        if row2 and row2[0] > 0:
            new_remaining = int(row2[0])
            new_role = await ensure_winner_role(ctx.guild, new_remaining)
            try:
                await member.add_roles(new_role, reason="Winner picks updated (assign)")
            except discord.Forbidden:
                pass
    except Exception:
        pass

    # OPTIONAL: if you already have a working role-update helper, call it here.
    # Example:
    # await update_winner_role_from_db(member)
    # (Leave this commented unless you tell me the exact helper name you already use.)

    # Post to claims thread (nickname/display name)
    try:
        thread = ctx.guild.get_thread(tid) or await ctx.guild.fetch_channel(tid)
    except Exception:
        return await ctx.send("✅ Assigned in DB, but I couldn't fetch the claims thread to post the log.")

    raw_url = get_raw_url_for_item(item_code)
    display2 = member.display_name

    text = (
        "✅ **Card Claimed (Staff Assign)**\n"
        f"- **User:** `{display2}`\n"
        f"- **Category:** {category_emoji(category)} {category_name(category)}\n"
        f"- **Reason:** {reason}\n"
        f"- **Item:** Item #{item_code}\n"
        f"- **WM Filename:** {wm_filename}\n"
        f"- **RAW Filename:** {raw_filename}\n"
        f"- **Timestamp (UTC):** {claimed_at}\n"
        f"- **RAW Link:** {raw_url if raw_url else '(missing)'}"
    )
    await thread.send(text)

    # DM user (best effort)
    try:
        await member.send(
            f"✅ Staff recorded your claim.\n"
            f"- Item: Item #{item_code}\n"
            f"- Category: {category_name(category)}\n"
            f"- Reason: {reason}\n"
            f"- RAW Link: {raw_url if raw_url else '(missing)'}"
        )
    except Exception:
        pass
    # Confirm remaining from DB
    st2 = get_winner_state(member.id)
    remaining2 = int(st2[1]) if st2 else 0
    await ctx.send(f"✅ Assigned **Item #{item_code}** to {member.mention}. Picks remaining: **{remaining2}**")


@bot.command(name="random")
@commands.guild_only()
async def random_cmd(ctx: commands.Context, member: discord.Member, category: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    cat = category.strip().upper()
    if cat not in {"N", "S"}:
        return await ctx.send("❌ Category must be N or S.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active show thread. Run !newshow first.")

    st = get_winner_state(member.id)
    if not st:
        return await ctx.send("❌ User is not an active winner.")
    reason, remaining = st
    if remaining <= 0:
        return await ctx.send("❌ User has 0 picks remaining.")

    unclaimed = list_unclaimed_items(tid, cat)
    if not unclaimed:
        return await ctx.send(f"❌ No remaining {category_name(cat)} items to assign.")

    pick = random.choice(unclaimed)
    await force_claim_discord(ctx.guild, member, pick, reason, used_emoji="🎲", decrement_pick=True, method="random")
    await ctx.send(f"✅ Random assigned **Item {pick}** to {member.mention}.")

@bot.command(name="randomguest")
@commands.guild_only()
async def randomguest_cmd(ctx: commands.Context, guest_tag: str, category: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    guest_tag = guest_tag.strip()
    cat = category.strip().upper()
    if cat not in {"N", "S"}:
        return await ctx.send("❌ Category must be N or S.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active show thread. Run !newshow first.")

    st = get_guest_winner(guest_tag)
    if not st:
        return await ctx.send("❌ Guest has no picks. Use !guestwinner first.")
    reason, remaining = st
    if remaining <= 0:
        return await ctx.send("❌ Guest has 0 picks remaining.")

    unclaimed = list_unclaimed_items(tid, cat)
    if not unclaimed:
        return await ctx.send(f"❌ No remaining {category_name(cat)} items to assign.")

    pick = random.choice(unclaimed)
    await force_claim_guest(ctx.guild, guest_tag, pick, reason, used_emoji="🎲", decrement_pick=True, method="randomguest")
    await ctx.send(f"✅ Random assigned **Item {pick}** to guest **{guest_tag}**.")

@bot.command(name="guestclaim")
@commands.guild_only()
async def guestclaim_cmd(ctx: commands.Context, guest_tag: str, item_code: str):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active show thread. Run !newshow first.")

    guest_tag = guest_tag.strip()
    ic = parse_item_code(item_code)
    if not ic:
        return await ctx.send("❌ Item must look like N042 or S012.")

    st = get_guest_winner(guest_tag)
    if not st:
        return await ctx.send("❌ Guest has no picks. Use !guestwinner first.")
    reason, remaining = st
    if remaining <= 0:
        return await ctx.send("❌ Guest has 0 picks remaining.")

    if is_item_claimed(ic, tid):
        return await ctx.send("❌ That item is already claimed.")
    if not get_item(ic):
        return await ctx.send("❌ Item not found (did you run !upload?).")

    await force_claim_guest(ctx.guild, guest_tag, ic, reason, used_emoji="🧾", decrement_pick=True, method="guestclaim")
    await ctx.send(f"✅ Guest **{guest_tag}** claimed **Item {ic}**. Picks left: **{remaining - 1}**")

@bot.command(name="unassign")
@commands.guild_only()
async def unassign_cmd(ctx: commands.Context, member: discord.Member, item_code: str):
    """
    Removes that claim for this discord member, gives +1 pick back, reposts listing.
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active show thread.")

    ic = parse_item_code(item_code)
    if not ic:
        return await ctx.send("❌ Item must look like N042 or S012.")

    claim = get_claim_for_item(tid, ic)
    if not claim:
        return await ctx.send("❌ That item is not claimed in the current show.")

    claim_id, claimed_at, user_id, user_tag, reason, cat, item_code_db, item_number, wm_fn, raw_fn = claim
    if user_id is None or int(user_id) != member.id:
        return await ctx.send(f"❌ That claim belongs to `{user_tag}` not {member.mention}.")
    
    thread_id = get_active_thread_id()
    if thread_id:
     deleted = await delete_claim_post_from_thread(
        ctx.guild,
        thread_id=thread_id,
        user_id=member.id,
        user_tag=str(member),
        item_code=item_code
    )
    else:
       deleted = False

    delete_claim_by_id(claim_id)

    st = get_winner_state(member.id)
    if st:
        reason_live, remaining = st
        await set_member_winner_remaining(member, reason_live, remaining + 1)
    else:
        await set_member_winner_remaining(member, reason, 1)

    ok = await repost_listing(ctx.guild, ic)
    await ctx.send(f"✅ Unassigned Item {ic} from {member.mention}. Listing reposted={ok}.")

@bot.command(name="swap")
@commands.guild_only()
async def swap_cmd(ctx: commands.Context, member: discord.Member, old_item: str, new_item: str):
    """
    Swaps member's claimed old_item for unclaimed new_item. No pick change.
    Must be within same category (both N or both S).
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active show thread.")

    old_ic = parse_item_code(old_item)
    new_ic = parse_item_code(new_item)
    if not old_ic or not new_ic:
        return await ctx.send("❌ Items must look like N042 / S012.")
    if old_ic[0] != new_ic[0]:
        return await ctx.send("❌ Swap must be within the same category (both N or both S).")

    old_claim = get_claim_for_item(tid, old_ic)
    if not old_claim:
        return await ctx.send("❌ Old item is not claimed in current show.")
    old_claim_id, old_claimed_at, old_user_id, old_user_tag, old_reason, old_cat, *_ = old_claim
    if old_user_id is None or int(old_user_id) != member.id:
        return await ctx.send(f"❌ Old item is claimed by `{old_user_tag}`, not {member.mention}.")

    if is_item_claimed(new_ic, tid):
        return await ctx.send("❌ New item is already claimed.")
    if not get_item(new_ic):
        return await ctx.send("❌ New item not found (did you upload it?).")

    delete_claim_by_id(old_claim_id)
    await repost_listing(ctx.guild, old_ic)

    # claim new item for same user; do NOT decrement pick
    await force_claim_discord(ctx.guild, member, new_ic, old_reason, used_emoji="🔁", decrement_pick=False, method="swap")

    await ctx.send(f"✅ Swapped {member.mention}: **{old_ic} → {new_ic}**")

@bot.command(name="export")
@commands.guild_only()
async def export_cmd(ctx: commands.Context, mode: str = ""):
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    mode = (mode or "").lower().strip()
    active_tid = get_active_thread_id()
    if mode != "all" and not active_tid:
        return await ctx.send("❌ No active show thread. Run `!newshow` first or use `!export all`.")

    if mode == "all":
        rows = conn.execute(
            "SELECT claimed_at, user_id, user_tag, reason, category, item_code, item_number, raw_filename, wm_filename, thread_id "
            "FROM claims ORDER BY id ASC"
        ).fetchall()
        scope = "ALL"
    else:
        rows = conn.execute(
            "SELECT claimed_at, user_id, user_tag, reason, category, item_code, item_number, raw_filename, wm_filename, thread_id "
            "FROM claims WHERE thread_id=? ORDER BY id ASC",
            (active_tid,)
        ).fetchall()
        scope = f"thread_id={active_tid}"

    out = io.StringIO()
    w = csv.writer(out)
    w.writerow(["claimed_at_utc", "user_id(blank=guest)", "user_tag", "reason", "category", "item_code", "item_number", "raw_filename", "wm_filename", "thread_id"])
    for r in rows:
        w.writerow(list(r))

    data = out.getvalue().encode("utf-8")
    fp = io.BytesIO(data)
    fp.seek(0)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    await ctx.send(f"✅ Export ready ({scope}).", file=discord.File(fp, f"claimed_cards_{ts}_utc.csv"))

@bot.command(name="sort")
@commands.guild_only()
async def sort_cmd(ctx: commands.Context):
    """
    Deletes the bot's claim messages in the active claims thread and reposts grouped by user.
    Re-attaches RAW image per claim (fallback to RAW link if attachment fails).
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    if not tid:
        return await ctx.send("❌ No active claims thread. Run `!newshow` first.")

    try:
        thread = ctx.guild.get_thread(tid) or await ctx.guild.fetch_channel(tid)
    except Exception:
        return await ctx.send("❌ Could not fetch the active claims thread.")

    # Delete bot claim messages
    deleted = 0
    try:
        async for msg in thread.history(limit=None, oldest_first=False):
            if is_bot_claim_message(msg):
                try:
                    await msg.delete()
                    deleted += 1
                    await asyncio.sleep(0.35)
                except (discord.Forbidden, discord.NotFound, discord.HTTPException):
                    pass
    except discord.Forbidden:
        return await ctx.send("❌ I can’t read thread history here (need Read Message History).")

    claims = get_claims_for_thread(tid)
    if not claims:
        return await ctx.send(f"✅ Sorted (deleted {deleted} claim posts). No claims to repost.")
    
    print(f"[SORT] claims fetched for thread {tid}: {len(claims)}")

    grouped: Dict[int, List[tuple]] = {}
    user_name_map: Dict[int, str] = {}

    for row in claims:
        user_tag, user_id, category, item_code, item_number, wm_filename, raw_filename, claimed_at, reason = row
        uid = int(user_id)

    # uid=0 indicates guest in your query
    if uid != 0:
        m = ctx.guild.get_member(uid)
        display = m.display_name if m else user_tag  # nickname/display if possible
    else:
        display = user_tag  # guest name string

    user_name_map[uid] = display
    grouped.setdefault(uid, []).append(row)

    global http_session
    if http_session is None:
        http_session = aiohttp.ClientSession()

    await thread.send("🗂️ **Sorted Claim Log (rebuilt by bot)**\n*(RAW images re-attached per claim)*")

    rebuilt = 0

    for user_id, rows in grouped.items():
        display_name = user_name_map.get(user_id, "Unknown")

        # user_id=0 indicates guest in our query
        if user_id != 0:
            st = get_winner_state(user_id)
        else:
            st = None

        remaining = st[1] if st else 0
        reason_live = st[0] if st else None

        header = f"👤 **{display_name}** — Claimed: **{len(rows)}**"
        if remaining > 0:
            header += f" | Picks remaining: **{remaining}**"
            if reason_live:
                header += f" | Reason: {reason_live}"
        await thread.send(header)
        await asyncio.sleep(0.6)

        for r in rows:
            try:
                user_tag2, user_id2, category, item_code, item_number, wm_filename, raw_filename, claimed_at, reason = r

                # Ensure strings not None (prevents crashes)
                wm_filename = wm_filename or ""
                raw_filename = raw_filename or ""

                raw_url = get_raw_url_for_item(item_code)

                # Show nickname/display name in rebuilt entries
                uid2 = int(user_id2)
                if uid2 != 0:
                    m2 = ctx.guild.get_member(uid2)
                    display2 = m2.display_name if m2 else user_tag2
                else:
                    display2 = user_tag2

                text = (
                    "✅ **Card Claimed**\n"
                    f"- **User:** `{display2}`\n"
                    f"- **Category:** {category_emoji(category)} {category_name(category)}\n"
                    f"- **Reason:** {reason}\n"
                    f"- **Item:** Item #{item_code}\n"
                    f"- **WM Filename:** {wm_filename}\n"
                    f"- **RAW Filename:** {raw_filename}\n"
                    f"- **Timestamp (UTC):** {claimed_at}\n"
                    f"- **RAW Link:** {raw_url if raw_url else '(missing)'}"
                )

                await thread.send(text)
                rebuilt += 1
                await asyncio.sleep(1.1)

            except Exception as ex:
                print(f"[SORT] failed rebuilding claim row={r} -> {type(ex).__name__}: {ex}")
                # keep going
                continue

            # Send robustly: never let one failure stop the whole sort
            try:


                if raw_url:
                    await thread.send(text + f"\n- **RAW Link:** {raw_url}")
                else:
                    await thread.send(text + "\n- **RAW Link:** *(missing)*")

            except discord.Forbidden as ex:
                print(f"[SORT] Forbidden sending claim {item_code}: {ex}")
                try:
                    # At least log text without attachment
                    await thread.send(text + "\n⚠️ (Send failed due to missing permissions.)")
                except Exception:
                    pass
            except discord.HTTPException as ex:
                print(f"[SORT] HTTPException sending claim {item_code}: {ex}")
                try:
                    # Usually "file too large" or bad request
                    fallback = text
                    if raw_url:
                        fallback += f"\n⚠️ (Attachment failed; linking RAW)\n- **RAW Link:** {raw_url}"
                    await thread.send(fallback)
                except Exception:
                    pass
            except Exception as ex:
                print(f"[SORT] Unexpected error sending claim {item_code}: {ex}")
                try:
                    await thread.send(text + "\n⚠️ (Unexpected error posting this claim.)")
                except Exception:
                    pass

            rebuilt += 1
            await asyncio.sleep(1.1)


    # Users with picks remaining
    winners = conn.execute(
        "SELECT user_id, reason, remaining FROM winners WHERE remaining > 0 ORDER BY remaining DESC"
    ).fetchall()
    if winners:
        await thread.send("🏁 **Users with picks remaining**")
        for user_id, reason, remaining in winners:
            member = ctx.guild.get_member(user_id)
            name = member.mention if member else f"`{user_id}`"
            await thread.send(f"- {name}: **{remaining}** remaining | Reason: {reason}")
            await asyncio.sleep(0.4)

    guests = conn.execute(
        "SELECT guest_tag, reason, remaining FROM guest_winners WHERE remaining > 0 ORDER BY remaining DESC"
    ).fetchall()
    if guests:
        await thread.send("🏁 **Guests with picks remaining**")
        for guest_tag, reason, remaining in guests:
            await thread.send(f"- `{guest_tag}`: **{remaining}** remaining | Reason: {reason}")
            await asyncio.sleep(0.4)

    await ctx.send(f"✅ Sorted: deleted **{deleted}** and rebuilt **{rebuilt}** claim posts (RAW included).")

@bot.command(name="endshow")
@commands.guild_only()
async def endshow_cmd(ctx: commands.Context, confirm: Optional[str] = None):

    """
    Ends show:
    - auto sort (rebuild claim log w/ RAW)
    - lock BOTH selection channels (stop reactions/messages)
    - archive/lock claims thread
    - close claims (clears active thread pointer)
    """
    if not isinstance(ctx.author, discord.Member) or not is_staff(ctx.author):
        return await ctx.send("❌ You don't have permission.")

    tid = get_active_thread_id()
    thread = None
    if tid:
        try:
            thread = ctx.guild.get_thread(tid) or await ctx.guild.fetch_channel(tid)
        except Exception:
            thread = None

    if (confirm or "").upper() != "CONFIRM":
        if _needs_confirm(ctx.guild.id, ctx.author.id, "endshow"):
            return await ctx.send(
                "⚠️ **Confirm End Show**\n"
                "- This will **sort**, **lock selection channels**, and **archive/lock** the claims thread.\n"
                f"Type: `!endshow CONFIRM` within {CONFIRM_WINDOW_SECONDS} seconds to proceed."
            )

    for ch_id in (NSFW_SELECTION_CHANNEL_ID, SFW_SELECTION_CHANNEL_ID):
        ch = ctx.guild.get_channel(ch_id)
        if isinstance(ch, discord.TextChannel):
            try:
                ow = ch.overwrites_for(ctx.guild.default_role)
                ow.send_messages = False
                ow.add_reactions = False
                await ch.set_permissions(ctx.guild.default_role, overwrite=ow, reason="Show ended")
            except discord.Forbidden:
                pass

    if thread:
        try:
            await thread.send("🧾 **Show ended. Claims are now closed.**")
            await thread.edit(archived=True, locked=True)
        except discord.Forbidden:
            pass

    clear_setting("active_claims_thread_id")
    await ctx.send("✅ Show ended. Claims closed, thread archived, selection channels locked.")

# ---------------------------
# Force-claim functions used by random/swap/guestclaim
# ---------------------------
async def force_claim_discord(guild: discord.Guild, member: discord.Member, item_code: str, reason: str,
                              used_emoji: str, decrement_pick: bool, method: str) -> None:
    tid = get_active_thread_id()
    if not tid:
        return

    item = get_item(item_code)
    if not item:
        return

    item_code_db, cat, num, wm_fn, wm_url, raw_fn, raw_url, sel_msg_id = item

    # delete listing message
    if sel_msg_id:
        try:
            ch = guild.get_channel(selection_channel_id_for(cat)) or await guild.fetch_channel(selection_channel_id_for(cat))
            msg = await ch.fetch_message(sel_msg_id)
            await msg.delete()
        except Exception:
            pass
        update_item_selection_message_id(item_code_db, 0)

    claimed_at = add_claim(
        guild_id=guild.id,
        thread_id=tid,
        user_id=member.id,
        user_tag=str(member),
        reason=reason,
        category=cat,
        item_code=item_code_db,
        item_number=num,
        wm_filename=wm_fn,
        raw_filename=raw_fn
    )

    # decrement pick
    if decrement_pick:
        st = get_winner_state(member.id)
        if st:
            r, rem = st
            await set_member_winner_remaining(member, r, max(0, rem - 1))

    # post to thread with RAW
    try:
        thread = guild.get_thread(tid) or await guild.fetch_channel(tid)
    except Exception:
        thread = None

    file_obj = None
    global http_session
    if http_session and raw_url:
        file_obj = await download_to_discord_file(http_session, raw_url, raw_fn)

    if thread:
        text = (
            "✅ **Card Claimed**\n"
            f"- **User:** {member.mention} (`{member}`)\n"
            f"- **Category:** {category_emoji(cat)} {category_name(cat)}\n"
            f"- **Reason:** {reason}\n"
            f"- **Item:** {item_code_db}\n"
            f"- **Method:** {method} {used_emoji}\n"
            f"- **RAW Link:** {raw_url if raw_url else '(missing)'}"
        )
        try:
            if file_obj:
                await thread.send(text + f"\n- **RAW Link:** {raw_url}")
            else:
                await thread.send(text + (f"\n- **RAW Link:** {raw_url}" if raw_url else "\n- **RAW Link:** *(missing)*"))
        except Exception:
            pass

async def force_claim_guest(guild: discord.Guild, guest_tag: str, item_code: str, reason: str,
                            used_emoji: str, decrement_pick: bool, method: str) -> None:
    tid = get_active_thread_id()
    if not tid:
        return

    item = get_item(item_code)
    if not item:
        return

    item_code_db, cat, num, wm_fn, wm_url, raw_fn, raw_url, sel_msg_id = item

    # delete listing message
    if sel_msg_id:
        try:
            ch = guild.get_channel(selection_channel_id_for(cat)) or await guild.fetch_channel(selection_channel_id_for(cat))
            msg = await ch.fetch_message(sel_msg_id)
            await msg.delete()
        except Exception:
            pass
        update_item_selection_message_id(item_code_db, 0)

    claimed_at = add_claim(
        guild_id=guild.id,
        thread_id=tid,
        user_id=None,
        user_tag=guest_tag,
        reason=reason,
        category=cat,
        item_code=item_code_db,
        item_number=num,
        wm_filename=wm_fn,
        raw_filename=raw_fn
    )

    if decrement_pick:
        st = get_guest_winner(guest_tag)
        if st:
            r, rem = st
            upsert_guest_winner(guest_tag, r, max(0, rem - 1))

    # post to thread with RAW
    try:
        thread = guild.get_thread(tid) or await guild.fetch_channel(tid)
    except Exception:
        thread = None

    file_obj = None
    global http_session
    if http_session and raw_url:
        file_obj = await download_to_discord_file(http_session, raw_url, raw_fn)

    if thread:
        text = (
            "✅ **Card Claimed (Guest)**\n"
            f"- **Guest:** `{guest_tag}`\n"
            f"- **Category:** {category_emoji(cat)} {category_name(cat)}\n"
            f"- **Reason:** {reason}\n"
            f"- **Item:** {item_code_db}\n"
            f"- **Method:** {method} {used_emoji}\n"
            f"- **RAW Link:** {raw_url if raw_url else '(missing)'}"
        )
        try:
            if file_obj:
                await thread.send(text + f"\n- **RAW Link:** {raw_url}")
            else:
                await thread.send(text + (f"\n- **RAW Link:** {raw_url}" if raw_url else "\n- **RAW Link:** *(missing)*"))
        except Exception:
            pass

# ---------------------------
# Reaction claim handling (Discord users)
# ---------------------------
@bot.event
async def on_member_join(member: discord.Member):
    try:
        unverified = member.guild.get_role(UNVERIFIED_ROLE_ID)
        if unverified:
            await member.add_roles(unverified, reason="New member join gating")
    except discord.Forbidden:
        pass

    ch = member.guild.get_channel(VERIFY_CHANNEL_ID)
    if isinstance(ch, discord.TextChannel):
        try:
            await ch.send(
                f"👋 Welcome {member.mention}!\n"
                f"Please type your **Whatnot username** in this channel (or use `!verify YourWhatnotName`).\n"
                f"Once verified, you’ll get full access."
            )
        except discord.Forbidden:
            pass
@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    # Let commands still work
    await bot.process_commands(message)

    # Only act in the verify channel
    if message.guild is None or message.channel.id != VERIFY_CHANNEL_ID:
        return

    member = message.author
    if not isinstance(member, discord.Member):
        return

    # Only apply if they still have Unverified
    unverified = message.guild.get_role(UNVERIFIED_ROLE_ID)
    if unverified and unverified not in member.roles:
        return

    # Treat message content as name
    try:
        new_name = _clean_whatnot_name(message.content)
    except ValueError:
        return  # ignore junk

    # Set nickname + roles
    try:
        await member.edit(nick=new_name, reason="Whatnot verification nickname set")
    except discord.Forbidden:
        return await message.channel.send("❌ I can’t change your nickname (check bot role permissions).")

    verified = message.guild.get_role(VERIFIED_ROLE_ID)
    try:
        if unverified:
            await member.remove_roles(unverified, reason="Verified")
        if verified:
            await member.add_roles(verified, reason="Verified")
    except discord.Forbidden:
        pass

    await message.channel.send(f"✅ Verified! Welcome, **{new_name}**.")


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if not bot.user or payload.user_id == bot.user.id:
        return

    tid = get_active_thread_id()
    if not tid:
        return

    if str(payload.emoji) not in CLAIM_EMOJIS:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return
        # PANIC MODE: pause all claiming
    enabled, _actor, _at = get_panic_meta()
    if enabled:
        await remove_user_reaction_safe(guild, payload.channel_id, payload.message_id, payload.emoji, payload.user_id)
        return

    item = get_item_by_selection_message(payload.message_id)
    if not item:
        return

    item_code_db, cat, num, wm_fn, wm_url, raw_fn, raw_url, sel_msg_id = item

    st = get_winner_state(payload.user_id)
    if not st:
        await remove_user_reaction_safe(guild, payload.channel_id, payload.message_id, payload.emoji, payload.user_id)
        try:
            u = bot.get_user(payload.user_id) or await bot.fetch_user(payload.user_id)
            await dm_user_safe(u, "❌ You can’t claim cards right now (no active Winner tag).")
        except Exception:
            pass
        return

    reason, remaining = st
    if remaining <= 0:
        await remove_user_reaction_safe(guild, payload.channel_id, payload.message_id, payload.emoji, payload.user_id)
        try:
            u = bot.get_user(payload.user_id) or await bot.fetch_user(payload.user_id)
            await dm_user_safe(u, "❌ You don’t have any picks remaining.")
        except Exception:
            pass
        return

    if is_item_claimed(item_code_db, tid):
        await remove_user_reaction_safe(guild, payload.channel_id, payload.message_id, payload.emoji, payload.user_id)
        try:
            u = bot.get_user(payload.user_id) or await bot.fetch_user(payload.user_id)
            await dm_user_safe(u, f"❌ Item {item_code_db} was already claimed.")
        except Exception:
            pass
        return

    member = payload.member
    if member is None:
        try:
            member = await guild.fetch_member(payload.user_id)
        except Exception:
            return

    claimed_at = add_claim(
        guild_id=guild.id,
        thread_id=tid,
        user_id=member.id,
        user_tag=str(member),
        reason=reason,
        category=cat,
        item_code=item_code_db,
        item_number=num,
        wm_filename=wm_fn,
        raw_filename=raw_fn
    )

    new_remaining = remaining - 1
    await set_member_winner_remaining(member, reason, new_remaining)

    # --- Post to active show claims thread (link-only) ---
    try:
        thread = guild.get_thread(tid) or await guild.fetch_channel(tid)
    except Exception as ex:
        print(f"[CLAIM] thread fetch failed: {type(ex).__name__}: {ex}")
        thread = None

    # Use the item info we already have from get_item_by_selection_message()
    category = cat
    item_code = item_code_db
    wm_filename = wm_fn
    raw_filename = raw_fn
    raw_url2 = raw_url  # from item tuple (may already be present)
    if not raw_url2:
        raw_url2 = get_raw_url_for_item(item_code)

    # Display name (nickname/server display)
    display2 = member.display_name if member else str(payload.user_id)

    text = (
        "✅ **Card Claimed**\n"
        f"- **User:** `{display2}`\n"
        f"- **Category:** {category_emoji(category)} {category_name(category)}\n"
        f"- **Reason:** {reason}\n"
        f"- **Item:** Item #{item_code}\n"
        f"- **WM Filename:** {wm_filename}\n"
        f"- **RAW Filename:** {raw_filename}\n"
        f"- **Timestamp (UTC):** {claimed_at}\n"
        f"- **RAW Link:** {raw_url2 if raw_url2 else '(missing)'}"
    )

    if thread is not None:
        try:
            await thread.send(text)
        except Exception as ex:
            print(f"[CLAIM] thread.send failed item={item_code}: {type(ex).__name__}: {ex}")
    else:
        print("[CLAIM] No thread object; skipping thread post.")

    # delete listing from selection channel
    try:
        selection_ch = bot.get_channel(payload.channel_id)
        if selection_ch is None:
            selection_ch = await bot.fetch_channel(payload.channel_id)
        msg = await selection_ch.fetch_message(payload.message_id)
        await msg.delete()
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        pass

    update_item_selection_message_id(item_code_db, 0)

    # DM user confirmation
    try:
        u = bot.get_user(payload.user_id) or await bot.fetch_user(payload.user_id)
        dm = (
            f"✅ Claim confirmed!\n"
            f"- Category: {category_emoji(cat)} {category_name(cat)}\n"
            f"- Item: {item_code_db}\n"
            f"- Reason: {reason}\n"
            f"- Picks left: {new_remaining}\n"
            f"- Timestamp (UTC): {claimed_at}"
        )
        await dm_user_safe(u, dm)
    except Exception:
        pass

# ---------------------------
# Run
# ---------------------------
bot.run(DISCORD_TOKEN)
