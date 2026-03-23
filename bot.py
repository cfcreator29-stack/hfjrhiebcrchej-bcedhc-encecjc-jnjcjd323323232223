"""
🎶 Music — Telegram Music Bot
"""

from __future__ import annotations

import json
import logging
import random
import asyncio
import subprocess
import time
from datetime import datetime
from pathlib import Path

from telegram import (
    Update, InlineKeyboardButton, InlineKeyboardMarkup, BotCommand,
)
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes,
)
from telegram.error import TelegramError
from ytmusicapi import YTMusic
import yt_dlp

# ═══════════════════════════════════════════════════
#  НАСТРОЙКИ
# ═══════════════════════════════════════════════════

BOT_TOKEN       = "8746018197:AAGBrL62143LPDOZkhrWVRJIE5w46rFV0Y8"
ADMIN_IDS       = [8535260202]
CHANNEL_ID      = "-1003852929433"
CHANNEL_LINK    = "https://t.me/FV_bots"
AUDIO_EFFECT_ID = "5104841245755180586"

DOWNLOADS    = Path("downloads")
DOWNLOADS.mkdir(exist_ok=True)
FAV_FILE     = Path("favorites.json")
REVIEWS_FILE = Path("reviews.json")
USERS_FILE   = Path("users.json")
BANS_FILE    = Path("bans.json")
COOKIES_FILE = Path("cookies.txt")

SEARCH_PER_PAGE = 5
SEARCH_LIMIT    = 100
WAVE_SIZE       = 10
CHART_SIZE      = 50
REV_PER_PAGE    = 3
USERS_PER_PAGE  = 8
CHART_CACHE_TTL = 1800  # 30 минут

BOT_USERNAME = ""

_chart_cache: tuple[list, float] = ([], 0.0)
_dl_semaphore: asyncio.Semaphore | None = None  # инициализируется в main

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
log = logging.getLogger(__name__)

ytmusic = YTMusic()

# ═══════════════════════════════════════════════════
#  ЭМОДЗИ
# ═══════════════════════════════════════════════════

EMO_SEARCH  = "5870676941614354370"
EMO_FAVS    = "6032644646587338669"
EMO_WAVE    = "5345906554510012647"
EMO_HOME    = "5873147866364514353"
EMO_DL      = "6039802767931871481"
EMO_HEART   = "6039486778597970865"
EMO_UNHEART = "5870657884844462243"
EMO_OK      = "5870633910337015697"
EMO_PREV    = "5893057118545646106"
EMO_NEXT    = "5963103826075456248"
EMO_SUB     = "6039486778597970865"
EMO_CHECK   = "5870633910337015697"
EMO_CHART   = "5870930636742595124"
EMO_REVIEW  = "5870764288364252592"
EMO_ADMIN   = "5870982283724328568"
EMO_TRASH   = "5870875489362513438"
EMO_SEND    = "5963103826075456248"
EMO_STAR    = "6041731551845159060"
EMO_USERS   = "5870982283724328568"

# ═══════════════════════════════════════════════════
#  УТИЛИТЫ
# ═══════════════════════════════════════════════════

CUT = 38

def _cut(s: str) -> str:
    return s if len(s) <= CUT else s[:CUT - 1] + "…"

def _stars(n: int) -> str:
    return "⭐" * n + "☆" * (5 - n)

async def _safe_delete(bot, chat_id: int, msg_id: int | None):
    if not msg_id:
        return
    try:
        await bot.delete_message(chat_id, msg_id)
    except TelegramError:
        pass

async def _safe_edit(bot, chat_id: int, msg_id: int, text: str, kb=None) -> bool:
    try:
        await bot.edit_message_text(
            text, chat_id=chat_id, message_id=msg_id,
            reply_markup=kb, parse_mode="HTML")
        return True
    except TelegramError:
        return False

async def _send_menu(bot, chat_id: int, uid: int, ctx) -> int:
    msg = await bot.send_message(chat_id, MENU_TEXT, reply_markup=_menu_kb(uid), parse_mode="HTML")
    ctx.user_data["mid"] = msg.message_id
    ctx.user_data["main_mid"] = msg.message_id
    return msg.message_id

async def _go_home(bot, chat_id: int, uid: int, ctx, q=None):
    ctx.user_data.pop("state", None)
    await _del_extra(bot, chat_id, ctx)
    if q and q.message and q.message.text is not None:
        try:
            await q.message.edit_text(MENU_TEXT, reply_markup=_menu_kb(uid), parse_mode="HTML")
            ctx.user_data["mid"] = q.message.message_id
            ctx.user_data["main_mid"] = q.message.message_id
            return
        except TelegramError:
            pass
    await _safe_delete(bot, chat_id, ctx.user_data.get("main_mid"))
    await _send_menu(bot, chat_id, uid, ctx)

# ═══════════════════════════════════════════════════
#  БД: ПОЛЬЗОВАТЕЛИ
# ═══════════════════════════════════════════════════

def _load_users() -> dict:
    if not USERS_FILE.exists():
        return {}
    data = json.loads(USERS_FILE.read_text("utf-8"))
    if isinstance(data, list):
        data = {str(u): {"name": "", "username": ""} for u in data}
    return data

def _save_users(data: dict):
    USERS_FILE.write_text(json.dumps(data, ensure_ascii=False), "utf-8")

def user_register(uid: int, name: str = "", username: str = ""):
    data = _load_users()
    key = str(uid)
    entry = data.get(key, {})
    entry["name"] = name
    entry["username"] = username
    data[key] = entry
    _save_users(data)

def _users_ids() -> list[int]:
    return [int(k) for k in _load_users()]

def get_user_display(uid: int) -> str:
    info  = _load_users().get(str(uid), {})
    name  = info.get("name", "").strip()
    uname = info.get("username", "").strip()
    if name:
        return f"{name} (@{uname})" if uname else name
    return f"@{uname}" if uname else str(uid)

# ═══════════════════════════════════════════════════
#  БД: БАНЫ
# ═══════════════════════════════════════════════════

def _load_bans() -> list:
    if BANS_FILE.exists():
        return json.loads(BANS_FILE.read_text("utf-8"))
    return []

def _save_bans(lst: list):
    BANS_FILE.write_text(json.dumps(lst, ensure_ascii=False), "utf-8")

def ban_add(uid: int):
    lst = _load_bans()
    if uid not in lst:
        lst.append(uid)
        _save_bans(lst)

def ban_rm(uid: int):
    lst = _load_bans()
    if uid in lst:
        lst.remove(uid)
        _save_bans(lst)

def is_banned(uid: int) -> bool:
    return uid in _load_bans()

# ═══════════════════════════════════════════════════
#  БД: ИЗБРАННОЕ
# ═══════════════════════════════════════════════════

def _load_favs() -> dict:
    if FAV_FILE.exists():
        return json.loads(FAV_FILE.read_text("utf-8"))
    return {}

def _save_favs(d: dict):
    FAV_FILE.write_text(json.dumps(d, ensure_ascii=False, indent=2), "utf-8")

def fav_list(uid: int) -> list[dict]:
    return _load_favs().get(str(uid), [])

def fav_add(uid: int, t: dict) -> bool:
    d = _load_favs(); k = str(uid)
    d.setdefault(k, [])
    if any(x["id"] == t["id"] for x in d[k]):
        return False
    d[k].append(t)
    _save_favs(d)
    return True

def fav_rm(uid: int, vid: str):
    d = _load_favs(); k = str(uid)
    if k in d:
        d[k] = [x for x in d[k] if x["id"] != vid]
        _save_favs(d)

def fav_ok(uid: int, vid: str) -> bool:
    return any(x["id"] == vid for x in fav_list(uid))

# ═══════════════════════════════════════════════════
#  БД: ОТЗЫВЫ
# ═══════════════════════════════════════════════════

def _load_revs() -> list:
    if REVIEWS_FILE.exists():
        return json.loads(REVIEWS_FILE.read_text("utf-8"))
    return []

def _save_revs(lst: list):
    REVIEWS_FILE.write_text(json.dumps(lst, ensure_ascii=False, indent=2), "utf-8")

def rev_add(uid: int, stars: int, text: str) -> int:
    lst = _load_revs()
    new_id = max((r["id"] for r in lst), default=0) + 1
    lst.append({"id": new_id, "uid": uid, "stars": stars, "text": text,
                "date": datetime.now().strftime("%d.%m.%Y")})
    _save_revs(lst)
    return new_id

def rev_delete(rev_id: int) -> dict | None:
    lst = _load_revs()
    deleted = next((r for r in lst if r["id"] == rev_id), None)
    if deleted:
        _save_revs([r for r in lst if r["id"] != rev_id])
    return deleted

def rev_all() -> list:
    return _load_revs()

def rev_has(uid: int) -> bool:
    return any(r["uid"] == uid for r in _load_revs())

# ═══════════════════════════════════════════════════
#  УДАЛЕНИЕ ВРЕМЕННЫХ СООБЩЕНИЙ
# ═══════════════════════════════════════════════════

async def _del_extra(bot, chat_id: int, ctx):
    ids = ctx.user_data.pop("extra_msgs", [])
    await asyncio.gather(*[_safe_delete(bot, chat_id, mid) for mid in ids], return_exceptions=True)

def _track_msg(ctx, msg_id: int):
    ctx.user_data.setdefault("extra_msgs", [])
    if msg_id not in ctx.user_data["extra_msgs"]:
        ctx.user_data["extra_msgs"].append(msg_id)

# ═══════════════════════════════════════════════════
#  ПОДПИСКА
# ═══════════════════════════════════════════════════

async def is_subscribed(bot, uid: int) -> bool:
    try:
        m = await bot.get_chat_member(CHANNEL_ID, uid)
        return m.status in ("member", "administrator", "creator")
    except TelegramError as e:
        log.warning("is_subscribed: %s", e)
        return False

def _sub_kb():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("Подписаться", url=CHANNEL_LINK, icon_custom_emoji_id=EMO_SUB)],
        [InlineKeyboardButton("Проверить подписку", callback_data="check_sub", icon_custom_emoji_id=EMO_CHECK)],
    ])

SUB_TEXT = f'<b><tg-emoji emoji-id="{EMO_SUB}">🔔</tg-emoji> Подпишись на канал, чтобы пользоваться ботом</b>'

# ═══════════════════════════════════════════════════
#  YTMUSIC
# ═══════════════════════════════════════════════════

def yt_search(query: str, limit: int = SEARCH_LIMIT) -> list[dict]:
    try:
        raw = ytmusic.search(query, filter="songs", limit=limit)
    except Exception as e:
        log.error("search: %s", e)
        return []
    out = []
    for r in raw[:limit]:
        vid = r.get("videoId")
        if not vid:
            continue
        artists = ", ".join(a["name"] for a in r.get("artists", []) if isinstance(a, dict)) or "—"
        out.append({"id": vid, "title": r.get("title") or "—", "artist": artists, "dur": r.get("duration", "")})
    return out

def _parse_chart_item(r: dict) -> dict | None:
    try:
        vid = r.get("videoId")
        if not vid and isinstance(r.get("title"), dict):
            vid = r["title"].get("videoId")
        if not vid:
            return None
        title = r.get("title", "—")
        if isinstance(title, dict):
            runs = title.get("runs") or []
            title = runs[0].get("text", "—") if runs else title.get("text", "—")
        artists_raw = r.get("artists") or r.get("artist") or []
        if isinstance(artists_raw, list):
            artists = ", ".join((a.get("name") or a.get("text") or "") for a in artists_raw if isinstance(a, dict)) or "—"
        else:
            artists = str(artists_raw) or "—"
        dur = r.get("duration") or ""
        if isinstance(dur, int):
            dur = f"{dur // 60}:{dur % 60:02d}"
        return {"id": vid, "title": str(title) or "—", "artist": artists, "dur": str(dur)}
    except Exception as e:
        log.debug("_parse_chart_item: %s", e)
        return None

# Плейлисты русских чартов на YouTube Music
_RU_CHART_PLAYLISTS = [
    "RDCLAK5uy_kmPRjHDECIcuVwnKsx2Ns7MmAFHTVAtiY",  # Hot 100 RU
    "RDCLAK5uy_lv8-EoTBt5zYXqMCGMnpBmxGhFEqzDUQU",  # Trending RU
]

def _fetch_playlist_tracks(playlist_id: str, limit: int) -> list[dict]:
    try:
        pl = ytmusic.get_playlist(playlist_id, limit=limit)
        out = []
        for t in pl.get("tracks", []):
            vid = t.get("videoId")
            if not vid:
                continue
            artists = ", ".join(a["name"] for a in t.get("artists", []) if isinstance(a, dict)) or "—"
            out.append({"id": vid, "title": t.get("title") or "—", "artist": artists, "dur": t.get("duration", "")})
        return out
    except Exception as e:
        log.debug("_fetch_playlist_tracks %s: %s", playlist_id, e)
        return []

def yt_charts() -> list[dict]:
    global _chart_cache
    cached_items, cached_at = _chart_cache
    if cached_items and (time.time() - cached_at) < CHART_CACHE_TTL:
        return cached_items
    log.info("charts: загружаю русские чарты...")

    # 1. Пробуем get_charts(country="RU")
    try:
        charts = ytmusic.get_charts(country="RU")
        songs_block = charts.get("songs") or {}
        if isinstance(songs_block, dict):
            raw_items = songs_block.get("items") or songs_block.get("content") or []
        elif isinstance(songs_block, list):
            raw_items = songs_block
        else:
            raw_items = []
        out = [p for r in raw_items[:CHART_SIZE] if isinstance(r, dict) for p in [_parse_chart_item(r)] if p]
        if out:
            _chart_cache = (out, time.time())
            log.info("charts: %d треков из get_charts(RU)", len(out))
            return out
        log.warning("charts: get_charts(RU) вернул 0 треков")
    except Exception as e:
        log.error("charts get_charts: %s", e)

    # 2. Пробуем русские плейлисты чартов
    for pl_id in _RU_CHART_PLAYLISTS:
        out = _fetch_playlist_tracks(pl_id, CHART_SIZE)
        if out:
            _chart_cache = (out, time.time())
            log.info("charts: %d треков из плейлиста %s", len(out), pl_id)
            return out

    # 3. Фолбэк — поиск по нескольким запросам, объединяем и дедуплицируем
    log.info("charts: фолбэк на поиск русских хитов")
    fallback_queries = [
        "снг хиты 2025 2026",
        "русские хиты рэп 2025 2026",
        "хиты России 2025 2026",
        "хиты рэп 2025 2026",
        "хиты 2025 2026",
        "русские хиты 2025 2026",
    ]
    combined: list[dict] = []
    seen_ids: set[str] = set()
    for fq in fallback_queries:
        for track in yt_search(fq, CHART_SIZE // len(fallback_queries) + 5):
            if track["id"] not in seen_ids:
                seen_ids.add(track["id"])
                combined.append(track)
        if len(combined) >= CHART_SIZE:
            break
    result = combined[:CHART_SIZE]
    if result:
        _chart_cache = (result, time.time())
        log.info("charts: фолбэк собрал %d треков", len(result))
        return result
    return []

def yt_wave(uid: int) -> list[dict]:
    favs = fav_list(uid)
    if not favs:
        return yt_search(random.choice(["top hits 2025", "хиты 2025", "trending songs"]), WAVE_SIZE)
    wave: list[dict] = []
    seeds = random.sample(favs, min(3, len(favs)))
    seen = {f["id"] for f in favs}
    for s in seeds:
        try:
            pl = ytmusic.get_watch_playlist(videoId=s["id"], limit=25)
            for t in pl.get("tracks", [])[1:]:
                vid = t.get("videoId")
                if not vid or vid in seen:
                    continue
                seen.add(vid)
                wave.append({
                    "id": vid,
                    "title": t.get("title") or "—",
                    "artist": ", ".join(a["name"] for a in t.get("artists", []) if isinstance(a, dict)) or "—",
                    "dur": t.get("duration", "")
                })
        except Exception as e:
            log.error("wave: %s", e)
    random.shuffle(wave)
    return wave[:WAVE_SIZE]

# ═══════════════════════════════════════════════════
#  СКАЧИВАНИЕ
# ═══════════════════════════════════════════════════

def _clean(vid: str):
    for f in DOWNLOADS.glob(f"{vid}*"):
        try:
            f.unlink()
        except OSError:
            pass

def _find(vid: str) -> Path | None:
    for ext in (".mp3", ".m4a", ".opus", ".ogg", ".webm"):
        p = DOWNLOADS / f"{vid}{ext}"
        if p.exists() and p.stat().st_size > 10_000:
            return p
    return None

def _base_opts(vid: str) -> dict:
    return {
        "format": "bestaudio/best",
        "outtmpl": str(DOWNLOADS / f"{vid}.%(ext)s"),
        "postprocessors": [{"key": "FFmpegExtractAudio", "preferredcodec": "mp3", "preferredquality": "192"}],
        "quiet": True, "no_warnings": True, "noplaylist": True,
        "socket_timeout": 30, "geo_bypass": True,
    }

def _dl_soundcloud(vid: str, title: str, artist: str) -> Path | None:
    query = f"{artist} - {title}" if artist and artist != "—" else title
    try:
        with yt_dlp.YoutubeDL(_base_opts(vid)) as y:
            y.download([f"scsearch1:{query}"])
        return _find(vid)
    except Exception:
        return None

def _dl_youtube(vid: str) -> Path | None:
    cookie = str(COOKIES_FILE) if COOKIES_FILE.exists() else None
    configs = [
        {"extractor_args": {"youtube": {"player_client": ["android"]}}},
        {"extractor_args": {"youtube": {"player_client": ["ios"]}}},
        {"cookiefile": cookie, "extractor_args": {"youtube": {"player_client": ["android"]}}},
        {"cookiefile": cookie},
    ]
    for extra in configs:
        _clean(vid)
        try:
            with yt_dlp.YoutubeDL({**_base_opts(vid), **extra, "no_check_formats": True}) as y:
                y.download([f"https://www.youtube.com/watch?v={vid}"])
            p = _find(vid)
            if p:
                return p
        except Exception:
            pass
        time.sleep(0.5)
    return None

def yt_dl(vid: str, title: str = "", artist: str = "") -> Path | None:
    _clean(vid)
    if title:
        p = _dl_soundcloud(vid, title, artist)
        if p:
            return p
    return _dl_youtube(vid)

# ═══════════════════════════════════════════════════
#  ПРОГРЕСС-БАР
# ═══════════════════════════════════════════════════

_BAR_LEN = 10

def _progress_text(pct: int, stage: str, title: str = "", artist: str = "") -> str:
    filled = round(_BAR_LEN * pct / 100)
    bar    = "█" * filled + "░" * (_BAR_LEN - filled)
    header = f"<b>{artist} — {title}</b>\n" if title else ""
    return f"{header}<code>{bar}</code>  <b>{pct}%</b>\n<i>{stage}</i>"

# ═══════════════════════════════════════════════════
#  МЕНЮ
# ═══════════════════════════════════════════════════

MENU_TEXT = f'<b><tg-emoji emoji-id="6041731551845159060">🎉</tg-emoji> Music</b>'

def _menu_kb(uid: int) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton("Поиск",     callback_data="search",  icon_custom_emoji_id=EMO_SEARCH)],
        [InlineKeyboardButton("Хиты",      callback_data="charts",  icon_custom_emoji_id=EMO_CHART),
         InlineKeyboardButton("Отзывы",    callback_data="reviews", icon_custom_emoji_id=EMO_REVIEW)],
        [InlineKeyboardButton("Избранное", callback_data="favs",    icon_custom_emoji_id=EMO_FAVS),
         InlineKeyboardButton("Моя волна", callback_data="wave",    icon_custom_emoji_id=EMO_WAVE)],
    ]
    if uid in ADMIN_IDS:
        rows.append([InlineKeyboardButton("Админ", callback_data="admin", icon_custom_emoji_id=EMO_ADMIN)])
    return InlineKeyboardMarkup(rows)

# ═══════════════════════════════════════════════════
#  ЭКРАНЫ
# ═══════════════════════════════════════════════════

async def _open_search(chat, ctx, bot):
    ctx.user_data["state"] = "sinput"
    await _del_extra(bot, chat.id, ctx)
    await _safe_delete(bot, chat.id, ctx.user_data.pop("main_mid", None))
    msg = await chat.send_message(
        f'<b><tg-emoji emoji-id="{EMO_SEARCH}">🔎</tg-emoji> Название трека или исполнитель:</b>',
        parse_mode="HTML",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]
        ])
    )
    ctx.user_data["search_mid"] = msg.message_id
    ctx.user_data["mid"] = msg.message_id
    _track_msg(ctx, msg.message_id)

async def _show_search_results(bot, chat_id: int, msg_id: int, ctx, page: int = 0):
    items = ctx.user_data.get("res", [])
    query = ctx.user_data.get("query", "")
    if not items:
        await _safe_edit(bot, chat_id, msg_id, "<b>Ничего не найдено</b>",
                         InlineKeyboardMarkup([
                             [InlineKeyboardButton("Поиск",   callback_data="search", icon_custom_emoji_id=EMO_SEARCH)],
                             [InlineKeyboardButton("Главная", callback_data="home",   icon_custom_emoji_id=EMO_HOME)],
                         ]))
        return
    total  = len(items)
    pages  = (total - 1) // SEARCH_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    ctx.user_data["spage"] = page
    s      = page * SEARCH_PER_PAGE
    chunk  = items[s:s + SEARCH_PER_PAGE]
    text   = f'<b><tg-emoji emoji-id="{EMO_SEARCH}">🔎</tg-emoji> {query} — {total} треков</b>'
    b = [[InlineKeyboardButton(_cut(f"{tr['artist']} — {tr['title']}"), callback_data=f"sr_{s + i}")] for i, tr in enumerate(chunk)]
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data="sprev", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data="snext", icon_custom_emoji_id=EMO_NEXT))
    b.append(nav)
    b.append([InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)])
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

async def _show_favs(bot, chat_id: int, msg_id: int, uid: int, page: int = 0):
    items = fav_list(uid)
    if not items:
        await _safe_edit(bot, chat_id, msg_id,
                         f'<b><tg-emoji emoji-id="{EMO_FAVS}">❤️</tg-emoji> Избранное пусто</b>',
                         InlineKeyboardMarkup([[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]))
        return
    pages  = (len(items) - 1) // SEARCH_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    s      = page * SEARCH_PER_PAGE
    chunk  = items[s:s + SEARCH_PER_PAGE]
    text   = f'<b><tg-emoji emoji-id="{EMO_FAVS}">❤️</tg-emoji> Избранное — {len(items)} треков</b>'
    b = [[InlineKeyboardButton(_cut(f"{tr['artist']} — {tr['title']}"), callback_data=f"ft_{s + i}")] for i, tr in enumerate(chunk)]
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data=f"fp_{page - 1}", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data=f"fp_{page + 1}", icon_custom_emoji_id=EMO_NEXT))
    b.append(nav)
    b.append([InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)])
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

async def _show_wave(bot, chat_id: int, msg_id: int, tr: dict, uid: int, ctx):
    vid  = tr["id"]
    heart_emo, heart_text = (EMO_UNHEART, "Убрать") if fav_ok(uid, vid) else (EMO_HEART, "В избр.")
    idx  = ctx.user_data.get("wi", 0)
    total = len(ctx.user_data.get("wave", []))
    dur  = f"  •  {tr['dur']}" if tr.get("dur") else ""
    text = (f'<b><tg-emoji emoji-id="{EMO_WAVE}">🌊</tg-emoji> Моя волна  {idx + 1}/{total}</b>\n\n'
            f"<b>{tr['title']}</b>\n<blockquote>{tr['artist']}{dur}</blockquote>")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Пред.",    callback_data="wp",         icon_custom_emoji_id=EMO_PREV),
         InlineKeyboardButton("Скачать", callback_data=f"wd_{vid}",  icon_custom_emoji_id=EMO_DL),
         InlineKeyboardButton(heart_text,callback_data=f"wf_{vid}",  icon_custom_emoji_id=heart_emo),
         InlineKeyboardButton("След.",   callback_data="wn",         icon_custom_emoji_id=EMO_NEXT)],
        [InlineKeyboardButton("Главная", callback_data="home",        icon_custom_emoji_id=EMO_HOME)],
    ])
    await _safe_edit(bot, chat_id, msg_id, text, kb)

async def _show_charts(bot, chat_id: int, msg_id: int, ctx, page: int = 0):
    items = ctx.user_data.get("chart_items", [])
    if not items:
        items = await asyncio.to_thread(yt_charts)
        ctx.user_data["chart_items"] = items
    if not items:
        await _safe_edit(bot, chat_id, msg_id, "<b>Хиты недоступны</b>",
                         InlineKeyboardMarkup([[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]))
        return
    total  = len(items)
    pages  = (total - 1) // SEARCH_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    ctx.user_data["cpage"] = page
    s      = page * SEARCH_PER_PAGE
    chunk  = items[s:s + SEARCH_PER_PAGE]
    text   = f'<b><tg-emoji emoji-id="{EMO_CHART}">📊</tg-emoji> Хиты — {total} треков</b>'
    b = [[InlineKeyboardButton(_cut(f"#{s + i + 1} {tr['artist']} — {tr['title']}"), callback_data=f"ch_{s + i}")] for i, tr in enumerate(chunk)]
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data=f"cp_{page - 1}", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data=f"cp_{page + 1}", icon_custom_emoji_id=EMO_NEXT))
    b.append(nav)
    b.append([InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)])
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

async def _show_chart_track(bot, chat_id: int, msg_id: int, tr: dict, uid: int, ctx):
    vid  = tr["id"]
    heart_emo, heart_text = (EMO_UNHEART, "Убрать") if fav_ok(uid, vid) else (EMO_HEART, "В избр.")
    dur  = f"  •  {tr['dur']}" if tr.get("dur") else ""
    cpage = ctx.user_data.get("cpage", 0)
    text = (f'<b><tg-emoji emoji-id="{EMO_CHART}">📊</tg-emoji> Хиты</b>\n\n'
            f"<b>{tr['title']}</b>\n<blockquote>{tr['artist']}{dur}</blockquote>")
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Скачать",  callback_data=f"chd_{vid}",  icon_custom_emoji_id=EMO_DL),
         InlineKeyboardButton(heart_text, callback_data=f"chf_{vid}",  icon_custom_emoji_id=heart_emo)],
        [InlineKeyboardButton("К хитам",  callback_data=f"cp_{cpage}", icon_custom_emoji_id=EMO_PREV),
         InlineKeyboardButton("Главная",  callback_data="home",         icon_custom_emoji_id=EMO_HOME)],
    ])
    await _safe_edit(bot, chat_id, msg_id, text, kb)

def _stars_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(str(i), callback_data=f"rev_s_{i}") for i in range(1, 6)],
        [InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)],
    ])

async def _show_reviews(bot, chat_id: int, msg_id: int, uid: int = 0, page: int = 0):
    items = rev_all()
    user_has_review = rev_has(uid) if uid else False
    review_btn = [] if user_has_review else [[InlineKeyboardButton("Оставить отзыв", callback_data="rev_new", icon_custom_emoji_id=EMO_STAR)]]
    home_btn   = [[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]
    if not items:
        await _safe_edit(bot, chat_id, msg_id,
                         f'<b><tg-emoji emoji-id="{EMO_REVIEW}">🙂</tg-emoji> Отзывов пока нет</b>',
                         InlineKeyboardMarkup(review_btn + home_btn))
        return
    total  = len(items)
    pages  = (total - 1) // REV_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    s      = page * REV_PER_PAGE
    chunk  = items[s:s + REV_PER_PAGE]
    text   = f'<b><tg-emoji emoji-id="{EMO_REVIEW}">🙂</tg-emoji> Отзывы — {total} шт.</b>\n\n'
    for r in chunk:
        text += f'<b>Отзыв #{r["id"]}</b>  {_stars(r["stars"])}\n<i>{r["text"]}</i>\n<code>{r["date"]}</code>\n\n'
    b = []
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data=f"rp_{page - 1}", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data=f"rp_{page + 1}", icon_custom_emoji_id=EMO_NEXT))
    if nav:
        b.append(nav)
    b += review_btn + home_btn
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

async def _show_admin(bot, chat_id: int, msg_id: int | None = None):
    text = (f'<b><tg-emoji emoji-id="{EMO_ADMIN}">⚙️</tg-emoji> Админ-панель</b>\n\n'
            f'👥 Пользователей: <b>{len(_users_ids())}</b>\n'
            f'🚫 Забанено: <b>{len(_load_bans())}</b>\n'
            f'💬 Отзывов: <b>{len(rev_all())}</b>')
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("Пользователи",  callback_data="adm_users_0",   icon_custom_emoji_id=EMO_USERS)],
        [InlineKeyboardButton("Рассылка",      callback_data="adm_broadcast", icon_custom_emoji_id=EMO_SEND)],
        [InlineKeyboardButton("Удалить отзыв", callback_data="adm_revs",      icon_custom_emoji_id=EMO_TRASH)],
        [InlineKeyboardButton("Главная",        callback_data="home",          icon_custom_emoji_id=EMO_HOME)],
    ])
    if msg_id and await _safe_edit(bot, chat_id, msg_id, text, kb):
        return
    await bot.send_message(chat_id, text, reply_markup=kb, parse_mode="HTML")

async def _show_admin_users(bot, chat_id: int, msg_id: int, page: int = 0):
    users = _users_ids()
    bans  = _load_bans()
    if not users:
        await _safe_edit(bot, chat_id, msg_id, "<b>Пользователей нет</b>",
                         InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="admin", icon_custom_emoji_id=EMO_PREV)]]))
        return
    total  = len(users)
    pages  = (total - 1) // USERS_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    s      = page * USERS_PER_PAGE
    chunk  = users[s:s + USERS_PER_PAGE]
    text   = (f'<b><tg-emoji emoji-id="{EMO_USERS}">⚙️</tg-emoji> Пользователи — {total} чел.</b>\n'
              f'<i>👤 — забанить  |  🚫 — разбанить</i>')
    b = []
    for u in chunk:
        banned = u in bans
        label  = f"🚫 {get_user_display(u)}" if banned else f"👤 {get_user_display(u)}"
        cb     = f"adm_unban_{u}" if banned else f"adm_ban_{u}"
        b.append([InlineKeyboardButton(label, callback_data=cb)])
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data=f"adm_users_{page - 1}", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data=f"adm_users_{page + 1}", icon_custom_emoji_id=EMO_NEXT))
    if nav:
        b.append(nav)
    b.append([InlineKeyboardButton("В панель", callback_data="admin", icon_custom_emoji_id=EMO_ADMIN)])
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

async def _show_admin_reviews(bot, chat_id: int, msg_id: int, page: int = 0):
    items = rev_all()
    if not items:
        await _safe_edit(bot, chat_id, msg_id, "<b>Отзывов нет</b>",
                         InlineKeyboardMarkup([[InlineKeyboardButton("Назад", callback_data="admin", icon_custom_emoji_id=EMO_PREV)]]))
        return
    pages  = (len(items) - 1) // REV_PER_PAGE + 1
    page   = max(0, min(page, pages - 1))
    s      = page * REV_PER_PAGE
    chunk  = items[s:s + REV_PER_PAGE]
    text   = f'<b><tg-emoji emoji-id="{EMO_TRASH}">🗑</tg-emoji> Управление отзывами</b>\n\n'
    for r in chunk:
        text += f'<b>#{r["id"]}</b> {_stars(r["stars"])} — <i>{r["text"][:50]}</i>\n'
    b = [[InlineKeyboardButton(f"Удалить #{r['id']}", callback_data=f"adm_del_{r['id']}", icon_custom_emoji_id=EMO_TRASH)] for r in chunk]
    nav = []
    if page > 0:         nav.append(InlineKeyboardButton("Назад", callback_data=f"arp_{page - 1}", icon_custom_emoji_id=EMO_PREV))
    nav.append(InlineKeyboardButton(f"{page + 1}/{pages}", callback_data="noop"))
    if page < pages - 1: nav.append(InlineKeyboardButton("Далее", callback_data=f"arp_{page + 1}", icon_custom_emoji_id=EMO_NEXT))
    if nav:
        b.append(nav)
    b.append([InlineKeyboardButton("В панель", callback_data="admin", icon_custom_emoji_id=EMO_ADMIN)])
    await _safe_edit(bot, chat_id, msg_id, text, InlineKeyboardMarkup(b))

# ═══════════════════════════════════════════════════
#  СКАЧИВАНИЕ С ПРОГРЕССОМ
# ═══════════════════════════════════════════════════

async def _do_dl(bot, chat_id: int, uid: int, tr: dict, ctx):
    global _dl_semaphore
    vid    = tr["id"]
    title  = tr.get("title", "")
    artist = tr.get("artist", "")

    status_msg = await bot.send_message(
        chat_id, _progress_text(0, "⏳ Подключаюсь...", title, artist), parse_mode="HTML")
    status_id = status_msg.message_id

    progress_q: asyncio.Queue = asyncio.Queue()
    stop = asyncio.Event()

    async def _animate():
        cur_text = ""
        while not stop.is_set() or not progress_q.empty():
            latest = None
            while not progress_q.empty():
                try:
                    latest = progress_q.get_nowait()
                except asyncio.QueueEmpty:
                    break
            if latest is not None:
                pct, stage = latest
                new_text = _progress_text(pct, stage, title, artist)
                if new_text != cur_text:
                    cur_text = new_text
                    await _safe_edit(bot, chat_id, status_id, new_text)
            try:
                await asyncio.wait_for(stop.wait(), timeout=0.8)
            except asyncio.TimeoutError:
                pass

    async def _push_progress():
        stages = [
            (5,  "🔍 Ищу трек..."),
            (15, "🔗 Нашёл источник..."),
            (30, "⬇️ Скачиваю аудио..."),
            (50, "⬇️ Скачиваю аудио..."),
            (65, "⬇️ Скачиваю аудио..."),
            (78, "🎛 Конвертирую в MP3..."),
            (88, "🎛 Конвертирую в MP3..."),
            (95, "📦 Почти готово..."),
        ]
        for pct, stage in stages:
            if stop.is_set():
                break
            await progress_q.put((pct, stage))
            await asyncio.sleep(random.uniform(1.2, 2.2))

    anim_task     = asyncio.create_task(_animate())
    progress_task = asyncio.create_task(_push_progress())

    sem = _dl_semaphore or asyncio.Semaphore(3)
    async with sem:
        path = await asyncio.to_thread(yt_dl, vid, title, artist)

    progress_task.cancel()
    stop.set()
    await asyncio.gather(anim_task, return_exceptions=True)

    if path:
        await _safe_edit(bot, chat_id, status_id, _progress_text(100, "✅ Готово! Отправляю...", title, artist))
        try:
            bot_link = f"https://t.me/{BOT_USERNAME}" if BOT_USERNAME else None
            caption  = f"<b>{artist} — {title}</b>"
            if bot_link:
                caption += f'\n🎵 <a href="{bot_link}">Слушать музыку в боте</a>'
            with open(path, "rb") as f:
                sent = await bot.send_audio(
                    chat_id, audio=f, title=title, performer=artist,
                    caption=caption, parse_mode="HTML",
                    reply_markup=InlineKeyboardMarkup([
                        [InlineKeyboardButton(
                             "💔 Убрать" if fav_ok(uid, vid) else "❤️ В избранное",
                             callback_data=f"trf_{vid}",
                             icon_custom_emoji_id=EMO_UNHEART if fav_ok(uid, vid) else EMO_HEART),
                         InlineKeyboardButton("🗑 Удалить", callback_data="del_track", icon_custom_emoji_id=EMO_TRASH)],
                    ]),
                    message_effect_id=AUDIO_EFFECT_ID
                )
            ctx.user_data["last_track_msg"] = sent.message_id
            ctx.user_data["last_track"] = tr
        except Exception as e:
            log.error("send_audio: %s", e)
        finally:
            await _safe_delete(bot, chat_id, status_id)
            try:
                path.unlink()
            except OSError:
                pass
        await _send_menu(bot, chat_id, uid, ctx)
    else:
        await _safe_edit(bot, chat_id, status_id,
                         f'❌ <b>Не удалось скачать</b>\n<i>{artist} — {title}</i>',
                         InlineKeyboardMarkup([
                             [InlineKeyboardButton("Поиск",   callback_data="goto_search", icon_custom_emoji_id=EMO_SEARCH),
                              InlineKeyboardButton("Главная", callback_data="home",        icon_custom_emoji_id=EMO_HOME)],
                         ]))

# ═══════════════════════════════════════════════════
#  ОБРАБОТЧИКИ
# ═══════════════════════════════════════════════════

async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    user = update.effective_user
    name = " ".join(filter(None, [user.first_name or "", user.last_name or ""])).strip()
    user_register(uid, name=name, username=user.username or "")
    try:
        await update.message.delete()
    except TelegramError:
        pass
    if uid not in ADMIN_IDS and is_banned(uid):
        return
    # Чистим предыдущие сообщения бота
    for key in ("mid", "main_mid"):
        await _safe_delete(ctx.bot, update.effective_chat.id, ctx.user_data.get(key))
    await _del_extra(ctx.bot, update.effective_chat.id, ctx)
    ctx.user_data.clear()
    if uid not in ADMIN_IDS and not await is_subscribed(ctx.bot, uid):
        msg = await ctx.bot.send_message(update.effective_chat.id, SUB_TEXT, reply_markup=_sub_kb(), parse_mode="HTML")
        ctx.user_data["mid"] = msg.message_id
        return
    await _send_menu(ctx.bot, update.effective_chat.id, uid, ctx)


async def on_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    uid  = update.effective_user.id
    user = update.effective_user
    if uid not in ADMIN_IDS and is_banned(uid):
        try:
            await update.message.delete()
        except TelegramError:
            pass
        return
    name = " ".join(filter(None, [user.first_name or "", user.last_name or ""])).strip()
    user_register(uid, name=name, username=user.username or "")
    state   = ctx.user_data.get("state")
    text    = update.message.text.strip()
    chat_id = update.effective_chat.id
    try:
        await update.message.delete()
    except TelegramError:
        pass

    if state == "sinput":
        ctx.user_data["state"] = None
        ctx.user_data["query"] = text
        search_mid = ctx.user_data.get("search_mid") or ctx.user_data.get("mid")
        if not search_mid:
            return
        await _safe_edit(ctx.bot, chat_id, search_mid,
                         f'<b><tg-emoji emoji-id="{EMO_SEARCH}">🔎</tg-emoji> {text}...</b>')
        items = await asyncio.to_thread(yt_search, text, SEARCH_LIMIT)
        ctx.user_data["res"] = items
        ctx.user_data["spage"] = 0
        await _show_search_results(ctx.bot, chat_id, search_mid, ctx, 0)

    elif state == "rev_text":
        stars = ctx.user_data.get("rev_stars", 5)
        ctx.user_data["state"] = None
        rev_id  = rev_add(uid, stars, text)
        rev_mid = ctx.user_data.get("rev_mid") or ctx.user_data.get("mid")
        if rev_mid:
            await _safe_edit(ctx.bot, chat_id, rev_mid,
                             f'<b><tg-emoji emoji-id="{EMO_OK}">✅</tg-emoji> Отзыв #{rev_id} сохранён! {_stars(stars)}</b>\n\n<i>{text}</i>',
                             InlineKeyboardMarkup([
                                 [InlineKeyboardButton("Все отзывы", callback_data="reviews", icon_custom_emoji_id=EMO_REVIEW)],
                                 [InlineKeyboardButton("Главная",     callback_data="home",    icon_custom_emoji_id=EMO_HOME)],
                             ]))

    elif state == "broadcast" and uid in ADMIN_IDS:
        ctx.user_data["state"] = None
        users  = _users_ids()
        bans   = _load_bans()
        ok = fail = 0
        bc_mid = ctx.user_data.get("bc_mid") or ctx.user_data.get("mid")
        if bc_mid:
            await _safe_edit(ctx.bot, chat_id, bc_mid,
                             f'<b><tg-emoji emoji-id="{EMO_SEND}">⬆</tg-emoji> Рассылка: 0/{len(users)}</b>')
        for i, target_uid in enumerate(users):
            if target_uid in bans:
                fail += 1
                continue
            try:
                await ctx.bot.send_message(target_uid, text, parse_mode="HTML")
                ok += 1
            except TelegramError:
                fail += 1
            if (i + 1) % 10 == 0 and bc_mid:
                await _safe_edit(ctx.bot, chat_id, bc_mid, f'<b>Рассылка: {i + 1}/{len(users)}...</b>')
            await asyncio.sleep(0.05)
        if bc_mid:
            await _safe_edit(ctx.bot, chat_id, bc_mid,
                             f'<b><tg-emoji emoji-id="{EMO_OK}">✅</tg-emoji> Рассылка завершена\n'
                             f'Доставлено: {ok} | Ошибок: {fail}</b>',
                             InlineKeyboardMarkup([[InlineKeyboardButton("В панель", callback_data="admin", icon_custom_emoji_id=EMO_ADMIN)]]))


async def on_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    await q.answer()
    d       = q.data
    uid     = q.from_user.id
    chat_id = q.message.chat_id
    msg_id  = q.message.message_id
    bot     = ctx.bot

    if uid not in ADMIN_IDS and is_banned(uid):
        await _safe_delete(bot, chat_id, msg_id)
        return

    u = q.from_user
    user_register(uid,
                  name=" ".join(filter(None, [u.first_name or "", u.last_name or ""])).strip(),
                  username=u.username or "")

    if d != "check_sub" and uid not in ADMIN_IDS:
        if not await is_subscribed(bot, uid):
            await _safe_edit(bot, chat_id, msg_id, SUB_TEXT, _sub_kb())
            return

    # ── Подписка ──
    if d == "check_sub":
        if await is_subscribed(bot, uid):
            await _del_extra(bot, chat_id, ctx)
            await _safe_delete(bot, chat_id, msg_id)
            await _send_menu(bot, chat_id, uid, ctx)
        else:
            await q.answer("Подпишись на канал и попробуй снова!", show_alert=True)

    # ── Главная ──
    elif d == "home":
        await _go_home(bot, chat_id, uid, ctx, q)

    elif d == "noop":
        pass

    # ── Поиск ──
    elif d in ("search", "goto_search"):
        await _open_search(q.message.chat, ctx, bot)

    elif d == "snext":
        await _show_search_results(bot, chat_id, msg_id, ctx, ctx.user_data.get("spage", 0) + 1)

    elif d == "sprev":
        await _show_search_results(bot, chat_id, msg_id, ctx, ctx.user_data.get("spage", 0) - 1)

    elif d.startswith("sr_"):
        i = int(d[3:])
        res = ctx.user_data.get("res", [])
        if i < len(res):
            tr = res[i]
            ctx.user_data["cur"] = tr
            await _del_extra(bot, chat_id, ctx)
            await _safe_delete(bot, chat_id, msg_id)
            await _do_dl(bot, chat_id, uid, tr, ctx)

    # ── Удалить трек ──
    elif d == "del_track":
        await _safe_delete(bot, chat_id, msg_id)

    # ── Избранное под треком ──
    elif d.startswith("trf_"):
        vid = d[4:]
        # Ищем трек в памяти, потом в избранном (на случай если ctx сбросился)
        tr = ctx.user_data.get("last_track") or ctx.user_data.get("cur")
        if not tr or tr.get("id") != vid:
            tr = next((t for t in fav_list(uid) if t["id"] == vid), None)
        if not tr:
            # Минимальный объект — хватит для fav_add/fav_rm
            tr = {"id": vid, "title": "", "artist": ""}
        if fav_ok(uid, vid):
            fav_rm(uid, vid)
            new_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("❤️ В избранное", callback_data=f"trf_{vid}", icon_custom_emoji_id=EMO_HEART),
                 InlineKeyboardButton("🗑 Удалить",     callback_data="del_track",  icon_custom_emoji_id=EMO_TRASH)],
            ])
            await q.answer("Убрано из избранного", show_alert=False)
        else:
            fav_add(uid, tr)
            new_kb = InlineKeyboardMarkup([
                [InlineKeyboardButton("💔 Убрать",  callback_data=f"trf_{vid}", icon_custom_emoji_id=EMO_UNHEART),
                 InlineKeyboardButton("🗑 Удалить", callback_data="del_track",  icon_custom_emoji_id=EMO_TRASH)],
            ])
            await q.answer("Добавлено в избранное ❤️", show_alert=False)
        try:
            await q.message.edit_reply_markup(new_kb)
        except TelegramError:
            pass

    # ── Избранное ──
    elif d == "favs":
        await _del_extra(bot, chat_id, ctx)
        await _show_favs(bot, chat_id, msg_id, uid)

    elif d.startswith("fp_"):
        await _show_favs(bot, chat_id, msg_id, uid, int(d[3:]))

    elif d.startswith("ft_"):
        i = int(d[3:])
        fl = fav_list(uid)
        if i < len(fl):
            tr = fl[i]
            ctx.user_data["cur"] = tr
            await _del_extra(bot, chat_id, ctx)
            await _safe_delete(bot, chat_id, msg_id)
            await _do_dl(bot, chat_id, uid, tr, ctx)

    # ── Волна ──
    elif d == "wave":
        await _del_extra(bot, chat_id, ctx)
        await _safe_edit(bot, chat_id, msg_id,
                         f'<b><tg-emoji emoji-id="{EMO_WAVE}">🌊</tg-emoji> Подбираю треки...</b>')
        w = await asyncio.to_thread(yt_wave, uid)
        if not w:
            await _safe_edit(bot, chat_id, msg_id, "<b>Не удалось подобрать треки</b>",
                             InlineKeyboardMarkup([[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]))
            return
        ctx.user_data["wave"] = w
        ctx.user_data["wi"]   = 0
        await _show_wave(bot, chat_id, msg_id, w[0], uid, ctx)

    elif d == "wn":
        w = ctx.user_data.get("wave", [])
        i = ctx.user_data.get("wi", 0) + 1
        if i >= len(w):
            more = await asyncio.to_thread(yt_wave, uid)
            w.extend(more)
            ctx.user_data["wave"] = w
        if i < len(w):
            ctx.user_data["wi"] = i
            await _show_wave(bot, chat_id, msg_id, w[i], uid, ctx)
        else:
            await _safe_edit(bot, chat_id, msg_id,
                             f'<b><tg-emoji emoji-id="{EMO_WAVE}">🌊</tg-emoji> Треки закончились</b>',
                             InlineKeyboardMarkup([[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]))

    elif d == "wp":
        w = ctx.user_data.get("wave", [])
        i = max(0, ctx.user_data.get("wi", 0) - 1)
        ctx.user_data["wi"] = i
        if w:
            await _show_wave(bot, chat_id, msg_id, w[i], uid, ctx)

    elif d.startswith("wd_"):
        vid = d[3:]
        w   = ctx.user_data.get("wave", [])
        tr  = next((t for t in w if t["id"] == vid), {"id": vid, "title": "", "artist": ""})
        ctx.user_data["cur"] = tr
        await _del_extra(bot, chat_id, ctx)
        await _safe_delete(bot, chat_id, msg_id)
        await _do_dl(bot, chat_id, uid, tr, ctx)

    elif d.startswith("wf_"):
        vid = d[3:]
        w   = ctx.user_data.get("wave", [])
        tr  = next((t for t in w if t["id"] == vid), None)
        if tr:
            fav_rm(uid, vid) if fav_ok(uid, vid) else fav_add(uid, tr)
            await _show_wave(bot, chat_id, msg_id, tr, uid, ctx)

    # ── Чарты ──
    elif d == "charts":
        await _del_extra(bot, chat_id, ctx)
        cached, cached_at = _chart_cache
        if not (cached and (time.time() - cached_at) < CHART_CACHE_TTL):
            await _safe_edit(bot, chat_id, msg_id,
                             f'<b><tg-emoji emoji-id="{EMO_CHART}">📊</tg-emoji> Загружаю хиты...</b>')
        ctx.user_data["cpage"] = 0
        await _show_charts(bot, chat_id, msg_id, ctx, 0)

    elif d.startswith("cp_"):
        await _show_charts(bot, chat_id, msg_id, ctx, int(d[3:]))

    elif d.startswith("ch_"):
        i = int(d[3:])
        items = ctx.user_data.get("chart_items", [])
        if i < len(items):
            tr = items[i]
            ctx.user_data["cur"] = tr
            ctx.user_data["chart_track"] = tr
            await _show_chart_track(bot, chat_id, msg_id, tr, uid, ctx)

    elif d.startswith("chd_"):
        vid = d[4:]
        tr  = ctx.user_data.get("chart_track") or ctx.user_data.get("cur") or {"id": vid, "title": "", "artist": ""}
        await _del_extra(bot, chat_id, ctx)
        await _safe_delete(bot, chat_id, msg_id)
        await _do_dl(bot, chat_id, uid, tr, ctx)

    elif d.startswith("chf_"):
        vid = d[4:]
        tr  = ctx.user_data.get("chart_track") or ctx.user_data.get("cur")
        if tr:
            fav_rm(uid, vid) if fav_ok(uid, vid) else fav_add(uid, tr)
            await _show_chart_track(bot, chat_id, msg_id, tr, uid, ctx)

    # ── Отзывы ──
    elif d == "reviews":
        ctx.user_data.pop("state", None)
        await _del_extra(bot, chat_id, ctx)
        await _show_reviews(bot, chat_id, msg_id, uid)

    elif d.startswith("rp_"):
        await _show_reviews(bot, chat_id, msg_id, uid, int(d[3:]))

    elif d == "rev_new":
        if rev_has(uid):
            await q.answer("Ты уже оставлял отзыв. Спасибо! 😊", show_alert=True)
            return
        ctx.user_data["state"]   = "rev_stars"
        ctx.user_data["rev_mid"] = msg_id
        await _safe_edit(bot, chat_id, msg_id,
                         f'<b><tg-emoji emoji-id="{EMO_REVIEW}">🙂</tg-emoji> Оцени бота от 1 до 5:</b>',
                         _stars_kb())

    elif d.startswith("rev_s_"):
        stars = int(d[6:])
        ctx.user_data["rev_stars"] = stars
        ctx.user_data["state"]     = "rev_text"
        ctx.user_data["rev_mid"]   = msg_id
        await _safe_edit(bot, chat_id, msg_id,
                         f'<b><tg-emoji emoji-id="{EMO_REVIEW}">🙂</tg-emoji> Оценка: {stars}/5\n\nНапиши свой отзыв:</b>',
                         InlineKeyboardMarkup([[InlineKeyboardButton("Главная", callback_data="home", icon_custom_emoji_id=EMO_HOME)]]))

    # ── Админ ──
    elif d == "admin":
        if uid not in ADMIN_IDS:
            await q.answer("Нет доступа", show_alert=True)
            return
        await _del_extra(bot, chat_id, ctx)
        await _show_admin(bot, chat_id, msg_id)

    elif d.startswith("adm_users_"):
        if uid not in ADMIN_IDS:
            return
        pg = int(d[10:])
        ctx.user_data["adm_users_page"] = pg
        await _show_admin_users(bot, chat_id, msg_id, pg)

    elif d.startswith("adm_ban_"):
        if uid not in ADMIN_IDS:
            return
        target_uid = int(d[8:])
        ban_add(target_uid)
        display = get_user_display(target_uid)
        await q.answer(f"Пользователь {display} забанен 🚫", show_alert=True)
        try:
            await bot.send_message(target_uid, '<b>🚫 Вы были заблокированы в боте.</b>', parse_mode="HTML")
        except TelegramError:
            pass
        await _show_admin_users(bot, chat_id, msg_id, ctx.user_data.get("adm_users_page", 0))

    elif d.startswith("adm_unban_"):
        if uid not in ADMIN_IDS:
            return
        target_uid = int(d[10:])
        ban_rm(target_uid)
        display = get_user_display(target_uid)
        await q.answer(f"Пользователь {display} разбанен ✅", show_alert=True)
        try:
            await bot.send_message(target_uid,
                                   '<b>✅ Ваша блокировка снята. Добро пожаловать обратно!</b>\n\nНажмите /start чтобы продолжить.',
                                   parse_mode="HTML")
        except TelegramError:
            pass
        await _show_admin_users(bot, chat_id, msg_id, ctx.user_data.get("adm_users_page", 0))

    elif d == "adm_broadcast":
        if uid not in ADMIN_IDS:
            return
        ctx.user_data["state"] = "broadcast"
        bc_msg = await bot.send_message(
            chat_id,
            f'<b><tg-emoji emoji-id="{EMO_SEND}">⬆</tg-emoji> Отправь сообщение для рассылки (HTML поддерживается):</b>',
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("Отмена", callback_data="admin", icon_custom_emoji_id=EMO_HOME)]])
        )
        ctx.user_data["bc_mid"] = bc_msg.message_id
        _track_msg(ctx, bc_msg.message_id)

    elif d == "adm_revs":
        if uid not in ADMIN_IDS:
            return
        await _show_admin_reviews(bot, chat_id, msg_id)

    elif d.startswith("arp_"):
        if uid not in ADMIN_IDS:
            return
        await _show_admin_reviews(bot, chat_id, msg_id, int(d[4:]))

    elif d.startswith("adm_del_"):
        if uid not in ADMIN_IDS:
            return
        rev_id  = int(d[8:])
        deleted = rev_delete(rev_id)
        await q.answer(f"Отзыв #{rev_id} удалён ✅", show_alert=True)
        if deleted:
            author_uid = deleted.get("uid")
            if author_uid:
                try:
                    await bot.send_message(
                        author_uid,
                        f'<b>🗑 Ваш отзыв #{rev_id} был удалён администратором.</b>\n\n'
                        f'Вы можете оставить новый отзыв в разделе «Отзывы».',
                        parse_mode="HTML"
                    )
                except TelegramError:
                    pass
        await _show_admin_reviews(bot, chat_id, msg_id)

# ═══════════════════════════════════════════════════
#  ЗАПУСК
# ═══════════════════════════════════════════════════

async def _chart_auto_update():
    """Фоновая задача: обновляет кэш хитов каждые 30 минут."""
    # Первый запуск — сразу
    log.info("charts: прогреваю кэш при старте...")
    await asyncio.to_thread(yt_charts)
    log.info("charts: кэш готов ✅")
    # Бесконечный цикл обновления
    while True:
        await asyncio.sleep(CHART_CACHE_TTL)
        log.info("charts: плановое обновление кэша...")
        global _chart_cache
        _chart_cache = ([], 0.0)  # сбрасываем кэш чтобы yt_charts загрузил свежие
        await asyncio.to_thread(yt_charts)
        log.info("charts: кэш обновлён ✅")

async def post_init(app: Application):
    global BOT_USERNAME
    me = await app.bot.get_me()
    BOT_USERNAME = me.username or ""
    log.info("🤖 @%s", BOT_USERNAME)
    asyncio.create_task(_chart_auto_update())
    await app.bot.set_my_commands([BotCommand("start", "🎶 Music")])

def main():
    global _dl_semaphore
    _dl_semaphore = asyncio.Semaphore(3)

    try:
        subprocess.run(["ffmpeg", "-version"], capture_output=True, timeout=5)
        log.info("✅ ffmpeg найден")
    except Exception:
        log.error("❌ ffmpeg не найден!")

    app = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .concurrent_updates(True)
        .build()
    )
    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CallbackQueryHandler(on_cb))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    log.info("🎵 Бот запущен")
    app.run_polling(drop_pending_updates=True)

if __name__ == "__main__":
    main()