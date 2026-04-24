import os
import asyncio
import base64
import qrcode
import sqlite3
import threading
import time
import queue
import logging
import hashlib
import functools
from io import BytesIO
from datetime import datetime
from telethon.errors import AuthKeyUnregisteredError
from flask import (
    Flask, session, request, render_template_string,
    jsonify, g, redirect, url_for, send_from_directory, render_template 
)
from telethon import TelegramClient, events
import json
from flask_socketio import SocketIO, emit
from telethon.events import MessageDeleted


import os
import uuid
import json
from werkzeug.utils import secure_filename
from flask import send_from_directory


import secrets
import math
import pandas as pd
import tempfile
import pdfplumber
import re
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
sqlite3.register_adapter(datetime, lambda d: d.strftime("%Y-%m-%d %H:%M:%S"))

try:
    from google import genai as google_genai_sdk
    from google.genai import types as google_genai_types
except Exception:
    google_genai_sdk = None
    google_genai_types = None






# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

if load_dotenv:
    for env_name in (".env", ".env.local"):
        env_path = os.path.join(BASE_DIR, env_name)
        if os.path.exists(env_path):
            load_dotenv(env_path, override=False)

app = Flask(__name__)
app.secret_key = 'supersecretkey_for_demo'
app.config['DATABASE'] = os.path.join(BASE_DIR, 'app.db')
app.json.ensure_ascii = False
# ПЕРЕНЕСТИ СЮДА (строки, которые сейчас перед шаблонами)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading', ping_timeout=120, ping_interval=25) 
PRODUCT_SEARCH_CACHE = {}
ACCESS_TREE_CACHE = {}

@app.after_request
def add_no_cache_headers(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate, max-age=0'
    response.headers['Pragma'] = 'no-cache'
    response.headers['Expires'] = '0'
    return response


# Настройка папки для хранения фото
UPLOAD_FOLDER = os.path.join(BASE_DIR, 'uploads')
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

# НОВЫЙ МАРШРУТ: Отдаем загруженные файлы по ссылке
@app.route('/uploads/<filename>')
def uploaded_file(filename):
    response = send_from_directory(app.config['UPLOAD_FOLDER'], filename, max_age=31536000)
    response.cache_control.public = True
    response.cache_control.max_age = 31536000
    return response

def invalidate_product_search_cache(user_id=None):
    if user_id is None:
        PRODUCT_SEARCH_CACHE.clear()
        return
    PRODUCT_SEARCH_CACHE.pop(int(user_id), None)

def invalidate_access_tree_cache(user_id=None):
    if user_id is None:
        ACCESS_TREE_CACHE.clear()
        return
    ACCESS_TREE_CACHE.pop(int(user_id), None)

def notify_clients(data_type="general"):
    invalidate_product_search_cache()
    invalidate_access_tree_cache()
    socketio.emit('db_updated', {'type': data_type})
# ---------- Глобальные структуры ----------
background_tasks = {}      # session_id -> thread
user_clients = {}          # session_id -> (thread, client, user_id)
qr_sessions = {}           # session_id -> данные для QR
listener_locks = {}        # блокировки для каждого session_id
user_loops = {}            # session_id -> asyncio event loop


import asyncio
import logging

# --- ПОДАВЛЕНИЕ СИСТЕМНЫХ ОШИБОК TELETHON ПРИ ЗАКРЫТИИ ПОТОКОВ ---
def custom_exception_handler(loop, context):
    exception = context.get("exception")
    msg_str = str(exception) if exception else str(context.get("message", ""))
    
    # 1. Игнорируем ошибки закрытого цикла
    if isinstance(exception, (RuntimeError, GeneratorExit)) or "Task was destroyed" in msg_str or "Event loop is closed" in msg_str:
        return
    
    # 2. Игнорируем внутренний баг Telethon при отмене задач (AttributeError)
    if isinstance(exception, AttributeError) and "'NoneType' object has no attribute" in msg_str and ("recv" in msg_str or "disconnect" in msg_str):
        return
        
    # Для остальных, реальных ошибок используем стандартный обработчик
    loop.default_exception_handler(context)

# Применяем этот обработчик ко всем новым потокам
# (Вам нужно добавить `loop.set_exception_handler(custom_exception_handler)` 
# в функцию, где вы создаете новый цикл `loop = asyncio.new_event_loop()`)

import functools
from flask import session, redirect, url_for
from datetime import datetime, timedelta, timezone

def build_expired_session_response():
    session.clear()
    if request.path.startswith('/api/'):
        return jsonify({'error': 'Сеанс завершен'}), 401
    return redirect(url_for('login_page'))

def get_session_user_record():
    if 'user_id' not in session:
        return None
    db = get_db()
    return db.execute(
        "SELECT id, role, session_token FROM users WHERE id = ?",
        (session['user_id'],)
    ).fetchone()

def has_invalid_session_token(user):
    if not user:
        return True
    if 'session_token' in user.keys() and user['session_token']:
        return session.get('session_token') != user['session_token']
    return False

def login_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
            
        user = get_session_user_record()
        if has_invalid_session_token(user):
            return build_expired_session_response()
        
        return f(*args, **kwargs)
    return wrapped

def ensure_event_loop():
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop

# ---------- Работа с БД ----------
def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        # 1. Добавляем timeout=20. Если база занята, поток подождет 20 секунд, а не упадет сразу
        db = g._database = sqlite3.connect(app.config['DATABASE'], timeout=20)
        
        # 2. Включаем режим WAL для параллельной работы
        db.execute("PRAGMA journal_mode=WAL;")
        
        # 3. Синхронизация (опционально для ускорения записи)
        db.execute("PRAGMA synchronous=NORMAL;")
        
        db.row_factory = sqlite3.Row
    return db

def get_table_sql(db, table_name):
    row = db.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return (row[0] if row else '') or ''


def table_exists(db, table_name):
    return bool(get_table_sql(db, table_name))


def row_has_column(row, column_name):
    return row is not None and column_name in row.keys()


def normalize_telegram_ref(value):
    raw = str(value or '').strip().lower()
    if not raw:
        return ''
    raw = raw.split('?', 1)[0].strip()
    raw = raw.rstrip('/').strip()
    match = re.search(r'(?:t\.me|telegram\.me)/([^/\s)]+)', raw)
    if match:
        raw = match.group(1)
    raw = re.sub(r'^(?:https?://)?(?:www\.)?', '', raw)
    raw = raw.lstrip('@').strip()
    return raw


def get_interaction_bot_tracked_chat_ids(db, user_id, bot_rows):
    tracked_ids = set()
    bot_rows = [row for row in bot_rows if row]
    if not bot_rows:
        return tracked_ids

    for bot in bot_rows:
        if row_has_column(bot, 'tracked_chat_id') and bot['tracked_chat_id']:
            tracked_ids.add(int(bot['tracked_chat_id']))
        if row_has_column(bot, 'resolved_chat_id') and bot['resolved_chat_id']:
            rows = db.execute(
                "SELECT id FROM tracked_chats WHERE user_id=? AND chat_id=?",
                (user_id, bot['resolved_chat_id'])
            ).fetchall()
            tracked_ids.update(int(row['id']) for row in rows)

    tracked_rows = db.execute(
        "SELECT id, chat_id, chat_title, custom_name FROM tracked_chats WHERE user_id=?",
        (user_id,)
    ).fetchall()
    for bot in bot_rows:
        bot_refs = {
            normalize_telegram_ref(bot['bot_username'] if 'bot_username' in bot.keys() else ''),
            normalize_telegram_ref(bot['custom_name'] if 'custom_name' in bot.keys() else ''),
        }
        bot_refs.discard('')
        if not bot_refs:
            continue
        for chat in tracked_rows:
            chat_refs = {
                normalize_telegram_ref(chat['chat_id']),
                normalize_telegram_ref(chat['chat_title']),
                normalize_telegram_ref(chat['custom_name']),
            }
            chat_refs.discard('')
            if bot_refs.intersection(chat_refs):
                tracked_ids.add(int(chat['id']))
    return tracked_ids


def delete_tracked_chats_by_ids(db, user_id, tracked_ids):
    deleted = 0
    for tracked_id in sorted({int(item) for item in tracked_ids if item}):
        row = db.execute(
            "SELECT chat_id FROM tracked_chats WHERE id=? AND user_id=?",
            (tracked_id, user_id)
        ).fetchone()
        if not row:
            continue
        real_chat_id = row['chat_id']
        db.execute(
            "DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE chat_id=? AND user_id=?)",
            (real_chat_id, user_id)
        )
        db.execute("DELETE FROM messages WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
        db.execute("DELETE FROM excel_configs WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
        db.execute("DELETE FROM excel_missing_sheets WHERE chat_id=? AND user_id=?", (real_chat_id, user_id))
        db.execute("DELETE FROM tracked_chats WHERE id=? AND user_id=?", (tracked_id, user_id))
        deleted += 1
    return deleted


def delete_all_tracked_chats_for_user(db, user_id):
    rows = db.execute("SELECT id FROM tracked_chats WHERE user_id=?", (user_id,)).fetchall()
    return delete_tracked_chats_by_ids(db, user_id, [row['id'] for row in rows])


def get_next_product_sort_index(db, user_id):
    row = db.execute(
        "SELECT COALESCE(MAX(sort_index), 0) + 1 FROM products WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    return int(row[0] if row and row[0] is not None else 1)


def build_product_sort_index(base_index, product_name=None):
    try:
        normalized_base = int(base_index)
    except (TypeError, ValueError):
        return None

    raw_name = str(product_name or '').lower()
    if 'esim' in raw_name and '1sim' not in raw_name and '2sim' not in raw_name:
        variant_rank = 0
    elif '1sim' in raw_name or 'sim+esim' in raw_name:
        variant_rank = 1
    elif '2sim' in raw_name:
        variant_rank = 2
    else:
        variant_rank = 3
    return normalized_base * 10 + variant_rank


def update_product_sort_index(db, user_id, product_id, sort_index):
    if sort_index is None:
        return
    try:
        normalized_sort_index = int(sort_index)
    except (TypeError, ValueError):
        return
    db.execute(
        "UPDATE products SET sort_index = ? WHERE id = ? AND user_id = ?",
        (normalized_sort_index, product_id, user_id),
    )


def maybe_sync_product_sort_from_message(db, user_id, product_id, message_id, line_index):
    if line_index is None or int(line_index) < 0:
        return
    product = db.execute(
        "SELECT name FROM products WHERE id = ? AND user_id = ?",
        (product_id, user_id),
    ).fetchone()
    msg = db.execute(
        "SELECT chat_id, type FROM messages WHERE id = ? AND user_id = ?",
        (message_id, user_id),
    ).fetchone()
    if not msg or not product:
        return

    msg_type = str(msg['type'] or '')
    chat_id = str(msg['chat_id'] or '')
    if msg_type.startswith('excel') or chat_id.startswith('api_src_'):
        update_product_sort_index(
            db,
            user_id,
            product_id,
            build_product_sort_index(int(line_index), product['name']),
        )

def bot_interaction_scheduler():
    """Фоновый поток для автоматической отправки команд ботам по расписанию"""
    while True:
        time.sleep(60)  # Проверяем каждую минуту
        try:
            with app.app_context():
                db = get_db()
                active_interactions = db.execute("SELECT * FROM interaction_bots WHERE status = 'active'").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for interaction in active_interactions:
                    # Оборачиваем КАЖДОГО бота в свой try..except, чтобы ошибка в одном не ломала других
                    try:
                        userbot_id = interaction['userbot_id']
                        
                        if not is_parsing_allowed(userbot_id):
                            continue 
                            
                        last_run = interaction['last_run']
                        if last_run:
                            try:
                                last_run_dt = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S.%f')
                            except ValueError:
                                last_run_dt = datetime.strptime(last_run, '%Y-%m-%d %H:%M:%S')
                                
                            diff_minutes = (now - last_run_dt).total_seconds() / 60
                            if diff_minutes < interaction['interval_minutes']:
                                continue  # Время еще не пришло
                        
                        if userbot_id in user_clients:
                            _, client, _ = user_clients[userbot_id]
                            loop = user_loops.get(userbot_id)
                            
                            if loop and loop.is_running() and client.is_connected():
                                commands = json.loads(interaction['commands'])
                                bot_username = interaction['bot_username']
                                
                                logger.info(f"🔄 Запуск команд для бота {bot_username}...")
                                
                                for cmd in commands:
                                    # Асинхронная обертка для гарантированной отправки и получения ошибок
                                    async def send_cmd(target, text):
                                        # get_entity заставляет Telethon "вспомнить" бота перед отправкой
                                        entity = await client.get_entity(target)
                                        await client.send_message(entity, text)
                                    
                                    try:
                                        # Запускаем и ЖДЕМ результат максимум 15 секунд
                                        future = asyncio.run_coroutine_threadsafe(send_cmd(bot_username, cmd), loop)
                                        future.result(timeout=15) 
                                        logger.info(f"✅ Успешно отправлена команда '{cmd}' боту {bot_username}")
                                    except Exception as e:
                                        logger.error(f"❌ Ошибка отправки '{cmd}' боту {bot_username}: {e}")
                                        
                                    time.sleep(2) # Небольшая пауза между командами
                                
                                # Обновляем время последнего запуска
                                db.execute("UPDATE interaction_bots SET last_run = ? WHERE id = ?", (now, interaction['id']))
                                db.commit()
                                
                                if 'notify_clients' in globals():
                                    notify_clients()
                                    
                    except Exception as bot_e:
                        logger.error(f"Сбой в логике расписания для бота {interaction['bot_username']}: {bot_e}")
                             
        except Exception as e:
            logger.error(f"Глобальная ошибка в планировщике ботов: {e}")





# --- Маршруты для Товаров ---
@app.before_request
def check_session_token():
    # Пропускаем проверку для страниц логина и логаута
    if request.endpoint in ['login_page', 'static', 'logout']:
        return
        
    if 'user_id' in session:
        try:
            user = get_session_user_record()
            if has_invalid_session_token(user):
                return build_expired_session_response()
        except Exception:
            pass

import google.generativeai as genai
import json
from flask import request, jsonify

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY") or ""
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)

AUTOFILL_RESPONSE_KEYS = [
    "brand",
    "country_sim",
    "weight",
    "color",
    "storage",
    "ram",
    "display",
    "processor",
    "camera",
    "front_camera",
    "video",
    "connectivity",
    "battery",
    "os",
    "biometrics",
    "charging",
    "warranty",
    "model_no",
    "description",
]

AUTOFILL_REQUIRED_NOTE = "Версия без RuStore"
AUTOFILL_APPLE_NOTE = "Оригинальная продукция Apple"
AUTOFILL_MODELS = ["gemini-2.5-flash", "gemini-2.5-flash-lite"]
AUTOFILL_SUPPLEMENT_MIN_SCORE = 1.2
DEFAULT_IPHONE_OS_VERSION = "iOS 18"
STORE_WARRANTY_DEFAULT = "Гарантия от магазина 1 год"
GENERIC_BRAND_LABELS = {
    "card",
    "cm",
    "аксессуары",
    "аксессуары для",
    "гаджеты",
    "гейминг",
    "геймпады для",
    "детские умные часы",
    "игры и подписки для",
    "наушники и колонки",
    "портативные игровые приставки",
    "планшеты",
    "продукция dyson",
    "смартфоны",
    "умные часы",
    "умные часы и фитнес-браслеты",
}
BRAND_COUNTRY_FALLBACKS = {
    "A4TECH": "Китай",
    "Anker": "Китай",
    "Aukey": "Китай",
    "Awei": "Китай",
    "Asus": "Тайвань",
    "Baseus": "Китай",
    "Belkin": "США",
    "Borofone": "Китай",
    "Canyon": "Китай",
    "Dyson": "Малайзия",
    "Elago": "Южная Корея",
    "ELARI": "Китай",
    "Google": "Вьетнам",
    "HOCO": "Китай",
    "Honor": "Китай",
    "HP": "Китай",
    "Huawei": "Китай",
    "Inkax": "Китай",
    "Kodak": "Китай",
    "Lenovo": "Китай",
    "Ldnio": "Китай",
    "Logitech": "Китай",
    "Native Union": "Гонконг",
    "Nintendo": "Япония",
    "Nothing": "Китай",
    "Nubia": "Китай",
    "OnePlus": "Китай",
    "Philips": "Нидерланды",
    "PlayStation": "Япония",
    "PowerA": "США",
    "reMarkable": "Китай",
    "Ritmix": "Китай",
    "Samsung": "Вьетнам",
    "SanDisk": "США",
    "Satechi": "США",
    "Sony": "Китай",
    "Tecno": "Китай",
    "Transcend": "Тайвань",
    "UAG": "США",
    "Valve": "США",
    "WIWU": "Китай",
    "Whoop": "США",
    "Xbox": "США",
    "Xiaomi": "Китай",
    "Яндекс": "Россия",
}
FOLDER_NAME_TRAILING_FILLER_RE = re.compile(r"\s+для\s*$", re.IGNORECASE)
BRAND_KEYWORDS = (
    ("Anker", ("anker",)),
    ("Aukey", ("aukey",)),
    ("Awei", ("awei",)),
    ("Asus", ("asus", "rog phone", "zenfone")),
    ("Samsung", ("galaxy", "samsung", "z fold", "z flip")),
    ("Xiaomi", ("xiaomi", "redmi", "poco")),
    ("Google", ("pixel", "google")),
    ("Honor", ("honor",)),
    ("Realme", ("realme",)),
    ("OnePlus", ("oneplus", "one plus")),
    ("Nothing", ("nothing phone", "cmf phone", "cmf by nothing")),
    ("Oppo", ("oppo",)),
    ("Vivo", ("vivo",)),
    ("Huawei", ("huawei",)),
    ("Яндекс", ("яндекс", "yandex")),
    ("Motorola", ("motorola", " moto ")),
    ("Sony", ("sony", "xperia")),
    ("Infinix", ("infinix",)),
    ("Tecno", ("tecno",)),
    ("Valve", ("steam deck", "valve")),
    ("Lenovo", ("lenovo legion", "legion go")),
    ("Nintendo", ("nintendo switch", "switch oled", "switch lite")),
    ("PlayStation", ("playstation", " ps5", " ps4")),
    ("Xbox", ("xbox",)),
    ("PowerA", ("powera",)),
    ("Dyson", ("dyson",)),
    ("SanDisk", ("sandisk",)),
    ("Kingston", ("kingston",)),
    ("Philips", ("philips",)),
    ("HP", ("hp ", "hp elite", "hp pro")),
    ("Belkin", ("belkin",)),
    ("Transcend", ("transcend",)),
    ("Baseus", ("baseus",)),
    ("Borofone", ("borofone",)),
    ("uBear", ("ubear",)),
    ("UNIQ", ("uniq",)),
    ("Breaking", ("breaking",)),
    ("Remax", ("remax",)),
    ("Magssory", ("magssory",)),
    ("Netac", ("netac",)),
    ("OltraMax", ("oltramax",)),
    ("TV-COM", ("tv-com",)),
    ("ADATA", ("adata",)),
    ("Silicon Power", ("silicon power",)),
    ("WIWU", ("wiwu",)),
    ("UAG", ("uag", "urban armor gear")),
    ("Elago", ("elago",)),
    ("Satechi", ("satechi",)),
    ("Native Union", ("native union",)),
    ("Ldnio", ("ldnio",)),
    ("Inkax", ("inkax",)),
    ("Anker", ("anker",)),
    ("Awei", ("awei",)),
    ("Aukey", ("aukey",)),
    ("Prime Line", ("prime line",)),
    ("Pitaka", ("pitaka",)),
    ("Deppa", ("deppa",)),
    ("Defender", ("defender",)),
    ("DEXP", ("dexp",)),
    ("Oxion", ("oxion",)),
    ("PNY", ("pny",)),
    ("HOCO", ("hoco",)),
    ("Kodak", ("kodak",)),
    ("JBL", ("jbl",)),
    ("Logitech", ("logitech",)),
    ("Canyon", ("canyon",)),
    ("Ritmix", ("ritmix",)),
    ("A4TECH", ("a4tech",)),
    ("reMarkable", ("remarkable",)),
    ("Whoop", ("whoop",)),
    ("Nubia", ("nubia", "redmagic")),
)
ACCESSORY_COMPATIBILITY_MARKERS = (
    "airpods",
    "apple watch",
    "iphone",
    "ipad",
    "для apple",
    "for apple",
    "для samsung",
    "for samsung",
    "mag safe",
    "magsafe",
)
DESCRIPTION_TEMPLATE_MARKERS = (
    "это понятная и аккуратно оформленная карточка для магазина",
    "карточка, собранная из актуального каталога cmstore",
    "товар оформлен для каталога магазина",
    "ключевые характеристики:",
    "исполнение:",
    "читать полностью",
    "<div",
)
APPLE_DESCRIPTION_NOTE_KINDS = {"smartphone", "tablet", "laptop", "watch"}
ANDROID_BRANDS = {
    "Asus",
    "Google",
    "Honor",
    "Huawei",
    "Motorola",
    "Nothing",
    "OnePlus",
    "Oppo",
    "Realme",
    "Samsung",
    "Sony",
    "Tecno",
    "Vivo",
    "Xiaomi",
}
IPHONE_MODEL_WEIGHT_MAP = {
    "11": "0.194",
    "12": "0.164",
    "13 Mini": "0.141",
    "13": "0.174",
    "14": "0.172",
    "14 Plus": "0.203",
    "15": "0.171",
    "15 Plus": "0.201",
    "15 Pro": "0.187",
    "15 Pro Max": "0.221",
    "16e": "0.167",
    "16": "0.170",
    "16 Plus": "0.199",
    "16 Pro": "0.199",
    "16 Pro Max": "0.227",
    "17e": "0.180",
    "17": "0.190",
    "17 Air": "0.170",
    "17 Pro": "0.200",
    "17 Pro Max": "0.240",
    "SE 2022": "0.144",
}
IPHONE_MODEL_PATTERNS = [
    (r"^17\s+pro\s+max\b", "17 Pro Max"),
    (r"^17\s+pro\b", "17 Pro"),
    (r"^17\s+air\b", "17 Air"),
    (r"^17e\b", "17e"),
    (r"^17\b", "17"),
    (r"^16\s+pro\s+max\b", "16 Pro Max"),
    (r"^16\s+pro\b", "16 Pro"),
    (r"^16\s+plus\b", "16 Plus"),
    (r"^16e\b", "16e"),
    (r"^16\b", "16"),
    (r"^15\s+pro\s+max\b", "15 Pro Max"),
    (r"^15\s+pro\b", "15 Pro"),
    (r"^15\s+plus\b", "15 Plus"),
    (r"^15\b", "15"),
    (r"^14\s+plus\b", "14 Plus"),
    (r"^14\b", "14"),
    (r"^13\s+mini\b", "13 Mini"),
    (r"^13\b", "13"),
    (r"^12\b", "12"),
    (r"^11\b", "11"),
    (r"^se(?:\s+2022)?\b", "SE 2022"),
]
APPLE_PART_NUMBER_PATTERN = re.compile(r"\bM(?=[A-Z0-9]{4}(?:/[A-Z])?\b)(?=[A-Z0-9]*\d)[A-Z0-9]{4}(?:/[A-Z])?\b", re.IGNORECASE)
APPLE_FAMILY_PREFIX_PATTERN = re.compile(
    r"^(macbook|ipad|airpods|apple watch|imac|mac mini|mac studio|vision pro)\b",
    re.IGNORECASE,
)
SIM_INLINE_VARIANT_PATTERN = re.compile(r"\s*\((?:[^)]*\b(?:e\s*sim|sim)\b[^)]*)\)", re.IGNORECASE)
SIM_DESCRIPTION_PATTERN = re.compile(
    r"(?:\b(?:e\s*sim|1\s*sim|2\s*sim|sim\+e\s*sim)\b|sim-карт|sim карт|физическ\w+\s+sim|слот\w*\s+для\s+sim)",
    re.IGNORECASE,
)
AUTOFILL_SPEC_KEYS = [
    "display",
    "processor",
    "camera",
    "front_camera",
    "video",
    "connectivity",
    "battery",
    "os",
    "biometrics",
    "charging",
]
AUTOFILL_CACHE_MAX_AGE_DAYS = 30
AI_REPAIR_JOB_BATCH_SIZE = 10
VARIANT_FOLDER_NAMES = {"esim", "sim+esim", "2sim"}
STORAGE_FOLDER_NAME_RE = re.compile(r"^\d+(?:\.\d+)?(?:gb|tb)$", re.IGNORECASE)
IPHONE_CATALOG_PATH = ("Электроника", "Смартфоны", "iPhone")
DATA_CABLES_CATALOG_PATH = ("Электроника", "Аксессуары", "Зарядные устройства", "Дата-кабели")
NETWORK_CHARGERS_CATALOG_PATH = ("Электроника", "Аксессуары", "Зарядные устройства", "Сетевые зарядные устройства")
WATCH_CHARGERS_CATALOG_PATH = (
    "Электроника",
    "Аксессуары",
    "Зарядные устройства",
    "Зарядные устройства для умных часов и фитнес-браслетов",
)
YANDEX_STATION_CATALOG_PATH = ("Электроника", "Наушники и колонки", "Умные колонки", "Яндекс станция")
JBL_SPEAKER_CATALOG_PATH = (
    "Электроника",
    "Наушники и колонки",
    "Портативные колонки и акустические системы",
    "JBL",
)
PLAYSTATION_SUBSCRIPTIONS_CATALOG_PATH = ("Электроника", "Гейминг", "PlayStation", "Игры и подписки для")
XBOX_SUBSCRIPTIONS_CATALOG_PATH = ("Электроника", "Гейминг", "Xbox", "Игры и подписки для")
STEAM_DECK_CATALOG_PATH = ("Электроника", "Гейминг", "Портативные игровые приставки", "Steam Deck")
CONSTRUCTORS_CATALOG_PATH = ("Электроника", "Гаджеты", "Хобби и отдых", "Конструкторы")
COMPUTER_ACCESSORIES_CATALOG_PATH = ("Электроника", "Аксессуары", "Аксессуары для компьютера")
AIRPODS_CATALOG_PATH = ("Электроника", "Наушники и колонки", "Наушники", "AirPods")
IPHONE_MODEL_FOLDER_PATTERNS = (
    (r"\b17\s+pro\s+max\b", "iPhone 17 Pro Max"),
    (r"\b17\s+pro\b", "iPhone 17 Pro"),
    (r"\b17\s+air\b", "iPhone 17 Air"),
    (r"\b17\s*[eе]\b", "iPhone 17e"),
    (r"\b17\b", "iPhone 17"),
    (r"\b16\s+pro\s+max\b", "iPhone 16 Pro Max"),
    (r"\b16\s+pro\b", "iPhone 16 Pro"),
    (r"\b16\s+plus\b", "iPhone 16 Plus"),
    (r"\b16\s*[eе]\b", "iPhone 16e"),
    (r"\b16\b", "iPhone 16"),
    (r"\b15\s+pro\s+max\b", "iPhone 15 Pro Max"),
    (r"\b15\s+pro\b", "iPhone 15 Pro"),
    (r"\b15\s+plus\b", "iPhone 15 Plus"),
    (r"\b15\b", "iPhone 15"),
    (r"\b14\s+pro\s+max\b", "iPhone 14 Pro Max"),
    (r"\b14\s+pro\b", "iPhone 14 Pro"),
    (r"\b14\s+plus\b", "iPhone 14 Plus"),
    (r"\b14\b", "iPhone 14"),
    (r"\b13\s+mini\b", "iPhone 13 Mini"),
    (r"\b13\b", "iPhone 13"),
    (r"\b12\b", "iPhone 12"),
    (r"\b11\b", "iPhone 11"),
    (r"\bse(?:\s+2022)?\b", "iPhone SE 2022"),
)
IPHONE_17_PRO_ESIM_FLAGS = {"🇧🇭", "🇨🇦", "🇬🇺", "🇯🇵", "🇰🇼", "🇲🇽", "🇴🇲", "🇶🇦", "🇸🇦", "🇦🇪", "🇺🇸", "🇻🇮"}
IPHONE_17_PRO_GLOBAL_FLAGS = {"🇭🇰", "🇫🇷", "🇩🇪", "🇬🇧", "🇮🇹", "🇪🇸", "🇸🇪", "🇳🇱", "🇵🇱", "🇦🇺", "🇳🇿", "🇰🇷", "🇸🇬", "🇪🇺"}
IPHONE_17_PRO_2SIM_FLAGS = {"🇨🇳"}
IPHONE_14_16_ESIM_FLAGS = {"🇺🇸"}
IPHONE_14_16_2SIM_FLAGS = {"🇭🇰", "🇨🇳"}
BRAND_FALLBACK_TOKEN_STOPWORDS = {
    "cm",
    "wi-fi",
    "usb",
    "vga",
    "hdmi",
    "displayport",
    "type-c",
    "lightning",
    "digital",
    "silicone",
    "universal",
    "mini",
}
ACCESSORY_COUNTRY_FALLBACK_MARKERS = (
    "заряд",
    "charger",
    "адаптер",
    "кабель",
    "cable",
    "чех",
    "case",
    "cover",
    "держатель",
    "airpods",
    "науш",
    "гарнитур",
    "колонк",
    "speaker",
    "power bank",
    "powerbank",
    "флеш",
    "карта памяти",
    "ssd",
    "airtag",
    "hub",
    "dock",
    "док-станц",
    "ремеш",
    "strap",
)
BRAND_FALLBACK_PREFIX_RE = re.compile(
    r"^(?:автомобильное зарядное устройство|сетевое зарядное устройство|беспроводное зарядное устройство|"
    r"автомобильный держатель(?: магнитный)?|адаптер-разветвитель для компьютера|адаптер-переходник|"
    r"внешний аккумулятор|дата-кабель|аудиокабель|кабель|чехол(?:-книжка)?|usb[- ]флешка|usb[- ]фонарик|"
    r"детские умные часы|графический планшет|мышь беспроводная|стилус|ремешок на запястье для смартфона|"
    r"батарейка|патч-корд|док-станция|кардхолдер)\s+",
    re.IGNORECASE,
)


def brand_marker_in_text(text, marker):
    haystack = str(text or "").lower()
    needle = str(marker or "").lower()
    if not haystack or not needle:
        return False
    if re.fullmatch(r"[a-z0-9]+", needle):
        return bool(re.search(rf"(?<![a-z0-9]){re.escape(needle)}(?![a-z0-9])", haystack))
    return needle in haystack


def detect_brand_fallback_candidate(product_name):
    original = str(product_name or "").strip()
    normalized = BRAND_FALLBACK_PREFIX_RE.sub("", original, count=1).strip()
    if not normalized:
        normalized = original
    for token in re.findall(r"[A-Za-z][A-Za-z0-9+\-]{1,}", normalized):
        lowered = token.lower()
        if lowered in BRAND_FALLBACK_TOKEN_STOPWORDS:
            continue
        return token
    return ""


def detect_brand_from_name(product_name):
    name = str(product_name or "").lower()
    if APPLE_PART_NUMBER_PATTERN.search(str(product_name or "")):
        return "Apple"
    for brand, markers in BRAND_KEYWORDS:
        if brand == "Apple":
            continue
        if any(brand_marker_in_text(name, marker) for marker in markers) and any(marker in name for marker in ACCESSORY_COMPATIBILITY_MARKERS):
            return brand
    apple_markers = (
        "iphone", "ipad", "macbook", "apple watch", "airpods", "imac", "mac mini", "vision pro"
    )
    if any(marker in name for marker in apple_markers):
        return "Apple"
    if looks_like_iphone_shorthand(product_name):
        return "Apple"
    for brand, markers in BRAND_KEYWORDS:
        if any(brand_marker_in_text(name, marker) for marker in markers):
            return brand
    fallback_brand = sanitize_brand_value(detect_brand_fallback_candidate(product_name))
    if fallback_brand:
        return fallback_brand
    return ""


def normalize_folder_display_name(folder_name):
    display_name = re.sub(r"\s+", " ", str(folder_name or "").strip())
    if not display_name:
        return ""
    return FOLDER_NAME_TRAILING_FILLER_RE.sub("", display_name).strip()


def is_generic_brand_value(value):
    normalized = re.sub(r"\s+", " ", str(value or "").strip()).casefold()
    if not normalized:
        return True
    return normalized in GENERIC_BRAND_LABELS or normalized.startswith("для ")


def sanitize_brand_value(value):
    brand = re.sub(r"\s+", " ", str(value or "").strip())
    return "" if is_generic_brand_value(brand) else brand


def infer_brand_country_fallback(brand):
    return BRAND_COUNTRY_FALLBACKS.get(str(brand or "").strip(), "")


def infer_accessory_country_fallback(product_name, brand=""):
    brand_country = infer_brand_country_fallback(brand)
    if brand_country:
        return brand_country
    normalized = str(product_name or "").lower()
    if any(marker in normalized for marker in ACCESSORY_COUNTRY_FALLBACK_MARKERS):
        return "Китай"
    return ""


def infer_handheld_processor(product_name):
    lower = str(product_name or "").lower()
    if "z2 extreme" in lower:
        return "AMD Ryzen Z2 Extreme"
    if "z1 extreme" in lower:
        return "AMD Ryzen Z1 Extreme"
    if "z2 go" in lower:
        return "AMD Ryzen Z2 Go"
    return ""


def build_known_product_description(product_name, kind_title, highlights, extra_line=""):
    safe_name = re.sub(r"\s+", " ", str(product_name or "").strip())
    lines = [f"{safe_name} — {kind_title}, который хорошо подходит для повседневного использования."]
    if extra_line:
        lines.append(str(extra_line).strip().rstrip(".") + ".")
    if highlights:
        lines.append("Среди основных особенностей — " + ", ".join(highlights[:4]) + ".")
    return "\n\n".join(lines)


def supplement_autofill_payload_from_known_products(product_name, article="", payload=None):
    next_payload = dict(payload or {})
    raw_name = re.sub(r"\s+", " ", str(product_name or "").strip())
    lower = raw_name.lower()
    inferred_ram, inferred_storage = extract_ram_storage_from_name(raw_name)

    def fill_if_missing(key, value):
        value = str(value or "").strip()
        if value and not str(next_payload.get(key) or "").strip():
            next_payload[key] = value

    def fill_many(values):
        for key, value in (values or {}).items():
            fill_if_missing(key, value)

    if lower.startswith("игра для nintendo switch"):
        game_title = re.sub(r"^игра\s+для\s+nintendo\s+switch(?:\s+2)?\s*", "", raw_name, flags=re.IGNORECASE).strip()
        fill_many({
            "brand": "Nintendo",
            "country_sim": "Япония",
            "display": "Зависит от консоли",
            "processor": "Зависит от консоли",
            "camera": "Не применяется",
            "front_camera": "Не применяется",
            "video": "Зависит от режима вывода консоли",
            "connectivity": "Совместимость с Nintendo Switch",
            "battery": "Зависит от консоли",
            "os": "Nintendo Switch system software",
            "biometrics": "Не применяется",
            "charging": "Не применяется",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if game_title and not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "игровое издание для Nintendo Switch",
                [
                    f"Платформа: {'Nintendo Switch 2' if 'switch 2' in lower else 'Nintendo Switch'}",
                    f"Название: {game_title}",
                    "Формат: игровой картридж или цифровая версия",
                ],
            )
        return next_payload

    if "nintendo switch" in lower and any(token in lower for token in ("joy-con", "controller", "powera", "wheel", "геймпад", "руль")):
        accessory_brand = "PowerA" if "powera" in lower else "Nintendo"
        fill_many({
            "brand": accessory_brand,
            "country_sim": infer_brand_country_fallback(accessory_brand),
            "display": "Не применяется",
            "processor": "Не применяется",
            "camera": "Не применяется",
            "front_camera": "Не применяется",
            "video": "Не применяется",
            "connectivity": "Совместимость с Nintendo Switch",
            "battery": "Зависит от модели аксессуара",
            "os": "Не применяется",
            "biometrics": "Не применяется",
            "charging": "Зависит от модели аксессуара",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "игровой аксессуар",
                [
                    f"Бренд: {accessory_brand}",
                    f"Совместимость: {'Nintendo Switch 2' if 'switch 2' in lower else 'Nintendo Switch'}",
                ],
            )
        return next_payload

    if "steam deck oled" in lower:
        fill_many({
            "brand": "Valve",
            "country_sim": infer_brand_country_fallback("Valve"),
            "weight": "0.640",
            "storage": inferred_storage or "512 GB",
            "ram": "16 GB",
            "display": '7.4" OLED HDR, 90 Гц',
            "processor": "AMD APU Zen 2 / RDNA 2",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "1280x800, HDR, 90 Гц",
            "connectivity": "Wi-Fi 6E, Bluetooth 5.3, USB-C",
            "battery": "50 Wh",
            "os": "SteamOS 3",
            "biometrics": "Нет",
            "charging": "USB-C PD 45 W",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 7.4\" OLED HDR", "ОЗУ: 16 GB", f"Память: {inferred_storage or '512 GB'}", "ОС: SteamOS 3"],
            )
        return next_payload

    if "steam deck" in lower:
        fill_many({
            "brand": "Valve",
            "country_sim": infer_brand_country_fallback("Valve"),
            "weight": "0.669",
            "storage": inferred_storage or "256 GB",
            "ram": "16 GB",
            "display": '7.0" IPS, 60 Гц',
            "processor": "AMD APU Zen 2 / RDNA 2",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "1280x800, 60 Гц",
            "connectivity": "Wi-Fi 5, Bluetooth 5.0, USB-C",
            "battery": "40 Wh",
            "os": "SteamOS 3",
            "biometrics": "Нет",
            "charging": "USB-C PD 45 W",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 7.0\" IPS", "ОЗУ: 16 GB", f"Память: {inferred_storage or '256 GB'}", "ОС: SteamOS 3"],
            )
        return next_payload

    if "nintendo switch 2" in lower:
        fill_many({
            "brand": "Nintendo",
            "country_sim": "Япония",
            "weight": "0.534",
            "storage": inferred_storage or "256 GB",
            "display": '7.9" LCD, 120 Гц',
            "processor": "NVIDIA custom processor",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "До 4K в ТВ-режиме / до 1080p в портативном режиме",
            "connectivity": "Wi-Fi 6, Bluetooth, USB-C, NFC",
            "battery": "5220 мАч",
            "os": "Nintendo Switch system software",
            "biometrics": "Нет",
            "charging": "USB-C",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 7.9\" LCD 120 Гц", f"Память: {inferred_storage or '256 GB'}", "Вывод изображения: до 4K в док-режиме"],
            )
        return next_payload

    if "nintendo switch oled" in lower:
        fill_many({
            "brand": "Nintendo",
            "country_sim": "Япония",
            "weight": "0.420",
            "storage": inferred_storage or "64 GB",
            "display": '7.0" OLED',
            "processor": "NVIDIA custom processor",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "До 1080p в ТВ-режиме / 720p в портативном режиме",
            "connectivity": "Wi-Fi, Bluetooth 4.1, USB-C, NFC",
            "battery": "До 9 часов работы",
            "os": "Nintendo Switch system software",
            "biometrics": "Нет",
            "charging": "USB-C",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 7.0\" OLED", f"Память: {inferred_storage or '64 GB'}", "Вывод изображения: до Full HD в ТВ-режиме"],
            )
        return next_payload

    if "nintendo switch lite" in lower:
        fill_many({
            "brand": "Nintendo",
            "country_sim": "Япония",
            "weight": "0.275",
            "storage": inferred_storage or "32 GB",
            "display": '5.5" LCD',
            "processor": "NVIDIA custom processor",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "До 720p на встроенном экране",
            "connectivity": "Wi-Fi, Bluetooth, USB-C",
            "battery": "До 7 часов работы",
            "os": "Nintendo Switch system software",
            "biometrics": "Нет",
            "charging": "USB-C",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 5.5\" LCD", f"Память: {inferred_storage or '32 GB'}", "Формат: компактная портативная версия"],
            )
        return next_payload

    if re.search(r"\bnintendo switch\b", lower):
        fill_many({
            "brand": "Nintendo",
            "country_sim": "Япония",
            "weight": "0.398",
            "storage": inferred_storage or "32 GB",
            "display": '6.2" LCD',
            "processor": "NVIDIA custom processor",
            "camera": "Нет",
            "front_camera": "Нет",
            "video": "До 1080p в ТВ-режиме / 720p в портативном режиме",
            "connectivity": "Wi-Fi, Bluetooth 4.1, USB-C, NFC",
            "battery": "До 9 часов работы",
            "os": "Nintendo Switch system software",
            "biometrics": "Нет",
            "charging": "USB-C",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                ["Дисплей: 6.2\" LCD", f"Память: {inferred_storage or '32 GB'}", "Вывод изображения: до Full HD в ТВ-режиме"],
            )
        return next_payload

    if any(marker in lower for marker in ("rog ally", "legion go", "xbox ally")):
        handheld_brand = sanitize_brand_value(next_payload.get("brand")) or detect_brand_from_inputs(raw_name, article)
        processor = infer_handheld_processor(raw_name)
        fill_many({
            "brand": handheld_brand,
            "country_sim": infer_brand_country_fallback(handheld_brand),
            "storage": inferred_storage,
            "ram": inferred_ram,
            "processor": processor,
            "camera": "Нет",
            "front_camera": "Нет",
            "os": "SteamOS" if "steamos" in lower else "Windows 11",
            "biometrics": "Нет",
            "charging": "USB-C PD",
            "warranty": STORE_WARRANTY_DEFAULT,
        })
        if not str(next_payload.get("description") or "").strip():
            highlights = []
            if processor:
                highlights.append(f"Процессор: {processor}")
            if inferred_ram:
                highlights.append(f"ОЗУ: {inferred_ram}")
            if inferred_storage:
                highlights.append(f"Память: {inferred_storage}")
            next_payload["description"] = build_known_product_description(
                raw_name,
                "портативная игровая консоль",
                highlights,
                "Формат: компактная игровая система для портативного использования.",
            )
        return next_payload

    return next_payload


def has_apple_part_number(product_name):
    return bool(APPLE_PART_NUMBER_PATTERN.search(str(product_name or "")))


def extract_apple_model_no(value):
    match = APPLE_PART_NUMBER_PATTERN.search(str(value or ""))
    if not match:
        return ""
    return match.group(0).split("/")[0].upper()


def detect_brand_from_inputs(product_name, article=""):
    if has_apple_part_number(article) or has_apple_part_number(product_name):
        return "Apple"
    return detect_brand_from_name(product_name)


def looks_like_iphone_shorthand(product_name):
    normalized = str(product_name or "").strip().lower()
    if normalized.startswith("iphone "):
        return True
    if re.match(r"^(?:📱\s*)?se(?:\s+2022)?\b", normalized):
        return True
    return bool(re.match(r"^(?:📱\s*)?(?:\d{2}|\d{2}[a-z]?)(?:\s+(?:pro max|pro|plus|mini|air|e))?\b", normalized))


def detect_iphone_model_family(product_name):
    normalized = re.sub(r"^iphone\s+", "", str(product_name or "").strip(), flags=re.IGNORECASE)
    normalized = SIM_INLINE_VARIANT_PATTERN.sub("", normalized)
    normalized = normalized.lower().strip()
    for pattern, family in IPHONE_MODEL_PATTERNS:
        if re.search(pattern, normalized):
            return family
    return ""


def is_apple_iphone_product(product_name, brand=""):
    normalized_brand = str(brand or "").strip()
    if normalized_brand and normalized_brand != "Apple":
        return False
    if not looks_like_iphone_shorthand(product_name):
        return False
    return normalized_brand == "Apple" or detect_brand_from_name(product_name) == "Apple"


def infer_iphone_weight_from_name(product_name):
    family = detect_iphone_model_family(product_name)
    return normalize_autofill_weight(IPHONE_MODEL_WEIGHT_MAP.get(family, ""))


def normalize_iphone_os_value(value, product_name, brand=""):
    raw = str(value or "").strip()
    if not is_apple_iphone_product(product_name, brand):
        return raw
    if not raw or raw.lower() == "ios":
        return DEFAULT_IPHONE_OS_VERSION
    return raw


def detect_condition_marker(product_name):
    normalized = str(product_name or "").lower()
    if "asis" in normalized:
        return "ASIS"
    if "cpo" in normalized:
        return "CPO"
    return ""


def extract_country_flags(product_name):
    text = str(product_name or "")
    known_flags = (
        IPHONE_17_PRO_ESIM_FLAGS
        | IPHONE_17_PRO_GLOBAL_FLAGS
        | IPHONE_17_PRO_2SIM_FLAGS
        | IPHONE_14_16_ESIM_FLAGS
        | IPHONE_14_16_2SIM_FLAGS
        | {"🇮🇳", "🇷🇺", "🇲🇾", "🇮🇩", "🇰🇿", "🇨🇦", "🇯🇵", "🇦🇪", "🇪🇺", "🇬🇧", "🇦🇺", "🇰🇷"}
    )
    return [flag for flag in known_flags if flag in text]


def infer_iphone_variant_from_flags(product_name):
    name = str(product_name or "")
    flags = set(extract_country_flags(name))
    lower = name.lower()
    if not flags:
        return ""

    if "17 air" in lower:
        if "🇨🇳" in flags:
            return ""
        return "eSim Only"

    if "17 pro max" in lower or re.search(r"\b17\s+pro\b", lower):
        if flags & IPHONE_17_PRO_2SIM_FLAGS:
            return "Китай"
        if flags & IPHONE_17_PRO_ESIM_FLAGS:
            return "eSim Only"
        if flags & IPHONE_17_PRO_GLOBAL_FLAGS:
            return "Глобальная версия"
        return ""

    if re.search(r"\b1[456](?:e)?\b", lower) or "14 plus" in lower or "14 pro" in lower or "15 plus" in lower or "15 pro" in lower or "16 plus" in lower or "16 pro" in lower:
        if flags & IPHONE_14_16_2SIM_FLAGS:
            return "Китай"
        if flags & IPHONE_14_16_ESIM_FLAGS:
            return "eSim Only"
        return "Глобальная версия"

    return ""


def detect_variant_folder_name(product_name):
    normalized = re.sub(r"\s+", "", str(product_name or "").lower())
    if "2sim" in normalized:
        return "2sim"
    if "1sim+esim" in normalized or "sim+esim" in normalized or "1sim" in normalized:
        return "sim+esim"
    if "esim" in normalized:
        return "esim"
    return ""


def detect_country_variant(product_name):
    variant_folder_name = detect_variant_folder_name(product_name)
    if variant_folder_name == "2sim":
        return "Китай"
    if variant_folder_name == "sim+esim":
        return "Глобальная версия"
    if variant_folder_name == "esim":
        return "eSim Only"
    inferred_variant = infer_iphone_variant_from_flags(product_name)
    if inferred_variant:
        return inferred_variant
    return ""


def normalize_folder_id(value):
    if value in (None, "", "null", "None"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def is_variant_folder_name(folder_name):
    return str(folder_name or "").strip().lower() in VARIANT_FOLDER_NAMES


def is_storage_folder_name(folder_name):
    normalized_name = re.sub(r"\s+", "", str(folder_name or "").strip())
    return bool(STORAGE_FOLDER_NAME_RE.fullmatch(normalized_name))


def get_user_folder(db, user_id, folder_id):
    normalized_folder_id = normalize_folder_id(folder_id)
    if not normalized_folder_id:
        return None
    return db.execute(
        "SELECT id, name, parent_id FROM folders WHERE id = ? AND user_id = ?",
        (normalized_folder_id, user_id),
    ).fetchone()


def normalize_folder_lookup_name(folder_name):
    return normalize_folder_display_name(folder_name).casefold()


def find_folder_by_name_and_parent(db, user_id, folder_name, parent_id):
    normalized_name = normalize_folder_lookup_name(folder_name)
    if not normalized_name:
        return None
    normalized_parent_id = normalize_folder_id(parent_id)
    if normalized_parent_id is None:
        rows = db.execute(
            "SELECT id, name, parent_id FROM folders WHERE user_id = ? AND parent_id IS NULL ORDER BY id",
            (user_id,),
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT id, name, parent_id FROM folders WHERE user_id = ? AND parent_id = ? ORDER BY id",
            (user_id, normalized_parent_id),
        ).fetchall()
    for row in rows:
        if normalize_folder_lookup_name(row["name"]) == normalized_name:
            return row
    return None


def ensure_named_folder(db, user_id, folder_name, parent_id):
    display_name = normalize_folder_display_name(folder_name)
    normalized_name = normalize_folder_lookup_name(display_name)
    if not normalized_name:
        return normalize_folder_id(parent_id)
    existing = find_folder_by_name_and_parent(db, user_id, display_name, parent_id)
    if existing:
        return int(existing["id"])
    cursor = db.execute(
        "INSERT INTO folders (user_id, name, parent_id) VALUES (?, ?, ?)",
        (user_id, display_name, normalize_folder_id(parent_id)),
    )
    return int(cursor.lastrowid)


def get_folder_path_names(db, user_id, folder_id):
    normalized_folder_id = normalize_folder_id(folder_id)
    visited = set()
    path = []
    while normalized_folder_id and normalized_folder_id not in visited:
        visited.add(normalized_folder_id)
        folder = get_user_folder(db, user_id, normalized_folder_id)
        if not folder:
            break
        path.append(normalize_folder_display_name(folder["name"]))
        normalized_folder_id = normalize_folder_id(folder["parent_id"])
    return list(reversed(path))


def folder_path_startswith(db, user_id, folder_id, expected_path):
    actual_path = [part.lower() for part in get_folder_path_names(db, user_id, folder_id)]
    expected = [str(part or "").strip().lower() for part in expected_path if str(part or "").strip()]
    if len(actual_path) < len(expected):
        return False
    return actual_path[:len(expected)] == expected


def detect_storage_folder_name(product_name="", storage_value=""):
    normalized_storage = normalize_capacity(storage_value, allow_tb=True)
    if normalized_storage:
        return normalized_storage.replace(" ", "")

    text = str(product_name or "")
    explicit_match = re.search(r"\b(\d+(?:\.\d+)?)\s*(TB|GB)\b", text, re.IGNORECASE)
    if explicit_match:
        return f"{explicit_match.group(1)}{explicit_match.group(2).upper()}"

    bare_match = re.search(r"\b(64|128|256|512|1024|2048)\b", text)
    if not bare_match:
        return ""

    amount = bare_match.group(1)
    if amount == "1024":
        return "1TB"
    if amount == "2048":
        return "2TB"
    return f"{amount}GB"


def normalize_folder_path_parts(parts):
    normalized_parts = []
    for part in parts or ():
        display_name = normalize_folder_display_name(part)
        if display_name:
            normalized_parts.append(display_name)
    return tuple(normalized_parts)


def folder_path_equals_parts(actual_path, expected_path):
    actual = [part.casefold() for part in normalize_folder_path_parts(actual_path)]
    expected = [part.casefold() for part in normalize_folder_path_parts(expected_path)]
    return actual == expected


def infer_airpods_model_folder_name(product_name):
    normalized_name = re.sub(r"\s+", " ", str(product_name or "").strip()).lower()
    if not normalized_name:
        return None
    if "airpods max 2" in normalized_name:
        return "AirPods Max 2"
    if "airpods max" in normalized_name:
        return "AirPods Max"
    if "airpods pro 3" in normalized_name:
        return "AirPods Pro 3"
    if "airpods pro 2" in normalized_name or ("airpods pro" in normalized_name and "type-c" in normalized_name):
        return "AirPods Pro 2 (Type-C)"
    if "airpods 4" in normalized_name:
        if any(marker in normalized_name for marker in ("anc", "шумоподав", "noise cancel")):
            return "AirPods 4 с активным шумоподавлением"
        return "AirPods 4"
    if "airpods 3" in normalized_name:
        return "AirPods 3"
    if "airpods 2" in normalized_name:
        return "AirPods 2"
    return None


def infer_specific_catalog_folder_path(product_name, current_path, storage_value=""):
    normalized_name = re.sub(r"\s+", " ", str(product_name or "").strip())
    lower_name = normalized_name.lower()
    normalized_path = normalize_folder_path_parts(current_path)
    if not normalized_name or not normalized_path:
        return None

    if folder_path_equals_parts(normalized_path, DATA_CABLES_CATALOG_PATH):
        if "apple watch" in lower_name:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Watch")
        if "galaxy watch" in lower_name or "samsung watch" in lower_name:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Samsung")
        if any(marker in lower_name for marker in ("xiaomi watch", "redmi watch", "mi band")):
            return (*WATCH_CHARGERS_CATALOG_PATH, "Xiaomi")
        if "magsafe 3" in lower_name or ("mag safe 3" in lower_name) or ("macbook" in lower_name and "magsafe" in lower_name):
            return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ для ноутбука")
        if "lightning" in lower_name:
            return (*DATA_CABLES_CATALOG_PATH, "Дата-кабели Lightning")
        if "micro" in lower_name:
            return (*DATA_CABLES_CATALOG_PATH, "Дата-кабели Micro")
        if any(marker in lower_name for marker in ("type-c", "usb-c", "thunderbolt")):
            return (*DATA_CABLES_CATALOG_PATH, "Дата-кабели Type-C")
        return (*DATA_CABLES_CATALOG_PATH, "Прочие дата-кабели")

    if folder_path_equals_parts(normalized_path, NETWORK_CHARGERS_CATALOG_PATH):
        watch_target = None
        has_apple_watch = "apple watch" in lower_name
        has_samsung_watch = "galaxy watch" in lower_name or "samsung watch" in lower_name
        has_xiaomi_watch = any(marker in lower_name for marker in ("xiaomi watch", "redmi watch", "mi band"))
        watch_target_count = sum((has_apple_watch, has_samsung_watch, has_xiaomi_watch))
        if watch_target_count > 1:
            watch_target = "Универсальные"
        elif has_apple_watch:
            watch_target = "Watch"
        elif has_samsung_watch:
            watch_target = "Samsung"
        elif has_xiaomi_watch:
            watch_target = "Xiaomi"
        if watch_target:
            return (*WATCH_CHARGERS_CATALOG_PATH, watch_target)

        if any(marker in lower_name for marker in ("macbook", "magsafe", "mag safe", "ноутбук")):
            return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ для ноутбука")

        has_bundle_marker = any(marker in lower_name for marker in ("с кабелем", "+ кабель", "with cable"))
        if has_bundle_marker and "lightning" in lower_name:
            return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ + Lightning")
        if has_bundle_marker and "micro" in lower_name:
            return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ + Micro")
        if has_bundle_marker and any(marker in lower_name for marker in ("type-c", "usb-c")):
            return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ + Type-C")
        return (*NETWORK_CHARGERS_CATALOG_PATH, "СЗУ блок")

    if folder_path_equals_parts(normalized_path, WATCH_CHARGERS_CATALOG_PATH):
        has_apple_watch = "apple watch" in lower_name
        has_samsung_watch = "galaxy watch" in lower_name or "samsung watch" in lower_name
        has_xiaomi_watch = any(marker in lower_name for marker in ("xiaomi watch", "redmi watch", "mi band"))
        target_count = sum((has_apple_watch, has_samsung_watch, has_xiaomi_watch))
        if target_count > 1:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Универсальные")
        if has_apple_watch:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Watch")
        if has_samsung_watch:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Samsung")
        if has_xiaomi_watch:
            return (*WATCH_CHARGERS_CATALOG_PATH, "Xiaomi")
        return (*WATCH_CHARGERS_CATALOG_PATH, "Универсальные")

    if folder_path_equals_parts(normalized_path, YANDEX_STATION_CATALOG_PATH):
        if "мини 3 pro" in lower_name or "mini 3 pro" in lower_name:
            return (*YANDEX_STATION_CATALOG_PATH, "Станция Мини 3 Pro")
        if "мини 3" in lower_name or "mini 3" in lower_name:
            return (*YANDEX_STATION_CATALOG_PATH, "Станция Мини 3")
        if re.search(r"\bстанция\s+3\b", lower_name) and "мини" not in lower_name:
            return (*YANDEX_STATION_CATALOG_PATH, "Станция 3")

    if folder_path_equals_parts(normalized_path, JBL_SPEAKER_CATALOG_PATH):
        if "partybox" in lower_name:
            return (*JBL_SPEAKER_CATALOG_PATH, "PartyBox")

    if folder_path_equals_parts(normalized_path, PLAYSTATION_SUBSCRIPTIONS_CATALOG_PATH):
        if any(marker in lower_name for marker in ("подпис", "plus", "store", "учетной записи", "учётной записи", "аккаунт", "пополнен")):
            return (*PLAYSTATION_SUBSCRIPTIONS_CATALOG_PATH, "Подписки")
        return (*PLAYSTATION_SUBSCRIPTIONS_CATALOG_PATH, "Игры")

    if folder_path_equals_parts(normalized_path, XBOX_SUBSCRIPTIONS_CATALOG_PATH):
        if any(marker in lower_name for marker in ("подпис", "game pass", "core", "ultimate", "live", "gold", "пополнен")):
            return (*XBOX_SUBSCRIPTIONS_CATALOG_PATH, "Подписки")
        return (*XBOX_SUBSCRIPTIONS_CATALOG_PATH, "Игры")

    if folder_path_equals_parts(normalized_path, STEAM_DECK_CATALOG_PATH):
        target_storage_folder = detect_storage_folder_name(normalized_name, storage_value)
        if target_storage_folder:
            return (*STEAM_DECK_CATALOG_PATH, target_storage_folder.replace(" ", ""))

    if folder_path_equals_parts(normalized_path, CONSTRUCTORS_CATALOG_PATH):
        if "franzis" in lower_name:
            return (*CONSTRUCTORS_CATALOG_PATH, "Franzis")
        if "lego" in lower_name or re.search(r"\btechnic\b", lower_name):
            return (*CONSTRUCTORS_CATALOG_PATH, "LEGO")
        if any(marker in lower_name for marker in ("xiaomi", "onebot", "dawn of jupiter")):
            return (*CONSTRUCTORS_CATALOG_PATH, "Xiaomi")

    if folder_path_equals_parts(normalized_path, COMPUTER_ACCESSORIES_CATALOG_PATH):
        if any(marker in lower_name for marker in ("чех", "case", "storage box", "organizer")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Чехлы и органайзеры")
        if any(marker in lower_name for marker in ("клавиат", "keyboard")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Клавиатуры")
        if any(marker in lower_name for marker in ("мыш", "mouse", "трекпад", "trackpad")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Мышки и трекпады")
        if any(marker in lower_name for marker in ("веб-кам", "webcam")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Веб-камеры")
        if any(marker in lower_name for marker in ("router", "роутер", "wi-fi", "wifi")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Роутеры и адаптеры Wi-Fi")
        if any(marker in lower_name for marker in ("hub", "адаптер", "adapter", "dock", "док-станц")):
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Разветвители и адаптеры")
        if "графический планшет" in lower_name:
            return (*COMPUTER_ACCESSORIES_CATALOG_PATH, "Графические планшеты")

    if folder_path_equals_parts(normalized_path, AIRPODS_CATALOG_PATH):
        if any(marker in lower_name for marker in ("подстав", "holder", "stand")):
            return ("Электроника", "Аксессуары", "Прочие аксессуары", "Подставки")
        model_folder_name = infer_airpods_model_folder_name(normalized_name)
        if model_folder_name:
            return (*AIRPODS_CATALOG_PATH, model_folder_name)

    return None


def infer_iphone_model_folder_name(product_name):
    normalized_name = re.sub(r"\s+", " ", str(product_name or "").replace("ё", "е").lower()).strip()
    if not normalized_name:
        return None
    for pattern, folder_name in IPHONE_MODEL_FOLDER_PATTERNS:
        if re.search(pattern, normalized_name):
            return folder_name
    return None


def ensure_folder_path(db, user_id, folder_names):
    parent_id = None
    for folder_name in folder_names:
        parent_id = ensure_named_folder(db, user_id, folder_name, parent_id)
    return parent_id


def find_or_create_default_model_folder(db, user_id, product_name):
    if not is_apple_iphone_product(product_name):
        return None
    iphone_model_folder_name = infer_iphone_model_folder_name(product_name)
    if iphone_model_folder_name:
        return ensure_folder_path(db, user_id, (*IPHONE_CATALOG_PATH, iphone_model_folder_name))
    return None


def resolve_product_folder_assignment(db, user_id, product_name, folder_id=None, storage_value=""):
    normalized_folder_id = normalize_folder_id(folder_id)
    target_variant_folder = detect_variant_folder_name(product_name)
    target_storage_folder = detect_storage_folder_name(product_name, storage_value)
    inferred_model_folder_id = find_or_create_default_model_folder(db, user_id, product_name)

    current_folder = None
    if normalized_folder_id:
        if inferred_model_folder_id and not folder_path_startswith(db, user_id, normalized_folder_id, IPHONE_CATALOG_PATH):
            current_folder = None
        else:
            current_folder = get_user_folder(db, user_id, normalized_folder_id)

    model_folder_id = None
    variant_folder_id = None
    if current_folder:
        current_name = str(current_folder["name"] or "").strip()
        current_name_lower = current_name.lower()
        current_parent_id = normalize_folder_id(current_folder["parent_id"])
        current_path = get_folder_path_names(db, user_id, current_folder["id"])

        if is_storage_folder_name(current_name):
            parent_variant_folder = get_user_folder(db, user_id, current_parent_id)
            if parent_variant_folder and is_variant_folder_name(parent_variant_folder["name"]):
                model_folder_id = normalize_folder_id(parent_variant_folder["parent_id"])
                if not target_variant_folder or str(parent_variant_folder["name"] or "").strip().lower() == target_variant_folder:
                    variant_folder_id = int(parent_variant_folder["id"])
                    if not target_storage_folder or current_name_lower == target_storage_folder.lower():
                        return int(current_folder["id"])
            else:
                model_folder_id = current_parent_id
                if not target_variant_folder and (not target_storage_folder or current_name_lower == target_storage_folder.lower()):
                    return int(current_folder["id"])
        elif is_variant_folder_name(current_name):
            model_folder_id = current_parent_id
            if not target_variant_folder or current_name_lower == target_variant_folder:
                variant_folder_id = int(current_folder["id"])
        elif inferred_model_folder_id and [part.lower() for part in current_path[:len(IPHONE_CATALOG_PATH)]] == [part.lower() for part in IPHONE_CATALOG_PATH]:
            if len(current_path) >= len(IPHONE_CATALOG_PATH) + 1:
                model_folder_id = int(current_folder["id"])
        else:
            model_folder_id = int(current_folder["id"])

    if inferred_model_folder_id and not folder_path_startswith(db, user_id, model_folder_id, IPHONE_CATALOG_PATH):
        model_folder_id = inferred_model_folder_id
    if model_folder_id is None:
        model_folder_id = inferred_model_folder_id

    iphone_folder_context = folder_path_startswith(db, user_id, model_folder_id, IPHONE_CATALOG_PATH)

    if target_variant_folder and iphone_folder_context:
        if variant_folder_id is None:
            if model_folder_id is None:
                return normalized_folder_id
            variant_folder_id = ensure_named_folder(db, user_id, target_variant_folder, model_folder_id)
        if target_storage_folder:
            return ensure_named_folder(db, user_id, target_storage_folder, variant_folder_id)
        return variant_folder_id

    if target_storage_folder and model_folder_id is not None and iphone_folder_context:
        resolved_folder_id = ensure_named_folder(db, user_id, target_storage_folder, model_folder_id)
    else:
        resolved_folder_id = model_folder_id if model_folder_id is not None else normalized_folder_id

    refined_current_path = get_folder_path_names(db, user_id, resolved_folder_id) if resolved_folder_id else []
    refined_catalog_path = infer_specific_catalog_folder_path(product_name, refined_current_path, storage_value)
    if refined_catalog_path:
        return ensure_folder_path(db, user_id, refined_catalog_path)
    return resolved_folder_id


def normalize_autofill_weight(value):
    raw = str(value or "").strip().replace(",", ".")
    if not raw:
        return ""
    match = re.search(r"(\d+(?:\.\d+)?)", raw)
    if not match:
        return ""
    try:
        numeric = float(match.group(1))
    except ValueError:
        return ""
    lowered = raw.lower()
    if "кг" in lowered or "kg" in lowered:
        grams = numeric * 1000.0
    elif "г" in lowered:
        grams = numeric
    else:
        grams = numeric * 1000.0 if numeric < 10 else numeric
    if grams <= 0 or grams >= 100000:
        return ""
    return str(int(round(grams)))


def normalize_capacity(value, allow_tb=False):
    raw = str(value or "").strip().upper().replace(" ", "")
    if not raw:
        return ""
    match = re.fullmatch(r"(\d+(?:\.\d+)?)(GB|TB)", raw)
    if not match:
        return ""
    amount, unit = match.groups()
    if unit == "TB" and not allow_tb:
        return ""
    return f"{amount} {unit}"


def capacity_to_gb(value):
    normalized = normalize_capacity(value, allow_tb=True)
    if not normalized:
        return 0.0
    amount_str, unit = normalized.split()
    amount = float(amount_str)
    return amount * 1024.0 if unit == "TB" else amount


def extract_ram_storage_from_name(product_name):
    raw_name = str(product_name or "").replace("ГБ", "GB").replace("гб", "GB").replace("ТБ", "TB").replace("тб", "TB")
    pair_patterns = (
        re.compile(r"\b(\d{1,3})\s*/\s*(\d{2,4})\s*(GB|TB)\b", re.IGNORECASE),
        re.compile(r"\b(\d{1,3})\s*(GB)\s*/\s*(\d{2,4})\s*(GB|TB)\b", re.IGNORECASE),
    )
    for pattern in pair_patterns:
        match = pattern.search(raw_name)
        if not match:
            continue
        groups = match.groups()
        if len(groups) == 3:
            ram_amount, storage_amount, storage_unit = groups
            ram_value = normalize_capacity(f"{ram_amount}GB", allow_tb=False)
            storage_value = normalize_capacity(f"{storage_amount}{storage_unit}", allow_tb=True)
        else:
            ram_amount, ram_unit, storage_amount, storage_unit = groups
            ram_value = normalize_capacity(f"{ram_amount}{ram_unit}", allow_tb=False)
            storage_value = normalize_capacity(f"{storage_amount}{storage_unit}", allow_tb=True)
        if ram_value or storage_value:
            return ram_value, storage_value

    capacities = []
    for amount, unit in re.findall(r"\b(\d+(?:\.\d+)?)\s*(GB|TB)\b", raw_name, flags=re.IGNORECASE):
        normalized = normalize_capacity(f"{amount}{unit}", allow_tb=True)
        if normalized:
            capacities.append(normalized)
    unique_capacities = []
    seen = set()
    for capacity in capacities:
        key = capacity.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_capacities.append(capacity)
    if not unique_capacities:
        return "", ""
    if len(unique_capacities) == 1:
        return "", unique_capacities[0]
    ordered = sorted(unique_capacities, key=capacity_to_gb)
    ram_candidate = ordered[0] if capacity_to_gb(ordered[0]) <= 64 else ""
    storage_candidate = ordered[-1]
    if ram_candidate and ram_candidate.lower() == storage_candidate.lower():
        ram_candidate = ""
    return ram_candidate, storage_candidate


def infer_basic_os(product_name, brand=""):
    lower = str(product_name or "").lower()
    normalized_brand = str(brand or "").strip()
    if normalized_brand == "Apple":
        if "ipad" in lower:
            return "iPadOS"
        if "watch" in lower:
            return "watchOS"
        if any(marker in lower for marker in ("macbook", "imac", "mac mini", "mac studio", "mac pro")):
            return "macOS"
        if "vision pro" in lower:
            return "visionOS"
        if any(marker in lower for marker in ("iphone",)):
            return DEFAULT_IPHONE_OS_VERSION
        return ""
    if normalized_brand in ANDROID_BRANDS:
        return "Android"
    if "galaxy watch" in lower or "wear os" in lower:
        return "Wear OS"
    return ""


def normalize_model_no(value, brand):
    model_no = str(value or "").strip().upper()
    if not model_no:
        return ""
    if brand == "Apple":
        return model_no if re.fullmatch(r"M[A-Z0-9]{4}", model_no) else ""
    if len(model_no) > 32:
        return ""
    if not re.fullmatch(r"[A-Z0-9][A-Z0-9\-/ ]{1,31}", model_no):
        return ""
    if not re.search(r"\d", model_no):
        return ""
    return model_no


def strip_description_notes(description):
    raw_text = str(description or "").replace("\r", "")
    raw_text = re.sub(r"<[^>]+>", " ", raw_text)
    raw_text = raw_text.replace("&nbsp;", " ").replace("&amp;", "&").replace("&quot;", '"')
    raw_text = re.sub(r"читать полностью.*", "", raw_text, flags=re.IGNORECASE)
    for note in (AUTOFILL_REQUIRED_NOTE, AUTOFILL_APPLE_NOTE):
        raw_text = re.sub(rf"\s*{re.escape(note)}\s*", "\n", raw_text, flags=re.IGNORECASE)
    raw_text = re.sub(r"\s*без\s+rustore\s*", "\n", raw_text, flags=re.IGNORECASE)
    lines = [line.rstrip() for line in raw_text.split("\n")]
    banned_lines = {
        AUTOFILL_REQUIRED_NOTE.lower(),
        AUTOFILL_APPLE_NOTE.lower(),
    }
    cleaned_lines = []
    for line in lines:
        normalized_line = line.strip().lower()
        if not normalized_line:
            cleaned_lines.append(line)
            continue
        if normalized_line in banned_lines:
            continue
        if any(marker in normalized_line for marker in DESCRIPTION_TEMPLATE_MARKERS):
            continue
        if normalized_line.startswith("преимущества модели:"):
            continue
        if normalized_line.startswith("исполнение:") or normalized_line.startswith("ключевые характеристики:"):
            continue
        if "rustore" in normalized_line:
            continue
        if normalized_line.startswith("недостатки:"):
            continue
        if normalized_line == "◊ юридическая информация" or normalized_line.startswith("юридическая информация"):
            continue
        if "apple.com/" in normalized_line:
            continue
        cleaned_lines.append(line)
    return "\n".join(cleaned_lines).strip()


def remove_sim_mentions_from_description(description):
    text = SIM_INLINE_VARIANT_PATTERN.sub("", str(description or "").replace("\r", ""))
    parts = [part.strip() for part in re.split(r"\n\s*\n", text) if part.strip()]
    cleaned_parts = []
    for part in parts:
        if SIM_DESCRIPTION_PATTERN.search(part):
            continue
        cleaned_parts.append(part)
    return "\n\n".join(cleaned_parts).strip()


def infer_description_kind_from_name(product_name):
    lower = str(product_name or "").lower()
    if not lower:
        return ""
    if any(marker in lower for marker in ("чех", "case", "cover")):
        return "case"
    if any(marker in lower for marker in ("ремеш", "strap", "браслет для")):
        return "strap"
    if any(marker in lower for marker in ("стекл", "пленк", "protector", "glass")):
        return "protection"
    if any(marker in lower for marker in ("держател", "mount", "stand", "подстав")):
        return "mount"
    if any(marker in lower for marker in ("кабель", "cable")):
        return "cable"
    if any(marker in lower for marker in ("заряд", "charger", "адаптер")):
        return "charger"
    if any(marker in lower for marker in ("airpods", "науш", "гарнитур", "buds")):
        return "headphones"
    if any(marker in lower for marker in ("колонк", "speaker", "homepod", "станция")):
        return "speaker"
    if any(marker in lower for marker in ("watch", "часы", "fit")):
        return "watch"
    if any(marker in lower for marker in ("ipad", "планш")):
        return "tablet"
    if any(marker in lower for marker in ("macbook", "ноут", "laptop")):
        return "laptop"
    if looks_like_iphone_shorthand(product_name) or any(marker in lower for marker in ("iphone", "смартфон", "galaxy", "pixel", "redmi note", "nothing phone")):
        return "smartphone"
    return ""


def should_append_apple_notes(brand, product_name="", kind=""):
    if str(brand or "").strip() != "Apple":
        return False
    effective_kind = str(kind or "").strip().lower() or infer_description_kind_from_name(product_name)
    if effective_kind in APPLE_DESCRIPTION_NOTE_KINDS:
        return True
    lower = str(product_name or "").lower()
    return any(marker in lower for marker in ("iphone", "ipad", "macbook", "apple watch", "imac", "mac mini", "vision pro"))


def finalize_description(description, brand, product_name="", kind=""):
    base_description = remove_sim_mentions_from_description(strip_description_notes(description))
    parts = []
    seen_parts = set()
    for part in re.split(r"\n\s*\n", base_description):
        cleaned_part = re.sub(r"\s+", " ", str(part or "").strip())
        normalized_part = cleaned_part.lower()
        if not cleaned_part or normalized_part in seen_parts:
            continue
        seen_parts.add(normalized_part)
        parts.append(cleaned_part)
    return "\n\n".join(parts[:4]).strip()


def build_synonyms(product_name, brand, article=""):
    original = re.sub(r"\s+", " ", str(product_name or "").strip())
    if not original:
        return []

    def append_variant(variants_list, value):
        cleaned = re.sub(r"\s+", " ", str(value or "").strip())
        if cleaned:
            variants_list.append(cleaned)

    variants = []
    variant_match = re.search(r"\(([^)]*(?:e\s*sim|sim)[^)]*)\)", original, re.IGNORECASE)
    variant_suffix = variant_match.group(1).strip() if variant_match else ""
    variant_suffix_lower = variant_suffix.lower()
    no_variant = (
        original[:variant_match.start()] + " " + original[variant_match.end():]
        if variant_match else original
    )
    no_variant = re.sub(r"\s+", " ", no_variant).strip()
    storage_case_normalized = re.sub(
        r"\b(\d+(?:\.\d+)?)\s*(gb|tb)\b",
        lambda match: f"{match.group(1)}{match.group(2).upper()}",
        no_variant,
        flags=re.IGNORECASE,
    )
    storage_case_normalized = re.sub(r"\s+", " ", storage_case_normalized).strip()
    normalized_storage = re.sub(
        r"(?<!\d)(\d{3,4})(?!\s*(?:GB|TB)\b)(?=\s+[A-Za-z])",
        r"\1GB",
        storage_case_normalized,
        flags=re.IGNORECASE,
    )
    normalized_storage = re.sub(r"\s+", " ", normalized_storage).strip()
    base_name = normalized_storage or storage_case_normalized or no_variant
    article_model_no = extract_apple_model_no(article) or extract_apple_model_no(product_name)

    if no_variant and no_variant.lower() != original.lower():
        append_variant(variants, no_variant)
    if storage_case_normalized and storage_case_normalized.lower() not in {original.lower(), no_variant.lower()}:
        append_variant(variants, storage_case_normalized)
    if no_variant and variant_suffix:
        append_variant(variants, f"{no_variant} {variant_suffix}")
        if variant_suffix_lower == "1sim+esim":
            append_variant(variants, f"{no_variant} 1Sim")
        elif variant_suffix_lower == "esim":
            append_variant(variants, f"{no_variant} eSim")
        elif variant_suffix_lower == "2sim":
            append_variant(variants, f"{no_variant} 2Sim")
    if storage_case_normalized and variant_suffix:
        append_variant(variants, f"{storage_case_normalized} {variant_suffix}")
        if variant_suffix_lower == "1sim+esim":
            append_variant(variants, f"{storage_case_normalized} 1Sim")
        elif variant_suffix_lower == "esim":
            append_variant(variants, f"{storage_case_normalized} eSim")
        elif variant_suffix_lower == "2sim":
            append_variant(variants, f"{storage_case_normalized} 2Sim")
    if normalized_storage and normalized_storage.lower() not in {original.lower(), no_variant.lower()}:
        append_variant(variants, normalized_storage)
        if variant_suffix:
            append_variant(variants, f"{normalized_storage} {variant_suffix}")
            if variant_suffix_lower == "1sim+esim":
                append_variant(variants, f"{normalized_storage} 1Sim")
            elif variant_suffix_lower == "esim":
                append_variant(variants, f"{normalized_storage} eSim")
            elif variant_suffix_lower == "2sim":
                append_variant(variants, f"{normalized_storage} 2Sim")
    if (
        brand == "Apple"
        and normalized_storage
        and looks_like_iphone_shorthand(original)
        and not normalized_storage.lower().startswith("iphone ")
        and not has_apple_part_number(original)
    ):
        append_variant(variants, f"iPhone {normalized_storage}")
        if variant_suffix:
            append_variant(variants, f"iPhone {normalized_storage} {variant_suffix}")
            if variant_suffix_lower == "1sim+esim":
                append_variant(variants, f"iPhone {normalized_storage} 1Sim")
            elif variant_suffix_lower == "esim":
                append_variant(variants, f"iPhone {normalized_storage} eSim")
            elif variant_suffix_lower == "2sim":
                append_variant(variants, f"iPhone {normalized_storage} 2Sim")
    if brand == "Apple" and base_name and APPLE_FAMILY_PREFIX_PATTERN.match(base_name):
        if not base_name.lower().startswith("apple "):
            append_variant(variants, f"Apple {base_name}")
            if variant_suffix:
                append_variant(variants, f"Apple {base_name} {variant_suffix}")
        if article_model_no:
            append_variant(variants, f"{base_name} {article_model_no}")
            if not base_name.lower().startswith("apple "):
                append_variant(variants, f"Apple {base_name} {article_model_no}")

    unique_variants = []
    seen = {original.lower()}
    for variant in variants:
        key = variant.lower()
        if not variant or key in seen:
            continue
        seen.add(key)
        unique_variants.append(variant)
        if len(unique_variants) >= 5:
            break
    return unique_variants


def build_offline_autofill_payload(product_name, article=""):
    brand = sanitize_brand_value(detect_brand_from_inputs(product_name, article))
    ram_value, storage_value = extract_ram_storage_from_name(product_name)
    apple_model_no = extract_apple_model_no(article) or extract_apple_model_no(product_name)
    payload = {key: "" for key in AUTOFILL_RESPONSE_KEYS}
    payload["brand"] = brand
    payload["country_sim"] = detect_country_variant(product_name) or infer_accessory_country_fallback(product_name, brand)
    payload["weight"] = infer_iphone_weight_from_name(product_name) if is_apple_iphone_product(product_name, brand) else ""
    payload["storage"] = storage_value
    payload["ram"] = ram_value
    payload["model_no"] = apple_model_no if brand == "Apple" else ""
    payload["os"] = infer_basic_os(product_name, brand)
    payload["warranty"] = STORE_WARRANTY_DEFAULT
    payload["description"] = ""
    payload = supplement_autofill_payload_from_known_products(product_name, article, payload)
    payload = supplement_autofill_payload_from_catalog(product_name, article, payload)
    return normalize_autofill_payload(product_name, payload, article)


def build_autofill_prompt(product_name, article=""):
    article_line = f'Артикул / part number: "{article}"\n' if article else ""
    return f"""
Ты заполняешь карточку товара для магазина в русском стиле.
Товар: "{product_name}"
{article_line}Данные должны быть актуальны на 2026 год и позже, включая текущее состояние на момент запроса.
Если можешь использовать онлайн-поиск, обязательно опирайся на него. Если в точности не уверен, оставляй поле пустым.
Если артикул передан, сначала выполни поиск именно по точному артикулу в кавычках и только потом сверяй название товара.

СТИЛЬ КАРТОЧКИ:
1. Описание только на русском языке.
2. Тон строго под российский интернет-магазин: деловой, понятный, без англоязычного маркетингового мусора.
3. 3 коротких абзаца без списков и markdown.
4. Первое предложение начинай с точного названия модели в стиле: "iPhone 17 Pro — ...", "Samsung Galaxy S25 — ...".
5. Не добавляй в описание служебные пометки вроде "Версия без RuStore", "Без RuStore", "Оригинальная продукция Apple".
6. Не перечисляй характеристики списком внутри описания и не дублируй поля карточки дословно.

ПРАВИЛА:
- Если название вида "17", "17 Pro", "17 Pro Max", "17 Air", "17e" без бренда, считай что это iPhone соответствующей модели.
- Если в названии есть ASIS, это б/у товар. Не описывай его как новый, запечатанный или заводской.
- Если в названии есть CPO, это Certified Pre-Owned. Тоже не описывай как абсолютно новый товар.
- Флаги стран вроде 🇮🇳 🇦🇪 🇭🇰 🇯🇵 — это регион поставки/происхождения и подсказка для поиска, но НЕ значение поля country_sim.
- Если в названии есть Apple part number вроде "MQL03", "MU773", "MC7X4", ты ОБЯЗАН определить устройство именно по part number через онлайн-поиск. Никогда не трактуй такой код как номер iPhone.
- Если артикул передан отдельно, он важнее названия для определения точной модели и конфигурации.
- При конфликте между названием и артикулом доверяй артикулу, а не догадке по названию.
- Для Apple part number сначала ищи точное совпадение part number, затем сверяй цвет, память и тип устройства. Если тип устройства не совпал, не заполняй догадкой.
- Если запрос невозможно подтвердить онлайн, лучше верни пустые поля, чем неправильную модель.
- Если это Apple, постарайся заполнить максимум подтверждаемых полей в стиле карточек магазина: display, processor, camera, front_camera, connectivity, os, biometrics, charging.
- Не выдумывай артикулы, вес, камеры, RAM, частоты, батарею и SIM-версию.
- Если поле не удалось подтвердить, верни "".
- Цвет только на английском, как в названии.
- storage и ram возвращай только с пробелом: "256 GB", "8 GB", "1 TB".
- Гарантия всегда "12 месяцев".
- country_sim только из этого набора, если можно определить по названию:
  eSIM -> "eSim Only"
  1SIM+eSIM / SIM+eSIM -> "Глобальная версия"
  2SIM -> "Китай"
- model_no для Apple только если это реальный код из 5 символов, начинающийся на M.

Верни только JSON без пояснений.
{{
"brand": "",
"country_sim": "",
"weight": "",
"color": "",
"storage": "",
"ram": "",
"display": "",
"processor": "",
"camera": "",
"front_camera": "",
"video": "",
"connectivity": "",
"battery": "",
"os": "",
"biometrics": "",
"charging": "",
"warranty": "12 месяцев",
"model_no": "",
"description": ""
}}
    """


def call_gemini_online(prompt):
    if not google_genai_sdk or not google_genai_types:
        raise RuntimeError("Модуль google-genai не установлен, онлайн-поиск Gemini недоступен")
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY не задан")
    client = google_genai_sdk.Client(api_key=GEMINI_API_KEY)
    last_error = None
    for model_name in AUTOFILL_MODELS:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config=google_genai_types.GenerateContentConfig(
                    temperature=0.1,
                    tools=[google_genai_types.Tool(google_search=google_genai_types.GoogleSearch())],
                ),
            )
            response_text = getattr(response, "text", None)
            if response_text and response_text.strip():
                return response_text.strip()
            raise RuntimeError(f"{model_name} вернул пустой ответ после онлайн-поиска")
        except Exception as e:
            last_error = e
            logger.warning(f"Gemini online grounding недоступен на {model_name}: {e}")
    raise RuntimeError(
        f"Онлайн-поиск Gemini сейчас недоступен. Автозаполнение остановлено, чтобы не подставлять устаревшие или выдуманные данные. Последняя ошибка: {last_error}"
    )


def parse_autofill_json(result_text):
    cleaned = str(result_text or "").replace("```json", "").replace("```", "").strip()
    if not cleaned:
        raise RuntimeError("Gemini вернул пустой ответ")
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        start = cleaned.find("{")
        end = cleaned.rfind("}")
        if start != -1 and end > start:
            return json.loads(cleaned[start:end + 1])
        raise


def supplement_autofill_payload_from_catalog(product_name, article="", payload=None):
    next_payload = dict(payload or {})
    try:
        from scripts import backfill_cmstore_product_details as bf
        from scripts import fill_catalog_from_cmstore as fc
    except Exception as e:
        logger.warning(f"Не удалось подключить каталоговый добор ТХ: {e}")
        return next_payload

    source_url = str(next_payload.get("source_url") or "").strip()
    if not source_url:
        try:
            candidate, score = bf.choose_candidate(product_name, timeout=20)
        except Exception as e:
            logger.warning(f"Поиск карточки-донора для '{product_name}' не удался: {e}")
            return next_payload
        if not candidate or score < AUTOFILL_SUPPLEMENT_MIN_SCORE:
            return next_payload
        source_url = str(candidate.get("url") or "").strip()
        if source_url:
            next_payload["source_url"] = source_url

    if not source_url:
        return next_payload

    try:
        details, page_description, _ = fc.get_page_payload(source_url, {})
    except Exception as e:
        logger.warning(f"Не удалось получить детали товара из '{source_url}': {e}")
        return next_payload

    detail_specs = bf.build_spec_updates(details)

    def fill_if_missing(key, value):
        value = str(value or "").strip()
        if value and not str(next_payload.get(key) or "").strip():
            next_payload[key] = value

    fill_if_missing("brand", detect_brand_from_inputs(product_name, article))
    fill_if_missing("weight", normalize_autofill_weight(bf.find_detail(details, *fc.WEIGHT_DETAIL_LABELS)))
    fill_if_missing("model_no", bf.find_detail(details, *fc.MODEL_DETAIL_LABELS))
    fill_if_missing("color", fc.detect_color(
        {
            "title": product_name,
            "brand": next_payload.get("brand") or detect_brand_from_inputs(product_name, article),
            "shorty_map": {},
        },
        details,
    ))

    for key in AUTOFILL_SPEC_KEYS:
        fill_if_missing(key, detail_specs.get(key))

    if page_description and not str(next_payload.get("description") or "").strip():
        next_payload["description"] = finalize_description(
            page_description,
            next_payload.get("brand"),
            product_name=product_name,
        )

    return next_payload

SPEC_DUPLICATE_LABEL_TARGETS = {
    "дисплей": ("display",),
    "процессор": ("processor",),
    "камера": ("camera",),
    "фронтальная камера": ("front_camera",),
    "видео": ("video",),
    "связь": ("connectivity",),
    "подключение": ("connectivity",),
    "интерфейс": ("connectivity", "charging"),
    "батарея": ("battery",),
    "ос": ("os",),
    "операционная система": ("os",),
    "биометрия": ("biometrics",),
    "зарядка": ("charging",),
    "память": ("storage",),
    "встроенная память": ("storage",),
    "объем встроенной памяти": ("storage",),
    "озу": ("ram",),
    "ram": ("ram",),
    "оперативная память": ("ram",),
    "объем оперативной памяти": ("ram",),
    "цвет": ("color",),
    "бренд": ("brand",),
    "производитель": ("brand",),
    "страна": ("country",),
    "страна производства": ("country",),
    "страна-производитель": ("country",),
    "вес": ("weight",),
    "модель": ("model_number",),
    "код производителя": ("model_number",),
    "артикул": ("model_number",),
    "гарантия": ("warranty",),
}
SPEC_ALWAYS_REDUNDANT_LABELS = {
    "тип",
    "тип товара",
    "категория",
}


def get_product_field_value(product, key):
    if product is None:
        return ""
    if isinstance(product, dict):
        return product.get(key, "")
    try:
        return product[key]
    except Exception:
        return ""


def normalize_spec_label_for_compare(value):
    text = str(value or "").strip().rstrip(":").lower().replace("ё", "е")
    text = text.replace("№", "").replace("(color)", "")
    return re.sub(r"\s+", " ", text).strip()


def normalize_spec_value_for_compare(value):
    text = str(value or "").strip().lower().replace("ё", "е")
    replacements = {
        "wi fi": "wi-fi",
        "type c": "type-c",
        "usb c": "usb-c",
        "гб": "gb",
        "тб": "tb",
        "мпикс": "mp",
        "мач": "mah",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return re.sub(r"[^a-zа-я0-9]+", "", text)


def normalize_weight_for_compare(value):
    return normalize_autofill_weight(value)


def are_duplicate_spec_values(label, left, right):
    if not str(left or "").strip() or not str(right or "").strip():
        return False
    normalized_label = normalize_spec_label_for_compare(label)
    if normalized_label == "вес":
        return normalize_weight_for_compare(left) == normalize_weight_for_compare(right)
    return normalize_spec_value_for_compare(left) == normalize_spec_value_for_compare(right)


def sanitize_specs_payload(raw_value, product=None):
    if isinstance(raw_value, dict):
        parsed = {key: str(value).strip() for key, value in raw_value.items() if str(value).strip()}
    else:
        parsed = parse_specs_payload(raw_value)
    if not parsed:
        return {}

    compare_values = {}
    for key in AUTOFILL_SPEC_KEYS:
        value = str(parsed.get(key) or "").strip()
        if value:
            compare_values[key] = value
    for key in ("brand", "color", "country", "weight", "model_number", "storage", "ram", "warranty"):
        value = str(get_product_field_value(product, key) or "").strip()
        if value:
            compare_values[key] = value

    cleaned = {}
    for key, value in parsed.items():
        clean_key = str(key or "").strip()
        clean_value = str(value or "").strip()
        if not clean_key or not clean_value:
            continue
        if clean_key in AUTOFILL_SPEC_KEYS:
            cleaned[clean_key] = clean_value
            continue
        normalized_label = normalize_spec_label_for_compare(clean_key)
        if normalized_label in SPEC_ALWAYS_REDUNDANT_LABELS:
            continue
        duplicate_targets = SPEC_DUPLICATE_LABEL_TARGETS.get(normalized_label, ())
        if any(are_duplicate_spec_values(normalized_label, clean_value, compare_values.get(target)) for target in duplicate_targets):
            continue
        cleaned[clean_key] = clean_value
    return cleaned


def parse_specs_payload(raw_value, product=None):
    if isinstance(raw_value, dict):
        parsed = {key: str(value).strip() for key, value in raw_value.items() if str(value).strip()}
        return sanitize_specs_payload(parsed, product)
    if not raw_value:
        return {}
    try:
        parsed = json.loads(raw_value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    if not isinstance(parsed, dict):
        return {}
    cleaned = {key: str(value).strip() for key, value in parsed.items() if str(value).strip()}
    return sanitize_specs_payload(cleaned, product)


def build_specs_payload_from_autofill(existing_specs, payload):
    merged = dict(existing_specs or {})
    for key in AUTOFILL_SPEC_KEYS:
        value = str(payload.get(key, "") or "").strip()
        if value:
            merged[key] = value
    return sanitize_specs_payload(merged, payload)


def merge_synonyms(existing_synonyms, incoming_synonyms):
    merged = []
    seen = set()
    for source in (incoming_synonyms or [], str(existing_synonyms or "").split(",")):
        if isinstance(source, str):
            candidates = [source]
        else:
            candidates = list(source or [])
        for candidate in candidates:
            value = str(candidate or "").strip()
            key = value.lower()
            if not value or key in seen:
                continue
            seen.add(key)
            merged.append(value)
    return ", ".join(merged)


def build_autofill_cache_key(product_name, article=""):
    raw = f"{str(product_name or '').strip().lower()}|{str(article or '').strip().lower()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def get_cached_autofill_payload(db, product_name, article=""):
    cache_key = build_autofill_cache_key(product_name, article)
    max_age = f"-{int(AUTOFILL_CACHE_MAX_AGE_DAYS)} days"
    row = db.execute(
        """
        SELECT payload
        FROM ai_autofill_cache
        WHERE cache_key = ?
          AND updated_at >= datetime('now', ?)
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        (cache_key, max_age),
    ).fetchone()
    if not row or not row["payload"]:
        return None
    try:
        payload = json.loads(row["payload"])
    except (TypeError, ValueError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    payload["synonyms"] = payload.get("synonyms") or []
    return payload


def save_autofill_cache(db, user_id, product_name, article, payload):
    cache_key = build_autofill_cache_key(product_name, article)
    serialized_payload = json.dumps(payload, ensure_ascii=False)
    existing = db.execute(
        "SELECT id FROM ai_autofill_cache WHERE cache_key = ?",
        (cache_key,),
    ).fetchone()
    if existing:
        db.execute(
            """
            UPDATE ai_autofill_cache
            SET user_id = ?, product_name = ?, article = ?, payload = ?, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (user_id, product_name, article, serialized_payload, existing["id"]),
        )
    else:
        db.execute(
            """
            INSERT INTO ai_autofill_cache (cache_key, user_id, product_name, article, payload)
            VALUES (?, ?, ?, ?, ?)
            """,
            (cache_key, user_id, product_name, article, serialized_payload),
        )


def payload_improves_product(product_row, payload):
    product = dict(product_row or {})
    comparisons = (
        ("brand", sanitize_brand_value(product.get("brand")), sanitize_brand_value(payload.get("brand"))),
        ("country", str(product.get("country") or "").strip(), str(payload.get("country_sim") or "").strip()),
        ("weight", str(product.get("weight") or "").strip(), str(payload.get("weight") or "").strip()),
        ("model_number", normalize_model_no(product.get("model_number"), sanitize_brand_value(product.get("brand"))), normalize_model_no(payload.get("model_no"), sanitize_brand_value(payload.get("brand")))),
        ("color", str(product.get("color") or "").strip(), str(payload.get("color") or "").strip()),
        ("storage", str(product.get("storage") or "").strip(), str(payload.get("storage") or "").strip()),
        ("ram", str(product.get("ram") or "").strip(), str(payload.get("ram") or "").strip()),
        ("warranty", str(product.get("warranty") or "").strip(), str(payload.get("warranty") or "").strip()),
        ("description", strip_description_notes(product.get("description")), strip_description_notes(payload.get("description"))),
    )
    for _, current_value, next_value in comparisons:
        if next_value and next_value != current_value:
            return True

    existing_specs = parse_specs_payload(product.get("specs"))
    for key in AUTOFILL_SPEC_KEYS:
        next_value = str(payload.get(key) or "").strip()
        current_value = str(existing_specs.get(key) or "").strip()
        if next_value and next_value != current_value:
            return True

    existing_synonyms = {str(item or "").strip().lower() for item in str(product.get("synonyms") or "").split(",") if str(item or "").strip()}
    for synonym in payload.get("synonyms", []):
        normalized = str(synonym or "").strip().lower()
        if normalized and normalized not in existing_synonyms:
            return True
    return False


def upsert_product_from_autofill(db, user_id, product_row, payload, preferred_folder_id=None):
    product = dict(product_row)
    resolved_folder_id = resolve_product_folder_assignment(
        db,
        user_id,
        product.get("name", ""),
        preferred_folder_id if preferred_folder_id is not None else product.get("folder_id"),
        payload.get("storage") or product.get("storage"),
    )
    merged_specs = build_specs_payload_from_autofill(parse_specs_payload(product.get("specs")), payload)
    merged_synonyms = merge_synonyms(product.get("synonyms"), payload.get("synonyms", []))
    resolved_brand = sanitize_brand_value(payload.get("brand")) or sanitize_brand_value(product.get("brand"))
    resolved_country = (
        payload.get("country_sim")
        or product.get("country")
        or infer_brand_country_fallback(resolved_brand)
        or infer_accessory_country_fallback(product.get("name", ""), resolved_brand)
    )
    resolved_model_no = normalize_model_no(payload.get("model_no"), resolved_brand)
    if not resolved_model_no:
        resolved_model_no = normalize_model_no(product.get("model_number"), sanitize_brand_value(product.get("brand")))
    resolved_warranty = str(payload.get("warranty") or product.get("warranty") or STORE_WARRANTY_DEFAULT).strip()
    if resolved_warranty in {"Гарантия CMstore", "Гарантия от Магазина", "Гарантия от магазина", "Гарантия от магазина 1 год."}:
        resolved_warranty = STORE_WARRANTY_DEFAULT
    db.execute(
        """
        UPDATE products
        SET brand = ?, country = ?, weight = ?, model_number = ?, folder_id = ?,
            color = ?, storage = ?, ram = ?, warranty = ?, description = ?, specs = ?, synonyms = ?, source_url = ?
        WHERE id = ? AND user_id = ?
        """,
        (
            resolved_brand,
            resolved_country,
            payload.get("weight") or product.get("weight"),
            resolved_model_no,
            resolved_folder_id,
            payload.get("color") or product.get("color"),
            payload.get("storage") or product.get("storage"),
            payload.get("ram") or product.get("ram"),
            resolved_warranty,
            payload.get("description") or product.get("description"),
            json.dumps(merged_specs, ensure_ascii=False) if merged_specs else "",
            merged_synonyms,
            payload.get("source_url") or product.get("source_url"),
            product["id"],
            user_id,
        ),
    )
    return resolved_folder_id


def is_retryable_ai_error(error):
    text = str(error or "").lower()
    retry_markers = (
        "429",
        "503",
        "quota",
        "resource_exhausted",
        "rate limit",
        "high demand",
        "unavailable",
        "temporarily unavailable",
    )
    return any(marker in text for marker in retry_markers)


def compute_ai_retry_delay_seconds(attempts):
    if attempts <= 1:
        return 5 * 60
    if attempts == 2:
        return 15 * 60
    if attempts == 3:
        return 30 * 60
    return 60 * 60


def enqueue_ai_repair_job(db, user_id, product_id, product_name, article="", preferred_folder_id=None, last_error=""):
    existing = db.execute(
        """
        SELECT id, attempts
        FROM ai_repair_jobs
        WHERE user_id = ? AND product_id = ? AND status IN ('pending', 'processing', 'retry_wait')
        ORDER BY id DESC
        LIMIT 1
        """,
        (user_id, product_id),
    ).fetchone()
    if existing:
        db.execute(
            """
            UPDATE ai_repair_jobs
            SET product_name = ?, article = ?, preferred_folder_id = ?, status = 'retry_wait',
                last_error = ?, next_retry_at = CURRENT_TIMESTAMP, updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (product_name, article, normalize_folder_id(preferred_folder_id), str(last_error or "")[:1000], existing["id"]),
        )
        return int(existing["id"])
    cursor = db.execute(
        """
        INSERT INTO ai_repair_jobs (
            user_id, product_id, product_name, article, preferred_folder_id,
            status, attempts, next_retry_at, last_error
        ) VALUES (?, ?, ?, ?, ?, 'retry_wait', 0, CURRENT_TIMESTAMP, ?)
        """,
        (
            user_id,
            product_id,
            product_name,
            article,
            normalize_folder_id(preferred_folder_id),
            str(last_error or "")[:1000],
        ),
    )
    return int(cursor.lastrowid)


def normalize_autofill_payload(product_name, parsed_data, article=""):
    payload = {key: str(parsed_data.get(key, "") or "").strip() for key in AUTOFILL_RESPONSE_KEYS}
    payload["source_url"] = str(parsed_data.get("source_url", "") or "").strip()
    detected_brand = sanitize_brand_value(detect_brand_from_inputs(product_name, article))
    payload["brand"] = sanitize_brand_value(payload["brand"]) or detected_brand
    article_model_no = extract_apple_model_no(article) or extract_apple_model_no(product_name)
    if payload["brand"] == "Apple" and article_model_no:
        payload["model_no"] = article_model_no
    payload["country_sim"] = (
        detect_country_variant(product_name)
        or payload["country_sim"]
        or infer_brand_country_fallback(payload["brand"])
        or infer_accessory_country_fallback(product_name, payload["brand"])
    )
    payload["weight"] = normalize_autofill_weight(payload["weight"])
    if is_apple_iphone_product(product_name, payload["brand"]):
        payload["weight"] = infer_iphone_weight_from_name(product_name) or payload["weight"]
        payload["os"] = normalize_iphone_os_value(payload["os"], product_name, payload["brand"])
    elif not payload["os"]:
        payload["os"] = infer_basic_os(product_name, payload["brand"])
    inferred_ram, inferred_storage = extract_ram_storage_from_name(product_name)
    if inferred_storage and not payload["storage"]:
        payload["storage"] = inferred_storage
    if inferred_ram and not payload["ram"]:
        payload["ram"] = inferred_ram
    payload["storage"] = normalize_capacity(payload["storage"], allow_tb=True)
    payload["ram"] = normalize_capacity(payload["ram"], allow_tb=False)
    payload["model_no"] = normalize_model_no(payload["model_no"], payload["brand"])
    if payload["warranty"] in {"Гарантия CMstore", "Гарантия от Магазина", "Гарантия от магазина", "Гарантия от магазина 1 год."}:
        payload["warranty"] = STORE_WARRANTY_DEFAULT
    payload["warranty"] = payload["warranty"] or STORE_WARRANTY_DEFAULT
    payload["description"] = finalize_description(
        payload["description"],
        payload["brand"],
        product_name=product_name,
    )
    payload["synonyms"] = build_synonyms(product_name, payload["brand"], article)
    return payload


def get_or_generate_autofill_payload(db, user_id, product_name, article="", use_cache=True):
    if use_cache:
        cached_payload = get_cached_autofill_payload(db, product_name, article)
        if cached_payload:
            return cached_payload, "cache"
    if not GEMINI_API_KEY:
        return build_offline_autofill_payload(product_name, article), "offline"
    result_text = call_gemini_online(build_autofill_prompt(product_name, article))
    payload = normalize_autofill_payload(product_name, parse_autofill_json(result_text), article)
    payload = normalize_autofill_payload(
        product_name,
        supplement_autofill_payload_from_known_products(product_name, article, payload),
        article,
    )
    payload = normalize_autofill_payload(
        product_name,
        supplement_autofill_payload_from_catalog(product_name, article, payload),
        article,
    )
    save_autofill_cache(db, user_id, product_name, article, payload)
    return payload, "live"


@app.route('/api/autofill', methods=['POST'])
def autofill():
    db = get_db()
    data = request.json or {}
    product_name = data.get('name', '').strip()
    article = data.get('article', '').strip()
    use_cache = not bool(data.get('force_online'))

    if not product_name:
        return jsonify({"error": "No product name"}), 400

    try:
        payload, source = get_or_generate_autofill_payload(db, 0, product_name, article, use_cache=use_cache)
        db.commit()
        return jsonify({**payload, "_source": source})
    except Exception as e:
        print("Ошибка Gemini:", str(e))
        return jsonify({"error": str(e)}), 503


@app.route('/api/products/<int:prod_id>/repair_with_ai', methods=['POST'])
@login_required
def repair_product_with_ai(prod_id):
    user_id = session['user_id']
    db = get_db()
    product_row = db.execute(
        "SELECT * FROM products WHERE id = ? AND user_id = ?",
        (prod_id, user_id),
    ).fetchone()
    if not product_row:
        return jsonify({"error": "Товар не найден"}), 404

    data = request.get_json(silent=True) or {}
    product = dict(product_row)
    product_name = str(data.get("name") or product.get("name") or "").strip()
    article = str(data.get("article") or product.get("model_number") or "").strip()
    preferred_folder_id = normalize_folder_id(data.get("folder_id"))

    if not product_name:
        return jsonify({"error": "У товара нет названия"}), 400

    try:
        payload, source = get_or_generate_autofill_payload(db, user_id, product_name, article, use_cache=True)
        if source == "cache" and not payload_improves_product(product_row, payload):
            payload, source = get_or_generate_autofill_payload(db, user_id, product_name, article, use_cache=False)
        if not payload_improves_product(product_row, payload):
            return jsonify({
                "success": False,
                "queued": False,
                "message": "Не удалось подтвердить новые данные для этой карточки. Нужен более точный источник или ручная проверка.",
            }), 200
        resolved_folder_id = upsert_product_from_autofill(
            db,
            user_id,
            product_row,
            payload,
            preferred_folder_id if preferred_folder_id is not None else product.get("folder_id"),
        )
        db.commit()
        notify_clients()
        variant_folder_name = detect_variant_folder_name(product_name)
        folder_message = f"Папка: {variant_folder_name}" if variant_folder_name else "Папка не менялась"
        return jsonify({
            "success": True,
            "queued": False,
            "folder_id": resolved_folder_id,
            "message": f"Карточка исправлена. {folder_message}. Источник: {'кэш' if source == 'cache' else 'онлайн'}",
        })
    except Exception as e:
        if is_retryable_ai_error(e):
            job_id = enqueue_ai_repair_job(
                db,
                user_id,
                prod_id,
                product_name,
                article,
                preferred_folder_id if preferred_folder_id is not None else product.get("folder_id"),
                last_error=e,
            )
            db.commit()
            notify_clients()
            return jsonify({
                "success": False,
                "queued": True,
                "job_id": job_id,
                "message": "Лимит или временная недоступность AI. Карточка поставлена в очередь и будет доисправлена автоматически.",
            }), 202
        logger.error(f"Ошибка AI-исправления товара {prod_id}: {e}")
        return jsonify({"error": str(e)}), 503



@app.route('/api/products/<int:prod_id>/messages')
@login_required
def get_product_messages(prod_id):
    user_id = session['user_id']
    db = get_db()
    
    # 1. Получаем синонимы товара
    prod = db.execute("SELECT synonyms FROM products WHERE id = ? AND user_id = ?", (prod_id, user_id)).fetchone()
    if not prod or not prod['synonyms']:
        return jsonify({'proposed': [], 'confirmed': []})
    
    # --- УМНАЯ ОЧИСТКА (превращает строку в набор слов) ---
    # --- УЛУЧШЕННАЯ ОЧИСТКА ---
    import re
    def clean_for_search(text):
        if not text: return set()
        
        # ДОБАВЛЯЕМ '*' (звездочку) в список удаляемых символов
        # Также добавили '.', если вдруг поставщик пишет "17.512"
        t = re.sub(r'[()*.[\]{},;!|/+\-]', ' ', text)
        
        # Убираем лишние пробелы и неразрывные пробелы
        t = re.sub(r'\s+', ' ', t.replace('\xa0', ' ')).strip().lower()
        
        # Решаем проблему кириллицы/латиницы
        t = t.translate(str.maketrans('асеокрх', 'aceokpx'))
        
        # Возвращаем набор чистых слов
        return set(t.split())

    # Подготовка синонимов (разбиваем на сеты слов)
    synonyms_as_word_sets = []
    for s in prod['synonyms'].split(','):
        if s.strip():
            words = clean_for_search(s)
            if words:
                synonyms_as_word_sets.append(words)

    # --- ИСПРАВЛЕНИЕ: Получаем все последние сообщения (all_msgs) ---
    # Без этого блока была ошибка "all_msgs is not defined"
    all_msgs_rows = db.execute("""
        SELECT m.id, m.text, m.date, m.chat_id, m.sender_name, m.chat_title, m.type, tc.custom_name
        FROM messages m
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE m.user_id = ? 
        ORDER BY m.date DESC LIMIT 1000
    """, (user_id,)).fetchall()
    all_msgs = [dict(r) for r in all_msgs_rows]

    # --- ИСПРАВЛЕНИЕ: Собираем актуальные отпечатки (latest_fingerprints) ---
    # Без этого блока была ошибка "latest_fingerprints is not defined"
    latest_fingerprints = {}
    for msg in all_msgs:
        if not msg['text']: continue
        lines = msg['text'].split('\n')
        for i, line in enumerate(lines):
            if not line.strip(): continue
            fp = get_fingerprint(msg['chat_id'], msg['sender_name'], line)
            if fp not in latest_fingerprints or msg['date'] > latest_fingerprints[fp]['date']:
                # Ищем цену в текущей строке или на 1-2 строки ниже
                price_val = extract_price(line)
                if not price_val and i + 1 < len(lines):
                    price_val = extract_price(lines[i+1])
                if not price_val and i + 2 < len(lines):
                    price_val = extract_price(lines[i+2])

                latest_fingerprints[fp] = {
                    'text': line,
                    'price': price_val,
                    'date': msg['date'],
                    'message_id': msg['id'],
                    'line_index': i
                }

    # 2. Обработка ПОДТВЕРЖДЕННЫХ
    confirmed_rows = db.execute("""
        SELECT pm.id as pm_id, pm.group_id, pm.message_id, m.chat_title, m.chat_id, m.sender_name, 
               pm.extracted_price, m.text, m.date, pm.line_index, tc.custom_name, m.type
        FROM product_messages pm
        JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE pm.product_id = ? AND pm.status = 'confirmed'
    """, (prod_id,)).fetchall()
    
    confirmed_raw = []
    confirmed_fingerprints = set()
    has_updates = False 

    for r in confirmed_rows:
        d = dict(r)
        if d['line_index'] != -1 and d['text']:
            lines = d['text'].split('\n')
            if 0 <= d['line_index'] < len(lines):
                orig_line = lines[d['line_index']]
                fp = get_fingerprint(d['chat_id'], d['sender_name'], orig_line)
                confirmed_fingerprints.add(fp)
                
                latest = latest_fingerprints.get(fp)
                if latest and latest['date'] > d['date']:
                    d['text'] = latest['text']
                    d['extracted_price'] = latest['price']
                    d['date'] = latest['date'] 
                    current_id = d.get('pm_id')
                    try:
                        db.execute("UPDATE product_messages SET message_id=?, line_index=?, extracted_price=? WHERE id=?",
                                   (latest['message_id'], latest['line_index'], latest['price'], current_id))
                        has_updates = True
                    except: pass
                else:
                    d['text'] = orig_line
        confirmed_raw.append(d)
        
    if has_updates:
        db.commit()
        notify_clients()
        
    grouped_confirmed = {}
    for d in confirmed_raw:
        g_id = d['group_id'] if d['group_id'] else d['pm_id']
        if g_id not in grouped_confirmed or d['date'] > grouped_confirmed[g_id]['date']:
            grouped_confirmed[g_id] = d
    
    confirmed = sorted(list(grouped_confirmed.values()), key=lambda x: x['date'], reverse=True)

    # === НОВОЕ: Собираем все строки, которые уже привязаны к ДРУГИМИ товарам ===
    occupied_rows = db.execute(
        "SELECT message_id, line_index FROM product_messages WHERE status = 'confirmed' AND product_id != ?", 
        (prod_id,)
    ).fetchall()
    
    # Создаем удобный список занятых строк, чтобы быстро их проверять
    occupied = {(r['message_id'], r['line_index']) for r in occupied_rows}
    # ===========================================================================
    
    # 3. Обработка ПРЕДЛОЖЕННЫХ (склеивание дубликатов и поиск по словам)
    # 3. Обработка ПРЕДЛОЖЕННЫХ (склеивание дубликатов и поиск по словам)
    proposed_list = []
    seen_fps = set()
    
    for row in all_msgs:
        lines = row['text'].split('\n') if row['text'] else []
        for i, line in enumerate(lines):
            if not line.strip(): continue
            
            # --- НОВОЕ СТРОГОЕ ПРАВИЛО ---
            # Если эта строка уже кем-то занята - мы просто игнорируем ее
            # и даже не показываем в списке "Предложенные"
            if (row['id'], i) in occupied or (row['id'], -1) in occupied:
                continue
            # -----------------------------

            fp = get_fingerprint(row['chat_id'], row['sender_name'], line)
            
            if fp in seen_fps or fp in confirmed_fingerprints:
                continue
            
            # --- ЛОГИКА ЖЕСТКОГО ПОИСКА ПО СЛОВАМ ---
            # --- ЛОГИКА ЖЕСТКОГО ПОИСКА ПО СЛОВАМ ---
            line_words = clean_for_search(line)
            is_match = False
            for syn_set in synonyms_as_word_sets:
                # Если ВСЕ слова из синонима есть в строке поставщика
                if syn_set.issubset(line_words):
                    # ЖЕСТКОЕ СРАВНЕНИЕ: Отсекаем старшие/другие модели, если их нет в синониме
                    if "pro" not in syn_set and "pro" in line_words: continue
                    if "max" not in syn_set and "max" in line_words: continue
                    if "plus" not in syn_set and "plus" in line_words: continue
                    if "ultra" not in syn_set and "ultra" in line_words: continue
                    if "mini" not in syn_set and "mini" in line_words: continue
                    if "fe" not in syn_set and "fe" in line_words: continue
                    
                    is_match = True
                    break
            
            if is_match:
                seen_fps.add(fp)
                
                # Ищем цену в текущей строке или на 1-2 строки ниже
                s_price = extract_price(line)
                if not s_price and i + 1 < len(lines):
                    s_price = extract_price(lines[i+1])
                if not s_price and i + 2 < len(lines):
                    s_price = extract_price(lines[i+2])
                
                proposed_list.append({
                    'message_id': row['id'],
                    'line_index': i,
                    'chat_title': row['chat_title'],
                    'sender_name': row['sender_name'],
                    'custom_name': row.get('custom_name'),
                    'type': row['type'],
                    'match_line': line,
                    'suggested_price': s_price,
                    'date': row['date']
                })
    
    proposed_list.sort(key=lambda x: x['date'], reverse=True)
    return jsonify({'proposed': proposed_list, 'confirmed': confirmed})

import re

def parse_number(s):
    clean = re.sub(r'[^\d]', '', s)
    return float(clean) if clean else 0.0

def extract_price(line):
    if not line: return None
    # Добавил K/R для обработки "KR" как на вашем скриншоте
    line_clean = re.sub(r'[₽$€рRkKкК]', '', line) 
    number_pattern = r'(\d[\d\s.,]*)'
    all_numbers = list(re.finditer(number_pattern, line_clean))
    
    if not all_numbers: return None
    
    for match in reversed(all_numbers):
        number_str = match.group(1).strip()
        try:
            price_value = parse_number(number_str)
            if price_value < 3000: continue
        except: continue
        
        end_pos = match.end()
        text_after = line_clean[end_pos:].strip().lower()
        if re.match(r'^[-]?\s*(шт|шт\.|штук|pcs|pc|item|ед|единиц)', text_after): continue
        
        text_before = line_clean[:match.start()].strip()
        if text_before.endswith('-'): return price_value
        if end_pos >= len(line_clean) or line_clean[end_pos] in '()': return price_value
        if end_pos == len(line_clean) or not line_clean[end_pos].isdigit(): return price_value
        
    for match in reversed(all_numbers):
        number_str = match.group(1).strip()
        try:
            price_value = parse_number(number_str)
            if price_value >= 1500:
                end_pos = match.end()
                text_after = line_clean[end_pos:].strip().lower()
                if not re.match(r'^[-]?\s*(шт|шт\.|штук|pcs|pc|item|ед|единиц)', text_after):
                    return price_value
        except: continue
    return None

def get_fingerprint(chat_id, sender_name, line):
    """Создает 'слепок' строки, удаляя цены для сравнения товаров"""
    # Заменяем все числа от 1000 и выше (с точками и пробелами) на тег [PRICE]
    norm_line = re.sub(r'\b\d{1,3}(?:[ .,]\d{3})+\b|\b\d{4,}\b', '[PRICE]', line.lower())
    norm_line = re.sub(r'[₽$€рRkKкК]', '', norm_line).strip()
    return f"{chat_id}_{sender_name}_{norm_line}"







@app.route('/api/product_messages/confirm', methods=['POST'])
@login_required
def confirm_product_message():
    data = request.get_json()
    db = get_db()
    try:
        line_idx = data.get('line_index', -1)
        price = data.get('price')
        if price == '': 
            price = None
            
        # --- НОВАЯ СТРОГАЯ ПРОВЕРКА: Занята ли уже эта строка другим товаром? ---
        if line_idx != -1:
            existing = db.execute("""
                SELECT product_id FROM product_messages 
                WHERE message_id = ? AND line_index = ?
            """, (data['message_id'], line_idx)).fetchone()
            
            if existing and existing['product_id'] != data['product_id']:
                return jsonify({'error': 'Эта строка уже привязана к другому товару!'}), 400
        # ------------------------------------------------------------------------

        db.execute("""
            INSERT INTO product_messages (product_id, message_id, line_index, status, extracted_price) 
            VALUES (?, ?, ?, 'confirmed', ?)
            ON CONFLICT(product_id, message_id, line_index) DO UPDATE SET status='confirmed', extracted_price=excluded.extracted_price
        """, (data['product_id'], data['message_id'], line_idx, price))
        maybe_sync_product_sort_from_message(db, session['user_id'], data['product_id'], data['message_id'], line_idx)
        db.commit()
        notify_clients()
         
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка привязки: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/api/product_messages/detach', methods=['POST'])
@login_required
def detach_product_message():
    data = request.get_json()
    db = get_db()
    # Ищем, состоит ли сообщение в группе. Если да — удаляем всю группу
    pm = db.execute("SELECT id, group_id FROM product_messages WHERE product_id=? AND message_id=?", 
                    (data['product_id'], data['message_id'])).fetchone()
    if pm:
        if pm['group_id']:
            db.execute("DELETE FROM product_messages WHERE group_id = ?", (pm['group_id'],))
        else:
            db.execute("DELETE FROM product_messages WHERE id = ?", (pm['id'],))
        db.commit()
        notify_clients()
         
    return jsonify({'success': True})

@app.route('/api/product_messages/update_price', methods=['POST'])
@login_required
def update_product_price():
    data = request.get_json()
    db = get_db()
    db.execute("UPDATE product_messages SET extracted_price = ? WHERE id = ?", 
               (data['price'], data['pm_id']))
    db.commit()
    notify_clients()
     
    return jsonify({'success': True})

def clean_for_search(text):
    if not text: return set()
    t = re.sub(r'[()*.[\]{},;!|/+\-]', ' ', text)
    t = re.sub(r'\s+', ' ', t.replace('\xa0', ' ')).strip().lower()
    t = t.translate(str.maketrans('асеокрх', 'aceokpx'))
    return set(t.split())


PRODUCT_SEARCH_ALIAS_GROUPS = [
    ['геймпад', 'гейм пад', 'геймад', 'джойстик', 'джой', 'джостик', 'контроллер', 'controller', 'gamepad', 'game pad', 'joystick'],
    ['наушники', 'наушник', 'гарнитура', 'гарнитуры', 'headset', 'headsets', 'headphones', 'headphone', 'earbuds', 'earphones'],
    ['эйрподс', 'аирподс', 'airpods', 'air pods'],
    ['колонка', 'колонки', 'акустика', 'акустическая система', 'спикер', 'speaker', 'speakers'],
    ['зарядка', 'зарядки', 'зарядное', 'зарядное устройство', 'зарядник', 'сзу', 'charger', 'charging', 'power adapter', 'адаптер питания', 'блок питания'],
    ['кабель', 'кабели', 'шнур', 'провод', 'cable', 'cord', 'wire'],
    ['чехол', 'чехлы', 'кейс', 'кейсы', 'накладка', 'бампер', 'case', 'cover', 'sleeve'],
    ['пленка', 'пленки', 'плёнка', 'плёнки', 'стекло', 'стекла', 'стекло защитное', 'защитное стекло', 'glass', 'film'],
    ['ноутбук', 'ноутбуки', 'ноут', 'лэптоп', 'лаптоп', 'laptop', 'notebook'],
    ['макбук', 'мак', 'macbook', 'mac'],
    ['смартфон', 'смартфоны', 'телефон', 'телефоны', 'мобильник', 'phone', 'smartphone'],
    ['айфон', 'айфоны', 'iphone'],
    ['pro', 'про'],
    ['max', 'макс', 'мах'],
    ['plus', 'плюс'],
    ['ultra', 'ультра'],
    ['mini', 'мини'],
    ['air', 'эйр', 'аир'],
    ['планшет', 'планшеты', 'tablet'],
    ['айпад', 'айпеды', 'ipad'],
    ['часы', 'часики', 'умные часы', 'смарт часы', 'смарт-часы', 'smartwatch', 'watch'],
    ['монитор', 'мониторы', 'display', 'screen', 'экран'],
    ['мышь', 'мышка', 'мышки', 'mouse'],
    ['клавиатура', 'клава', 'keyboard'],
    ['роутер', 'роутеры', 'маршрутизатор', 'маршрутизаторы', 'router'],
    ['адаптер', 'адаптеры', 'переходник', 'переходники', 'adapter', 'hub', 'usb hub'],
    ['пауэрбанк', 'повербанк', 'powerbank', 'power bank', 'внешний аккумулятор', 'внешний аккум', 'аккум', 'акб'],
    ['dualsense', 'dual sense', 'dual-sense', 'дуалсенс', 'дуал сенс', 'дуал-сенс'],
    ['joycon', 'joy con', 'joy-con', 'джойкон', 'джой кон', 'джой- кон'],
    ['vr', 'виртуальная реальность', 'шлем', 'очки', 'гарнитура vr', 'vision pro'],
    ['камера', 'экшн камера', 'экшн-камера', 'action camera', 'gopro'],
    ['apple', 'эпл', 'эппл'],
    ['samsung', 'самсунг', 'самс', 'galaxy', 'галакси'],
    ['xiaomi', 'сяоми', 'ксиаоми', 'mi', 'redmi', 'редми', 'poco', 'поко'],
    ['huawei', 'хуавей'],
    ['honor', 'хонор'],
    ['dyson', 'дайсон'],
    ['sony', 'сони'],
    ['asus', 'асус'],
    ['lenovo', 'леново'],
    ['hp', 'хп', 'эйчпи'],
    ['nintendo', 'нинтендо', 'switch', 'свитч'],
    ['playstation', 'play station', 'плейстейшен', 'плейстейшн', 'плойка', 'сонька', 'ps', 'ps4', 'ps5', 'пс4', 'пс5'],
    ['xbox', 'x box', 'иксбокс'],
    ['steam deck', 'steamdeck', 'стим дек', 'стимдек'],
    ['type c', 'type-c', 'typec', 'usb c', 'usb-c', 'usbc', 'тайп си', 'тайпси', 'юсб си', 'юсбси'],
]

PRODUCT_SEARCH_STOP_WORDS = {
    'для', 'на', 'под', 'к', 'ко', 'от', 'с', 'со', 'из', 'у', 'в', 'во', 'по', 'and', 'the', 'for', 'with', 'of'
}

PRODUCT_SEARCH_NORMALIZATION_REPLACEMENTS = [
    (re.compile(r"\b(?:steam|стим)\s*deck\b", re.IGNORECASE), 'steamdeck'),
    (re.compile(r"\bстим\s*дек\b", re.IGNORECASE), 'steamdeck'),
    (re.compile(r"\b(?:type|тайп)\s*-?\s*(?:c|си)\b", re.IGNORECASE), 'typec'),
    (re.compile(r"\b(?:usb|юсб)\s*-?\s*(?:c|си)\b", re.IGNORECASE), 'usbc'),
    (re.compile(r"\bplay\s+station\b", re.IGNORECASE), 'playstation'),
    (re.compile(r"\bплей\s*стейш[её]н\b", re.IGNORECASE), 'плейстейшн'),
    (re.compile(r"\bdual[\s-]+sense\b", re.IGNORECASE), 'dualsense'),
    (re.compile(r"\bдуал[\s-]*сенс\b", re.IGNORECASE), 'дуалсенс'),
    (re.compile(r"\bjoy[\s-]*con\b", re.IGNORECASE), 'joycon'),
    (re.compile(r"\bair[\s-]*pods\b", re.IGNORECASE), 'airpods'),
    (re.compile(r"\bpower\s+bank\b", re.IGNORECASE), 'powerbank'),
    (re.compile(r"\bgame\s+pad\b", re.IGNORECASE), 'gamepad'),
    (re.compile(r"\bsmart[\s-]*watch\b", re.IGNORECASE), 'smartwatch'),
    (re.compile(r"\be\s*sim\b", re.IGNORECASE), 'esim'),
    (re.compile(r"\bps\s+([45])\b", re.IGNORECASE), r'ps\1'),
    (re.compile(r"\bпс\s+([45])\b", re.IGNORECASE), r'пс\1'),
    (re.compile(r"\b(?:playstation|плейстейшн|плейстейшен|плойка|сонька)\s*([45])\b", re.IGNORECASE), r'ps\1'),
]

PRODUCT_ACCESSORY_SEARCH_HINTS = [
    'аксессуар', 'аксессуары', 'чехол', 'чехлы', 'кейс', 'кейсы', 'бампер', 'накладка', 'накладки',
    'ремешок', 'ремешки', 'браслет', 'браслеты', 'стекло', 'пленка', 'пленки', 'зарядка', 'зарядное',
    'кабель', 'кабели', 'адаптер', 'адаптеры', 'переходник', 'переходники', 'док станция', 'dock',
    'станция', 'держатель', 'holder', 'mount', 'cover', 'strap', 'charger', 'cable',
    'ssd', 'накопитель', 'дисковод', 'панель', 'панели', 'игра', 'игры', 'game', 'subscription', 'подписка'
]

PRODUCT_COMMON_STORAGE_NUMBERS = {'32', '64', '128', '256', '512', '1024', '2048'}

PRODUCT_SQL_LITERAL_SEARCH_EXPANSIONS = {
    'steamdeck': ['Steam Deck', 'steam deck'],
    'applewatch': ['Watch', 'Apple Watch', 'apple watch'],
    'typec': ['Type-C', 'Type C', 'type-c', 'type c'],
    'usbc': ['USB-C', 'USB C', 'usb-c', 'usb c'],
}

RU_TO_EN_KEYBOARD = str.maketrans({
    'й': 'q', 'ц': 'w', 'у': 'e', 'к': 'r', 'е': 't', 'н': 'y', 'г': 'u', 'ш': 'i', 'щ': 'o', 'з': 'p', 'х': 'h', 'ъ': '',
    'ф': 'a', 'ы': 's', 'в': 'd', 'а': 'f', 'п': 'g', 'р': 'h', 'о': 'j', 'л': 'k', 'д': 'l', 'ж': '', 'э': '',
    'я': 'z', 'ч': 'x', 'с': 'c', 'м': 'v', 'и': 'b', 'т': 'n', 'ь': 'm', 'б': '', 'ю': '',
})
EN_TO_RU_KEYBOARD = str.maketrans({
    'q': 'й', 'w': 'ц', 'e': 'у', 'r': 'к', 't': 'е', 'y': 'н', 'u': 'г', 'i': 'ш', 'o': 'щ', 'p': 'з',
    'a': 'ф', 's': 'ы', 'd': 'в', 'f': 'а', 'g': 'п', 'h': 'р', 'j': 'о', 'k': 'л', 'l': 'д',
    'z': 'я', 'x': 'ч', 'c': 'с', 'v': 'м', 'b': 'и', 'n': 'т', 'm': 'ь',
})


def normalize_product_search_text(value):
    normalized = str(value or '').lower().replace('ё', 'е')
    normalized = re.sub(r'[+"\'`«»]', ' ', normalized)
    normalized = re.sub(r'[\u2010-\u2015]', '-', normalized)
    for pattern, replacement in PRODUCT_SEARCH_NORMALIZATION_REPLACEMENTS:
        normalized = pattern.sub(replacement, normalized)
    normalized = re.sub(r'[^a-zа-я0-9]+', ' ', normalized)
    return re.sub(r'\s+', ' ', normalized).strip()


def keyboard_layout_variants(value):
    normalized = normalize_product_search_text(value)
    if not normalized:
        return []
    variants = []
    if re.search(r'[а-я]', normalized):
        variants.append(normalize_product_search_text(normalized.translate(RU_TO_EN_KEYBOARD)))
    if re.search(r'[a-z]', normalized):
        variants.append(normalize_product_search_text(normalized.translate(EN_TO_RU_KEYBOARD)))
    return [variant for variant in variants if variant and variant != normalized]


def tokenize_product_search_query(value, keep_stop_words=False):
    normalized = normalize_product_search_text(value)
    if not normalized:
        return []
    words = [word for word in normalized.split() if word]
    if keep_stop_words:
        return words
    meaningful = [word for word in words if word not in PRODUCT_SEARCH_STOP_WORDS]
    return meaningful or words


def should_expand_alias_group_by_term(source_term, alias_term):
    normalized_source = normalize_product_search_text(source_term)
    normalized_alias = normalize_product_search_text(alias_term)
    if not normalized_source or not normalized_alias:
        return False
    if normalized_source == normalized_alias:
        return True
    return len(normalized_source) >= 3 and normalized_alias.startswith(normalized_source)


PRODUCT_SEARCH_ALIAS_INDEX = {}
for alias_group in PRODUCT_SEARCH_ALIAS_GROUPS:
    normalized_group = [normalize_product_search_text(item) for item in alias_group]
    normalized_group = [item for item in normalized_group if item]
    for item in normalized_group:
        PRODUCT_SEARCH_ALIAS_INDEX[item] = normalized_group


def collect_product_search_terms(value):
    normalized = normalize_product_search_text(value)
    if not normalized:
        return []

    layout_variants = keyboard_layout_variants(normalized)
    result = {normalized, normalized.replace(' ', ''), *layout_variants}
    result.update(variant.replace(' ', '') for variant in layout_variants)
    words = [word for word in normalized.split() if word]
    alias_candidates = list(dict.fromkeys([normalized, *layout_variants, *words]))

    canonical_units = {
        'мм': ['мм', 'mm', 'м'],
        'mm': ['мм', 'mm', 'м'],
        'м': ['мм', 'mm', 'м'],
        'gb': ['gb', 'гб'],
        'гб': ['gb', 'гб'],
        'tb': ['tb', 'тб'],
        'тб': ['tb', 'тб'],
        'hz': ['hz', 'гц'],
        'гц': ['hz', 'гц'],
        'mah': ['mah', 'мач'],
        'мач': ['mah', 'мач'],
        'вт': ['вт', 'w'],
        'w': ['вт', 'w'],
    }

    for word in [*words, *layout_variants]:
        result.add(word)

        if len(word) > 4 and word.endswith(('ы', 'и')):
            result.add(word[:-1])
        if len(word) > 5 and word.endswith(('а', 'е', 'о', 'у', 'я')):
            result.add(word[:-1])

        compact_unit_match = re.match(r'^(\d+)(мм|mm|м|gb|гб|tb|тб|hz|гц|mah|мач|вт|w)$', word, re.IGNORECASE)
        if compact_unit_match:
            numeric_value = compact_unit_match.group(1)
            raw_unit = compact_unit_match.group(2).lower()
            result.add(numeric_value)
            for unit in canonical_units.get(raw_unit, [raw_unit]):
                result.add(f'{numeric_value}{unit}')
                result.add(f'{numeric_value} {unit}')

        for alias in PRODUCT_SEARCH_ALIAS_INDEX.get(word, []):
            result.add(alias)

    for alias_group in PRODUCT_SEARCH_ALIAS_GROUPS:
        if any(should_expand_alias_group_by_term(candidate, item) for candidate in alias_candidates for item in alias_group):
            for item in alias_group:
                normalized_item = normalize_product_search_text(item)
                if normalized_item:
                    result.add(normalized_item)

    return [item for item in result if item]


def build_derived_product_search_aliases(product):
    combined = normalize_product_search_text(' '.join([
        str(product.get('name') or ''),
        str(product.get('brand') or ''),
        str(product.get('model_number') or ''),
        str(product.get('synonyms') or ''),
    ]))

    aliases = set()
    is_gamepad = bool(re.search(r'(геймпад|джойстик|контроллер|controller|gamepad|joystick|dualsense|joycon)', combined))
    is_playstation = bool(re.search(r'(ps5|ps4|playstation|плейстейшн|пс5|пс4|плойка|сонька)', combined))
    is_xbox = bool(re.search(r'(xbox|иксбокс)', combined))
    is_switch = bool(re.search(r'(switch|nintendo|нинтендо|joycon)', combined))

    if 'dualsense' in combined or (is_gamepad and is_playstation):
        aliases.update({
            'джойстик для ps5',
            'джой для ps5',
            'геймпад для ps5',
            'контроллер ps5',
            'джойстик для плойки',
            'геймпад для playstation',
            'sony dualsense',
            'dual sense',
            'дуал сенс',
        })

    if is_gamepad and is_xbox:
        aliases.update({
            'джойстик xbox',
            'геймпад xbox',
            'контроллер xbox',
            'xbox controller',
        })

    if ('joycon' in combined or 'joy con' in combined) and is_switch:
        aliases.update({
            'джойкон',
            'джойстик для switch',
            'геймпад для nintendo switch',
            'joy-con',
        })
    elif is_gamepad and is_switch:
        aliases.update({
            'джойстик для switch',
            'геймпад switch',
            'контроллер nintendo switch',
        })

    if 'airpods' in combined:
        aliases.update({
            'беспроводные наушники apple',
            'наушники для iphone',
            'air pods',
        })

    if re.search(r'(powerbank|пауэрбанк|повербанк|внешний аккумулятор|magnetic wireless charging)', combined):
        aliases.update({
            'power bank',
            'внешний аккум',
            'пауэрбанк',
            'повербанк',
        })

    return list(aliases)


def bounded_levenshtein_distance(left, right, max_distance):
    if left == right:
        return 0
    if abs(len(left) - len(right)) > max_distance:
        return max_distance + 1

    previous = list(range(len(right) + 1))
    for left_index, left_char in enumerate(left, 1):
        current = [left_index]
        row_min = current[0]
        for right_index, right_char in enumerate(right, 1):
            cost = 0 if left_char == right_char else 1
            value = min(
                previous[right_index] + 1,
                current[right_index - 1] + 1,
                previous[right_index - 1] + cost,
            )
            current.append(value)
            row_min = min(row_min, value)
        if row_min > max_distance:
            return max_distance + 1
        previous = current
    return previous[-1]


def is_product_search_typo_match(token, variant):
    if token.isdigit() or variant.isdigit():
        return False
    if len(token) < 4 or len(variant) < 4 or len(token) > 24 or len(variant) > 24:
        return False
    if token[0] != variant[0]:
        return False
    shorter_len = min(len(token), len(variant))
    max_typos = 1 if shorter_len < 8 else 2
    return bounded_levenshtein_distance(token, variant, max_typos) <= max_typos


def build_product_search_typo_keys(value):
    normalized = normalize_product_search_text(value).replace(' ', '')
    if len(normalized) < 4 or len(normalized) > 24 or normalized.isdigit():
        return []
    keys = {normalized}
    for index in range(len(normalized)):
        keys.add(normalized[:index] + normalized[index + 1:])
    return list(keys)


def token_matches_product_search_variant(token, variant):
    normalized_token = normalize_product_search_text(token)
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_token or not normalized_variant:
        return False
    if re.fullmatch(r'\d+', normalized_variant):
        if re.fullmatch(r'\d+', normalized_token):
            return normalized_token == normalized_variant
        if normalized_token.endswith(normalized_variant) and len(normalized_token) <= len(normalized_variant) + 1:
            return True
        return False
    if normalized_token == normalized_variant:
        return True
    if normalized_token in PRODUCT_SEARCH_STOP_WORDS:
        return False
    if len(normalized_token) < 2 or len(normalized_variant) < 2:
        return False
    return (
        normalized_token.startswith(normalized_variant)
        or normalized_token in normalized_variant
        or normalized_variant in normalized_token
        or is_product_search_typo_match(normalized_token, normalized_variant)
    )


def token_list_matches_product_search_variant(token_list, variant):
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_variant:
        return False
    return any(token_matches_product_search_variant(token, normalized_variant) for token in (token_list or []))


def search_text_matches_variant(text, variant):
    normalized_text = normalize_product_search_text(text)
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_variant:
        return False
    if re.fullmatch(r'\d+', normalized_variant):
        return any(token_matches_product_search_variant(token, normalized_variant) for token in normalized_text.split())
    haystack = f' {normalized_text} '
    if f' {normalized_variant} ' in haystack:
        return True
    if token_list_matches_product_search_variant(normalized_text.split(), normalized_variant):
        return True
    return normalized_variant in haystack


def has_explicit_storage_query_intent(words):
    return any(re.match(r'^(?:\d+\s*(?:gb|tb|гб|тб)|\d+(?:gb|tb|гб|тб)|gb|tb|гб|тб)$', normalize_product_search_text(word)) for word in (words or []))


def extract_product_search_numbers(value):
    normalized = normalize_product_search_text(value)
    return list(dict.fromkeys(re.findall(r'\b\d+\b', normalized)))


def extract_storage_search_numbers(*values):
    numbers = []
    seen = set()
    for value in values:
        normalized = normalize_product_search_text(value)
        for match in re.finditer(r'\b(\d+)\s*(gb|tb|гб|тб)\b', normalized):
            number = match.group(1)
            if number and number not in seen:
                seen.add(number)
                numbers.append(number)
    return numbers


def is_accessory_search_intent(words):
    for word in (words or []):
        variants = collect_product_search_terms(word)
        if any(hint in variant for variant in variants for hint in PRODUCT_ACCESSORY_SEARCH_HINTS):
            return True
    return False


def is_gamepad_search_intent(words):
    hints = {
        'геймпад', 'джойстик', 'джой', 'контроллер', 'controller', 'gamepad',
        'joystick', 'dualsense', 'dual sense', 'joycon', 'joy con', 'джойкон'
    }
    for word in (words or []):
        variants = collect_product_search_terms(word)
        if any(hint in variant for variant in variants for hint in hints):
            return True
    return False


def looks_like_device_model_query(words):
    normalized_words = [normalize_product_search_text(word) for word in (words or []) if normalize_product_search_text(word)]
    if not normalized_words:
        return False

    device_brands = {
        'iphone', 'айфон', 'apple', 'galaxy', 'samsung', 'xiaomi', 'redmi',
        'poco', 'honor', 'huawei', 'oneplus', 'pixel', 'realme'
    }
    device_modifiers = {'pro', 'max', 'plus', 'mini', 'ultra'}
    numeric_words = [word for word in normalized_words if re.fullmatch(r'\d+', word)]

    return (
        any(word in device_brands for word in normalized_words)
        or any(word in device_modifiers for word in normalized_words)
        or (len(numeric_words) == 1 and len(normalized_words) >= 2)
    )


def get_product_search_sort_tuple(entry):
    sort_index = entry.get('sort_index')
    sort_bucket = 1 if sort_index is None else 0
    sort_value = sort_index if sort_index is not None else 0
    display_name = str(entry.get('display_name') or '')
    return (sort_bucket, sort_value, display_name.lower(), int(entry.get('id') or 0))


def build_search_token_set(*values):
    tokens = []
    seen = set()
    for value in values:
        normalized = normalize_product_search_text(value)
        for token in normalized.split():
            if not token or token in PRODUCT_SEARCH_STOP_WORDS:
                continue
            if token in seen:
                continue
            seen.add(token)
            tokens.append(token)
    return frozenset(tokens)


def build_compound_product_search_terms(chunks):
    compounds = []
    seen = set()
    for chunk in chunks:
        words = [
            word
            for word in normalize_product_search_text(chunk).split()
            if len(word) >= 2 and word not in PRODUCT_SEARCH_STOP_WORDS
        ]
        for size in (2, 3):
            if len(words) < size:
                continue
            for index in range(0, len(words) - size + 1):
                compound = ''.join(words[index:index + size])
                if len(compound) < 4 or len(compound) > 36 or compound in seen:
                    continue
                seen.add(compound)
                compounds.append(compound)
    return compounds


def build_server_product_search_entry(product_row):
    product = dict(product_row)
    derived_aliases = build_derived_product_search_aliases(product)
    combined_search_source = normalize_product_search_text(' '.join([
        str(product.get('name') or ''),
        str(product.get('brand') or ''),
        str(product.get('model_number') or ''),
        str(product.get('synonyms') or ''),
        ' '.join(derived_aliases),
    ]))
    normalized_chunks = [
        normalize_product_search_text(chunk)
        for chunk in (
            product.get('name'),
            product.get('brand'),
            product.get('model_number'),
            product.get('country'),
            product.get('storage'),
            product.get('color'),
            product.get('synonyms'),
            ' '.join(derived_aliases),
        )
        if chunk
    ]
    compound_chunks = build_compound_product_search_terms(normalized_chunks)
    compact_chunks = [
        chunk.replace(' ', '')
        for chunk in normalized_chunks
        if 4 <= len(chunk.replace(' ', '')) <= 36
    ]
    unique_chunks = list(dict.fromkeys([chunk for chunk in [*normalized_chunks, *compound_chunks, *compact_chunks] if chunk]))
    search_blob = f" {' '.join(unique_chunks)} "
    token_list = list(dict.fromkeys([
        token
        for chunk in [*normalized_chunks, *compound_chunks, *compact_chunks]
        for token in chunk.split()
        if len(token) >= 2 and token not in PRODUCT_SEARCH_STOP_WORDS
    ]))
    search_name = normalize_product_search_text(product.get('name'))
    search_brand = normalize_product_search_text(product.get('brand'))
    search_model = normalize_product_search_text(product.get('model_number'))
    search_synonyms = normalize_product_search_text(' '.join([
        str(product.get('synonyms') or ''),
        *derived_aliases,
    ]))
    typo_chunks = [search_name, search_brand, search_model]
    typo_token_set = frozenset([
        token
        for chunk in [*typo_chunks, *build_compound_product_search_terms(typo_chunks)]
        for token in normalize_product_search_text(chunk).split()
        if 4 <= len(token) <= 18
        and token not in PRODUCT_SEARCH_STOP_WORDS
        and re.fullmatch(r'[a-zа-я]+', token)
    ])
    sort_index_raw = product.get('sort_index')
    try:
        sort_index = int(sort_index_raw) if sort_index_raw is not None else None
    except (TypeError, ValueError):
        sort_index = None

    return {
        'id': int(product.get('id') or 0),
        'folder_id': int(product.get('folder_id') or 0) or None,
        'sort_index': sort_index,
        'display_name': str(product.get('name') or ''),
        'search_name': search_name,
        'search_brand': search_brand,
        'search_model': search_model,
        'search_synonyms': search_synonyms,
        'search_blob': search_blob,
        'search_token_list': token_list,
        'search_token_set': frozenset(token_list),
        'search_typo_token_set': typo_token_set,
        'search_name_tokens': build_search_token_set(search_name),
        'search_brand_tokens': build_search_token_set(search_brand),
        'search_model_tokens': build_search_token_set(search_model),
        'search_synonyms_tokens': build_search_token_set(search_synonyms),
        'search_model_numbers': extract_product_search_numbers(' '.join([
            str(product.get('model_number') or ''),
        ])),
        'search_storage_numbers': extract_storage_search_numbers(
            product.get('storage'),
            product.get('name'),
            product.get('synonyms'),
        ),
        'search_is_ps5_console': bool(re.match(r'^(?:sony )?(?:playstation 5|ps5)\b', search_name)),
        'search_is_accessory': any(hint in combined_search_source for hint in PRODUCT_ACCESSORY_SEARCH_HINTS),
        'search_is_gamepad': bool(re.search(r'(геймпад|джойстик|контроллер|controller|gamepad|joystick|dualsense|joy\s*con|joycon)', combined_search_source)),
        'search_is_game': bool(re.search(r'(^| )игра( |$)|(^| )game( |$)', combined_search_source)),
        'search_is_phone': bool(re.search(r'(iphone|айфон|смартфон|phone|galaxy|redmi|oneplus|pixel|xiaomi|honor|huawei|realme)', combined_search_source)),
    }


def get_cached_product_search_entries(db, user_id):
    cached_index = PRODUCT_SEARCH_CACHE.get(user_id)
    if cached_index is not None:
        return cached_index

    rows = db.execute(
        """
        SELECT id, name, synonyms, brand, model_number, country, storage, color, folder_id, sort_index
        FROM products
        WHERE user_id = ?
        ORDER BY CASE WHEN sort_index IS NULL THEN 1 ELSE 0 END, sort_index, id
        """,
        (user_id,),
    ).fetchall()
    entries = [build_server_product_search_entry(row) for row in rows]
    entry_by_id = {int(entry['id']): entry for entry in entries}
    token_index = {}
    typo_index = {}
    for entry in entries:
        product_id = int(entry['id'])
        for token in entry.get('search_token_set') or ():
            token_index.setdefault(token, set()).add(product_id)
        for token in entry.get('search_typo_token_set') or ():
            for typo_key in build_product_search_typo_keys(token):
                typo_index.setdefault(typo_key, set()).add(product_id)

    cached_index = {
        'entries': entries,
        'entry_by_id': entry_by_id,
        'token_index': token_index,
        'typo_index': typo_index,
    }
    PRODUCT_SEARCH_CACHE[user_id] = cached_index
    return cached_index


def build_product_search_needles(query):
    words = tokenize_product_search_query(query)
    if not words:
        return []
    needles = []
    for word in words:
        variants = set(collect_product_search_terms(word))
        variants.add(normalize_product_search_text(word))
        needles.append([variant for variant in variants if variant])
    return needles


def token_set_matches_product_search_variant(token_set, variant):
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_variant:
        return False
    tokens = token_set or ()
    if normalized_variant in tokens:
        return True

    if normalized_variant.isdigit():
        for token in tokens:
            if token == normalized_variant:
                return True
            # A16 / i16 should match query "16", but router model WE1626 should not.
            if token.endswith(normalized_variant) and len(token) <= len(normalized_variant) + 3:
                prefix = token[:-len(normalized_variant)]
                if prefix.isalpha():
                    return True
        return False

    if len(normalized_variant) < 3:
        return False

    for token in tokens:
        if len(token) < 2 or token in PRODUCT_SEARCH_STOP_WORDS:
            continue
        if token.startswith(normalized_variant):
            return True
        if normalized_variant in token or token in normalized_variant:
            return True
        if is_product_search_typo_match(token, normalized_variant):
            return True
    return False


def normalized_search_field_matches(field, token_set, variant):
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_variant:
        return False
    normalized_field = str(field or '')
    if f' {normalized_variant} ' in f' {normalized_field} ':
        return True
    if token_set_matches_product_search_variant(token_set, normalized_variant):
        return True
    if normalized_variant.isdigit():
        return False
    return len(normalized_variant) >= 3 and normalized_variant in normalized_field


def normalized_search_field_matches_fast(field, token_set, variant):
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_variant:
        return False
    normalized_field = str(field or '')
    tokens = token_set or set()
    if normalized_variant in tokens:
        return True
    if f' {normalized_variant} ' in f' {normalized_field} ':
        return True
    if normalized_variant.isdigit():
        return False
    return len(normalized_variant) >= 3 and normalized_variant in normalized_field


def token_matches_product_search_variant_fast(token, variant):
    normalized_token = normalize_product_search_text(token)
    normalized_variant = normalize_product_search_text(variant)
    if not normalized_token or not normalized_variant:
        return False
    if normalized_token == normalized_variant:
        return True
    if normalized_variant.isdigit():
        if normalized_token.endswith(normalized_variant) and len(normalized_token) <= len(normalized_variant) + 3:
            prefix = normalized_token[:-len(normalized_variant)]
            return prefix.isalpha()
        return False
    if len(normalized_variant) < 3 or len(normalized_token) < 2:
        return False
    return (
        normalized_token.startswith(normalized_variant)
        or normalized_variant in normalized_token
        or normalized_token in normalized_variant
        or is_product_search_typo_match(normalized_token, normalized_variant)
    )


def get_candidate_ids_from_search_index(search_index, needles):
    if not isinstance(search_index, dict):
        return None
    token_index = search_index.get('token_index') or {}
    typo_index = search_index.get('typo_index') or {}
    if not token_index or not needles:
        return None

    candidate_sets = []
    token_items = list(token_index.items())
    for variants in needles:
        ids_for_word = set()
        normalized_variants = [
            normalize_product_search_text(variant)
            for variant in (variants or [])
            if normalize_product_search_text(variant)
        ]
        for variant in normalized_variants:
            direct_ids = token_index.get(variant)
            if direct_ids:
                ids_for_word.update(direct_ids)
        if ids_for_word:
            candidate_sets.append(ids_for_word)
            continue

        for variant in normalized_variants:
            typo_ids = set()
            for typo_key in build_product_search_typo_keys(variant):
                found_ids = typo_index.get(typo_key)
                if found_ids:
                    typo_ids.update(found_ids)
            if typo_ids:
                ids_for_word.update(typo_ids)
        if ids_for_word:
            candidate_sets.append(ids_for_word)
            continue

        for variant in normalized_variants:
            for token, product_ids in token_items:
                if token_matches_product_search_variant_fast(token, variant):
                    ids_for_word.update(product_ids)

        if not ids_for_word:
            return set()
        candidate_sets.append(ids_for_word)

    if not candidate_sets:
        return None
    candidates = candidate_sets[0]
    for ids_for_word in candidate_sets[1:]:
        candidates = candidates.intersection(ids_for_word)
        if not candidates:
            break
    return candidates


def product_entry_matches_search(entry, needles):
    if not needles:
        return True
    haystack = str(entry.get('search_blob') or '')
    token_set = entry.get('search_token_set') or set()

    for variants in needles:
        matched = False
        for variant in variants:
            normalized_variant = normalize_product_search_text(variant)
            if not normalized_variant:
                continue
            if normalized_search_field_matches(haystack, token_set, normalized_variant):
                matched = True
                break
        if not matched:
            return False
    return True


def get_product_entry_search_relevance(entry, needles, query_words):
    name = str(entry.get('search_name') or '')
    brand = str(entry.get('search_brand') or '')
    model = str(entry.get('search_model') or '')
    synonyms = str(entry.get('search_synonyms') or '')
    name_tokens = entry.get('search_name_tokens') or set()
    brand_tokens = entry.get('search_brand_tokens') or set()
    model_tokens = entry.get('search_model_tokens') or set()
    synonym_tokens = entry.get('search_synonyms_tokens') or set()
    normalized_phrase = normalize_product_search_text(' '.join(query_words or []))
    accessory_intent = is_accessory_search_intent(query_words)
    gamepad_intent = is_gamepad_search_intent(query_words)
    device_model_intent = looks_like_device_model_query(query_words)
    accessory_product = bool(entry.get('search_is_accessory'))
    gamepad_product = bool(entry.get('search_is_gamepad'))
    game_product = bool(entry.get('search_is_game'))
    phone_product = bool(entry.get('search_is_phone'))
    ps5_console_product = bool(entry.get('search_is_ps5_console'))
    ps5_intent = is_ps5_search_intent(query_words)
    dualsense_product = 'dualsense' in name or 'dual sense' in name
    numeric_words = [word for word in (query_words or []) if re.match(r'^\d+$', word)]
    has_storage_intent = has_explicit_storage_query_intent(query_words)
    model_numbers = entry.get('search_model_numbers') or []
    storage_numbers = entry.get('search_storage_numbers') or []

    score = 0
    name_hits = 0
    direct_hits = 0

    for variants in needles:
        has_name_hit = any(normalized_search_field_matches(name, name_tokens, variant) for variant in variants)
        has_brand_hit = any(normalized_search_field_matches(brand, brand_tokens, variant) for variant in variants)
        has_model_hit = any(normalized_search_field_matches(model, model_tokens, variant) for variant in variants)
        has_synonym_hit = any(normalized_search_field_matches(synonyms, synonym_tokens, variant) for variant in variants)

        if has_name_hit:
            score += 260
            name_hits += 1
            direct_hits += 1
        if has_brand_hit:
            score += 180
            direct_hits += 1
        if has_model_hit:
            score += 170
            direct_hits += 1
        if has_synonym_hit:
            score += 120

    if normalized_phrase:
        if normalized_search_field_matches(name, name_tokens, normalized_phrase):
            score += 280
        elif normalized_search_field_matches(model, model_tokens, normalized_phrase):
            score += 220
        elif normalized_search_field_matches(synonyms, synonym_tokens, normalized_phrase):
            score += 140

    if (not accessory_intent) and (not has_storage_intent) and len(numeric_words) == 1 and numeric_words[0] not in PRODUCT_COMMON_STORAGE_NUMBERS:
        query_number = numeric_words[0]
        has_model_number_hit = query_number in model_numbers or normalized_search_field_matches(model, model_tokens, query_number)
        has_storage_only_hit = query_number in storage_numbers and not has_model_number_hit

        if has_model_number_hit:
            score += 260
        if has_storage_only_hit:
            score -= 520 if accessory_product else 180

    if needles and name_hits == len(needles):
        score += 320
    if needles and direct_hits == len(needles):
        score += 180

    if accessory_intent:
        if accessory_product:
            score += 140
    else:
        score += -360 if accessory_product else 180

    if gamepad_intent:
        if gamepad_product:
            score += 980
        if ps5_intent:
            score += 3600 if dualsense_product else 200
        if game_product:
            score -= 1180
    elif ps5_intent and not accessory_intent:
        if ps5_console_product:
            score += 5000
        elif gamepad_product:
            score += 1300
        elif accessory_product or game_product:
            score -= 1400
        else:
            score -= 700

    if device_model_intent and not accessory_intent:
        if accessory_product:
            score -= 980
        if phone_product:
            score += 260

    return score


def get_quick_product_entry_search_relevance(entry, query_words):
    name = str(entry.get('search_name') or '')
    brand = str(entry.get('search_brand') or '')
    model = str(entry.get('search_model') or '')
    synonyms = str(entry.get('search_synonyms') or '')
    name_tokens = entry.get('search_name_tokens') or set()
    brand_tokens = entry.get('search_brand_tokens') or set()
    model_tokens = entry.get('search_model_tokens') or set()
    synonym_tokens = entry.get('search_synonyms_tokens') or set()
    normalized_words = [normalize_product_search_text(word) for word in (query_words or []) if normalize_product_search_text(word)]
    normalized_phrase = normalize_product_search_text(' '.join(normalized_words))
    accessory_intent = is_accessory_search_intent(normalized_words)
    accessory_product = bool(entry.get('search_is_accessory'))
    gamepad_intent = is_gamepad_search_intent(normalized_words)
    gamepad_product = bool(entry.get('search_is_gamepad'))
    game_product = bool(entry.get('search_is_game'))
    phone_product = bool(entry.get('search_is_phone'))
    ps5_console_product = bool(entry.get('search_is_ps5_console'))
    ps5_intent = is_ps5_search_intent(normalized_words)
    dualsense_product = 'dualsense' in name or 'dual sense' in name
    device_model_intent = looks_like_device_model_query(normalized_words)

    score = 0
    if normalized_phrase:
        if normalized_search_field_matches_fast(name, name_tokens, normalized_phrase):
            score += 420
        if normalized_search_field_matches_fast(brand, brand_tokens, normalized_phrase):
            score += 360
        if normalized_search_field_matches_fast(model, model_tokens, normalized_phrase):
            score += 300
        if normalized_search_field_matches_fast(synonyms, synonym_tokens, normalized_phrase):
            score += 160

    for word in normalized_words:
        if normalized_search_field_matches_fast(brand, brand_tokens, word):
            score += 180
        if normalized_search_field_matches_fast(name, name_tokens, word):
            score += 150
        if normalized_search_field_matches_fast(model, model_tokens, word):
            score += 130
        if normalized_search_field_matches_fast(synonyms, synonym_tokens, word):
            score += 70

    if accessory_intent:
        if accessory_product:
            score += 140
    else:
        score += -360 if accessory_product else 180

    if gamepad_intent:
        if gamepad_product:
            score += 980
        if ps5_intent:
            score += 3600 if dualsense_product else 200
        if game_product:
            score -= 1180
    elif ps5_intent and not accessory_intent:
        if ps5_console_product:
            score += 5000
        elif gamepad_product:
            score += 1300
        elif accessory_product or game_product:
            score -= 1400
        else:
            score -= 700

    if device_model_intent and not accessory_intent:
        if accessory_product:
            score -= 980
        if phone_product:
            score += 260

    return score


def search_products_in_entries(entries, query, folder_ids=None, limit=200):
    needles = build_product_search_needles(query)
    query_words = tokenize_product_search_query(query)
    if not needles:
        return []

    normalized_folder_ids = {int(folder_id) for folder_id in (folder_ids or set()) if folder_id}
    indexed_candidate_mode = False
    if isinstance(entries, dict):
        entry_by_id = entries.get('entry_by_id') or {}
        candidate_ids = get_candidate_ids_from_search_index(entries, needles)
        if candidate_ids is None:
            filtered_entries = list(entry_by_id.values())
        else:
            filtered_entries = [entry_by_id[product_id] for product_id in candidate_ids if product_id in entry_by_id]
            indexed_candidate_mode = True
    else:
        filtered_entries = entries

    if normalized_folder_ids:
        filtered_entries = [
            entry for entry in filtered_entries
            if int(entry.get('folder_id') or 0) in normalized_folder_ids
        ]

    ranked_entries = []
    use_quick_score = indexed_candidate_mode and len(filtered_entries) > 250
    verify_candidates = (not indexed_candidate_mode) or len(filtered_entries) <= 1000
    for entry in filtered_entries:
        if verify_candidates and not product_entry_matches_search(entry, needles):
            continue
        ranked_entries.append({
            'id': entry['id'],
            'score': get_quick_product_entry_search_relevance(entry, query_words) if use_quick_score else get_product_entry_search_relevance(entry, needles, query_words),
            'sort_tuple': get_product_search_sort_tuple(entry),
        })

    ranked_entries.sort(key=lambda item: (-item['score'], *item['sort_tuple']))
    if limit is None:
        return [item['id'] for item in ranked_entries]
    return [item['id'] for item in ranked_entries[:limit]]


def build_compact_product_payload(product_row):
    product = dict(product_row)
    photo_url = str(product.get('photo_url') or '').strip()
    if photo_url:
        product['photo_url'] = photo_url.split(',')[0].strip()
    product['proposed_count'] = 1 if int(product.get('pending_count') or 0) > 0 and int(product.get('confirmed_count') or 0) == 0 else 0
    return product


def build_compact_product_payloads(db, user_id, product_rows):
    products = [dict(product_row) for product_row in (product_rows or [])]
    if not products:
        return []

    product_ids = [int(product.get('id') or 0) for product in products if product.get('id')]
    counts_by_id = {
        product_id: {'confirmed_count': 0, 'pending_count': 0}
        for product_id in product_ids
    }
    if product_ids:
        placeholders = ','.join('?' for _ in product_ids)
        confirmed_rows = db.execute(f"""
            SELECT pm.product_id, COUNT(*) AS confirmed_count
            FROM product_messages pm
            JOIN messages m ON pm.message_id = m.id
            WHERE pm.status = 'confirmed'
              AND m.user_id = ?
              AND pm.product_id IN ({placeholders})
            GROUP BY pm.product_id
        """, (user_id, *product_ids)).fetchall()
        for row in confirmed_rows:
            counts_by_id[int(row['product_id'])]['confirmed_count'] = int(row['confirmed_count'] or 0)

        pending_rows = db.execute(f"""
            SELECT product_id, COUNT(*) AS pending_count
            FROM product_messages
            WHERE status = 'pending'
              AND product_id IN ({placeholders})
            GROUP BY product_id
        """, tuple(product_ids)).fetchall()
        for row in pending_rows:
            counts_by_id[int(row['product_id'])]['pending_count'] = int(row['pending_count'] or 0)

    for product in products:
        counts = counts_by_id.get(int(product.get('id') or 0), {})
        product['confirmed_count'] = int(counts.get('confirmed_count') or 0)
        product['pending_count'] = int(counts.get('pending_count') or 0)

    return [build_compact_product_payload(product) for product in products]


def build_sql_search_variants_for_word(word, max_variants=14):
    normalized_word = normalize_product_search_text(word)
    if not normalized_word:
        return []
    candidates = [normalized_word, *collect_product_search_terms(normalized_word)]
    variants = []
    seen = set()
    for candidate in candidates:
        normalized = normalize_product_search_text(candidate)
        if len(normalized) < 2 or normalized in PRODUCT_SEARCH_STOP_WORDS or normalized in seen:
            continue
        seen.add(normalized)
        variants.append(normalized)
        if len(variants) >= max_variants:
            break
    for literal in PRODUCT_SQL_LITERAL_SEARCH_EXPANSIONS.get(normalized_word, []):
        if literal not in seen and len(variants) < max_variants:
            seen.add(literal)
            variants.append(literal)
    return variants


def build_sql_search_needles(query):
    return [
        variants
        for variants in (build_sql_search_variants_for_word(word) for word in tokenize_product_search_query(query))
        if variants
    ]


def compact_sql_search_value(value):
    return normalize_product_search_text(value).replace(' ', '')


def compact_sql_search_field_sql(field):
    return (
        "LOWER("
        f"REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE({field}, ''), ' ', ''), '-', ''), '/', ''), '.', ''), '+', '')"
        ")"
    )


def fetch_ranked_product_rows_for_ids(db, select_fields, base_from, base_params, ranked_ids):
    page_ids = [int(product_id) for product_id in (ranked_ids or []) if product_id]
    if not page_ids:
        return []
    placeholders = ','.join('?' for _ in page_ids)
    order_case = ' '.join(f"WHEN {product_id} THEN {index}" for index, product_id in enumerate(page_ids))
    query = f"""
        SELECT {select_fields}
        {base_from}
        AND p.id IN ({placeholders})
        ORDER BY CASE p.id {order_case} ELSE {len(page_ids)} END
    """
    return db.execute(query, (*base_params, *page_ids)).fetchall()


def append_sql_search_field_match(parts, params, field, like_variant):
    like_value = f"%{like_variant}%"
    parts.append(f"{field} LIKE ?")
    params.append(like_value)


def build_fast_iphone_search_phrases(query):
    words = tokenize_product_search_query(query)
    if not words:
        return []

    canonical_words = []
    has_iphone = False
    for word in words:
        variants = set(collect_product_search_terms(word))
        normalized = normalize_product_search_text(word)
        if 'iphone' in variants:
            canonical_words.append('iphone')
            has_iphone = True
        elif 'pro' in variants:
            canonical_words.append('pro')
        elif 'max' in variants:
            canonical_words.append('max')
        elif 'plus' in variants:
            canonical_words.append('plus')
        elif 'ultra' in variants:
            canonical_words.append('ultra')
        elif 'mini' in variants:
            canonical_words.append('mini')
        elif 'air' in variants:
            canonical_words.append('air')
        elif normalized:
            canonical_words.append(normalized)

    if not has_iphone or len(canonical_words) < 2:
        return []

    phrases = []
    main_phrase = ' '.join(canonical_words)
    model_phrase = ' '.join(word for word in canonical_words if word != 'iphone')
    candidate_phrases = [main_phrase]
    if model_phrase and not re.fullmatch(r'\d+', model_phrase):
        candidate_phrases.append(model_phrase)
    for phrase in candidate_phrases:
        phrase = normalize_product_search_text(phrase)
        if phrase and phrase not in phrases:
            phrases.append(phrase)
    return phrases


def has_search_alias(words, alias):
    normalized_alias = normalize_product_search_text(alias)
    return any(normalized_alias in set(collect_product_search_terms(word)) for word in (words or []))


def is_ps5_search_intent(words, raw_query=""):
    normalized_words = {
        normalize_product_search_text(word)
        for word in (words or [])
        if normalize_product_search_text(word)
    }
    normalized_query = normalize_product_search_text(raw_query)
    expanded_terms = set()
    for word in (words or []):
        expanded_terms.update(collect_product_search_terms(word))
    return (
        'ps5' in normalized_words
        or 'пс5' in normalized_words
        or 'ps5' in expanded_terms
        or 'пс5' in expanded_terms
        or re.search(r'\bps5\b', normalized_query)
        or re.search(r'\bпс5\b', normalized_query)
    )


def expand_like_case_variants(variant):
    values = [variant]
    title_value = variant.title()
    capitalize_value = variant.capitalize()
    upper_value = variant.upper()
    for value in (title_value, capitalize_value, upper_value):
        if value and value not in values:
            values.append(value)
    return values


def build_ps5_extra_search_conditions(words):
    conditions = []
    params = []
    searchable_fields = (
        'p.name',
        'p.synonyms',
        'p.brand',
        'p.model_number',
        'p.color',
        'p.storage',
    )
    for word in (words or []):
        normalized_word = normalize_product_search_text(word)
        word_terms = set(collect_product_search_terms(word))
        if normalized_word in {'ps5', 'пс5'} or 'ps5' in word_terms or 'пс5' in word_terms:
            continue
        variants = build_sql_search_variants_for_word(word, max_variants=12)
        parts = []
        for variant in variants:
            for like_variant in expand_like_case_variants(variant):
                for field in searchable_fields:
                    append_sql_search_field_match(parts, params, field, like_variant)
        if parts:
            conditions.append(f"({' OR '.join(parts)})")
    return conditions, params


def product_accessory_match_sql():
    return """
        (
            p.name LIKE '%чех%' OR p.name LIKE '%Чех%' OR LOWER(p.name) LIKE '%case%' OR LOWER(p.name) LIKE '%cover%' OR LOWER(p.name) LIKE '%sleeve%'
            OR p.name LIKE '%накладк%' OR p.name LIKE '%Накладк%' OR p.name LIKE '%стекл%' OR p.name LIKE '%Стекл%' OR LOWER(p.name) LIKE '%glass%' OR LOWER(p.name) LIKE '%film%'
            OR p.name LIKE '%кабель%' OR p.name LIKE '%Кабель%' OR LOWER(p.name) LIKE '%cable%' OR LOWER(p.name) LIKE '%cord%'
            OR p.name LIKE '%заряд%' OR p.name LIKE '%Заряд%' OR LOWER(p.name) LIKE '%charger%' OR LOWER(p.name) LIKE '%adapter%'
            OR p.name LIKE '%ремеш%' OR p.name LIKE '%Ремеш%' OR p.name LIKE '%браслет%' OR p.name LIKE '%Браслет%' OR LOWER(p.name) LIKE '%strap%' OR LOWER(p.name) LIKE '% band%' OR LOWER(p.name) LIKE '% loop%' OR LOWER(p.name) LIKE '%milanese%'
            OR LOWER(p.name) LIKE '%ssd%' OR p.name LIKE '%накопител%' OR p.name LIKE '%Накопител%' OR p.name LIKE '%дисковод%' OR p.name LIKE '%Дисковод%' OR p.name LIKE '%панел%' OR p.name LIKE '%Панел%' OR LOWER(p.name) LIKE '%panel%'
            OR p.name LIKE '%подписк%' OR p.name LIKE '%Подписк%' OR p.name LIKE '%игра для%' OR p.name LIKE '%Игра для%' OR LOWER(p.name) LIKE '% game for%'
            OR p.synonyms LIKE '%чех%' OR p.synonyms LIKE '%Чех%' OR LOWER(p.synonyms) LIKE '%case%' OR LOWER(p.synonyms) LIKE '%cover%' OR LOWER(p.synonyms) LIKE '%sleeve%'
            OR p.synonyms LIKE '%накладк%' OR p.synonyms LIKE '%Накладк%' OR p.synonyms LIKE '%стекл%' OR p.synonyms LIKE '%Стекл%' OR LOWER(p.synonyms) LIKE '%glass%' OR LOWER(p.synonyms) LIKE '%film%'
            OR p.synonyms LIKE '%кабель%' OR p.synonyms LIKE '%Кабель%' OR LOWER(p.synonyms) LIKE '%cable%' OR LOWER(p.synonyms) LIKE '%cord%'
            OR p.synonyms LIKE '%заряд%' OR p.synonyms LIKE '%Заряд%' OR LOWER(p.synonyms) LIKE '%charger%' OR LOWER(p.synonyms) LIKE '%adapter%'
            OR p.synonyms LIKE '%ремеш%' OR p.synonyms LIKE '%Ремеш%' OR p.synonyms LIKE '%браслет%' OR p.synonyms LIKE '%Браслет%' OR LOWER(p.synonyms) LIKE '%strap%' OR LOWER(p.synonyms) LIKE '% band%' OR LOWER(p.synonyms) LIKE '% loop%' OR LOWER(p.synonyms) LIKE '%milanese%'
            OR LOWER(p.synonyms) LIKE '%ssd%' OR p.synonyms LIKE '%накопител%' OR p.synonyms LIKE '%Накопител%' OR p.synonyms LIKE '%дисковод%' OR p.synonyms LIKE '%Дисковод%' OR p.synonyms LIKE '%панел%' OR p.synonyms LIKE '%Панел%' OR LOWER(p.synonyms) LIKE '%panel%'
            OR p.synonyms LIKE '%подписк%' OR p.synonyms LIKE '%Подписк%' OR p.synonyms LIKE '%игра для%' OR p.synonyms LIKE '%Игра для%' OR LOWER(p.synonyms) LIKE '% game for%'
        )
    """


def product_accessory_penalty_sql(accessory_first=False):
    accessory_rank = 0 if accessory_first else 1
    default_rank = 1 if accessory_first else 0
    return f"""
        CASE
            WHEN {product_accessory_match_sql()} THEN {accessory_rank}
            ELSE {default_rank}
        END
    """


def product_primary_device_rank_sql():
    return f"""
        CASE
            WHEN p.name LIKE 'Смартфон %' OR p.name LIKE 'смартфон %' OR LOWER(p.name) LIKE 'apple iphone%' OR LOWER(p.name) LIKE '% apple iphone%' THEN 0
            WHEN LOWER(p.name) LIKE 'apple watch%' OR LOWER(p.name) LIKE 'samsung galaxy watch%' OR LOWER(p.name) LIKE 'huawei watch%' THEN 0
            WHEN LOWER(p.name) LIKE 'sony playstation 5%' OR LOWER(p.name) LIKE 'playstation 5%' THEN 0
            WHEN LOWER(p.name) LIKE '%macbook%' OR LOWER(p.name) LIKE '%ipad%' OR p.name LIKE '%Планшет%' OR p.name LIKE '%планшет%' OR p.name LIKE '%Ноутбук%' OR p.name LIKE '%ноутбук%' THEN 0
            WHEN LOWER(p.name) LIKE '%airpods%' OR p.name LIKE '%Наушники Apple%' OR p.name LIKE '%наушники Apple%' THEN 0
            WHEN LOWER(p.name) LIKE '%dualsense%' OR LOWER(p.name) LIKE '%gamepad%' OR p.name LIKE '%Геймпад%' OR p.name LIKE '%геймпад%' THEN 1
            WHEN {product_accessory_match_sql()} THEN 3
            ELSE 2
        END
    """


def product_variant_rank_sql():
    return """
        CASE
            WHEN LOWER(p.name) LIKE '%iphone%' THEN
                CASE
                    WHEN LOWER(p.name) LIKE '% pro max%' THEN 4
                    WHEN LOWER(p.name) LIKE '% ultra%' THEN 5
                    WHEN LOWER(p.name) LIKE '% pro%' THEN 3
                    WHEN LOWER(p.name) LIKE '% plus%' THEN 2
                    WHEN LOWER(p.name) LIKE '% air%' THEN 1
                    WHEN LOWER(p.name) LIKE '% mini%' THEN 6
                    ELSE 0
                END
            ELSE 0
        END
    """


def product_storage_rank_sql():
    return """
        CASE
            WHEN p.storage LIKE '64 %' OR p.storage LIKE '64GB%' OR p.name LIKE '%64 ГБ%' OR p.name LIKE '%64 GB%' THEN 64
            WHEN p.storage LIKE '128 %' OR p.storage LIKE '128GB%' OR p.name LIKE '%128 ГБ%' OR p.name LIKE '%128 GB%' THEN 128
            WHEN p.storage LIKE '256 %' OR p.storage LIKE '256GB%' OR p.name LIKE '%256 ГБ%' OR p.name LIKE '%256 GB%' THEN 256
            WHEN p.storage LIKE '512 %' OR p.storage LIKE '512GB%' OR p.name LIKE '%512 ГБ%' OR p.name LIKE '%512 GB%' THEN 512
            WHEN p.storage LIKE '1 T%' OR p.storage LIKE '1T%' OR p.name LIKE '%1 ТБ%' OR p.name LIKE '%1 TB%' THEN 1024
            WHEN p.storage LIKE '2 T%' OR p.storage LIKE '2T%' OR p.name LIKE '%2 ТБ%' OR p.name LIKE '%2 TB%' THEN 2048
            ELSE 999999
        END
    """


@app.route('/api/products', methods=['GET', 'POST'])
@login_required
def manage_products():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        is_compact = str(request.args.get('compact', '')).strip().lower() in {'1', 'true', 'yes'}
        connected_only = str(request.args.get('connected_only', '')).strip().lower() in {'1', 'true', 'yes'}
        raw_query = str(request.args.get('q', '') or '').strip()
        page_arg = request.args.get('page')
        limit_arg = request.args.get('limit')
        use_paged_compact = is_compact and (
            bool(raw_query)
            or page_arg is not None
            or limit_arg is not None
            or bool(str(request.args.get('folder_ids', '') or '').strip())
        )
        try:
            page = int(page_arg or 1)
        except (TypeError, ValueError):
            page = 1
        try:
            limit = int(limit_arg or 20)
        except (TypeError, ValueError):
            limit = 20
        page = max(1, page)
        limit = max(1, min(limit, 100))

        folder_ids = []
        for part in str(request.args.get('folder_ids', '') or '').split(','):
            part = part.strip()
            if not part:
                continue
            try:
                folder_ids.append(int(part))
            except ValueError:
                continue
        folder_ids = list(dict.fromkeys(folder_ids))
        if len(folder_ids) > 200:
            folder_ids = []

        select_fields = """
            p.id,
            p.name,
            p.synonyms,
            p.price,
            p.folder_id,
            p.photo_url,
            p.brand,
            p.country,
            p.weight,
            p.model_number,
            p.is_on_request,
            p.color,
            p.storage,
            p.ram,
            p.warranty,
            p.sort_index
        """ if is_compact else "p.*"

        if is_compact:
            base_from = "FROM products p WHERE p.user_id = ?"
            base_params = [user_id]
        else:
            base_from = f"""
                FROM products p
                LEFT JOIN (
                    SELECT pm.product_id, COUNT(*) AS confirmed_count
                    FROM product_messages pm
                    JOIN messages m ON pm.message_id = m.id
                    WHERE pm.status = 'confirmed' AND m.user_id = ?
                    GROUP BY pm.product_id
                ) confirmed ON confirmed.product_id = p.id
                LEFT JOIN (
                    SELECT product_id, COUNT(*) AS pending_count
                    FROM product_messages
                    WHERE status = 'pending'
                    GROUP BY product_id
                ) pending ON pending.product_id = p.id
                WHERE p.user_id = ?
            """
            base_params = [user_id, user_id]

        if folder_ids:
            placeholders = ','.join('?' for _ in folder_ids)
            base_from += f" AND p.folder_id IN ({placeholders})"
            base_params.extend(folder_ids)
        if connected_only:
            if is_compact:
                base_from += """
                    AND EXISTS (
                        SELECT 1
                        FROM product_messages pm
                        JOIN messages m ON pm.message_id = m.id
                        WHERE pm.product_id = p.id
                          AND pm.status = 'confirmed'
                          AND m.user_id = ?
                    )
                """
                base_params.append(user_id)
            else:
                base_from += " AND COALESCE(confirmed.confirmed_count, 0) > 0"

        if use_paged_compact:
            offset = (page - 1) * limit

            if raw_query:
                sql_needles = build_sql_search_needles(raw_query)
                if not sql_needles:
                    return jsonify({
                        'items': [],
                        'total': 0,
                        'page': page,
                        'limit': limit,
                    })
                query_words = tokenize_product_search_query(raw_query)
                ps5_search_intent = is_ps5_search_intent(query_words, raw_query)
                if len(query_words) == 1 and not ps5_search_intent:
                    brand_variants = build_sql_search_variants_for_word(query_words[0], max_variants=24)
                    if brand_variants:
                        brand_placeholders = ','.join('?' for _ in brand_variants)
                        brand_from = f"{base_from} AND p.brand COLLATE NOCASE IN ({brand_placeholders})"
                        brand_params = [*base_params, *brand_variants]
                        brand_total = int(db.execute(
                            f"SELECT COUNT(*) AS total {brand_from}",
                            tuple(brand_params),
                        ).fetchone()['total'] or 0)
                        if brand_total:
                            brand_accessory_intent = is_accessory_search_intent(query_words)
                            brand_query = f"""
                                SELECT {select_fields}
                                {brand_from}
                                ORDER BY
                                    {product_accessory_penalty_sql(True) if brand_accessory_intent else product_primary_device_rank_sql()},
                                    {product_accessory_penalty_sql()} ,
                                    {product_variant_rank_sql()},
                                    {product_storage_rank_sql()},
                                    CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END,
                                    p.sort_index,
                                    p.id
                                LIMIT ? OFFSET ?
                            """
                            rows = db.execute(brand_query, (*brand_params, limit, offset)).fetchall()
                            return jsonify({
                                'items': build_compact_product_payloads(db, user_id, rows),
                                'total': brand_total,
                                'page': page,
                                'limit': limit,
                            })

                fast_phrase_conditions = []
                fast_phrase_params = []
                for phrase in build_fast_iphone_search_phrases(raw_query):
                    like_value = f"%{phrase}%"
                    fast_phrase_conditions.extend([
                        "p.name LIKE ? COLLATE NOCASE",
                        "p.synonyms LIKE ? COLLATE NOCASE",
                    ])
                    fast_phrase_params.extend([like_value, like_value])
                if fast_phrase_conditions:
                    phrase_from = f"{base_from} AND ({' OR '.join(fast_phrase_conditions)})"
                    phrase_params = [*base_params, *fast_phrase_params]
                    phrase_total = int(db.execute(
                        f"SELECT COUNT(*) AS total {phrase_from}",
                        tuple(phrase_params),
                    ).fetchone()['total'] or 0)
                    if phrase_total:
                        phrase_accessory_intent = is_accessory_search_intent(query_words)
                        phrase_query = f"""
                            SELECT {select_fields}
                            {phrase_from}
                            ORDER BY
                                {product_accessory_penalty_sql(True) if phrase_accessory_intent else product_primary_device_rank_sql()},
                                {product_accessory_penalty_sql()} ,
                                CASE WHEN LOWER(p.name) LIKE ? THEN 0 ELSE 1 END,
                                {product_variant_rank_sql()},
                                {product_storage_rank_sql()},
                                CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END,
                                p.sort_index,
                                p.id
                            LIMIT ? OFFSET ?
                        """
                        phrase_rank_params = [f"%{normalize_product_search_text(raw_query)}%", limit, offset]
                        rows = db.execute(phrase_query, (*phrase_params, *phrase_rank_params)).fetchall()
                        return jsonify({
                            'items': build_compact_product_payloads(db, user_id, rows),
                            'total': phrase_total,
                            'page': page,
                            'limit': limit,
                        })

                if is_gamepad_search_intent(query_words) and ps5_search_intent:
                    gamepad_from = f"""
                        {base_from}
                        AND (
                            p.name LIKE ? COLLATE NOCASE
                            OR p.synonyms LIKE ? COLLATE NOCASE
                            OR p.name LIKE ? COLLATE NOCASE
                            OR p.synonyms LIKE ? COLLATE NOCASE
                            OR p.name LIKE ? COLLATE NOCASE
                            OR p.synonyms LIKE ? COLLATE NOCASE
                        )
                        AND (
                            p.name LIKE ? COLLATE NOCASE
                            OR p.synonyms LIKE ? COLLATE NOCASE
                        )
                    """
                    gamepad_params = [
                        *base_params,
                        '%DualSense%',
                        '%DualSense%',
                        '%геймпад%',
                        '%геймпад%',
                        '%controller%',
                        '%controller%',
                        '%PS5%',
                        '%PS5%',
                    ]
                    gamepad_total = int(db.execute(
                        f"SELECT COUNT(*) AS total {gamepad_from}",
                        tuple(gamepad_params),
                    ).fetchone()['total'] or 0)
                    if gamepad_total:
                        gamepad_query = f"""
                            SELECT {select_fields}
                            {gamepad_from}
                            ORDER BY
                                CASE WHEN p.name LIKE '%DualSense%' THEN 0 ELSE 1 END,
                                CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END,
                                p.sort_index,
                                p.id
                            LIMIT ? OFFSET ?
                        """
                        rows = db.execute(gamepad_query, (*gamepad_params, limit, offset)).fetchall()
                        return jsonify({
                            'items': build_compact_product_payloads(db, user_id, rows),
                            'total': gamepad_total,
                            'page': page,
                                'limit': limit,
                            })

                if ps5_search_intent and not is_accessory_search_intent(query_words):
                    ps5_extra_conditions, ps5_extra_params = build_ps5_extra_search_conditions(query_words)
                    ps5_extra_sql = ''.join(f" AND {condition}" for condition in ps5_extra_conditions)
                    ps5_from = f"""
                        {base_from}
                        AND (
                            LOWER(p.name) LIKE '%playstation 5%'
                            OR LOWER(p.name) LIKE '%ps5%'
                            OR LOWER(p.model_number) LIKE '%ps5%'
                            OR LOWER(p.synonyms) LIKE '%playstation 5%'
                            OR LOWER(p.synonyms) LIKE '%ps5%'
                        )
                        {ps5_extra_sql}
                    """
                    ps5_params = [*base_params, *ps5_extra_params]
                    ps5_total = int(db.execute(
                        f"SELECT COUNT(*) AS total {ps5_from}",
                        tuple(ps5_params),
                    ).fetchone()['total'] or 0)
                    if ps5_total:
                        ps5_query = f"""
                            SELECT {select_fields}
                            {ps5_from}
                            ORDER BY
                                CASE
                                    WHEN LOWER(p.name) LIKE 'sony playstation 5%' OR LOWER(p.name) LIKE 'playstation 5%' THEN 0
                                    WHEN LOWER(p.name) LIKE '%dualsense%' THEN 1
                                    WHEN LOWER(p.name) LIKE '%gamepad%' OR p.name LIKE '%Геймпад%' OR p.name LIKE '%геймпад%' THEN 2
                                    WHEN {product_accessory_match_sql()} THEN 4
                                    ELSE 3
                                END,
                                {product_storage_rank_sql()},
                                CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END,
                                p.sort_index,
                                p.id
                            LIMIT ? OFFSET ?
                        """
                        rows = db.execute(ps5_query, (*ps5_params, limit, offset)).fetchall()
                        return jsonify({
                            'items': build_compact_product_payloads(db, user_id, rows),
                            'total': ps5_total,
                            'page': page,
                            'limit': limit,
                        })

                search_from = base_from
                search_params = list(base_params)
                searchable_fields = (
                    'p.name',
                    'p.synonyms',
                    'p.brand',
                    'p.model_number',
                    'p.color',
                    'p.storage',
                )
                for variants in sql_needles:
                    parts = []
                    for variant in variants:
                        for like_variant in expand_like_case_variants(variant):
                            for field in searchable_fields:
                                append_sql_search_field_match(parts, search_params, field, like_variant)
                    if parts:
                        search_from += f" AND ({' OR '.join(parts)})"

                total_query = f"SELECT COUNT(*) AS total {search_from}"
                total = int(db.execute(total_query, tuple(search_params)).fetchone()['total'] or 0)
                if total == 0:
                    fallback_ids = search_products_in_entries(
                        get_cached_product_search_entries(db, user_id),
                        raw_query,
                        folder_ids=set(folder_ids),
                        limit=500,
                    )
                    if fallback_ids:
                        fallback_page_ids = fallback_ids[offset:offset + limit]
                        rows = fetch_ranked_product_rows_for_ids(
                            db,
                            select_fields,
                            base_from,
                            base_params,
                            fallback_page_ids,
                        )
                        return jsonify({
                            'items': build_compact_product_payloads(db, user_id, rows),
                            'total': len(fallback_ids),
                            'page': page,
                            'limit': limit,
                        })

                normalized_query = normalize_product_search_text(raw_query)
                first_word = tokenize_product_search_query(raw_query)[0] if tokenize_product_search_query(raw_query) else ''
                first_word = normalize_product_search_text(first_word)
                accessory_order_sql = product_accessory_penalty_sql(is_accessory_search_intent(query_words))
                query = f"""
                    SELECT {select_fields}
                    {search_from}
                    ORDER BY
                        CASE WHEN LOWER(p.brand) = ? THEN 0 ELSE 1 END,
                        {accessory_order_sql if is_accessory_search_intent(query_words) else product_primary_device_rank_sql()},
                        {product_accessory_penalty_sql()} ,
                        CASE WHEN LOWER(p.name) LIKE ? THEN 0 ELSE 1 END,
                        {product_variant_rank_sql()},
                        {product_storage_rank_sql()},
                        CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END,
                        p.sort_index,
                        p.id
                    LIMIT ? OFFSET ?
                """
                rank_params = [
                    first_word or normalized_query,
                    f"%{normalized_query}%",
                    limit,
                    offset,
                ]
                rows = db.execute(query, (*search_params, *rank_params)).fetchall()
                items = build_compact_product_payloads(db, user_id, rows)
                return jsonify({
                    'items': items,
                    'total': total,
                    'page': page,
                    'limit': limit,
                })

            total_query = f"SELECT COUNT(*) AS total {base_from}"
            total = int(db.execute(total_query, tuple(base_params)).fetchone()['total'] or 0)
            query = f"""
                SELECT {select_fields}
                {base_from}
                ORDER BY CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END, p.sort_index, p.id
                LIMIT ? OFFSET ?
            """
            rows = db.execute(query, (*base_params, limit, offset)).fetchall()
            return jsonify({
                'items': build_compact_product_payloads(db, user_id, rows),
                'total': total,
                'page': page,
                'limit': limit,
            })

        count_fields = "" if is_compact else """,
                   COALESCE(confirmed.confirmed_count, 0) AS confirmed_count,
                   COALESCE(pending.pending_count, 0) AS pending_count
        """
        query = f"""
            SELECT {select_fields}{count_fields}
            {base_from}
            ORDER BY CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END, p.sort_index, p.id
        """
        products = db.execute(query, tuple(base_params)).fetchall()

        if is_compact:
            return jsonify(build_compact_product_payloads(db, user_id, products))

        all_msgs = db.execute("SELECT text FROM messages WHERE user_id = ? ORDER BY date DESC LIMIT 1000", (user_id,)).fetchall()
        recent_message_lines = []
        for row in all_msgs:
            lines = row['text'].split('\n') if row['text'] else []
            for line in lines:
                if not line.strip():
                    continue
                line_words = clean_for_search(line)
                if line_words:
                    recent_message_lines.append(line_words)

        result = []
        for p in products:
            p_dict = dict(p)
            p_dict['proposed_count'] = 0

            if p_dict['confirmed_count'] == 0 and p_dict['synonyms']:
                synonyms_as_word_sets = []
                for s in p_dict['synonyms'].split(','):
                    if s.strip():
                        words = clean_for_search(s)
                        if words:
                            synonyms_as_word_sets.append(words)

                has_proposed = False
                if synonyms_as_word_sets and recent_message_lines:
                    for line_words in recent_message_lines:
                        for syn_set in synonyms_as_word_sets:
                            if syn_set.issubset(line_words):
                                if "pro" not in syn_set and "pro" in line_words:
                                    continue
                                if "max" not in syn_set and "max" in line_words:
                                    continue
                                if "plus" not in syn_set and "plus" in line_words:
                                    continue
                                if "ultra" not in syn_set and "ultra" in line_words:
                                    continue
                                if "mini" not in syn_set and "mini" in line_words:
                                    continue
                                if "fe" not in syn_set and "fe" in line_words:
                                    continue

                                has_proposed = True
                                break
                        if has_proposed:
                            break

                p_dict['proposed_count'] = 1 if has_proposed else 0

            if p_dict['proposed_count'] == 0 and int(p_dict.get('pending_count') or 0) > 0:
                p_dict['proposed_count'] = 1

            result.append(p_dict)

        return jsonify(result)
        
    # POST
    # POST
    # POST
    # POST
    # Так как мы передаем файлы, используем request.form, а не get_json()
    folder_id = request.form.get('folder_id')
    
    # 1. Обработка синонимов (теперь они приходят из формы)
    try:
        syns = json.loads(request.form.get('synonyms', '[]'))
    except:
        syns = []
    synonyms_str = ", ".join([s.strip() for s in syns if s.strip()])
    resolved_folder_id = resolve_product_folder_assignment(
        db,
        user_id,
        request.form.get('name'),
        folder_id,
        request.form.get('storage'),
    )
    
    # 2. ВСТАВЛЯЕМ ВАШ НОВЫЙ КОД ДЛЯ ФОТО ЗДЕСЬ:
    files = request.files.getlist('photos')
    saved_names = []
    for file in files:
        if file and file.filename:
            # Генерируем уникальное имя, чтобы не было конфликтов
            ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
            filename = f"{uuid.uuid4().hex}.{ext}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            saved_names.append(filename)

    # Сохраняем в БД просто как строку через запятую: "id1.jpg, id2.jpg"
    photo_url_str = ",".join(saved_names) 

    # 3. Сохраняем все в базу данных
    specs_payload = parse_specs_payload(
        request.form.get('specs'),
        {
            'brand': request.form.get('brand'),
            'country': request.form.get('country'),
            'weight': request.form.get('weight'),
            'model_number': request.form.get('model_number'),
            'color': request.form.get('color'),
            'storage': request.form.get('storage'),
            'ram': request.form.get('ram'),
            'warranty': request.form.get('warranty'),
        },
    )
    specs_json = json.dumps(specs_payload, ensure_ascii=False) if specs_payload else ""

    # 3. Сохраняем все в базу данных
    db.execute("""
        INSERT INTO products 
        (user_id, name, synonyms, price, folder_id, photo_url, brand, country, weight, model_number, is_on_request, color, storage, ram, warranty, description, description_html, specs, sort_index) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        user_id, 
        request.form.get('name'), 
        synonyms_str, 
        float(request.form.get('price', 0.0)), 
        resolved_folder_id,
        
        photo_url_str,
        
        request.form.get('brand'),
        request.form.get('country'),
        request.form.get('weight'),
        request.form.get('model_number'),
        1 if request.form.get('is_on_request') == '1' else 0,
        
        # 🔽 ДОБАВЛЯЕМ ЭТИ СТРОКИ СЮДА 🔽
        request.form.get('color'),
        request.form.get('storage'),
        request.form.get('ram'),
        request.form.get('warranty'),
        request.form.get('description'),
        request.form.get('description_html'),
        specs_json,
        get_next_product_sort_index(db, user_id)
    ))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/products/search')
@login_required
def search_products_route():
    user_id = session['user_id']
    raw_query = str(request.args.get('q', '') or '').strip()
    if not raw_query:
        return jsonify({'ids': [], 'count': 0})

    try:
        limit = int(request.args.get('limit', 200))
    except (TypeError, ValueError):
        limit = 200
    limit = max(10, min(limit, 500))

    folder_ids = set()
    for part in str(request.args.get('folder_ids', '') or '').split(','):
        part = part.strip()
        if not part:
            continue
        try:
            folder_ids.add(int(part))
        except ValueError:
            continue

    db = get_db()
    entries = get_cached_product_search_entries(db, user_id)
    result_ids = search_products_in_entries(entries, raw_query, folder_ids=folder_ids, limit=limit)
    return jsonify({
        'ids': result_ids,
        'count': len(result_ids),
    })

@app.route('/api/products/<int:prod_id>', methods=['GET', 'DELETE'])
@login_required
def delete_product(prod_id):
    if request.method == 'GET':
        db = get_db()
        user_id = session['user_id']
        product = db.execute(
            """
            SELECT p.*,
                   COALESCE(confirmed.confirmed_count, 0) AS confirmed_count,
                   COALESCE(pending.pending_count, 0) AS pending_count
            FROM products p
            LEFT JOIN (
                SELECT pm.product_id, COUNT(*) AS confirmed_count
                FROM product_messages pm
                JOIN messages m ON pm.message_id = m.id
                WHERE pm.status = 'confirmed' AND m.user_id = ?
                GROUP BY pm.product_id
            ) confirmed ON confirmed.product_id = p.id
            LEFT JOIN (
                SELECT product_id, COUNT(*) AS pending_count
                FROM product_messages
                WHERE status = 'pending'
                GROUP BY product_id
            ) pending ON pending.product_id = p.id
            WHERE p.id = ? AND p.user_id = ?
            """,
            (user_id, prod_id, user_id),
        ).fetchone()
        if not product:
            return jsonify({'error': 'Товар не найден'}), 404

        product_dict = dict(product)
        product_dict['proposed_count'] = 1 if int(product_dict.get('pending_count') or 0) > 0 and int(product_dict.get('confirmed_count') or 0) == 0 else 0
        return jsonify(product_dict)

    db = get_db()
    db.execute("DELETE FROM products WHERE id=? AND user_id=?", (prod_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/reports/confirmed')
@login_required
def get_all_confirmed():
    user_id = session['user_id']
    db = get_db()
    
    query = """
        SELECT pm.id as binding_id, pm.group_id, p.id as product_id, p.name as product_name, p.sort_index as product_sort_index, pm.extracted_price, m.text, 
               m.chat_id, m.chat_title, m.sender_name, m.date, pm.line_index, pm.message_id, pm.is_actual,
               m.telegram_message_id, m.type, ec.sheet_url, tc.chat_title as tc_chat_title, tc.custom_name
        FROM product_messages pm
        JOIN products p ON pm.product_id = p.id
        JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        LEFT JOIN excel_configs ec ON m.chat_id = ec.chat_id AND m.type LIKE 'excel%'
        WHERE p.user_id = ?
        AND pm.status = 'confirmed'
    """
    rows = db.execute(query, (user_id,)).fetchall()
    
    # Собираем актуальные версии строк (с добавленными telegram_message_id и type)
    all_msgs = db.execute("SELECT id, text, chat_id, sender_name, date, telegram_message_id, type FROM messages WHERE user_id = ? AND (is_blocked IS NULL OR is_blocked = 0) AND (is_delayed IS NULL OR is_delayed = 0) ORDER BY date DESC", (user_id,)).fetchall()
    latest_fingerprints = {}
    for row in all_msgs:
        if not row['text']: continue
        lines = row['text'].split('\n')
        for i, line in enumerate(lines):
            if not line.strip(): continue
            fp = get_fingerprint(row['chat_id'], row['sender_name'], line)
            if fp not in latest_fingerprints:
                price_val = extract_price(line)
                if not price_val and i + 1 < len(lines):
                    price_val = extract_price(lines[i+1])
                if not price_val and i + 2 < len(lines):
                    price_val = extract_price(lines[i+2])

                latest_fingerprints[fp] = {
                    'message_id': row['id'],
                    'line_index': i,
                    'text': line,
                    'date': row['date'],
                    'price': price_val,
                    'telegram_message_id': row['telegram_message_id'],
                    'type': row['type']
                }


    result_raw = []
    has_updates = False 
    for r in rows:
        d = dict(r)
        if d['line_index'] != -1 and d['text']:
            lines = d['text'].split('\n')
            if 0 <= d['line_index'] < len(lines):
                orig_line = lines[d['line_index']]
                fp = get_fingerprint(d['chat_id'], d['sender_name'], orig_line)
                
                latest = latest_fingerprints.get(fp)
                if latest and latest['date'] > d['date']:
                    d['text'] = latest['text']
                    d['extracted_price'] = latest['price']
                    d['date'] = latest['date']
                    d['telegram_message_id'] = latest['telegram_message_id']
                    d['type'] = latest['type']
                    
                    current_id = d.get('pm_id') or d.get('id')

                    try:
                        db.execute("UPDATE product_messages SET message_id=?, line_index=?, extracted_price=? WHERE id=?",
                                   (latest['message_id'], latest['line_index'], latest['price'], current_id))
                        has_updates = True
                    except sqlite3.IntegrityError:
                        if current_id:
                            db.execute("DELETE FROM product_messages WHERE id=?", (current_id,))
                            has_updates = True
                else:
                    d['text'] = orig_line
        result_raw.append(d)
        
    if has_updates:
        db.commit()
        notify_clients()

    grouped_reports = {}
    for d in result_raw:
        g_id = d['group_id'] if d['group_id'] else d['binding_id']
        if g_id not in grouped_reports or d['date'] > grouped_reports[g_id]['date']:
            grouped_reports[g_id] = d
            
    final_result = list(grouped_reports.values())
    final_result.sort(key=lambda x: x['date'], reverse=True)
    return jsonify(final_result)


# --- Маршруты для Взаимодействия с ботами ---
@app.route('/api/interaction_bots', methods=['GET', 'POST'])
@login_required
def manage_interaction_bots():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        bots = db.execute("SELECT * FROM interaction_bots WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(b) for b in bots])
    
    # POST
    data = request.get_json()
    bot_username = data.get('bot_username').strip()
    custom_name = data.get('custom_name')
    userbot_id = data.get('userbot_id')
    
    # 1. Находим запущенного юзербота (он нужен, чтобы узнать числовой ID бота)
    client_to_use = None
    client_loop_to_use = None
    
    if userbot_id in user_clients:
        _, client_to_use, _ = user_clients[userbot_id]
        client_loop_to_use = user_loops.get(userbot_id)
    else:
        active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
        if active_sessions:
            sid = active_sessions[0]
            _, client_to_use, _ = user_clients[sid]
            client_loop_to_use = user_loops.get(sid)

    # 2. Превращаем текстовый @username в ЧИСЛОВОЙ ID для парсера
    numeric_chat_id = None
    if client_to_use and client_loop_to_use:
        async def get_bot_id():
            try:
                entity = await client_to_use.get_entity(bot_username)
                return entity.id
            except Exception as e:
                logging.error(f"Не удалось получить ID для {bot_username}: {e}")
                return None
        
        try:
            future = asyncio.run_coroutine_threadsafe(get_bot_id(), client_loop_to_use)
            numeric_chat_id = future.result(timeout=10)
        except Exception:
            pass

    # Если юзербот выключен, мы не сможем получить ID — выдаем ошибку
    if not numeric_chat_id:
        return jsonify({'error': 'Не удалось определить ID бота. Убедитесь, что юзербот включен и юзернейм указан верно (например, @MyBot).'}), 400

    # 3. АВТОМАТИЧЕСКОЕ ДОБАВЛЕНИЕ В ПАРСЕР (здесь используем ЧИСЛОВОЙ ID!)
    tracked_chat_id = None
    try:
        db.execute("INSERT INTO tracked_chats (user_id, chat_id, chat_title, custom_name) VALUES (?, ?, ?, ?)",
                   (user_id, numeric_chat_id, bot_username, custom_name))
    except sqlite3.IntegrityError:
        pass # Уже добавлен
    tracked_row = db.execute(
        "SELECT id FROM tracked_chats WHERE user_id=? AND chat_id=?",
        (user_id, numeric_chat_id)
    ).fetchone()
    if tracked_row:
        tracked_chat_id = tracked_row['id']

    # 4. Сохраняем автоматизацию и прямую связь с записью в отслеживаемых чатах.
    db.execute("""INSERT INTO interaction_bots 
                  (user_id, userbot_id, bot_username, custom_name, commands, interval_minutes, status, tracked_chat_id, resolved_chat_id) 
                  VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?)""",
               (user_id, userbot_id, bot_username, custom_name,
                json.dumps(data.get('commands', [])), data.get('interval_minutes', 60), tracked_chat_id, numeric_chat_id))

    db.commit()
    notify_clients()

    # 5. ПАРСИНГ ИСТОРИИ (также по числовому ID)
    # 5. ПАРСИНГ ИСТОРИИ 
    chat_title_for_parser = custom_name if custom_name else bot_username

    return jsonify({'success': True})

@app.route('/api/interaction_bots/<int:id>', methods=['DELETE'])
@login_required
def delete_interaction_bot(id):
    db = get_db()
    user_id = session['user_id']
    
    bot = db.execute("SELECT * FROM interaction_bots WHERE id=? AND user_id=?", (id, user_id)).fetchone()
    if bot:
        tracked_ids = get_interaction_bot_tracked_chat_ids(db, user_id, [bot])
        delete_tracked_chats_by_ids(db, user_id, tracked_ids)
            
    db.execute("DELETE FROM interaction_bots WHERE id=? AND user_id=?", (id, user_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()

def init_db():
    if not os.path.exists('sessions'):
        os.makedirs('sessions')
    with app.app_context():
        db = get_db()
        db.execute('''
        CREATE TABLE IF NOT EXISTS pub_markups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    pub_id INTEGER NOT NULL,
                    folder_id INTEGER DEFAULT 0,
                    markup_type TEXT DEFAULT 'percent',
                    markup_value REAL DEFAULT 0,
                    rounding INTEGER DEFAULT 100,
                    FOREIGN KEY(pub_id) REFERENCES publications(id) ON DELETE CASCADE,
                    UNIQUE(pub_id, folder_id)
                )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS publications (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            is_active INTEGER DEFAULT 0,
            interval_min INTEGER DEFAULT 60,
            chat_id TEXT,
            message_id TEXT,
            userbot_id INTEGER,
            template TEXT,
            allowed_items TEXT -- Здесь будет храниться JSON с выбранными галочками (как в API)
        )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS api_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT,
            url TEXT,
            token TEXT,
            interval_min INTEGER DEFAULT 60,
            is_active INTEGER DEFAULT 1
        )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS ai_autofill_cache (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cache_key TEXT UNIQUE NOT NULL,
            user_id INTEGER DEFAULT 0,
            product_name TEXT NOT NULL,
            article TEXT DEFAULT '',
            payload TEXT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
        db.execute('''
        CREATE TABLE IF NOT EXISTS ai_repair_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            product_name TEXT NOT NULL,
            article TEXT DEFAULT '',
            preferred_folder_id INTEGER,
            status TEXT DEFAULT 'pending',
            attempts INTEGER DEFAULT 0,
            next_retry_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_error TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE
        )
    ''')
        db.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                login TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                role TEXT DEFAULT 'user'
            );
            CREATE TABLE IF NOT EXISTS user_telegram_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                api_id TEXT,
                api_hash TEXT,
                session_file TEXT,
                status TEXT DEFAULT 'inactive',
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                telegram_message_id INTEGER,
                type TEXT,
                text TEXT,
                date TIMESTAMP,
                chat_id INTEGER,
                chat_title TEXT,
                is_blocked INTEGER DEFAULT 0,
                sender_name TEXT,
                is_delayed INTEGER DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, telegram_message_id)
            );
            CREATE TABLE IF NOT EXISTS folders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                name TEXT NOT NULL,
                parent_id INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(parent_id) REFERENCES folders(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS saved_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id INTEGER,
                folder_id INTEGER,
                saved_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                FOREIGN KEY(folder_id) REFERENCES folders(id) ON DELETE CASCADE,
                UNIQUE(message_id, folder_id)
            );
            CREATE TABLE IF NOT EXISTS tracked_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                chat_title TEXT,
                added_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, chat_id)
            );
            CREATE TABLE IF NOT EXISTS products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                synonyms TEXT,
                price REAL,
                sort_index INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
            );
            CREATE TABLE IF NOT EXISTS product_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                product_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                line_index INTEGER DEFAULT -1,
                group_id INTEGER,
                status TEXT DEFAULT 'proposed',
                extracted_price REAL,
                is_actual INTEGER DEFAULT 1,
                FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
                FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                UNIQUE(product_id, message_id, line_index)
            );
            
            CREATE TABLE IF NOT EXISTS interaction_bots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                userbot_id INTEGER NOT NULL,
                bot_username TEXT NOT NULL,
                commands TEXT NOT NULL,
                interval_minutes INTEGER NOT NULL DEFAULT 60,
                last_run TIMESTAMP,
                status TEXT DEFAULT 'active',
                tracked_chat_id INTEGER,
                resolved_chat_id INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                FOREIGN KEY(userbot_id) REFERENCES user_telegram_sessions(id) ON DELETE CASCADE
            );
        ''')
        # Добавляем новые колонки в существующие таблицы (игнорируем ошибку, если они уже есть)
        # 1. Безопасное добавление колонок
        columns_to_add = [
            ("products", "photo_url TEXT DEFAULT NULL"),
            ("products", "source_url TEXT DEFAULT NULL"),
            ("products", "brand TEXT DEFAULT NULL"),
            ("products", "country TEXT DEFAULT NULL"),
            ("products", "weight TEXT DEFAULT NULL"),
            ("products", "model_number TEXT DEFAULT NULL"),
            ("products", "is_on_request INTEGER DEFAULT 0"),
            ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
           ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
            ("products", "description TEXT DEFAULT NULL"),
            ("products", "description_html TEXT DEFAULT NULL"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("products", "sort_index INTEGER"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("products", "color TEXT DEFAULT NULL"),
            ("products", "storage TEXT DEFAULT NULL"),
            ("products", "ram TEXT DEFAULT NULL"),
            ("products", "warranty TEXT DEFAULT NULL"),
            ("products", "description TEXT DEFAULT NULL"),
            ("products", "description_html TEXT DEFAULT NULL"),
            ("products", "specs TEXT DEFAULT NULL"),
            ("product_messages", "line_index INTEGER DEFAULT -1"),
            ("product_messages", "group_id INTEGER"),
            ("product_messages", "line_index INTEGER DEFAULT -1"),
            ("product_messages", "group_id INTEGER"),
            ("product_messages", "is_actual INTEGER DEFAULT 1"), 
            ("messages", "is_blocked INTEGER DEFAULT 0"),
            ("messages", "is_delayed INTEGER DEFAULT 0"),
            ("tracked_chats", "custom_name TEXT"),
            ("interaction_bots", "custom_name TEXT"),
            ("interaction_bots", "tracked_chat_id INTEGER"),
            ("interaction_bots", "resolved_chat_id INTEGER"),
            ("messages", "sender_name TEXT"),
            ("products", "folder_id INTEGER"),
            ("user_telegram_sessions", "account_name TEXT"),
            ("user_telegram_sessions", "time_start TEXT DEFAULT '00:00'"), 
            ("user_telegram_sessions", "time_end TEXT DEFAULT '23:59'"),   
            ("user_telegram_sessions", "schedule_enabled INTEGER DEFAULT 0"),
            # --- НОВЫЕ КОЛОНКИ ДЛЯ API КЛИЕНТОВ ---
            ("api_clients", "time_start TEXT DEFAULT '00:00'"),
            ("api_clients", "time_end TEXT DEFAULT '23:59'"),
            ("api_clients", "schedule_enabled INTEGER DEFAULT 0"),
            ("api_clients", "access_rules TEXT"),
            ("api_clients", "allowed_folders TEXT DEFAULT 'all'"), # <--- ДОБАВИТЬ ЭТО
            ("api_clients", "allowed_chats TEXT DEFAULT 'all'"),
            ("api_clients", "publish_chat_id INTEGER"),
            ("api_clients", "publish_message_id INTEGER"),
            ("api_clients", "publish_enabled INTEGER DEFAULT 0"),
            ("api_clients", "userbot_id INTEGER"),
            ("api_clients", "publish_template TEXT"),
            ("publications", "user_id INTEGER"),
            ("api_clients", "folders TEXT"),
            ("api_clients", "publish_interval INTEGER DEFAULT 60"),
            ("users", "session_token TEXT"),
            ("excel_configs", "is_grouped INTEGER DEFAULT 0"),
            ("publications", "markup_type TEXT DEFAULT 'percent'"), # <--- НОВОЕ
            ("publications", "markup_value REAL DEFAULT 0"),        # <--- НОВОЕ
            ("publications", "rounding INTEGER DEFAULT 100"),
            ("excel_configs", "sku_col INTEGER DEFAULT -1"),
            ("excel_configs", "source_type TEXT DEFAULT 'file'"),       # 'file' или 'google_sheet'
            ("excel_configs", "sheet_url TEXT"),                        # Ссылка на таблицу
            ("excel_configs", "parse_interval INTEGER DEFAULT 60")
        ]
        for table, col in columns_to_add:
            try:
                db.execute(f"ALTER TABLE {table} ADD COLUMN {col};")
            except sqlite3.OperationalError:
                pass # Если колонка уже есть, просто идем к следующей

        try:
            db.execute("UPDATE products SET sort_index = id WHERE sort_index IS NULL")
        except sqlite3.OperationalError:
            pass

        # 2. Обновление таблицы привязок (чтобы разные строки одного сообщения не затирали друг друга)
        # 2. Обновление таблицы привязок

        excel_configs_sql = get_table_sql(db, 'excel_configs')
        if excel_configs_sql and 'sheet_name TEXT NOT NULL' not in excel_configs_sql:
            db.execute("PRAGMA foreign_keys = OFF")
            db.executescript('''
                CREATE TABLE IF NOT EXISTS excel_configs_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    sheet_name TEXT NOT NULL,
                    name_col INTEGER NOT NULL,
                    name_row_offset INTEGER NOT NULL,
                    price_col INTEGER NOT NULL,
                    price_row_offset INTEGER NOT NULL,
                    block_step INTEGER NOT NULL,
                    start_row INTEGER NOT NULL,
                    is_grouped INTEGER DEFAULT 0,
                    sku_col INTEGER DEFAULT -1,
                    source_type TEXT DEFAULT 'excel',
                    sheet_url TEXT,
                    parse_interval INTEGER DEFAULT 60,
                    folder_id INTEGER,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(chat_id, sheet_name)
                );
                INSERT OR IGNORE INTO excel_configs_new (
                    id, user_id, chat_id, sheet_name, name_col, name_row_offset,
                    price_col, price_row_offset, block_step, start_row,
                    is_grouped, sku_col, source_type, sheet_url, parse_interval, folder_id
                )
                SELECT
                    id, user_id, chat_id, 'Лист1', name_col, name_row_offset,
                    price_col, price_row_offset, block_step, start_row,
                    0, -1, 'excel', NULL, 60, NULL
                FROM excel_configs;
                DROP TABLE excel_configs;
                ALTER TABLE excel_configs_new RENAME TO excel_configs;
            ''')
            db.execute("PRAGMA foreign_keys = ON")



        try:
            # --- ТАБЛИЦА ДЛЯ НАСТРОЕК EXCEL ---
            db.executescript('''
                CREATE TABLE IF NOT EXISTS excel_missing_sheets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                chat_id INTEGER,
                chat_title TEXT,
                sheet_name TEXT,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, sheet_name)
            );
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS excel_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    chat_id INTEGER NOT NULL,
                    sheet_name TEXT NOT NULL DEFAULT '*',
                    name_col INTEGER NOT NULL,
                    name_row_offset INTEGER NOT NULL,
                    price_col INTEGER NOT NULL,
                    price_row_offset INTEGER NOT NULL,
                    block_step INTEGER NOT NULL,
                    start_row INTEGER NOT NULL,
                    is_grouped INTEGER DEFAULT 0,
                    sku_col INTEGER DEFAULT -1,
                    source_type TEXT DEFAULT 'excel',
                    sheet_url TEXT,
                    parse_interval INTEGER DEFAULT 60,
                    folder_id INTEGER,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                    UNIQUE(chat_id, sheet_name)
                );
            ''')
            # --- ТАБЛИЦЫ ДЛЯ API И НАЦЕНОК ---
            db.executescript('''
                CREATE TABLE IF NOT EXISTS api_clients (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    token TEXT UNIQUE NOT NULL,
                    FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE
                );
                CREATE TABLE IF NOT EXISTS api_markups (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    client_id INTEGER NOT NULL,
                    folder_id INTEGER DEFAULT 0, -- 0 означает "Для всех остальных папок"
                    markup_type TEXT DEFAULT 'percent', -- 'percent' или 'fixed'
                    markup_value REAL DEFAULT 0,
                    rounding INTEGER DEFAULT 100, -- 100, 500, 1000 и тд.
                    FOREIGN KEY(client_id) REFERENCES api_clients(id) ON DELETE CASCADE,
                    UNIQUE(client_id, folder_id)
                );
            ''')
            db.executescript('''
                CREATE TABLE IF NOT EXISTS pm_new (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id INTEGER NOT NULL,
                    message_id INTEGER NOT NULL,
                    line_index INTEGER DEFAULT -1,
                    group_id INTEGER,
                    status TEXT DEFAULT 'proposed',
                    extracted_price REAL,
                    is_actual INTEGER DEFAULT 1, -- <--- ДОБАВЛЕНА ЭТА СТРОКА
                    FOREIGN KEY(product_id) REFERENCES products(id) ON DELETE CASCADE,
                    FOREIGN KEY(message_id) REFERENCES messages(id) ON DELETE CASCADE,
                    UNIQUE(product_id, message_id, line_index)
                );
                -- Переносим старые данные (добавляем 1 по умолчанию для is_actual)
                INSERT OR IGNORE INTO pm_new (id, product_id, message_id, line_index, group_id, status, extracted_price, is_actual) 
                SELECT id, product_id, message_id, line_index, group_id, status, extracted_price, 1 FROM product_messages;
                
                -- Заменяем старую таблицу на новую
                DROP TABLE IF EXISTS product_messages;
                ALTER TABLE pm_new RENAME TO product_messages;
            ''')
        except Exception as e:
            pass


        messages_sql = get_table_sql(db, 'messages')
        if messages_sql and 'UNIQUE(chat_id, telegram_message_id)' not in messages_sql:
            try:
                db.execute("PRAGMA foreign_keys = OFF")
                db.executescript('''
                    CREATE TABLE IF NOT EXISTS messages_new (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        user_id INTEGER,
                        telegram_message_id INTEGER,
                        type TEXT,
                        text TEXT,
                        date TIMESTAMP,
                        chat_id INTEGER,
                        chat_title TEXT,
                        is_blocked INTEGER DEFAULT 0,
                        sender_name TEXT,
                        is_delayed INTEGER DEFAULT 0,
                        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                        UNIQUE(chat_id, telegram_message_id)
                    );
                    INSERT OR IGNORE INTO messages_new (
                        id, user_id, telegram_message_id, type, text, date,
                        chat_id, chat_title, is_blocked, sender_name, is_delayed
                    )
                    SELECT
                        id, user_id, telegram_message_id, type, text, date,
                        chat_id, chat_title, is_blocked, sender_name,
                        0
                    FROM messages;
                    DROP TABLE messages;
                    ALTER TABLE messages_new RENAME TO messages;
                ''')
                db.execute("PRAGMA foreign_keys = ON")
            except Exception as e:
                logger.error(f"Ошибка миграции сообщений: {e}")

        performance_indexes = (
            "CREATE INDEX IF NOT EXISTS idx_products_user_sort ON products(user_id, sort_index, id)",
            "CREATE INDEX IF NOT EXISTS idx_products_user_folder_sort ON products(user_id, folder_id, sort_index, id)",
            "CREATE INDEX IF NOT EXISTS idx_products_user_brand ON products(user_id, brand)",
            "CREATE INDEX IF NOT EXISTS idx_products_user_brand_nocase ON products(user_id, brand COLLATE NOCASE, sort_index, id)",
            "CREATE INDEX IF NOT EXISTS idx_product_messages_product_status ON product_messages(product_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_product_messages_status_product ON product_messages(status, product_id)",
            "CREATE INDEX IF NOT EXISTS idx_product_messages_message ON product_messages(message_id)",
            "CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id, id)",
            "CREATE INDEX IF NOT EXISTS idx_folders_user_parent ON folders(user_id, parent_id, id)",
        )
        for index_sql in performance_indexes:
            try:
                db.execute(index_sql)
            except sqlite3.OperationalError as e:
                logger.warning(f"Не удалось создать индекс производительности: {e}")

        # 3. Создание учетки администратора по умолчанию
        cursor = db.execute("SELECT COUNT(*) FROM users")
        if cursor.fetchone()[0] == 0:
            pwd_hash = hashlib.sha256('admin'.encode()).hexdigest()
            db.execute("INSERT INTO users (login, password_hash, role) VALUES (?, ?, ?)",
                       ('admin', pwd_hash, 'admin'))
            db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (1, 'По умолчанию', NULL)")
            
        db.commit()
        notify_clients()

init_db()


@app.route('/api/products/<int:prod_id>/link_folder', methods=['POST'])
@login_required
def link_product_folder(prod_id):
    data = request.get_json()
    folder_id = data.get('folder_id') # Может быть None (отвязка)
    db = get_db()
    db.execute("UPDATE products SET folder_id=? WHERE id=? AND user_id=?", (folder_id, prod_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/products/<int:prod_id>', methods=['PUT'])
@login_required
def edit_product(prod_id):
    db = get_db()
    
    # 1. Синонимы (из FormData в JSON)
    try:
        syns = json.loads(request.form.get('synonyms', '[]'))
    except:
        syns = []
    synonyms_str = ", ".join([s.strip() for s in syns if s.strip()])
    resolved_folder_id = resolve_product_folder_assignment(
        db,
        session['user_id'],
        request.form.get('name'),
        request.form.get('folder_id'),
        request.form.get('storage'),
    )

    # 2. Существующие фото, которые не были удалены
    try:
        existing_photos = json.loads(request.form.get('existing_photos', '[]'))
    except:
        existing_photos = []
    try:
        photo_order = json.loads(request.form.get('photo_order', '[]'))
    except:
        photo_order = []

    # 3. Сохраняем новые фото, если они были добавлены
    files = request.files.getlist('photos')
    new_photo_tokens = request.form.getlist('new_photo_tokens')
    saved_names = []
    saved_by_token = {}
    for index, file in enumerate(files):
        if file and file.filename:
            ext = file.filename.rsplit('.', 1)[1].lower() if '.' in file.filename else 'jpg'
            filename = f"{uuid.uuid4().hex}.{ext}"
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            saved_names.append(filename)
            if index < len(new_photo_tokens):
                saved_by_token[new_photo_tokens[index]] = filename

    # 4. Собираем финальный порядок фото
    all_photos = []
    used_existing = set()
    used_new = set()
    if isinstance(photo_order, list) and photo_order:
        for item in photo_order:
            if not isinstance(item, dict):
                continue
            item_kind = item.get('kind')
            item_value = item.get('value')
            if item_kind == 'existing' and item_value in existing_photos and item_value not in used_existing:
                all_photos.append(item_value)
                used_existing.add(item_value)
            elif item_kind == 'new' and item_value in saved_by_token and item_value not in used_new:
                all_photos.append(saved_by_token[item_value])
                used_new.add(item_value)

    for photo_name in existing_photos:
        if photo_name not in used_existing:
            all_photos.append(photo_name)
    for token, photo_name in saved_by_token.items():
        if token not in used_new:
            all_photos.append(photo_name)
    if not all_photos:
        all_photos = existing_photos + saved_names

    photo_url_str = ",".join(all_photos)

    specs_payload = parse_specs_payload(
        request.form.get('specs'),
        {
            'brand': request.form.get('brand'),
            'country': request.form.get('country'),
            'weight': request.form.get('weight'),
            'model_number': request.form.get('model_number'),
            'color': request.form.get('color'),
            'storage': request.form.get('storage'),
            'ram': request.form.get('ram'),
            'warranty': request.form.get('warranty'),
        },
    )
    specs_json = json.dumps(specs_payload, ensure_ascii=False) if specs_payload else ""

    # 5. Обновляем товар в базе
    db.execute("""
        UPDATE products 
        SET name=?, synonyms=?, photo_url=?, brand=?, country=?, weight=?, model_number=?, is_on_request=?, folder_id=?,
            color=?, storage=?, ram=?, warranty=?, description=?, description_html=?, specs=?
        WHERE id=? AND user_id=?
    """, (
        request.form.get('name'), 
        synonyms_str, 
        photo_url_str, 
        request.form.get('brand'),
        request.form.get('country'),
        request.form.get('weight'),
        request.form.get('model_number'),
        1 if request.form.get('is_on_request') == '1' else 0,
        resolved_folder_id, 
        
        # 🔽 ДОБАВЛЯЕМ ПРИЕМ ПОЛЕЙ
        request.form.get('color'),
        request.form.get('storage'),
        request.form.get('ram'),
        request.form.get('warranty'),
        request.form.get('description'),
        request.form.get('description_html'),
        specs_json,

        prod_id, 
        session['user_id']
    ))
    db.commit()
    notify_clients()
    return jsonify({'success': True})





@app.route('/api/access_tree')
@login_required
def get_access_tree():
    user_id = session['user_id']
    cached_tree = ACCESS_TREE_CACHE.get(int(user_id))
    if cached_tree is not None:
        return jsonify(cached_tree)

    db = get_db()
    folders = [dict(f) for f in db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()]
    
    # Гениальный запрос: вытаскиваем товары сразу с их подтвержденными поставщиками
    prods_raw = db.execute("""
        SELECT p.id as product_id, p.name as product_name, p.folder_id,
               m.chat_id, tc.custom_name, tc.chat_title
        FROM products p
        LEFT JOIN product_messages pm ON p.id = pm.product_id AND pm.status = 'confirmed'
        LEFT JOIN messages m ON pm.message_id = m.id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE p.user_id = ?
        GROUP BY p.id, m.chat_id
        ORDER BY CASE WHEN p.sort_index IS NULL THEN 1 ELSE 0 END, p.sort_index, p.id
    """, (user_id,)).fetchall()
    
    products = {}
    for row in prods_raw:
        pid = row['product_id']
        if pid not in products:
            products[pid] = {'id': pid, 'name': row['product_name'], 'folder_id': row['folder_id'], 'chats': []}
        if row['chat_id']:
            c_name = row['custom_name'] if row['custom_name'] else (row['chat_title'] or 'Неизвестный поставщик')
            if not any(c['chat_id'] == row['chat_id'] for c in products[pid]['chats']):
                products[pid]['chats'].append({'chat_id': row['chat_id'], 'name': c_name})

    payload = {'folders': folders, 'products': list(products.values())}
    ACCESS_TREE_CACHE[int(user_id)] = payload
    return jsonify(payload)



def fetch_google_sheet_as_df(sheet_url):
    """
    Превращает обычную ссылку на Google Таблицу в прямую ссылку на скачивание XLSX
    и сразу читает её через pandas.
    Пример: https://docs.google.com/spreadsheets/d/1ABC123.../edit#gid=0
    """
    try:
        if "/edit" in sheet_url:
            # Заменяем концовку ссылки на команду экспорта
            export_url = sheet_url.split('/edit')[0] + '/export?format=xlsx'
        else:
            export_url = sheet_url
            
        # Читаем напрямую из интернета в DataFrame
        df = pd.read_excel(export_url)
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке Google Таблицы: {e}")
        return None

@app.route('/api/excel/google_sheet/preview', methods=['POST'])
@login_required
def preview_google_sheet_route():
    data = request.json
    sheet_url = data.get('sheet_url')
    try:
        import requests
        from io import BytesIO
        from openpyxl import load_workbook

        if "/edit" in sheet_url:
            export_url = sheet_url.split('/edit')[0] + '/export?format=xlsx'
        else:
            export_url = sheet_url
            
        resp = requests.get(export_url, timeout=30)
        if resp.status_code != 200:
            return jsonify({'error': 'Не удалось скачать таблицу'}), 400

        # Загружаем книгу ПРАВИЛЬНО (не в режиме read_only)
        xls_file = BytesIO(resp.content)
        wb = load_workbook(xls_file, data_only=True) # data_only=True читает значения, а не формулы
        
        sheets_data = {}
        # Оставляем только видимые листы
        visible_sheets = [s.title for s in wb.worksheets if s.sheet_state == 'visible']
        
        for sn in visible_sheets:
            ws = wb[sn]
            rows_list = []
            count = 0
            
            # Итерируемся по строкам напрямую через openpyxl
            for row in ws.iter_rows(values_only=True):
                # Проверяем, не скрыта ли строка
                row_idx = count + 1
                if row_idx in ws.row_dimensions and ws.row_dimensions[row_idx].hidden:
                    count += 1
                    continue
                
                # Очищаем данные от None
                clean_row = [str(cell) if cell is not None else "" for cell in row]
                rows_list.append(clean_row)
                count += 1
                
                # Ограничиваем превью 15 видимыми строками
                if len(rows_list) >= 15:
                    break
            
            sheets_data[sn] = rows_list
            
        return jsonify({'success': True, 'sheets_data': sheets_data})
    except Exception as e:
        logger.error(f"GS Preview Error: {e}")
        return jsonify({'error': str(e)}), 500



import pdfplumber

@app.route('/api/pdf/preview', methods=['POST'])
@login_required
def preview_pdf_route():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    
    file = request.files['file']
    try:
        with pdfplumber.open(file) as pdf:
            if not pdf.pages:
                return jsonify({'error': 'PDF пустой'}), 400
            
            # Извлекаем таблицу с первой страницы
            table = pdf.pages[0].extract_table()
            if not table:
                return jsonify({'error': 'Не удалось найти чёткую таблицу в PDF (возможно, нет линий сетки).'}), 400
            
            # Очищаем таблицу от пустых значений (None) и переносов строк
            cleaned_table = []
            for row in table:
                cleaned_table.append([str(cell).replace('\n', ' ') if cell is not None else "" for cell in row])
                
        # Возвращаем в формате "1 лист" (т.к. у PDF нет листов как в Excel)
        return jsonify({'success': True, 'sheets_data': {'Страница 1': cleaned_table[:30]}}) # берем первые 30 строк для превью
    except Exception as e:
        logger.error(f"PDF Preview Error: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/pdf/save_config', methods=['POST'])
@login_required
def save_pdf_config():
    data = request.json
    db = get_db()
    try:
        # Сохраняем в ту же таблицу, но с меткой source_type = 'pdf'
        db.execute("""
            INSERT INTO excel_configs 
            (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col, source_type, parse_interval) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pdf', ?)
            ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
            name_col=excluded.name_col, price_col=excluded.price_col, 
            block_step=excluded.block_step, start_row=excluded.start_row,
            is_grouped=excluded.is_grouped, sku_col=excluded.sku_col, source_type='pdf'
        """, (
            session['user_id'], data['chat_id'], data['sheet_name'], 
            data['name_col'], 0, data['price_col'], data.get('price_row_offset', 0), 
            data.get('block_step', 1), data['start_row'], data.get('is_grouped', 0), 
            data.get('sku_col', -1), data.get('parse_interval', 60)
        ))
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel/google_sheet/save_config', methods=['POST'])
@login_required
def save_gs_config():
    data = request.json
    chat_id = data.get('chat_id')
    if not chat_id:
        return jsonify({'error': 'Ошибка: Выбор чата поставщика обязателен!'}), 400
    db = get_db()
    try:
        db.execute("""
            INSERT INTO excel_configs 
            (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col, source_type, sheet_url, parse_interval) 
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'google_sheet', ?, ?)
            ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
            name_col=excluded.name_col, name_row_offset=excluded.name_row_offset, price_col=excluded.price_col, 
            price_row_offset=excluded.price_row_offset, block_step=excluded.block_step, start_row=excluded.start_row,
            is_grouped=excluded.is_grouped, sku_col=excluded.sku_col, source_type=excluded.source_type, 
            sheet_url=excluded.sheet_url, parse_interval=excluded.parse_interval
        """, (
            session['user_id'], data['chat_id'], data['sheet_name'], 
            data['name_col'], 0, data['price_col'], data.get('price_row_offset', 0), 
            data.get('block_step', 1), data['start_row'], data.get('is_grouped', 0), 
            data.get('sku_col', -1), data['sheet_url'], data['parse_interval']
        ))
        
        # Удаляем из missing_sheets (убираем плашку "Не настроен лист")
        cid_str = str(data['chat_id'])
        core_id = cid_str[4:] if cid_str.startswith('-100') else (cid_str[1:] if cid_str.startswith('-') else cid_str)
        possible_ids = [core_id, f"-{core_id}", f"-100{core_id}"]
        for pid in possible_ids:
            if data['sheet_name'] == '*':
                db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND user_id = ?", (pid, session['user_id']))
            else:
                db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND sheet_name = ? AND user_id = ?", (pid, data['sheet_name'], session['user_id']))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка сохранения конфига Google Sheet: {e}")
        return jsonify({'error': str(e)}), 500


def format_price_list(template, products):
    """Вставляет товары в шаблон между маркерами START_PRICE_LIST и END_PRICE_LIST."""
    lines = []
    for p in products:
        lines.append(f"• {p['name']} — {p['price']} руб.")
    price_list = "\n".join(lines)
    
    if "<!-- START_PRICE_LIST -->" in template and "<!-- END_PRICE_LIST -->" in template:
        # Заменяем только часть между маркерами
        parts = template.split("<!-- START_PRICE_LIST -->")
        before = parts[0]
        after = parts[1].split("<!-- END_PRICE_LIST -->")[1]
        return before + "<!-- START_PRICE_LIST -->\n" + price_list + "\n<!-- END_PRICE_LIST -->" + after
    else:
        # Иначе заменяем всё сообщение
        return price_list

# Словарь в памяти для отслеживания времени последней публикации
# Словарь в памяти для отслеживания времени последней публикации
last_publish_times = {}

def publish_scheduler():
    """Фоновый поток для публикации каталогов в Telegram."""
    time.sleep(10)
    with app.app_context():
        while True:
            try:
                db = get_db()
                # Берем все активные публикации
                pubs = db.execute("SELECT * FROM publications WHERE is_active = 1").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for pub in pubs:
                    pub_id = pub['id']
                    interval_minutes = pub['interval_min'] or 60
                    
                    # 1. ПРОВЕРКА ИНТЕРВАЛА
                    last_run = last_publish_times.get(pub_id)
                    if last_run:
                        diff_minutes = (now - last_run).total_seconds() / 60
                        if diff_minutes < interval_minutes:
                            continue  # Время публикации еще не пришло
                    
                    # 2. ПОЛУЧЕНИЕ ЦЕН И ТОВАРОВ
                    userbot_id = pub['userbot_id']
                    if userbot_id not in user_clients: continue
                    
                    # Извлекаем user_id из юзербота
                    _, tg_client, user_id = user_clients[userbot_id] 
                    
                    allowed_items = {}
                    try: allowed_items = json.loads(pub['allowed_items']) 
                    except: pass
                    
                    if not allowed_items: continue
                    
                    # ---- НОВОЕ: ДОСТАЕМ НАЦЕНКИ ----
                    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM pub_markups WHERE pub_id=?", (pub_id,)).fetchall()
                    markups = {}
                    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
                    for m in markups_raw:
                        if m['folder_id'] == 0: default_markup = dict(m)
                        else: markups[m['folder_id']] = dict(m)
                    # --------------------------------
                    
                    final_products = []
                    
                    # Перебираем товары и разрешенных поставщиков из JSON
                    # Перебираем товары и разрешенных поставщиков из JSON
                    for p_id_str, allowed_suppliers in allowed_items.items():
                        if not allowed_suppliers: 
                            continue
                        
                        # --- НОВАЯ ЛОГИКА ДЛЯ "ЦЕНА ПО ЗАПРОСУ" ---
                        if '0' in allowed_suppliers or 0 in allowed_suppliers:
                            # У товара нет цен, достаем из базы только его название
                            p_info = db.execute("SELECT name FROM products WHERE id = ?", (int(p_id_str),)).fetchone()
                            if p_info:
                                final_products.append({
                                    'name': p_info['name'], 
                                    'price': 'Цена по запросу'
                                })
                            continue  # Пропускаем запросы цен, идем к следующему товару
                        # -----------------------------------------

                        placeholders = ','.join(['?'] * len(allowed_suppliers))
                        query_params = [int(p_id_str)] + allowed_suppliers
                        
                        # ВАЖНО: Добавлено p.folder_id в SELECT
                        price_row = db.execute(f"""
                            SELECT pm.extracted_price, p.name, p.folder_id 
                            FROM product_messages pm 
                            JOIN messages m ON pm.message_id = m.id 
                            JOIN products p ON p.id = pm.product_id
                            WHERE pm.product_id = ? 
                              AND pm.status = 'confirmed' 
                              AND pm.is_actual = 1 
                              AND m.chat_id IN ({placeholders})
                            ORDER BY m.date DESC LIMIT 1
                        """, query_params).fetchone()
                        
                        if price_row and price_row['extracted_price'] is not None:
                            # -------- ПРИМЕНЯЕМ НАЦЕНКУ ПО ПАПКАМ ---------
                            base_price = float(price_row['extracted_price'])
                            f_id = price_row['folder_id'] or 0
                            mk = markups.get(f_id, default_markup)
                            
                            if mk['markup_type'] == 'percent':
                                final_price = base_price * (1 + mk['markup_value'] / 100)
                            else:
                                final_price = base_price + mk['markup_value']
                                
                            rnd = mk['rounding']
                            if rnd > 0:
                                final_price = math.ceil(final_price / rnd) * rnd

                            final_products.append({
                                'name': price_row['name'], 
                                'price': int(final_price)
                            })
                    
                    if not final_products: continue

                    template = pub['template'] or "🔥 Актуальные цены:\n\n{prices}\n\nДля заказа пишите менеджеру!"
                    
                    # 1. Красиво собираем товары в текстовый список
                    prices_lines = []
                    for p in final_products:
                        if p['price'] == 'Цена по запросу':
                            prices_lines.append(f"▪️ {p['name']} — {p['price']}")
                        else:
                            prices_lines.append(f"▪️ {p['name']} — {p['price']} ₽")
                            
                    prices_text = "\n".join(prices_lines)
                    
                    # 2. Подставляем список вместо метки {prices}
                    if "{prices}" in template:
                        message_text = template.replace("{prices}", prices_text)
                    else:
                        # Если вы случайно забыли написать {prices} в шаблоне, 
                        # бот просто аккуратно приклеит прайс-лист в самый низ сообщения
                        message_text = f"{template}\n\n{prices_text}"

                    loop = user_loops.get(userbot_id)
                    if not loop or not loop.is_running(): continue

                    chat_id = pub['chat_id']
                    message_id = pub['message_id']
                    
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    # 3. ОТПРАВКА В ТЕЛЕГРАМ
                    async def update_or_create(cid, mid, txt, p_id):
                        try:
                            cid = int(cid)
                        except ValueError:
                            pass

                        # --- ЛОГИКА РАЗДЕЛЕНИЯ ТЕКСТА ---
                        parts = []
                        max_len = 4000 # Оставляем небольшой запас до 4096
                        temp_txt = txt
                        
                        while len(temp_txt) > max_len:
                            # Ищем последний перенос строки в пределах лимита, чтобы не рвать строку пополам
                            split_idx = temp_txt.rfind('\n', 0, max_len)
                            if split_idx == -1:
                                split_idx = max_len # Если нет переносов, режем жестко
                                
                            parts.append(temp_txt[:split_idx])
                            temp_txt = temp_txt[split_idx:].strip()
                            
                        if temp_txt:
                            parts.append(temp_txt)
                        # --------------------------------

                        if mid:
                            try:
                                # ВАЖНЫЙ НЮАНС ПРИ РЕДАКТИРОВАНИИ: 
                                # Telegram не позволяет превратить 1 старое сообщение в 2 новых.
                                # Поэтому при редактировании обновится только первая часть, а остаток обрежется.
                                await tg_client.edit_message(cid, int(mid), parts[0])
                                if len(parts) > 1:
                                    logger.warning(f"Прайс слишком большой для обновления одного сообщения (ID {mid}). Хвост обрезан.")
                            except Exception as e:
                                logger.error(f"Ошибка редактирования: {e}")
                        else:
                            try:
                                # В режиме "отправки нового" просто шлем все куски друг за другом
                                first_msg = None
                                for part in parts:
                                    msg = await tg_client.send_message(cid, part)
                                    if not first_msg:
                                        first_msg = msg # Запоминаем только самое первое сообщение

                                # Сохраняем в базу ID первого сообщения (если захотите его потом удалять/редактировать)
                                if first_msg:
                                    with app.app_context():
                                        local_db = get_db()
                                        local_db.execute("UPDATE publications SET message_id = ? WHERE id = ?", (str(first_msg.id), p_id))
                                        local_db.commit()
                            except Exception as e:
                                logger.error(f"Ошибка отправки разделенного сообщения: {e}")
                                
                    try:
                        asyncio.run_coroutine_threadsafe(update_or_create(chat_id, message_id, message_text, pub_id), loop)
                        last_publish_times[pub_id] = now
                    except Exception as e:
                        logger.error(f"Ошибка передачи в цикл публикации: {e}")

            except Exception as e:
                logging.error(f"Ошибка в планировщике публикации: {e}")
            time.sleep(60)


def get_catalog_for_client(client_id):
    """Возвращает каталог для указанного клиента (как в /api/v1/catalog)."""
    db = get_db()
    client = db.execute("SELECT * FROM api_clients WHERE id=?", (client_id,)).fetchone()
    if not client:
        return None

    user_id = client['user_id']
    
    if client['schedule_enabled']:
        from datetime import timedelta
        now = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
        start_time = datetime.strptime(client['time_start'], '%H:%M').time()
        end_time = datetime.strptime(client['time_end'], '%H:%M').time()
        
        is_active = start_time <= now <= end_time if start_time <= end_time else (now >= start_time or now <= end_time)
        if not is_active: return {'categories': [], 'products': []}
        
    user_id = client['user_id']
    client_id = client['id']
    
    allowed_folders = client['allowed_folders'] if 'allowed_folders' in client.keys() else 'all'
    allowed_chats = client['allowed_chats'] if 'allowed_chats' in client.keys() else 'all'

    folders_list = allowed_folders.split(',') if allowed_folders not in ('all', '', None) else allowed_folders
    chats_list = allowed_chats.split(',') if allowed_chats not in ('all', '', None) else allowed_chats

    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM api_markups WHERE client_id=?", (client_id,)).fetchall()
    markups = {}
    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
    for m in markups_raw:
        if m['folder_id'] == 0: default_markup = dict(m)
        else: markups[m['folder_id']] = dict(m)
            
    products_raw = db.execute("""
        SELECT p.id, p.name, p.folder_id, p.price as manual_price, p.photo_url, p.brand, p.country, p.weight, p.model_number, p.is_on_request,
               p.color, p.storage, p.ram, p.warranty, p.description, p.description_html, p.specs,
               (SELECT pm.extracted_price FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as parsed_price,
               (SELECT pm.is_actual FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as is_actual,
               (SELECT m.chat_id FROM product_messages pm JOIN messages m ON pm.message_id = m.id WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as latest_chat_id
        FROM products p
        WHERE p.user_id = ?
    """, (user_id,)).fetchall()
    
    products = []
    active_folder_ids = set()
    
    try:
        access_rules = json.loads(client['access_rules']) if 'access_rules' in client.keys() and client['access_rules'] else {}
    except:
        access_rules = {}
        
    for p in products_raw:
        if not access_rules: continue
        
        pid_str = str(p['id'])
        if pid_str not in access_rules: continue
        
        allowed_chats = access_rules[pid_str]
        chat_id_str = str(p['latest_chat_id'])
        
        if chat_id_str != 'None':
            if allowed_chats != ['all'] and chat_id_str not in allowed_chats: 
                continue
        
        base_price = float(p['parsed_price'] if (p['parsed_price'] is not None and p['is_actual'] == 1) else (p['manual_price'] or 0))
        
        if base_price > 0 and not p['is_on_request']:
            mk = markups.get(p['folder_id'], default_markup)
            if mk['markup_type'] == 'percent':
                final_price = base_price * (1 + mk['markup_value'] / 100)
            else:
                final_price = base_price + mk['markup_value']
                
            rnd = mk['rounding']
            if rnd > 0:
                final_price = math.ceil(final_price / rnd) * rnd
            final_price = int(final_price)
        else:
            final_price = "По запросу"
            
        photo_list = []
        if p['photo_url']:
            for url in p['photo_url'].split(','):
                url = url.strip()
                if url:
                    if url.startswith('http'):
                        photo_list.append(url)
                    else:
                        photo_list.append(f"https://engine.astoredirect.ru/uploads/{url}")

        # Единый, правильный парсинг specs
        specs_data = parse_specs_payload(p['specs'], p)

        products.append({
            'id': p['id'],
            'name': p['name'],
            'category_id': p['folder_id'],
            'price': final_price,
            'price2': "По запросу" ,
            'photo_url': photo_list, 
            'brand': p['brand'],
            'country': p['country'],
            'weight': p['weight'],
            'model_number': p['model_number'],
            'is_on_request': bool(p['is_on_request']),
            
            # Все системные поля и объект характеристик
            'color': p['color'],
            'storage': p['storage'],
            'ram': p['ram'],
            'warranty': p['warranty'],
            'description': p['description'],
            'description_html': p['description_html'],
            'specs': specs_data
        })
        
        if p['folder_id']:
            active_folder_ids.add(p['folder_id'])
            
    folders_raw = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    
    parent_map = {f['id']: f['parent_id'] for f in folders_raw}
    folders_to_keep = set(active_folder_ids)
    
    for fid in active_folder_ids:
        current = fid
        while current in parent_map and parent_map[current] is not None:
            current = parent_map[current]
            folders_to_keep.add(current)
            
    categories = [{'id': f['id'], 'name': f['name'], 'parent_id': f['parent_id']} 
                  for f in folders_raw if f['id'] in folders_to_keep]
        
    return {
        'categories': categories,
        'products': products
    }

def google_sheets_scheduler():
    """Фоновый поток для парсинга Гугл Таблиц по расписанию с учетом скрытых строк"""
    import requests
    from io import BytesIO
    from openpyxl import load_workbook
    
    time.sleep(5)
    last_parsed = {}
    
    with app.app_context():
        while True:
            try:
                db = get_db()
                # Берем все конфиги для Google Таблиц
                configs = db.execute("SELECT * FROM excel_configs WHERE source_type = 'google_sheet'").fetchall()
                current_time = time.time()
                
                # Группируем по URL, чтобы не скачивать один и тот же файл многократно
                url_configs = {}
                for c in configs:
                    url = c['sheet_url']
                    if url not in url_configs: url_configs[url] = []
                    url_configs[url].append(c)

                for url, conf_list in url_configs.items():
                    # Проверяем интервал по первому конфигу в группе
                    first_conf = conf_list[0]
                    cid = first_conf['id']
                    interval_sec = int(first_conf['parse_interval'] or 60) * 60
                    
                    if cid not in last_parsed or (current_time - last_parsed[cid]) >= interval_sec:
                        logger.info(f"Парсинг Гугл Таблицы {url} по расписанию...")
                        
                        try:
                            # Формируем ссылку на экспорт
                            export_url = url.split('/edit')[0] + '/export?format=xlsx' if "/edit" in url else url
                            resp = requests.get(export_url, timeout=30)
                            if resp.status_code != 200:
                                logger.error(f"Не удалось скачать таблицу: {resp.status_code}")
                                continue
                            
                            # Загружаем книгу через openpyxl (БЕЗ read_only, чтобы видеть скрытые строки)
                            xls_file = BytesIO(resp.content)
                            wb = load_workbook(xls_file, data_only=True)
                        except Exception as e:
                            logger.error(f"Ошибка загрузки книги: {e}")
                            continue

                        # Определяем видимые листы
                        visible_sheets = [s.title for s in wb.worksheets if s.sheet_state == 'visible']

                        for config in conf_list:
                            c_dict = dict(config) # Конвертируем Row в dict
                            sheet_target = c_dict['sheet_name']
                            user_id = c_dict['user_id']
                            chat_id = c_dict['chat_id']
                            
                            # Пропускаем, если конкретный лист скрыт
                            if sheet_target != '*' and sheet_target not in visible_sheets:
                                continue 
                                
                            sheets_to_parse = visible_sheets if sheet_target == '*' else [sheet_target]
                            
                            parsed_lines = []
                            for sn in sheets_to_parse:
                                if sn not in wb.sheetnames: continue
                                ws = wb[sn]
                                
                                # Превращаем данные листа в список строк для удобства
                                # Индексация в all_rows будет с 0, а в Excel (row_dimensions) с 1
                                all_rows = list(ws.iter_rows(values_only=True))
                                
                                step = int(c_dict.get('block_step', 1))
                                start = int(c_dict.get('start_row', 0))
                                n_col = int(c_dict.get('name_col', 0))
                                p_col = int(c_dict.get('price_col', 1))
                                n_off = int(c_dict.get('name_row_offset', 0))
                                p_off = int(c_dict.get('price_row_offset', 0))
                                is_grouped = c_dict.get('is_grouped', 0)
                                sku_col = c_dict.get('sku_col', -1)

                                if is_grouped == 1:
                                    # --- ЛОГИКА ДЛЯ СГРУППИРОВАННЫХ ТАБЛИЦ ---
                                    current_group = ""
                                    for i in range(start, len(all_rows)):
                                        # ПРОВЕРКА: Скрыта ли строка? (openpyxl индексирует с 1)
                                        if i + 1 in ws.row_dimensions and ws.row_dimensions[i + 1].hidden:
                                            continue
                                            
                                        row = all_rows[i]
                                        col_n_val = str(row[n_col]).strip() if n_col < len(row) and row[n_col] is not None else ""
                                        col_sku_val = str(row[sku_col]).strip() if sku_col != -1 and sku_col < len(row) and row[sku_col] is not None else ""
                                        col_p_val = str(row[p_col]).strip() if p_col < len(row) and row[p_col] is not None else ""
                                        
                                        has_price = (col_p_val and col_p_val.lower() != 'none' and col_p_val.lower() != 'nan')
                                        
                                        # Проверка цены
                                        extracted = extract_price(col_p_val) if has_price else None
                                        if extracted is not None and extracted <= 0: continue

                                        if n_col == sku_col:
                                            if col_n_val and not has_price: current_group = col_n_val
                                            elif col_n_val and has_price and current_group:
                                                parsed_lines.append(f"{current_group} {col_n_val} - {col_p_val}")
                                        else:
                                            if col_n_val and col_n_val.lower() != 'none' and not has_price: 
                                                current_group = col_n_val
                                            if col_sku_val and has_price and current_group:
                                                parsed_lines.append(f"{current_group} {col_sku_val} - {col_p_val}")
                                else:
                                    # --- ЛОГИКА ДЛЯ ОБЫЧНЫХ ТАБЛИЦ ---
                                    for i in range(start, len(all_rows), step):
                                        # Проверка на скрытость основной строки
                                        if i + 1 in ws.row_dimensions and ws.row_dimensions[i + 1].hidden:
                                            continue
                                            
                                        name_idx = i + n_off
                                        price_idx = i + p_off
                                        
                                        if name_idx < len(all_rows) and price_idx < len(all_rows):
                                            # Проверяем на скрытость строки со смещением
                                            if (name_idx + 1 in ws.row_dimensions and ws.row_dimensions[name_idx + 1].hidden) or \
                                               (price_idx + 1 in ws.row_dimensions and ws.row_dimensions[price_idx + 1].hidden):
                                                continue
                                                
                                            row_n = all_rows[name_idx]
                                            row_p = all_rows[price_idx]
                                            
                                            n_val = str(row_n[n_col]).strip() if n_col < len(row_n) and row_n[n_col] is not None else ""
                                            p_val = str(row_p[p_col]).strip() if p_col < len(row_p) and row_p[p_col] is not None else ""
                                            
                                            if n_val and p_val and n_val.lower() != 'none' and p_val.lower() != 'none':
                                                extracted = extract_price(p_val)
                                                if extracted is not None and extracted > 0:
                                                    parsed_lines.append(f"{n_val} - {p_val}")

                            if parsed_lines:
                                msg_text = "📊 [GOOGLE ТАБЛИЦА]:\n" + "\n".join(parsed_lines)
                                tc = db.execute("SELECT custom_name, chat_title FROM tracked_chats WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)).fetchone()
                                chat_title = tc['custom_name'] if tc and tc['custom_name'] else (tc['chat_title'] if tc else str(chat_id))
                                
                                from datetime import timedelta
                                now_str = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                                msg_type = f"excel_{c_dict['id']}" 
                                
                                # Сохраняем сообщение в базу
                                cursor = db.execute(
                                    "INSERT INTO messages (user_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?)",
                                    (user_id, msg_type, msg_text, now_str, chat_id, chat_title, f"Лист: {sheet_target}")
                                )
                                new_msg_id = cursor.lastrowid
                                
                                # --- МИГРАЦИЯ ПРИВЯЗОК ---
                                old_bindings = db.execute("""
                                    SELECT pm.id, pm.line_index, p.synonyms
                                    FROM product_messages pm
                                    JOIN products p ON pm.product_id = p.id
                                    JOIN messages m ON pm.message_id = m.id
                                    WHERE m.chat_id = ? AND m.user_id = ? AND m.id != ? AND m.type = ?
                                """, (chat_id, user_id, new_msg_id, msg_type)).fetchall()

                                if old_bindings:
                                    lines = msg_text.split('\n')
                                    for b in old_bindings:
                                        synonyms = [s.strip().lower() for s in (b['synonyms'] or "").split(',') if s.strip()]
                                        match_found = False
                                        new_price = None
                                        new_line_idx = 0
                                        
                                        for i, line in enumerate(lines):
                                            if any(syn in line.lower() for syn in synonyms):
                                                match_found = True
                                                new_price = extract_price(line)
                                                new_line_idx = i
                                                break
                                        
                                        if match_found and new_price and new_price > 0:
                                            db.execute("UPDATE product_messages SET message_id = ?, line_index = ?, extracted_price = ?, is_actual = 1 WHERE id = ?", (new_msg_id, new_line_idx, new_price, b['id']))
                                        else:
                                            db.execute("UPDATE product_messages SET is_actual = 0 WHERE id = ?", (b['id'],))
                                            
                                # Чистим старые сообщения этого листа
                                db.execute("""
                                    DELETE FROM messages 
                                    WHERE chat_id = ? AND user_id = ? AND id != ? AND type = ? 
                                    AND id NOT IN (SELECT message_id FROM product_messages)
                                """, (chat_id, user_id, new_msg_id, msg_type))
                                
                                # --- АВТО-ПРИВЯЗКА НОВЫХ ---
                                all_prods = db.execute("SELECT id, synonyms FROM products WHERE user_id=?", (user_id,)).fetchall()
                                for idx, line in enumerate(parsed_lines):
                                    ext_p = extract_price(line)
                                    if not ext_p or ext_p <= 0: continue
                                    line_l = line.lower()
                                    for prod in all_prods:
                                        syns = [s.strip().lower() for s in (prod['synonyms'] or '').split(',') if s.strip()]
                                        if syns and any(syn in line_l for syn in syns):
                                            exists = db.execute("SELECT id FROM product_messages WHERE product_id=? AND message_id=?", (prod['id'], new_msg_id)).fetchone()
                                            if not exists:
                                                db.execute("INSERT INTO product_messages (product_id, message_id, extracted_price, status, is_actual, line_index) VALUES (?, ?, ?, 'pending', 1, ?)", (prod['id'], new_msg_id, ext_p, idx + 1))
                                                update_product_sort_index(
                                                    db,
                                                    user_id,
                                                    prod['id'],
                                                    build_product_sort_index(idx + 1, prod.get('name')),
                                                )
                                                break
                                            
                                db.commit()
                                notify_clients()
                                logger.info(f"✅ Успешно: Лист {sheet_target} из {url}")
                        
                        # Запоминаем время последнего парсинга
                        for config in conf_list:
                            last_parsed[config['id']] = current_time

            except Exception as e:
                logger.error(f"Глобальная ошибка планировщика GS: {e}", exc_info=True)
                
            time.sleep(60) # Проверка раз в минуту

# Запускаем поток вместе с сервером
threading.Thread(target=google_sheets_scheduler, daemon=True).start()



@app.route('/api/messages/<int:msg_id>/block', methods=['POST'])
@login_required
def block_message(msg_id):
    db = get_db()
    # Устанавливаем флаг блокировки для сообщения
    db.execute("UPDATE messages SET is_blocked = 1 WHERE id = ? AND user_id = ?", (msg_id, session['user_id']))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/product_messages/merge', methods=['POST'])
@login_required
def merge_product_messages():
    data = request.get_json()
    pm_ids = data.get('pm_ids', [])
    if len(pm_ids) < 2:
        return jsonify({'error': 'Выберите минимум 2 сообщения'}), 400
    
    db = get_db()
    # Берем минимальный ID как главный номер группы
    group_id = min(map(int, pm_ids))
    placeholders = ','.join('?' for _ in pm_ids)
    
    db.execute(f"UPDATE product_messages SET group_id = ? WHERE id IN ({placeholders})", [group_id] + pm_ids)
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/products/<int:source_id>/merge', methods=['POST'])
@login_required
def merge_product(source_id):
    target_id = request.get_json()['target_id']
    user_id = session['user_id']
    db = get_db()
    
    # Получаем синонимы обоих товаров
    src = db.execute("SELECT synonyms FROM products WHERE id=? AND user_id=?", (source_id, user_id)).fetchone()
    tgt = db.execute("SELECT synonyms FROM products WHERE id=? AND user_id=?", (target_id, user_id)).fetchone()
    
    if src and tgt:
        src_syns = [s.strip() for s in src['synonyms'].split(',') if s.strip()]
        tgt_syns = [s.strip() for s in tgt['synonyms'].split(',') if s.strip()]
        combined = list(set(src_syns + tgt_syns)) # Объединяем без дубликатов
        
        # Обновляем синонимы целевого товара
        db.execute("UPDATE products SET synonyms=? WHERE id=?", (', '.join(combined), target_id))
        
        # Переносим привязанные сообщения (игнорируем ошибку, если такое сообщение уже привязано)
        try:
            db.execute("UPDATE OR IGNORE product_messages SET product_id=? WHERE product_id=?", (target_id, source_id))
        except:
            pass
            
        # Удаляем старый товар
        db.execute("DELETE FROM products WHERE id=?", (source_id,))
        db.commit()
        notify_clients()
    return jsonify({'success': True})

import re
@app.route('/api/profile/change_password', methods=['POST'])
@login_required
def change_password():
    user_id = session['user_id']
    data = request.get_json()
    
    old_password = data.get('old_password')
    new_password = data.get('new_password')

    if not old_password or not new_password:
        return jsonify({'error': 'Заполните все поля'}), 400

    db = get_db()
    
    # Хэшируем введённый старый пароль и проверяем его
    old_pwd_hash = hashlib.sha256(old_password.encode()).hexdigest()
    user = db.execute("SELECT id FROM users WHERE id = ? AND password_hash = ?", (user_id, old_pwd_hash)).fetchone()

    if not user:
        return jsonify({'error': 'Неверный старый пароль'}), 400

    # Если всё верно, хэшируем новый пароль и записываем в БД 
    new_pwd_hash = hashlib.sha256(new_password.encode()).hexdigest()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (new_pwd_hash, user_id))
    db.commit()

    return jsonify({'success': True})


async def fetch_chat_history(client, target_entity, chat_title, user_id, db_chat_id=None):
    """Парсинг старых сообщений с подробной статистикой"""
    if db_chat_id is None:
        db_chat_id = getattr(target_entity, 'id', target_entity)

    try:
        with app.app_context():
            db = get_db()
            added = 0
            duplicates = 0
            no_text = 0
            errors = 0
            
            async for message in client.iter_messages(target_entity): 
                # Пропускаем пустые (например, просто фото без подписи)
                if (not message.text and not message.document):
                    no_text += 1
                    continue
                
                try:
                    sender = await message.get_sender()
                    sender_name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip() or "Unknown"
                    msg_type = 'channel' if message.is_channel else 'group' if message.is_group else 'private'
                    
                    parsed_text = await parse_excel_message(client, message, db_chat_id, user_id)
                    if not parsed_text:
                        no_text += 1
                        continue

                    # ИСПРАВЛЕНИЕ: Форматируем дату вручную, чтобы не было DeprecationWarning
                    from datetime import timedelta
                    msg_date = (message.date + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

                    db.execute(
                        'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                        (user_id, message.id, msg_type, parsed_text, msg_date, db_chat_id, chat_title, sender_name)
                    )
                    db.commit()
                    added += 1
                    
                    if added % 50 == 0:
                        
                        notify_clients()
                        
                except sqlite3.IntegrityError:
                    duplicates += 1  # Сообщение уже есть в базе
                except Exception as msg_e:
                    errors += 1
                    logger.warning(f"Ошибка в сообщении {message.id}: {msg_e}")
                    
            db.commit()
            notify_clients()
            
            # ВЫВОДИМ ПОДРОБНЫЙ ОТЧЕТ В КОНСОЛЬ
            logger.info(f"Парсинг [{chat_title}]: Добавлено {added} | Уже были в базе: {duplicates} | Без текста/фото: {no_text} | Ошибок: {errors}")
            
    except Exception as e:
        logger.error(f"Глобальная ошибка парсинга истории для {chat_title}: {e}")

@app.route('/api/api_clients/<int:client_id>/publish', methods=['POST'])
@login_required
def update_publish_settings(client_id):
    data = request.json
    db = get_db()
    # Проверяем, что клиент принадлежит текущему пользователю
    client = db.execute("SELECT id FROM api_clients WHERE id=? AND user_id=?", 
                        (client_id, session['user_id'])).fetchone()
    if not client:
        return jsonify({'error': 'Клиент не найден'}), 404

    db.execute("""
        UPDATE api_clients 
        SET publish_enabled=?, publish_chat_id=?, userbot_id=?, publish_template=?, folders=?, publish_interval=?
        WHERE id=?
    """, (data.get('publish_enabled', 0), data.get('publish_chat_id'), data.get('userbot_id'),
          data.get('publish_template'), data.get('folders'), data.get('publish_interval', 60), client_id))
    db.commit()
    return jsonify({'success': True})

# ---------- Декораторы авторизации ----------
def login_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        user = get_session_user_record()
        if has_invalid_session_token(user):
            return build_expired_session_response()
        return f(*args, **kwargs)
    return wrapped

def admin_required(f):
    @functools.wraps(f)
    def wrapped(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('login_page'))
        row = get_session_user_record()
        if has_invalid_session_token(row):
            return build_expired_session_response()
        if not row or row['role'] != 'admin':
            return "Access denied", 403
        return f(*args, **kwargs)
    return wrapped

# ---------- Фоновый слушатель сообщений ----------
def stop_message_listener(session_id):
    client_info = user_clients.get(session_id)
    if client_info:
        _, client, _ = client_info
        loop = user_loops.get(session_id)
        if loop and loop.is_running():
            async def safe_disconnect():
                try:
                    await client.disconnect()
                except:
                    pass
            try:
                asyncio.run_coroutine_threadsafe(safe_disconnect(), loop)
            except:
                pass

    # ВАЖНО: Мы БОЛЬШЕ НЕ удаляем user_clients.pop() здесь!
    # Это сделает сам фоновый поток перед своей смертью.

    try:
        with app.app_context():
            db = get_db()
            db.execute("UPDATE user_telegram_sessions SET status = 'inactive' WHERE id = ?", (session_id,))
            db.commit()
            if 'notify_clients' in globals(): notify_clients()
    except Exception as e:
        logging.error(f"Ошибка БД при остановке сессии {session_id}: {e}")

@app.route('/logo.svg')
def serve_logo():
    # Отдаем логотип из директории приложения, а не из текущего cwd процесса
    response = send_from_directory(BASE_DIR, 'logo.svg', max_age=31536000)
    response.cache_control.public = True
    response.cache_control.max_age = 31536000
    return response

# Было: @app.route('/api/excel/configs/chat/<int:chat_id>', methods=['DELETE'])
@app.route('/api/excel/configs/chat/<chat_id>', methods=['DELETE'])
def delete_excel_chat_configs(chat_id):
    try:
        chat_id = int(chat_id)
        user_id = session.get('user_id')
        db = get_db()
        
        # 1. Удаляем привязки к товарам
        db.execute("""
            DELETE FROM product_messages 
            WHERE message_id IN (SELECT id FROM messages WHERE chat_id = ? AND user_id = ? AND type LIKE 'excel%')
        """, (chat_id, user_id))
        
        # 2. Удаляем сами сообщения (прайс-листы)
        db.execute("DELETE FROM messages WHERE chat_id = ? AND user_id = ? AND type LIKE 'excel%'", (chat_id, user_id))
        
        # 3. Удаляем конфигурацию парсера
        db.execute("DELETE FROM excel_configs WHERE chat_id = ? AND user_id = ?", (chat_id, user_id))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Ошибка при удалении файла: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/publications/<int:pub_id>/markups', methods=['GET', 'POST'])
@login_required
def manage_pub_markups(pub_id):
    db = get_db()
    if request.method == 'GET':
        markups = db.execute("""
            SELECT pm.*, f.name as folder_name 
            FROM pub_markups pm 
            LEFT JOIN folders f ON pm.folder_id = f.id 
            WHERE pm.pub_id=?
        """, (pub_id,)).fetchall()
        return jsonify([dict(m) for m in markups])
    
    data = request.json
    folder_id = data.get('folder_id') or 0
    db.execute("""
        INSERT INTO pub_markups (pub_id, folder_id, markup_type, markup_value, rounding) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(pub_id, folder_id) DO UPDATE SET 
        markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
    """, (pub_id, folder_id, data['markup_type'], data['markup_value'], data['rounding']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/pub_markups/<int:markup_id>', methods=['DELETE'])
@login_required
def delete_pub_markup(markup_id):
    db = get_db()
    db.execute("DELETE FROM pub_markups WHERE id=? AND folder_id != 0", (markup_id,))
    db.commit()
    return jsonify({'success': True})


def start_message_listener(session_id, user_id, api_id, api_hash):
    with listener_locks.setdefault(session_id, threading.Lock()):
        if session_id in background_tasks:
            logging.info(f"Слушатель для сессии {session_id} уже запущен")
            return

        def listener():
            loop = asyncio.new_event_loop()
         
            asyncio.set_event_loop(loop)
            loop.set_exception_handler(custom_exception_handler)
            session_file = f'sessions/user_{user_id}_{session_id}'
            client = TelegramClient(session_file, int(api_id), api_hash)
            
            user_clients[session_id] = (threading.current_thread(), client, user_id)
            user_loops[session_id] = loop

           

   
            # --- НОВЫЙ БЛОК: СЛУШАЕМ УДАЛЕНИЯ ---
            @client.on(MessageDeleted)
            async def deleted_handler(event):
                with app.app_context():
                    db = get_db()
                    try:
                        for msg_id in event.deleted_ids:
                            # 1. Отвязываем товары
                            db.execute("""
                                UPDATE product_messages 
                                SET is_actual = 0 
                                WHERE message_id IN (SELECT id FROM messages WHERE telegram_message_id = ?)
                            """, (msg_id,))
                            
                            # 2. ФИЗИЧЕСКИ удаляем сообщение из базы
                            db.execute("""
                                DELETE FROM messages WHERE telegram_message_id = ?
                            """, (msg_id,))
                            
                        db.commit()
                    except sqlite3.OperationalError as e:
                        logger.warning(f"База занята, не удалось обновить удаленные сообщения: {e}")
                    finally:
                        notify_clients()

            @client.on(events.MessageEdited)
            async def edit_handler(event):
                message = event.message
                if (not message.text and not message.document):
                    return
                
                chat = await event.get_chat()
                
                # --- ИГНОРИРУЕМ ИСХОДЯЩИЕ ТОЛЬКО ДЛЯ БОТОВ ---
                if getattr(message, 'out', False) and event.is_private and getattr(chat, 'bot', False):
                    return
                # ---------------------------------------------
                
                chat_id = chat.id
                
              
                # --- УМНАЯ ИЗОЛЯЦИЯ ПАРСИНГА БОТОВ (ФИНАЛ) ---
                sender = await event.get_sender()
                is_bot = getattr(sender, 'bot', False)
                
                if is_bot:
                    bot_un = getattr(sender, 'username', '')
                    if not bot_un:
                        try:
                            entity = await client.get_entity(chat_id)
                            bot_un = getattr(entity, 'username', '')
                        except: pass
                        
                    bot_un_clean = bot_un.lower().replace('@', '').strip() if bot_un else ''
                    
                    # Генерируем все возможные варианты того, как бот мог быть записан в базу
                    bot_un_at = f"@{bot_un_clean}"
                    bot_link_1 = f"https://t.me/{bot_un_clean}"
                    bot_link_2 = f"t.me/{bot_un_clean}"
                    
                    print(f"\n[БОТ ДЕБАГ] 🤖 Пришло сообщение от бота: '{bot_un_clean}'. Chat ID: {chat_id}")
                    
                    with app.app_context():
                        local_db = get_db()
                        # Ищем любое из 4-х совпадений: ник, @ник, t.me/ник или https://t.me/ник
                        check_bot = local_db.execute(
                            "SELECT id FROM interaction_bots WHERE user_id = ? AND (LOWER(bot_username) IN (?, ?, ?, ?))",
                            (user_id, bot_un_clean, bot_un_at, bot_link_1, bot_link_2)
                        ).fetchone()
                        
                        if not check_bot:
                            print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот '{bot_un_clean}' не привязан к аккаунту {user_id}! Игнорируем.")
                            return
                        else:
                            print(f"[БОТ ДЕБАГ] ✅ БОТ ОДОБРЕН: '{bot_un_clean}' найден в базе. Идем дальше...")
                            
                            check_tracked = local_db.execute(
                                "SELECT id FROM tracked_chats WHERE chat_id = ? AND user_id = ?",
                                (chat_id, user_id)
                            ).fetchone()
                            
                            if not check_tracked:
                                print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот одобрен, но не добавлен в источники (tracked_chats)!")
                            else:
                                print(f"[БОТ ДЕБАГ] ✅ ПОЛНЫЙ УСПЕХ: Начинаем парсить!")
                # -------------------------------------------------------------
                
                # --- МАГИЯ EXCEL ---
                parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                if not parsed_text:
                    return

                with app.app_context():
                    db = get_db()
                    # 1. Обновляем текст в базе
                    db.execute("UPDATE messages SET text = ? WHERE telegram_message_id = ? AND chat_id = ? AND user_id = ?", 
                               (parsed_text, message.id, chat_id, user_id))
        
                    bindings = db.execute("""
                        SELECT pm.id, pm.line_index, p.synonyms 
                        FROM product_messages pm 
                        JOIN products p ON pm.product_id = p.id 
                        JOIN messages m ON pm.message_id = m.id
                        WHERE m.telegram_message_id = ? AND m.chat_id = ? AND m.user_id = ?
                    """, (message.id, chat_id, user_id)).fetchall()
        
                    lines = parsed_text.split('\n') if parsed_text else []

                    for b in bindings:
                        line_idx = b['line_index']
                        synonyms = [s.strip().lower() for s in b['synonyms'].split(',') if s.strip()]
                        
                        is_actual = 0
                        new_price = None
                        
                        # Если привязка идет ко всему сообщению (line_index = -1)
                        # Если привязка идет ко всему сообщению (line_index = -1)
                        if line_idx == -1:
                            text_lower = parsed_text.lower()
                            if any(syn in text_lower for syn in synonyms):
                                is_actual = 1
                                new_price = extract_price(parsed_text)
                                
                        # Если привязка идет к конкретной строке
                        else:
                            if 0 <= line_idx < len(lines):
                                edited_line = lines[line_idx]
                                text_lower = edited_line.lower()
                                
                                # Проверяем, остался ли товар в этой строке
                                if any(syn in text_lower for syn in synonyms):
                                    is_actual = 1
                                    # ДОСТАЕМ НОВУЮ ЦЕНУ ИЗ ОТРЕДАКТИРОВАННОЙ СТРОКИ
                                    new_price = extract_price(edited_line)
                        
                        # Обновляем и статус актуальности, и НОВУЮ ЦЕНУ
                        db.execute("""
                            UPDATE product_messages 
                            SET is_actual = ?, extracted_price = ? 
                            WHERE id = ?
                        """, (is_actual, new_price, b['id']))
            
                    db.commit()
                    notify_clients()

            # --------------------------------------------------
            # --------------------------------------------------

            

            @client.on(events.NewMessage)
            async def handler(event):
                message = event.message
                
                if (not message.text and not message.document):
                    return

                chat = await event.get_chat()

                # --- ИГНОРИРУЕМ ИСХОДЯЩИЕ ТОЛЬКО ДЛЯ БОТОВ ---
                if getattr(message, 'out', False) and event.is_private and getattr(chat, 'bot', False):
                    return
                # ---------------------------------------------

                allowed_now = is_parsing_allowed(session_id)
                is_delayed = 0 if allowed_now else 1
                
                chat_id = chat.id

                # Проверка: отслеживаем ли мы этот чат?
                with app.app_context():
                    db = get_db()
                    cursor = db.execute(
                        "SELECT COUNT(*) FROM tracked_chats WHERE user_id = ? AND chat_id = ?",
                        (user_id, chat_id)
                    )
                    count = cursor.fetchone()[0]
                    
                    # СТРОГОЕ ПРАВИЛО: Если чат не добавлен в список - игнорируем сообщение!
                    if count == 0:
                        return

                # Определяем тип и название чата
                if event.is_private:
                    first = getattr(chat, 'first_name', '')
                    last = getattr(chat, 'last_name', '')
                    chat_title = f"{first} {last}".strip() or "Unknown"
                    msg_type = 'private'
                elif event.is_group:
                    chat_title = getattr(chat, 'title', 'Untitled Group')
                    msg_type = 'group'
                elif event.is_channel:
                    chat_title = getattr(chat, 'title', 'Untitled Channel')
                    msg_type = 'channel'
                else:
                    chat_title = "Unknown"
                    msg_type = 'unknown'

                # --- УМНАЯ ИЗОЛЯЦИЯ ПАРСИНГА БОТОВ (ФИНАЛ) ---
                sender = await event.get_sender()
                is_bot = getattr(sender, 'bot', False)
                sender_name = getattr(sender, 'username', '')
                if not sender_name:
                    sender_name = getattr(sender, 'first_name', 'Бот')
                    
                if is_bot:
                    bot_un = getattr(sender, 'username', '')
                    if not bot_un:
                        try:
                            entity = await client.get_entity(chat_id)
                            bot_un = getattr(entity, 'username', '')
                        except: pass
                        
                    bot_un_clean = bot_un.lower().replace('@', '').strip() if bot_un else ''
                    
                    # Генерируем все возможные варианты того, как бот мог быть записан в базу
                    bot_un_at = f"@{bot_un_clean}"
                    bot_link_1 = f"https://t.me/{bot_un_clean}"
                    bot_link_2 = f"t.me/{bot_un_clean}"
                    
                    print(f"\n[БОТ ДЕБАГ] 🤖 Пришло сообщение от бота: '{bot_un_clean}'. Chat ID: {chat_id}")
                    
                    with app.app_context():
                        local_db = get_db()
                        # Ищем любое из 4-х совпадений: ник, @ник, t.me/ник или https://t.me/ник
                        check_bot = local_db.execute(
                            "SELECT id FROM interaction_bots WHERE user_id = ? AND (LOWER(bot_username) IN (?, ?, ?, ?))",
                            (user_id, bot_un_clean, bot_un_at, bot_link_1, bot_link_2)
                        ).fetchone()
                        
                        if not check_bot:
                            print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот '{bot_un_clean}' не привязан к аккаунту {user_id}! Игнорируем.")
                            return
                        else:
                            print(f"[БОТ ДЕБАГ] ✅ БОТ ОДОБРЕН: '{bot_un_clean}' найден в базе. Идем дальше...")
                            
                            check_tracked = local_db.execute(
                                "SELECT id FROM tracked_chats WHERE chat_id = ? AND user_id = ?",
                                (chat_id, user_id)
                            ).fetchone()
                            
                            if not check_tracked:
                                print(f"[БОТ ДЕБАГ] ❌ ОТКАЗ: Бот одобрен, но не добавлен в источники (tracked_chats)!")
                                return
                            else:
                                print(f"[БОТ ДЕБАГ] ✅ ПОЛНЫЙ УСПЕХ: Начинаем парсить!")
                # -------------------------------------------------------------

                parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                
                # Если после парсинга текста нет (например, пустой Excel или не настроен), прерываем
                if not parsed_text:
                    return

                with app.app_context():
                    db = get_db()
                    try:
                        # 1. Записываем новое сообщение
                        from datetime import timedelta
                        msg_date = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")
                        cursor = db.execute(
                            'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name, is_delayed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
                            (user_id, message.id, msg_type, parsed_text, msg_date, chat_id, chat_title, sender_name, is_delayed)
                        )
                        db.commit()

                        # ТАМОЖНЯ: Если время нерабочее - выходим (не обновляем интерфейс мгновенно)
                        if not allowed_now:
                            return

                        # Оповещаем веб-интерфейс о новом сообщении
                        notify_clients()
                        
                        # Сохраняем имя чата, если оно ранее не было сохранено
                        if chat_title:
                            db.execute(
                                "UPDATE tracked_chats SET chat_title = ? WHERE user_id = ? AND chat_id = ? AND chat_title IS NULL",
                                (chat_title, user_id, chat_id)
                            )
                            db.commit()
                            notify_clients()
                            
                    except sqlite3.IntegrityError:
                        pass

            async def main():
                await client.connect()
            
                # --- ОБРАБОТКА СЛЕТЕВШЕЙ АВТОРИЗАЦИИ ---
                if not await client.is_user_authorized():
                    logging.error(f"Сессия {session_id}: Клиент не авторизован!")
                    try:
                        await client.disconnect()
                    except Exception:
                        pass
                
                    with app.app_context():
                        db = get_db()
                        # Устанавливаем статус unauthorized
                        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                        db.commit()
                        if 'notify_clients' in globals(): notify_clients()
                    return
                # ---------------------------------------

                me = await client.get_me()
            
                # --- Получаем @username или Имя юзербота ---
                account_name = getattr(me, 'username', None)
                if account_name:
                    account_name = f"@{account_name}"
                else:
                    account_name = getattr(me, 'first_name', f"ID: {me.id}")
                # --------------------------------------------------
            
                logging.info(f"Сессия {session_id}: слушатель запущен для {me.first_name}")
            
                with app.app_context():
                    db = get_db()
                    try:
                        db.execute(
                            "UPDATE user_telegram_sessions SET status = 'active', session_file = ?, account_name = ? WHERE id = ?",
                            (session_file, account_name, session_id)
                        )
                    except sqlite3.OperationalError:
                        db.execute(
                            "UPDATE user_telegram_sessions SET status = 'active', session_file = ? WHERE id = ?",
                            (session_file, session_id)
                        )
                    db.commit()
                    if 'notify_clients' in globals(): notify_clients()
                
                # Блокирующий вызов: клиент слушает события до отключения
                # Блокирующий вызов: клиент слушает события до отключения
                try:
                    await client.run_until_disconnected()
                except AuthKeyUnregisteredError:
                    logging.error(f"Сессия {session_id}: Ключ авторизации отозван сервером Telegram (слетела сессия).")
                    with app.app_context():
                        db = get_db()
                        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                        db.commit()
                        if 'notify_clients' in globals(): notify_clients()


            # --- ЗАПУСК И БЕЗОПАСНАЯ ОЧИСТКА В ЦИКЛЕ ---
            try:
                loop.run_until_complete(main())
            except Exception as e:
                logging.exception(f"Сессия {session_id}: ошибка в слушателе - {e}")
            finally:
                # 1. Корректное отключение от серверов Telegram
                try:
                    if client and client.is_connected():
                        loop.run_until_complete(client.disconnect())
                except Exception:
                    pass
            
                # 2. Очищаем глобальные словари ВНУТРИ родного потока.
                # Это полностью исключает панику сборщика мусора (GC) в Flask!
                user_clients.pop(session_id, None)
                user_loops.pop(session_id, None)
                background_tasks.pop(session_id, None)

                # 3. Отменяем оставшиеся фоновые задачи Telethon и закрываем цикл
                try:
                    tasks = asyncio.all_tasks(loop)
                    for t in tasks:
                        t.cancel()
                
                    if tasks:
                        # Даем задачам шанс чисто закрыться (игнорируя ошибки отмены)
                        loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
                
                    loop.close()
                except Exception:
                    pass
                
                # 4. === ЗАЩИТА СТАТУСА: Сбрасываем статус на "выключен", если поток упал ===
                try:
                    with app.app_context():
                        local_db = get_db()
                        local_db.execute("UPDATE user_telegram_sessions SET status = 'inactive' WHERE id = ?", (session_id,))
                        local_db.commit()
                        if 'notify_clients' in globals(): 
                            notify_clients()
                except Exception as e:
                    logging.error(f"Не удалось сбросить статус сессии {session_id} в БД: {e}")
                # ===========================================================================

                logging.info(f"Сессия {session_id}: слушатель полностью остановлен")

    # --- СОЗДАНИЕ И ЗАПУСК ПОТОКА ---
    # Даем потоку имя для удобства дебага
    thread = threading.Thread(target=listener, daemon=True, name=f"UserbotThread-{session_id}")
    thread.start()
    background_tasks[session_id] = thread
    logging.info(f"Поток слушателя для сессии {session_id} создан")

# ---------- Маршруты Flask ----------
@app.route('/')
@login_required
def index():
    return render_template('store.html')

@app.route('/api/profile/logout_others', methods=['POST'])
@login_required
def logout_others():
    user_id = session['user_id']
    new_token = secrets.token_hex(16)
    
    db = get_db()
    db.execute("UPDATE users SET session_token = ? WHERE id = ?", (new_token, user_id))
    db.commit()
    
    session['session_token'] = new_token
    
    # --- НОВАЯ СТРОКА: Рассылаем сигнал всем подключенным клиентам ---
    socketio.emit('force_logout')
    
    return jsonify({'success': True})

@app.route('/login', methods=['GET', 'POST'])
def login_page():
    if request.method == 'POST':
        login = request.form.get('login')
        password = request.form.get('password')
        pwd_hash = hashlib.sha256(password.encode()).hexdigest()
        
        db = get_db()
        user = db.execute("SELECT * FROM users WHERE login = ? AND password_hash = ?", (login, pwd_hash)).fetchone()
        
        if user:
            # --- ГЕНЕРАЦИЯ И СОХРАНЕНИЕ ТОКЕНА ---
            token = user['session_token'] if 'session_token' in user.keys() else None
            if not token:
                token = secrets.token_hex(16)
                try:
                    db.execute("UPDATE users SET session_token = ? WHERE id = ?", (token, user['id']))
                    db.commit()
                except sqlite3.OperationalError:
                    pass # На случай, если колонка еще не создалась
            
            session['user_id'] = user['id']
            session['role'] = user['role']
            session['login'] = user['login']
            session['session_token'] = token # Обязательно записываем в браузер
            # -------------------------------------
            
            return redirect(url_for('index'))
        return render_template_string(LOGIN_TEMPLATE, error="Неверный логин или пароль")
    return render_template_string(LOGIN_TEMPLATE)


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login_page'))

@app.route('/api/publish_tree_data', methods=['GET'])
@login_required
def get_publish_tree_data():
    db = get_db()
    user_id = session.get('user_id')
    
    folders = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    products = db.execute("""
        SELECT id, name, folder_id, sort_index
        FROM products
        WHERE user_id=?
        ORDER BY CASE WHEN sort_index IS NULL THEN 1 ELSE 0 END, sort_index, id
    """, (user_id,)).fetchall()
    
    # Получаем поставщиков, у которых есть актуальные цены на товары
    # ВАЖНО: Я использую m.chat_id и m.chat_title. Если в вашей базе таблица messages 
    # использует sender_id или channel_name, просто замените эти слова в запросе ниже!
    suppliers = db.execute("""
        SELECT pm.product_id, m.chat_id as supplier_id, m.chat_title as supplier_name, pm.extracted_price
        FROM product_messages pm
        JOIN messages m ON pm.message_id = m.id
        WHERE pm.status = 'confirmed' AND pm.is_actual = 1 AND m.user_id = ?
        GROUP BY pm.product_id, m.chat_id
    """, (user_id,)).fetchall()
    
    return jsonify({
        'folders': [dict(f) for f in folders],
        'products': [dict(p) for p in products],
        'suppliers': [dict(s) for s in suppliers]
    })

@app.route('/api/tracked_chats/<int:chat_id>', methods=['PUT'])
@login_required
def update_tracked_chat(chat_id):
    data = request.get_json()
    custom_name = data.get('custom_name')
    user_id = session['user_id']
    db = get_db()
    # Проверяем, принадлежит ли чат пользователю
    cur = db.execute("SELECT id FROM tracked_chats WHERE id = ? AND user_id = ?", (chat_id, user_id))
    if not cur.fetchone():
        return jsonify({'error': 'Not found'}), 404
    # Разрешаем пустую строку, сохраняем как NULL
    if custom_name == '':
        custom_name = None
    db.execute("UPDATE tracked_chats SET custom_name = ? WHERE id = ?", (custom_name, chat_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/get_qr', methods=['POST'])
@login_required
def get_qr():
    ensure_event_loop()
    data = request.get_json()
    api_id = data.get('api_id')
    api_hash = data.get('api_hash')
    session_id = data.get('session_id') # <--- ВОТ ЗДЕСЬ ПРИНИМАЕМ ID СТАРОЙ СЕССИИ
    user_id = session['user_id']

    if not api_id or not api_hash:
        return jsonify({'success': False, 'error': 'Missing fields'}), 400

    db = get_db()
    
    # --- ИСПРАВЛЕННАЯ ЛОГИКА ---
    if session_id:
        # Если передали ID старой сессии, просто обновляем её статус
        db.execute("UPDATE user_telegram_sessions SET status = 'pending' WHERE id = ? AND user_id = ?", (session_id, user_id))
    else:
        # Иначе создаем новую
        cursor = db.execute(
            "INSERT INTO user_telegram_sessions (user_id, api_id, api_hash, status) VALUES (?, ?, ?, 'pending')",
            (user_id, api_id, api_hash)
        )
        session_id = cursor.lastrowid
    # ---------------------------
        
    db.commit()
    if 'notify_clients' in globals(): notify_clients()
    
    session_file = f'sessions/user_{user_id}_{session_id}'
    q = queue.Queue()

    def qr_worker():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = TelegramClient(session_file, int(api_id), api_hash)

        async def _run():
            await client.connect()
            if await client.is_user_authorized():
                await client.disconnect()
                q.put({'already_authorized': True})
                return

            try:
                qr_login = await client.qr_login()
                q.put({'qr_url': qr_login.url})
                
                await qr_login.wait(timeout=300)
                await client.disconnect()
                
                with app.app_context():
                    local_db = get_db()
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'active' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
                logging.info(f"Сессия {session_id}: статус обновлён на active")
                start_message_listener(session_id, user_id, api_id, api_hash)

            except asyncio.TimeoutError:
                logging.error(f"Сессия {session_id}: таймаут ожидания QR")
                await client.disconnect()
                with app.app_context():
                    local_db = get_db()
                    # Если ошибка, возвращаем статус unauthorized вместо удаления
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
            except Exception as e:
                logging.exception(f"Сессия {session_id}: ошибка при ожидании QR")
                await client.disconnect()
                with app.app_context():
                    local_db = get_db()
                    local_db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
                    local_db.commit()
                    if 'notify_clients' in globals(): notify_clients()
            finally:
                qr_sessions.pop(session_id, None)

        loop.run_until_complete(_run())
        loop.close()

    thread = threading.Thread(target=qr_worker, daemon=True)
    thread.start()

    try:
        res = q.get(timeout=15)
    except queue.Empty:
        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': False, 'error': 'Таймаут генерации QR'}), 500

    if res.get('already_authorized'):
        start_message_listener(session_id, user_id, api_id, api_hash)
        db.execute("UPDATE user_telegram_sessions SET status = 'active' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': True, 'already_authorized': True})

    qr_url = res.get('qr_url')
    if not qr_url:
        db.execute("UPDATE user_telegram_sessions SET status = 'unauthorized' WHERE id = ?", (session_id,))
        db.commit()
        if 'notify_clients' in globals(): notify_clients()
        return jsonify({'success': False, 'error': 'Не удалось сгенерировать QR'}), 500

    qr_sessions[session_id] = True 

    img = qrcode.make(qr_url)
    buffer = BytesIO()
    img.save(buffer, format='PNG')
    buffer.seek(0)
    img_base64 = base64.b64encode(buffer.getvalue()).decode()

    return jsonify({'success': True, 'qr': img_base64, 'session_id': session_id})

@app.route('/api/check_login')
@login_required
def check_login():
    session_id = request.args.get('session_id')
    if not session_id:
        return jsonify({'success': False, 'error': 'Missing session_id'}), 400
    session_id = int(session_id)

    db = get_db()
    row = db.execute("SELECT status FROM user_telegram_sessions WHERE id = ?", (session_id,)).fetchone()
    if row and row['status'] == 'active':
        return jsonify({'success': True})
    return jsonify({'success': False})

@app.route('/api/check_session')
@login_required
def check_session():
    user_id = session['user_id']
    active = any(uid == user_id for _, _, uid in user_clients.values())
    return jsonify({'authorized': active})

import requests

# --- РОУТЫ ДЛЯ НАСТРОЙКИ API-ПАРСЕРА ---
@app.route('/api/api_sources', methods=['GET', 'POST'])
@login_required
def handle_api_sources():
    db = get_db()
    user_id = session.get('user_id')
    
    if request.method == 'GET':
        sources = db.execute("SELECT * FROM api_sources WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(s) for s in sources])
    else:
        data = request.json
        db.execute("""
            INSERT INTO api_sources (user_id, name, url, token, interval_min)
            VALUES (?, ?, ?, ?, ?)
        """, (user_id, data.get('name'), data.get('url'), data.get('token'), data.get('interval_min', 60)))
        db.commit()
        return jsonify({'success': True})

@app.route('/api/api_sources/<int:source_id>', methods=['DELETE'])
def delete_api_source(source_id):
    try:
        db = get_db()
        user_id = session.get('user_id')
        dummy_chat_id = f"api_src_{source_id}"
        
        # 1. Удаляем привязки цен к товарам
        db.execute("DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE chat_id=? AND user_id=?)", (dummy_chat_id, user_id))
        
        # 2. Удаляем сами сохраненные прайсы/сообщения
        db.execute("DELETE FROM messages WHERE chat_id=? AND user_id=?", (dummy_chat_id, user_id))
        
        # 3. Удаляем само подключение API
        db.execute("DELETE FROM api_sources WHERE id=? AND user_id=?", (source_id, user_id))
        
        db.commit()
        return jsonify({'success': True})
    except Exception as e:
        print(f"Ошибка при удалении API: {e}")
        return jsonify({'error': str(e)}), 500

# --- ФОНОВЫЙ ПАРСЕР ЧУЖИХ API ---
def api_parser_scheduler():
    """Раз в минуту проверяет чужие API и заносит товары в базу как поставщик"""
    time.sleep(5)
    last_sync_times = {}
    
    with app.app_context():
        while True:
            try:
                db = get_db()
                sources = db.execute("SELECT * FROM api_sources WHERE is_active=1").fetchall()
                from datetime import timedelta
                now = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)
                
                for src in sources:
                    src_id = src['id']
                    user_id = src['user_id']
                    interval = src['interval_min']
                    
                    # Проверяем, пришло ли время опрашивать этот API
                    last_run = last_sync_times.get(src_id)
                    if last_run and (now - last_run).total_seconds() / 60 < interval:
                        continue
                        
                    # Формируем URL с токеном (как в вашей системе)
                    url = f"{src['url']}?token={src['token']}"
                    
                    try:
                        resp = requests.get(url, timeout=15)
                        if resp.status_code == 200:
                            data = resp.json()
                            products = data.get('products', [])
                            
                            # Отбираем только валидные товары
                            valid_products = [p for p in products if p.get('name') and p.get('price') is not None]
                            
                            # Создаем "фейковый чат", чтобы API выглядел как поставщик
                            dummy_chat_id = f"api_src_{src_id}"
                            dummy_chat_title = f"🌐 API: {src['name']}"
                            
                            # 1. Собираем все товары в единый текст (построчно)
                            lines = [f"Авто-обновление из API ({len(valid_products)} товаров)"]
                            for p in valid_products:
                                lines.append(f"{p['name']} — {p['price']}")
                                
                            msg_text = "\n".join(lines)
                            sender_name = f"🌐 API: {src['name']}"
                            
                            # Вставляем "сообщение" от этого поставщика с полным списком
                            cursor = db.execute(
                                "INSERT INTO messages (user_id, chat_id, chat_title, text, date, sender_name) VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                                (user_id, dummy_chat_id, dummy_chat_title, msg_text, now, sender_name)
                            )
                            dummy_msg_id = cursor.fetchone()['id']
                            
                            # 2. Привязываем товары с указанием конкретной строки (line_index)
                            for i, p in enumerate(valid_products):
                                name = p.get('name')
                                price = p.get('price')
                                
                                # Ищем товар в базе или создаем новый (без папки)
                                prod_row = db.execute("SELECT id FROM products WHERE name=? AND user_id=?", (name, user_id)).fetchone()
                                if prod_row:
                                    prod_id = prod_row['id']
                                else:
                                    cur = db.execute(
                                        "INSERT INTO products (user_id, name, sort_index) VALUES (?, ?, ?) RETURNING id",
                                        (user_id, name, build_product_sort_index(i + 1, name)),
                                    )
                                    prod_id = cur.fetchone()['id']
                                update_product_sort_index(
                                    db,
                                    user_id,
                                    prod_id,
                                    build_product_sort_index(i + 1, name),
                                )
                                    
                                # line_index = i + 1, так как 0-я строка - это заголовок "Авто-обновление..."
                                db.execute("""
                                    INSERT INTO product_messages (product_id, message_id, extracted_price, status, is_actual, line_index)
                                    VALUES (?, ?, ?, 'pending', 1, ?)
                                """, (prod_id, dummy_msg_id, price, i + 1))
                                
                            db.commit()
                            notify_clients()
                            print(f"✅ Спарсили {len(valid_products)} товаров из {src['name']}")
                    except Exception as req_err:
                        print(f"❌ Ошибка запроса к API {src['name']}: {req_err}")
                        
                    last_sync_times[src_id] = now
            except Exception as e:
                print(f"Ошибка в планировщике парсинга API: {e}")
                
            time.sleep(60) # Проверяем таймеры каждую минуту



import math
from flask import request, jsonify, session

import math
from flask import request, jsonify, session

@app.route('/api/messages')
def get_messages():
    # Проверка авторизации
    if 'user_id' not in session:
        return jsonify({'messages': [], 'error': 'Not authorized'}), 401
        
    user_id = session['user_id']
    msg_type = request.args.get('type', 'all')
    search = request.args.get('search', '').lower()
    exclude = request.args.get('exclude', '').lower()
    
    sender = request.args.get('sender', '')
    msg_source_type = request.args.get('msg_type', '')
    
    try:
        page = int(request.args.get('page', 1))
        if page < 1: page = 1
    except ValueError:
        page = 1
        
    try:
        limit = int(request.args.get('limit', 20))
        if limit < 20: limit = 20
        elif limit > 100: limit = 100
    except ValueError:
        limit = 20
        
    offset = (page - 1) * limit

    db = get_db()
    
    # Скрываем сообщения, которые пришли вне тайминга (is_delayed = 1)
    where_clauses = ["m.user_id = ?", "(m.is_blocked IS NULL OR m.is_blocked = 0)", "(m.is_delayed IS NULL OR m.is_delayed = 0)"]
    params = [user_id]

    if msg_type == 'favorites':
        where_clauses.append("m.is_favorite = 1")
    elif msg_type.startswith('folder_'):
        folder_id = msg_type.replace('folder_', '')
        where_clauses.append("m.id IN (SELECT message_id FROM saved_messages WHERE folder_id = ?)")
        params.append(folder_id)
    elif msg_type.startswith('chat_'):
        chat_id = msg_type.replace('chat_', '')
        where_clauses.append("m.chat_id = ?")
        params.append(chat_id)

    if search:
        where_clauses.append("LOWER(m.text) LIKE ?")
        params.append(f"%{search}%")
        
    if exclude:
        where_clauses.append("LOWER(m.text) NOT LIKE ?")
        params.append(f"%{exclude}%")

    if sender and sender != 'all':
        where_clauses.append("(tc.custom_name = ? OR m.chat_title = ? OR REPLACE(m.sender_name, ' None', '') = ?)")
        params.extend([sender, sender, sender])

    if msg_source_type and msg_source_type != 'all':
        where_clauses.append("m.type = ?")
        params.append(msg_source_type)

    where_str = " AND ".join(where_clauses)
    
    try:
        # 1. Достаем все подходящие сообщения, сортируя от новых к старым 
        # (Ограничиваем 2000 для скорости, так как старые дубли все равно скроются)
        data_query = f"""
            SELECT m.*, tc.custom_name
            FROM messages m
            LEFT JOIN tracked_chats tc 
                   ON CAST(m.chat_id AS TEXT) = CAST(tc.chat_id AS TEXT) 
                  AND m.user_id = tc.user_id
            WHERE {where_str} 
            ORDER BY m.date DESC
            LIMIT 2000
        """
        cursor = db.execute(data_query, params)
        all_messages = [dict(row) for row in cursor.fetchall()]

        # === 2. УМНАЯ ФИЛЬТРАЦИЯ ДУБЛИКАТОВ ===
       
        import re
        grouped_messages = []
        
        seen_products_per_seller = {} # Сохраненные товары для каждого продавца
        seller_msg_count = {}         # Счетчик проверенных сообщений от продавца

        for msg in all_messages:
            chat_id = msg['chat_id']
            
            # Инициализируем память для нового продавца
            if chat_id not in seen_products_per_seller:
                seen_products_per_seller[chat_id] = set()
                seller_msg_count[chat_id] = 0
                
            seller_msg_count[chat_id] += 1
            
            # ПРАВИЛО 3: Анализируем только последние 15 сообщений от каждого продавца
            if seller_msg_count[chat_id] > 1000:
                continue

            text = msg['text'] if msg['text'] else ''
            # Разбиваем сообщение на отдельные строки (товары)
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            
            if not lines:
                continue

            # Функция: удаляет цены (числа >1000) и значки валют из строки
            def clean_line(l):
                l = re.sub(r'\b\d{1,3}(?:[ .,]\d{3})+\b|\b\d{4,}\b', '', l.lower())
                l = re.sub(r'[₽$€рRkKкК]', '', l)
                return l.strip()

            # ПРАВИЛО 1: Берем ПЕРВЫЙ осмысленный товар в старом сообщении
            first_product = ""
            for line in lines:
                cleaned = clean_line(line)
                if len(cleaned) > 3:  # Игнорируем пустые строки или одиночные смайлики
                    first_product = cleaned
                    break
            
            # ПРАВИЛО 2: Ищем этот товар в списке того, что продавец уже присылал НЕДАВНО
            if first_product and first_product in seen_products_per_seller[chat_id]:
                # Если нашли — значит это дубликат старого прайса. ПРОПУСКАЕМ (скрываем)!
                continue
                
            # Если мы дошли сюда, значит сообщение содержит новые товары. ПОКАЗЫВАЕМ ЕГО.
            grouped_messages.append(msg)
            
            # А теперь берем 2-й, 3-й и ВСЕ остальные товары из этого сообщения 
            # и записываем их в память, чтобы в будущем отсекать старые дубликаты
            for line in lines:
                cleaned = clean_line(line)
                if len(cleaned) > 3:
                    seen_products_per_seller[chat_id].add(cleaned)
        # ==============================================================================

        # 3. Ручная пагинация обрезанного списка
        total_count = len(grouped_messages)
        total_pages = math.ceil(total_count / limit) if total_count > 0 else 1
        messages_page = grouped_messages[offset : offset + limit]

        return jsonify({
            'messages': messages_page,
            'total': total_count,
            'page': page,
            'limit': limit,
            'total_pages': total_pages
        })

    except Exception as e:
        print(f"Ошибка в API сообщений: {e}")
        return jsonify({'messages': [], 'total': 0, 'page': page, 'limit': limit, 'total_pages': 1, 'error': str(e)})

# --- МАРШРУТЫ ДЛЯ АВТОПУБЛИКАЦИЙ ---

@app.route('/api/publications', methods=['GET'])
@login_required
def get_publications():
    db = get_db()
    user_id = session['user_id'] # Получаем ID текущего пользователя
    pubs = db.execute("SELECT * FROM publications WHERE user_id = ?", (user_id,)).fetchall() # Фильтруем
    return jsonify([dict(p) for p in pubs])

@app.route('/api/publications', methods=['POST'])
@login_required
def save_publication():
    data = request.json
    db = get_db()
    pub_id = data.get('id')
    
    allowed_items_json = json.dumps(data.get('allowed_items', {}))
    markup_type = data.get('markup_type', 'percent')
    markup_value = float(data.get('markup_value', 0))
    rounding = int(data.get('rounding', 100))
    
    if pub_id:
        db.execute("""
            UPDATE publications 
            SET name=?, is_active=?, interval_min=?, chat_id=?, message_id=?, userbot_id=?, template=?, allowed_items=?, markup_type=?, markup_value=?, rounding=?
            WHERE id=?
        """, (data.get('name'), data.get('is_active', 0), data.get('interval_min', 60), 
              data.get('chat_id'), data.get('message_id'), data.get('userbot_id'), 
              data.get('template'), allowed_items_json, markup_type, markup_value, rounding, pub_id))
    else:
        db.execute("""
    INSERT INTO publications (name, is_active, interval_min, chat_id, message_id, userbot_id, template, allowed_items, markup_type, markup_value, rounding, user_id)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (data.get('name'), data.get('is_active', 0), data.get('interval_min', 60), 
              data.get('chat_id'), data.get('message_id'), data.get('userbot_id'), 
              data.get('template'), allowed_items_json, markup_type, markup_value, rounding,session['user_id']))
        # Получаем ID только что созданной публикации
        pub_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        
    # --- СОХРАНЕНИЕ НАЦЕНОК, КОТОРЫЕ БЫЛИ ДОБАВЛЕНЫ ДО СОЗДАНИЯ ПУБЛИКАЦИИ ---
    temp_markups = data.get('temp_markups', [])
    for m in temp_markups:
        db.execute("""
            INSERT INTO pub_markups (pub_id, folder_id, markup_type, markup_value, rounding) 
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(pub_id, folder_id) DO UPDATE SET 
            markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
        """, (pub_id, m['folder_id'], m['markup_type'], m['markup_value'], m['rounding']))
        
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/publications/<int:pub_id>', methods=['DELETE'])
@login_required
def delete_publication(pub_id):
    db = get_db()
    db.execute("DELETE FROM publications WHERE id=?", (pub_id,))
    db.commit()
    notify_clients()
    return jsonify({'success': True})
    
@app.route('/api/publications/<int:pub_id>/toggle', methods=['POST'])
@login_required
def toggle_publication(pub_id):
    data = request.json
    db = get_db()
    db.execute("UPDATE publications SET is_active=? WHERE id=?", (data.get('is_active', 0), pub_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

import re


def smart_folder_sort_key(folder):
    """
    Умная сортировка:
    1. 'По умолчанию' всегда сверху.
    2. Обычные папки идут по алфавиту.
    3. Папки с гигабайтами и терабайтами идут в конце и сортируются по объему.
    """
    name = folder.get('name', '').strip()
    name_lower = name.lower()
    
    # Папка "По умолчанию" всегда будет самой первой (-1)
    if name_lower == 'по умолчанию':
        return (-1, 0, name)
        
    # Ищем названия, состоящие из цифр (64, 128) и опционально "гб" или "тб"
    match = re.fullmatch(r'(\d+)\s*(тб|tb|гб|gb)?', name_lower)
    
    if match:
        val = int(match.group(1))
        unit = match.group(2)
        
        # Переводим терабайты в гигабайты (1 ТБ = 1024 ГБ) для правильной последовательности
        if unit in ['тб', 'tb']:
            val *= 1024
            
        # (1, ...) отправляет такие папки в конец списка, затем сортирует по числу
        return (1, val, name)
        
    # (0, ...) оставляет обычные папки в начале списка и сортирует по алфавиту
    return (0, 0, name)

def sort_folders_recursive(folders_list):
    """Рекурсивно сортирует каждую папку и её подпапки"""
    folders_list.sort(key=smart_folder_sort_key)
    for f in folders_list:
        if f.get('children'):
            sort_folders_recursive(f['children'])


@app.route('/api/folders/tree')
@login_required
def get_folder_tree():
    user_id = session['user_id']
    db = get_db()
    
    # Извлекаем все папки из БД
    cursor = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id = ?", (user_id,))
    rows = cursor.fetchall()

    direct_product_counts = {
        int(row['folder_id']): int(row['count'] or 0)
        for row in db.execute(
            """
            SELECT folder_id, COUNT(*) AS count
            FROM products
            WHERE user_id = ? AND folder_id IS NOT NULL
            GROUP BY folder_id
            """,
            (user_id,),
        ).fetchall()
        if row['folder_id'] is not None
    }
    direct_connected_counts = {
        int(row['folder_id']): int(row['count'] or 0)
        for row in db.execute(
            """
            SELECT p.folder_id, COUNT(*) AS count
            FROM products p
            WHERE p.user_id = ?
              AND p.folder_id IS NOT NULL
              AND EXISTS (
                  SELECT 1
                  FROM product_messages pm
                  JOIN messages m ON pm.message_id = m.id
                  WHERE pm.product_id = p.id
                    AND pm.status = 'confirmed'
                    AND m.user_id = ?
              )
            GROUP BY p.folder_id
            """,
            (user_id, user_id),
        ).fetchall()
        if row['folder_id'] is not None
    }
    
    folders = {}
    roots = []
    
    # 1. Заполняем словарь всеми папками
    for row in rows:
        folders[row['id']] = {
            'id': row['id'],
            'name': row['name'],
            'parent_id': row['parent_id'],
            'direct_product_count': direct_product_counts.get(int(row['id']), 0),
            'direct_connected_product_count': direct_connected_counts.get(int(row['id']), 0),
            'product_count': 0,
            'connected_product_count': 0,
            'children': []
        }
        
    # 2. Выстраиваем дерево (кто в какой папке лежит)
    for fid, f in folders.items():
        if f['parent_id'] is None:
            roots.append(f)
        else:
            if f['parent_id'] in folders:
                folders[f['parent_id']]['children'].append(f)
                
    # 3. Применяем умную сортировку ко всему дереву перед отправкой на фронтенд
    sort_folders_recursive(roots)

    def populate_counts(items):
        total_products = 0
        total_connected = 0
        for item in items:
            child_products, child_connected = populate_counts(item.get('children') or [])
            item['product_count'] = int(item.get('direct_product_count') or 0) + child_products
            item['connected_product_count'] = int(item.get('direct_connected_product_count') or 0) + child_connected
            total_products += item['product_count']
            total_connected += item['connected_product_count']
        return total_products, total_connected

    populate_counts(roots)
                
    return jsonify(roots)

@app.route('/api/folders', methods=['POST'])
@login_required
def create_folder():
    user_id = session['user_id']
    data = request.get_json()
    name = data.get('name')
    parent_id = data.get('parent_id')
    if not name:
        return jsonify({'error': 'Name required'}), 400
    db = get_db()
    try:
        db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (?, ?, ?)",
                   (user_id, name, parent_id))
        db.commit()
        notify_clients()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Folder exists'}), 400

@app.route('/api/folders/<int:folder_id>/rename', methods=['POST'])
@login_required
def rename_folder_route(folder_id):
    user_id = session['user_id']
    data = request.get_json()
    new_name = data.get('name')
    
    if not new_name:
        return jsonify({'error': 'Имя не может быть пустым'}), 400
        
    db = get_db()
    
    row = db.execute("SELECT name FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id)).fetchone()
    if not row:
        return jsonify({'error': 'Папка не найдена'}), 404
    if row['name'] == 'По умолчанию':
        return jsonify({'error': 'Нельзя переименовать папку по умолчанию'}), 400
        
    try:
        db.execute("UPDATE folders SET name = ? WHERE id = ? AND user_id = ?", (new_name, folder_id, user_id))
        db.commit()
        notify_clients() # Уведомляем клиентов для автообновления
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка переименования: {e}")
        return jsonify({'error': 'Системная ошибка при переименовании'}), 500

@app.route('/api/folders/<int:folder_id>/move', methods=['POST'])
@login_required
def move_folder_route(folder_id):
    user_id = session['user_id']
    data = request.get_json() or {}
    parent_id = data.get('parent_id')

    db = get_db()
    folder = db.execute(
        "SELECT id, name, parent_id FROM folders WHERE id = ? AND user_id = ?",
        (folder_id, user_id)
    ).fetchone()
    if not folder:
        return jsonify({'error': 'Папка не найдена'}), 404
    if folder['name'] == 'По умолчанию':
        return jsonify({'error': 'Нельзя перемещать папку по умолчанию'}), 400

    if parent_id in ('', None):
        parent_id = None
    else:
        try:
            parent_id = int(parent_id)
        except (TypeError, ValueError):
            return jsonify({'error': 'Некорректная целевая папка'}), 400

    if parent_id == folder_id:
        return jsonify({'error': 'Нельзя переместить папку в саму себя'}), 400

    if parent_id is not None:
        target = db.execute(
            "SELECT id FROM folders WHERE id = ? AND user_id = ?",
            (parent_id, user_id)
        ).fetchone()
        if not target:
            return jsonify({'error': 'Целевая папка не найдена'}), 404

        folders_raw = db.execute(
            "SELECT id, parent_id FROM folders WHERE user_id = ?",
            (user_id,)
        ).fetchall()
        parent_map = {row['id']: row['parent_id'] for row in folders_raw}
        cursor = parent_id
        while cursor is not None:
            if cursor == folder_id:
                return jsonify({'error': 'Нельзя переместить папку внутрь самой себя или своей подпапки'}), 400
            cursor = parent_map.get(cursor)

    db.execute(
        "UPDATE folders SET parent_id = ? WHERE id = ? AND user_id = ?",
        (parent_id, folder_id, user_id)
    )
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/folders/<int:folder_id>', methods=['DELETE'])
@login_required
def delete_folder(folder_id):
    user_id = session['user_id']
    db = get_db()
    row = db.execute("SELECT name FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id)).fetchone()
    if not row:
        return jsonify({'error': 'Folder not found'}), 404
    if row['name'] == 'По умолчанию':
        return jsonify({'error': 'Cannot delete default folder'}), 400
        
    db.execute("DELETE FROM folders WHERE id = ?", (folder_id,))
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/folder/<int:folder_id>/messages')
@login_required
def get_folder_messages(folder_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("""
        SELECT m.*, tc.custom_name, tc.chat_title as tc_chat_title
        FROM messages m
        JOIN saved_messages sm ON m.id = sm.message_id
        LEFT JOIN tracked_chats tc ON m.chat_id = tc.chat_id AND m.user_id = tc.user_id
        WHERE sm.folder_id = ? AND m.user_id = ?
        AND (m.is_blocked IS NULL OR m.is_blocked = 0)
        ORDER BY sm.saved_date DESC
    """, (folder_id, user_id))
    # ...
    rows = cursor.fetchall()
    messages = [{
        'id': row['id'],
        'telegram_message_id': row['telegram_message_id'], 
        'chat_id': row['chat_id'],                         
        'type': row['type'],
        'text': row['text'],
        'date': row['date'],
        'chat_title': row['chat_title'],
        'sender_name': row['sender_name'] if 'sender_name' in row.keys() else 'Unknown',
        'custom_name': row['custom_name'] if 'custom_name' in row.keys() else None,
        'tc_chat_title': row['tc_chat_title'] if 'tc_chat_title' in row.keys() else None # <--- ДОБАВИТЬ ЭТУ СТРОКУ
    } for row in rows]
    return jsonify(messages)


@app.route('/api/save_messages', methods=['POST'])
@login_required
def save_messages():
    user_id = session['user_id']
    data = request.get_json()
    message_ids = data.get('message_ids', [])
    folder_id = data.get('folder_id')
    if not message_ids or not folder_id:
        return jsonify({'error': 'Missing data'}), 400
    db = get_db()
    cursor = db.execute("SELECT id FROM folders WHERE id = ? AND user_id = ?", (folder_id, user_id))
    if not cursor.fetchone():
        return jsonify({'error': 'Folder not found'}), 404
    for mid in message_ids:
        try:
            db.execute("INSERT INTO saved_messages (message_id, folder_id) VALUES (?, ?)", (mid, folder_id))
        except sqlite3.IntegrityError:
            pass
    db.commit()
    notify_clients()
    return jsonify({'success': True})

@app.route('/api/userbots', methods=['GET'])
@login_required
def get_userbots():
    user_id = session['user_id']
    db = get_db()
    # ДОБАВИЛИ time_start, time_end, schedule_enabled в SQL-запрос
    try:
        cursor = db.execute("SELECT id, api_id, api_hash, status, account_name, time_start, time_end, schedule_enabled FROM user_telegram_sessions WHERE user_id=?", (user_id,))
    except sqlite3.OperationalError:
        cursor = db.execute("SELECT id, api_id, api_hash, status, time_start, time_end, schedule_enabled FROM user_telegram_sessions WHERE user_id=?", (user_id,))
        
    bots = [dict(row) for row in cursor.fetchall()]
    return jsonify(bots)

# --- НОВЫЙ МАРШРУТ ДЛЯ СОХРАНЕНИЯ РАСПИСАНИЯ ---
@app.route('/api/userbots/<int:bot_id>/schedule', methods=['POST'])
@login_required
def update_userbot_schedule(bot_id):
    user_id = session['user_id']
    data = request.get_json()
    db = get_db()
    db.execute("""
        UPDATE user_telegram_sessions 
        SET time_start = ?, time_end = ?, schedule_enabled = ? 
        WHERE id = ? AND user_id = ?
    """, (data['time_start'], data['time_end'], int(data['schedule_enabled']), bot_id, user_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True})
# -----------------------------------------------

@app.route('/api/userbots/<int:bot_id>/toggle', methods=['POST'])
@login_required
def toggle_userbot(bot_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("SELECT user_id, api_id, api_hash, status FROM user_telegram_sessions WHERE id=?", (bot_id,))
    row = cursor.fetchone()
    if not row or row['user_id'] != user_id:
        return jsonify({'error': 'Not found'}), 404

    if row['status'] == 'active':
        stop_message_listener(bot_id)
        new_status = 'inactive'
    else:
        start_message_listener(bot_id, user_id, row['api_id'], row['api_hash'])
        new_status = 'active'

    db.execute("UPDATE user_telegram_sessions SET status = ? WHERE id = ?", (new_status, bot_id))
    db.commit()
    notify_clients()
    return jsonify({'success': True, 'status': new_status})

@app.route('/api/userbots/<int:bot_id>', methods=['DELETE'])
@login_required
def delete_userbot(bot_id):
    user_id = session['user_id']
    db = get_db()
    cursor = db.execute("SELECT user_id, session_file FROM user_telegram_sessions WHERE id=?", (bot_id,))
    row = cursor.fetchone()
    if not row or row['user_id'] != user_id:
        return jsonify({'error': 'Not found'}), 404

    if bot_id in user_clients:
        stop_message_listener(bot_id)

    session_file = row['session_file']
    if session_file and os.path.exists(session_file):
        os.remove(session_file)

    interaction_rows = db.execute(
        "SELECT * FROM interaction_bots WHERE user_id=? AND userbot_id=?",
        (user_id, bot_id)
    ).fetchall()
    tracked_ids = get_interaction_bot_tracked_chat_ids(db, user_id, interaction_rows)
    delete_tracked_chats_by_ids(db, user_id, tracked_ids)
    db.execute("DELETE FROM interaction_bots WHERE user_id=? AND userbot_id=?", (user_id, bot_id))
    db.execute("DELETE FROM user_telegram_sessions WHERE id = ?", (bot_id,))

    remaining_accounts = db.execute(
        "SELECT COUNT(*) FROM user_telegram_sessions WHERE user_id=?",
        (user_id,)
    ).fetchone()[0]
    if not remaining_accounts:
        db.execute("DELETE FROM interaction_bots WHERE user_id=?", (user_id,))
        delete_all_tracked_chats_for_user(db, user_id)

    db.commit()
    notify_clients()
    return jsonify({'success': True})



@app.route('/api/excel/missing_sheets', methods=['GET'])
@login_required
def get_missing_sheets():
    db = get_db()
    missing = db.execute("SELECT * FROM excel_missing_sheets WHERE user_id = ?", (session['user_id'],)).fetchall()
    return jsonify([dict(m) for m in missing])

@app.route('/api/excel/missing_sheets/<int:m_id>', methods=['DELETE'])
@login_required
def delete_missing_sheet(m_id):
    db = get_db()
    db.execute("DELETE FROM excel_missing_sheets WHERE id = ? AND user_id = ?", (m_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/excel/parse_latest/<chat_id>', methods=['POST'])
@login_required
def parse_latest_excel_route(chat_id):
    chat_id = int(chat_id)
    user_id = session['user_id']    
    
    # 1. Собираем ВСЕ активные сессии текущего пользователя
    active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
    if not active_sessions:
        return jsonify({'error': 'Юзерботы не запущены. Включите хотя бы один аккаунт во вкладке "Подключение".'}), 400

    def background_parser():
        with app.app_context():
            db = get_db()
            tc = db.execute("SELECT chat_title, custom_name FROM tracked_chats WHERE chat_id = ? AND user_id = ?", (chat_id, user_id)).fetchone()
            chat_title = tc['custom_name'] if tc and tc['custom_name'] else (tc['chat_title'] if tc else str(chat_id))

        success = False
        
        # 2. ПЕРЕБИРАЕМ ВСЕ АККАУНТЫ ПО ОЧЕРЕДИ
        for sid in active_sessions:
            if sid not in user_clients: continue
            thread, client, uid = user_clients[sid]
            client_loop = user_loops.get(sid)
            
            if not client_loop: continue

            async def try_parse_for_account():
                try:
                    # Пытаемся получить доступ к каналу
                    entity = await client.get_entity(chat_id)
                    
                    # Ищем последние сообщения
                    async for message in client.iter_messages(entity, limit=100):
                        if message.document and getattr(message.file, 'ext', '').lower() in ['.xlsx', '.xls', '.pdf']:
                            
                            parsed_text = await parse_excel_message(client, message, chat_id, user_id)
                            
                            if parsed_text:
                                msg_type = 'channel' if message.is_channel else 'group' if message.is_group else 'private'
                                sender = await message.get_sender()
                                sender_name = f"{getattr(sender, 'first_name', '')} {getattr(sender, 'last_name', '')}".strip() or "Unknown"
                                
                                from datetime import timedelta
                                msg_date = (message.date + timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

                                # 3. Записываем в БД для вывода в СКЛАД
                                with app.app_context():
                                    db = get_db()
                                    try:
                                        db.execute(
                                            'INSERT INTO messages (user_id, telegram_message_id, type, text, date, chat_id, chat_title, sender_name) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                            (user_id, message.id, msg_type, parsed_text, msg_date, chat_id, chat_title, sender_name)
                                        )
                                    except sqlite3.IntegrityError:
                                        db.execute(
                                            "UPDATE messages SET text = ? WHERE telegram_message_id = ? AND user_id = ?", 
                                            (parsed_text, message.id, user_id)
                                        )
                                    db.commit()
                                    # Обновляем Склад в реальном времени
                                    notify_clients()
                                    logger.info(f"✅ Успешно спарсили файл из {chat_title} через аккаунт сессии {sid}")
                                return True # Сообщаем об успехе
                    return False # Файл не найден в последних 100 сообщениях
                except Exception as e:
                    # У этого аккаунта нет доступа к каналу или произошла другая ошибка
                    logger.warning(f"Аккаунт (сессия {sid}) не имеет доступа к чату {chat_id} или произошла ошибка: {e}")
                    return False

            try:
                # Передаем задачу в цикл событий конкретного Telethon-клиента
                future = asyncio.run_coroutine_threadsafe(try_parse_for_account(), client_loop)
                is_parsed = future.result(timeout=60)
                
                if is_parsed:
                    success = True
                    break # 🔥 КАК ТОЛЬКО НАШЛИ И СПАРСИЛИ — ОСТАНАВЛИВАЕМ ПЕРЕБОР
                    
            except Exception as e:
                logger.error(f"Таймаут или критическая ошибка при попытке парсинга через аккаунт {sid}: {e}")
                continue # Пробуем следующий аккаунт

        if not success:
            logger.error(f"❌ Ни один из {len(active_sessions)} аккаунтов не смог спарсить файл для {chat_id}. Возможно, нет доступа или файла нет в последних 100 сообщениях.")

    # Запускаем логику перебора в отдельном потоке, чтобы не подвешивать сервер Flask
    threading.Thread(target=background_parser, daemon=True).start()
    
    return jsonify({'success': True})


@app.route('/api/tracked_chats', methods=['GET'])
@login_required
def get_tracked_chats():
    user_id = session['user_id']
    db = get_db()
    # ДОБАВЛЕНО: custom_name в выборку
    cursor = db.execute("SELECT id, chat_id, chat_title, custom_name FROM tracked_chats WHERE user_id=?", (user_id,))
    chats = [dict(row) for row in cursor.fetchall()]
    return jsonify(chats)

@app.route('/api/tracked_chats', methods=['POST'])
@login_required
def add_tracked_chat():
    data = request.get_json()
    raw_chat_id = data.get('chat_id')
    custom_name = data.get('custom_name') # Получаем кастомное название
    user_id = session['user_id']
  
    if not raw_chat_id:
        return jsonify({'error': 'chat_id required'}), 400

    chat_title = custom_name # Изначально ставим кастомное имя (если оно пустое, ниже получим из ТГ)
    numeric_chat_id = None
    client_to_use = None
    client_loop_to_use = None

    try:
        entity_query = int(raw_chat_id)
    except ValueError:
        entity_query = raw_chat_id

    # Ищем активную сессию юзербота для текущего пользователя
    # Ищем активные сессии юзерботов для текущего пользователя
    active_sessions = [sid for sid, (_, _, uid) in user_clients.items() if uid == user_id]
    
    # ПЕРЕБИРАЕМ ВСЕХ БОТОВ, ПОКА НЕ НАЙДЕМ ТОГО, У КОТОРОГО ЕСТЬ ДОСТУП
    for sid in active_sessions:
        thread, client, uid = user_clients[sid]
        client_loop = user_loops.get(sid)
        
        async def fetch_chat():
            return await client.get_entity(entity_query)
            
        if client_loop:
            try:
                future = asyncio.run_coroutine_threadsafe(fetch_chat(), client_loop) 
                entity = future.result(timeout=5)
                numeric_chat_id = entity.id 
                
                # Если доступ есть, запоминаем этого бота как рабочего для данного чата
                client_to_use = client
                client_loop_to_use = client_loop
                
                # Записываем название чата
                if not chat_title:
                    if hasattr(entity, 'title'):
                        chat_title = entity.title
                    else:
                        chat_title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
                        
                break # Успешно нашли нужного бота, прерываем цикл перебора!
                
            except Exception as e:
                logging.warning(f"Бот сессии {sid} не имеет доступа к чату или произошла ошибка: {e}")
                continue # Пробуем следующего бота
                
                # Если кастомное имя не ввели, берем оригинальное название из Telegram
                if not chat_title:
                    if hasattr(entity, 'title'):
                        chat_title = entity.title
                    else:
                        chat_title = f"{entity.first_name or ''} {entity.last_name or ''}".strip()
            except Exception as e:
                logging.warning(f"Не удалось получить информацию о чате: {e}")

    # Фолбэк, если юзербот не запущен или произошла ошибка
    if not numeric_chat_id:
        try:
            numeric_chat_id = int(raw_chat_id)
        except ValueError:
            return jsonify({'error': 'Запустите юзербота, чтобы добавлять чаты по ссылке, или введите точный числовой ID'}), 400

    # Если название всё ещё пустое (например, юзербот выключен и ввели просто ID без кастомного имени)
    if not chat_title:
        chat_title = str(numeric_chat_id)

    db = get_db()
    try:
        db.execute(
            "INSERT INTO tracked_chats (user_id, chat_id, chat_title, custom_name) VALUES (?, ?, ?, ?)",
            (user_id, numeric_chat_id, chat_title, custom_name)
        )
        db.commit()
        notify_clients()
        
        # Запускаем парсинг истории сообщений в фоне, если юзербот активен
        # Запускаем парсинг истории сообщений в фоне, если юзербот активен
        if client_to_use and client_loop_to_use and numeric_chat_id:
            target_to_parse = entity if 'entity' in locals() else numeric_chat_id
            asyncio.run_coroutine_threadsafe(
                fetch_chat_history(client_to_use, target_to_parse, chat_title, user_id, numeric_chat_id), 
                client_loop_to_use
            )

        return jsonify({'success': True, 'chat_title': chat_title})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Этот чат уже добавлен в список отслеживаемых'}), 400

@app.route('/api/tracked_chats/<item_id>', methods=['DELETE'])
@login_required
def delete_tracked_chat(item_id):
    try:
        db = get_db()
        user_id = session.get('user_id')
        delete_tracked_chats_by_ids(db, user_id, [item_id])
        db.commit()
        notify_clients()
        return jsonify({'success': True})
    except Exception as e:
        db.rollback()
        print(f"Ошибка при удалении чата: {e}")
        return jsonify({'error': str(e)}), 500

# ---------- Административные маршруты ----------
@app.route('/api/admin/users')
@admin_required
def admin_users():
    db = get_db()
    cursor = db.execute("SELECT id, login, role FROM users")
    users = [{'id': row['id'], 'login': row['login'], 'role': row['role']} for row in cursor.fetchall()]
    return jsonify(users)

@app.route('/api/admin/add_user', methods=['POST'])
@admin_required
def admin_add_user():
    data = request.get_json()
    login = data.get('login')
    password = data.get('password')
    if not login or not password:
        return jsonify({'error': 'Login and password required'}), 400
    pwd_hash = hashlib.sha256(password.encode()).hexdigest()
    db = get_db()
    try:
        db.execute("INSERT INTO users (login, password_hash, role) VALUES (?, ?, 'user')", (login, pwd_hash))
        new_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
        db.execute("INSERT INTO folders (user_id, name, parent_id) VALUES (?, 'По умолчанию', NULL)", (new_id,))
        db.commit()
        notify_clients()
        return jsonify({'success': True})
    except sqlite3.IntegrityError:
        return jsonify({'error': 'Login already exists'}), 400


@app.route('/api/admin/users/<int:user_id>/password', methods=['POST'])
@admin_required
def admin_reset_user_password(user_id):
    data = request.get_json() or {}
    password = (data.get('password') or '').strip()
    if not password:
        return jsonify({'error': 'Password required'}), 400

    db = get_db()
    user = db.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    pwd_hash = hashlib.sha256(password.encode()).hexdigest()
    db.execute("UPDATE users SET password_hash = ? WHERE id = ?", (pwd_hash, user_id))
    db.commit()
    return jsonify({'success': True})


@app.route('/api/admin/users/<int:user_id>', methods=['DELETE'])
@admin_required
def admin_delete_user(user_id):
    current_user_id = session.get('user_id')
    if current_user_id == user_id:
        return jsonify({'error': 'Нельзя удалить текущего администратора'}), 400

    db = get_db()
    user = db.execute("SELECT id FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        return jsonify({'error': 'User not found'}), 404

    try:
        db.execute("DELETE FROM saved_messages WHERE message_id IN (SELECT id FROM messages WHERE user_id = ?)", (user_id,))
        db.execute("DELETE FROM saved_messages WHERE folder_id IN (SELECT id FROM folders WHERE user_id = ?)", (user_id,))
        db.execute("DELETE FROM product_messages WHERE message_id IN (SELECT id FROM messages WHERE user_id = ?)", (user_id,))
        db.execute("DELETE FROM product_messages WHERE product_id IN (SELECT id FROM products WHERE user_id = ?)", (user_id,))

        if table_exists(db, "pub_markups"):
            db.execute("DELETE FROM pub_markups WHERE pub_id IN (SELECT id FROM publications WHERE user_id = ?)", (user_id,))
        if table_exists(db, "publication_items"):
            db.execute("DELETE FROM publication_items WHERE publication_id IN (SELECT id FROM publications WHERE user_id = ?)", (user_id,))
        if table_exists(db, "api_markups"):
            db.execute("DELETE FROM api_markups WHERE client_id IN (SELECT id FROM api_clients WHERE user_id = ?)", (user_id,))

        if table_exists(db, "excel_missing_sheets"):
            db.execute("DELETE FROM excel_missing_sheets WHERE user_id = ?", (user_id,))
        if table_exists(db, "excel_configs"):
            db.execute("DELETE FROM excel_configs WHERE user_id = ?", (user_id,))
        if table_exists(db, "interaction_bots"):
            db.execute("DELETE FROM interaction_bots WHERE user_id = ?", (user_id,))
        if table_exists(db, "api_sources"):
            db.execute("DELETE FROM api_sources WHERE user_id = ?", (user_id,))
        if table_exists(db, "api_clients"):
            db.execute("DELETE FROM api_clients WHERE user_id = ?", (user_id,))

        db.execute("DELETE FROM publications WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM tracked_chats WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM user_telegram_sessions WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM products WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM messages WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM folders WHERE user_id = ?", (user_id,))
        db.execute("DELETE FROM users WHERE id = ?", (user_id,))
        db.commit()
    except Exception:
        db.rollback()
        raise

    socketio.emit('force_logout')
    notify_clients()
    return jsonify({'success': True})




@app.route('/api/admin/clear_messages', methods=['POST'])
@admin_required
def admin_clear_messages():
    data = request.get_json()
    period = data.get('period', 'all')
    db = get_db()
    
    try:
        if period == 'all':
            db.execute("DELETE FROM messages")
        else:
            days = int(period)
            cutoff_date = datetime.now() - timedelta(days=days)
            cutoff_str = cutoff_date.strftime("%Y-%m-%d %H:%M:%S")
            db.execute("DELETE FROM messages WHERE date < ?", (cutoff_str,))
        
        db.commit()
        notify_clients() 
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f"Ошибка при очистке сообщений: {e}")
        return jsonify({'error': str(e)}), 500



# ---------- Шаблоны страниц ----------
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html lang="ru">
<head>
<link rel="icon" type="image/svg+xml" href="/logo.svg">
<script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.0.1/socket.io.js"></script>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Astore Engine</title>
    <style>
        :root {
            --accent-blue: #b8d8f5;
            --panel-border: rgba(150, 180, 214, 0.18);
            --text-soft: #bcc6d2;
            --text-faint: #90a0b4;
            --footer-bg:
                radial-gradient(circle at center top, rgba(104, 107, 114, 0.16) 0%, rgba(39, 41, 47, 0.08) 28%, rgba(12, 13, 17, 0) 58%),
                linear-gradient(90deg, #0a0b0f 0%, #171920 50%, #262932 100%);
        }
        html {
            min-height: 100%;
            background: #0b111a;
        }
        * { box-sizing: border-box; }
        body {
            margin: 0;
            min-height: 100vh;
            font-family: 'Helvetica Neue', Helvetica, Arial, sans-serif;
            color: #e0e0e0;
            background-color: #0b111a;
            background:
                radial-gradient(circle at top center, rgba(72, 106, 156, 0.22) 0%, rgba(22, 35, 54, 0.90) 34%, rgba(10,16,25,1) 100%),
                linear-gradient(180deg, #111820 0%, #0f1724 42%, #0b111a 100%);
            background-repeat: no-repeat, no-repeat;
            background-size: 160% 130%, 100% 100%;
            background-position: center top, center top;
            background-attachment: fixed, fixed;
            display: flex;
            flex-direction: column;
            overflow-x: hidden;
        }
        .login-topbar {
            background:
                radial-gradient(circle at center top, rgba(104, 107, 114, 0.20) 0%, rgba(39, 41, 47, 0.12) 28%, rgba(12, 13, 17, 0) 58%),
                linear-gradient(90deg, #0a0b0f 0%, #171920 50%, #262932 100%);
            background-repeat: no-repeat, no-repeat;
            background-size: 140% 120%, 100% 100%;
            background-position: center top, center top;
            border-bottom: 1px solid rgba(255,255,255,0.08);
            box-shadow: inset 0 -1px 0 rgba(255,255,255,0.04), 0 14px 30px rgba(0,0,0,0.34);
        }
        .login-topbar-inner {
            max-width: 1400px;
            margin: 0 auto;
            min-height: 84px;
            padding: 0 28px;
            display: flex;
            align-items: center;
            gap: 14px;
        }
        .login-wordmark {
            color: #eef3f8;
            font-size: 22px;
            font-weight: 800;
            letter-spacing: 0;
            line-height: 1;
            white-space: nowrap;
        }
        .login-shell {
            flex: 1;
            display: flex;
            align-items: center;
            justify-content: center;
            padding: 40px 20px;
            min-height: calc(100vh - 84px - 64px);
        }
        .login-box {
            width: 100%;
            max-width: 420px;
            padding: 30px 30px 26px;
            border-radius: 20px;
            border: 1px solid var(--panel-border);
            background: linear-gradient(180deg, rgba(29, 42, 61, 0.98) 0%, rgba(20, 31, 47, 0.98) 100%);
            box-shadow: 0 24px 60px rgba(0,0,0,0.30);
        }
        .login-box h2 {
            margin: 0 0 8px;
            color: var(--accent-blue);
            font-size: 22px;
            font-weight: 800;
            text-align: center;
        }
        .login-subtitle {
            margin: 0 0 22px;
            color: var(--text-soft);
            font-size: 14px;
            text-align: center;
            font-weight: 600;
        }
        input {
            width: 100%;
            padding: 13px 14px;
            margin: 8px 0 16px 0;
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.10);
            color: #fff;
            border-radius: 10px;
            font-size: 14px;
            outline: none;
        }
        input::placeholder {
            color: var(--text-faint);
        }
        input:focus {
            border-color: rgba(184, 216, 245, 0.42);
            box-shadow: 0 0 0 3px rgba(184, 216, 245, 0.10);
        }
        button {
            width: 100%;
            padding: 13px 14px;
            background:
                radial-gradient(circle at center top, rgba(104, 107, 114, 0.12) 0%, rgba(39, 41, 47, 0.08) 28%, rgba(12, 13, 17, 0) 58%),
                linear-gradient(90deg, #171920 0%, #22252d 50%, #2b2f38 100%);
            color: #cfd3da;
            border: 1px solid rgba(255,255,255,0.10);
            border-radius: 10px;
            cursor: pointer;
            font-weight: 700;
            font-size: 15px;
            transition: background 0.18s ease, border-color 0.18s ease, color 0.18s ease;
        }
        button:hover {
            background:
                radial-gradient(circle at center top, rgba(116, 120, 128, 0.14) 0%, rgba(48, 50, 58, 0.10) 28%, rgba(12, 13, 17, 0) 58%),
                linear-gradient(90deg, #1b1d24 0%, #262932 50%, #323742 100%);
            border-color: rgba(255,255,255,0.16);
            color: #dce0e7;
        }
        .error {
            color: #ffd2d0;
            background: rgba(231, 98, 95, 0.14);
            padding: 10px 12px;
            border-radius: 10px;
            margin-bottom: 15px;
            font-size: 14px;
            border: 1px solid rgba(231, 98, 95, 0.24);
        }
        .password-container { position: relative; margin: 8px 0 16px 0; }
        .password-container input { margin: 0; padding-right: 40px; }
        .toggle-password {
            position: absolute;
            right: 12px;
            top: 50%;
            transform: translateY(-50%);
            cursor: pointer;
            color: var(--text-faint);
            font-size: 18px;
            user-select: none;
        }
        .login-footer {
            background: var(--footer-bg);
            background-repeat: no-repeat, no-repeat;
            background-size: 140% 120%, 100% 100%;
            background-position: center top, center top;
            border-top: 1px solid rgba(255,255,255,0.08);
            box-shadow: inset 0 1px 0 rgba(255,255,255,0.04);
        }
        .login-footer-inner {
            max-width: 1400px;
            min-height: 64px;
            margin: 0 auto;
            padding: 0 28px;
            display: flex;
            align-items: center;
            justify-content: center;
            color: var(--text-faint);
            font-size: 14px;
            text-align: center;
        }
    </style>
</head>
<body>
    <div class="login-topbar">
        <div class="login-topbar-inner">
            <div class="login-wordmark" aria-label="Astore Engine">Astore Engine</div>
        </div>
    </div>
    <div class="login-shell">
        <div class="login-box">
            <h2>Вход в систему</h2>
            <p class="login-subtitle">Авторизация в рабочем интерфейсе Astore Engine</p>
            {% if error %}
                <div class="error">{{ error }}</div>
            {% endif %}
            <form action="/login" method="POST">
                <input type="text" name="login" placeholder="Логин" required>
                
                <div class="password-container">
                    <input type="password" id="login-password" name="password" placeholder="Пароль" required>
                    <span class="toggle-password" onclick="togglePassword(this)"></span>
                </div>

                <button type="submit">Вход</button>
            </form>
        </div>
    </div>
    <div class="login-footer">
        <div class="login-footer-inner">© Astore Engine, 2022-2026. Все права защищены.</div>
    </div>

    <script>
        function togglePassword(iconElement) {
            const passwordInput = document.getElementById('login-password');
            if (passwordInput.type === 'password') {
                passwordInput.type = 'text';
                iconElement.innerText = '🙈'; // Меняем иконку на закрытый глаз
            } else {
                passwordInput.type = 'password';
                iconElement.innerText = '👁️'; // Меняем иконку на открытый глаз
            }
        }
    </script>
</body>
</html>
"""


@app.route('/api/interaction_bots/<int:id>', methods=['PUT'])
@login_required
def update_interaction_bot(id):
    data = request.get_json()
    user_id = session['user_id']
    db = get_db()
    
    bot_username = data.get('bot_username').strip()
    custom_name = data.get('custom_name')
    userbot_id = data.get('userbot_id')
    commands = json.dumps(data.get('commands', []))
    interval_minutes = data.get('interval_minutes', 60)
    
    # Обновляем настройки бота
    db.execute("""
        UPDATE interaction_bots 
        SET userbot_id=?, bot_username=?, custom_name=?, commands=?, interval_minutes=?
        WHERE id=? AND user_id=?
    """, (userbot_id, bot_username, custom_name, commands, interval_minutes, id, user_id))
    
    db.commit()
    notify_clients()
    return jsonify({'success': True})


@app.route('/api/fix_db')
@login_required
def fix_db():
    db = get_db()
    try:
        # Отключаем проверки ключей на секунду, чтобы SQLite разрешил замену
        db.execute("PRAGMA foreign_keys = OFF")
        db.executescript('''
            CREATE TABLE IF NOT EXISTS messages_new (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                telegram_message_id INTEGER,
                type TEXT,
                text TEXT,
                date TIMESTAMP,
                chat_id INTEGER,
                chat_title TEXT,
                is_blocked INTEGER DEFAULT 0,
                sender_name TEXT,
                is_delayed INTEGER DEFAULT 0,
                FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(chat_id, telegram_message_id)
            );
            INSERT OR IGNORE INTO messages_new (id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name, is_delayed)
            SELECT id, user_id, telegram_message_id, type, text, date, chat_id, chat_title, is_blocked, sender_name, 0 FROM messages;
            DROP TABLE messages;
            ALTER TABLE messages_new RENAME TO messages;
        ''')
        db.execute("PRAGMA foreign_keys = ON")
        db.commit()
        return "<h1>✅ База данных успешно исправлена!</h1><p>Глобальная блокировка сообщений снята. Теперь парсер будет выкачивать всё.</p><br><a href='/'>Вернуться в парсер</a>"
    except Exception as e:
        return f"<h1>❌ Ошибка:</h1><p>{e}</p>"

@app.route('/api/cleanup_old_bug')
@login_required
def cleanup_old_bug():
    user_id = session['user_id']
    db = get_db()
    
    # Считаем, сколько сломанных сообщений мы найдем
    cursor = db.execute("SELECT COUNT(*) FROM messages WHERE telegram_message_id IS NULL AND user_id = ?", (user_id,))
    bad_count = cursor.fetchone()[0]
    
    if bad_count > 0:
        # Удаляем их (каскадное удаление само почистит привязки к товарам, если они были)
        db.execute("DELETE FROM messages WHERE telegram_message_id IS NULL AND user_id = ?", (user_id,))
        db.commit()
        notify_clients()
        return f"<h1>Уборка завершена! 🧹</h1><p>Удалено сломанных сообщений: <b>{bad_count}</b>.</p><br><a href='/'>Вернуться в парсер</a>"
    else:
        return "<h1>Всё чисто! ✨</h1><p>Сломанных сообщений в базе не найдено.</p><br><a href='/'>Вернуться в парсер</a>"






# ================= УПРАВЛЕНИЕ API И КЛИЕНТАМИ =================

# ✅ А ЭТОТ БЛОК ДОЛЖЕН ОСТАТЬСЯ ✅
@app.route('/api/api_clients/<int:client_id>/filters', methods=['POST'])
@login_required
def update_api_client_filters(client_id):
    data = request.json
    access_rules = json.dumps(data.get('access_rules', {})) # Конвертируем в JSON
    db = get_db()
    db.execute("UPDATE api_clients SET access_rules=? WHERE id=? AND user_id=?", 
               (access_rules, client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


@app.route('/api/api_clients', methods=['GET', 'POST'])
@login_required
def manage_api_clients():
    user_id = session['user_id']
    db = get_db()
    if request.method == 'GET':
        # Теперь достаем все поля, включая тайминги
        clients = db.execute("SELECT * FROM api_clients WHERE user_id=?", (user_id,)).fetchall()
        return jsonify([dict(c) for c in clients])
    
    # POST: Создаем нового клиента
    data = request.json
    name = data.get('name')
    schedule_enabled = int(data.get('schedule_enabled', 0))
    time_start = data.get('time_start', '00:00')
    time_end = data.get('time_end', '23:59')
    token = secrets.token_hex(24) 
    
    db.execute("""
        INSERT INTO api_clients (user_id, name, token, schedule_enabled, time_start, time_end) 
        VALUES (?, ?, ?, ?, ?, ?)
    """, (user_id, name, token, schedule_enabled, time_start, time_end))
    
    client_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]
    
    db.execute("INSERT INTO api_markups (client_id, folder_id, markup_type, markup_value, rounding) VALUES (?, 0, 'percent', 0, 100)", (client_id,))
    db.commit()
    return jsonify({'success': True})

# --- ДОБАВЬ НОВЫЙ МАРШРУТ ДЛЯ РЕДАКТИРОВАНИЯ ТАЙМИНГОВ ---
@app.route('/api/api_clients/<int:client_id>/schedule', methods=['POST'])
@login_required
def update_api_client_schedule(client_id):
    data = request.json
    db = get_db()
    db.execute("""
        UPDATE api_clients 
        SET schedule_enabled=?, time_start=?, time_end=? 
        WHERE id=? AND user_id=?
    """, (int(data['schedule_enabled']), data['time_start'], data['time_end'], client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_clients/<int:client_id>', methods=['DELETE'])
@login_required
def delete_api_client(client_id):
    db = get_db()
    db.execute("DELETE FROM api_clients WHERE id=? AND user_id=?", (client_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_clients/<int:client_id>/markups', methods=['GET', 'POST'])
@login_required
def manage_markups(client_id):
    db = get_db()
    if request.method == 'GET':
        markups = db.execute("""
            SELECT am.*, f.name as folder_name 
            FROM api_markups am 
            LEFT JOIN folders f ON am.folder_id = f.id 
            WHERE am.client_id=?
        """, (client_id,)).fetchall()
        return jsonify([dict(m) for m in markups])
    
    data = request.json
    folder_id = data.get('folder_id') or 0
    db.execute("""
        INSERT INTO api_markups (client_id, folder_id, markup_type, markup_value, rounding) 
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(client_id, folder_id) DO UPDATE SET 
        markup_type=excluded.markup_type, markup_value=excluded.markup_value, rounding=excluded.rounding
    """, (client_id, folder_id, data['markup_type'], data['markup_value'], data['rounding']))
    db.commit()
    return jsonify({'success': True})

@app.route('/api/api_markups/<int:markup_id>', methods=['DELETE'])
@login_required
def delete_markup(markup_id):
    db = get_db()
    # Запрещаем удалять базовую настройку (folder_id = 0)
    db.execute("DELETE FROM api_markups WHERE id=? AND folder_id != 0 AND client_id IN (SELECT id FROM api_clients WHERE user_id=?)", (markup_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


# ================= ПУБЛИЧНЫЙ API ДЛЯ ВЫДАЧИ КАТАЛОГА =================
@app.route('/api/v1/catalog', methods=['GET'])
def public_api_catalog():
    token = request.args.get('token')
    if not token:
        return jsonify({'error': 'Токен не предоставлен'}), 401
        
    db = get_db()
    client = db.execute("SELECT * FROM api_clients WHERE token=?", (token,)).fetchone()
    if not client:
        return jsonify({'error': 'Неверный токен'}), 401
        
    # --- ПРОВЕРКА РАСПИСАНИЯ ---
    if client['schedule_enabled']:
        from datetime import timedelta
        # МСК время (UTC+3)
        now = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
        start_time = datetime.strptime(client['time_start'], '%H:%M').time()
        end_time = datetime.strptime(client['time_end'], '%H:%M').time()
        
        is_active = start_time <= now <= end_time if start_time <= end_time else (now >= start_time or now <= end_time)
        if not is_active: 
            return jsonify({'categories': [], 'products': []})
            
    user_id = client['user_id']
    client_id = client['id']

    # --- ПОЛУЧАЕМ НАЦЕНКИ ---
    markups_raw = db.execute("SELECT folder_id, markup_type, markup_value, rounding FROM api_markups WHERE client_id=?", (client_id,)).fetchall()
    markups = {}
    default_markup = {'markup_type': 'percent', 'markup_value': 0, 'rounding': 100}
    for m in markups_raw:
        if m['folder_id'] == 0: 
            default_markup = dict(m)
        else: 
            markups[m['folder_id']] = dict(m)

    # --- ПОЛУЧАЕМ ТОВАРЫ (С ПОЛНЫМ НАБОРОМ ПОЛЕЙ И ЦЕНАМИ) ---
    # Используем подзапросы, чтобы сразу вытащить последнюю подтвержденную цену
    products_raw = db.execute("""
        SELECT p.id, p.name, p.folder_id, p.price as manual_price, p.photo_url, p.brand, p.country, p.weight, p.model_number, p.is_on_request,
               p.color, p.storage, p.ram, p.warranty, p.description, p.description_html, p.specs,
               (SELECT pm.extracted_price FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as parsed_price,
               (SELECT pm.is_actual FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as is_actual,
               (SELECT m.chat_id FROM product_messages pm JOIN messages m ON pm.message_id = m.id 
                WHERE pm.product_id = p.id AND pm.status = 'confirmed' ORDER BY m.date DESC LIMIT 1) as latest_chat_id
        FROM products p
        WHERE p.user_id = ?
    """, (user_id,)).fetchall()
    
    products = []
    active_folder_ids = set()
    
    # Правила доступа (какие товары и каких поставщиков видит этот клиент)
    try:
        access_rules = json.loads(client['access_rules']) if 'access_rules' in client.keys() and client['access_rules'] else {}
    except:
        access_rules = {}

    for p in products_raw:
        # 1. Фильтр доступа: если товар не отмечен в access_rules - пропускаем
        pid_str = str(p['id'])
        if not access_rules or pid_str not in access_rules: 
            continue
            
        # 2. Фильтр поставщиков (чатов)
        allowed_chats = access_rules[pid_str]
        chat_id_str = str(p['latest_chat_id'])
        
        # Если есть цена от конкретного чата, проверяем, разрешен ли он (или разрешены 'all')
        if chat_id_str != 'None':
            if allowed_chats != ['all'] and chat_id_str not in allowed_chats:
                # Цена от этого поставщика запрещена, но мы можем использовать ручную цену p['manual_price']
                # Если ручной цены нет, товар станет "По запросу"
                current_price_source = None 
            else:
                current_price_source = p['parsed_price'] if p['is_actual'] == 1 else None
        else:
            current_price_source = None

        # 3. Расчет базовой цены
        base_price = float(current_price_source if current_price_source is not None else (p['manual_price'] or 0))
        
        # 4. Применение наценок и округления
        if base_price > 0 and not p['is_on_request']:
            mk = markups.get(p['folder_id'], default_markup)
            if mk['markup_type'] == 'percent':
                final_price = base_price * (1 + mk['markup_value'] / 100)
            else:
                final_price = base_price + mk['markup_value']
                
            rnd = mk['rounding']
            if rnd > 0:
                final_price = math.ceil(final_price / rnd) * rnd
            final_price = int(final_price)
        else:
            final_price = "По запросу"

        # 5. Обработка фотографий (формируем полные ссылки)
        photo_links = []
        if p['photo_url']:
            # Базовый URL вашего сервера
            base_img_url = "https://engine.astoredirect.ru/uploads/"
            photo_links = [
                (f"{base_img_url}{link.strip()}" if not link.strip().startswith('http') else link.strip())
                for link in p['photo_url'].split(',') if link.strip()
            ]

        # 6. Обработка технических характеристик (specs)
        specs_data = parse_specs_payload(p['specs'], p)

        # 7. Формируем финальный объект товара
        products.append({
            'id': p['id'],
            'name': p['name'],
            'category_id': p['folder_id'],
            'price': final_price, 
            'price2': "По запросу" ,
            "photo_url": photo_links,
            'brand': p['brand'],
            'country': p['country'],
            'weight': p['weight'],
            'model_number': p['model_number'],
            'is_on_request': bool(p['is_on_request']),
            
            # Новые поля
            'color': p['color'],
            'storage': p['storage'],
            'ram': p['ram'],
            'warranty': p['warranty'],
            'description': p['description'],
            'description_html': p['description_html'],
            'specs': specs_data
        })
        
        # Запоминаем ID папки, чтобы потом показать её в списке категорий
        if p['folder_id']:
            active_folder_ids.add(p['folder_id'])

    # --- ОБРАБОТКА КАТЕГОРИЙ (только те, в которых есть товары) ---
    folders_raw = db.execute("SELECT id, name, parent_id FROM folders WHERE user_id=?", (user_id,)).fetchall()
    parent_map = {f['id']: f['parent_id'] for f in folders_raw}
    folders_to_keep = set(active_folder_ids)
    
    # Добавляем всех родителей активных папок (чтобы дерево не рассыпалось)
    for fid in active_folder_ids:
        current = fid
        while current in parent_map and parent_map[current] is not None:
            current = parent_map[current]
            folders_to_keep.add(current)
            
    categories = [
        {'id': f['id'], 'name': f['name'], 'parent_id': f['parent_id']} 
        for f in folders_raw if f['id'] in folders_to_keep
    ]
                  
    # --- СОРТИРОВКА (А-Я) ---
    products.sort(key=lambda x: (x['name'] or '').lower())
    categories.sort(key=lambda x: (x['name'] or '').lower())
        
    return jsonify({
        'categories': categories,
        'products': products
    })


# ================= ПАРСИНГ EXCEL ТАБЛИЦ =================
async def parse_excel_message(client, message, chat_id, user_id):
    text_to_save = message.text or ""
    if not message.document:
        return text_to_save
        
    ext = getattr(message.file, 'ext', '').lower()
    if ext not in ['.xlsx', '.xls', '.pdf']:
        return text_to_save

    # Функция для приведения ID к единому формату (без -100)
    def clean_id(cid):
        s = str(cid)
        if s.startswith('-100'): return s[4:]
        if s.startswith('-'): return s[1:]
        return s

    with app.app_context():
        db = get_db()
        target_id = clean_id(chat_id)
        
        # Загружаем все конфиги для этого пользователя
        all_configs = db.execute("SELECT * FROM excel_configs WHERE user_id = ?", (user_id,)).fetchall()
        configs = {}
        for c in all_configs:
            if clean_id(c['chat_id']) == target_id:
                configs[c['sheet_name']] = dict(c)
        
        # Если конфигов нет вообще - выходим сразу
        if not configs:
            logger.warning(f"Нет настроек парсинга для чата {chat_id}")
            return text_to_save

        tmp_path = None
        try:
            # Скачиваем файл во временное хранилище
            suffix = ext
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
                await client.download_media(message, tmp.name)
                tmp_path = tmp.name
                
            parsed_lines = []
            unknown_sheets = []
            wildcard_config = configs.get('*')

            # --- ОБРАБОТКА PDF ---
            if ext == '.pdf':
                import pdfplumber
                # Для PDF ищем: 'Страница 1', затем '*', если нет - берем первый попавшийся
                config = configs.get('Страница 1') or wildcard_config
                if not config and configs:
                    config = list(configs.values())[0]
                
                if config:
                    with pdfplumber.open(tmp_path) as pdf:
                        for page in pdf.pages:
                            table = page.extract_table()
                            if not table: continue
                            
                            # Превращаем таблицу в безопасный список строк
                            clean_rows = []
                            for row in table:
                                clean_rows.append([str(cell).replace('\n', ' ') if cell is not None else "" for cell in row])
                            
                            step = int(config['block_step'])
                            start = int(config['start_row'])
                            n_col = int(config['name_col'])
                            p_col = int(config['price_col'])
                            n_off = int(config['name_row_offset'])
                            p_off = int(config['price_row_offset'])
                            is_grouped = config.get('is_grouped', 0)
                            sku_col = config.get('sku_col', -1)

                            if is_grouped == 1:
                                current_group = ""
                                for i in range(start, len(clean_rows)):
                                    row = clean_rows[i]
                                    # Проверка: хватает ли колонок в этой строке PDF?
                                    if len(row) <= max(n_col, p_col, sku_col): continue
                                    
                                    col_n_val = row[n_col].strip()
                                    col_sku_val = row[sku_col].strip() if sku_col != -1 else ""
                                    col_p_val = row[p_col].strip()
                                    
                                    if col_n_val and not col_p_val: current_group = col_n_val
                                    if col_p_val and current_group:
                                        name = col_sku_val if col_sku_val else col_n_val
                                        parsed_lines.append(f"{current_group} {name} - {col_p_val}")
                            else:
                                for i in range(start, len(clean_rows), step):
                                    name_idx = i + n_off
                                    price_idx = i + p_off
                                    if name_idx < len(clean_rows) and price_idx < len(clean_rows):
                                        row_n = clean_rows[name_idx]
                                        row_p = clean_rows[price_idx]
                                        if len(row_n) > n_col and len(row_p) > p_col:
                                            n_val = row_n[n_col].strip()
                                            p_val = row_p[p_col].strip()
                                            if n_val and p_val:
                                                parsed_lines.append(f"{n_val} - {p_val}")

            # --- ОБРАБОТКА EXCEL ---
            else:
                xls = pd.ExcelFile(tmp_path)
                for sheet_name in xls.sheet_names:
                    config = configs.get(sheet_name) or wildcard_config
                    if not config:
                        unknown_sheets.append(sheet_name)
                        continue            
                
                        
                    df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
                    df = df.fillna("")
                    
                    step = config['block_step']
                    start = config['start_row']
                    n_col = config['name_col']
                    p_col = config['price_col']
                    n_off = config['name_row_offset']
                    p_off = config['price_row_offset']
                    is_grouped = config.get('is_grouped', 0)
                    sku_col = config.get('sku_col', -1)
                    
                    if is_grouped == 1:
                        current_group = ""
                        for i in range(start, len(df)):
                            try:
                                col_n_val = str(df.iat[i, n_col]).strip()
                                col_sku_val = str(df.iat[i, sku_col]).strip() if sku_col != -1 else ""
                                col_p_val = str(df.iat[i, p_col]).strip()
                                
                                is_empty_name = (not col_n_val or col_n_val == 'nan')
                                has_price = (col_p_val and col_p_val != 'nan')
                                
                                if n_col == sku_col:
                                    if col_n_val and not has_price: current_group = col_n_val
                                    elif col_n_val and has_price and current_group:
                                        parsed_lines.append(f"{current_group} {col_n_val} - {col_p_val}")
                                else:
                                    if col_n_val and not is_empty_name: current_group = col_n_val
                                    if col_sku_val and col_sku_val != 'nan' and has_price and current_group:
                                        parsed_lines.append(f"{current_group} {col_sku_val} - {col_p_val}")
                            except Exception:
                                continue
                    else:
                        for i in range(start, len(df), step):
                            try:
                                name_row = i + n_off
                                price_row = i + p_off
                                if name_row < len(df) and price_row < len(df):
                                    name_val = str(df.iat[name_row, n_col]).strip()
                                    price_val = str(df.iat[price_row, p_col]).strip()
                                    
                                    if name_val and name_val != 'nan' and price_val and price_val != 'nan':
                                        parsed_lines.append(f"{name_val} - {price_val}")
                            except Exception:
                                continue
            
            # --- ФОРМИРОВАНИЕ ИТОГОВОГО ТЕКСТА ---
            if parsed_lines:
                doc_type = "PDF" if ext == '.pdf' else "EXCEL"
                text_to_save = f"📊 [{doc_type} ДАННЫЕ]:\n" + "\n".join(parsed_lines)
                
                if unknown_sheets and ext != '.pdf':
                    with app.app_context():
                        db = get_db()
                        all_chats = db.execute("SELECT chat_id, chat_title, custom_name FROM tracked_chats WHERE user_id = ?", (user_id,)).fetchall()
                        chat_title = str(chat_id)
                        for tc in all_chats:
                            if clean_id(tc['chat_id']) == target_id:
                                chat_title = tc['custom_name'] if tc['custom_name'] else tc['chat_title']
                                break
                                
                        for us in unknown_sheets:
                            db.execute("INSERT OR IGNORE INTO excel_missing_sheets (user_id, chat_id, chat_title, sheet_name) VALUES (?, ?, ?, ?)", (user_id, chat_id, chat_title, us))
                        db.commit()
                        
                    warning_text = f"\n\n⚠️ Внимание! В файле найдены ненастроенные листы: {', '.join(unknown_sheets)}. Перейдите в раздел «Парсинг Excel», чтобы их настроить."
                    if text_to_save:
                        text_to_save += warning_text
                    else:
                        text_to_save = warning_text.strip()
                        
        except Exception as e:
            logger.error(f"Ошибка парсинга файла: {e}")
        finally:
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.remove(tmp_path)
                
    return text_to_save


@app.route('/api/excel/preview_latest/<chat_id>', methods=['GET'])
@login_required
def preview_latest_excel(chat_id):
    chat_id = int(chat_id) # Добавляем преобразование здесь
    user_id = session['user_id']
    
    # Ищем активную сессию юзербота
    client_to_use = None
    client_loop_to_use = None
    for sid, (thread, client, uid) in user_clients.items():
        if uid == user_id:
            client_to_use = client
            client_loop_to_use = user_loops.get(sid)
            break
            
    if not client_to_use:
        return jsonify({'error': 'Юзербот не запущен. Запустите его во вкладке "Юзерботы".'}), 400

    async def fetch_and_preview():
        try:
            entity = await client_to_use.get_entity(chat_id)
            # Ищем последние 50 сообщений
            async for message in client_to_use.iter_messages(entity, limit=50):
                if message.document and getattr(message.file, 'ext', '').lower() in ['.xlsx', '.xls', '.pdf']:
                    ext = getattr(message.file, 'ext', '').lower()
                    
                    # Скачиваем файл во временную директорию
                    with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
                        await client_to_use.download_media(message, tmp.name)
                        tmp_path = tmp.name
                        
                    try:
                        xls = pd.ExcelFile(tmp_path)
                        data = {}
                        # Читаем по 15 строк для предпросмотра
                        for sheet in xls.sheet_names:
                            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=15)
                            df = df.fillna("")
                            data[sheet] = df.values.tolist()
                        return {'success': True, 'sheets_data': data}
                    finally:
                        if os.path.exists(tmp_path):
                            os.remove(tmp_path)
                            
            return {'success': False, 'error': 'В последних 50 сообщениях чата не найден Excel файл'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # Запускаем асинхронную задачу в цикле юзербота и ждем результат
    future = asyncio.run_coroutine_threadsafe(fetch_and_preview(), client_loop_to_use)
    try:
        result = future.result(timeout=30)
        if result.get('success'):
            return jsonify(result)
        else:
            return jsonify({'error': result.get('error')}), 400
    except Exception as e:
        return jsonify({'error': 'Таймаут или ошибка при скачивании файла: ' + str(e)}), 500


@app.route('/api/excel/preview', methods=['POST'])
@login_required
def preview_excel():
    if 'file' not in request.files:
        return jsonify({'error': 'Нет файла'}), 400
    file = request.files['file']
    try:
        xls = pd.ExcelFile(file)
        data = {}
        # Читаем по 15 строк с КАЖДОГО листа
        for sheet in xls.sheet_names:
            df = pd.read_excel(xls, sheet_name=sheet, header=None, nrows=15)
            df = df.fillna("")
            data[sheet] = df.values.tolist()
        return jsonify({'success': True, 'sheets_data': data})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/excel/save_config', methods=['POST'])
@login_required
def save_excel_config():
    data = request.json
    db = get_db()
    
    chat_id = int(data['chat_id'])
    sheet_name = data['sheet_name']
    is_grouped = data.get('is_grouped', 0)
    sku_col = data.get('sku_col', -1)
    
    db.execute("""
        INSERT INTO excel_configs (user_id, chat_id, sheet_name, name_col, name_row_offset, price_col, price_row_offset, block_step, start_row, is_grouped, sku_col) 
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(chat_id, sheet_name) DO UPDATE SET 
        name_col=excluded.name_col, name_row_offset=excluded.name_row_offset, price_col=excluded.price_col, 
        price_row_offset=excluded.price_row_offset, block_step=excluded.block_step, start_row=excluded.start_row,
        is_grouped=excluded.is_grouped, sku_col=excluded.sku_col
    """, (session['user_id'], chat_id, sheet_name, data['name_col'], data['name_row_offset'], data['price_col'], data['price_row_offset'], data['block_step'], data['start_row'], is_grouped, sku_col))

    cid_str = str(chat_id)
    core_id = cid_str[4:] if cid_str.startswith('-100') else (cid_str[1:] if cid_str.startswith('-') else cid_str)
    possible_ids = [core_id, f"-{core_id}", f"-100{core_id}"]
    
    for pid in possible_ids:
        if sheet_name == '*':
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND user_id = ?", (pid, session['user_id']))
        else:
            db.execute("DELETE FROM excel_missing_sheets WHERE chat_id = ? AND sheet_name = ? AND user_id = ?", (pid, sheet_name, session['user_id']))
    
    db.commit()
    return jsonify({'success': True})



@app.route('/api/excel/configs', methods=['GET'])
@login_required
def get_excel_configs():
    db = get_db()
    # Используем GROUP_CONCAT для объединения названий листов в одну строку
    query = """
        SELECT chat_id, source_type, sheet_url, 
               GROUP_CONCAT(sheet_name, ', ') as sheet_names,
               MAX(id) as id
        FROM excel_configs 
        WHERE user_id = ?
        GROUP BY chat_id, source_type, sheet_url
    """
    configs = db.execute(query, (session['user_id'],)).fetchall()
    return jsonify([dict(c) for c in configs])

@app.route('/api/excel/configs/<int:config_id>', methods=['DELETE'])
@login_required
def delete_excel_config(config_id):
    db = get_db()
    db.execute("DELETE FROM excel_configs WHERE id = ? AND user_id = ?", (config_id, session['user_id']))
    db.commit()
    return jsonify({'success': True})


import sqlite3

def update_database():
    conn = sqlite3.connect(app.config['DATABASE'])
    cursor = conn.cursor()
    
    print("Начинаю проверку базы данных...")

    # --- ДОБАВЛЯЕМ КОЛОНКУ IS_DELAYED ---
    try:
        cursor.execute("ALTER TABLE messages ADD COLUMN is_delayed INTEGER DEFAULT 0")
        print("✅ Колонка is_delayed успешно добавлена в таблицу messages!")
    except sqlite3.OperationalError:
        pass

    # Полный список всех колонок для работы парсера (Google, PDF, Excel)
    columns_to_add = {
        "source_type": "TEXT DEFAULT 'excel'",
        "sheet_url": "TEXT",
        "parse_interval": "INTEGER DEFAULT 60",
        "is_grouped": "INTEGER DEFAULT 0",
        "sku_col": "INTEGER DEFAULT -1",
        "name_col": "INTEGER DEFAULT -1",
        "name_row_offset": "INTEGER DEFAULT 0",
        "price_row_offset": "INTEGER DEFAULT 0",
        "block_step": "INTEGER DEFAULT 1",
        "sheet_name": "TEXT DEFAULT '*'"
    }

    for col, col_type in columns_to_add.items():
        try:
            cursor.execute(f"ALTER TABLE excel_configs ADD COLUMN {col} {col_type}")
            print(f"✅ Колонка {col} успешно добавлена.")
        except sqlite3.OperationalError:
            print(f"⏩ Колонка {col} уже существует, пропускаем.")

    # Устанавливаем тип 'excel' для всех записей, где он еще не задан
    cursor.execute("UPDATE excel_configs SET source_type = 'excel' WHERE source_type IS NULL OR source_type = ''")

    # --- ДОБАВЛЯЕМ КОЛОНКИ FOLDER_ID ---
    try:
        cursor.execute("ALTER TABLE target_chats ADD COLUMN folder_id INTEGER;")
        print("✅ Колонка folder_id добавлена в таблицу target_chats")
    except sqlite3.OperationalError:
        pass

    try:
        cursor.execute("ALTER TABLE excel_configs ADD COLUMN folder_id INTEGER;")
        print("✅ Колонка folder_id добавлена в таблицу excel_configs")
    except sqlite3.OperationalError:
        pass

    # Фиксируем все изменения ОДИН РАЗ и закрываем базу
    conn.commit()
    conn.close()
    
    print("✅ Данные обновлены, старые конфиги теперь помечены как excel")
    print("🎉 Полное обновление базы данных завершено!")




@app.route('/api/fix_delayed')
def fix_delayed():
    try:
        db = get_db()
        db.execute("ALTER TABLE messages ADD COLUMN is_delayed INTEGER DEFAULT 0")
        db.commit()
        return "<h1>✅ База данных успешно обновлена! Колонка is_delayed добавлена.</h1>"
    except Exception as e:
        return f"<h1>Уже добавлена или произошла ошибка:</h1><p>{e}</p>"

from datetime import datetime

def is_parsing_allowed(session_id):
    """
    Проверяет, попадает ли текущее время в разрешенный интервал тайминга.
    """
    try:
        with app.app_context():
            db = get_db()
            # Добавили проверку schedule_enabled
            row = db.execute("SELECT schedule_enabled, time_start, time_end FROM user_telegram_sessions WHERE id = ?", (session_id,)).fetchone()
            
            # Если галочка "Расписание" не стоит или данных нет — разрешаем 24/7
            if not row or not row['schedule_enabled']:
                return True 
            
            # Сдвигаем время сервера на +3 часа (Москва).
            current_time = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3)).time()
            
            start_time = datetime.strptime(row['time_start'], '%H:%M').time()
            end_time = datetime.strptime(row['time_end'], '%H:%M').time()

            if start_time <= end_time:
                return start_time <= current_time <= end_time
            else:
                return current_time >= start_time or current_time <= end_time

    except Exception as e:
        logger.error(f"Ошибка проверки тайминга для сессии {session_id}: {e}")
        return True

def delayed_messages_scheduler():
    while True:
        try:
            with app.app_context():
                db = get_db()
                # Проверяем все активные аккаунты
                cursor = db.execute("SELECT id as session_id, user_id FROM user_telegram_sessions WHERE status = 'active'")
                for session_row in cursor.fetchall():
                    session_id = session_row['session_id']
                    user_id = session_row['user_id']
                    
                    # Если для этого аккаунта СЕЙЧАС рабочее время
                    if is_parsing_allowed(session_id):
                        # Достаем все его скрытые сообщения
                        msg_cursor = db.execute("SELECT * FROM messages WHERE is_delayed = 1 AND user_id = ? ORDER BY date ASC", (user_id,))
                        delayed_messages = msg_cursor.fetchall()
                        
                        for msg in delayed_messages:
                            msg_id = msg['id']
                            parsed_text = msg['text']
                            chat_id = msg['chat_id']
                            
                            # 1. Снимаем флаг 'скрыто'
                            db.execute("UPDATE messages SET is_delayed = 0 WHERE id = ?", (msg_id,))
                            
                            # 2. ЗАПУСКАЕМ ОБРАБОТКУ (миграция привязок к товарам)
                            old_bindings = db.execute("""
                                SELECT pm.id, pm.line_index, p.synonyms
                                FROM product_messages pm
                                JOIN products p ON pm.product_id = p.id
                                JOIN messages m ON pm.message_id = m.id
                                WHERE m.chat_id = ? AND m.user_id = ? AND m.id != ?
                            """, (chat_id, user_id, msg_id)).fetchall()

                            if old_bindings and parsed_text:
                                lines = parsed_text.split('\n')
                                text_lower = parsed_text.lower()
                                
                                for b in old_bindings:
                                    synonyms = [s.strip().lower() for s in (b['synonyms'] or '').split(',') if s.strip()]
                                    line_idx = b['line_index']
                                    match_found = False
                                    new_price = None
                                    
                                    if line_idx == -1:
                                        if any(syn in text_lower for syn in synonyms):
                                            match_found = True
                                            new_price = extract_price(parsed_text)
                                    else:
                                        for i, line in enumerate(lines):
                                            if any(syn in line.lower() for syn in synonyms):
                                                match_found = True
                                                new_price = extract_price(line)
                                                if not new_price and i + 1 < len(lines):
                                                    new_price = extract_price(lines[i+1])
                                                if not new_price and i + 2 < len(lines):
                                                    new_price = extract_price(lines[i+2])
                                                line_idx = i
                                                break
                                     
                                    if match_found:
                                        db.execute("""
                                            UPDATE product_messages 
                                            SET message_id = ?, line_index = ?, extracted_price = ?, is_actual = 1 
                                            WHERE id = ?
                                        """, (msg_id, line_idx, new_price, b['id']))
                            
                            # Очистка старых сообщений (зазор 5 минут)
                            cutoff_time = (datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(hours=3) - timedelta(minutes=5)).strftime("%Y-%m-%d %H:%M:%S")
                            
                            # 1. Помечаем товары как неактуальные
                            db.execute("""
                                UPDATE product_messages 
                                SET is_actual = 0 
                                WHERE message_id IN (
                                    SELECT id FROM messages 
                                    WHERE chat_id = ? AND user_id = ? AND id != ? AND date < ?
                                )
                            """, (chat_id, user_id, msg_id, cutoff_time))
                            
                            # 2. Удаляем старые сообщения
                            db.execute("""
                                DELETE FROM messages 
                                WHERE chat_id = ? AND user_id = ? AND id != ? AND date < ?
                                  AND id NOT IN (SELECT message_id FROM product_messages WHERE is_actual = 1)
                            """, (chat_id, user_id, msg_id, cutoff_time))

                            db.commit()
                            
                            # 3. Отправляем уведомление на фронтенд
                            notify_clients()
                            
        except Exception as e:
            print(f"Ошибка в планировщике отложенных сообщений: {e}")
            
        time.sleep(60) # Проверяем тайминги каждую минуту


def process_pending_ai_repair_jobs():
    with app.app_context():
        db = get_db()
        jobs = db.execute(
            """
            SELECT *
            FROM ai_repair_jobs
            WHERE status IN ('pending', 'retry_wait')
              AND next_retry_at <= CURRENT_TIMESTAMP
            ORDER BY created_at
            LIMIT ?
            """,
            (AI_REPAIR_JOB_BATCH_SIZE,),
        ).fetchall()

        for job in jobs:
            db.execute(
                "UPDATE ai_repair_jobs SET status = 'processing', updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (job["id"],),
            )
            db.commit()

            product_row = db.execute(
                "SELECT * FROM products WHERE id = ? AND user_id = ?",
                (job["product_id"], job["user_id"]),
            ).fetchone()
            if not product_row:
                db.execute(
                    """
                    UPDATE ai_repair_jobs
                    SET status = 'failed', last_error = ?, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    ("Товар удален или недоступен", job["id"]),
                )
                db.commit()
                continue

            product_name = str(job["product_name"] or product_row["name"] or "").strip()
            article = str(job["article"] or product_row["model_number"] or "").strip()
            preferred_folder_id = normalize_folder_id(job["preferred_folder_id"])

            try:
                payload, _ = get_or_generate_autofill_payload(
                    db,
                    job["user_id"],
                    product_name,
                    article,
                    use_cache=True,
                )
                upsert_product_from_autofill(
                    db,
                    job["user_id"],
                    product_row,
                    payload,
                    preferred_folder_id if preferred_folder_id is not None else product_row["folder_id"],
                )
                db.execute(
                    """
                    UPDATE ai_repair_jobs
                    SET status = 'completed', attempts = attempts + 1, last_error = NULL, updated_at = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (job["id"],),
                )
                db.commit()
                notify_clients()
            except Exception as e:
                attempts = int(job["attempts"] or 0) + 1
                if is_retryable_ai_error(e):
                    retry_delay_seconds = compute_ai_retry_delay_seconds(attempts)
                    db.execute(
                        """
                        UPDATE ai_repair_jobs
                        SET status = 'retry_wait',
                            attempts = ?,
                            last_error = ?,
                            next_retry_at = datetime('now', '+' || ? || ' seconds'),
                            updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (attempts, str(e)[:1000], retry_delay_seconds, job["id"]),
                    )
                else:
                    db.execute(
                        """
                        UPDATE ai_repair_jobs
                        SET status = 'failed', attempts = ?, last_error = ?, updated_at = CURRENT_TIMESTAMP
                        WHERE id = ?
                        """,
                        (attempts, str(e)[:1000], job["id"]),
                    )
                db.commit()


def ai_repair_scheduler():
    while True:
        try:
            process_pending_ai_repair_jobs()
        except Exception as e:
            logger.error(f"Ошибка AI-планировщика исправлений: {e}")
        time.sleep(60)


def warm_product_search_cache():
    try:
        with app.app_context():
            db = get_db()
            rows = db.execute("SELECT id FROM users ORDER BY id").fetchall()
            for row in rows:
                get_cached_product_search_entries(db, int(row['id']))
            logger.info("Product search cache warmed")
    except Exception as e:
        logger.error(f"Ошибка прогрева кеша поиска товаров: {e}")


_background_schedulers_started = False


def start_background_schedulers():
    global _background_schedulers_started
    if _background_schedulers_started:
        return
    threading.Thread(target=publish_scheduler, daemon=True).start()
    threading.Thread(target=api_parser_scheduler, daemon=True).start()
    threading.Thread(target=delayed_messages_scheduler, daemon=True).start()
    threading.Thread(target=ai_repair_scheduler, daemon=True).start()
    threading.Thread(target=bot_interaction_scheduler, daemon=True).start()
    threading.Thread(target=warm_product_search_cache, daemon=True).start()
    _background_schedulers_started = True

# ---------- Запуск приложения ----------

if __name__ == '__main__':
    update_database()
    start_background_schedulers()
    
    with app.app_context():
        db = get_db()
        cursor = db.execute("SELECT id, user_id, api_id, api_hash FROM user_telegram_sessions WHERE status = 'active'")
        for row in cursor.fetchall():
            start_message_listener(row['id'], row['user_id'], row['api_id'], row['api_hash'])
    port = int(os.environ.get('PORT', '5001'))
    socketio.run(app, debug=True, host='0.0.0.0', port=port, use_reloader=False, allow_unsafe_werkzeug=True)
