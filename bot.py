import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, MessageHandler, filters,
    CallbackQueryHandler, ContextTypes, ConversationHandler
)
import html
import json
import sqlite3
import logging
import os
import re
import redis
from typing import List, Dict, Tuple, Optional, Any, Set
import asyncio # Import asyncio for potential sleep
import uuid # For generating unique IDs for approved hadiths
from datetime import datetime # For submission timestamp

# --- Configuration ---
BOT_TOKEN = "7378891608:AAFUPueUuSAPHd4BPN8znb-jcDGsjnnm_f8"  # Token المستخدم
JSON_FILE = '1.json'     # اسم ملف الأحاديث JSON
DB_NAME = 'hadith_bot.db'      # اسم ملف قاعدة البيانات SQLite
DEVELOPER_NAME = "عبد المجيد" # اسم المطور
MAX_MESSAGE_LENGTH = 4000      # الحد الأقصى لطول رسالة تليجرام
SNIPPET_CONTEXT_WORDS = 5      # Number of words before/after keyword in snippet
BOT_OWNER_ID = 6504095190       # !!! معرف المستخدم الخاص بالمالك !!! (تم وضع معرفك كمثال)

# --- Redis Configuration ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRY_SECONDS = 55555555555555    # مدة صلاحية الكاش (مثال: ساعة واحدة)

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.DEBUG # Keep DEBUG for detailed logs if needed
)
logger = logging.getLogger(__name__)

# --- Redis Connection Pool ---
redis_pool = None
try:
    redis_pool = redis.ConnectionPool(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
    logger.info(f"Redis connection pool created for {REDIS_HOST}:{REDIS_PORT}")
except Exception as e:
    logger.error(f"Failed to create Redis connection pool: {e}")
    redis_pool = None

def get_redis_connection() -> Optional[redis.Redis]:
    """Gets a Redis connection from the pool."""
    # Unchanged
    if redis_pool:
        try:
            r = redis.Redis(connection_pool=redis_pool)
            r.ping()
            return r
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error: {e}")
            return None
        except Exception as e:
             logger.error(f"Failed to get Redis connection: {e}")
             return None
    return None

# --- Database Functions ---

def get_db_connection() -> sqlite3.Connection:
    # Unchanged
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    """Initializes the database, creating necessary tables."""
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('search_count', 0))
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('user_count', 0))
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('start_usage', 0))
        cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY)")
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts USING fts5(
                original_id,
                book UNINDEXED,
                arabic_text,
                grading UNINDEXED
            );
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS pending_hadiths (
                submission_id INTEGER PRIMARY KEY AUTOINCREMENT,
                submitter_id INTEGER NOT NULL,
                submitter_username TEXT,
                book TEXT NOT NULL,
                arabic_text TEXT NOT NULL,
                grading TEXT,
                submission_time DATETIME DEFAULT CURRENT_TIMESTAMP,
                approval_message_id INTEGER NULL
            );
        ''')
        conn.commit()
        conn.close()
        logger.info("Database initialized successfully (including pending_hadiths table).")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise

def populate_db_from_json(filename: str):
    # Unchanged
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM hadiths_fts")
        count = cursor.fetchone()[0]
        if count == 0:
            logger.info("FTS Hadiths table is empty. Populating from JSON file...")
            try:
                with open(filename, 'r', encoding='utf-8') as f: data = json.load(f)
                added_count = 0
                for hadith in data:
                    text = hadith.get('arabicText', '')
                    cleaned_text = re.sub(r"^\s*\d+[\sـ.-]*", "", text).strip()
                    original_id_str = str(hadith.get('id', f'gen_{added_count}'))
                    if not cleaned_text: continue
                    cursor.execute(
                        "INSERT INTO hadiths_fts (original_id, book, arabic_text, grading) VALUES (?, ?, ?, ?)",
                        (original_id_str, hadith.get('book'), cleaned_text, hadith.get('majlisiGrading'))
                    )
                    added_count += 1
                conn.commit()
                logger.info(f"Successfully added {added_count} hadiths to the FTS database.")
            except FileNotFoundError: logger.error(f"Error: JSON file '{filename}' not found during population.")
            except json.JSONDecodeError: logger.error(f"Error: Could not decode JSON from '{filename}'. Check file format.")
            except Exception as e: logger.error(f"An unexpected error occurred loading hadiths from JSON: {e}")
        else: logger.info("FTS Hadiths table already populated.")
    except sqlite3.Error as e:
        logger.error(f"Database error during FTS population check/insert: {e}")
    finally: conn.close()


def update_stats(key: str, increment: int = 1):
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE stats SET value = value + ? WHERE key = ?", (increment, key))
        conn.commit()
        conn.close()
    except sqlite3.Error as e: logger.error(f"Error updating stats for key '{key}': {e}")

def get_stat(key: str) -> int:
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT value FROM stats WHERE key = ?", (key,))
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logger.error(f"Error getting stat for key '{key}': {e}")
        return 0

def get_total_hadiths_count() -> int:
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(DISTINCT original_id) FROM hadiths_fts")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logger.error(f"Error getting distinct hadiths count from FTS: {e}")
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM hadiths_fts")
            result = cursor.fetchone()
            conn.close()
            return result[0] if result else 0
        except sqlite3.Error as e2:
            logger.error(f"Fallback error getting total hadiths count: {e2}")
            return 0


def log_user(user_id: int):
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.execute("UPDATE stats SET value = ? WHERE key = ?", (user_count, 'user_count'))
        conn.commit()
        conn.close()
    except sqlite3.Error as e: logger.error(f"Error logging user {user_id}: {e}")

# --- Helper Functions ---

def arabic_number_to_word(n: int) -> str:
    """Converts an integer (1-20+) to its Arabic word representation."""
    # Unchanged
    if not isinstance(n, int) or n <= 0: return str(n)
    words = { 1: "الأول", 2: "الثاني", 3: "الثالث", 4: "الرابع", 5: "الخامس", 6: "السادس", 7: "السابع", 8: "الثامن", 9: "التاسع", 10: "العاشر", 11: "الحادي عشر", 12: "الثاني عشر", 13: "الثالث عشر", 14: "الرابع عشر", 15: "الخامس عشر", 16: "السادس عشر", 17: "السابع عشر", 18: "الثامن عشر", 19: "التاسع عشر", 20: "العشرون" }
    if n > 20: return f"الـ {n}"
    return words.get(n, str(n))

def split_message(text: str) -> List[str]:
    """Splits a long message into parts respecting MAX_MESSAGE_LENGTH."""
    # Unchanged
    parts = []
    if not text: return []
    while len(text) > MAX_MESSAGE_LENGTH:
        split_pos = text.rfind('\n', 0, MAX_MESSAGE_LENGTH)
        if split_pos == -1: split_pos = text.rfind(' ', 0, MAX_MESSAGE_LENGTH)
        if split_pos == -1 or split_pos == 0: split_pos = MAX_MESSAGE_LENGTH
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    parts.append(text)
    return parts

def search_hadiths_db(query: str) -> List[int]:
    """Searches hadiths using FTS, prefixes, deduplication."""
    # Unchanged (Reverted fuzzy search)
    if not query: return []
    normalized_query = query.strip().lower()
    cache_key = f"hadith_search_unique:{normalized_query}"
    redis_conn = get_redis_connection()
    cached_result = None
    if redis_conn:
        try:
            cached_data = redis_conn.get(cache_key)
            if cached_data:
                cached_result = json.loads(cached_data.decode('utf-8'))
                if isinstance(cached_result, list):
                    logger.info(f"Cache HIT for unique query '{query}'. Found {len(cached_result)} results in Redis.")
                    return cached_result
                else:
                    logger.warning(f"Invalid data type in cache for key '{cache_key}'. Ignoring cache.")
                    redis_conn.delete(cache_key)
            else: logger.info(f"Cache MISS for unique query '{query}'.")
        except json.JSONDecodeError:
            logger.error(f"Error decoding cached JSON for key '{cache_key}'. Ignoring cache.")
            if redis_conn: redis_conn.delete(cache_key)
        except redis.exceptions.RedisError as e: logger.error(f"Redis error getting cache for key '{cache_key}': {e}")
        except Exception as e: logger.error(f"Unexpected error during Redis cache get: {e}")

    logger.info(f"Searching SQLite FTS for query '{query}' with prefixes and deduplication.")
    conn = get_db_connection()
    cursor = conn.cursor()
    unique_rowids = []
    seen_original_ids: Set[str] = set()
    try:
        prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
        fts_query_parts = [f'"{query}"']
        for p in prefixes: fts_query_parts.append(f'"{p}{query}"')
        fts_match_query = " OR ".join(fts_query_parts)
        logger.debug(f"Constructed FTS MATCH query: {fts_match_query}")
        cursor.execute("SELECT rowid, original_id FROM hadiths_fts WHERE hadiths_fts MATCH ? ORDER BY rank", (fts_match_query,))
        results = cursor.fetchall()
        logger.info(f"Raw FTS Search for '{query}' found {len(results)} potential matches.")
        logger.debug(f"Starting deduplication for query '{query}'.")
        for row in results:
            rowid = row['rowid']; original_id = row['original_id']
            logger.debug(f"  Processing rowid: {rowid}, original_id: '{original_id}' (type: {type(original_id)})")
            if original_id is None: logger.warning(f"  Skipping rowid {rowid} due to None original_id."); continue
            original_id_str = str(original_id)
            if original_id_str not in seen_original_ids:
                logger.debug(f"    -> Adding rowid {rowid} (new original_id: '{original_id_str}')")
                seen_original_ids.add(original_id_str); unique_rowids.append(rowid)
            else: logger.debug(f"    -> Skipping rowid {rowid} (duplicate original_id: '{original_id_str}')")
        logger.debug(f"Finished deduplication. Seen IDs count: {len(seen_original_ids)}. Unique rowids: {len(unique_rowids)}")
        if unique_rowids and redis_conn:
            try:
                serialized_results = json.dumps(unique_rowids)
                redis_conn.set(cache_key, serialized_results, ex=CACHE_EXPIRY_SECONDS)
                logger.info(f"Cached {len(unique_rowids)} unique results for query '{query}' in Redis.")
            except redis.exceptions.RedisError as e: logger.error(f"Redis error setting cache for key '{cache_key}': {e}")
            except Exception as e: logger.error(f"Unexpected error during Redis cache set: {e}")
    except sqlite3.Error as e:
        if "malformed MATCH expression" in str(e): logger.warning(f"FTS query syntax error for query '{query}' (constructed: {fts_match_query}): {e}")
        else: logger.error(f"Database FTS search error for query '{query}': {e}")
    finally: conn.close()
    return unique_rowids

def get_hadith_details_by_db_id(row_id: int) -> Optional[sqlite3.Row]:
    # Unchanged
    conn = get_db_connection()
    cursor = conn.cursor()
    hadith_details = None
    try: cursor.execute("SELECT rowid, original_id, book, arabic_text, grading FROM hadiths_fts WHERE rowid = ?", (row_id,)); hadith_details = cursor.fetchone()
    except sqlite3.Error as e: logger.error(f"Error fetching hadith details for rowid {row_id}: {e}")
    finally: conn.close()
    return hadith_details

# --- Add Hadith Conversation States ---
ASK_BOOK, ASK_TEXT, ASK_GRADING = range(3)

# --- Add Hadith Feature Functions ---

async def add_hadith_start(update: Update, context: ContextTypes.DEFAULT_TYPE, is_button: bool = False) -> int:
    """Starts the conversation to add a new hadith."""
    # Unchanged
    reply_target = update.message if not is_button else update.callback_query.message
    user_id = update.effective_user.id
    logger.info(f"User {user_id} started 'add hadith' process (is_button={is_button}).")
    await reply_target.reply_text(
        "أهلاً بك في خدمة إضافة حديث جديد.\n"
        "الرجاء إرسال <b>اسم الكتاب</b> أولاً.\n\n"
        "لإلغاء العملية في أي وقت، أرسل /cancel.",
        parse_mode='HTML'
    )
    return ASK_BOOK

async def add_hadith_start_button(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles the 'Add Hadith' button press from the start menu."""
    # Unchanged
    query = update.callback_query
    await query.answer()
    logger.info(f"User {query.from_user.id} initiated add hadith via button.")
    return await add_hadith_start(update, context, is_button=True)

async def receive_book(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the book name and asks for the hadith text."""
    # Unchanged
    book_name = update.message.text.strip()
    if not book_name:
        await update.message.reply_text("لم يتم إرسال اسم الكتاب. الرجاء إرسال اسم الكتاب.")
        return ASK_BOOK
    context.user_data['new_hadith_book'] = book_name
    logger.info(f"Received book name: {book_name}")
    await update.message.reply_text("شكرًا لك. الآن الرجاء إرسال <b>نص الحديث</b> كاملاً.", parse_mode='HTML')
    return ASK_TEXT

async def receive_text(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the hadith text and asks for the grading."""
    # Unchanged
    hadith_text = update.message.text.strip()
    if not hadith_text:
        await update.message.reply_text("لم يتم إرسال نص الحديث. الرجاء إرسال نص الحديث.")
        return ASK_TEXT
    context.user_data['new_hadith_text'] = hadith_text
    logger.info(f"Received hadith text (length {len(hadith_text)}).")
    await update.message.reply_text("ممتاز. أخيراً، الرجاء إرسال <b> صحة الحديث</b> (إن وجدت، أو اضغط /skip للتخطي).", parse_mode='HTML')
    return ASK_GRADING

async def receive_grading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Stores the grading, saves to pending, notifies owner, and ends conversation."""
    # Unchanged
    grading = update.message.text.strip()
    context.user_data['new_hadith_grading'] = grading
    logger.info(f"Received grading: {grading}")
    await save_and_notify_owner(update, context)
    return ConversationHandler.END

async def skip_grading(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Handles skipping the grading step."""
    # Unchanged
    context.user_data['new_hadith_grading'] = None
    logger.info("Grading step skipped.")
    await update.message.reply_text("تم تخطي تصنيف الحديث .")
    await save_and_notify_owner(update, context)
    return ConversationHandler.END

async def save_and_notify_owner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Saves the pending hadith to DB and notifies the owner."""
    # Unchanged (Includes updated confirmation message)
    user = update.effective_user
    book = context.user_data.get('new_hadith_book')
    text = context.user_data.get('new_hadith_text')
    grading = context.user_data.get('new_hadith_grading', 'لم يحدد')

    if not book or not text:
        logger.error("Missing book or text when trying to save pending hadith.")
        await update.message.reply_text("حدث خطأ. بعض المعلومات ناقصة. يرجى المحاولة مرة أخرى باستخدام /addhadith.")
        context.user_data.clear(); return

    submission_id = None
    try:
        conn = get_db_connection(); cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO pending_hadiths (submitter_id, submitter_username, book, arabic_text, grading) VALUES (?, ?, ?, ?, ?)",
            (user.id, user.username or 'N/A', book, text, grading)
        )
        submission_id = cursor.lastrowid; conn.commit(); conn.close()
        logger.info(f"Saved pending hadith with submission_id: {submission_id}")
        await update.message.reply_text("شكراً لك، سيتم مراجعة الحديث والموافقة عليه قريباً إن شاء الله.")
    except sqlite3.Error as e: logger.error(f"Database error saving pending hadith: {e}"); await update.message.reply_text("حدث خطأ أثناء حفظ الحديث للمراجعة. يرجى المحاولة لاحقاً."); context.user_data.clear(); return
    except Exception as e: logger.error(f"Unexpected error saving pending hadith: {e}"); await update.message.reply_text("حدث خطأ غير متوقع. يرجى المحاولة لاحقاً."); context.user_data.clear(); return

    if submission_id and BOT_OWNER_ID:
        try:
            owner_message_text = f"""<b>مراجعة حديث جديد</b> ⏳
<b>المُرسِل:</b> {user.mention_html()} (ID: <code>{user.id}</code>)
<b>الوقت:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
---
📖 <b>الكتاب:</b> {html.escape(book)}
---
📜 <b>الحديث:</b>
{html.escape(text[:1000])}{'...' if len(text) > 1000 else ''}
---
⚖️ <b>الصحة:</b> {html.escape(grading if grading else 'لم يحدد')}
---
<b>Submission ID:</b> <code>{submission_id}</code>"""
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("✅ موافقة", callback_data=f"approve_{submission_id}"), InlineKeyboardButton("❌ رفض", callback_data=f"reject_{submission_id}")]])
            sent_message = await context.bot.send_message(chat_id=BOT_OWNER_ID, text=owner_message_text, parse_mode='HTML', reply_markup=keyboard)
            conn = get_db_connection(); cursor = conn.cursor()
            cursor.execute("UPDATE pending_hadiths SET approval_message_id = ? WHERE submission_id = ?", (sent_message.message_id, submission_id))
            conn.commit(); conn.close()
        except telegram.error.TelegramError as e: logger.error(f"Failed to send notification to owner (ID: {BOT_OWNER_ID}): {e}")
        except Exception as e: logger.error(f"Unexpected error notifying owner: {e}")
    context.user_data.clear()


async def cancel_submission(update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
    """Cancels the conversation."""
    # Unchanged
    context.user_data.clear()
    await update.message.reply_text("تم إلغاء عملية إضافة الرواية.")
    return ConversationHandler.END

# --- Approval Callback Handler ---

async def handle_approval_callback(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Handles the owner's approval or rejection."""
    # Unchanged
    query = update.callback_query; await query.answer(); data = query.data; user_id = query.from_user.id
    if user_id != BOT_OWNER_ID: await context.bot.send_message(chat_id=user_id, text="ليس لديك الصلاحية للموافقة أو الرفض."); logger.warning(f"Unauthorized attempt to handle approval by user {user_id} for data {data}"); return
    logger.info(f"Approval callback received from owner: {data}")
    try: action, submission_id_str = data.split('_', 1); submission_id = int(submission_id_str)
    except (ValueError, IndexError) as e: logger.error(f"Error parsing approval callback data '{data}': {e}"); await query.edit_message_text(text="خطأ في بيانات الزر."); return

    conn = get_db_connection(); cursor = conn.cursor()
    try:
        cursor.execute("SELECT submitter_id, book, arabic_text, grading FROM pending_hadiths WHERE submission_id = ?", (submission_id,))
        pending_hadith = cursor.fetchone()
        if not pending_hadith: logger.warning(f"Pending hadith with submission_id {submission_id} not found for action '{action}'."); await query.edit_message_text(text=f"لم يتم العثور على الطلب رقم {submission_id} (ربما تمت معالجته مسبقاً)."); return
        submitter_id = pending_hadith['submitter_id']; book = pending_hadith['book']; text = pending_hadith['arabic_text']; grading = pending_hadith['grading']
        if action == "approve":
            try:
                new_original_id = str(uuid.uuid4())
                cursor.execute("INSERT INTO hadiths_fts (original_id, book, arabic_text, grading) VALUES (?, ?, ?, ?)", (new_original_id, book, text, grading))
                cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,))
                conn.commit(); logger.info(f"Approved and added hadith from submission {submission_id} with new ID {new_original_id}")
                await query.edit_message_text(text=f"✅ تمت الموافقة على الرواية رقم {submission_id} وإضافته بنجاح.\n\n{query.message.text}", parse_mode='HTML')
                await context.bot.send_message(chat_id=submitter_id, text=f"🎉 تمت الموافقة على الحديث الذي أرسلته حول '{book[:30]}...' وتمت إضافته لقاعدة البيانات!")
            except sqlite3.Error as e: conn.rollback(); logger.error(f"Database error approving submission {submission_id}: {e}"); await query.edit_message_text(text=f"⚠️ حدث خطأ في قاعدة البيانات أثناء الموافقة على الطلب {submission_id}.")
            except Exception as e: conn.rollback(); logger.error(f"Unexpected error approving submission {submission_id}: {e}"); await query.edit_message_text(text=f"⚠️ حدث خطأ غير متوقع أثناء الموافقة على الطلب {submission_id}.")
        elif action == "reject":
            try:
                cursor.execute("DELETE FROM pending_hadiths WHERE submission_id = ?", (submission_id,))
                conn.commit(); logger.info(f"Rejected submission {submission_id}")
                await query.edit_message_text(text=f"❌ تم رفض الطلب رقم {submission_id}.\n\n{query.message.text}", parse_mode='HTML')
                await context.bot.send_message(chat_id=submitter_id, text=f"ℹ️ نعتذر، لم تتم الموافقة على الحديث الذي أرسلته حول '{book[:30]}...' في الوقت الحالي.")
            except sqlite3.Error as e: conn.rollback(); logger.error(f"Database error rejecting submission {submission_id}: {e}"); await query.edit_message_text(text=f"⚠️ حدث خطأ في قاعدة البيانات أثناء رفض الطلب {submission_id}.")
            except Exception as e: conn.rollback(); logger.error(f"Unexpected error rejecting submission {submission_id}: {e}"); await query.edit_message_text(text=f"⚠️ حدث خطأ غير متوقع أثناء رفض الطلب {submission_id}.")
    finally: conn.close()


# --- Bot Command Handlers (start, help_command) ---

async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command with a detailed welcome message and buttons."""
    # Unchanged
    user = update.effective_user
    log_user(user.id)
    update_stats('start_usage')
    keyboard = [
        [InlineKeyboardButton("➕ أضفني إلى مجموعتك", url=f"https://t.me/{context.bot.username}?startgroup=true")],
        [InlineKeyboardButton("➕ إضافة حديث", callback_data="start_add_hadith")], # Button added
        [InlineKeyboardButton("📢 قناة البوت", url="https://t.me/shia_b0t")]
    ]
    user_name = html.escape(user.first_name)
    welcome_message = f"""
    <b>مرحبا {user_name}!
    أنا بوت كاشف أحاديث الشيعة في قاعدة بياناتي اكثر من 26152 رواية تستطيع اضافة اي رواية لقاعدة بياناتي من اجل ان افيد اهل السنة في حوار الشيعة  🔍</b>

    <i> الكتب في قاعدة البيانات:</i>
    - كتاب الكافي للكليني مع التصحيح من مراة العقول للمجلسي
    - جميع الاحاديث الموجودة في عيون اخبار الرضا للصدوق
    - كتاب نهج البلاغة
    - كتاب الخصال للصدوق
    - وسيتم اضافة باقي كتب الشيعة
    - كتاب الامالي للصدوق
    - كتاب الامالي للمفيد
    - كتاب التوحيد للصدوق
    - كتاب فضائل الشيعة للصدوق
    - كتاب كامل الزيارات لابن قولويه القمي
    - كتاب الضعفاء لابن الغضائري
    - كتاب الغيبة للنعماني
    - كتاب الغيبة للطوسي
    - كتاب المؤمن لحسين بن سعيد الكوفي الاهوازي
    - كتاب الزهد لحسين بن سعيد الكوفي الاهوازي
    - كتاب معاني الاخبار للصدوق
    - كتاب معجم الاحاديث المعتبرة لمحمد اصف محسني
    - كتاب نهج البلاغة لعلي بن ابي طالب
    - كتاب رسالة الحقوق للامام زين العابدين

    <b>طريقة الاستخدام:</b>
    <code>شيعة [جزء من النص]</code>

    <b>مثال:</b>
    <code>شيعة باهتوهم</code>

    يمكنك أيضاً إضافة حديث جديد باستخدام الأمر /addhadith أو الزر أدناه.

    ادعو لوالدي بالرحمة بارك الله فيكم ان استفدتم من هذا العمل
    """
    await update.message.reply_html(welcome_message, reply_markup=InlineKeyboardMarkup(keyboard))


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /help command, showing stats and adding a developer button."""
    # Unchanged
    log_user(update.effective_user.id)
    total_hadiths = get_total_hadiths_count()
    search_count = get_stat('search_count')
    user_count = get_stat('user_count')
    start_usage_count = get_stat('start_usage')
    help_text = f"""
    <b>مساعدة وإحصائيات بوت الأحاديث</b> 

    📊 <b>الإحصائيات:</b>
    - عدد الأحاديث في قاعدة البيانات: {total_hadiths}
    - إجمالي عمليات البحث: {search_count}
    - عدد المستخدمين : {user_count}

    🔍 <b>كيفية البحث:</b>
    أرسل رسالة تبدأ بـ <code>شيعة</code> أو <code>شيعه</code> ثم مسافة ثم الكلمة أو الجملة التي تريد البحث عنها.
    مثال: <code>شيعه باهتوهم   </code>

    ➕ <b>إضافة حديث:</b>
    استخدم الأمر /addhadith أو النص "اضافة حديث" لبدء عملية إضافة حديث جديد للمراجعة.
    """
    developer_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(" المطور: عبد المجيد", url="https://t.me/j_dd_j")]])
    await update.message.reply_html(help_text, reply_markup=developer_keyboard, disable_web_page_preview=True)


# --- Message Handler ---

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming messages starting with 'شيعة' or 'شيعه' followed by search terms."""
    # Unchanged (using reverted search logic)
    user_id = update.effective_user.id
    message_text = update.message.text.strip()
    match = re.match(r'^(شيعة|شيعه)\s+(.+)', message_text, re.IGNORECASE | re.UNICODE)
    if not match:
        if message_text == "اضافة حديث":
             logger.debug(f"Ignoring 'اضافة حديث' text in handle_search, letting ConversationHandler take over.")
             return
        elif message_text.lower() in ['شيعة', 'شيعه']: await update.message.reply_text("⚠️ يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'.\nمثال: <code>شيعة علي</code>", parse_mode='HTML')
        else: logger.debug(f"Ignoring message from {user_id}: {message_text}")
        return
    search_query = match.group(2).strip()
    if not search_query: await update.message.reply_text("⚠️ يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'.\nمثال: <code>شيعة علي</code>", parse_mode='HTML'); return
    log_user(user_id); update_stats('search_count')
    safe_search_query = html.escape(search_query)
    logger.info(f"User {user_id} searching (FTS w/ Redis, prefixes, deduplication) for: '{search_query}'")
    matching_rowids = search_hadiths_db(search_query)
    num_results = len(matching_rowids)
    if num_results == 0: await update.message.reply_text(f"🤷‍♂️ لم يتم العثور على نتائج لكلمة البحث '<b>{safe_search_query}</b>'.", parse_mode='HTML'); return

    if num_results == 1:
        # Unchanged from previous version
        logger.info("Found single result, manually constructing first part.")
        row_id = matching_rowids[0]; hadith_details = get_hadith_details_by_db_id(row_id)
        if hadith_details:
            book = html.escape(hadith_details['book'] if hadith_details['book'] else 'غير متوفر'); actual_text = html.escape(hadith_details['arabic_text'] if hadith_details['arabic_text'] else 'النص غير متوفر'); grading_text = html.escape(hadith_details['grading'] if hadith_details['grading'] else 'الصحة غير متوفرة')
            header = f"📖 <b>الكتاب:</b> {book}\n\n📜 <b>الحديث:</b>\n"; footer = f"\n\n\n⚖️ <b>الصحة:</b> {grading_text}"
            temp_part_prefix = "<b>الجزء الأول من 99</b>\n\n"; remaining_space_for_text = MAX_MESSAGE_LENGTH - len(header) - len(temp_part_prefix) - 20
            first_part_hadith_text = ""; remaining_hadith_text = actual_text
            if len(actual_text) + len(footer) <= remaining_space_for_text: first_part_hadith_text = actual_text; remaining_hadith_text = ""; first_part_message_content = header + first_part_hadith_text + footer; remaining_parts = []
            elif len(actual_text) <= remaining_space_for_text: first_part_hadith_text = actual_text; remaining_hadith_text = ""; first_part_message_content = header + first_part_hadith_text; remaining_parts = split_message(footer.strip())
            else:
                split_pos = actual_text.rfind('\n', 0, remaining_space_for_text)
                if split_pos == -1: split_pos = actual_text.rfind(' ', 0, remaining_space_for_text)
                if split_pos == -1 or split_pos == 0: split_pos = remaining_space_for_text
                first_part_hadith_text = actual_text[:split_pos]; remaining_hadith_text = actual_text[split_pos:].lstrip(); first_part_message_content = header + first_part_hadith_text
                text_for_remaining_parts = remaining_hadith_text + footer; remaining_parts = split_message(text_for_remaining_parts)
            total_parts_count = 1 + len(remaining_parts)
            if total_parts_count > 1: message_to_send = f"<b>الجزء {arabic_number_to_word(1)} من {total_parts_count}</b>\n\n{first_part_message_content}"
            else: message_to_send = first_part_message_content
            if len(message_to_send) > MAX_MESSAGE_LENGTH: logger.warning(f"Calculated first message exceeds limit ({len(message_to_send)} > {MAX_MESSAGE_LENGTH}). Truncating."); message_to_send = message_to_send[:MAX_MESSAGE_LENGTH]
            message_sent = await update.message.reply_html(text=message_to_send)
            logger.info(f"Sent part 1 (single search result, manually constructed) for rowid {row_id}, message_id {message_sent.message_id}")
            if total_parts_count > 1:
                context.user_data[f'remaining_parts_{message_sent.message_id}'] = remaining_parts; context.user_data[f'total_parts_{message_sent.message_id}'] = total_parts_count
                callback_data_more = f"more_{message_sent.message_id}_2"; keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد ", callback_data=callback_data_more)]]) # Fixed: Use _2
                try: await context.bot.edit_message_reply_markup(chat_id=update.message.chat_id, message_id=message_sent.message_id, reply_markup=keyboard); logger.info(f"Edited message {message_sent.message_id} to add 'More' button.")
                except telegram.error.BadRequest as e: logger.warning(f"Could not edit message {message_sent.message_id} to add 'More' button: {e}")
                except telegram.error.TelegramError as e: logger.error(f"Telegram error editing message {message_sent.message_id} to add 'More' button: {e}")
        else: logger.error(f"Could not retrieve details for single result rowid {row_id}"); await update.message.reply_text("⚠️ حدث خطأ أثناء جلب تفاصيل النتيجة الوحيدة.")
        return

    if 1 < num_results <= 10:
        # Unchanged (numbered snippets)
        logger.info(f"Found {num_results} unique results, displaying numbered snippets.")
        response_text = f"💡 تم العثور على <b>{num_results}</b> نتائج مطابقة للبحث عن '<b>{safe_search_query}</b>':\n\n"; buttons = []; snippet_num = 1
        for row_id in matching_rowids:
            hadith_details = get_hadith_details_by_db_id(row_id)
            if hadith_details:
                book = html.escape(hadith_details['book'] if hadith_details['book'] else 'غير متوفر'); text_unescaped = hadith_details['arabic_text'] if hadith_details['arabic_text'] else ''
                context_snippet = "..."; found_keyword = None; keyword_index = -1; search_query_len = 0
                temp_index = text_unescaped.lower().find(search_query.lower())
                if temp_index != -1: keyword_index = temp_index; search_query_len = len(search_query); found_keyword = text_unescaped[keyword_index : keyword_index + search_query_len]
                if keyword_index == -1:
                     prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
                     for p in prefixes:
                         prefixed_query = p + search_query; temp_index = text_unescaped.lower().find(prefixed_query.lower())
                         if temp_index != -1: keyword_index = temp_index; search_query_len = len(prefixed_query); found_keyword = text_unescaped[keyword_index : keyword_index + search_query_len]; break
                if keyword_index != -1 and found_keyword:
                    start_context_index = max(0, keyword_index - 100); end_context_index = min(len(text_unescaped), keyword_index + search_query_len + 100)
                    text_before_raw = text_unescaped[start_context_index:keyword_index].strip(); text_after_raw = text_unescaped[keyword_index + search_query_len : end_context_index].strip()
                    words_before = text_before_raw.split(); words_after = text_after_raw.split()
                    context_before = " ".join(words_before[-SNIPPET_CONTEXT_WORDS:]); context_after = " ".join(words_after[:SNIPPET_CONTEXT_WORDS])
                    ellipsis_before = "... " if len(words_before) > SNIPPET_CONTEXT_WORDS or start_context_index > 0 else ""; ellipsis_after = " ..." if len(words_after) > SNIPPET_CONTEXT_WORDS or end_context_index < len(text_unescaped) else ""
                    context_snippet = f"{ellipsis_before}{context_before} <b>{html.escape(found_keyword)}</b> {context_after}{ellipsis_after}".strip()
                elif text_unescaped: words = text_unescaped.split(); context_snippet = " ".join(words[:SNIPPET_CONTEXT_WORDS*2]) + ('...' if len(words) > SNIPPET_CONTEXT_WORDS*2 else '')
                safe_context_snippet = context_snippet
                response_text += f"{snippet_num}. 📖 <b>الكتاب:</b> {book}\n   📝 <b>الحديث:</b> {safe_context_snippet}\n\n---\n\n"
                truncated_book = book[:25] + ('...' if len(book) > 25 else ''); simple_snippet_words = text_unescaped.split(); simple_snippet = " ".join(simple_snippet_words[:5]) + ('...' if len(simple_snippet_words) > 5 else '')
                button_text = f"{snippet_num}. 📜 {truncated_book} - {html.escape(simple_snippet)}"; callback_data = f"view_{row_id}"; buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
                snippet_num += 1
            else: logger.warning(f"Could not retrieve details for rowid {row_id} during snippet generation.")
        if buttons:
            if len(response_text) > MAX_MESSAGE_LENGTH: await update.message.reply_text(f"⚠️ تم العثور على {num_results} نتيجة مطابقة، لكن قائمة المقتطفات طويلة جدًا.")
            else: await update.message.reply_html(response_text)
            keyboard = InlineKeyboardMarkup(buttons); await update.message.reply_text(" اضغط على زر الحديث لعرضه كاملاً:", reply_markup=keyboard)
        else: await update.message.reply_text("⚠️ حدث خطأ أثناء عرض النتائج.")
        return

    if num_results > 10: await update.message.reply_text(f"⚠️ تم العثور على {num_results} نتيجة مطابقة لكلمة البحث '<b>{safe_search_query}</b>'. النتائج كثيرة جدًا لعرض المقتطفات. يرجى تحديد بحثك أكثر.", parse_mode='HTML'); return

# --- Callback Query Handler ---

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button clicks (callbacks) for viewing full hadith or getting more parts."""
    query = update.callback_query; await query.answer(); data = query.data; logger.info(f"Callback received: {data}")
    try:
        action, value = data.split('_', 1)
        if action == "view":
            row_id = int(value); hadith_details = get_hadith_details_by_db_id(row_id)
            if hadith_details:
                try: await query.delete_message(); logger.info(f"Deleted original button list message {query.message.message_id}")
                except telegram.error.BadRequest as e: logger.warning(f"Could not delete original button list message {query.message.message_id}: {e}")
                book = html.escape(hadith_details['book'] if hadith_details['book'] else 'غير متوفر'); actual_text = html.escape(hadith_details['arabic_text'] if hadith_details['arabic_text'] else 'النص غير متوفر'); grading_text = html.escape(hadith_details['grading'] if hadith_details['grading'] else 'الصحة غير متوفرة')
                header = f"📖 <b>الكتاب:</b> {book}\n\n📜 <b>الحديث:</b>\n"; footer = f"\n\n\n⚖️ <b>الصحة:</b> {grading_text}"
                temp_part_prefix = "<b>الجزء الأول من 99</b>\n\n"; remaining_space_for_text = MAX_MESSAGE_LENGTH - len(header) - len(temp_part_prefix) - 20
                first_part_hadith_text = ""; remaining_hadith_text = actual_text
                if len(actual_text) + len(footer) <= remaining_space_for_text: first_part_hadith_text = actual_text; remaining_hadith_text = ""; first_part_message_content = header + first_part_hadith_text + footer; remaining_parts = []
                elif len(actual_text) <= remaining_space_for_text: first_part_hadith_text = actual_text; remaining_hadith_text = ""; first_part_message_content = header + first_part_hadith_text; remaining_parts = split_message(footer.strip())
                else:
                    split_pos = actual_text.rfind('\n', 0, remaining_space_for_text)
                    if split_pos == -1: split_pos = actual_text.rfind(' ', 0, remaining_space_for_text)
                    if split_pos == -1 or split_pos == 0: split_pos = remaining_space_for_text
                    first_part_hadith_text = actual_text[:split_pos]; remaining_hadith_text = actual_text[split_pos:].lstrip(); first_part_message_content = header + first_part_hadith_text
                    text_for_remaining_parts = remaining_hadith_text + footer; remaining_parts = split_message(text_for_remaining_parts)
                total_parts_count = 1 + len(remaining_parts)
                if total_parts_count > 1: message_to_send = f"<b>الجزء {arabic_number_to_word(1)} من {total_parts_count}</b>\n\n{first_part_message_content}"
                else: message_to_send = first_part_message_content
                if len(message_to_send) > MAX_MESSAGE_LENGTH: logger.warning(f"Calculated first message (view) exceeds limit ({len(message_to_send)} > {MAX_MESSAGE_LENGTH}). Truncating."); message_to_send = message_to_send[:MAX_MESSAGE_LENGTH]
                message_sent = await context.bot.send_message(chat_id=query.message.chat_id, text=message_to_send, parse_mode='HTML')
                logger.info(f"Sent part 1 (view action, manually constructed) for rowid {row_id}, message_id {message_sent.message_id}")
                if total_parts_count > 1:
                    context.user_data[f'remaining_parts_{message_sent.message_id}'] = remaining_parts; context.user_data[f'total_parts_{message_sent.message_id}'] = total_parts_count
                    # --- FIXED: Use _2 for the first 'More' button callback ---
                    callback_data_more = f"more_{message_sent.message_id}_2";
                    # --- END FIX ---
                    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد ", callback_data=callback_data_more)]])
                    try: await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=message_sent.message_id, reply_markup=keyboard); logger.info(f"Edited message {message_sent.message_id} to add 'More' button.")
                    except telegram.error.BadRequest as e: logger.warning(f"Could not edit message {message_sent.message_id} to add 'More' button: {e}")
                    except telegram.error.TelegramError as e: logger.error(f"Telegram error editing message {message_sent.message_id} to add 'More' button: {e}")
            else: await context.bot.send_message(chat_id=query.message.chat_id, text="⚠️ خطأ: لم يتم العثور على تفاصيل الحديث المحدد في قاعدة البيانات.")
        elif action == "more":
            value_parts = value.split('_');
            if len(value_parts) == 2:
                original_message_id = int(value_parts[0]);
                # This is the overall part number we need to display next
                next_part_overall_num = int(value_parts[1])
                # Calculate the index into remaining_parts list
                next_remaining_part_index = next_part_overall_num - 2
                logger.debug(f"--- MORE ACTION --- Callback value parts: {value_parts}, Original Msg ID: {original_message_id}, Next Overall Part: {next_part_overall_num}, Calculated Index: {next_remaining_part_index}") # Added detailed log

                remaining_parts = context.user_data.get(f'remaining_parts_{original_message_id}');
                total_parts = context.user_data.get(f'total_parts_{original_message_id}')
                logger.debug(f"  Retrieved total_parts: {total_parts}")
                if remaining_parts is not None: logger.debug(f"  Retrieved remaining_parts (length {len(remaining_parts)})")
                else: logger.warning(f"  Could not find remaining_parts in context for key: remaining_parts_{original_message_id}")

                if remaining_parts is not None and total_parts is not None and 0 <= next_remaining_part_index < len(remaining_parts):
                    current_part_text = remaining_parts[next_remaining_part_index];
                    # The overall part number *being displayed now* is correct
                    current_part_display_num = next_remaining_part_index + 2
                    part_num_word = arabic_number_to_word(current_part_display_num)
                    logger.debug(f"  Current Part Display Num: {current_part_display_num}")
                    logger.debug(f"  Current Part Text (first 50 chars): {current_part_text[:50]}...")

                    part_text_with_title = f"<b>الجزء {part_num_word} من {total_parts}</b>\n\n{current_part_text}"
                    new_message = await context.bot.send_message(chat_id=query.message.chat_id, text=part_text_with_title, parse_mode='HTML')
                    logger.info(f"Sent part {current_part_display_num} (more action) message_id {new_message.message_id}")
                    try:
                        await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None)
                        logger.info(f"Removed 'More' button from previous message {original_message_id}")
                        context.user_data.pop(f'remaining_parts_{original_message_id}', None); context.user_data.pop(f'total_parts_{original_message_id}', None)
                    except telegram.error.BadRequest as e: logger.warning(f"Couldn't remove 'More' button from message {original_message_id}: {e}")
                    except telegram.error.TelegramError as e: logger.error(f"Telegram error removing 'More' button from message {original_message_id}: {e}")

                    if current_part_display_num < total_parts:
                        context.user_data[f'remaining_parts_{new_message.message_id}'] = remaining_parts; context.user_data[f'total_parts_{new_message.message_id}'] = total_parts
                        # Next overall part number is current_part_display_num + 1
                        next_overall_part_for_button = current_part_display_num + 1
                        callback_data_next = f"more_{new_message.message_id}_{next_overall_part_for_button}"
                        logger.debug(f"  Generating next callback data: {callback_data_next}") # Log next callback
                        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد ", callback_data=callback_data_next)]])
                        try: await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=new_message.message_id, reply_markup=keyboard); logger.info(f"Added 'More' button to new message {new_message.message_id}")
                        except telegram.error.BadRequest as e: logger.warning(f"Could not add 'More' button to more result message {new_message.message_id}: {e}")
                        except telegram.error.TelegramError as e: logger.error(f"Telegram error adding 'More' button to new message {new_message.message_id}: {e}")
                else:
                    logger.warning(f"Could not find remaining parts in context or index out of bounds for 'more' callback: {data}")
                    try: await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None)
                    except Exception: pass
            else:
                logger.warning(f"Received 'more' callback with unexpected data format: {data}")
                try: await query.edit_message_reply_markup(reply_markup=None)
                except Exception: pass
    except ValueError as e:
        logger.error(f"Error parsing callback data '{data}': {e}")
        try: await query.message.reply_text("⚠️ حدث خطأ أثناء معالجة طلبك.")
        except Exception as send_err: logger.error(f"Failed to send error message to user after ValueError: {send_err}")
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram API error in handle_button_click for data {data}: {e}")
        try: await query.message.reply_text(f"⚠️ حدث خطأ في تليجرام: {e.message}")
        except Exception as send_err: logger.error(f"Failed to send Telegram error message to user: {send_err}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in handle_button_click for data {data}: {e}")
        try: await query.message.reply_text("⚠️ عذراً، حدث خطأ غير متوقع.")
        except Exception as send_err: logger.error(f"Failed to send generic error message to user: {send_err}")


# --- Main Function ---

def main():
    """Starts the bot."""
    if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN: # Check against placeholder
        logger.error("Please replace 'YOUR_BOT_TOKEN' with your actual bot token in the code.")
    if not os.path.exists(JSON_FILE): # Use JSON_FILE constant
         logger.error(f"JSON file '{JSON_FILE}' not found. Please ensure it exists in the same directory.")
         return
    if not redis_pool: logger.warning("Redis connection pool not available. Caching will be disabled.")
    if not BOT_OWNER_ID or BOT_OWNER_ID == 123456789: # Check placeholder owner ID
         logger.warning("BOT_OWNER_ID is not set correctly. The 'Add Hadith' approval feature will not work.")

    try: init_db(); populate_db_from_json(JSON_FILE) # Use JSON_FILE constant
    except Exception as e: logger.error(f"Failed to initialize or populate database. Exiting. Error: {e}"); return

    application = Application.builder().token(BOT_TOKEN).build()

    # --- MODIFIED: Add ConversationHandler entry points and start button handler ---
    add_hadith_conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler('addhadith', add_hadith_start),
            MessageHandler(filters.Regex(r'^اضافة حديث$') & ~filters.COMMAND, add_hadith_start),
            CallbackQueryHandler(add_hadith_start_button, pattern=r'^start_add_hadith$')
            ],
        states={
            ASK_BOOK: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_book)],
            ASK_TEXT: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_text)],
            ASK_GRADING: [MessageHandler(filters.TEXT & ~filters.COMMAND, receive_grading), CommandHandler('skip', skip_grading)],
        },
        fallbacks=[CommandHandler('cancel', cancel_submission)],
    )
    application.add_handler(add_hadith_conv_handler)
    # --- End Modification ---

    # Register other handlers
    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CommandHandler("help", help_command))
    search_pattern = r'^(شيعة|شيعه)\s+(.+)$'
    application.add_handler(MessageHandler(filters.Regex(search_pattern) & ~filters.COMMAND, handle_search))
    trigger_only_pattern = r'^(شيعة|شيعه)$'
    application.add_handler(MessageHandler(filters.Regex(trigger_only_pattern) & ~filters.COMMAND, handle_search))

    # Register approval callback handler
    application.add_handler(CallbackQueryHandler(handle_approval_callback, pattern=r'^(approve|reject)_\d+$'))
    # Register view/more callback handler
    application.add_handler(CallbackQueryHandler(handle_button_click, pattern=r'^(view|more)_'))


    logger.info("Bot starting with FTS5, Redis cache, deduplication, and improved formatting...")
    application.run_polling()
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()
