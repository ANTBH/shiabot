import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
import html
import json
import sqlite3
import logging
import os
import re
import redis
from typing import List, Dict, Tuple, Optional, Any, Set

# --- Configuration ---
BOT_TOKEN = "7378891608:AAFUPueUuSAPHd4BPN8znb-jcDGsjnnm_f8"  # استبدل هذا بالتوكن الخاص ببوتك
JSON_FILE = '1.json'     # اسم ملف الأحاديث JSON (للقراءة الأولية فقط)
DB_NAME = 'hadith_bot.db'      # اسم ملف قاعدة البيانات SQLite
DEVELOPER_NAME = "عبدالمجيد " # استبدل هذا باسم المطور
MAX_MESSAGE_LENGTH = 4000      # الحد الأقصى لطول رسالة تليجرام
CONTEXT_WORDS = 10              # Number of words to show after keyword in snippet

# --- Redis Configuration ---
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
REDIS_DB = 0
CACHE_EXPIRY_SECONDS = 3600    # مدة صلاحية الكاش (مثال: ساعة واحدة)

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
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
    # Unchanged
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("CREATE TABLE IF NOT EXISTS stats (key TEXT PRIMARY KEY, value INTEGER NOT NULL)")
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('search_count', 0))
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('user_count', 0))
        cursor.execute("CREATE TABLE IF NOT EXISTS users (user_id INTEGER PRIMARY KEY)")
        # Ensure original_id has an index for faster lookups if needed, though FTS is primary
        # FTS5 table definition - store original_id to help with deduplication later
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts USING fts5(
                original_id,  -- Keep original_id for deduplication
                book UNINDEXED,
                arabic_text,
                grading UNINDEXED,
                content='hadiths_content' -- Optional: Specify external content table if needed
            );
        ''')
        # Optional: Create a regular table to store full hadith details if not using external content
        # cursor.execute('''
        #     CREATE TABLE IF NOT EXISTS hadiths_content (
        #         rowid INTEGER PRIMARY KEY,
        #         original_id TEXT,
        #         book TEXT,
        #         arabic_text TEXT,
        #         grading TEXT
        #     );
        # ''')

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully (using FTS5 table).")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise

def populate_db_from_json(filename: str):
    # Unchanged from previous version regarding population logic
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
                    original_id_str = str(hadith.get('id', f'gen_{added_count}')) # Ensure an ID exists
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
    except sqlite3.Error as e: logger.error(f"Database error during FTS population check/insert: {e}")
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
    # Unchanged (Counts rows in FTS table)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # Count distinct original_id to get a more accurate count of unique hadiths
        cursor.execute("SELECT COUNT(DISTINCT original_id) FROM hadiths_fts")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logger.error(f"Error getting distinct hadiths count from FTS: {e}")
        # Fallback to total rows if distinct fails
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
        # Update user count stat based on the actual count in the users table
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.execute("UPDATE stats SET value = ? WHERE key = ?", (user_count, 'user_count'))
        conn.commit()
        conn.close()
    except sqlite3.Error as e: logger.error(f"Error logging user {user_id}: {e}")

# --- Helper Functions ---

def arabic_number_to_word(n: int) -> str:
    """Converts an integer (1-20+) to its Arabic word representation."""
    if not isinstance(n, int) or n <= 0:
        return str(n) # Return original if not a positive integer

    words = {
        1: "الأول", 2: "الثاني", 3: "الثالث", 4: "الرابع", 5: "الخامس",
        6: "السادس", 7: "السابع", 8: "الثامن", 9: "التاسع", 10: "العاشر",
        11: "الحادي عشر", 12: "الثاني عشر", 13: "الثالث عشر", 14: "الرابع عشر", 15: "الخامس عشر",
        16: "السادس عشر", 17: "السابع عشر", 18: "الثامن عشر", 19: "التاسع عشر", 20: "العشرون"
        # Add more if needed, e.g., 21: "الحادي والعشرون", etc.
    }
    # For numbers > 20, you might need a more complex logic or just return the number
    if n > 20:
         # Simple fallback for now
         return f"الـ {n}" # e.g., "الـ 21"

    return words.get(n, str(n)) # Fallback to string representation if not in dict

def format_hadith_from_row(hadith_row: sqlite3.Row) -> str:
    # Unchanged
    book = html.escape(hadith_row['book'] if hadith_row['book'] else 'غير متوفر')
    text = html.escape(hadith_row['arabic_text'] if hadith_row['arabic_text'] else 'النص غير متوفر')
    grading = html.escape(hadith_row['grading'] if hadith_row['grading'] else 'الصحة غير متوفرة')
    return f"📖 <b>الكتاب:</b> {book}\n\n📜 <b>الحديث:</b>\n{text}\n\n⚖️ <b>الصحة:</b> {grading}"

def split_message(text: str) -> List[str]:
    # Unchanged
    parts = []
    while len(text) > MAX_MESSAGE_LENGTH:
        split_pos = text.rfind('\n', 0, MAX_MESSAGE_LENGTH)
        if split_pos == -1: split_pos = text.rfind(' ', 0, MAX_MESSAGE_LENGTH)
        if split_pos == -1: split_pos = MAX_MESSAGE_LENGTH
        parts.append(text[:split_pos])
        text = text[split_pos:].lstrip()
    parts.append(text)
    return parts

def search_hadiths_db(query: str) -> List[int]:
    """
    Searches for hadiths using FTS, handles prefixes, and deduplicates results
    based on original_id. Checks Redis cache first.
    Returns a list of unique FTS rowids of matching hadiths.
    """
    if not query: return []

    normalized_query = query.strip().lower()
    cache_key = f"hadith_search_unique:{normalized_query}" # Modified cache key
    redis_conn = get_redis_connection()
    cached_result = None

    # 1. Check Redis Cache
    if redis_conn:
        try:
            cached_data = redis_conn.get(cache_key)
            if cached_data:
                cached_result = json.loads(cached_data.decode('utf-8'))
                logger.info(f"Cache HIT for unique query '{query}'. Found {len(cached_result)} results in Redis.")
                return cached_result
        except json.JSONDecodeError: logger.error(f"Error decoding cached JSON for key '{cache_key}'. Ignoring cache.")
        except redis.exceptions.RedisError as e: logger.error(f"Redis error when getting cache for key '{cache_key}': {e}")
        except Exception as e: logger.error(f"Unexpected error during Redis cache get: {e}")

    logger.info(f"Cache MISS for unique query '{query}'. Searching SQLite FTS with prefix handling and deduplication.")
    # 2. Search SQLite FTS (Fallback or if cache missed)
    conn = get_db_connection()
    cursor = conn.cursor()
    unique_rowids = []
    seen_original_ids: Set[str] = set() # To track unique hadiths

    try:
        # Build FTS query with prefixes
        prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
        fts_query_parts = [f'"{query}"']
        for p in prefixes:
            fts_query_parts.append(f'"{p}{query}"')
        fts_match_query = " OR ".join(fts_query_parts)

        logger.debug(f"Constructed FTS MATCH query: {fts_match_query}")
        # Select rowid and original_id to allow deduplication
        cursor.execute(
            "SELECT rowid, original_id FROM hadiths_fts WHERE hadiths_fts MATCH ? ORDER BY rank",
            (fts_match_query,)
        )
        results = cursor.fetchall()
        logger.info(f"Raw FTS Search for '{query}' found {len(results)} potential matches.")

        # --- MODIFIED: Deduplicate results based on original_id ---
        for row in results:
            original_id = row['original_id']
            if original_id not in seen_original_ids:
                seen_original_ids.add(original_id)
                unique_rowids.append(row['rowid'])

        logger.info(f"Deduplicated search for '{query}' resulted in {len(unique_rowids)} unique hadiths.")

        # 3. Cache the deduplicated result in Redis
        if unique_rowids and redis_conn:
            try:
                serialized_results = json.dumps(unique_rowids)
                redis_conn.set(cache_key, serialized_results, ex=CACHE_EXPIRY_SECONDS)
                logger.info(f"Cached {len(unique_rowids)} unique results for query '{query}' in Redis.")
            except redis.exceptions.RedisError as e: logger.error(f"Redis error when setting cache for key '{cache_key}': {e}")
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
    try:
        # Ensure we fetch all necessary columns, including original_id if needed elsewhere
        cursor.execute("SELECT rowid, original_id, book, arabic_text, grading FROM hadiths_fts WHERE rowid = ?", (row_id,))
        hadith_details = cursor.fetchone()
    except sqlite3.Error as e: logger.error(f"Error fetching hadith details for rowid {row_id}: {e}")
    finally: conn.close()
    return hadith_details

# --- Bot Command Handlers (start, help_command) ---
# Unchanged

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Unchanged
    user = update.effective_user
    log_user(user.id)
    bot_username = context.bot.username
    add_group_url = f"https://t.me/{bot_username}?startgroup=true"
    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("➕ أضف البوت إلى مجموعتك", url=add_group_url)]])
    welcome_message = f"""
    🕌 أهلاً بك يا {user.mention_html()} في بوت الأحاديث!

    للبحث، أرسل رسالة تبدأ بـ `شيعة` أو `شيعه` متبوعة بكلمة البحث.
    مثال: `شيعة الحسين`

    استخدم الأمر /help لعرض المساعدة والإحصائيات.
    """
    await update.message.reply_html(welcome_message, reply_markup=keyboard)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Unchanged (but uses updated get_total_hadiths_count)
    log_user(update.effective_user.id)
    total_hadiths = get_total_hadiths_count()
    search_count = get_stat('search_count')
    user_count = get_stat('user_count')
    help_text = f"""
    <b>مساعدة وإحصائيات بوت الأحاديث</b> 🕌

    📊 <b>الإحصائيات:</b>
    - عدد الأحاديث الفريدة في قاعدة البيانات: {total_hadiths}
    - إجمالي عمليات البحث: {search_count}
    - عدد المستخدمين الفريدين: {user_count}

    🔍 <b>كيفية البحث:</b>
    أرسل رسالة تبدأ بـ <code>شيعة</code> أو <code>شيعه</code> ثم مسافة ثم الكلمة أو الجملة التي تريد البحث عنها.
    مثال: <code>شيعه علي بن ابي طالب</code>
    (يستخدم الآن البحث بالنص الكامل FTS مع تجاهل البادئات الشائعة ومنع تكرار النتائج، بالإضافة لذاكرة التخزين المؤقت Redis لتحسين الأداء)

    ✨ <b>المطور:</b> {html.escape(DEVELOPER_NAME)}
    """
    await update.message.reply_html(help_text, disable_web_page_preview=True)


# --- Message Handler ---

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming messages starting with 'شيعة' or 'شيعه' followed by search terms."""
    # Unchanged initial checks and query extraction
    user_id = update.effective_user.id
    message_text = update.message.text.strip()
    match = re.match(r'^(شيعة|شيعه)\s+(.+)', message_text, re.IGNORECASE | re.UNICODE)

    if not match:
        if message_text.lower() in ['شيعة', 'شيعه']:
            await update.message.reply_text("⚠️ يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'.\nمثال: <code>شيعة علي</code>", parse_mode='HTML')
        else: logger.debug(f"Ignoring message from {user_id}: {message_text}")
        return

    search_query = match.group(2).strip()
    if not search_query:
         await update.message.reply_text("⚠️ يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'.\nمثال: <code>شيعة علي</code>", parse_mode='HTML')
         return

    log_user(user_id)
    update_stats('search_count')
    safe_search_query = html.escape(search_query)
    logger.info(f"User {user_id} searching (FTS w/ Redis, prefixes, deduplication) for: '{search_query}'")

    # Uses the updated search function with deduplication
    matching_rowids = search_hadiths_db(search_query)
    num_results = len(matching_rowids)

    if num_results == 0:
        await update.message.reply_text(f"🤷‍♂️ لم يتم العثور على نتائج لكلمة البحث '<b>{safe_search_query}</b>'.", parse_mode='HTML')
        return

    # --- MODIFIED: Handle single result - Improved Part Numbering ---
    if num_results == 1:
        logger.info("Found single result, displaying directly.")
        row_id = matching_rowids[0]
        hadith_details = get_hadith_details_by_db_id(row_id)
        if hadith_details:
            full_text = format_hadith_from_row(hadith_details)
            parts = split_message(full_text)
            total_parts_count = len(parts)

            # --- MODIFICATION: Only add part number if more than one part ---
            if total_parts_count > 1:
                part_num_word = arabic_number_to_word(1) # Get word for "First"
                part_text_with_title = f"<b>الجزء {part_num_word} من {total_parts_count}</b>\n\n{parts[0]}"
            else:
                part_text_with_title = parts[0] # No title needed for single part

            message_sent = await update.message.reply_html(text=part_text_with_title)

            # Add "More" button only if there are more parts
            if total_parts_count > 1:
                # Start next part index at 1 (second part) for the callback
                callback_data_more = f"more_{message_sent.message_id}_{row_id}_1_{total_parts_count}"
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد 👇", callback_data=callback_data_more)]])
                try:
                    await context.bot.edit_message_reply_markup(chat_id=update.message.chat_id, message_id=message_sent.message_id, reply_markup=keyboard)
                except telegram.error.BadRequest as e:
                    logger.warning(f"Could not add 'More' button to single result message {message_sent.message_id}: {e}")
        else:
            logger.error(f"Could not retrieve details for single result rowid {row_id}")
            await update.message.reply_text("⚠️ حدث خطأ أثناء جلب تفاصيل النتيجة الوحيدة.")
        return

    # Handle 2-10 results (Snippet logic largely unchanged, uses deduplicated rowids)
    if 1 < num_results <= 10:
        logger.info(f"Found {num_results} unique results, displaying snippets.")
        response_text = f"💡 تم العثور على <b>{num_results}</b> نتائج فريدة للبحث عن '<b>{safe_search_query}</b>':\n\n"
        buttons = []
        for row_id in matching_rowids: # Iterate through unique rowids
            hadith_details = get_hadith_details_by_db_id(row_id)
            if hadith_details:
                book = html.escape(hadith_details['book'] if hadith_details['book'] else 'غير متوفر')
                text_unescaped = hadith_details['arabic_text'] if hadith_details['arabic_text'] else ''
                # --- Snippet Generation (Minor change: use original query for highlighting) ---
                context_snippet = "..."
                # Try finding the original query first for better context
                keyword_index = text_unescaped.lower().find(search_query.lower())
                # If not found, try finding prefixed versions (less ideal for context start)
                if keyword_index == -1:
                     prefixes = ['و', 'ف', 'ب', 'ل', 'ك']
                     for p in prefixes:
                         prefixed_query = p + search_query
                         keyword_index = text_unescaped.lower().find(prefixed_query.lower())
                         if keyword_index != -1:
                             # Use the prefixed query length if found this way
                             search_query_len = len(prefixed_query)
                             break # Stop after finding the first prefix match
                     else: # If no prefix match either
                         search_query_len = len(search_query) # Fallback to original length
                else:
                    search_query_len = len(search_query) # Use original length if found directly

                if keyword_index != -1:
                    start_context_index = keyword_index + search_query_len
                    text_after_keyword = text_unescaped[start_context_index:].strip()
                    if text_after_keyword:
                        words_after_keyword = text_after_keyword.split()
                        context_words = words_after_keyword[:CONTEXT_WORDS]
                        if context_words:
                             ellipsis_before = "... " if keyword_index > 0 else ""
                             ellipsis_after = " ..." if len(words_after_keyword) > CONTEXT_WORDS else ""
                             # Bold the found keyword (original or prefixed) in the snippet - tricky, maybe skip for now
                             # context_snippet = ellipsis_before + f"<b>{text_unescaped[keyword_index:keyword_index+search_query_len]}</b>" + " ".join(context_words) + ellipsis_after
                             context_snippet = ellipsis_before + " ".join(context_words) + ellipsis_after # Simpler snippet without bolding

                safe_context_snippet = html.escape(context_snippet)
                # --- End Snippet Generation ---
                response_text += f"📖 <b>الكتاب:</b> {book}\n📝 <b>مقتطف:</b> {safe_context_snippet}\n\n---\n\n"
                truncated_book = book[:25] + ('...' if len(book) > 25 else '')
                # Use a simpler snippet for the button text
                simple_snippet = text_unescaped[keyword_index:keyword_index+30].strip() + ('...' if len(text_unescaped) > keyword_index+30 else '') if keyword_index != -1 else text_unescaped[:30] + '...'
                button_text = f"📜 {truncated_book} - {html.escape(simple_snippet)}"
                callback_data = f"view_{row_id}"
                buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])
            else: logger.warning(f"Could not retrieve details for rowid {row_id} during snippet generation.")

        if buttons:
            # Send snippets first, then the buttons
            # Check if response_text exceeds limit (unlikely with only 10 snippets)
            if len(response_text) > MAX_MESSAGE_LENGTH:
                 await update.message.reply_text(f"⚠️ تم العثور على {num_results} نتيجة فريدة، لكن قائمة المقتطفات طويلة جدًا.")
            else:
                 await update.message.reply_html(response_text)

            keyboard = InlineKeyboardMarkup(buttons)
            await update.message.reply_text("👇 اضغط على زر الحديث لعرضه كاملاً:", reply_markup=keyboard)
        else: await update.message.reply_text("⚠️ حدث خطأ أثناء عرض النتائج.")
        return

    # Handle > 10 results
    if num_results > 10:
        await update.message.reply_text(f"⚠️ تم العثور على {num_results} نتيجة فريدة لكلمة البحث '<b>{safe_search_query}</b>'. النتائج كثيرة جدًا لعرض المقتطفات. يرجى تحديد بحثك أكثر.", parse_mode='HTML')
        return

# --- Callback Query Handler ---

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button clicks (callbacks) for viewing full hadith or getting more parts."""
    query = update.callback_query
    await query.answer() # Acknowledge the button press
    data = query.data
    logger.info(f"Callback received: {data}")

    try:
        action, value = data.split('_', 1)

        if action == "view":
            row_id = int(value)
            hadith_details = get_hadith_details_by_db_id(row_id)
            if hadith_details:
                full_text = format_hadith_from_row(hadith_details)
                parts = split_message(full_text)
                total_parts_count = len(parts)

                # --- MODIFICATION: Only add part number if more than one part & use words ---
                if total_parts_count > 1:
                    part_num_word = arabic_number_to_word(1) # Get word for "First"
                    part_text_with_title = f"<b>الجزء {part_num_word} من {total_parts_count}</b>\n\n{parts[0]}"
                else:
                    part_text_with_title = parts[0] # No title needed for single part

                # Send the first part (or the only part)
                # We reply to the original message containing the button list
                message_sent = await query.message.reply_html(text=part_text_with_title)
                logger.info(f"Sent part 1 (view action) for rowid {row_id}, message_id {message_sent.message_id}")


                # Add "More" button only if there are more parts
                if total_parts_count > 1:
                    # Start next part index at 1 (second part) for the callback
                    callback_data_more = f"more_{message_sent.message_id}_{row_id}_1_{total_parts_count}"
                    keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد 👇", callback_data=callback_data_more)]])
                    try:
                        # Edit the *newly sent* message to add the button
                        await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=message_sent.message_id, reply_markup=keyboard)
                        logger.info(f"Added 'More' button to message {message_sent.message_id}")
                    except telegram.error.BadRequest as e:
                        logger.warning(f"Could not add 'More' button to view result message {message_sent.message_id}: {e}")

                 # Optionally remove the original button list message after sending the first part
                # try:
                #      await query.delete_message() # Deletes the message with the buttons
                # except telegram.error.BadRequest as e:
                #      logger.warning(f"Could not delete original button message {query.message.message_id}: {e}")


            else: await query.message.reply_text("⚠️ خطأ: لم يتم العثور على تفاصيل الحديث المحدد في قاعدة البيانات.")

        elif action == "more":
            value_parts = value.split('_')
            if len(value_parts) == 4:
                original_message_id = int(value_parts[0]) # ID of the message *with the button*
                row_id = int(value_parts[1])
                next_part_index = int(value_parts[2]) # Index of the *next* part to send (0-based)
                total_parts = int(value_parts[3])

                hadith_details = get_hadith_details_by_db_id(row_id)
                # Check if next_part_index is valid
                if hadith_details and 0 <= next_part_index < total_parts:
                    full_text = format_hadith_from_row(hadith_details)
                    all_parts = split_message(full_text)
                    current_part_display_num = next_part_index + 1 # Part number for display (1-based)
                    part_num_word = arabic_number_to_word(current_part_display_num) # Get word

                    # --- MODIFIED: Add part number (as word) to the current part ---
                    part_text_with_title = f"<b>الجزء {part_num_word} من {total_parts}</b>\n\n{all_parts[next_part_index]}"
                    # Send the next part as a new message
                    new_message = await query.message.reply_html(text=part_text_with_title)
                    logger.info(f"Sent part {current_part_display_num} (more action) for rowid {row_id}, message_id {new_message.message_id}")


                    # Remove button from the *previous* message (the one that was clicked)
                    try:
                        await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None)
                        logger.info(f"Removed 'More' button from previous message {original_message_id}")
                    except telegram.error.BadRequest as e:
                        # Ignore common errors like message not modified or not found
                        if "message is not modified" in str(e).lower() or "message to edit not found" in str(e).lower():
                            logger.warning(f"Couldn't remove 'More' button from message {original_message_id} (likely already removed or message deleted): {e}")
                        else:
                            logger.error(f"Error removing 'More' button from message {original_message_id}: {e}")

                    # Add "More" button to the *new* message if there are further parts
                    if current_part_display_num < total_parts:
                        # Increment next_part_index for the *next* callback
                        callback_data_next = f"more_{new_message.message_id}_{row_id}_{next_part_index + 1}_{total_parts}"
                        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("المزيد 👇", callback_data=callback_data_next)]])
                        try:
                             await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=new_message.message_id, reply_markup=keyboard)
                             logger.info(f"Added 'More' button to new message {new_message.message_id}")
                        except telegram.error.BadRequest as e:
                             logger.warning(f"Could not add 'More' button to more result message {new_message.message_id}: {e}")

                else:
                     logger.warning(f"Invalid data or hadith not found for 'more' callback: {value}")
                     # Attempt to remove button from the clicked message even if data is bad
                     try: await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None)
                     except Exception: pass
            else:
                 logger.warning(f"Received 'more' callback with unexpected data format: {value}")
                 # Attempt to remove button from the clicked message
                 try: await query.edit_message_reply_markup(reply_markup=None)
                 except Exception: pass

    except ValueError as e:
        logger.error(f"Error parsing callback data '{data}': {e}")
        # Use query.message.reply_text for consistency, even though query.edit_message_text might be possible
        try: await query.message.reply_text("⚠️ حدث خطأ أثناء معالجة طلبك.")
        except Exception as send_err: logger.error(f"Failed to send error message to user after ValueError: {send_err}")
    except telegram.error.TelegramError as e:
        logger.error(f"Telegram API error in handle_button_click for data {data}: {e}")
        # Try sending a message back if possible
        try: await query.message.reply_text(f"⚠️ حدث خطأ في تليجرام: {e.message}")
        except Exception as send_err: logger.error(f"Failed to send Telegram error message to user: {send_err}")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in handle_button_click for data {data}: {e}")
        try: await query.message.reply_text("⚠️ عذراً، حدث خطأ غير متوقع.")
        except Exception as send_err: logger.error(f"Failed to send generic error message to user: {send_err}")


# --- Main Function ---

def main():
    """Starts the bot."""
    # Unchanged startup checks and initialization
    if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
        logger.error("Please replace 'YOUR_BOT_TOKEN' with your actual bot token.")
        return
    if not redis_pool:
         logger.warning("Redis connection pool not available. Caching will be disabled.")

    try:
        init_db()
        populate_db_from_json(JSON_FILE)
    except Exception as e:
         logger.error(f"Failed to initialize or populate database. Exiting. Error: {e}")
         return

    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers (unchanged)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    # Regex updated slightly to ensure space is mandatory after trigger word
    search_pattern = r'^(شيعة|شيعه)\s+(.+)$' # Requires space and search term
    application.add_handler(MessageHandler(filters.Regex(search_pattern) & ~filters.COMMAND, handle_search))
    # Add handler for messages that *only* contain the trigger word (without search term)
    trigger_only_pattern = r'^(شيعة|شيعه)$'
    application.add_handler(MessageHandler(filters.Regex(trigger_only_pattern) & ~filters.COMMAND, handle_search)) # Let handle_search give the error message
    application.add_handler(CallbackQueryHandler(handle_button_click))

    logger.info("Bot starting with FTS5, Redis cache, deduplication, and improved formatting...")
    application.run_polling()
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()
