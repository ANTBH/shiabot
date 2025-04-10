import telegram
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
import json
import sqlite3
import logging
import os
import re
from typing import List, Dict, Tuple, Optional, Any

# --- Configuration ---
BOT_TOKEN = "7378891608:AAFUPueUuSAPHd4BPN8znb-jcDGsjnnm_f8"  # استبدل هذا بالتوكن الخاص ببوتك
JSON_FILE = '1.json'     # اسم ملف الأحاديث JSON (للقراءة الأولية فقط)
DB_NAME = 'hadith_bot.db'      # اسم ملف قاعدة البيانات SQLite
DEVELOPER_NAME = "عبدالمجيد " # استبدل هذا باسم المطور
MAX_MESSAGE_LENGTH = 4000      # الحد الأقصى لطول رسالة تليجرام
MAX_SNIPPET_LENGTH = 150       # طول المقتطف المعروض في نتائج البحث الأولية

# --- Logging Setup ---
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- Database Functions ---

def get_db_connection() -> sqlite3.Connection:
    """Establishes and returns a database connection."""
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row # Return rows as dictionary-like objects
    return conn

def init_db():
    """Initializes the SQLite database and creates tables if they don't exist."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # Create stats table (unchanged)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS stats (
                key TEXT PRIMARY KEY,
                value INTEGER NOT NULL
            )
        ''')
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('search_count', 0))
        cursor.execute("INSERT OR IGNORE INTO stats (key, value) VALUES (?, ?)", ('user_count', 0))

        # Create users table (unchanged)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY
            )
        ''')

        # --- MODIFIED: Create FTS5 virtual table for hadiths ---
        # This table will store the hadith content and be indexed for full-text search
        cursor.execute('''
            CREATE VIRTUAL TABLE IF NOT EXISTS hadiths_fts USING fts5(
                original_id UNINDEXED,  -- Store original ID, but don't index with FTS
                book UNINDEXED,         -- Store book name, don't index with FTS
                arabic_text,            -- The main text column to be indexed and searched by FTS
                grading UNINDEXED       -- Store grading, don't index with FTS
            );
        ''')
        # Note: The primary key for FTS tables is the implicit 'rowid'

        # --- REMOVED: Old hadiths_content table and indexes ---
        # cursor.execute('DROP TABLE IF EXISTS hadiths_content;') # Optional cleanup if migrating
        # cursor.execute('DROP INDEX IF EXISTS idx_hadith_text;')
        # cursor.execute('DROP INDEX IF EXISTS idx_hadith_original_id;')

        conn.commit()
        conn.close()
        logger.info("Database initialized successfully (using FTS5 table).")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        raise

def populate_db_from_json(filename: str):
    """Populates the hadiths_fts table from the JSON file if the table is empty."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Check if the FTS table is empty
        cursor.execute("SELECT COUNT(*) FROM hadiths_fts")
        count = cursor.fetchone()[0]

        if count == 0:
            logger.info("FTS Hadiths table is empty. Populating from JSON file...")
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                added_count = 0
                for hadith in data:
                    text = hadith.get('arabicText', '')
                    cleaned_text = re.sub(r"^\s*\d+[\sـ.-]*", "", text).strip()
                    if not cleaned_text: continue

                    # --- MODIFIED: Insert into hadiths_fts table ---
                    cursor.execute(
                        """
                        INSERT INTO hadiths_fts (original_id, book, arabic_text, grading)
                        VALUES (?, ?, ?, ?)
                        """,
                        (
                            hadith.get('id'),
                            hadith.get('book'),
                            cleaned_text,
                            hadith.get('majlisiGrading')
                        )
                    )
                    added_count += 1
                conn.commit()
                logger.info(f"Successfully added {added_count} hadiths to the FTS database.")

            except FileNotFoundError:
                logger.error(f"Error: JSON file '{filename}' not found during population.")
            except json.JSONDecodeError:
                logger.error(f"Error: Could not decode JSON from '{filename}'. Check file format.")
            except Exception as e:
                logger.error(f"An unexpected error occurred loading hadiths from JSON: {e}")
        else:
            logger.info("FTS Hadiths table already populated.")

    except sqlite3.Error as e:
        logger.error(f"Database error during FTS population check/insert: {e}")
    finally:
        conn.close()

# --- Other Database Functions (update_stats, get_stat, log_user) remain largely the same ---

def update_stats(key: str, increment: int = 1):
    """Increments a value in the stats table."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("UPDATE stats SET value = value + ? WHERE key = ?", (increment, key))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Error updating stats for key '{key}': {e}")

def get_stat(key: str) -> int:
    """Gets a value from the stats table."""
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
    """Gets the total number of hadiths from the FTS database."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        # --- MODIFIED: Query the FTS table ---
        cursor.execute("SELECT COUNT(*) FROM hadiths_fts")
        result = cursor.fetchone()
        conn.close()
        return result[0] if result else 0
    except sqlite3.Error as e:
        logger.error(f"Error getting total hadiths count from FTS: {e}")
        return 0

def log_user(user_id: int):
    """Logs a unique user ID and updates the user count."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("INSERT OR IGNORE INTO users (user_id) VALUES (?)", (user_id,))
        cursor.execute("SELECT COUNT(*) FROM users")
        user_count = cursor.fetchone()[0]
        cursor.execute("UPDATE stats SET value = ? WHERE key = ?", (user_count, 'user_count'))
        conn.commit()
        conn.close()
    except sqlite3.Error as e:
        logger.error(f"Error logging user {user_id}: {e}")

# --- Helper Functions ---

def format_hadith_from_row(hadith_row: sqlite3.Row) -> str:
    """Formats a single Hadith from a database row for display."""
    # Works with rows from hadiths_fts table as well
    book = hadith_row['book'] if hadith_row['book'] else 'غير متوفر'
    text = hadith_row['arabic_text'] if hadith_row['arabic_text'] else 'النص غير متوفر'
    grading = hadith_row['grading'] if hadith_row['grading'] else 'الصحة غير متوفرة'
    return f"📖 **الكتاب:** {book}\n\n📜 **الحديث:**\n{text}\n\n⚖️ **الصحة:** {grading}"

def split_message(text: str) -> List[str]:
    """Splits a long message into parts respecting MAX_MESSAGE_LENGTH."""
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
    Searches for hadiths in the FTS table using the MATCH operator.
    Returns a list of FTS rowids of matching hadiths.
    """
    if not query:
        return []

    conn = get_db_connection()
    cursor = conn.cursor()
    matching_rowids = []
    try:
        # --- MODIFIED: Use FTS MATCH operator ---
        # The query syntax might need adjustment based on specific needs (e.g., phrase search)
        # For simple term matching, just passing the query works.
        # ORDER BY rank is optional but often useful to get best matches first.
        cursor.execute(
            "SELECT rowid FROM hadiths_fts WHERE hadiths_fts MATCH ? ORDER BY rank",
            (query,) # Pass the user query directly for basic matching
        )
        results = cursor.fetchall()
        # FTS inherently deduplicates by document (row)
        matching_rowids = [row['rowid'] for row in results]
        logger.info(f"FTS Search for '{query}' found {len(matching_rowids)} results.")
    except sqlite3.Error as e:
        # Handle specific FTS query errors if needed
        if "malformed MATCH expression" in str(e):
             logger.warning(f"FTS query syntax error for query '{query}': {e}")
             # Optionally inform the user about invalid syntax
        else:
            logger.error(f"Database FTS search error for query '{query}': {e}")
    finally:
        conn.close()
    return matching_rowids

def get_hadith_details_by_db_id(row_id: int) -> Optional[sqlite3.Row]:
    """Retrieves full hadith details from the FTS table using its rowid."""
    conn = get_db_connection()
    cursor = conn.cursor()
    hadith_details = None
    try:
        # --- MODIFIED: Select from hadiths_fts using rowid ---
        # Select all needed columns explicitly or use *
        cursor.execute("SELECT rowid, original_id, book, arabic_text, grading FROM hadiths_fts WHERE rowid = ?", (row_id,))
        hadith_details = cursor.fetchone()
    except sqlite3.Error as e:
        logger.error(f"Error fetching hadith details for rowid {row_id}: {e}")
    finally:
        conn.close()
    return hadith_details


# --- Bot Command Handlers (start, help_command) ---
# help_command already uses get_total_hadiths_count which now queries FTS table

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /start command."""
    user = update.effective_user
    log_user(user.id)

    bot_username = context.bot.username
    add_group_url = f"https://t.me/{bot_username}?startgroup=true"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("➕ أضف البوت إلى مجموعتك", url=add_group_url)]
    ])
    welcome_message = f"""
    أهلاً بك يا {user.mention_html()} في بوت الأحاديث!

    للبحث، أرسل رسالة تبدأ بـ `شيعة` أو `شيعه` متبوعة بكلمة البحث.
    مثال: `شيعة الحسين`

    استخدم الأمر /help لعرض المساعدة والإحصائيات.
    """
    await update.message.reply_html(welcome_message, reply_markup=keyboard)

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles the /help command."""
    log_user(update.effective_user.id)

    total_hadiths = get_total_hadiths_count() # Gets count from FTS table now
    search_count = get_stat('search_count')
    user_count = get_stat('user_count')

    help_text = f"""
    **مساعدة وإحصائيات بوت الأحاديث**

    📊 **الإحصائيات:**
    - عدد الأحاديث في قاعدة البيانات: {total_hadiths}
    - إجمالي عمليات البحث: {search_count}
    - عدد المستخدمين الفريدين: {user_count}

    🔍 **كيفية البحث:**
    أرسل رسالة تبدأ بـ `شيعة` أو `شيعه` ثم مسافة ثم الكلمة أو الجملة التي تريد البحث عنها.
    مثال: `شيعه علي بن ابي طالب`
    (يستخدم الآن البحث بالنص الكامل FTS لتحسين الأداء والدقة)

    ✨ **المطور:** {DEVELOPER_NAME}
    """
    await update.message.reply_html(help_text, disable_web_page_preview=True)


# --- Message Handler (handle_search) ---

async def handle_search(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles incoming messages starting with 'شيعة' or 'شيعه' followed by search terms."""
    user_id = update.effective_user.id
    message_text = update.message.text.strip()

    match = re.match(r'^(شيعة|شيعه)\s+(.+)', message_text, re.IGNORECASE | re.UNICODE)

    if not match:
        if message_text.lower() in ['شيعة', 'شيعه']:
            await update.message.reply_text("يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'. مثال: شيعة علي")
        else:
             logger.debug(f"Ignoring message from {user_id}: {message_text}")
        return

    search_query = match.group(2).strip()
    if not search_query:
         await update.message.reply_text("يرجى كتابة كلمة البحث بعد 'شيعة' أو 'شيعه'. مثال: شيعة علي")
         return

    log_user(user_id)
    update_stats('search_count')
    logger.info(f"User {user_id} searching (FTS) for: '{search_query}'")

    # --- MODIFIED: Use FTS search ---
    matching_rowids = search_hadiths_db(search_query)
    num_results = len(matching_rowids)

    if num_results == 0:
        await update.message.reply_text(f"لم يتم العثور على نتائج لكلمة البحث '{search_query}'.")
        return

    if num_results > 10:
        await update.message.reply_text(f"تم العثور على {num_results} نتيجة لكلمة البحث '{search_query}'. النتائج كثيرة جدًا ولن يتم عرض المقتطفات.")
        return

    response_text = f"نتائج البحث عن '{search_query}' ({num_results} نتيجة):\n\n"
    buttons = []
    # --- MODIFIED: Iterate through rowids ---
    for row_id in matching_rowids:
        # --- MODIFIED: Use row_id to get details ---
        hadith_details = get_hadith_details_by_db_id(row_id)
        if hadith_details:
            book = hadith_details['book'] if hadith_details['book'] else 'غير متوفر'
            text = hadith_details['arabic_text'] if hadith_details['arabic_text'] else ''
            snippet = text[:MAX_SNIPPET_LENGTH] + ('...' if len(text) > MAX_SNIPPET_LENGTH else '')

            button_text = f"📜 {book} - {snippet[:30]}..."
            # --- MODIFIED: Use row_id in callback data ---
            callback_data = f"view_{row_id}"
            buttons.append([InlineKeyboardButton(button_text, callback_data=callback_data)])

            response_text += f"📖 **الكتاب:** {book}\n📝 **مقتطف:** {snippet}\n\n---\n\n"
        else:
             logger.warning(f"Could not retrieve details for rowid {row_id} during snippet generation.")

    if buttons:
        await update.message.reply_text(response_text)
        keyboard = InlineKeyboardMarkup(buttons)
        await update.message.reply_text("اضغط على زر الحديث لعرضه كاملاً:", reply_markup=keyboard)
    else:
        await update.message.reply_text("حدث خطأ أثناء عرض النتائج.")


# --- Callback Query Handler (handle_button_click) ---

async def handle_button_click(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button clicks (callbacks)."""
    query = update.callback_query
    await query.answer()

    data = query.data
    logger.info(f"Callback received: {data}")

    try:
        action, value = data.split('_', 1)

        if action == "view":
            # --- MODIFIED: value is now rowid ---
            row_id = int(value)
            hadith_details = get_hadith_details_by_db_id(row_id)

            if hadith_details:
                full_text = format_hadith_from_row(hadith_details)
                parts = split_message(full_text)
                message_sent = await query.message.reply_text(text=parts[0])

                if len(parts) > 1:
                    # --- MODIFIED: Use row_id in callback data ---
                    callback_data_more = f"more_{message_sent.message_id}_{row_id}_1_{len(parts)}"
                    keyboard = InlineKeyboardMarkup([[
                        InlineKeyboardButton("المزيد 👇", callback_data=callback_data_more)
                    ]])
                    await context.bot.edit_message_reply_markup(
                        chat_id=query.message.chat_id,
                        message_id=message_sent.message_id,
                        reply_markup=keyboard
                    )
            else:
                await query.message.reply_text("خطأ: لم يتم العثور على تفاصيل الحديث المحدد في قاعدة البيانات.")

        elif action == "more":
            value_parts = value.split('_')
            if len(value_parts) == 4:
                original_message_id = int(value_parts[0])
                # --- MODIFIED: value_parts[1] is now rowid ---
                row_id = int(value_parts[1])
                next_part_index = int(value_parts[2])
                total_parts = int(value_parts[3])

                # --- MODIFIED: Use row_id to get details ---
                hadith_details = get_hadith_details_by_db_id(row_id)
                if hadith_details and 0 < next_part_index < total_parts:
                    full_text = format_hadith_from_row(hadith_details)
                    all_parts = split_message(full_text)
                    new_message = await query.message.reply_text(text=all_parts[next_part_index])

                    try:
                        await context.bot.edit_message_reply_markup(
                            chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None
                        )
                    except telegram.error.BadRequest as e:
                        # Ignore common errors when trying to remove button
                        if "message is not modified" in str(e) or "message to edit not found" in str(e):
                            logger.warning(f"Couldn't remove 'More' button from message {original_message_id}: {e}")
                        else:
                            logger.error(f"Error removing 'More' button from message {original_message_id}: {e}")

                    if next_part_index + 1 < total_parts:
                        # --- MODIFIED: Use row_id in callback data ---
                        callback_data_next = f"more_{new_message.message_id}_{row_id}_{next_part_index + 1}_{total_parts}"
                        keyboard = InlineKeyboardMarkup([[
                            InlineKeyboardButton("المزيد 👇", callback_data=callback_data_next)
                        ]])
                        await context.bot.edit_message_reply_markup(
                             chat_id=query.message.chat_id, message_id=new_message.message_id, reply_markup=keyboard
                        )
                else:
                     logger.warning(f"Invalid data or hadith not found for 'more' callback: {value}")
                     try: await context.bot.edit_message_reply_markup(chat_id=query.message.chat_id, message_id=original_message_id, reply_markup=None)
                     except Exception: pass

            else:
                 logger.warning(f"Received 'more' callback with unexpected data format: {value}")
                 try: await query.edit_message_reply_markup(reply_markup=None)
                 except Exception: pass

    except ValueError as e:
        logger.error(f"Error parsing callback data '{data}': {e}")
        await query.message.reply_text("حدث خطأ أثناء معالجة طلبك.")
    except Exception as e:
        logger.exception(f"An unexpected error occurred in handle_button_click for data {data}: {e}")
        try:
            await query.message.reply_text("عذراً، حدث خطأ غير متوقع.")
        except Exception as send_err:
             logger.error(f"Failed to send error message to user: {send_err}")


# --- Main Function ---

def main():
    """Starts the bot."""
    if BOT_TOKEN == "YOUR_BOT_TOKEN" or not BOT_TOKEN:
        logger.error("Please replace 'YOUR_BOT_TOKEN' with your actual bot token.")
        return

    try:
        init_db() # Initialize DB schema (creates FTS table if needed)
        populate_db_from_json(JSON_FILE) # Populate FTS table only if empty
    except Exception as e:
         logger.error(f"Failed to initialize or populate database. Exiting. Error: {e}")
         return

    application = Application.builder().token(BOT_TOKEN).build()

    # Register handlers (unchanged logic, but underlying functions now use FTS)
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", help_command))
    search_pattern = r'^(شيعة|شيعه)(\s+.*)?$'
    application.add_handler(MessageHandler(filters.Regex(search_pattern) & ~filters.COMMAND, handle_search))
    application.add_handler(CallbackQueryHandler(handle_button_click))

    logger.info("Bot starting with FTS5 support...")
    application.run_polling()
    logger.info("Bot stopped.")

if __name__ == '__main__':
    main()
