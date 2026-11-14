import os
import html
import json
import time
import urllib.parse
import asyncio
import threading
from pathlib import Path
from flask import Flask
from pymongo import MongoClient
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder,
    CommandHandler,
    MessageHandler,
    PollAnswerHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# ---------------- Render Web Service (Port Binding) ----------------
web_app = Flask(__name__)

@web_app.route("/")
def home():
    return "Telegram Quiz Bot with MongoDB is running successfully!"

@web_app.route("/health")
def health():
    return {"status": "ok", "quiz_status": quiz_state.get("status") if quiz_state else "idle"}, 200

def run_web():
    port = int(os.environ.get("PORT", 8080))
    web_app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

flask_thread = threading.Thread(target=run_web, daemon=True)
flask_thread.start()

# ---------------- Configuration & MongoDB Setup ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
ADMIN_ID = int(os.environ.get("ADMIN_ID", "0"))
DEFAULT_TIMER = int(os.environ.get("QUESTION_TIMER", "30"))
TIMER_OPTIONS = [15, 30, 45, 60, 90]
MONGO_URI = os.environ.get("MONGO_URI")

mongo_client = None
db = None
state_col = None
quizzes_col = None

pause_event = asyncio.Event()
pause_event.set()
active_polls = {}        # poll_id -> (mode, target_id, correct_index, quiz_id)
quiz_task = None
quiz_state = None
user_sessions = {}       # user_id -> self quiz session
group_sessions = {}      # chat_id -> shared group quiz session
group_tasks = {}         # chat_id -> asyncio.Task


def esc(val) -> str:
    """Safely escape dynamic text for Telegram HTML parse_mode."""
    return html.escape(str(val)) if val is not None else ""


def parse_question_item(item, index, total_q):
    """Safely extracts question text, options list (handles a/b/c/d & A/B/C/D), correct index, and explanation."""
    q_num = item.get("number", item.get("id", index + 1))
    q_text = f"[{index + 1}/{total_q}] Q{q_num}. {item.get('question', '')}"

    raw_options = item.get("options", {})
    if isinstance(raw_options, dict):
        norm_opts = {str(k).strip().upper(): v for k, v in raw_options.items()}
        options = [norm_opts[k] for k in ("A", "B", "C", "D") if k in norm_opts]
    elif isinstance(raw_options, list):
        options = raw_options[:4]
    else:
        options = []

    mapping = {"A": 0, "B": 1, "C": 2, "D": 3, "0": 0, "1": 1, "2": 2, "3": 3}
    correct_raw = str(item.get("answer", "A")).strip().upper()
    correct_index = mapping.get(correct_raw, 0)
    if correct_index >= len(options):
        correct_index = 0

    raw_exp = str(item.get("explanation", "")).strip()
    explanation = raw_exp[:200] if raw_exp else None

    return q_text, options, correct_index, explanation


# ---------------- MongoDB Persistence Functions ----------------
def init_db():
    global mongo_client, db, state_col, quizzes_col
    if not MONGO_URI:
        print("⚠️ WARNING: MONGO_URI set nahi hai! Data permanent save nahi hoga.")
        return
    try:
        mongo_client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            connectTimeoutMS=5000,
            socketTimeoutMS=10000,
        )
        mongo_client.admin.command("ping")
        db = mongo_client["telegram_quiz_bot"]
        state_col = db["active_state"]
        quizzes_col = db["saved_quizzes"]
        print("✅ MongoDB successfully connected!")
    except Exception as exc:
        print(f"❌ MongoDB Connection Error: {exc}")


def _sync_save_state(state_snapshot):
    if state_snapshot is None:
        return
    if state_col is not None:
        try:
            doc = dict(state_snapshot)
            doc["_id"] = "current_state"
            state_col.replace_one({"_id": "current_state"}, doc, upsert=True)
        except Exception as exc:
            print(f"❌ MongoDB save_state error: {exc}")
    save_quiz_to_library(state_snapshot)


def save_state():
    """Schedules non-blocking MongoDB save so event loop never freezes."""
    if quiz_state is None:
        return
    snapshot = dict(quiz_state)
    try:
        loop = asyncio.get_running_loop()
        loop.create_task(asyncio.to_thread(_sync_save_state, snapshot))
    except RuntimeError:
        _sync_save_state(snapshot)


def load_state():
    if state_col is None:
        return None
    try:
        doc = state_col.find_one({"_id": "current_state"})
        if doc:
            doc.pop("_id", None)
            return doc
    except Exception as exc:
        print(f"❌ MongoDB load_state error: {exc}")
    return None


def save_quiz_to_library(state_dict=None):
    """Stores a quiz permanently in MongoDB saved_quizzes collection."""
    target = state_dict or quiz_state
    if not target or not target.get("quiz_data"):
        return None
    quiz_id = str(target.get("quiz_id") or int(time.time()))
    target["quiz_id"] = quiz_id
    name = target.get("quiz_name") or f"Quiz-{quiz_id}"

    if quizzes_col is None:
        print("⚠️ quizzes_col is None! Check MONGO_URI.")
        return quiz_id

    try:
        payload_copy = dict(target)
        payload_copy.pop("_id", None)
        doc = {
            "_id": quiz_id,
            "quiz_id": quiz_id,
            "quiz_name": name,
            "saved_at": time.time(),
            "payload": payload_copy,
        }
        quizzes_col.replace_one({"_id": quiz_id}, doc, upsert=True)
        print(f"✅ Quiz '{name}' (ID: {quiz_id}) saved to MongoDB.")
    except Exception as exc:
        print(f"❌ MongoDB save_quiz_to_library error: {exc}")
    return quiz_id


def list_saved_quizzes():
    if quizzes_col is None:
        return []
    try:
        cursor = quizzes_col.find(
            {},
            {"quiz_id": 1, "quiz_name": 1, "payload.current_index": 1, "payload.total": 1, "payload.status": 1}
        ).sort("saved_at", -1)
        results = []
        for doc in cursor:
            payload = doc.get("payload", {})
            results.append((
                doc.get("quiz_id"),
                doc.get("quiz_name", "Untitled"),
                payload.get("current_index", 0),
                payload.get("total", 0),
                payload.get("status", "saved"),
            ))
        return results
    except Exception as exc:
        print(f"❌ MongoDB list_saved_quizzes error: {exc}")
        return []


def get_quiz_by_id(quiz_id=None):
    """Fetch a specific quiz from active state or MongoDB library."""
    if quiz_state and quiz_state.get("quiz_data"):
        if not quiz_id or str(quiz_state.get("quiz_id")) == str(quiz_id):
            return str(quiz_state.get("quiz_id", "current")), dict(quiz_state)

    if quizzes_col is not None:
        try:
            if quiz_id:
                doc = quizzes_col.find_one({"_id": str(quiz_id)})
            else:
                doc = quizzes_col.find_one(sort=[("saved_at", -1)])
            if doc and doc.get("payload"):
                return doc["quiz_id"], dict(doc["payload"])
        except Exception as exc:
            print(f"❌ MongoDB get_quiz_by_id error: {exc}")

    if quiz_state and quiz_state.get("quiz_data"):
        return str(quiz_state.get("quiz_id", "current")), dict(quiz_state)
    return None, None


def is_admin(update: Update) -> bool:
    return bool(update.effective_user and update.effective_user.id == ADMIN_ID and ADMIN_ID != 0)


# ---------------- Individual Self-Quiz Helper ----------------
async def send_self_quiz_question(bot, user_id):
    session = user_sessions.get(user_id)
    if not session or not session.get("active"):
        return

    quiz_data = session.get("quiz_data", [])
    total_q = len(quiz_data)
    idx = session.get("index", 0)
    quiz_id = session.get("quiz_id", "")

    if idx >= total_q:
        score = session.get("score", 0)
        pct = round((score / total_q) * 100, 1) if total_q > 0 else 0
        session["active"] = False

        bot_info = await bot.get_me()
        bot_username = bot_info.username
        replay_param = f"selfquiz_{quiz_id}" if quiz_id else "selfquiz"
        group_param = f"grpquiz_{quiz_id}" if quiz_id else "grpquiz"

        share_text = (
            f"🏆 Maine '{session.get('quiz_name', 'Mock Test')}' me {score}/{total_q} ({pct}%) score kiya!\n"
            f"👉 Aap bhi ye test dein: https://t.me/{bot_username}?start={replay_param}"
        )
        share_url = f"https://t.me/share/url?text={urllib.parse.quote(share_text)}"
        group_url = f"https://t.me/{bot_username}?startgroup={group_param}"

        keyboard = InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Dobara Practice Karein", url=f"https://t.me/{bot_username}?start={replay_param}")],
            [InlineKeyboardButton("👥 Apne Group Me Ye Quiz Chalayein", url=group_url)],
            [InlineKeyboardButton("📲 Score & Quiz Share Karein", url=share_url)],
        ])

        card = (
            "🎉 <b>SELF-TEST COMPLETE!</b> 🎯\n"
            "━━━━━━━━━━━━━━━━━━\n"
            f"📖 <b>Topic:</b> {esc(session.get('quiz_name', 'Mock Test'))}\n"
            f"📊 <b>Score:</b> <code>{score}/{total_q}</code> Marks\n"
            f"📈 <b>Accuracy:</b> <code>{pct}%</code>\n"
            "━━━━━━━━━━━━━━━━━━\n"
            "Niche diye buttons se aap is quiz ko share ya kisi bhi group me chala sakte hain!"
        )
        await bot.send_message(chat_id=user_id, text=card, reply_markup=keyboard, parse_mode="HTML")
        return

    item = quiz_data[idx]
    q_text, options, correct_index, explanation = parse_question_item(item, idx, total_q)

    if len(options) < 2:
        session["index"] = idx + 1
        await send_self_quiz_question(bot, user_id)
        return

    timer = session.get("timer", DEFAULT_TIMER)
    try:
        poll_kwargs = {
            "chat_id": user_id,
            "question": q_text[:300],
            "options": [str(opt)[:100] for opt in options],
            "type": "quiz",
            "correct_option_id": correct_index,
            "is_anonymous": False,
            "open_period": timer,
        }
        if explanation:
            poll_kwargs["explanation"] = explanation

        poll_msg = await bot.send_poll(**poll_kwargs)
        active_polls[poll_msg.poll.id] = ("self", user_id, correct_index, quiz_id)
    except Exception as exc:
        print(f"Self-poll error: {exc}")


# ---------------- Shared / Other Group Quiz Runner ----------------
async def run_shared_group_quiz(bot, target_chat_id):
    session = group_sessions.get(target_chat_id)
    if not session:
        return

    quiz_data = session["quiz_data"]
    total_q = len(quiz_data)
    timer = session.get("timer", DEFAULT_TIMER)
    quiz_name = session.get("quiz_name", "Mock Test")
    quiz_id = session.get("quiz_id", "")

    try:
        await bot.send_message(
            chat_id=target_chat_id,
            text=(
                "🔥 <b>SHARED QUIZ SHURU HO RAHA HAI!</b> 🔥\n"
                "━━━━━━━━━━━━━━━━━━\n"
                f"📖 <b>Topic:</b> {esc(quiz_name)}\n"
                f"📊 <b>Total Questions:</b> {total_q}\n"
                f"⏱️ <b>Timer:</b> {timer} seconds\n"
                "━━━━━━━━━━━━━━━━━━\n"
                "👉 Sabhi candidates taiyar ho jayein!"
            ),
            parse_mode="HTML",
        )
    except Exception as exc:
        print(f"Shared group start error: {exc}")
        session["active"] = False
        return

    for index, item in enumerate(quiz_data):
        if not session.get("active"):
            return

        q_text, options, correct_index, explanation = parse_question_item(item, index, total_q)

        if len(options) < 2:
            continue

        try:
            poll_kwargs = {
                "chat_id": target_chat_id,
                "question": q_text[:300],
                "options": [str(opt)[:100] for opt in options],
                "type": "quiz",
                "correct_option_id": correct_index,
                "is_anonymous": False,
                "open_period": timer,
            }
            if explanation:
                poll_kwargs["explanation"] = explanation

            poll_msg = await bot.send_poll(**poll_kwargs)
            active_polls[poll_msg.poll.id] = ("shared_group", target_chat_id, correct_index, quiz_id)
            await asyncio.sleep(timer + 2)
        except Exception as exc:
            print(f"Shared group poll error: {exc}")
            await asyncio.sleep(3)

    session["active"] = False
    await send_leaderboard(
        bot,
        target_chat_id,
        total_q,
        custom_names=session.get("names", {}),
        custom_scores=session.get("scores", {}),
        custom_quiz_name=quiz_name,
        custom_quiz_id=quiz_id,
    )


# ---------------- Commands ----------------
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    arg = context.args[0] if context.args else ""

    if arg.startswith("grpquiz"):
        if chat.type == "private":
            await update.message.reply_text("⚠️ Is link ko kisi Telegram Group me kholein taaki wahan quiz shuru ho sake.")
            return

        parts = arg.split("_", 1)
        req_id = parts[1] if len(parts) > 1 and parts[1] else None
        q_id, q_obj = await asyncio.to_thread(get_quiz_by_id, req_id)

        if not q_obj or not q_obj.get("quiz_data"):
            await update.message.reply_text("⚠️ Yeh quiz uplabdh nahi hai.")
            return

        if group_sessions.get(chat.id, {}).get("active"):
            await update.message.reply_text("⚠️ Is group me pehle se ek quiz chal raha hai. Uske khatam hone ka intezar karein.")
            return

        group_sessions[chat.id] = {
            "quiz_id": q_id,
            "quiz_name": q_obj.get("quiz_name", "Mock Test"),
            "quiz_data": q_obj.get("quiz_data", []),
            "timer": q_obj.get("timer", DEFAULT_TIMER),
            "scores": {},
            "names": {},
            "active": True,
        }
        group_tasks[chat.id] = asyncio.create_task(run_shared_group_quiz(context.bot, chat.id))
        return

    if arg.startswith("selfquiz"):
        parts = arg.split("_", 1)
        req_id = parts[1] if len(parts) > 1 and parts[1] else None
        q_id, q_obj = await asyncio.to_thread(get_quiz_by_id, req_id)

        if not q_obj or not q_obj.get("quiz_data"):
            await update.message.reply_text("⚠️ Abhi koi active ya saved quiz uplabdh nahi hai.")
            return

        user_id = update.effective_user.id
        user_sessions[user_id] = {
            "quiz_id": q_id,
            "quiz_name": q_obj.get("quiz_name", "Mock Test"),
            "quiz_data": q_obj.get("quiz_data", []),
            "timer": q_obj.get("timer", DEFAULT_TIMER),
            "index": 0,
            "score": 0,
            "active": True,
        }

        await update.message.reply_text(
            f"🎯 <b>Individual Self-Quiz Shuru Ho Raha Hai!</b>\n\n"
            f"📖 <b>Topic:</b> {esc(q_obj.get('quiz_name', 'Mock Test'))}\n"
            f"📊 <b>Questions:</b> {len(q_obj.get('quiz_data', []))}\n\n"
            f"Aap akele bina kisi shor ke practice karein. Har answer dene ke baad agla question apne aap aayega.",
            parse_mode="HTML"
        )
        await send_self_quiz_question(context.bot, user_id)
        return

    await update.message.reply_text(
        "👋 <b>Namaste! Quiz Bot Commands:</b>\n\n"
        "🔹 <code>/quizzes</code> - Sabhi completed/saved quizzes dekhein aur share karein\n"
        "🔹 <code>/selfquiz</code> - Bot ke saath akele (Private DM) me practice karein\n"
        "🔹 <code>/share</code> - Current quiz dosto ya groups me share karein\n"
        "🔹 <code>/status</code> - Current quiz progress check karein\n"
        "🔹 <code>/newquiz</code> - Nayi JSON quiz file upload karein (admin)\n"
        "🔹 <code>/savedquizzes</code> - Purane/Paused quizzes ko 1-click me resume karein (admin)\n"
        "🔹 <code>/restart</code> - Current quiz shuru se restart karein (admin)\n"
        "🔹 <code>/pause</code> | <code>/resume</code> | <code>/stop</code> - Quiz control (admin)",
        parse_mode="HTML"
    )


async def selfquiz_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    bot_info = await context.bot.get_me()
    req_id = context.args[0] if context.args else None
    q_id, q_obj = await asyncio.to_thread(get_quiz_by_id, req_id)

    if update.effective_chat.type != "private":
        param = f"selfquiz_{q_id}" if q_id else "selfquiz"
        link = f"https://t.me/{bot_info.username}?start={param}"
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton("🎯 Inbox me Self-Quiz Shuru Karein", url=link)]])
        await update.message.reply_text("Self-Quiz akele practice karne ke liye niche click karein:", reply_markup=keyboard)
        return

    if not q_obj or not q_obj.get("quiz_data"):
        await update.message.reply_text("Abhi koi quiz loaded nahi hai. /quizzes se purane quiz dekhein.")
        return

    user_id = update.effective_user.id
    user_sessions[user_id] = {
        "quiz_id": q_id,
        "quiz_name": q_obj.get("quiz_name", "Mock Test"),
        "quiz_data": q_obj.get("quiz_data", []),
        "timer": q_obj.get("timer", DEFAULT_TIMER),
        "index": 0,
        "score": 0,
        "active": True,
    }
    await update.message.reply_text(f"🎯 <b>{esc(q_obj.get('quiz_name', 'Self-Quiz'))}</b> shuru ho raha hai...", parse_mode="HTML")
    await send_self_quiz_question(context.bot, user_id)


async def quizzes_library_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    rows = await asyncio.to_thread(list_saved_quizzes)
    bot_info = await context.bot.get_me()
    bot_username = bot_info.username

    items = []
    seen_ids = set()

    if quiz_state and quiz_state.get("quiz_data"):
        cur_id = str(quiz_state.get("quiz_id", ""))
        if cur_id:
            seen_ids.add(cur_id)
            items.append((cur_id, quiz_state.get("quiz_name", "Current Quiz"), len(quiz_state["quiz_data"])))

    for q_id, name, done, total, _ in rows:
        if str(q_id) not in seen_ids:
            seen_ids.add(str(q_id))
            items.append((str(q_id), name, total or 0))

    if not items:
        await update.message.reply_text("Abhi library me koi quiz uplabdh nahi hai.")
        return

    lines = ["📚 <b>AVAILABLE QUIZZES LIBRARY:</b>\nKisi bhi quiz par click karke akele practice karein ya apne group me chalayein:\n"]
    buttons = []
    for idx, (q_id, name, total) in enumerate(items[:10], start=1):
        lines.append(f"{idx}. <b>{esc(name)}</b> ({total} Qs)")
        self_url = f"https://t.me/{bot_username}?start=selfquiz_{q_id}"
        group_url = f"https://t.me/{bot_username}?startgroup=grpquiz_{q_id}"
        buttons.append([
            InlineKeyboardButton(f"🎯 Self #{idx}", url=self_url),
            InlineKeyboardButton(f"👥 Group #{idx}", url=group_url),
        ])

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def ask_for_quiz_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global quiz_state
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin naya quiz shuru kar sakta hai.")
        return

    if quiz_state and quiz_state.get("status") == "running":
        await update.message.reply_text("⚠️ Pehle /pause karein, phir nayi JSON upload karein.")
        return

    if quiz_state and quiz_state.get("quiz_data"):
        await asyncio.to_thread(save_quiz_to_library, dict(quiz_state))

    context.user_data["waiting_for_file"] = True
    context.user_data["waiting_for_timer"] = False
    context.user_data["waiting_for_topic"] = False
    context.user_data["temp_source"] = None
    await update.message.reply_text("📂 Kripya valid <code>.json</code> quiz file upload karein.", parse_mode="HTML")


def build_topic_keyboard(topics, prefix="topic"):
    rows, row = [], []
    for i, t in enumerate(topics):
        row.append(InlineKeyboardButton(t[:60], callback_data=f"{prefix}:{i}"))
        if len(row) == 2:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    if prefix == "topic":
        rows.append([InlineKeyboardButton("📚 Sab Topics (All)", callback_data="topic:all")])
    return InlineKeyboardMarkup(rows)


def build_timer_keyboard(make_callback):
    row = [InlineKeyboardButton(f"⏱️ {s}s", callback_data=make_callback(s)) for s in TIMER_OPTIONS]
    return InlineKeyboardMarkup([row[:3], row[3:]])


async def handle_json_file(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        return

    doc = update.message.document
    if not doc or not doc.file_name or not doc.file_name.lower().endswith(".json"):
        if context.user_data.get("waiting_for_file"):
            await update.message.reply_text("❌ Kripya sirf <code>.json</code> file hi bhejein.", parse_mode="HTML")
        return

    if not context.user_data.get("waiting_for_file"):
        if quiz_state and quiz_state.get("status") == "running":
            await update.message.reply_text("⚠️ Pehle chal rahe quiz ko /pause karein, phir nayi JSON bhejein.")
            return
        if quiz_state and quiz_state.get("quiz_data"):
            await asyncio.to_thread(save_quiz_to_library, dict(quiz_state))

    tg_file = await doc.get_file()
    file_bytes = await tg_file.download_as_bytearray()

    try:
        quiz_data = json.loads(file_bytes.decode("utf-8"))
        if not isinstance(quiz_data, list) or not quiz_data:
            raise ValueError("JSON format non-empty questions ki list hona chahiye.")
        for i, item in enumerate(quiz_data, start=1):
            if not isinstance(item, dict) or "question" not in item or "options" not in item or "answer" not in item:
                raise ValueError(f"Question {i} me question/options/answer field missing hai.")
    except Exception as exc:
        await update.message.reply_text(f"❌ JSON file error: {exc}")
        return

    context.user_data["temp_quiz_data"] = quiz_data
    context.user_data["temp_quiz_name"] = Path(doc.file_name).stem
    context.user_data["temp_source"] = None
    context.user_data["waiting_for_file"] = False

    topics = []
    for item in quiz_data:
        t = str(item.get("topic", "")).strip()
        if t and t not in topics:
            topics.append(t)

    if len(topics) > 1:
        context.user_data["temp_topics"] = topics
        context.user_data["waiting_for_topic"] = True
        context.user_data["waiting_for_timer"] = False
        await update.message.reply_text(
            f"✅ <b>JSON Valid:</b> Total {len(quiz_data)} questions mile, {len(topics)} topics.\n\n"
            f"📖 Kaun sa topic chahiye? Niche se chunein:",
            reply_markup=build_topic_keyboard(topics),
            parse_mode="HTML",
        )
        return

    context.user_data["waiting_for_topic"] = False
    context.user_data["waiting_for_timer"] = True
    await update.message.reply_text(
        f"✅ <b>JSON Valid:</b> Total {len(quiz_data)} questions load hue hain.\n\n"
        f"⏱️ Har question ka timer chunein (ya seconds me likh kar bhejein):",
        reply_markup=build_timer_keyboard(lambda s: f"timer:{s}"),
        parse_mode="HTML",
    )


async def topic_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(update):
        return
    if not context.user_data.get("waiting_for_topic"):
        await query.edit_message_text("⚠️ Ye selection ab valid nahi hai. Naya file upload karein (/newquiz).")
        return

    choice = query.data.split(":", 1)[1]
    all_data = context.user_data["temp_quiz_data"]
    topics = context.user_data["temp_topics"]
    base_name = context.user_data["temp_quiz_name"]

    if choice == "all":
        selected = all_data
        label = base_name
    else:
        topic = topics[int(choice)]
        selected = [q for q in all_data if str(q.get("topic", "")).strip() == topic]
        label = topic

    context.user_data["temp_source"] = {"data": all_data, "topics": topics, "name": base_name}
    context.user_data["temp_quiz_data"] = selected
    context.user_data["temp_quiz_name"] = label
    context.user_data["waiting_for_topic"] = False
    context.user_data["waiting_for_timer"] = True

    await query.edit_message_text(
        f"✅ Topic: {label}\n"
        f"Total {len(selected)} questions.\n\n"
        f"⏱️ Ab timer chunein (ya seconds me likh kar bhejein):",
        reply_markup=build_timer_keyboard(lambda s: f"timer:{s}"),
    )


async def launch_quiz(bot, quiz_data, quiz_name, timer_val, source=None, chat_id=None):
    """Naya quiz shuru karta hai. Purana quiz MongoDB library me save hota hai."""
    global quiz_state, quiz_task

    if chat_id is None:
        raw_chat_id = os.environ.get("TARGET_CHAT_ID")
        if not raw_chat_id:
            raise ValueError("Render Environment me TARGET_CHAT_ID set nahi hai.")
        chat_id = int(raw_chat_id)

    archived_id = None
    if quiz_state and quiz_state.get("quiz_data"):
        if quiz_state.get("status") == "running":
            quiz_state["status"] = "paused"
        archived_id = await asyncio.to_thread(save_quiz_to_library, dict(quiz_state))
        if quiz_task is not None and not quiz_task.done():
            quiz_task.cancel()
            try:
                await quiz_task
            except asyncio.CancelledError:
                pass

    new_quiz_id = str(int(time.time()))
    quiz_state = {
        "quiz_id": new_quiz_id,
        "quiz_name": quiz_name,
        "chat_id": chat_id,
        "quiz_data": quiz_data,
        "current_index": 0,
        "timer": timer_val,
        "scores": {},
        "names": {},
        "status": "running",
        "total": len(quiz_data),
    }
    if source:
        quiz_state["source_data"] = source["data"]
        quiz_state["source_topics"] = source["topics"]
        quiz_state["source_name"] = source["name"]

    save_state()
    active_polls.clear()
    pause_event.set()
    quiz_task = asyncio.create_task(run_quiz(bot))
    return new_quiz_id, archived_id


async def timer_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(update):
        return
    if not context.user_data.get("waiting_for_timer"):
        await query.edit_message_text("⚠️ Ye selection ab valid nahi hai. Naya file upload karein (/newquiz).")
        return

    timer_val = int(query.data.split(":", 1)[1])
    context.user_data["waiting_for_timer"] = False

    quiz_data = context.user_data.get("temp_quiz_data")
    quiz_name = context.user_data.get("temp_quiz_name")
    source = context.user_data.get("temp_source")
    chat_id = context.user_data.pop("temp_chat_id", None)
    context.user_data["temp_source"] = None

    try:
        _, archived_id = await launch_quiz(context.bot, quiz_data, quiz_name, timer_val, source=source, chat_id=chat_id)
    except ValueError as exc:
        await query.edit_message_text(f"❌ {exc}")
        return

    saved_note = "\n🗂️ Purana quiz MongoDB me save ho gaya (/savedquizzes se kabhi bhi resume karein)." if archived_id else ""
    await query.edit_message_text(
        f"🚀 <b>Quiz Ready! Shuru ho raha hai...</b>\n\n"
        f"📌 <b>Topic:</b> {esc(quiz_name)}\n"
        f"📊 <b>Questions:</b> {len(quiz_data)}\n"
        f"⏱️ <b>Timer:</b> {timer_val}s per question{saved_note}",
        parse_mode="HTML",
    )


async def handle_text_messages(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.user_data.get("waiting_for_timer") or not is_admin(update):
        return

    text = update.message.text.strip()
    if not text.isdigit() or int(text) < 10 or int(text) > 300:
        await update.message.reply_text("⚠️ Kripya 10 se 300 seconds ke beech ka koi number bhejein (jaise: 30), ya upar ka button dabayein.")
        return

    timer_val = int(text)
    context.user_data["waiting_for_timer"] = False

    quiz_data = context.user_data.get("temp_quiz_data")
    quiz_name = context.user_data.get("temp_quiz_name")
    source = context.user_data.get("temp_source")
    chat_id = context.user_data.pop("temp_chat_id", None)
    context.user_data["temp_source"] = None

    try:
        _, archived_id = await launch_quiz(context.bot, quiz_data, quiz_name, timer_val, source=source, chat_id=chat_id)
    except ValueError as exc:
        await update.message.reply_text(f"❌ {exc}")
        return

    saved_note = f"\n🗂️ Purana quiz MongoDB library me save ho gaya (ID: <code>{esc(archived_id)}</code>)." if archived_id else ""
    await update.message.reply_text(
        f"🚀 <b>Quiz Ready! Shuru ho raha hai...</b>\n\n"
        f"📌 <b>Topic:</b> {esc(quiz_name)}\n"
        f"📊 <b>Questions:</b> {len(quiz_data)}\n"
        f"⏱️ <b>Timer:</b> {timer_val}s per question{saved_note}",
        parse_mode="HTML"
    )


async def next_topic_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(update):
        return
    if not quiz_state or quiz_state.get("status") != "completed" or not quiz_state.get("source_topics"):
        await query.edit_message_text("⚠️ Ye selection ab valid nahi hai. Naya file upload karein (/newquiz).")
        return

    topics = quiz_state["source_topics"]
    src = quiz_state["source_data"]
    try:
        idx = int(query.data.split(":", 1)[1])
        topic = topics[idx]
    except (ValueError, IndexError):
        await query.edit_message_text("⚠️ Topic samajh nahi aaya. Dobara chunein.")
        return

    selected = [q for q in src if str(q.get("topic", "")).strip() == topic]
    if not selected:
        await query.edit_message_text("⚠️ Is topic me koi question nahi mila.")
        return

    # Save in user_data too so typing timer (e.g. "15") works alongside inline buttons
    context.user_data["temp_quiz_data"] = selected
    context.user_data["temp_quiz_name"] = topic
    context.user_data["temp_source"] = {
        "data": src,
        "topics": topics,
        "name": quiz_state.get("source_name", ""),
    }
    context.user_data["temp_chat_id"] = quiz_state["chat_id"]
    context.user_data["waiting_for_topic"] = False
    context.user_data["waiting_for_timer"] = True

    await query.edit_message_text(
        f"📖 Topic: <b>{esc(topic)}</b>\n"
        f"📊 Total {len(selected)} questions.\n\n"
        f"⏱️ Ab timer chunein (ya seconds me likh kar bhejein):",
        reply_markup=build_timer_keyboard(lambda s: f"nexttimer:{idx}:{s}"),
        parse_mode="HTML",
    )


async def next_timer_selected(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(update):
        return
    if not quiz_state or quiz_state.get("status") != "completed" or not quiz_state.get("source_topics"):
        await query.edit_message_text("⚠️ Ye selection ab valid nahi hai. Naya file upload karein (/newquiz).")
        return

    try:
        _, idx_s, sec_s = query.data.split(":")
        idx, timer_val = int(idx_s), int(sec_s)
        topics = quiz_state["source_topics"]
        topic = topics[idx]
    except (ValueError, IndexError):
        await query.edit_message_text("⚠️ Selection samajh nahi aayi. Dobara chunein.")
        return

    src = quiz_state["source_data"]
    selected = [q for q in src if str(q.get("topic", "")).strip() == topic]
    if not selected:
        await query.edit_message_text("⚠️ Is topic me koi question nahi mila.")
        return

    context.user_data["waiting_for_timer"] = False
    context.user_data["temp_source"] = None
    context.user_data.pop("temp_chat_id", None)

    source = {
        "data": src,
        "topics": topics,
        "name": quiz_state.get("source_name", ""),
    }
    chat_id = quiz_state["chat_id"]

    await query.edit_message_text(
        f"🚀 Agla topic shuru ho raha hai: <b>{esc(topic)}</b>\n"
        f"📊 {len(selected)} questions, ⏱️ {timer_val}s per question.",
        parse_mode="HTML",
    )
    await launch_quiz(context.bot, selected, topic, timer_val, source=source, chat_id=chat_id)


async def restart_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global quiz_state, quiz_task
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin quiz restart kar sakta hai.")
        return

    if not quiz_state or not quiz_state.get("quiz_data"):
        await update.message.reply_text("⚠️ Restart karne ke liye koi quiz nahi mila. Pehle /newquiz se upload karein.")
        return

    if quiz_task is not None and not quiz_task.done():
        quiz_task.cancel()
        try:
            await quiz_task
        except asyncio.CancelledError:
            pass

    quiz_state["current_index"] = 0
    quiz_state["scores"] = {}
    quiz_state["names"] = {}
    quiz_state["status"] = "running"
    save_state()
    active_polls.clear()
    pause_event.set()

    await update.message.reply_text(
        f"🔄 <b>Quiz Restart Kiya Gaya!</b>\n\n"
        f"📖 <b>Topic:</b> {esc(quiz_state.get('quiz_name', 'Mock Test'))}\n"
        f"Question 1 se live test dobara shuru ho raha hai...",
        parse_mode="HTML"
    )
    quiz_task = asyncio.create_task(run_quiz(context.bot))


async def share_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    req_id = context.args[0] if context.args else None
    q_id, q_obj = await asyncio.to_thread(get_quiz_by_id, req_id)

    if not q_obj or not q_obj.get("quiz_data"):
        await update.message.reply_text("Abhi koi active ya saved quiz nahi hai.")
        return

    name = q_obj.get("quiz_name", "Live Mock Test")
    total = q_obj.get("total", len(q_obj.get("quiz_data", [])))
    timer = q_obj.get("timer", DEFAULT_TIMER)

    bot_info = await context.bot.get_me()
    bot_username = bot_info.username
    self_param = f"selfquiz_{q_id}" if q_id else "selfquiz"
    group_param = f"grpquiz_{q_id}" if q_id else "grpquiz"

    self_url = f"https://t.me/{bot_username}?start={self_param}"
    group_url = f"https://t.me/{bot_username}?startgroup={group_param}"

    share_text = (
        f"🎯 QUIZ CHALLENGE 🎯\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📖 Topic: {name}\n"
        f"📝 Total Questions: {total}\n"
        f"⏱️ Time Limit: {timer}s per question\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"👉 Akele Self-Test dene ke liye click karein: {self_url}\n"
        f"👉 Apne Group me chalane ke liye click karein: {group_url}"
    )

    encoded_text = urllib.parse.quote(share_text)
    share_url = f"https://t.me/share/url?text={encoded_text}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Akele Self-Quiz Karein (Inbox)", url=self_url)],
        [InlineKeyboardButton("👥 Apne Group Me Ye Quiz Chalayein", url=group_url)],
        [InlineKeyboardButton("📲 Dosto / Groups Me Share Karein", url=share_url)],
    ])

    await update.message.reply_text(
        f"📢 <b>{esc(name)} — Share &amp; Play Links:</b>",
        reply_markup=keyboard,
        parse_mode="HTML"
    )


async def pause_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global quiz_state
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin quiz pause kar sakta hai.")
        return
    if not quiz_state or quiz_state.get("status") != "running":
        await update.message.reply_text("⚠️ Koi running quiz nahi hai.")
        return

    quiz_state["status"] = "paused"
    save_state()
    pause_event.clear()
    await update.message.reply_text("⏸️ Quiz pause ho gaya hai. /resume se jaari karein.")


async def switch_and_resume_quiz(bot, requested_id):
    """Helper to switch active quiz to a saved quiz from MongoDB and resume it."""
    global quiz_state, quiz_task
    q_id, loaded_obj = await asyncio.to_thread(get_quiz_by_id, requested_id)
    if not loaded_obj:
        return False, "⚠️ Saved quiz nahi mila. /savedquizzes dekhein."

    if quiz_state and quiz_state.get("status") == "running" and str(quiz_state.get("quiz_id")) != str(q_id):
        return False, "⚠️ Pehle abhi chal rahe quiz ko /pause karein, phir dusra quiz resume karein."

    if quiz_state and quiz_state.get("quiz_data") and str(quiz_state.get("quiz_id")) != str(q_id):
        await asyncio.to_thread(save_quiz_to_library, dict(quiz_state))

    quiz_state = loaded_obj
    quiz_state["quiz_id"] = q_id
    quiz_state["status"] = "running"

    if quiz_state.get("current_index", 0) >= len(quiz_state.get("quiz_data", [])):
        quiz_state["current_index"] = 0
        quiz_state["scores"] = {}
        quiz_state["names"] = {}

    save_state()
    active_polls.clear()
    pause_event.set()

    if quiz_task is not None and not quiz_task.done():
        quiz_task.cancel()
        try:
            await quiz_task
        except asyncio.CancelledError:
            pass

    quiz_task = asyncio.create_task(run_quiz(bot))
    next_q = quiz_state["current_index"] + 1
    total_q = len(quiz_state.get("quiz_data", []))
    return True, f"▶️ <b>{esc(quiz_state.get('quiz_name', 'Quiz'))}</b> resume ho gaya! Question {next_q}/{total_q} se chal raha hai."


async def resume_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global quiz_state, quiz_task
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin quiz resume kar sakta hai.")
        return

    requested_id = context.args[0] if context.args else None

    if quiz_state and quiz_state.get("status") == "paused" and not requested_id:
        quiz_state["status"] = "running"
        save_state()
        pause_event.set()
        if quiz_task is None or quiz_task.done():
            quiz_task = asyncio.create_task(run_quiz(context.bot))
        await update.message.reply_text(
            f"▶️ Quiz resume ho gaya! Question {quiz_state['current_index'] + 1}/{quiz_state['total']} se shuru ho raha hai."
        )
        return

    if not requested_id:
        await update.message.reply_text("⚠️ Kis quiz ko resume karna hai? /savedquizzes bhejein aur button par click karein.", parse_mode="HTML")
        return

    ok, msg = await switch_and_resume_quiz(context.bot, requested_id)
    await update.message.reply_text(msg, parse_mode="HTML")


async def resume_saved_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if not is_admin(update):
        return

    q_id = query.data.split(":", 1)[1]
    ok, msg = await switch_and_resume_quiz(context.bot, q_id)
    await query.message.reply_text(msg, parse_mode="HTML")


async def savedquizzes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin saved quizzes dekh sakta hai.")
        return

    rows = await asyncio.to_thread(list_saved_quizzes)
    items = []
    seen = set()

    if quiz_state and quiz_state.get("quiz_data"):
        cur_id = str(quiz_state.get("quiz_id", "current"))
        seen.add(cur_id)
        items.append((
            cur_id,
            quiz_state.get("quiz_name", "Current Quiz"),
            quiz_state.get("current_index", 0),
            quiz_state.get("total", len(quiz_state["quiz_data"])),
            quiz_state.get("status", "running"),
        ))

    for q_id, name, done, total, status in rows:
        if str(q_id) not in seen:
            seen.add(str(q_id))
            items.append((str(q_id), name, done, total, status))

    if not items:
        await update.message.reply_text("Koi saved quiz nahi hai.")
        return

    lines = ["🗂️ <b>Saved Quizzes Library (MongoDB):</b>\nNiche kisi bhi quiz ke <b>▶️ Resume</b> button par click karke wahin se chalu karein:\n"]
    buttons = []

    for idx, (q_id, name, done, total, status) in enumerate(items[:12], start=1):
        total = total or 0
        if status == "running":
            progress_txt = f"🟢 Running (Q.{done + 1}/{total})"
        elif done >= total and total > 0:
            progress_txt = "Completed (Restart hoga)"
        else:
            progress_txt = f"Q.{done + 1}/{total} se baki"
        lines.append(f"{idx}. <b>{esc(name)}</b> — <i>{progress_txt}</i>")
        buttons.append([
            InlineKeyboardButton(f"▶️ Resume #{idx}: {name[:28]}", callback_data=f"resumesaved:{q_id}")
        ])

    await update.message.reply_text(
        "\n".join(lines),
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode="HTML"
    )


async def status_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not quiz_state:
        await update.message.reply_text("Abhi koi active quiz nahi hai.")
        return

    done = quiz_state.get("current_index", 0)
    total = quiz_state.get("total", len(quiz_state.get("quiz_data", [])))
    status = quiz_state.get("status", "unknown")
    await update.message.reply_text(
        f"📊 <b>Quiz Status:</b> <code>{esc(status.upper())}</code>\n"
        f"📖 <b>Topic:</b> {esc(quiz_state.get('quiz_name', 'N/A'))}\n"
        f"✅ <b>Progress:</b> {done}/{total} Questions\n"
        f"👥 <b>Active Participants:</b> {len(quiz_state.get('names', {}))}",
        parse_mode="HTML"
    )


async def stop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global quiz_state
    if not is_admin(update):
        await update.message.reply_text("❌ Sirf admin quiz stop kar sakta hai.")
        return
    if not quiz_state or quiz_state.get("status") not in ("running", "paused"):
        await update.message.reply_text("⚠️ Koi active quiz nahi hai.")
        return

    quiz_state["status"] = "stopped"
    save_state()
    pause_event.set()
    await update.message.reply_text("⏹️ Quiz band kar diya gaya hai aur MongoDB library me save kar diya gaya hai.")


# ---------------- Group Quiz Runner ----------------
async def run_quiz(bot):
    global quiz_state
    if not quiz_state:
        return

    chat_id = quiz_state["chat_id"]
    quiz_data = quiz_state["quiz_data"]
    total_q = len(quiz_data)
    timer = quiz_state.get("timer", DEFAULT_TIMER)
    cur_quiz_id = str(quiz_state.get("quiz_id", ""))

    if quiz_state.get("current_index", 0) == 0:
        try:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    "🔥 <b>QUIZ SHURU HO RAHA HAI!</b> 🔥\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    f"📖 <b>Topic:</b> {esc(quiz_state.get('quiz_name', 'Mock Test'))}\n"
                    f"📊 <b>Total Questions:</b> {total_q}\n"
                    f"⏱️ <b>Timer:</b> {timer} seconds\n"
                    "━━━━━━━━━━━━━━━━━━\n"
                    "👉 Sabhi candidates taiyar ho jayein!"
                ),
                parse_mode="HTML",
            )
        except Exception as exc:
            print(f"Start message error: {exc}")

    while quiz_state and quiz_state.get("current_index", 0) < total_q:
        await pause_event.wait()

        if not quiz_state or quiz_state.get("status") == "stopped":
            return
        if quiz_state.get("status") == "paused":
            await asyncio.sleep(1)
            continue

        index = quiz_state["current_index"]
        item = quiz_data[index]
        q_text, options, correct_index, explanation = parse_question_item(item, index, total_q)

        if len(options) < 2:
            quiz_state["current_index"] = index + 1
            save_state()
            continue

        try:
            poll_kwargs = {
                "chat_id": chat_id,
                "question": q_text[:300],
                "options": [str(opt)[:100] for opt in options],
                "type": "quiz",
                "correct_option_id": correct_index,
                "is_anonymous": False,
                "open_period": timer,
            }
            if explanation:
                poll_kwargs["explanation"] = explanation

            poll_msg = await bot.send_poll(**poll_kwargs)
            active_polls[poll_msg.poll.id] = ("group", chat_id, correct_index, cur_quiz_id)
            await asyncio.sleep(timer + 2)
        except Exception as exc:
            print(f"Poll send error: {exc}")
            await asyncio.sleep(3)

        if not quiz_state or str(quiz_state.get("quiz_id", "")) != cur_quiz_id:
            return
        if quiz_state.get("status") != "running":
            continue

        quiz_state["current_index"] = index + 1
        save_state()

    if quiz_state and quiz_state.get("status") == "running" and str(quiz_state.get("quiz_id", "")) == cur_quiz_id:
        quiz_state["status"] = "completed"
        save_state()
        saved_id = await asyncio.to_thread(save_quiz_to_library, dict(quiz_state))
        await send_leaderboard(bot, chat_id, total_q, custom_quiz_id=saved_id)

        if quiz_state.get("source_topics") and ADMIN_ID:
            try:
                await bot.send_message(
                    chat_id=ADMIN_ID,
                    text=(
                        f"✅ <b>{esc(quiz_state.get('quiz_name', 'Quiz'))}</b> complete ho gaya.\n\n"
                        f"📚 Agla topic chunein (phir timer chunenge):"
                    ),
                    reply_markup=build_topic_keyboard(quiz_state["source_topics"], prefix="nexttopic"),
                    parse_mode="HTML",
                )
            except Exception as exc:
                print(f"Next-topic menu error: {exc}")


# ---------------- Poll Answer Handler ----------------
async def handle_poll_answer(update: Update, context: ContextTypes.DEFAULT_TYPE):
    answer = update.poll_answer
    user = answer.user
    full_name = f"{user.first_name} {user.last_name or ''}".strip()

    if answer.poll_id not in active_polls:
        return

    mode, target_id, correct_option_id, poll_quiz_id = active_polls[answer.poll_id]

    if mode == "group":
        if not quiz_state or str(quiz_state.get("quiz_id", "")) != str(poll_quiz_id):
            return
        uid = str(user.id)
        quiz_state.setdefault("names", {})[uid] = full_name
        quiz_state.setdefault("scores", {}).setdefault(uid, 0)
        if answer.option_ids and answer.option_ids[0] == correct_option_id:
            quiz_state["scores"][uid] += 1
        save_state()

    elif mode == "shared_group":
        session = group_sessions.get(target_id)
        if session and session.get("active"):
            uid = str(user.id)
            session.setdefault("names", {})[uid] = full_name
            session.setdefault("scores", {}).setdefault(uid, 0)
            if answer.option_ids and answer.option_ids[0] == correct_option_id:
                session["scores"][uid] += 1

    elif mode == "self":
        user_id = target_id
        session = user_sessions.get(user_id)
        if session and session.get("active"):
            if answer.option_ids and answer.option_ids[0] == correct_option_id:
                session["score"] = session.get("score", 0) + 1
            session["index"] = session.get("index", 0) + 1
            await asyncio.sleep(1.5)
            await send_self_quiz_question(context.bot, user_id)


# ---------------- Stylish Leaderboard ----------------
async def send_leaderboard(
    bot,
    chat_id,
    total_q,
    custom_names=None,
    custom_scores=None,
    custom_quiz_name=None,
    custom_quiz_id=None,
):
    names = custom_names if custom_names is not None else (quiz_state.get("names", {}) if quiz_state else {})
    scores = custom_scores if custom_scores is not None else (quiz_state.get("scores", {}) if quiz_state else {})
    quiz_name = custom_quiz_name or (quiz_state.get("quiz_name", "Test") if quiz_state else "Test")
    quiz_id = str(custom_quiz_id or (quiz_state.get("quiz_id", "") if quiz_state else ""))

    bot_info = await bot.get_me()
    bot_username = bot_info.username

    sorted_users = sorted(names.keys(), key=lambda uid: scores.get(uid, 0), reverse=True)
    topper_id = sorted_users[0] if sorted_users else None
    topper_score = scores.get(topper_id, 0) if topper_id else 0
    topper_pct = round((topper_score / total_q) * 100, 1) if total_q > 0 and topper_id else 0

    res = (
        "╔════════════════════════╗\n"
        "       🏆 <b>FINAL LEADERBOARD</b> 🏆\n"
        "╚════════════════════════╝\n\n"
        f"📖 <b>Topic:</b> {esc(quiz_name)}\n"
        f"📊 <b>Questions:</b> {total_q}\n"
        f"👥 <b>Total Participants:</b> {len(sorted_users)}\n"
        "────────────────────────\n\n"
    )

    if topper_id:
        res += (
            f"👑 <b>TOPPER (RANK 1)</b>\n"
            f"🥇 <b>{esc(names[topper_id])}</b>\n"
            f"🎯 <b>Score:</b> <code>{topper_score}/{total_q}</code> ({topper_pct}%)\n"
            "🌟 <i>Outstanding Performance!</i> 👏\n\n"
            "📋 <b>TOP CANDIDATES SCORECARD:</b>\n"
        )
        rank_icons = {1: "🥇", 2: "🥈", 3: "🥉"}
        for rank, uid in enumerate(sorted_users[:15], start=1):
            sc = scores.get(uid, 0)
            pct = round((sc / total_q) * 100, 1) if total_q > 0 else 0
            icon = rank_icons.get(rank, f"<code>#{rank:02d}</code>")
            res += f"{icon} <b>{esc(names[uid])}</b> ➔ <code>{sc}/{total_q}</code> ({pct}%)\n"
    else:
        res += "Kisi bhi candidate ne attempt nahi kiya.\n"

    res += (
        "\n────────────────────────\n"
        "💡 <b>Test chhut gaya ya apne group me chalana hai?</b> Niche diye buttons se aap is quiz ko akele inbox me ya kisi bhi group me chala sakte hain!"
    )

    self_param = f"selfquiz_{quiz_id}" if quiz_id else "selfquiz"
    group_param = f"grpquiz_{quiz_id}" if quiz_id else "grpquiz"
    self_url = f"https://t.me/{bot_username}?start={self_param}"
    group_url = f"https://t.me/{bot_username}?startgroup={group_param}"

    share_text = (
        f"🏆 {quiz_name} Result: Topper score {topper_score}/{total_q}!\n"
        f"👉 Akele Self-Quiz dene ke liye: {self_url}\n"
        f"👉 Apne Group me chalane ke liye: {group_url}"
    )
    share_url = f"https://t.me/share/url?text={urllib.parse.quote(share_text)}"

    keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton("🎯 Akele Self-Quiz Karein (Inbox)", url=self_url)],
        [InlineKeyboardButton("👥 Dusre Group Me Ye Quiz Chalayein", url=group_url)],
        [InlineKeyboardButton("📲 Result / Quiz Link Share Karein", url=share_url)],
    ])

    await bot.send_message(chat_id=chat_id, text=res, reply_markup=keyboard, parse_mode="HTML")


# ---------------- Startup / Errors ----------------
async def restore_quiz(application):
    global quiz_state, quiz_task
    quiz_state = await asyncio.to_thread(load_state)

    if quiz_state and quiz_state.get("status") == "running":
        pause_event.set()
        quiz_task = asyncio.create_task(run_quiz(application.bot))
        print(">> Running quiz MongoDB se restore ho gaya.")
    elif quiz_state and quiz_state.get("status") == "paused":
        pause_event.clear()
        print(">> Quiz paused hai. /resume se continue karein.")
    else:
        pause_event.set()


async def error_handler(update, context):
    print(f">> Telegram Error: {context.error}")


def main():
    token = os.environ.get("BOT_TOKEN")
    if not token:
        print("❌ CRITICAL ERROR: BOT_TOKEN Environment variable set nahi hai!")
        while True:
            time.sleep(3600)

    init_db()

    app = (
        ApplicationBuilder()
        .token(token)
        .connect_timeout(30.0)
        .read_timeout(30.0)
        .write_timeout(30.0)
        .pool_timeout(30.0)
        .post_init(restore_quiz)
        .build()
    )

    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("selfquiz", selfquiz_command))
    app.add_handler(CommandHandler("quizzes", quizzes_library_command))
    app.add_handler(CommandHandler("newquiz", ask_for_quiz_file))
    app.add_handler(CommandHandler("restart", restart_command))
    app.add_handler(CommandHandler("share", share_command))
    app.add_handler(CommandHandler("pause", pause_command))
    app.add_handler(CommandHandler("resume", resume_command))
    app.add_handler(CommandHandler("savedquizzes", savedquizzes_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("stop", stop_command))
    app.add_handler(CallbackQueryHandler(topic_selected, pattern=r"^topic:"))
    app.add_handler(CallbackQueryHandler(timer_selected, pattern=r"^timer:"))
    app.add_handler(CallbackQueryHandler(next_topic_selected, pattern=r"^nexttopic:"))
    app.add_handler(CallbackQueryHandler(next_timer_selected, pattern=r"^nexttimer:"))
    app.add_handler(CallbackQueryHandler(resume_saved_callback, pattern=r"^resumesaved:"))
    app.add_handler(MessageHandler(filters.Document.ALL, handle_json_file))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_messages))
    app.add_handler(PollAnswerHandler(handle_poll_answer))
    app.add_error_handler(error_handler)

    print(">> Telegram Quiz Bot polling shuru ho chuki hai...")
    app.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
