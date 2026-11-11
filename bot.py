"""
Professional Personal Telegram Forward & Channel Manager Bot
Architecture:
  - Telethon: Personal User Account message forwarding & bulk join request approval
  - python-telegram-bot (v21+): Owner-only inline keyboard dashboard
  - aiosqlite: SQLite WAL engine for multi-task support
  - Flask: Web health server binding automatically to Render's dynamic PORT
"""

import asyncio
import datetime
import html
import json
import logging
import os
import sys
import threading
from typing import Any, Dict, List, Optional, Set, Tuple

from dotenv import load_dotenv
from flask import Flask, jsonify

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from telethon import TelegramClient, errors, utils
from telethon.sessions import StringSession
from telethon.tl.functions.messages import (
    GetChatInviteImportersRequest,
    HideAllChatJoinRequestsRequest,
    HideChatJoinRequestRequest,
)
from telethon.tl.types import (
    Channel,
    Chat,
    DocumentAttributeAudio,
    DocumentAttributeVideo,
    InputUserEmpty,
    MessageMediaContact,
    MessageMediaDocument,
    MessageMediaGeo,
    MessageMediaPhoto,
    MessageMediaPoll,
    User,
)

# ---------------------------------------------------------
# 1. ENVIRONMENT & CONFIGURATION
# ---------------------------------------------------------
load_dotenv()

BOT_TOKEN: str = os.getenv("BOT_TOKEN", "").strip()
API_ID_RAW: str = os.getenv("API_ID", "0").strip()
API_HASH: str = os.getenv("API_HASH", "").strip()
OWNER_ID_RAW: str = os.getenv("OWNER_ID", "0").strip()

if not BOT_TOKEN or not API_HASH or API_ID_RAW == "0" or OWNER_ID_RAW == "0":
    print("CRITICAL: BOT_TOKEN, API_ID, API_HASH, and OWNER_ID must be set in Environment Variables")

API_ID: int = int(API_ID_RAW) if API_ID_RAW.isdigit() else 0
OWNER_ID: int = int(OWNER_ID_RAW) if OWNER_ID_RAW.isdigit() else 0

DB_PATH: str = "forward_manager.db"
PORT: int = int(os.getenv("PORT", 8080))

logging.basicConfig(
    format="%(asctime)s - [%(levelname)s] - %(name)s: %(message)s",
    level=logging.INFO,
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("ForwardManager")

# ---------------------------------------------------------
# 2. FLASK SERVER FOR RENDER KEEP-ALIVE
# ---------------------------------------------------------
flask_app = Flask(__name__)


@flask_app.route("/")
def home():
    return jsonify(
        status="running",
        service="Telegram Forward & Channel Manager Bot",
        timestamp=datetime.datetime.now(datetime.timezone.utc).isoformat(),
    )


@flask_app.route("/health")
def health():
    return jsonify(status="healthy"), 200


def run_flask():
    logger.info("Starting background Flask server on port %s for Render...", PORT)
    flask_app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)


# ---------------------------------------------------------
# 3. DATABASE LAYER (aiosqlite with WAL mode for Multi-Tasking)
# ---------------------------------------------------------
import aiosqlite


class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            await db.execute("PRAGMA busy_timeout=15000;")
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS saved_chats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER UNIQUE,
                    title TEXT,
                    username TEXT,
                    chat_type TEXT,
                    category TEXT
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_id INTEGER,
                    source_title TEXT,
                    dest_ids TEXT,
                    dest_titles TEXT,
                    mode TEXT,
                    quantity INTEGER,
                    bunch_size INTEGER DEFAULT 10,
                    order_dir TEXT,
                    direction_filter TEXT,
                    types_filter TEXT,
                    include_keywords TEXT,
                    exclude_keywords TEXT,
                    status TEXT,
                    total_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0,
                    current_index INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER,
                    source_title TEXT,
                    dest_titles TEXT,
                    mode TEXT,
                    forwarded_count INTEGER,
                    failed_count INTEGER,
                    status TEXT,
                    error_log TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP
                )
                """
            )
            await db.commit()

            defaults = {
                "default_mode": "COPY",
                "default_quantity": "50",
                "default_bunch": "20",
                "default_order": "OLDEST_FIRST",
                "delay_between_messages": "0.3",
            }
            for k, v in defaults.items():
                await db.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (k, v),
                )
            await db.commit()

    async def get_setting(self, key: str, default: str = "") -> str:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT value FROM settings WHERE key = ?", (key,)
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else default

    async def set_setting(self, key: str, value: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )
            await db.commit()

    async def add_saved_chat(
        self,
        chat_id: int,
        title: str,
        username: str,
        chat_type: str,
        category: str,
    ):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO saved_chats (chat_id, title, username, chat_type, category)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    title = excluded.title,
                    username = excluded.username,
                    chat_type = excluded.chat_type,
                    category = excluded.category
                """,
                (chat_id, title, username or "", chat_type, category),
            )
            await db.commit()

    async def get_saved_chats(self, category: str) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM saved_chats WHERE category = ? ORDER BY id DESC",
                (category,),
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def remove_saved_chat(self, chat_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "DELETE FROM saved_chats WHERE chat_id = ?", (chat_id,)
            )
            await db.commit()

    async def create_task(self, task_data: Dict[str, Any]) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                """
                INSERT INTO tasks (
                    source_id, source_title, dest_ids, dest_titles, mode,
                    quantity, bunch_size, order_dir, direction_filter, types_filter,
                    include_keywords, exclude_keywords, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    task_data["source_id"],
                    task_data["source_title"],
                    json.dumps(task_data["dest_ids"]),
                    json.dumps(task_data["dest_titles"]),
                    task_data["mode"],
                    task_data["quantity"],
                    task_data.get("bunch_size", 20),
                    task_data["order_dir"],
                    task_data["direction_filter"],
                    json.dumps(task_data.get("types_filter", [])),
                    task_data.get("include_keywords", ""),
                    task_data.get("exclude_keywords", ""),
                    "PENDING",
                ),
            )
            await db.commit()
            return cursor.lastrowid

    async def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM tasks WHERE id = ?", (task_id,)
            ) as cursor:
                row = await cursor.fetchone()
                return dict(row) if row else None

    async def get_tasks(self) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM tasks ORDER BY id DESC"
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def update_task_status(
        self,
        task_id: int,
        status: str,
        total: Optional[int] = None,
        success: Optional[int] = None,
        failed: Optional[int] = None,
        current_idx: Optional[int] = None,
    ):
        async with aiosqlite.connect(self.db_path) as db:
            updates = ["status = ?", "updated_at = CURRENT_TIMESTAMP"]
            params: List[Any] = [status]
            if total is not None:
                updates.append("total_count = ?")
                params.append(total)
            if success is not None:
                updates.append("success_count = ?")
                params.append(success)
            if failed is not None:
                updates.append("failed_count = ?")
                params.append(failed)
            if current_idx is not None:
                updates.append("current_index = ?")
                params.append(current_idx)
            params.append(task_id)

            await db.execute(
                f"UPDATE tasks SET {', '.join(updates)} WHERE id = ?", params
            )
            await db.commit()

    async def add_history(self, history_data: Dict[str, Any]):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO history (
                    task_id, source_title, dest_titles, mode,
                    forwarded_count, failed_count, status, error_log,
                    start_time, end_time
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    history_data["task_id"],
                    history_data["source_title"],
                    history_data["dest_titles"],
                    history_data["mode"],
                    history_data["forwarded_count"],
                    history_data["failed_count"],
                    history_data["status"],
                    history_data.get("error_log", ""),
                    history_data["start_time"],
                    history_data["end_time"],
                ),
            )
            await db.commit()

    async def get_history(self, limit: int = 15) -> List[Dict[str, Any]]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            async with db.execute(
                "SELECT * FROM history ORDER BY id DESC LIMIT ?", (limit,)
            ) as cursor:
                rows = await cursor.fetchall()
                return [dict(r) for r in rows]

    async def clear_history(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM history")
            await db.commit()


db = Database(DB_PATH)

# ---------------------------------------------------------
# 4. TELETHON USER ACCOUNT MANAGER
# ---------------------------------------------------------
class UserAccountManager:
    def __init__(self, api_id: int, api_hash: str):
        self.api_id = api_id
        self.api_hash = api_hash
        self.client: Optional[TelegramClient] = None
        self.phone_code_hash: Optional[str] = None
        self.temp_phone: Optional[str] = None

    async def get_client(self) -> TelegramClient:
        if self.client is None or not self.client.is_connected():
            saved_str = await db.get_setting("user_string_session", "")
            session = StringSession(saved_str) if saved_str else StringSession()
            self.client = TelegramClient(session, self.api_id, self.api_hash)
            await self.client.connect()
        return self.client

    async def is_authorized(self) -> bool:
        client = await self.get_client()
        return await client.is_user_authorized()

    async def send_login_code(self, phone: str) -> str:
        client = await self.get_client()
        res = await client.send_code_request(phone)
        self.phone_code_hash = res.phone_code_hash
        self.temp_phone = phone
        return res.phone_code_hash

    async def complete_login(
        self, code: str, password: Optional[str] = None
    ) -> Tuple[bool, str]:
        client = await self.get_client()
        try:
            if not self.temp_phone or not self.phone_code_hash:
                return False, "Session expired. Please enter phone number again."
            await client.sign_in(
                phone=self.temp_phone,
                code=code,
                phone_code_hash=self.phone_code_hash,
            )
            saved_session = client.session.save()
            await db.set_setting("user_string_session", saved_session)
            self.phone_code_hash = None
            self.temp_phone = None
            me = await client.get_me()
            return True, f"Logged in as {utils.get_display_name(me)}!"
        except errors.SessionPasswordNeededError:
            if password:
                try:
                    await client.sign_in(password=password)
                    saved_session = client.session.save()
                    await db.set_setting("user_string_session", saved_session)
                    self.phone_code_hash = None
                    self.temp_phone = None
                    me = await client.get_me()
                    return True, f"Logged in as {utils.get_display_name(me)}!"
                except Exception as e:
                    return False, f"2FA Password incorrect: {str(e)}"
            return False, "2FA_REQUIRED"
        except Exception as e:
            return False, f"Sign-in failed: {str(e)}"

    async def logout(self):
        client = await self.get_client()
        if await client.is_user_authorized():
            await client.log_out()
        await db.set_setting("user_string_session", "")
        self.client = None

    async def get_me(self) -> Optional[User]:
        client = await self.get_client()
        if await client.is_user_authorized():
            return await client.get_me()
        return None

    async def fetch_dialogs(self, limit: int = 150) -> List[Dict[str, Any]]:
        client = await self.get_client()
        if not await client.is_user_authorized():
            return []
        dialogs = []
        async for d in client.iter_dialogs(limit=limit):
            entity = d.entity
            chat_type = "Personal Chat"
            if d.is_channel:
                chat_type = "Channel" if not d.is_group else "Supergroup"
            elif d.is_group:
                chat_type = "Group"

            username = getattr(entity, "username", None) or ""
            dialogs.append(
                {
                    "id": d.id,
                    "title": d.name or "Unnamed Chat",
                    "username": f"@{username}" if username else "",
                    "type": chat_type,
                    "is_channel_or_group": d.is_channel or d.is_group,
                }
            )
        return dialogs

    async def validate_entity(self, chat_id_or_username: Any) -> Optional[Dict[str, Any]]:
        client = await self.get_client()
        try:
            entity = await client.get_entity(chat_id_or_username)
            title = utils.get_display_name(entity)
            username = getattr(entity, "username", None) or ""
            chat_type = "Personal Chat"
            if isinstance(entity, Channel):
                chat_type = "Channel" if not entity.megagroup else "Supergroup"
            elif isinstance(entity, Chat):
                chat_type = "Group"
            return {
                "id": utils.get_peer_id(entity),
                "title": title,
                "username": f"@{username}" if username else "",
                "type": chat_type,
            }
        except Exception as e:
            logger.error("Failed to fetch entity %s: %s", chat_id_or_username, e)
            return None

    # Join Request Operations
    async def get_pending_requests_count(self, chat_id_or_username: Any) -> Tuple[bool, int, str]:
        client = await self.get_client()
        try:
            entity = await client.get_entity(chat_id_or_username)
            input_peer = await client.get_input_entity(entity)
            res = await client(
                GetChatInviteImportersRequest(
                    peer=input_peer,
                    offset_date=0,
                    offset_user=InputUserEmpty(),
                    limit=1,
                    requested=True,
                )
            )
            return True, getattr(res, "count", 0), utils.get_display_name(entity)
        except Exception as e:
            logger.error("get_pending_requests_count error: %s", e)
            return False, 0, str(e)

    async def accept_or_reject_all_requests(
        self, chat_id_or_username: Any, approve: bool = True
    ) -> Tuple[bool, str, int]:
        client = await self.get_client()
        try:
            entity = await client.get_entity(chat_id_or_username)
            input_peer = await client.get_input_entity(entity)
            action_name = "approved" if approve else "rejected"

            bulk_success = False
            try:
                await client(HideAllChatJoinRequestsRequest(peer=input_peer, approved=approve))
                bulk_success = True
            except Exception as bulk_err:
                logger.info("Bulk request failed (%s), running safe batch fallback...", bulk_err)

            total_handled = 0
            offset_date = 0
            offset_user = InputUserEmpty()

            while True:
                res = await client(
                    GetChatInviteImportersRequest(
                        peer=input_peer,
                        offset_date=offset_date,
                        offset_user=offset_user,
                        limit=100,
                        requested=True,
                    )
                )
                if not res.importers:
                    break

                for importer in res.importers:
                    try:
                        importer_user = await client.get_input_entity(importer.user_id)
                        await client(
                            HideChatJoinRequestRequest(
                                peer=input_peer,
                                user_id=importer_user,
                                approved=approve,
                            )
                        )
                        total_handled += 1
                        await asyncio.sleep(0.05)
                    except Exception as imp_err:
                        logger.debug("Failed single approval: %s", imp_err)

                if len(res.importers) < 100:
                    break

                last_imp = res.importers[-1]
                offset_date = last_imp.date
                offset_user = await client.get_input_entity(last_imp.user_id)

            if bulk_success and total_handled == 0:
                return True, f"All pending join requests {action_name} successfully!", 0
            elif total_handled > 0:
                return True, f"Successfully {action_name} {total_handled} join request(s)!", total_handled
            else:
                return True, f"No pending join requests found to {action_name}.", 0

        except Exception as e:
            logger.error("accept_or_reject_all_requests error: %s", e)
            return False, f"Failed: {str(e)}", 0


user_mgr = UserAccountManager(API_ID, API_HASH)

# ---------------------------------------------------------
# 5. ASYNC FORWARD ENGINE (MULTI-TASK & BUNCH/BATCH FORWARDING)
# ---------------------------------------------------------
class ForwardEngine:
    def __init__(self):
        self.active_tasks: Dict[int, asyncio.Task] = {}
        self.task_events: Dict[int, asyncio.Event] = {}
        self.task_stops: Set[int] = set()

    def is_running(self, task_id: int) -> bool:
        return (
            task_id in self.active_tasks
            and not self.active_tasks[task_id].done()
        )

    def pause_task(self, task_id: int):
        if task_id in self.task_events:
            self.task_events[task_id].clear()

    def resume_task(self, task_id: int):
        if task_id in self.task_events:
            self.task_events[task_id].set()

    def stop_task(self, task_id: int):
        self.task_stops.add(task_id)
        if task_id in self.task_events:
            self.task_events[task_id].set()

    def start_task(
        self,
        task_id: int,
        context: ContextTypes.DEFAULT_TYPE,
        status_msg_chat_id: int,
        status_msg_id: int,
    ):
        event = asyncio.Event()
        event.set()
        self.task_events[task_id] = event
        self.task_stops.discard(task_id)
        coro = self._run_forward_task(
            task_id, context, status_msg_chat_id, status_msg_id
        )
        task = asyncio.create_task(coro)
        self.active_tasks[task_id] = task
        return task

    async def _run_forward_task(
        self,
        task_id: int,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        msg_id: int,
    ):
        task = await db.get_task(task_id)
        if not task:
            return

        client = await user_mgr.get_client()
        if not await client.is_user_authorized():
            await db.update_task_status(task_id, "FAILED")
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"❌ <b>Task #{task_id} Failed:</b> Telegram account is not logged in.",
                parse_mode=ParseMode.HTML,
            )
            return

        source_id = task["source_id"]
        dest_ids = json.loads(task["dest_ids"])
        mode = task["mode"]
        quantity = task["quantity"]
        bunch_size = task.get("bunch_size", 20) or 20
        order_dir = task["order_dir"]
        dir_filter = task["direction_filter"]
        types_filter = set(json.loads(task["types_filter"]))
        inc_kw = (task["include_keywords"] or "").lower()
        exc_kw = (task["exclude_keywords"] or "").lower()

        delay_str = await db.get_setting("delay_between_messages", "0.3")
        try:
            delay = float(delay_str)
        except ValueError:
            delay = 0.3

        await db.update_task_status(task_id, "RUNNING")
        start_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        await context.bot.edit_message_text(
            chat_id=chat_id,
            message_id=msg_id,
            text=f"⚡ <b>Task #{task_id}:</b> Scanning messages for Bunch forwarding (Batch: {bunch_size})...",
            parse_mode=ParseMode.HTML,
        )

        messages_to_process = []
        try:
            fetch_limit = None if quantity == 0 else min(quantity * 3, 5000)
            async for msg in client.iter_messages(source_id, limit=fetch_limit):
                if not msg:
                    continue

                if dir_filter == "SENT_ONLY" and not msg.out:
                    continue
                if dir_filter == "RECEIVED_ONLY" and msg.out:
                    continue

                m_type = self._classify_message(msg)
                if types_filter and m_type not in types_filter:
                    continue

                msg_text = (msg.text or "").lower()
                if inc_kw and inc_kw not in msg_text:
                    continue
                if exc_kw and exc_kw in msg_text:
                    continue

                messages_to_process.append(msg)
                if quantity > 0 and len(messages_to_process) >= quantity:
                    break

        except Exception as e:
            logger.error("Scan error: %s", e)
            await db.update_task_status(task_id, "FAILED")
            await context.bot.send_message(
                chat_id=OWNER_ID,
                text=f"❌ Message fetching error on Task #{task_id}: {str(e)}",
            )
            return

        if order_dir == "OLDEST_FIRST":
            messages_to_process.reverse()

        total = len(messages_to_process)
        await db.update_task_status(task_id, "RUNNING", total=total)

        if total == 0:
            await db.update_task_status(task_id, "COMPLETED")
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=f"⚠️ <b>Task #{task_id} Completed:</b> No matching messages found for your filters.",
                parse_mode=ParseMode.HTML,
            )
            return

        # Divide into Bunches / Batches
        bunch_chunks = [
            messages_to_process[i:i + bunch_size]
            for i in range(0, total, bunch_size)
        ]

        success_count = 0
        failed_count = 0
        error_logs = []
        last_update_time = datetime.datetime.now(datetime.timezone.utc).timestamp()
        processed_messages_count = 0

        for chunk in bunch_chunks:
            if task_id in self.task_stops:
                await db.update_task_status(
                    task_id,
                    "STOPPED",
                    success=success_count,
                    failed=failed_count,
                    current_idx=processed_messages_count,
                )
                await self._render_status(
                    context, chat_id, msg_id, task_id, "STOPPED",
                    processed_messages_count, total, success_count, failed_count,
                    task["source_title"]
                )
                return

            event = self.task_events.get(task_id)
            if event and not event.is_set():
                await db.update_task_status(
                    task_id,
                    "PAUSED",
                    success=success_count,
                    failed=failed_count,
                    current_idx=processed_messages_count,
                )
                await self._render_status(
                    context, chat_id, msg_id, task_id, "PAUSED",
                    processed_messages_count, total, success_count, failed_count,
                    task["source_title"]
                )
                await event.wait()
                if task_id in self.task_stops:
                    return
                await db.update_task_status(task_id, "RUNNING")

            # Execute Bunch Forwarding
            chunk_msg_ids = [m.id for m in chunk]
            chunk_size_current = len(chunk)

            for dest_id in dest_ids:
                if mode == "ORIGINAL":
                    try:
                        await client.forward_messages(
                            dest_id, chunk_msg_ids, from_peer=source_id
                        )
                        success_count += chunk_size_current
                    except errors.FloodWaitError as fwe:
                        logger.warning("FloodWait: Sleeping %d seconds", fwe.seconds)
                        await asyncio.sleep(fwe.seconds + 2)
                        try:
                            await client.forward_messages(
                                dest_id, chunk_msg_ids, from_peer=source_id
                            )
                            success_count += chunk_size_current
                        except Exception as ex:
                            failed_count += chunk_size_current
                            error_logs.append(f"Bunch error: {str(ex)}")
                    except Exception as ex:
                        failed_count += chunk_size_current
                        error_logs.append(f"Bunch error: {str(ex)}")
                else:
                    # COPY MODE (clean sequential inside bunch)
                    for single_msg in chunk:
                        try:
                            if single_msg.media:
                                await client.send_file(
                                    dest_id,
                                    file=single_msg.media,
                                    caption=single_msg.text or "",
                                )
                            elif single_msg.text:
                                await client.send_message(dest_id, single_msg.text)
                            success_count += 1
                        except errors.FloodWaitError as fwe:
                            await asyncio.sleep(fwe.seconds + 2)
                            try:
                                if single_msg.media:
                                    await client.send_file(
                                        dest_id,
                                        file=single_msg.media,
                                        caption=single_msg.text or "",
                                    )
                                elif single_msg.text:
                                    await client.send_message(dest_id, single_msg.text)
                                success_count += 1
                            except Exception as ex2:
                                failed_count += 1
                                error_logs.append(f"Msg #{single_msg.id}: {str(ex2)}")
                        except Exception as ex:
                            failed_count += 1
                            error_logs.append(f"Msg #{single_msg.id}: {str(ex)}")

            processed_messages_count += chunk_size_current

            now = datetime.datetime.now(datetime.timezone.utc).timestamp()
            if now - last_update_time > 3 or processed_messages_count >= total:
                last_update_time = now
                await db.update_task_status(
                    task_id,
                    "RUNNING",
                    success=success_count,
                    failed=failed_count,
                    current_idx=processed_messages_count,
                )
                await self._render_status(
                    context, chat_id, msg_id, task_id, "RUNNING",
                    processed_messages_count, total, success_count, failed_count,
                    task["source_title"]
                )

            if delay > 0:
                await asyncio.sleep(delay)

        final_status = "COMPLETED" if failed_count == 0 else "COMPLETED_WITH_ERRORS"
        await db.update_task_status(
            task_id,
            final_status,
            success=success_count,
            failed=failed_count,
            current_idx=total,
        )
        end_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        await db.add_history(
            {
                "task_id": task_id,
                "source_title": task["source_title"],
                "dest_titles": ", ".join(json.loads(task["dest_titles"])),
                "mode": mode,
                "forwarded_count": success_count,
                "failed_count": failed_count,
                "status": final_status,
                "error_log": "\n".join(error_logs[:10]),
                "start_time": start_time,
                "end_time": end_time,
            }
        )

        await self._render_status(
            context,
            chat_id,
            msg_id,
            task_id,
            final_status,
            total,
            total,
            success_count,
            failed_count,
            task["source_title"],
        )

    def _classify_message(self, msg) -> str:
        if not msg.media:
            return "text"
        if isinstance(msg.media, MessageMediaPhoto):
            return "photo"
        if isinstance(msg.media, MessageMediaDocument):
            doc = msg.media.document
            if doc:
                for attr in doc.attributes:
                    if isinstance(attr, DocumentAttributeVideo):
                        return "video_note" if attr.round_message else "video"
                    if isinstance(attr, DocumentAttributeAudio):
                        return "voice" if attr.voice else "audio"
            return "document"
        if isinstance(msg.media, MessageMediaContact):
            return "contact"
        if isinstance(msg.media, MessageMediaGeo):
            return "location"
        if isinstance(msg.media, MessageMediaPoll):
            return "poll"
        return "other"

    async def _render_status(
        self,
        context: ContextTypes.DEFAULT_TYPE,
        chat_id: int,
        msg_id: int,
        task_id: int,
        status: str,
        current: int,
        total: int,
        success: int,
        failed: int,
        source_title: str,
    ):
        pct = int((current / total * 100)) if total > 0 else 0
        filled = int(pct / 10)
        bar = "▓" * filled + "░" * (10 - filled)

        status_emoji = {
            "RUNNING": "⚡ Processing (Bunch Mode)",
            "PAUSED": "⏸️ Paused",
            "STOPPED": "🛑 Stopped",
            "COMPLETED": "✅ Completed",
            "COMPLETED_WITH_ERRORS": "⚠️ Finished with Errors",
            "FAILED": "❌ Failed",
        }.get(status, status)

        text = (
            f"📊 <b>Task #{task_id} Progress</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Status:</b> {status_emoji}\n"
            f"<b>Source:</b> {html.escape(source_title)}\n"
            f"<b>Progress:</b> [{bar}] {pct}%\n"
            f"<b>Processed:</b> {current}/{total}\n"
            f"<b>✅ Success:</b> {success} | <b>❌ Failed:</b> {failed}\n"
            f"━━━━━━━━━━━━━━━━━━━"
        )

        buttons = []
        if status == "RUNNING":
            buttons.append(
                [
                    InlineKeyboardButton(
                        "⏸️ Pause", callback_data=f"task_pause_{task_id}"
                    ),
                    InlineKeyboardButton(
                        "🛑 Stop", callback_data=f"task_stop_{task_id}"
                    ),
                ]
            )
        elif status == "PAUSED":
            buttons.append(
                [
                    InlineKeyboardButton(
                        "▶️ Resume", callback_data=f"task_resume_{task_id}"
                    ),
                    InlineKeyboardButton(
                        "🛑 Stop", callback_data=f"task_stop_{task_id}"
                    ),
                ]
            )

        buttons.append(
            [InlineKeyboardButton("🔙 My Tasks", callback_data="menu_tasks")]
        )

        try:
            await context.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=text,
                parse_mode=ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup(buttons),
            )
        except Exception:
            pass


engine = ForwardEngine()

# ---------------------------------------------------------
# 6. UI HELPERS & NUMPAD BUILDER
# ---------------------------------------------------------
USER_STATE: Dict[int, Dict[str, Any]] = {}


async def safe_edit(target, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    try:
        if hasattr(target, "edit_message_text"):
            await target.edit_message_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        elif hasattr(target, "message") and hasattr(target.message, "edit_text"):
            await target.message.edit_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
        elif hasattr(target, "reply_text"):
            await target.reply_text(text=text, reply_markup=reply_markup, parse_mode=parse_mode)
    except Exception as e:
        if "Message is not modified" not in str(e):
            logger.debug("safe_edit caught: %s", e)


def get_otp_numpad_keyboard(current_code: str) -> InlineKeyboardMarkup:
    buttons = [
        [
            InlineKeyboardButton("1", callback_data="otp_d_1"),
            InlineKeyboardButton("2", callback_data="otp_d_2"),
            InlineKeyboardButton("3", callback_data="otp_d_3"),
        ],
        [
            InlineKeyboardButton("4", callback_data="otp_d_4"),
            InlineKeyboardButton("5", callback_data="otp_d_5"),
            InlineKeyboardButton("6", callback_data="otp_d_6"),
        ],
        [
            InlineKeyboardButton("7", callback_data="otp_d_7"),
            InlineKeyboardButton("8", callback_data="otp_d_8"),
            InlineKeyboardButton("9", callback_data="otp_d_9"),
        ],
        [
            InlineKeyboardButton("⌫ Del", callback_data="otp_d_del"),
            InlineKeyboardButton("0", callback_data="otp_d_0"),
            InlineKeyboardButton("🗑 Clear", callback_data="otp_d_clear"),
        ],
        [
            InlineKeyboardButton("✅ Submit Code", callback_data="otp_d_submit"),
        ],
        [
            InlineKeyboardButton("❌ Cancel Login", callback_data="menu_main"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    active_count = sum(1 for t in engine.active_tasks.values() if not t.done())
    tasks_btn_text = f"📋 My Tasks ({active_count} Running)" if active_count > 0 else "📋 My Tasks"
    buttons = [
        [
            InlineKeyboardButton(
                "🚀 New Forward Task", callback_data="wizard_start"
            )
        ],
        [
            InlineKeyboardButton(tasks_btn_text, callback_data="menu_tasks"),
            InlineKeyboardButton("📜 Forward History", callback_data="menu_history"),
        ],
        [
            InlineKeyboardButton("📂 Saved Sources", callback_data="menu_sources"),
            InlineKeyboardButton(
                "📁 Saved Destinations", callback_data="menu_destinations"
            ),
        ],
        [
            InlineKeyboardButton("👥 Join Requests (1-Click)", callback_data="menu_join_requests"),
            InlineKeyboardButton("👤 Account Info", callback_data="menu_account"),
        ],
        [
            InlineKeyboardButton("⚙️ Settings", callback_data="menu_settings"),
            InlineKeyboardButton("ℹ️ Help & Guide", callback_data="menu_help"),
        ],
    ]
    return InlineKeyboardMarkup(buttons)


def owner_only(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
        user_id = update.effective_user.id if update.effective_user else 0
        if user_id != OWNER_ID:
            if update.callback_query:
                await update.callback_query.answer("⛔ Unauthorized: Access Restricted to Owner.", show_alert=True)
            elif update.message:
                await update.message.reply_text("⛔ <b>Access Denied:</b> This bot is private.", parse_mode=ParseMode.HTML)
            return
        return await func(update, context, *args, **kwargs)

    return wrapper


@owner_only
async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    USER_STATE.pop(OWNER_ID, None)
    text = (
        "👑 <b>Personal Telegram Forward & Channel Manager</b>\n\n"
        "Welcome! Manage message forwarding (Bunch/Multi-task) and bulk channel join approvals from this dashboard."
    )
    if update.message:
        await update.message.reply_text(text, reply_markup=get_main_menu_keyboard(), parse_mode=ParseMode.HTML)
    elif update.callback_query:
        await safe_edit(update.callback_query, text, reply_markup=get_main_menu_keyboard())


# ------------------ ACCOUNT LOGIN (NUMPAD OTP) --------------------
@owner_only
async def handle_account_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    is_logged_in = await user_mgr.is_authorized()
    if is_logged_in:
        me = await user_mgr.get_me()
        first_name = html.escape(me.first_name if me else "Unknown")
        phone = html.escape(me.phone if me and me.phone else "Hidden")
        user_id = me.id if me else "Unknown"
        text = (
            "👤 <b>Connected Account Information</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Status:</b> ✅ Connected & Ready\n"
            f"<b>Name:</b> {first_name}\n"
            f"<b>Phone:</b> +{phone}\n"
            f"<b>User ID:</b> <code>{user_id}</code>\n"
            "━━━━━━━━━━━━━━━━━━━"
        )
        buttons = [
            [InlineKeyboardButton("🔴 Disconnect / Logout", callback_data="account_logout")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
        ]
    else:
        text = (
            "👤 <b>Connected Account Information</b>\n"
            "━━━━━━━━━━━━━━━━━━━\n"
            "<b>Status:</b> ❌ Disconnected\n"
            "Login with your phone number and OTP below (no manual session needed!):\n"
            "━━━━━━━━━━━━━━━━━━━"
        )
        buttons = [
            [InlineKeyboardButton("📱 Login with Phone & OTP", callback_data="account_login_phone")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
        ]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


@owner_only
async def handle_login_phone_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    USER_STATE[OWNER_ID] = {"stage": "WAITING_PHONE", "otp_buffer": ""}
    await safe_edit(
        query,
        "📱 <b>Account Login</b>\n\n"
        "Please reply with your phone number with country code (e.g., <code>+919876543210</code>):\n\n"
        "Send /cancel to abort.",
    )


async def render_otp_screen(target, phone: str, buffer: str):
    masked = " ".join(list(buffer)) + " " + " ".join(["_"] * max(0, 5 - len(buffer)))
    text = (
        f"📩 <b>Verification Code Sent!</b>\n\n"
        f"Phone: <code>{phone}</code>\n"
        f"Entered Code: <code>{masked}</code>\n\n"
        "👉 <b>Use the keypad below to enter your OTP:</b>\n"
        "<i>(Entering via buttons ensures Telegram never blocks the code!)</i>"
    )
    await safe_edit(target, text, reply_markup=get_otp_numpad_keyboard(buffer))


@owner_only
async def handle_logout_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await user_mgr.logout()
    await safe_edit(
        query,
        "✅ <b>Logged Out Successfully</b>\n\nYour session was removed cleanly.",
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]),
    )


# ------------------ JOIN REQUESTS (1-CLICK & REALTIME REFRESH) --------------------
@owner_only
async def show_join_requests_channels(query_or_msg, page: int = 0):
    if not await user_mgr.is_authorized():
        text = "⚠️ <b>Action Required:</b> Please connect your personal account first."
        buttons = [
            [InlineKeyboardButton("📱 Connect Account", callback_data="account_login_phone")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
        ]
        await safe_edit(query_or_msg, text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    dialogs = await user_mgr.fetch_dialogs(limit=100)
    channel_dialogs = [d for d in dialogs if d.get("is_channel_or_group")]

    page_size = 6
    start_idx = page * page_size
    end_idx = start_idx + page_size
    current_page = channel_dialogs[start_idx:end_idx]

    buttons = []
    for d in current_page:
        btn_text = f"[{d['type'][:4]}] {d['title'][:20]}"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"joinreq_sel_{d['id']}")])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"joinreq_page_{page-1}"))
    if end_idx < len(channel_dialogs):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"joinreq_page_{page+1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("✏️ Manual Channel ID / Username", callback_data="joinreq_manual")])
    buttons.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")])

    text = (
        "👥 <b>Channel Join Request Manager (1-Click)</b>\n\n"
        "Select your channel or group below to view and accept all pending join requests:"
    )
    await safe_edit(query_or_msg, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_channel_join_request_view(query_or_msg, chat_id: Any):
    ok, count, title = await user_mgr.get_pending_requests_count(chat_id)
    if not ok:
        text = f"❌ <b>Error fetching join requests:</b>\n<code>{html.escape(title)}</code>\n\nEnsure your account is Admin with 'Add Users' permission."
        buttons = [[InlineKeyboardButton("🔙 Back to Channels", callback_data="menu_join_requests")]]
        await safe_edit(query_or_msg, text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    text = (
        f"📢 <b>Channel:</b> {html.escape(title)}\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        f"⏳ <b>Pending Requests:</b> <code>{count}</code>\n"
        f"━━━━━━━━━━━━━━━━━━━\n"
        "Tap the button below to approve all join requests in 1-click:"
    )

    buttons = []
    if count > 0:
        buttons.append([InlineKeyboardButton("⚡ Accept All Requests (1-Click)", callback_data=f"joinreq_accept_{chat_id}")])
        buttons.append([InlineKeyboardButton("❌ Reject All Requests", callback_data=f"joinreq_reject_{chat_id}")])

    buttons.append([InlineKeyboardButton("🔄 Refresh Count", callback_data=f"joinreq_sel_{chat_id}")])
    buttons.append([InlineKeyboardButton("🔙 Back", callback_data="menu_join_requests")])
    await safe_edit(query_or_msg, text, reply_markup=InlineKeyboardMarkup(buttons))


# ------------------ TASK WIZARD --------------------
@owner_only
async def start_wizard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not await user_mgr.is_authorized():
        text = "⚠️ <b>Action Required:</b> Please connect your personal account first."
        buttons = [
            [InlineKeyboardButton("📱 Connect Account", callback_data="account_login_phone")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
        ]
        await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    USER_STATE[OWNER_ID] = {
        "wizard": {
            "source_id": None,
            "source_title": None,
            "dest_ids": [],
            "dest_titles": [],
            "quantity": int(await db.get_setting("default_quantity", "50")),
            "bunch_size": int(await db.get_setting("default_bunch", "20")),
            "order_dir": await db.get_setting("default_order", "OLDEST_FIRST"),
            "direction_filter": "ALL",
            "types_filter": [],
            "include_keywords": "",
            "exclude_keywords": "",
            "mode": await db.get_setting("default_mode", "COPY"),
        }
    }
    await show_source_selection_menu(query, page=0)


async def show_source_selection_menu(query, page: int = 0):
    dialogs = await user_mgr.fetch_dialogs(limit=100)
    page_size = 6
    start_idx = page * page_size
    end_idx = start_idx + page_size
    current_page = dialogs[start_idx:end_idx]

    buttons = []
    for d in current_page:
        btn_text = f"[{d['type'][:4]}] {d['title'][:22]}"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"wiz_src_sel_{d['id']}")])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"wiz_src_page_{page-1}"))
    if end_idx < len(dialogs):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"wiz_src_page_{page+1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("✏️ Manual Input", callback_data="wiz_src_manual"), InlineKeyboardButton("📂 From Saved", callback_data="wiz_src_saved")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="menu_main")])

    text = "🎯 <b>Step 1/5: Select Source Chat</b>\n\nChoose the chat/channel from which messages will be fetched:"
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_destination_selection_menu(query_or_msg, page: int = 0):
    wizard_data = USER_STATE.get(OWNER_ID, {}).get("wizard", {})
    selected_dests = wizard_data.get("dest_titles", [])

    dialogs = await user_mgr.fetch_dialogs(limit=100)
    page_size = 6
    start_idx = page * page_size
    end_idx = start_idx + page_size
    current_page = dialogs[start_idx:end_idx]

    buttons = []
    for d in current_page:
        is_selected = d["id"] in wizard_data.get("dest_ids", [])
        mark = "✅ " if is_selected else ""
        btn_text = f"{mark}[{d['type'][:4]}] {d['title'][:20]}"
        buttons.append([InlineKeyboardButton(btn_text, callback_data=f"wiz_dst_toggle_{d['id']}")])

    nav_row = []
    if page > 0:
        nav_row.append(InlineKeyboardButton("⬅️ Prev", callback_data=f"wiz_dst_page_{page-1}"))
    if end_idx < len(dialogs):
        nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"wiz_dst_page_{page+1}"))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton("✏️ Manual Input", callback_data="wiz_dst_manual"), InlineKeyboardButton("📁 From Saved", callback_data="wiz_dst_saved")])

    if wizard_data.get("dest_ids"):
        buttons.append([InlineKeyboardButton("➡️ Next: Quantity & Filters", callback_data="wiz_goto_filters")])
    buttons.append([InlineKeyboardButton("❌ Cancel", callback_data="menu_main")])

    selected_str = ", ".join(selected_dests) if selected_dests else "None selected yet"
    text = (
        f"🎯 <b>Step 2/5: Select Destination(s)</b>\n\n"
        f"<b>Selected:</b> {html.escape(selected_str)}\n\n"
        "Tap chats to add/remove multiple destinations:"
    )
    await safe_edit(query_or_msg, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_filters_menu(target):
    wiz = USER_STATE.get(OWNER_ID, {}).get("wizard", {})
    qty = wiz.get("quantity", 50)
    qty_str = "♾️ All Posts of the channel" if qty == 0 else f"{qty} messages"
    bunch = wiz.get("bunch_size", 20)
    order = "Oldest ➡️ Newest" if wiz.get("order_dir") == "OLDEST_FIRST" else "Newest ➡️ Oldest"
    direction = wiz.get("direction_filter", "ALL")
    types = ", ".join(wiz.get("types_filter", [])) or "All Types"
    inc_kw = wiz.get("include_keywords") or "None"
    exc_kw = wiz.get("exclude_keywords") or "None"

    text = (
        "🎯 <b>Step 3/5: Filters & Settings</b>\n\n"
        f"<b>Quantity:</b> <b>{qty_str}</b>\n"
        f"<b>📦 Bunch Size:</b> <b>{bunch} messages at once</b>\n"
        f"<b>Order:</b> {order}\n"
        f"<b>Direction:</b> {direction}\n"
        f"<b>Media Types:</b> {types}\n"
        f"<b>Must Include:</b> <code>{html.escape(inc_kw)}</code>\n"
        f"<b>Exclude:</b> <code>{html.escape(exc_kw)}</code>\n"
    )

    buttons = [
        [InlineKeyboardButton("🔢 Set Quantity", callback_data="wiz_set_qty"), InlineKeyboardButton("📦 Bunch Size", callback_data="wiz_set_bunch")],
        [InlineKeyboardButton("🔄 Toggle Order", callback_data="wiz_toggle_order"), InlineKeyboardButton("📨 Direction Filter", callback_data="wiz_toggle_dir")],
        [InlineKeyboardButton("🎞️ Media Types", callback_data="wiz_menu_types"), InlineKeyboardButton("➕ Include Keyword", callback_data="wiz_set_include_kw")],
        [InlineKeyboardButton("➖ Exclude Keyword", callback_data="wiz_set_exclude_kw"), InlineKeyboardButton("➡️ Next: Mode", callback_data="wiz_goto_mode")],
        [InlineKeyboardButton("❌ Cancel", callback_data="menu_main")],
    ]
    await safe_edit(target, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_media_types_menu(target):
    wiz = USER_STATE.get(OWNER_ID, {}).get("wizard", {})
    type_options = ["text", "photo", "video", "document", "voice", "video_note"]
    current_types = set(wiz.setdefault("types_filter", []))
    buttons = []
    for t in type_options:
        checked = "✅ " if t in current_types else "⬜ "
        buttons.append([InlineKeyboardButton(f"{checked}{t.capitalize()}", callback_data=f"wiz_type_toggle_{t}")])
    buttons.append([InlineKeyboardButton("🔙 Done & Return", callback_data="wiz_goto_filters")])

    text = "Select media filters (unselected allows all types):"
    await safe_edit(target, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_mode_menu(query):
    wiz = USER_STATE.get(OWNER_ID, {}).get("wizard", {})
    curr_mode = wiz.get("mode", "COPY")
    text = (
        "🎯 <b>Step 4/5: Forwarding Mode</b>\n\n"
        "• <b>Original Forward:</b> Telegram native forward, preserves sender signature.\n"
        "• <b>Copy Mode:</b> Reposts messages directly from your personal account cleanly without forward tag."
    )
    buttons = [
        [InlineKeyboardButton(f"{'✅ ' if curr_mode == 'ORIGINAL' else ''}Original Forward", callback_data="wiz_mode_ORIGINAL")],
        [InlineKeyboardButton(f"{'✅ ' if curr_mode == 'COPY' else ''}Copy Mode (Clean Repost)", callback_data="wiz_mode_COPY")],
        [InlineKeyboardButton("➡️ Next: Confirmation Screen", callback_data="wiz_goto_confirm")],
        [InlineKeyboardButton("❌ Cancel", callback_data="menu_main")],
    ]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


async def show_confirmation_screen(query):
    wiz = USER_STATE.get(OWNER_ID, {}).get("wizard", {})
    dests = ", ".join(wiz.get("dest_titles", []))
    qty = wiz.get("quantity")
    qty_str = "♾️ All Posts of the channel" if qty == 0 else f"{qty}"
    bunch = wiz.get("bunch_size", 20)

    text = (
        "📋 <b>Step 5/5: Confirm Forwarding Task</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Source:</b> {html.escape(wiz.get('source_title', ''))}\n"
        f"<b>Destination(s):</b> {html.escape(dests)}\n"
        f"<b>Quantity:</b> <b>{qty_str}</b>\n"
        f"<b>📦 Bunch Size:</b> <b>{bunch} msgs/batch</b>\n"
        f"<b>Order:</b> {wiz.get('order_dir')}\n"
        f"<b>Direction:</b> {wiz.get('direction_filter')}\n"
        f"<b>Mode:</b> {wiz.get('mode')}\n"
        f"<b>Types:</b> {', '.join(wiz.get('types_filter', [])) or 'All'}\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        "Start forwarding now at ultra-fast speed?"
    )
    buttons = [
        [InlineKeyboardButton("🚀 Start Bunch Forwarding", callback_data="wiz_commit_task")],
        [InlineKeyboardButton("❌ Abort", callback_data="menu_main")],
    ]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


# ------------------ TASKS & HISTORY MENUS --------------------
@owner_only
async def handle_tasks_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    tasks = await db.get_tasks()
    if not tasks:
        text = "📋 <b>My Tasks</b>\n\nNo tasks found."
        buttons = [
            [InlineKeyboardButton("🚀 Create Task", callback_data="wizard_start")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
        ]
        await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    text = "📋 <b>My Tasks (Multi-Task Running Active):</b>\n"
    buttons = []
    for t in tasks[:10]:
        is_active = engine.is_running(t["id"])
        status_sym = "⚡ (Active)" if is_active else ("✅" if t["status"] == "COMPLETED" else "⏸️")
        btn_label = f"#{t['id']} {status_sym}: {t['source_title'][:12]} ({t['status']})"
        buttons.append([InlineKeyboardButton(btn_label, callback_data=f"task_view_{t['id']}")])

    buttons.append([InlineKeyboardButton("🚀 New Forward Task", callback_data="wizard_start")])
    buttons.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")])
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


@owner_only
async def handle_history_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    records = await db.get_history(limit=8)
    if not records:
        text = "📜 <b>Forward History</b>\n\nNo records logged yet."
        buttons = [[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]
        await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    text = "📜 <b>Recent Forward History:</b>\n\n"
    for r in records:
        text += (
            f"• <b>Task #{r['task_id']}</b>: {html.escape(r['source_title'])} ➡️ {html.escape(r['dest_titles'])}\n"
            f"  Status: <code>{r['status']}</code> | Forwarded: {r['forwarded_count']} | Failed: {r['failed_count']}\n"
            f"  Completed: <i>{r['end_time']}</i>\n\n"
        )

    buttons = [
        [InlineKeyboardButton("🗑️ Clear History", callback_data="history_clear_confirm")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
    ]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


# ------------------ SAVED CHATS --------------------
@owner_only
async def handle_saved_chats(update: Update, context: ContextTypes.DEFAULT_TYPE, category: str):
    query = update.callback_query
    chats = await db.get_saved_chats(category)
    cat_label = "Sources" if category == "source" else "Destinations"

    text = f"📂 <b>Saved {cat_label}</b>\n\nManage shortcut list:\n"
    buttons = []
    for c in chats:
        buttons.append([InlineKeyboardButton(f"🗑️ Remove: {c['title'][:25]}", callback_data=f"saved_del_{c['chat_id']}_{category}")])

    buttons.append([InlineKeyboardButton(f"➕ Add New {cat_label[:-1]}", callback_data=f"saved_add_{category}")])
    buttons.append([InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")])
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


# ------------------ SETTINGS & HELP --------------------
@owner_only
async def handle_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    mode = await db.get_setting("default_mode", "COPY")
    qty = await db.get_setting("default_quantity", "50")
    bunch = await db.get_setting("default_bunch", "20")
    delay = await db.get_setting("delay_between_messages", "0.3")

    text = (
        "⚙️ <b>General Configuration</b>\n"
        "━━━━━━━━━━━━━━━━━━━\n"
        f"<b>Default Mode:</b> {mode}\n"
        f"<b>Default Quantity:</b> {qty}\n"
        f"<b>Default Bunch Size:</b> {bunch} msgs\n"
        f"<b>Safety Delay:</b> {delay}s per batch\n"
        "━━━━━━━━━━━━━━━━━━━"
    )
    buttons = [
        [InlineKeyboardButton("🔄 Toggle Default Mode", callback_data="set_toggle_mode")],
        [InlineKeyboardButton("⏱️ Adjust Delay", callback_data="set_adjust_delay")],
        [InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")],
    ]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


@owner_only
async def handle_help_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    text = (
        "ℹ️ <b>Personal Forward & Channel Manager Guide</b>\n\n"
        "1. <b>Bunch Forwarding:</b> Select 10, 20, 30, 50, or 100 to forward messages in bulk chunks natively.\n"
        "2. <b>Multi-Task Execution:</b> Run multiple forwarding tasks at the same time in parallel without waiting.\n"
        "3. <b>1-Click Join Requests:</b> Open 'Join Requests', select your channel, and accept all pending users in 1 tap!\n"
        "4. <b>Render Cloud Keep-Alive:</b> An embedded Flask server keeps Render alive 24/7."
    )
    buttons = [[InlineKeyboardButton("🔙 Main Menu", callback_data="menu_main")]]
    await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))


# ------------------ MASTER CALLBACK DISPATCHER ------------------
@owner_only
async def master_callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass

    data = query.data

    # Main Navigation
    if data == "menu_main":
        await start_command(update, context)
        return
    elif data == "menu_account":
        await handle_account_menu(update, context)
        return
    elif data == "account_login_phone":
        await handle_login_phone_callback(update, context)
        return
    elif data == "account_logout":
        await handle_logout_callback(update, context)
        return
    elif data == "wizard_start":
        await start_wizard(update, context)
        return
    elif data == "menu_tasks":
        await handle_tasks_menu(update, context)
        return
    elif data == "menu_history":
        await handle_history_menu(update, context)
        return
    elif data == "menu_sources":
        await handle_saved_chats(update, context, "source")
        return
    elif data == "menu_destinations":
        await handle_saved_chats(update, context, "destination")
        return
    elif data == "menu_settings":
        await handle_settings_menu(update, context)
        return
    elif data == "menu_help":
        await handle_help_menu(update, context)
        return

    # OTP Numpad
    if data.startswith("otp_d_"):
        action = data.replace("otp_d_", "")
        state = USER_STATE.get(OWNER_ID, {})
        buf = state.get("otp_buffer", "")
        phone = state.get("phone", "")

        if action.isdigit():
            if len(buf) < 6:
                buf += action
        elif action == "del":
            buf = buf[:-1]
        elif action == "clear":
            buf = ""
        elif action == "submit":
            if len(buf) >= 4:
                await safe_edit(query, "⏳ <b>Verifying OTP code...</b>", parse_mode=ParseMode.HTML)
                success, res = await user_mgr.complete_login(buf)
                if success:
                    USER_STATE.pop(OWNER_ID, None)
                    await safe_edit(
                        query,
                        f"🎉 <b>Success:</b> {res}\nYour personal account is now linked securely and permanently!",
                        reply_markup=get_main_menu_keyboard(),
                    )
                elif res == "2FA_REQUIRED":
                    state["stage"] = "WAITING_2FA"
                    await safe_edit(
                        query,
                        "🔐 <b>Two-Step Verification (2FA) is enabled.</b>\n\nPlease reply with your 2FA password in chat:",
                    )
                else:
                    await safe_edit(
                        query,
                        f"❌ <b>Login Failed:</b> {res}",
                        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Try Again", callback_data="account_login_phone")]]),
                    )
                return
            else:
                await query.answer("Please enter complete OTP code first!", show_alert=True)
                return

        state["otp_buffer"] = buf
        await render_otp_screen(query, phone, buf)
        return

    # Join Requests
    if data == "menu_join_requests":
        await show_join_requests_channels(query, page=0)
        return
    elif data.startswith("joinreq_page_"):
        page = int(data.split("_")[-1])
        await show_join_requests_channels(query, page=page)
        return
    elif data.startswith("joinreq_sel_"):
        chat_raw = data.replace("joinreq_sel_", "")
        try:
            chat_id = int(chat_raw)
        except ValueError:
            chat_id = chat_raw
        await show_channel_join_request_view(query, chat_id)
        return
    elif data == "joinreq_manual":
        USER_STATE[OWNER_ID] = {"stage": "WAITING_JOINREQ_MANUAL"}
        await safe_edit(
            query,
            "✏️ Reply with Channel ID (e.g. <code>-1001234567890</code>) or Username (<code>@channel</code>):",
        )
        return
    elif data.startswith("joinreq_accept_"):
        chat_raw = data.replace("joinreq_accept_", "")
        try:
            chat_id = int(chat_raw)
        except ValueError:
            chat_id = chat_raw

        await safe_edit(query, "⚡ <b>Processing...</b> Approving all join requests in 1-click...", reply_markup=None)
        ok, msg, _ = await user_mgr.accept_or_reject_all_requests(chat_id, approve=True)
        result_text = f"✅ <b>{msg}</b>" if ok else f"❌ <b>{msg}</b>"
        buttons = [
            [InlineKeyboardButton("🔄 Refresh Channel Status", callback_data=f"joinreq_sel_{chat_id}")],
            [InlineKeyboardButton("🔙 Back to Channels", callback_data="menu_join_requests")],
        ]
        await safe_edit(query, result_text, reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data.startswith("joinreq_reject_"):
        chat_raw = data.replace("joinreq_reject_", "")
        try:
            chat_id = int(chat_raw)
        except ValueError:
            chat_id = chat_raw

        await safe_edit(query, "⏳ <b>Processing...</b> Rejecting all join requests...", reply_markup=None)
        ok, msg, _ = await user_mgr.accept_or_reject_all_requests(chat_id, approve=False)
        result_text = f"✅ <b>{msg}</b>" if ok else f"❌ <b>{msg}</b>"
        buttons = [
            [InlineKeyboardButton("🔄 Refresh Channel Status", callback_data=f"joinreq_sel_{chat_id}")],
            [InlineKeyboardButton("🔙 Back to Channels", callback_data="menu_join_requests")],
        ]
        await safe_edit(query, result_text, reply_markup=InlineKeyboardMarkup(buttons))
        return

    # Wizard Steps
    wiz = USER_STATE.get(OWNER_ID, {}).get("wizard", {})

    if data.startswith("wiz_src_page_"):
        page = int(data.split("_")[-1])
        await show_source_selection_menu(query, page)
        return
    elif data.startswith("wiz_src_sel_"):
        chat_id = int(data.split("_")[-1])
        entity = await user_mgr.validate_entity(chat_id)
        if entity:
            wiz["source_id"] = entity["id"]
            wiz["source_title"] = entity["title"]
            await show_destination_selection_menu(query, page=0)
        else:
            await query.answer("Could not access this chat.", show_alert=True)
        return
    elif data == "wiz_src_manual":
        USER_STATE[OWNER_ID]["stage"] = "WIZ_SRC_MANUAL"
        await safe_edit(
            query,
            "✏️ Reply with Source Chat ID (e.g., <code>-10012345678</code>) or Username (<code>@channel</code>):",
        )
        return
    elif data == "wiz_src_saved":
        saved = await db.get_saved_chats("source")
        if not saved:
            await query.answer("No saved sources yet.", show_alert=True)
            return
        buttons = [[InlineKeyboardButton(s["title"], callback_data=f"wiz_src_sel_{s['chat_id']}")] for s in saved]
        buttons.append([InlineKeyboardButton("🔙 Back", callback_data="wizard_start")])
        await safe_edit(query, "Select from saved sources:", reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data.startswith("wiz_dst_page_"):
        page = int(data.split("_")[-1])
        await show_destination_selection_menu(query, page)
        return
    elif data.startswith("wiz_dst_toggle_"):
        chat_id = int(data.split("_")[-1])
        dest_ids = wiz.setdefault("dest_ids", [])
        dest_titles = wiz.setdefault("dest_titles", [])

        if chat_id in dest_ids:
            idx = dest_ids.index(chat_id)
            dest_ids.pop(idx)
            dest_titles.pop(idx)
        else:
            entity = await user_mgr.validate_entity(chat_id)
            if entity:
                dest_ids.append(entity["id"])
                dest_titles.append(entity["title"])
        await show_destination_selection_menu(query, page=0)
        return
    elif data == "wiz_dst_manual":
        USER_STATE[OWNER_ID]["stage"] = "WIZ_DST_MANUAL"
        await safe_edit(query, "✏️ Reply with Destination Chat ID or Username:")
        return
    elif data == "wiz_dst_saved":
        saved = await db.get_saved_chats("destination")
        if not saved:
            await query.answer("No saved destinations yet.", show_alert=True)
            return
        buttons = [[InlineKeyboardButton(s["title"], callback_data=f"wiz_dst_toggle_{s['chat_id']}")] for s in saved]
        buttons.append([InlineKeyboardButton("🔙 Done & Return", callback_data="wiz_goto_filters")])
        await safe_edit(query, "Select destination from saved:", reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data == "wiz_goto_filters":
        await show_filters_menu(query)
        return
    elif data == "wiz_toggle_order":
        curr = wiz.get("order_dir", "OLDEST_FIRST")
        wiz["order_dir"] = "NEWEST_FIRST" if curr == "OLDEST_FIRST" else "OLDEST_FIRST"
        await show_filters_menu(query)
        return
    elif data == "wiz_toggle_dir":
        dirs = ["ALL", "SENT_ONLY", "RECEIVED_ONLY"]
        curr = wiz.get("direction_filter", "ALL")
        wiz["direction_filter"] = dirs[(dirs.index(curr) + 1) % len(dirs)]
        await show_filters_menu(query)
        return
    elif data == "wiz_set_qty":
        buttons = [
            [
                InlineKeyboardButton("10", callback_data="wiz_qty_val_10"),
                InlineKeyboardButton("50", callback_data="wiz_qty_val_50"),
                InlineKeyboardButton("100", callback_data="wiz_qty_val_100"),
            ],
            [
                InlineKeyboardButton("500", callback_data="wiz_qty_val_500"),
                InlineKeyboardButton("1000", callback_data="wiz_qty_val_1000"),
                InlineKeyboardButton("Custom", callback_data="wiz_qty_val_custom"),
            ],
            [
                InlineKeyboardButton("♾️ All Posts of Channel", callback_data="wiz_qty_val_all"),
            ],
        ]
        await safe_edit(query, "Select quantity (or choose All Posts):", reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data.startswith("wiz_qty_val_"):
        val = data.split("_")[-1]
        if val == "custom":
            USER_STATE[OWNER_ID]["stage"] = "WIZ_QTY_CUSTOM"
            await safe_edit(query, "Reply with your custom quantity number (e.g. <code>250</code>):")
            return
        elif val == "all":
            wiz["quantity"] = 0
        else:
            wiz["quantity"] = int(val)
        await show_filters_menu(query)
        return
    elif data == "wiz_set_bunch":
        buttons = [
            [
                InlineKeyboardButton("10 msgs", callback_data="wiz_bunch_val_10"),
                InlineKeyboardButton("20 msgs", callback_data="wiz_bunch_val_20"),
                InlineKeyboardButton("30 msgs", callback_data="wiz_bunch_val_30"),
            ],
            [
                InlineKeyboardButton("50 msgs", callback_data="wiz_bunch_val_50"),
                InlineKeyboardButton("100 msgs", callback_data="wiz_bunch_val_100"),
            ],
            [InlineKeyboardButton("🔙 Back to Filters", callback_data="wiz_goto_filters")],
        ]
        await safe_edit(query, "Select Bunch Size (how many messages to forward at once):", reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data.startswith("wiz_bunch_val_"):
        b_val = int(data.split("_")[-1])
        wiz["bunch_size"] = b_val
        await show_filters_menu(query)
        return
    elif data == "wiz_menu_types":
        await show_media_types_menu(query)
        return
    elif data.startswith("wiz_type_toggle_"):
        t_name = data.replace("wiz_type_toggle_", "")
        types_set = set(wiz.setdefault("types_filter", []))
        if t_name in types_set:
            types_set.remove(t_name)
        else:
            types_set.add(t_name)
        wiz["types_filter"] = list(types_set)
        await show_media_types_menu(query)
        return
    elif data == "wiz_set_include_kw":
        USER_STATE[OWNER_ID]["stage"] = "WIZ_INC_KW"
        await safe_edit(query, "Reply with keyword the message MUST contain (or type 'clear' to reset):")
        return
    elif data == "wiz_set_exclude_kw":
        USER_STATE[OWNER_ID]["stage"] = "WIZ_EXC_KW"
        await safe_edit(query, "Reply with keyword the message MUST NOT contain (or type 'clear' to reset):")
        return
    elif data == "wiz_goto_mode":
        await show_mode_menu(query)
        return
    elif data.startswith("wiz_mode_"):
        wiz["mode"] = data.split("_")[-1]
        await show_mode_menu(query)
        return
    elif data == "wiz_goto_confirm":
        await show_confirmation_screen(query)
        return
    elif data == "wiz_commit_task":
        task_id = await db.create_task(wiz)
        status_msg = await query.message.edit_text(
            f"⚡ <b>Task #{task_id} Initiated!</b> Starting Bunch forward worker...",
            parse_mode=ParseMode.HTML,
        )
        engine.start_task(task_id, context, status_msg.chat_id, status_msg.message_id)
        USER_STATE.pop(OWNER_ID, None)
        return

    # Task Operations
    elif data.startswith("task_view_"):
        task_id = int(data.split("_")[-1])
        task = await db.get_task(task_id)
        if not task:
            await query.answer("Task not found.", show_alert=True)
            return

        text = (
            f"📋 <b>Task #{task['id']} Overview</b>\n"
            f"━━━━━━━━━━━━━━━━━━━\n"
            f"<b>Status:</b> {task['status']}\n"
            f"<b>Source:</b> {html.escape(task['source_title'])}\n"
            f"<b>Destinations:</b> {html.escape(task['dest_titles'])}\n"
            f"<b>Mode:</b> {task['mode']}\n"
            f"<b>Total:</b> {task['total_count']}\n"
            f"<b>Success:</b> {task['success_count']} | <b>Failed:</b> {task['failed_count']}\n"
            f"<b>Created:</b> {task['created_at']}\n"
            f"━━━━━━━━━━━━━━━━━━━"
        )
        buttons = []
        if task["status"] == "RUNNING":
            buttons.append([InlineKeyboardButton("⏸️ Pause", callback_data=f"task_pause_{task_id}"), InlineKeyboardButton("🛑 Stop", callback_data=f"task_stop_{task_id}")])
        elif task["status"] == "PAUSED":
            buttons.append([InlineKeyboardButton("▶️ Resume", callback_data=f"task_resume_{task_id}"), InlineKeyboardButton("🛑 Stop", callback_data=f"task_stop_{task_id}")])
        elif task["status"] in ["STOPPED", "FAILED", "COMPLETED_WITH_ERRORS"]:
            buttons.append([InlineKeyboardButton("🔄 Restart Task", callback_data=f"task_restart_{task_id}")])

        buttons.append([InlineKeyboardButton("🔙 Back to Tasks", callback_data="menu_tasks")])
        await safe_edit(query, text, reply_markup=InlineKeyboardMarkup(buttons))
        return
    elif data.startswith("task_pause_"):
        task_id = int(data.split("_")[-1])
        engine.pause_task(task_id)
        await query.answer(f"Task #{task_id} paused.")
        return
    elif data.startswith("task_resume_"):
        task_id = int(data.split("_")[-1])
        engine.resume_task(task_id)
        await query.answer(f"Task #{task_id} resumed.")
        return
    elif data.startswith("task_stop_"):
        task_id = int(data.split("_")[-1])
        engine.stop_task(task_id)
        await query.answer(f"Task #{task_id} stopping...")
        return
    elif data.startswith("task_restart_"):
        task_id = int(data.split("_")[-1])
        status_msg = await query.message.edit_text(f"🔄 Restarting Task #{task_id}...")
        engine.start_task(task_id, context, status_msg.chat_id, status_msg.message_id)
        return

    # History & Settings
    elif data == "history_clear_confirm":
        await db.clear_history()
        await query.answer("History cleared.", show_alert=True)
        await handle_history_menu(update, context)
        return
    elif data == "set_toggle_mode":
        curr = await db.get_setting("default_mode", "COPY")
        new_mode = "ORIGINAL" if curr == "COPY" else "COPY"
        await db.set_setting("default_mode", new_mode)
        await handle_settings_menu(update, context)
        return
    elif data == "set_adjust_delay":
        USER_STATE[OWNER_ID]["stage"] = "SET_DELAY"
        await safe_edit(query, "Reply with message delay in seconds (e.g. <code>0.3</code>):")
        return
    elif data.startswith("saved_del_"):
        parts = data.split("_")
        chat_id = int(parts[2])
        cat = parts[3]
        await db.remove_saved_chat(chat_id)
        await query.answer("Chat removed.")
        await handle_saved_chats(update, context, cat)
        return
    elif data.startswith("saved_add_"):
        cat = data.split("_")[-1]
        USER_STATE[OWNER_ID]["stage"] = f"SAVED_ADD_{cat.upper()}"
        await safe_edit(query, f"Reply with the chat ID or username to save as a {cat}:")
        return


# ------------------ TEXT INPUT DISPATCHER ------------------
@owner_only
async def handle_text_inputs(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_state = USER_STATE.get(OWNER_ID, {})
    stage = user_state.get("stage")
    text = update.message.text.strip()

    if text.startswith("/cancel"):
        USER_STATE.pop(OWNER_ID, None)
        await update.message.reply_text("Operation cancelled.", reply_markup=get_main_menu_keyboard())
        return

    if not stage:
        await update.message.reply_text("Select an option from the menu:", reply_markup=get_main_menu_keyboard())
        return

    if stage == "WAITING_PHONE":
        phone = text.replace(" ", "")
        try:
            await user_mgr.send_login_code(phone)
            user_state["stage"] = "WAITING_OTP"
            user_state["phone"] = phone
            user_state["otp_buffer"] = ""
            await render_otp_screen(update.message, phone, "")
        except Exception as e:
            USER_STATE.pop(OWNER_ID, None)
            await update.message.reply_text(f"❌ Failed to send code: {str(e)}", reply_markup=get_main_menu_keyboard())
        return

    elif stage == "WAITING_2FA":
        password = text
        success, res = await user_mgr.complete_login("", password=password)
        USER_STATE.pop(OWNER_ID, None)
        if success:
            await update.message.reply_text(
                f"🎉 <b>2FA Authentication successful!</b>\n{res}",
                parse_mode=ParseMode.HTML,
                reply_markup=get_main_menu_keyboard(),
            )
        else:
            await update.message.reply_text(f"❌ 2FA login failed: {res}", reply_markup=get_main_menu_keyboard())
        return

    elif stage == "WAITING_JOINREQ_MANUAL":
        entity = await user_mgr.validate_entity(text)
        if entity:
            USER_STATE.pop(OWNER_ID, None)
            await show_channel_join_request_view(update.message, entity["id"])
        else:
            await update.message.reply_text("❌ Could not access that channel. Check the ID/Username and ensure you are an Admin:")
        return

    elif stage == "WIZ_SRC_MANUAL":
        entity = await user_mgr.validate_entity(text)
        if entity:
            user_state.setdefault("wizard", {})["source_id"] = entity["id"]
            user_state["wizard"]["source_title"] = entity["title"]
            USER_STATE[OWNER_ID]["stage"] = None
            msg = await update.message.reply_text("✅ Source chat selected!")
            await show_destination_selection_menu(msg, page=0)
        else:
            await update.message.reply_text("❌ Could not access that chat. Ensure your personal account has joined it, then try again:")
        return

    elif stage == "WIZ_DST_MANUAL":
        entity = await user_mgr.validate_entity(text)
        if entity:
            wiz = user_state.setdefault("wizard", {})
            wiz.setdefault("dest_ids", []).append(entity["id"])
            wiz.setdefault("dest_titles", []).append(entity["title"])
            USER_STATE[OWNER_ID]["stage"] = None
            msg = await update.message.reply_text("✅ Destination added!")
            await show_destination_selection_menu(msg, page=0)
        else:
            await update.message.reply_text("❌ Could not access destination chat. Check permissions and try again:")
        return

    elif stage == "WIZ_QTY_CUSTOM":
        if text.isdigit() and int(text) > 0:
            user_state["wizard"]["quantity"] = int(text)
            USER_STATE[OWNER_ID]["stage"] = None
            await show_filters_menu(update.message)
        else:
            await update.message.reply_text("Please enter a valid positive number:")
        return

    elif stage == "WIZ_INC_KW":
        val = "" if text.lower() == "clear" else text
        user_state["wizard"]["include_keywords"] = val
        USER_STATE[OWNER_ID]["stage"] = None
        await show_filters_menu(update.message)
        return

    elif stage == "WIZ_EXC_KW":
        val = "" if text.lower() == "clear" else text
        user_state["wizard"]["exclude_keywords"] = val
        USER_STATE[OWNER_ID]["stage"] = None
        await show_filters_menu(update.message)
        return

    elif stage == "SET_DELAY":
        try:
            val = float(text)
            if val < 0.1:
                raise ValueError()
            await db.set_setting("delay_between_messages", str(val))
            USER_STATE.pop(OWNER_ID, None)
            await update.message.reply_text(f"⚡ Delay updated to {val}s (Ultra Fast).", reply_markup=get_main_menu_keyboard())
        except ValueError:
            await update.message.reply_text("Invalid delay. Enter a number >= 0.1:")
        return

    elif stage in ["SAVED_ADD_SOURCE", "SAVED_ADD_DESTINATION"]:
        cat = "source" if "SOURCE" in stage else "destination"
        entity = await user_mgr.validate_entity(text)
        if entity:
            await db.add_saved_chat(entity["id"], entity["title"], entity["username"], entity["type"], cat)
            USER_STATE.pop(OWNER_ID, None)
            await update.message.reply_text(f"✅ Added {entity['title']} to saved {cat}s!", reply_markup=get_main_menu_keyboard())
        else:
            await update.message.reply_text("❌ Could not resolve chat. Please check ID/Username and try again:")
        return


# ------------------ RUNNER ------------------
def main():
    try:
        loop = asyncio.get_event_loop_policy().get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start_command))
    application.add_handler(CallbackQueryHandler(master_callback_router))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_inputs)
    )

    async def post_init(app: Application):
        logger.info("Initializing SQLite database with WAL mode...")
        await db.init()
        logger.info("Connecting Telethon client...")
        await user_mgr.get_client()
        logger.info("Telethon connected. Authorized status: %s", await user_mgr.is_authorized())

    application.post_init = post_init

    logger.info("Bot starting up...")
    application.run_polling(drop_pending_updates=True)


if __name__ == "__main__":
    main()
