"""
import os
import asyncio
import logging
import re
import time
import uuid
import json
from decimal import Decimal
from typing import Optional, Tuple, Any, Callable, Dict, Awaitable
from aiogram.filters import Command, CommandStart, CommandObject
from aiogram.utils.keyboard import InlineKeyboardBuilder
import aiosqlite
from dotenv import load_dotenv
from aiogram.types import BotCommand
from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiohttp import web
from datetime import datetime, timedelta

storage = MemoryStorage()
CouponData = Optional[Tuple[Any, ...]]

# Попытка импортировать yookassa
YOOINSTALLED = False
try:
    from yookassa import Configuration, Payment
    from yookassa.domain.models import Webhook
    from yookassa.domain.request import WebhookRequest
    from yookassa.client import Yookassa
    YOOINSTALLED = True
except ImportError:
    pass

load_dotenv()
dp = Dispatcher(storage=storage)

# --- Утилиты ---
def escape_markdown_v2(text: str) -> str:
    """Экранирует специальные символы для Telegram's MarkdownV2 parse mode."""
    if text is None:
        return ""
    # Символы, которые нужно экранировать в MarkdownV2:
    # _, *, [, ], (, ), ~, `, >, #, +, -, =, |, {, }, ., !
    return re.sub(r'([_\*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))

# Note: escape_markdown_v2 returns strings escaped for MarkdownV2. We'll also provide a helper
# to escape parentheses except when they are part of markdown links like [text](url).

_link_re = re.compile(r'(\[.*?\]\(.*?\))', re.DOTALL)

def escape_parens_preserving_links(text: str) -> str:
    """Escape parentheses for MarkdownV2, but preserve parentheses inside markdown links.
    Example: "Hello (world) [User](tg://user?id=1)" -> "Hello \(world\) [User](tg://user?id=1)"""
    if not isinstance(text, str):
        return text
    placeholders = {}
    def repl(m):
        key = f"__LINK_PLACEHOLDER_{len(placeholders)}__"
        placeholders[key] = m.group(1)
        return key
    s = _link_re.sub(repl, text)
    s = s.replace('(', '\\(').replace(')', '\\)')
    # restore placeholders
    for k, v in placeholders.items():
        s = s.replace(k, v)
    return s

# --- Конфигурация ---
BOT_TOKEN = os.getenv("BOT_TOKEN")
SUPPORT_ADMIN_ID = int(os.getenv("SUPPORT_ADMIN_ID", 0))
ADMIN_IDS = set(int(x.strip()) for x in os.getenv("ADMIN_ID","").split(',') if x.strip() and x.strip().isdigit())
DB_PATH = os.getenv("DB_PATH","robux_bot.db")
YOOKASSA_SHOP_ID = os.getenv("YOOKASSA_SHOP_ID")
YOOKASSA_SECRET_KEY = os.getenv("YOOKASSA_SECRET_KEY")
NOTIFY_GROUP_ID = os.getenv("NOTIFY_GROUP_ID")
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST","")
WEBHOOK_PATH = "/yookassa_webhook"
PORT = int(os.getenv("PORT","8080"))
REFERRAL_BONUS_RUB = 5.0 # Бонус рефереру за привлечение

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN not found in environment (.env)")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}"

# Monkeypatch some bot methods to sanitize MarkdownV2 parentheses at runtime.
# This avoids many places in the code where parentheses in static text caused TelegramBadRequest.
_orig_send_message = bot.send_message
_orig_edit_message_text = bot.edit_message_text
_orig_send_photo = bot.send_photo

async def _send_message(chat_id, text, *args, **kwargs):
    if kwargs.get('parse_mode') == 'MarkdownV2' and isinstance(text, str):
        text = escape_parens_preserving_links(text)
    return await _orig_send_message(chat_id, text, *args, **kwargs)

async def _edit_message_text(*args, **kwargs):
    # edit_message_text signature varies; text may be positional or kw
    # Find text in args/kwargs and sanitize if parse_mode indicates MarkdownV2
    parse_mode = kwargs.get('parse_mode')
    if parse_mode == 'MarkdownV2':
        if 'text' in kwargs and isinstance(kwargs['text'], str):
            kwargs['text'] = escape_parens_preserving_links(kwargs['text'])
        else:
            # positional args: try to locate the text position (common signatures: chat_id, message_id, text,...)
            args = list(args)
            if len(args) >= 3 and isinstance(args[2], str):
                args[2] = escape_parens_preserving_links(args[2])
            args = tuple(args)
    return await _orig_edit_message_text(*args, **kwargs)

async def _send_photo(chat_id, photo, *args, **kwargs):
    if kwargs.get('parse_mode') == 'MarkdownV2' and 'caption' in kwargs and isinstance(kwargs['caption'], str):
        kwargs['caption'] = escape_parens_preserving_links(kwargs['caption'])
    return await _orig_send_photo(chat_id, photo, *args, **kwargs)

# apply monkeypatch
bot.send_message = _send_message
bot.edit_message_text = _edit_message_text
bot.send_photo = _send_photo

# --- Инициализация YooKassa ---
if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY and YOOINSTALLED:
    try:
        Configuration.account_id = YOOKASSA_SHOP_ID
        Configuration.secret_key = YOOKASSA_SECRET_KEY
        logger.info("YooKassa configured")
    except Exception:
        logger.exception("Failed to configure YooKassa")
elif (YOOKASSA_SHOP_ID or YOOKASSA_SECRET_KEY) and not YOOINSTALLED:
    logger.warning("YooKassa keys found but yookassa package is missing. Install yookassa to enable payments.")


# ==========================================
# 3. Анти-спам Middleware
# ==========================================
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, limit: float = 0.25):
        self.limit = limit
        self.cache = {}

    async def __call__(self,
        handler: Callable[[types.TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: types.TelegramObject,
        data: Dict[str, Any]
    ) -> Any:
        user = data.get("event_from_user")
        if user:
            user_id = user.id
            current_time = time.time()
            
            # Примитивная очистка кэша
            if len(self.cache) > 5000:
                self.cache = {}

            if user_id in self.cache:
                if current_time - self.cache[user_id] < self.limit:
                    return 
            
            self.cache[user_id] = current_time

        return await handler(event, data)

dp.update.middleware(ThrottlingMiddleware(limit=0.7))
# ==========================================


# --- FSM States ---
class CreateAdStates(StatesGroup):
    title = State()
    rate = State()
    min_amount = State()
    max_amount = State()
    payment_methods = State()
    description = State()
    confirm = State()

class LeaveReviewStates(StatesGroup):
    choose_seller = State()
    rating = State()
    comment = State()

class WithdrawStates(StatesGroup):
    amount_rub = State()
    method = State()
    details = State()

class BroadcastStates(StatesGroup):
    text = State()
    confirm = State()

class DealStates(StatesGroup): 
    in_progress = State()
    dispute = State()

class CreateDealStates(StatesGroup):
    enter_amount = State()
    enter_roblox_link = State() 
    confirm = State()

class AdminUserManagement(StatesGroup):
    enter_user_id = State()
    enter_new_balance = State()

class AdminCouponStates(StatesGroup):
    enter_code = State()
    enter_type = State()
    enter_value = State()
    enter_limit = State()
    enter_min_amount = State()
    confirm = State()

class UserCouponStates(StatesGroup):
    enter_code = State()

class ProofStates(StatesGroup):
    waiting_for_proof = State()

# --- Настройка Webhook YooKassa ---
async def setup_yookassa_webhook():
    if not WEBHOOK_HOST or not YOOINSTALLED:
        logger.warning("Webhook YooKassa не будет настроен.")
        return

    try:
        current_webhooks = Yookassa.get_all_webhooks().items
        for webhook in current_webhooks:
            Yookassa.remove_webhook(webhook.id)
            logger.info(f"Удален старый Webhook ID: {webhook.id}")
            
        request = WebhookRequest.builder().with_event(Webhook.Event.PAYMENT_SUCCEEDED).with_url(WEBHOOK_URL).build()
        Yookassa.add_webhook(request)
        logger.info(f"✅ Webhook YooKassa успешно установлен на: {WEBHOOK_URL}")

    except Exception as e:
        logger.error(f"❌ Ошибка при настройке Webhook YooKassa: {e}")  


# --- Клавиатуры ---
def main_menu_kb(is_admin_user: bool = False):
    kb = [
        [
            InlineKeyboardButton(text="💰 Купить", callback_data="menu_buy"),
            InlineKeyboardButton(text="💸 Продать", callback_data="menu_sell")
        ],
        [
            InlineKeyboardButton(text="👤 Профиль", callback_data="menu_profile"),
        ]
    ]
    if is_admin_user:
        kb[1].append(InlineKeyboardButton(text="⚙️ Админ панель", callback_data="menu_admin"))
    return InlineKeyboardMarkup(inline_keyboard=kb)

# (rest of file kept unchanged)
"""
