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
    return re.sub(r'([_*\[\]()~`>#+\-=|{}.!])', r'\\\1', str(text))

def format_date(dt_str: str) -> str:
    """Форматирует строку даты для вывода."""
    if not dt_str:
        return ""
    try:
        dt = datetime.strptime(dt_str.split('.')[0], '%Y-%m-%d %H:%M:%S')
        return dt.strftime('%d.%m.%Y %H:%M')
    except ValueError:
        return dt_str

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

    async def __call__(
        self,
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

def sell_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Мои объявления", callback_data="sell_my_ads")],
        [InlineKeyboardButton(text="➕ Создать объявление", callback_data="sell_create_ad")],
        [InlineKeyboardButton(text="📜 История продаж", callback_data="sell_history")],
        [InlineKeyboardButton(text="⭐ Отзывы", callback_data="sell_reviews")],
        [InlineKeyboardButton(text="👤 Моя анкета", callback_data="sell_profile")],
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_main")]
    ])

def back_main_kb(is_admin_user: bool = False):
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_main")]])

def back_admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin")]])

def profile_kb(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💸 Вывод RUB", callback_data="profile_withdraw")],
        [InlineKeyboardButton(text="💳 Мои транзакции", callback_data="profile_tx")],
        # ИЗМЕНЕНО: Теперь кнопка вызывает меню, а не открывает ссылку
        [InlineKeyboardButton(text="💌 Реф. программа", callback_data="profile_referral")], 
        [InlineKeyboardButton(text="✉️ Написать в поддержку", callback_data="support")], 
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")]
    ])

def admin_main_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📦 Споры", callback_data="adm_deals_dispute"), InlineKeyboardButton(text="📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton(text="💸 Выводы (Ждут)", callback_data="adm_withdraws"), InlineKeyboardButton(text="👤 Упр. Польз.", callback_data="adm_users")],
        [InlineKeyboardButton(text="🎫 Купоны", callback_data="adm_coupons"), InlineKeyboardButton(text="💌 Рассылка", callback_data="adm_broadcast")], 
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_main")]
    ])

def buy_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Просмотреть объявления", callback_data="buy_list_ads")],
        [InlineKeyboardButton(text="🎫 Активировать купон", callback_data="user_coupon_activate")], 
        [InlineKeyboardButton(text="◀️ Главное меню", callback_data="back_main")]
    ])

def admin_stats_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="7 дней", callback_data="stats_period:7"), InlineKeyboardButton(text="14 дней", callback_data="stats_period:14")],
        [InlineKeyboardButton(text="21 день", callback_data="stats_period:21"), InlineKeyboardButton(text="Месяц (30 дн.)", callback_data="stats_period:30")],
        [InlineKeyboardButton(text="Год (365 дн.)", callback_data="stats_period:365")],
        [InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin")]
    ])

def admin_coupons_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Создать новый купон", callback_data="coupon_create")],
        [InlineKeyboardButton(text="📋 Просмотр / Управление", callback_data="coupon_list")],
        [InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin")]
    ])

def deal_actions_buyer_kb(deal_id: int, status: str):
    kb = InlineKeyboardBuilder()
    if status == 'paid_waiting_proof':
        kb.row(InlineKeyboardButton(text="⚠️ Открыть спор", callback_data=f"deal_dispute:{deal_id}"))
    if status == 'completed':
        kb.row(InlineKeyboardButton(text="⭐ Оставить отзыв", callback_data=f"deal_review:{deal_id}"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
    return kb.as_markup()

def deal_actions_seller_kb(deal_id: int, status: str):
    kb = InlineKeyboardBuilder()
    if status == 'pending_proof':
        kb.row(InlineKeyboardButton(text="✅ Подтвердить выдачу", callback_data=f"deal_complete_seller:{deal_id}"))
    if status == 'dispute':
        kb.row(InlineKeyboardButton(text="✅ Подтвердить выдачу (Спор)", callback_data=f"deal_complete_seller_dispute:{deal_id}"))
    kb.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))
    return kb.as_markup()

def deal_proof_kb(deal_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📸 Загрузить скриншот оплаты", callback_data=f"deal_upload_proof:{deal_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]
    ])

# --- DB Helpers ---
async def log_event(user_id: int, action: str, details: str = ""):
    """Записывает событие в таблицу logs"""
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO logs (user_id, action, details) VALUES (?, ?, ?)",
                (user_id, action, details)
            )
            await db.commit()
    except Exception as e:
        print(f"[LOG ERROR] {e}")

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        
        # 1. Основные таблицы (Пользователи)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            balance REAL DEFAULT 0, 
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            referrer_id INTEGER DEFAULT NULL,
            active_coupon_id INTEGER DEFAULT NULL
        )
        """)

        # 2. Логи (Добавил event_type сразу в создание таблицы для новых БД)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            action TEXT,
            details TEXT,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            event_type TEXT
        )
        """)
        
        # Миграция для старых БД (если таблица logs уже была создана без event_type)
        try:
            await db.execute("ALTER TABLE logs ADD COLUMN event_type TEXT")
            await db.execute("CREATE INDEX IF NOT EXISTS idx_logs_user ON logs(user_id)")
        except aiosqlite.OperationalError as e:
            if "duplicate column name" not in str(e):
                logger.error(f"Error adding column 'event_type': {e}")

        # 3. Заказы (финансы)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            type TEXT,
            amount INTEGER,
            price REAL,
            status TEXT,
            details TEXT,
            payment_id TEXT,
            provider TEXT DEFAULT 'manual',
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # 4. Конфигурация
        await db.execute("""
        CREATE TABLE IF NOT EXISTS config (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        # 5. Объявления
        await db.execute("""
        CREATE TABLE IF NOT EXISTS ads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            title TEXT,
            rate REAL,               
            min_amount INTEGER,
            max_amount INTEGER,
            payment_methods TEXT,    
            active INTEGER DEFAULT 1,
            description TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # 6. Сделки P2P
        await db.execute("""
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            buyer_id INTEGER,
            seller_id INTEGER,
            ad_id INTEGER,
            amount INTEGER,
            price REAL,
            rub_amount REAL,
            roblox_link TEXT,
            payment_id TEXT,
            status TEXT,
            proof_file_id TEXT DEFAULT NULL, 
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            coupon_id INTEGER DEFAULT NULL,
            coupon_code TEXT DEFAULT NULL,
            dispute_reason TEXT DEFAULT NULL,
            dispute_admin_id INTEGER DEFAULT NULL,
            dispute_resolved_at DATETIME DEFAULT NULL
        )
        """)

        # 7. Отзывы
        await db.execute("""
        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reviewer_id INTEGER,
            target_id INTEGER,
            deal_id INTEGER UNIQUE,
            rating INTEGER,
            comment TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # 8. Купоны
        await db.execute("""
        CREATE TABLE IF NOT EXISTS coupons (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE,
            type TEXT,
            value REAL,
            uses_limit INTEGER DEFAULT 0,
            min_amount INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS coupon_uses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            coupon_id INTEGER,
            user_id INTEGER,
            deal_id INTEGER,
            used_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """)

        # 9. Установка начальных значений конфига (если их нет)
        cur = await db.execute("SELECT value FROM config WHERE key = ?", ("price_per_1000",))
        if not await cur.fetchone():
            await db.execute("INSERT INTO config(key, value) VALUES(?, ?)", ("price_per_1000", "300.00"))
            
        cur = await db.execute("SELECT value FROM config WHERE key = ?", ("min_withdraw",))
        if not await cur.fetchone():
            await db.execute("INSERT INTO config(key, value) VALUES(?, ?)", ("min_withdraw", "100.00"))

        await db.commit()

# --- DB Config Functions ---
async def get_config(key:str)->Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row[0] if row else None
    
    
async def get_coupon_data(coupon_id: Optional[int]) -> CouponData:
    """
    Получает данные купона из базы данных по его ID.

    :param coupon_id: ID купона. Может быть None.
    :return: Кортеж с данными купона или None, если купон не найден 
             или переданный coupon_id был None.
    """
    if coupon_id is None:
        return None
        
    async with aiosqlite.connect(DB_PATH) as db:
        # Используем тройные кавычки для многострочного SQL-запроса
        query = """
            SELECT 
                id, code, type, value, uses_limit, min_amount, is_active 
            FROM 
                coupons 
            WHERE 
                id = ?
        """
        
        cur = await db.execute(query, (coupon_id,))
        return await cur.fetchone()

# -------------------------------------------------------------------
# Функция set_config выглядела правильно, 
# но я даю ее в контексте для завершенности
# -------------------------------------------------------------------

async def set_config(key:str, value:str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("REPLACE INTO config(key,value) VALUES(?,?)", (key,value))
        await db.commit()
# --- DB User Functions ---
async def get_user_data(user_id:int):
    """Возвращает данные пользователя по ID."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT username, balance, created_at, referrer_id, active_coupon_id FROM users WHERE user_id = ?", (user_id,))
        return await cur.fetchone()

async def get_user_balance(user_id:int) -> float:
    """Возвращает баланс пользователя."""
    data = await get_user_data(user_id)
    return float(data[1]) if data and data[1] is not None else 0.0

async def update_user_balance(user_id:int, new_balance:float):
    """Обновляет баланс пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, user_id))
        await db.commit()
        await log_event(user_id, "BALANCE_UPDATE", f"New balance: {new_balance:.2f}")

async def create_user_if_not_exists(user: types.User, referrer_id: Optional[int] = None):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users WHERE user_id = ?", (user.id,))
        if not await cur.fetchone():
            referrer_id = referrer_id if referrer_id and referrer_id != user.id else None
            await db.execute("INSERT INTO users(user_id, username, referrer_id) VALUES(?, ?, ?)",
                             (user.id, user.username, referrer_id))
            await db.commit()
            if referrer_id:
                await log_event(user.id, "REFERRAL_REG", f"Referrer: {referrer_id}")
                return True # Новый пользователь по реф. ссылке
        return False # Уже существует или не по реф. ссылке

async def get_all_user_ids():
    """Возвращает список всех user_id."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT user_id FROM users")
        return [row[0] for row in await cur.fetchall()]

async def get_referral_stats(user_id: int):
    """Возвращает количество рефералов и заработок."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Количество рефералов
        cur_ref = await db.execute("SELECT COUNT(*) FROM users WHERE referrer_id = ?", (user_id,))
        ref_count = (await cur_ref.fetchone())[0]

        # Общий заработок с рефералов (фиксированная сумма за привлечение)
        rub_earned = ref_count * REFERRAL_BONUS_RUB

        return ref_count, rub_earned

async def set_user_active_coupon(user_id: int, coupon_id: Optional[int]):
    """Устанавливает активный купон для пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET active_coupon_id = ? WHERE user_id = ?", (coupon_id, user_id))
        await db.commit()

# --- DB Order Functions (Withdraws) ---
async def create_order(user_id:int, typ:str, amount:int, price:float, details:str='', provider:str='manual')->int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO orders(user_id,type,amount,price,status,details,provider) VALUES(?,?,?,?,?,?,?)",
                               (user_id, typ, amount, price, 'pending', details, provider))
        await db.commit()
        return cur.lastrowid

async def update_order_status(order_id:int, status:str, payment_id:Optional[str]=None):
    async with aiosqlite.connect(DB_PATH) as db:
        if payment_id:
            await db.execute("UPDATE orders SET status=?, payment_id=? WHERE id=?", (status, payment_id, order_id))
        else:
            await db.execute("UPDATE orders SET status=? WHERE id=?", (status, order_id))
        await db.commit()

async def get_orders_by_user(user_id:int, limit:int=100):
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, type, amount, price, status, details, created_at FROM orders WHERE user_id = ? ORDER BY created_at DESC LIMIT ?", (user_id, limit))
        return await cur.fetchall()

async def get_pending_withdrawals(limit:int=30):
    """Возвращает ожидающие выводы."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id,user_id,price,details,created_at FROM orders WHERE type = 'withdraw_rub' AND status = 'pending' ORDER BY created_at DESC LIMIT ?", (limit,))
        return await cur.fetchall()
        
async def get_order_data(order_id: int):
    """Возвращает данные о заказе/выводе."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id,user_id,type,amount,price,status,details,created_at FROM orders WHERE id = ?", (order_id,))
        return await cur.fetchone()

# --- DB Ad Functions ---
async def create_ad(user_id: int, title: str, rate: float, min_amount: int, max_amount: int, methods: str, description: str) -> int:
    """Создает новое объявление о продаже."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO ads (user_id, title, rate, min_amount, max_amount, payment_methods, description) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (user_id, title, rate, min_amount, max_amount, methods, description)
        )
        await db.commit()
        return cur.lastrowid

async def get_ads_by_user(user_id: int):
    """Возвращает объявления пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, user_id, title, rate, min_amount, max_amount, payment_methods, active, description FROM ads WHERE user_id = ? ORDER BY active DESC, created_at DESC", (user_id,))
        return await cur.fetchall()

async def get_active_ads():
    """Возвращает все активные объявления."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, user_id, title, rate, min_amount, max_amount, payment_methods, active, description FROM ads WHERE active = 1 ORDER BY created_at DESC")
        return await cur.fetchall()

async def get_ad_data(ad_id: int):
    """Возвращает данные объявления."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, user_id, title, rate, min_amount, max_amount, payment_methods, active, description FROM ads WHERE id = ?", (ad_id,))
        return await cur.fetchone()

async def toggle_ad_active(ad_id: int, active_status: int):
    """Переключает статус активности объявления (0 или 1)."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE ads SET active = ? WHERE id = ?", (active_status, ad_id))
        await db.commit()

# --- DB P2P Deals Functions ---
async def create_deal(buyer_id: int, seller_id: int, ad_id: int, amount: int, price: float, rub_amount: float, roblox_link: str, payment_id: str, coupon_id: Optional[int] = None, coupon_code: Optional[str] = None) -> int:
    """Создает новую P2P сделку в статусе 'pending_payment'."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO deals (buyer_id, seller_id, ad_id, amount, price, rub_amount, roblox_link, payment_id, status, coupon_id, coupon_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (buyer_id, seller_id, ad_id, amount, price, rub_amount, roblox_link, payment_id, 'pending_payment', coupon_id, coupon_code)
        )
        await db.commit()
        return cur.lastrowid

async def update_deal_status(deal_id: int, status: str):
    """Обновляет статус сделки P2P."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET status = ? WHERE id = ?", 
            (status, deal_id)
        )
        await db.commit()

async def set_deal_proof(deal_id: int, file_id: str):
    """Сохраняет file_id скриншота оплаты."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET proof_file_id = ?, status = 'pending_proof' WHERE id = ?",
            (file_id, deal_id)
        )
        await db.commit()

async def get_deal_data(deal_id: int):
    """Возвращает данные о сделке P2P."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, buyer_id, seller_id, ad_id, amount, rub_amount, roblox_link, payment_id, status, proof_file_id, created_at, coupon_id, coupon_code, dispute_reason, dispute_admin_id FROM deals WHERE id = ?", 
            (deal_id,)
        )
        return await cur.fetchone()

async def get_deals_by_user(user_id: int, is_seller: bool, limit: int = 20):
    """Возвращает сделки для покупателя или продавца."""
    role_col = 'seller_id' if is_seller else 'buyer_id'
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            f"SELECT id, amount, rub_amount, status, created_at, buyer_id, seller_id FROM deals WHERE {role_col} = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        return await cur.fetchall()

async def get_dispute_deals():
    """Возвращает сделки в статусе 'dispute'."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, buyer_id, seller_id, amount, rub_amount, created_at, dispute_reason, proof_file_id FROM deals WHERE status = 'dispute' ORDER BY created_at ASC"
        )
        return await cur.fetchall()

async def set_deal_dispute(deal_id: int, reason: str):
    """Переводит сделку в статус спора."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE deals SET status = 'dispute', dispute_reason = ? WHERE id = ?",
            (reason, deal_id)
        )
        await db.commit()

async def resolve_deal_dispute(deal_id: int, winner_id: int, admin_id: int, amount: float):
    """Разрешает спор, переводит средства победителю."""
    async with aiosqlite.connect(DB_PATH) as db:
        # Устанавливаем статус и админа
        await db.execute(
            "UPDATE deals SET status = 'resolved', dispute_admin_id = ?, dispute_resolved_at = CURRENT_TIMESTAMP WHERE id = ?",
            (admin_id, deal_id)
        )
        await db.commit()

        # Добавляем сумму победителю
        # Здесь логика немного сложнее: если победитель - продавец, ему зачисляется rub_amount. Если покупатель - ему возвращается rub_amount.
        # Поскольку деньги покупателя заблокированы на стороне YooKassa (если это была YooKassa оплата), 
        # или если мы используем escrow-модель (что сейчас не реализовано), 
        # самый простой подход: если победитель - продавец, он получает RUB на баланс. Если покупатель - он получает ROBUX (что сложнее).
        # Поскольку деньги покупателя заблокированы на стороне YooKassa (если это была YooKassa оплата), 
        # или если мы используем escrow-модель (что сейчас не реализовано), 
        # самый простой подход: если победитель - продавец, он получает RUB на баланс. Если покупатель - он получает ROBUX (что сложнее).
        # Для текущего кода, где оплата YooKassa идет *напрямую* продавцу:
        # Решение спора должно быть ручным: Админ должен решить, кому зачисляются средства/робуксы.
        # В P2P сделке деньги пошли напрямую продавцу. Если выигрывает покупатель, продавец должен вернуть деньги, или мы должны списать с его баланса
        # Для простоты: Переводим в статус 'resolved' и админ выполняет финансовые операции вручную или через отдельный интерфейс.
        # Пока просто залогируем и переведем в resolved.

        await log_event(admin_id, "DEAL_DISPUTE_RESOLVE", f"Deal #{deal_id} resolved by admin {admin_id}. Winner: {winner_id}. Amount: {amount:.2f} RUB")
        
# --- DB Review Functions ---
async def create_review(reviewer_id: int, target_id: int, deal_id: int, rating: int, comment: str):
    """Создает новый отзыв."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO reviews (reviewer_id, target_id, deal_id, rating, comment) VALUES (?, ?, ?, ?, ?)",
            (reviewer_id, target_id, deal_id, rating, comment)
        )
        await db.commit()

async def get_user_rating_avg(user_id: int) -> Tuple[float, int]:
    """Возвращает средний рейтинг и количество отзывов."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT AVG(rating), COUNT(id) FROM reviews WHERE target_id = ?",
            (user_id,)
        )
        avg, count = await cur.fetchone()
        return float(avg) if avg else 0.0, count

async def get_reviews_for_user(user_id: int, limit: int = 5):
    """Возвращает последние отзывы для пользователя."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT reviewer_id, rating, comment, created_at FROM reviews WHERE target_id = ? ORDER BY created_at DESC LIMIT ?",
            (user_id, limit)
        )
        return await cur.fetchall()

async def get_user_sales_stats(user_id: int) -> Tuple[int, float]:
    """Возвращает количество завершенных продаж и общий заработок."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(id), COALESCE(SUM(rub_amount), 0) FROM deals WHERE seller_id = ? AND status = 'completed'",
            (user_id,)
        )
        count, rub_amount = await cur.fetchone()
        return count, float(rub_amount)

# --- DB Coupon Functions ---
async def create_or_update_coupon(code: str, type: str, value: float, uses_limit: int, min_amount: int, is_active: bool, coupon_id: Optional[int] = None) -> int:
    """Создает или обновляет купон."""
    async with aiosqlite.connect(DB_PATH) as db:
        code = code.upper()
        if coupon_id:
            await db.execute(
                "UPDATE coupons SET type=?, value=?, uses_limit=?, min_amount=?, is_active=?, code=? WHERE id=?",
                (type, value, uses_limit, min_amount, is_active, code, coupon_id)
            )
            cid = coupon_id
        else:
            cur = await db.execute(
                "INSERT INTO coupons (code, type, value, uses_limit, min_amount, is_active) VALUES (?, ?, ?, ?, ?, ?)",
                (code, type, value, uses_limit, min_amount, is_active)
            )
            cid = cur.lastrowid
        await db.commit()
        return cid

async def get_coupon(code: str):
    """Возвращает купон по коду."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, code, type, value, uses_limit, min_amount, is_active FROM coupons WHERE code = ?",
            (code.upper(),)
        )
        return await cur.fetchone()

async def get_all_coupons():
    """Возвращает все купоны."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT id, code, type, value, uses_limit, min_amount, is_active FROM coupons ORDER BY created_at DESC"
        )
        return await cur.fetchall()

async def get_coupon_use_count(coupon_id: int):
    """Возвращает количество использований купона."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM coupon_uses WHERE coupon_id = ?",
            (coupon_id,)
        )
        return (await cur.fetchone())[0]

async def log_coupon_use(coupon_id: int, user_id: int, deal_id: int):
    """Логирует использование купона."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO coupon_uses (coupon_id, user_id, deal_id) VALUES (?, ?, ?)",
            (coupon_id, user_id, deal_id)
        )
        await db.commit()

async def has_user_used_coupon(user_id: int, coupon_id: int):
    """Проверяет, использовал ли пользователь купон ранее."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "SELECT COUNT(*) FROM coupon_uses WHERE user_id = ? AND coupon_id = ?",
            (user_id, coupon_id)
        )
        return (await cur.fetchone())[0] > 0
    

# --- DB Stats Function ---
async def get_stats_by_period(days: int):
    """
    Возвращает статистику за указанный период (в днях).
    :return: (new_users, total_robux_purchased, total_rub_turnover)
    """
    date_from = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d %H:%M:%S')
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM users WHERE created_at >= ?", (date_from,))
        new_users = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COALESCE(SUM(amount), 0) FROM deals WHERE status IN ('paid_waiting_proof', 'pending_proof', 'completed', 'dispute', 'resolved') AND created_at >= ?", (date_from,))
        robux_purchased = (await cur.fetchone())[0]

        cur = await db.execute("SELECT COALESCE(SUM(rub_amount), 0) FROM deals WHERE status IN ('paid_waiting_proof', 'pending_proof', 'completed', 'dispute', 'resolved') AND created_at >= ?", (date_from,))
        rub_turnover = (await cur.fetchone())[0]

        return new_users, robux_purchased, float(rub_turnover)
        
# --- YooKassa Webhook Handler ---
async def handle_yookassa_webhook(request):
    try:
        data = await request.json()
        if data['event'] == 'payment.succeeded':
            payment_id = data['object']['id']
            metadata = data['object'].get('metadata', {})
            deal_id = int(metadata.get('deal_id', 0))

            if deal_id and metadata.get('type') == 'p2p_deal':
                await handle_yookassa_success(deal_id, data['object'])
                
        return web.Response(text="OK", status=200)

    except Exception as e:
        logger.error(f"Error in YooKassa webhook: {e}")
        return web.Response(text="Error", status=500)

async def handle_yookassa_success(deal_id: int, yoo_payment: dict):
    """Обрабатывает успешную оплату P2P сделки."""

    # Получаем данные сделки
    deal_row = await get_deal_data(deal_id)
    if not deal_row:
        print(f"❌ Ошибка: сделка {deal_id} не найдена в БД")
        return web.Response(status=404)

    # ➤ Правильная распаковка ПОЛНОСТЬЮ соответствующая SELECT
    (
        deal_db_id,       # 0 id сделки
        buyer_id,         # 1 покупатель
        seller_id,        # 2 продавец
        ad_id,            # 3 объявление
        amount,           # 4 количество робуксов
        rub_amount,       # 5 сумма в рублях
        roblox_link,      # 6 ссылка на профиль Roblox
        payment_id_db,    # 7 payment_id из БД
        status,           # 8 статус сделки
        proof_file_id,    # 9 файл доказательства
        created_at,       # 10 создано
        coupon_id,        # 11 id купона
        coupon_code,      # 12 код купона
        dispute_reason,   # 13 причина спора
        dispute_admin_id  # 14 админ-арбитр
    ) = deal_row

    # Обработка успешно оплаченной сделки
    if status == 'pending_payment':

        # 1. Обновляем статус
        await update_deal_status(deal_id, 'paid_waiting_proof')

        # 2. Логируем купон (если был)
        if coupon_id:
            await log_coupon_use(coupon_id, buyer_id, deal_id)
            await set_user_active_coupon(buyer_id, None)

        # 3. Уведомление продавцу
        seller_msg = (
            f"🔔 **Новая P2P сделка! №{deal_id}**\n"
            f"Покупатель: [User {escape_markdown_v2(str(buyer_id))}](tg://user?id={buyer_id})"
            f"Вы получите: **{rub_amount:,.2f} ₽**\n"
            f"Аккаунт получателя: {escape_markdown_v2(roblox_link)}\n"
            f"Покупатель: [User {buyer_id}](tg://user?id={buyer_id})\n"
            "**Ожидаем скриншот оплаты от покупателя.**"
        )
        try:
            await bot.send_message(
                seller_id,
                seller_msg,
                parse_mode="MarkdownV2",
                reply_markup=deal_proof_kb(deal_id)
            )
        except TelegramForbiddenError:
            logger.warning(f"Seller {seller_id} blocked bot.")

        # 4. Уведомление покупателю
        buyer_msg = (
            f"✅ **Оплата по сделке №{deal_id} прошла успешно!**\n"
            f"Сумма: **{rub_amount:,.2f} ₽**\n"
            "**Теперь загрузите скриншот оплаты, чтобы продавец мог выдать Robux.**"
        )
        try:
            await bot.send_message(
                buyer_id,
                buyer_msg,
                parse_mode="MarkdownV2",
                reply_markup=deal_proof_kb(deal_id)
            )
        except TelegramForbiddenError:
            logger.warning(f"Buyer {buyer_id} blocked bot.")

        # 5. Уведомление админам
        admin_msg = (
            f"💳 **Оплачен P2P платёж №{deal_id}**\n"
            f"Сумма: {rub_amount:,.2f} ₽\n"
            f"Robux: {amount:,.0f} R\n"
            f"Продавец: [Seller {seller_id}](tg://user?id={seller_id})\n"
            f"Покупатель: [Buyer {buyer_id}](tg://user?id={buyer_id})\n"
            f"Аккаунт: {escape_markdown_v2(roblox_link)}"
            f"Купон: {coupon_code or 'Нет'}"
        )
        for admin in ADMIN_IDS:
            try:
                await bot.send_message(admin, admin_msg, parse_mode="MarkdownV2")
            except Exception:
                pass

        print(f"[DEAL #{deal_id}] Оплата подтверждена — уведомлены стороны.")
        await log_event(buyer_id, "DEAL_PAID", f"Deal: {deal_id}, Rub: {rub_amount}")


# --- Webhook Server Setup (for aiohttp) ---
async def start_webhook_server():
    if not WEBHOOK_HOST:
        return
    
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_yookassa_webhook)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    try:
        await site.start()
        logger.info(f"🌐 Webhook server started at http://0.0.0.0:{PORT}{WEBHOOK_PATH}")
    except Exception as e:
        logger.error(f"Failed to start webhook server: {e}")

# --- Handlers ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

async def set_bot_commands():
    """Устанавливает команды бота."""
    commands = [
        BotCommand(command="/start", description="Начать работу"),
        BotCommand(command="/menu", description="Главное меню")
    ]
    await bot.set_my_commands(commands)

@dp.message(CommandStart())
async def cmd_start(message: types.Message, command: CommandObject, state: FSMContext, bot: Bot):
    await state.clear()
    uid = message.from_user.id
    
    referrer_id = None
    deal_check_id = None # Для проверки сделки
    args = command.args

    if args:
        if args.startswith("ref_"):
            try:
                referrer_id = int(args.split("ref_")[1])
                if referrer_id == uid: referrer_id = None
            except ValueError: pass
        
        # ДОБАВЛЕНО: Обработка возврата после оплаты
        elif args.startswith("deal_"):
            try:
                deal_check_id = int(args.split("deal_")[1])
            except ValueError: pass

    is_new = await create_user_if_not_exists(message.from_user, referrer_id)
    
    # (Логика рефералов остается тут...)
    if is_new and referrer_id:
        # ... ваш код начисления бонуса ...
        pass

    # Если пользователь вернулся после оплаты:
    if deal_check_id:
        # Имитируем нажатие кнопки "Проверить оплату"
        # Нам нужно найти payment_id для этой сделки
        deal_data = await get_deal_data(deal_check_id)
        if deal_data:
             # deal_data[7] это payment_id
             payment_id = deal_data[7] 
             if payment_id:
                 # Вызываем функцию проверки. 
                 # ВАЖНО: Функция deal_check_payment_cb ожидает CallbackQuery, 
                 # но мы тут в Message. Поэтому лучше просто отправить сообщение с кнопкой.
                 await message.answer(
                     f"🔎 **Проверка сделки \\#{deal_check_id}**",
                     reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="🔄 Проверить статус оплаты", callback_data=f"deal_check_payment:{deal_check_id}:{payment_id}")]
                     ]),
                     parse_mode="MarkdownV2"
                 )
                 return # Прерываем, чтобы не слать главное меню поверх

    # ИСПРАВЛЕНО: Экранируем все спецсимволы в тексте приветствия
    text = (
        "👋 **Добро пожаловать в P2P Robux Бот\\!**\n\n"
        "Здесь вы можете:\n"
        "\\- 💰 **Купить** Robux по выгодному курсу у других пользователей\n"
        "\\- 💸 **Продать** свои Robux и заработать\n"
        "\\- 🤝 **Участвовать** в реферальной программе\n\n"
        "Начните с главного меню ниже 👇"
    )
    # Вывод главного меню (ваш старый код)
    await message.answer(text, reply_markup=main_menu_kb(is_admin(uid)), parse_mode="MarkdownV2")
    
@dp.message(Command("menu"))
async def cmd_menu(message: types.Message, state: FSMContext):
    await state.clear()
    uid = message.from_user.id
    await message.answer("🏠 **Главное меню**\nВыберите действие:", reply_markup=main_menu_kb(is_admin(uid)), parse_mode="MarkdownV2")

@dp.callback_query(F.data == "back_main")
@dp.callback_query(F.data == "menu")
async def back_main_handler(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    await call.answer()
    try:
        await call.message.edit_text(
            "🏠 **Главное меню**\nВыберите действие:",
            reply_markup=main_menu_kb(is_admin(uid)),
            parse_mode="MarkdownV2"
        )
    except TelegramBadRequest:
        pass # Сообщение не изменилось

# --- Main Menu Handlers ---
@dp.callback_query(lambda c: c.data and c.data.startswith("menu_"))
async def menu_handlers(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    uid = call.from_user.id
    action = call.data.split("_")[1]

    if action == "buy":
        await call.answer()
        # Проверяем активный купон
        user_data = await get_user_data(uid)
        active_coupon_id = user_data[4]
        coupon_info = ""
        if active_coupon_id:
            coupon = await get_coupon_data(active_coupon_id)
            if coupon:
                coupon_code, c_type, value, min_amount = coupon[1], coupon[2], coupon[3], coupon[5]
                discount_str = f"{value:.2f} ₽" if c_type == 'fixed' else f"{value:.2f}%"
                coupon_info = f"🔔 Активный купон: **{coupon_code}** \\({discount_str}\\)\n"

        await call.message.edit_text(
            f"🛒 **Меню Покупки Robux**\n\n{coupon_info}Выберите действие:",
            reply_markup=buy_menu_kb(),
            parse_mode="MarkdownV2"
        )
        return

    if action == "sell":
        await call.answer()
        await call.message.edit_text(
            "💰 **Меню Продажи Robux \\(P2P\\)**\n\n"
            "Здесь вы можете размещать объявления о продаже Robux и управлять ими\\.",
            reply_markup=sell_menu_kb(),
            parse_mode="MarkdownV2"
        )
        return

    if action == "profile":
        bal = await get_user_balance(uid)
        ref_count, ref_earned = await get_referral_stats(uid)
        
        text = (
            f"👤 **Ваш профиль**\n"
            f"Баланс: **{bal:,.2f} ₽**\n"
            f"ID: `{uid}`\n"
            f"Рефералы: **{ref_count}**\n"
            f"Реф\\. заработок: **{ref_earned:,.2f} ₽**"
        )
        await call.message.edit_text(text, reply_markup=profile_kb(uid), parse_mode="MarkdownV2")
        await call.answer()
        return

    if action == "admin":
        if is_admin(uid):
            await call.message.edit_text("🛠 Админ\\-панель", reply_markup=admin_main_kb())
        else:
            await call.answer("Доступ запрещен\\.", show_alert=True)
        return

@dp.callback_query(F.data == "support")
async def support_handler(callback: types.CallbackQuery, bot: Bot):
    await callback.answer()
    if not SUPPORT_ADMIN_ID:
        return await callback.message.answer("К сожалению, поддержка временно недоступна\\.")
        
    await callback.message.edit_text(
        f"✉️ **Написать в поддержку**\n\n"
        f"Для связи с администратором, пожалуйста, перейдите по ссылке: "
        f"[Поддержка](tg://user?id={SUPPORT_ADMIN_ID})\n"
        f"Ваш ID будет автоматически передан администратору\\.",
        reply_markup=back_main_kb(is_admin(callback.from_user.id)),
        parse_mode="MarkdownV2"
    )

# --- Profile Handlers ---
@dp.callback_query(F.data == "profile_referral")
async def profile_referral_cb(call: types.CallbackQuery):
    await call.answer()
    uid = call.from_user.id
    
    ref_count, ref_earned = await get_referral_stats(uid)
    
    bot_username = os.getenv('BOT_USERNAME', 'MyBot')
    ref_link = f"https://t.me/{bot_username}?start=ref_{uid}"
    
    ref_link_esc = escape_markdown_v2(ref_link)
    ref_earned_esc = escape_markdown_v2(f"{ref_earned:,.2f}")
    bonus_esc = escape_markdown_v2(f"{REFERRAL_BONUS_RUB}")
    
    text = (
        f"🤝 **Партнерская программа**\n\n"
        f"Приглашайте друзей и получайте **{bonus_esc} ₽** на баланс "
        f"за каждого нового пользователя\\!\n\n"
        f"📊 **Ваша статистика:**\n"
        f"👥 Приглашено людей: **{ref_count}**\n"
        f"💰 Всего заработано: **{ref_earned_esc} ₽**\n\n"
        f"🔗 **Ваша ссылка для приглашения:**\n"
        f"`{ref_link_esc}`"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 Отправить другу", 
                             url=f"https://t.me/share/url?url={ref_link}&text=Заходи%20и%20покупай%20Robux%20выгодно!")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_profile")]
    ])
    
    await call.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")

def format_number(n):
    try:
        return f"{int(n):,}".replace(",", " ")
    except:
        return str(n)

def status_icon(event_type):
    if event_type in ["BUY_ROBUX", "REFILL_BALANCE", "REFERRAL_BONUS"]:
        return "🟢"
    if event_type == "WITHDRAW_RUB":
        return "🟡"
    return "🔴"

async def inline_profile_menu(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_profile")]
    ])

async def get_latest_transactions(user_id: int, limit: int = 10):
    async with aiosqlite.connect(DB_PATH) as db:
        query = """
            SELECT event_type, details, timestamp 
            FROM logs 
            WHERE user_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """
        cursor = await db.execute(query, (user_id, limit))
        return await cursor.fetchall()

@dp.callback_query(F.data == "profile_tx")
async def profile_tx_cb(call: types.CallbackQuery):
    await call.answer("Загрузка ваших транзакций\\.\\.\\.", show_alert=False)
    uid = call.from_user.id

    transactions = await get_latest_transactions(uid, limit=10)

    text = ["**💳 Ваши последние транзакции**\n"]

    if not transactions:
        text.append("У вас пока нет транзакций\\.")
    else:
        for event_type, details, created_at in transactions:

            readable_details = {}
            if details:
                try:
                    readable_details = json.loads(details) 
                except:
                    readable_details = {"raw": details}

            try:
                tx_date = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S").strftime("%d\\.%m\\.%Y %H:%M")
            except ValueError:
                tx_date = escape_markdown_v2(created_at)
            
            tx_date_esc = escape_markdown_v2(tx_date)
            icon = status_icon(event_type)
            line = f"{icon} \\-\\- {tx_date_esc}: "

            if event_type == "BUY_ROBUX":
                amount = escape_markdown_v2(format_number(readable_details.get("robux_amount", "N/A")))
                price = escape_markdown_v2(format_number(readable_details.get("rub_price", "N/A")))
                line += f"**ПОКУПКА R**: {amount} R за {price} ₽"

            elif event_type == "WITHDRAW_RUB":
                amount = escape_markdown_v2(format_number(readable_details.get("rub_amount", "N/A")))
                line += f"**ВЫВОД RUB**: \\-{amount} ₽"

            elif event_type == "REFILL_BALANCE":
                amount = escape_markdown_v2(format_number(readable_details.get("rub_amount", "N/A")))
                line += f"**ПОПОЛНЕНИЕ БАЛАНСА**: \\+{amount} ₽"

            elif event_type == "REFERRAL_BONUS":
                bonus = escape_markdown_v2(format_number(readable_details.get("bonus_amount", "N/A")))
                ref_user = readable_details.get("ref_user_id", "N/A")
                line += f"**РЕФЕРАЛ БОНУС**: \\+{bonus} ₽ \\(от {ref_user}\\)"

            else:
                details_esc = escape_markdown_v2(str(readable_details.get("raw", details)))
                line += f"{escape_markdown_v2(event_type)}: {details_esc}"

            text.append(line)

    kb = inline_profile_menu(uid)

    await call.message.edit_text(
        "\n".join(text),
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )

# --- Withdraw Flow ---
@dp.callback_query(F.data == "profile_withdraw")
async def withdraw_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    uid = call.from_user.id
    balance = await get_user_balance(uid)
    min_withdraw_str = await get_config("min_withdraw")
    min_withdraw = float(min_withdraw_str) if min_withdraw_str else 100.0

    if balance < min_withdraw:
        return await call.message.edit_text(
            f"❌ **Вывод средств**\n\n"
            f"Минимальная сумма вывода: **{min_withdraw:,.2f} ₽**\n"
            f"Ваш баланс: **{balance:,.2f} ₽**\n\n"
            f"Недостаточно средств\\.",
            reply_markup=profile_kb(uid),
           parse_mode="MarkdownV2"
        )
    
    await state.update_data(balance=balance, min_withdraw=min_withdraw)
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]])
    await call.message.edit_text(
        f"💸 **Вывод средств**\n\n"
        f"Ваш баланс: **{balance:,.2f} ₽**\n"
        f"Введите сумму в рублях для вывода \\(мин\\. {min_withdraw:,.2f} ₽\\):",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(WithdrawStates.amount_rub)

@dp.message(WithdrawStates.amount_rub)
async def withdraw_amount_rub(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    data = await state.get_data()
    balance = data['balance']
    min_withdraw = data['min_withdraw']
    
    try:
        amount = float(message.text.replace(',', '.').strip())
        if amount <= 0:
            raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение\\. Введите положительное число\\.")
    
    if amount > balance:
        return await message.reply(f"Недостаточно средств\\. Ваш баланс: {balance:,.2f} ₽", parse_mode="MarkdownV2")

    if amount < min_withdraw:
        return await message.reply(f"Минимальная сумма вывода: {min_withdraw:,.2f} ₽", parse_mode="MarkdownV2")

    await state.update_data(amount=amount)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="СБП \\(Сбер, Тинькофф и т\\.\\.д\\.\\)", callback_data="withdraw_method:sbp")],
        [InlineKeyboardButton(text="Qiwi/ЮMoney", callback_data="withdraw_method:other")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]
    ])
    
    await message.reply(f"Сумма: **{amount:,.2f} ₽**\n\nВыберите способ вывода:", reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(WithdrawStates.method)

@dp.callback_query(lambda c: c.data and c.data.startswith("withdraw_method:"), WithdrawStates.method)
async def withdraw_method_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    method = call.data.split(":")[1]
    await state.update_data(method=method)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]])
    prompt = "Введите номер телефона или реквизиты для выбранного метода вывода \\(например, `\\+79991234567`\\):"
    
    await call.message.edit_text(prompt, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(WithdrawStates.details)

@dp.message(WithdrawStates.details)
async def withdraw_details(message: types.Message, state: FSMContext, bot: Bot):
    uid = message.from_user.id
    # Экранируем ввод пользователя, чтобы не сломать Markdown
    details = escape_markdown_v2(message.text.strip())
    
    data = await state.get_data()
    # Берем из FSM только сумму и метод. Баланс из FSM брать НЕЛЬЗЯ (он мог устареть).
    amount = data.get('amount')
    method = data.get('method')

    if not amount or not method:
        await state.clear()
        return await message.reply("❌ Ошибка сессии. Попробуйте начать вывод заново.")

    async with aiosqlite.connect(DB_PATH) as db:
        # 1. Получаем АКТУАЛЬНЫЙ баланс из БД прямо сейчас
        async with db.execute("SELECT balance FROM users WHERE user_id = ?", (uid,)) as cursor:
            row = await cursor.fetchone()
            
        if not row:
            await state.clear()
            return await message.reply("❌ Ошибка: Пользователь не найден.")
            
        current_real_balance = row[0]

        # 2. Проверяем, хватает ли денег (с защитой от отрицательного баланса)
        if current_real_balance < amount:
            await state.clear()
            return await message.reply(
                f"❌ **Ошибка вывода**\n"
                f"Ваш актуальный баланс: **{current_real_balance:,.2f} ₽**\n"
                f"Вы пытаетесь вывести: **{amount:,.2f} ₽**\n"
                f"Недостаточно средств.",
                parse_mode="MarkdownV2"
            )

        # 3. Вычисляем новый баланс с округлением (защита от float ошибок)
        new_balance = round(current_real_balance - amount, 2)

        try:
            # 4. Атомарная операция: Списываем деньги и создаем ордер
            await db.execute("UPDATE users SET balance = ? WHERE user_id = ?", (new_balance, uid))
            
            cursor = await db.execute(
                "INSERT INTO orders(user_id, type, amount, price, status, details, provider) VALUES(?,?,?,?,?,?,?)",
                (uid, 'withdraw_rub', int(amount * 100), amount, 'pending', f"Method: {method}, Details: {details}", 'withdraw')
            )
            order_id = cursor.lastrowid
            
            # Фиксируем изменения
            await db.commit()
            
        except Exception as e:
            logger.error(f"DB Error during withdraw: {e}")
            await message.reply("❌ Произошла ошибка базы данных. Попробуйте позже.")
            return

    # 5. Логирование и уведомления (уже после успешного коммита в БД)
    await log_event(uid, "WITHDRAW_REQUEST", f"Order: {order_id}, Amount: {amount:.2f}")
    
    await message.reply(
        f"✅ **Заявка на вывод принята!**\n"
        f"Сумма: **{amount:,.2f} ₽**\n"
        f"Ваш новый баланс: **{new_balance:,.2f} ₽**\n"
        "Ожидайте обработки администратором.",
        reply_markup=profile_kb(uid),
        parse_mode="MarkdownV2"
    )
    
    # Уведомление администраторов
    admin_msg = (
        f"🔔 **Новый вывод средств ожидает обработки!**\n"
        f"ID заявки: `#{order_id}`\n"
        f"Пользователь: [User {uid}](tg://user?id={uid})\n"
        f"Сумма: **{amount:,.2f} ₽**\n"
        f"Метод: `{method.upper()}`\n"
        f"Реквизиты: `{details}`"
    )
    
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_msg, parse_mode="MarkdownV2", reply_markup=admin_main_kb())
        except Exception:
            pass

    await state.clear()

# --- Admin Handlers ---
@dp.callback_query(F.data == "back_admin")
async def back_admin_cb(call: types.CallbackQuery, state: FSMContext):
    await state.clear()
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    await call.message.edit_text("🛠 Админ-панель", reply_markup=admin_main_kb())

# --- Admin Disputes/Deals ---
@dp.callback_query(F.data == "adm_deals_dispute")
async def adm_deals_dispute_cb(call: types.CallbackQuery):
    if not is_admin(call.from_user.id): 
        return await call.answer("Доступ запрещён.", show_alert=True)
    
    await call.answer("Загрузка споров...")

    disputes = await get_dispute_deals()
    text = ["**📦 Споры по P2P Сделкам**\n"]
    kb = InlineKeyboardBuilder()

    if not disputes:
        # FIXED: Added backslash before the dot
        text.append("Активных споров нет\\.") 
    else:
        for deal_id, buyer_id, seller_id, amount, rub_amount, created_at, dispute_reason, proof_file_id in disputes:
            
            # 1. Экранирование строки с датой
            date_info = escape_markdown_v2(format_date(created_at))
            
            # 2. Экранирование строки с суммами
            amount_info = f"Robux: {amount:,.0f} R | Сумма: {rub_amount:,.2f} ₽"
            amount_info_escaped = escape_markdown_v2(amount_info)
            
            # 3. Экранирование причины
            dispute_reason_escaped = escape_markdown_v2(dispute_reason)
            
            # Добавление строк в текст
            text.append(f"➖" * 15)
            # FIXED: Added backslashes before parentheses \( and \)
            text.append(f"**Спор \\#{deal_id}** \\(от {date_info}\\)") 
            text.append(amount_info_escaped)
            text.append(f"Покупатель: [User {buyer_id}](tg://user?id={buyer_id})")
            text.append(f"Продавец: [User {seller_id}](tg://user?id={seller_id})")
            text.append(f"Причина: {dispute_reason_escaped}")
            
            # Кнопка
            kb.row(InlineKeyboardButton(text=f"🔍 Спор #{deal_id}", callback_data=f"adm_view_dispute:{deal_id}"))

    # Кнопка Назад
    kb.row(InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin"))

    # Отправка сообщения
    await call.message.edit_text("\n".join(text), reply_markup=kb.as_markup(), parse_mode="MarkdownV2")

@dp.callback_query(lambda c: c.data and c.data.startswith("adm_view_dispute:"))
async def adm_view_dispute_cb(call: types.CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Просмотр спора...")

    deal_id = int(call.data.split(":")[1])
    deal_data = await get_deal_data(deal_id)

    if not deal_data:
        return await call.message.edit_text("Спор не найден.", reply_markup=back_admin_kb())

    _, buyer_id, seller_id, amount, rub_amount, roblox_link, _, status, proof_file_id, created_at, _, _, dispute_reason, _ = deal_data

    text = [
        f"**🛠 Разрешение спора по сделке #{deal_id}**\n",
        f"Статус: **{status.upper()}**",
        f"Robux: **{amount:,.0f} R** | Сумма: **{rub_amount:,.2f} ₽**",
        f"Покупатель: [User {buyer_id}](tg://user?id={buyer_id})",
        f"Продавец: [User {seller_id}](tg://user?id={seller_id})",
        f"Ссылка Roblox: {escape_markdown_v2(roblox_link)}",
        f"Причина спора: {escape_markdown_v2(dispute_reason or 'Не указана')}",
        "───────────────────────────"
    ]

    kb = InlineKeyboardBuilder()

    if proof_file_id:
        text.append("📸 **Есть скриншот оплаты/пруф**")
        kb.row(InlineKeyboardButton(text="🖼 Посмотреть пруф", callback_data=f"adm_show_proof:{deal_id}"))
    else:
        text.append("❌ **Нет скриншота оплаты/пруфа**")


    if status == 'dispute':
        kb.row(
            InlineKeyboardButton(text="✅ Выдать Продавцу", callback_data=f"adm_resolve_dispute:{deal_id}:{seller_id}:{rub_amount}"),
            InlineKeyboardButton(text="❌ Выдать Покупателю", callback_data=f"adm_resolve_dispute:{deal_id}:{buyer_id}:{rub_amount}")
        )
    
    kb.row(InlineKeyboardButton(text="◀️ Назад к спорам", callback_data="adm_deals_dispute"))

    await call.message.edit_text("\n".join(text), reply_markup=kb.as_markup(), parse_mode="MarkdownV2")


@dp.callback_query(lambda c: c.data and c.data.startswith("adm_show_proof:"))
async def adm_show_proof_cb(call: types.CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Отправка пруфа...")

    deal_id = int(call.data.split(":")[1])
    deal_data = await get_deal_data(deal_id)
    
    if not deal_data:
        return await call.answer("Сделка не найдена.", show_alert=True)
        
    proof_file_id = deal_data[9]
    if not proof_file_id:
        return await call.answer("Скриншот не найден.", show_alert=True)

    try:
        # Отправляем фото
        await bot.send_photo(
            chat_id=call.from_user.id,
            photo=proof_file_id,
            caption=f"📸 **Скриншот оплаты/пруф по сделке #{deal_id}**",
            parse_mode="MarkdownV2"
        )
        await call.answer("Скриншот отправлен в личные сообщения.", show_alert=True)
    except Exception as e:
        logger.error(f"Error sending proof photo: {e}")
        await call.answer("Ошибка при отправке скриншота.", show_alert=True)

@dp.callback_query(lambda c: c.data and c.data.startswith("adm_resolve_dispute:"))
async def adm_resolve_dispute_cb(call: types.CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Разрешение спора...")

    try:
        _, deal_id_str, winner_id_str, amount_str = call.data.split(":")
        deal_id = int(deal_id_str)
        winner_id = int(winner_id_str)
        amount = float(amount_str)
    except ValueError:
        return await call.answer("Некорректные данные в callback.", show_alert=True)
    
    admin_id = call.from_user.id
    deal_data = await get_deal_data(deal_id)
    if not deal_data or deal_data[8] != 'dispute': # Проверка статуса
        return await call.message.edit_text(f"Сделка #{deal_id} не найдена или спор уже разрешен.", reply_markup=back_admin_kb())

    await resolve_deal_dispute(deal_id, winner_id, admin_id, amount)
    
    # Уведомление сторон
    buyer_id, seller_id = deal_data[1], deal_data[2]
    
    winner_msg = f"✅ **Спор по сделке #{deal_id} разрешен!** Администратор принял решение в вашу пользу. Свяжитесь с продавцом/покупателем для завершения сделки."
    loser_msg = f"❌ **Спор по сделке #{deal_id} разрешен!** Администратор принял решение не в вашу пользу. Если вы не согласны, свяжитесь с поддержкой."
    
    try:
        await bot.send_message(winner_id, winner_msg, parse_mode="MarkdownV2")
        if winner_id == buyer_id:
            await bot.send_message(seller_id, loser_msg, parse_mode="MarkdownV2")
        else:
            await bot.send_message(buyer_id, loser_msg, parse_mode="MarkdownV2")
    except Exception as e:
        logger.error(f"Error notifying deal parties: {e}")

    await call.message.edit_text(
        f"✅ **Спор по сделке #{deal_id} разрешен!**\nПобедитель: [User {winner_id}](tg://user?id={winner_id})\n"
        f"Администратор должен выполнить финансовые операции вручную (списание/возврат).",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ К спорам", callback_data="adm_deals_dispute")]]),
        parse_mode="MarkdownV2"
    )

# --- Admin Withdraws ---
@dp.callback_query(F.data == "adm_withdraws")
async def adm_withdraws_cb(call: types.CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Загрузка ожидающих выводов...")
    
    withdrawals = await get_pending_withdrawals(limit=30)
    # ИСПРАВЛЕНО: Экранированы скобки ( и ) в заголовке
    text = ["**💸 Ожидающие выводы средств \\(RUB\\)**\n"]
    kb = InlineKeyboardBuilder()

    if not withdrawals:
        # ИСПРАВЛЕНО: Экранирована точка в конце
        text.append("Активных заявок на вывод нет\\.")
    else:
        for order_id, user_id, amount, details, created_at in withdrawals:
            text.append(f"➖" * 15)
            
            # ИСПРАВЛЕНО: Экранирование данных перед вставкой
            date_esc = escape_markdown_v2(format_date(created_at))
            amount_esc = escape_markdown_v2(f"{amount:,.2f}")
            
            # ИСПРАВЛЕНО: Экранированы #, ( и )
            text.append(f"**Заявка \\#{order_id}** \\(от {date_esc}\\)")
            text.append(f"Сумма: **{amount_esc} ₽**")
            text.append(f"Пользователь: [User {user_id}](tg://user?id={user_id})")
            
            # Обработка реквизитов
            match = re.search(r"Method: (\w+), Details: (.*)", details)
            if match:
                method, details_str = match.groups()
                # Для блоков кода (backticks) экранируем только ` и \
                safe_method = method.upper().replace('\\', '\\\\').replace('`', '\\`')
                safe_details = details_str.replace('\\', '\\\\').replace('`', '\\`')
                
                text.append(f"Метод: `{safe_method}`")
                text.append(f"Реквизиты: `{safe_details}`")
            else:
                # Если формат не совпал, выводим как есть с полным экранированием
                text.append(f"Info: {escape_markdown_v2(details)}")

            kb.row(InlineKeyboardButton(text=f"✅ Обработать #{order_id}", callback_data=f"adm_complete_withdraw:{order_id}"))

    kb.row(InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin"))

    await call.message.edit_text("\n".join(text), reply_markup=kb.as_markup(), parse_mode="MarkdownV2")

@dp.callback_query(lambda c: c.data and c.data.startswith("adm_complete_withdraw:"))
async def adm_complete_withdraw_cb(call: types.CallbackQuery, bot: Bot):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Обработка вывода...")
    
    order_id = int(call.data.split(":")[1])
    order_data = await get_order_data(order_id)
    
    if not order_data:
        return await call.message.edit_text("Заявка не найдена.", reply_markup=back_admin_kb())
        
    o_id, user_id, typ, amount_int, price, status, details, created_at = order_data[0], order_data[1], order_data[2], order_data[3], order_data[4], order_data[5], order_data[6], order_data[7]
    amount_rub = price
    
    if status != 'pending':
        return await call.message.edit_text(f"Заявка #{order_id} уже обработана (Статус: {status.upper()}).", reply_markup=back_admin_kb())

    # Меняем статус
    await update_order_status(order_id, 'completed')
    await log_event(user_id, "WITHDRAW_COMPLETED", f"Order: {order_id}, Admin: {call.from_user.id}")

    # Уведомление пользователя
    try:
        await bot.send_message(
            user_id,
            f"✅ **Ваша заявка на вывод #{order_id} выполнена!**\n"
            f"Сумма: **{amount_rub:,.2f} ₽**\n"
            f"Проверьте свои реквизиты.",
            parse_mode="MarkdownV2"
        )
    except TelegramForbiddenError:
        pass

    await call.message.edit_text(
        f"✅ **Заявка #{order_id} (Вывод {amount_rub:,.2f} ₽) успешно помечена как ВЫПОЛНЕННАЯ.**\n"
        "Средства должны быть отправлены вручную.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="◀️ К ожидающим выводам", callback_data="adm_withdraws")]]),
        parse_mode="MarkdownV2"
    )

# --- Admin User Management ---
@dp.callback_query(F.data == "adm_users")
async def adm_users_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await call.message.edit_text(
        "👤 **Управление пользователями**\n"
        "Введите **ID** пользователя, чей баланс хотите изменить:",
        reply_markup=kb
    )
    await state.set_state(AdminUserManagement.enter_user_id)

@dp.message(AdminUserManagement.enter_user_id)
async def adm_user_id_entered(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        target_id = int(message.text.strip())
    except ValueError:
        return await message.reply("Некорректный ID пользователя. Введите число.")
    
    user_data = await get_user_data(target_id)
    if not user_data:
        return await message.reply("Пользователь с таким ID не найден.")
        
    username, balance, created_at, _, _ = user_data
    
    await state.update_data(target_user_id=target_id, old_balance=balance)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await message.reply(
        f"**Управление пользователем `{target_id}`**\n"
        f"Username: @{username}\n"
        f"Текущий баланс: **{balance:,.2f} ₽**\n\n"
        "Введите **НОВЫЙ БАЛАНС** для этого пользователя:",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(AdminUserManagement.enter_new_balance)

@dp.message(AdminUserManagement.enter_new_balance)
async def adm_new_balance_entered(message: types.Message, state: FSMContext, bot: Bot):
    if not is_admin(message.from_user.id): return
    try:
        new_balance = float(message.text.replace(',', '.').strip())
        if new_balance < 0:
            raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите положительное число.")

    data = await state.get_data()
    target_id = data['target_user_id']
    old_balance = data['old_balance']
    
    await update_user_balance(target_id, new_balance)
    await log_event(target_id, "ADMIN_BALANCE_CHANGE", f"Admin {message.from_user.id} changed balance from {old_balance:.2f} to {new_balance:.2f}")

    # Уведомление пользователя
    try:
        await bot.send_message(
            target_id,
            f"🔔 **Ваш баланс был изменен администратором!**\n"
            f"Старый баланс: **{old_balance:,.2f} ₽**\n"
            f"Новый баланс: **{new_balance:,.2f} ₽**",
            parse_mode="MarkdownV2"
        )
    except Exception:
        pass

    await message.reply(
        f"✅ Баланс пользователя `{target_id}` успешно обновлен.\n"
        f"Новый баланс: **{new_balance:,.2f} ₽**",
        reply_markup=back_admin_kb(),
        parse_mode="MarkdownV2"
    )
    await state.clear()

# --- Admin Stats ---
@dp.callback_query(F.data == "adm_stats")
async def adm_stats_cb(call: types.CallbackQuery):
    """Показывает меню статистики."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    await call.message.edit_text(
        "📊 **Статистика**\n"
        "Выберите период для просмотра:",
        reply_markup=admin_stats_kb()
    )

@dp.callback_query(lambda c: c.data and c.data.startswith("stats_period:"))
async def stats_period_cb(call: types.CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Загрузка статистики...")
    
    try:
        days = int(call.data.split(":")[1])
    except ValueError:
        return await call.answer("Некорректный период.", show_alert=True)

    new_users, robux_purchased, rub_turnover = await get_stats_by_period(days)
    
    text = (
        f"📊 **Статистика за последние {days} дней**\n"
        "───────────────────────────\n"
        f"👤 Новых пользователей: **{new_users:,}**\n"
        f"📦 Robux куплено: **{robux_purchased:,.0f} R**\n"
        f"💰 Оборот (RUB): **{rub_turnover:,.2f} ₽**"
    )

    await call.message.edit_text(text, reply_markup=admin_stats_kb(), parse_mode="MarkdownV2")


# --- Admin Broadcast ---
@dp.callback_query(F.data == "adm_broadcast")
async def broadcast_start_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await call.message.edit_text(
        "💌 **Массовая рассылка**\n"
        "Введите текст сообщения для рассылки (поддерживается Markdown):",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(BroadcastStates.text)

@dp.message(BroadcastStates.text)
async def broadcast_text(message: types.Message, state: FSMContext):
    """Получает текст и запрашивает подтверждение."""
    if not is_admin(message.from_user.id): return
    text_to_send = message.text
    await state.update_data(text=text_to_send)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Начать рассылку", callback_data="broadcast_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]
    ])
    
    await message.reply(
        "**Подтверждение рассылки:**\n"
        "Вы уверены, что хотите отправить следующее сообщение всем пользователям?\n"
        "───────────────────────────\n"
        f"{text_to_send}",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(BroadcastStates.confirm)

@dp.callback_query(F.data == "broadcast_confirm", BroadcastStates.confirm)
async def broadcast_confirm_cb(call: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Выполняет рассылку."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Запуск рассылки...")
    
    data = await state.get_data()
    text = data['text']
    user_ids = await get_all_user_ids()
    sent_count = 0
    blocked_count = 0
    
    await call.message.edit_text(f"⏳ **Рассылка запущена...** (0/{len(user_ids)})")
    
    for uid in user_ids:
        await asyncio.sleep(0.1) # Задержка для предотвращения троттлинга
        try:
            await bot.send_message(uid, text, parse_mode="MarkdownV2")
            sent_count += 1
        except TelegramForbiddenError:
            blocked_count += 1
        except Exception:
            pass
            
        if (sent_count + blocked_count) % 50 == 0:
            try:
                await call.message.edit_text(f"⏳ **Рассылка в процессе...** ({sent_count}/{len(user_ids)}) Отправлено.")
            except TelegramBadRequest:
                pass # Сообщение не изменилось

    await log_event(call.from_user.id, "BROADCAST_SENT", f"Total: {len(user_ids)}, Sent: {sent_count}, Blocked: {blocked_count}")
    await state.clear()
    
    await call.message.edit_text(
        f"✅ **Рассылка завершена!**\n"
        f"Всего пользователей: **{len(user_ids)}**\n"
        f"Успешно отправлено: **{sent_count}**\n"
        f"Заблокировали бота: **{blocked_count}**",
        reply_markup=back_admin_kb(),
        parse_mode="MarkdownV2"
    )


# --- Admin Coupon Management ---
@dp.callback_query(F.data == "adm_coupons")
async def adm_coupons_cb(call: types.CallbackQuery):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    await call.message.edit_text("🎫 **Управление Купонами**", reply_markup=admin_coupons_kb())

@dp.callback_query(F.data == "coupon_create")
async def coupon_create_start(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    await state.clear()
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await call.message.edit_text("Введите **КОД** купона (только латинские буквы и цифры, без пробелов):", reply_markup=kb)
    await state.set_state(AdminCouponStates.enter_code)

@dp.message(AdminCouponStates.enter_code)
async def coupon_enter_code(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    code = message.text.strip().upper()
    
    if not re.match(r"^[A-Z0-9]+$", code):
        return await message.reply("Некорректный код. Используйте только латинские буквы и цифры.")
        
    if await get_coupon(code):
        return await message.reply("Купон с таким кодом уже существует. Введите другой код.")

    await state.update_data(code=code)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Процент (%)", callback_data="coupon_type:percent")],
        [InlineKeyboardButton(text="Фикс. сумма (₽)", callback_data="coupon_type:fixed")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]
    ])
    await message.reply("Выберите **тип** скидки:", reply_markup=kb)
    await state.set_state(AdminCouponStates.enter_type)

@dp.callback_query(lambda c: c.data and c.data.startswith("coupon_type:"), AdminCouponStates.enter_type)
async def coupon_enter_type(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer()
    c_type = call.data.split(":")[1]
    await state.update_data(type=c_type)
    
    prompt = "Введите **процент** скидки (например, `10`):"
    if c_type == 'fixed':
        prompt = "Введите **фиксированную сумму** скидки в рублях (например, `100.50`):"
        
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await call.message.edit_text(prompt, reply_markup=kb)
    await state.set_state(AdminCouponStates.enter_value)

@dp.message(AdminCouponStates.enter_value)
async def coupon_enter_value(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        value = float(message.text.replace(',', '.').strip())
        if value <= 0:
            raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите положительное число.")
        
    await state.update_data(value=value)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await message.reply("Введите **лимит использований** (0 для бесконечного):", reply_markup=kb)
    await state.set_state(AdminCouponStates.enter_limit)

@dp.message(AdminCouponStates.enter_limit)
async def coupon_enter_limit(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        limit = int(message.text.strip())
        if limit < 0:
            raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите целое число (0 или больше).")
        
    await state.update_data(uses_limit=limit)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]])
    await message.reply("Введите **минимальное количество Robux** для активации (0 для без ограничений):", reply_markup=kb)
    await state.set_state(AdminCouponStates.enter_min_amount)

@dp.message(AdminCouponStates.enter_min_amount)
async def coupon_enter_min_amount(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id): return
    try:
        min_amount = int(message.text.strip())
        if min_amount < 0:
            raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите целое число (0 или больше).")
        
    await state.update_data(min_amount=min_amount)
    data = await state.get_data()
    
    c_type_str = "Процент" if data['type'] == 'percent' else "Фикс. сумма"
    value_str = f"{data['value']:.2f}%" if data['type'] == 'percent' else f"{data['value']:.2f} ₽"
    limit_str = "Безлимитно" if data['uses_limit'] == 0 else f"{data['uses_limit']}"
    min_amount_str = "Нет" if data['min_amount'] == 0 else f"{data['min_amount']:,.0f} R"

    text = (
        "**Подтверждение создания купона:**\n"
        "───────────────────────────\n"
        f"**Код:** `{data['code']}`\n"
        f"**Тип:** {c_type_str}\n"
        f"**Скидка:** {value_str}\n"
        f"**Лимит:** {limit_str}\n"
        f"**Мин. Robux:** {min_amount_str}"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Создать купон", callback_data="coupon_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_admin")]
    ])
    
    await message.reply(text, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(AdminCouponStates.confirm)

@dp.callback_query(F.data == "coupon_confirm", AdminCouponStates.confirm)
async def coupon_confirm_cb(call: types.CallbackQuery, state: FSMContext):
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Создание купона...")

    data = await state.get_data()
    code = data['code']
    c_type = data['type']
    value = data['value']
    limit = data['uses_limit']
    min_amount = data['min_amount']

    try:
        coupon_id = await create_or_update_coupon(code, c_type, value, limit, min_amount, True)
        await call.message.edit_text(
            f"✅ Купон **{code}** (ID: #{coupon_id}) успешно создан!", 
            reply_markup=admin_coupons_kb(), 
            parse_mode="MarkdownV2"
        )
        await log_event(call.from_user.id, "COUPON_CREATE", f"Code: {code}, Value: {value}")
    except Exception as e:
        logger.error(f"Coupon creation error: {e}")
        await call.message.edit_text(
            f"❌ Ошибка при создании купона. Попробуйте снова. ({e})",
            reply_markup=admin_coupons_kb()
        )
    await state.clear()

@dp.callback_query(F.data == "coupon_list")
async def coupon_list_cb(call: types.CallbackQuery):
    """Показывает список всех купонов."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Загрузка купонов...")

    coupons = await get_all_coupons()
    text = ["**📋 Список Купонов**\n"]
    kb_builder = InlineKeyboardBuilder()

    if not coupons:
        text.append("Купонов нет.")
    else:
        for coupon_data in coupons:
            c_id, code, c_type, value, limit, min_amount, is_active = coupon_data
            
            status = "🟢" if is_active else "🔴"
            value_str = f"{value:.2f}%" if c_type == 'percent' else f"{value:.2f} ₽"
            uses_count = await get_coupon_use_count(c_id)
            limit_str = "∞" if limit == 0 else str(limit)

            text.append(f"➖" * 15)
            text.append(f"{status} **{code}** ({value_str})")
            text.append(f"Использовано: {uses_count}/{limit_str}")

            kb_builder.row(InlineKeyboardButton(text=f"⚙️ Упр. {code}", callback_data=f"coupon_view:{c_id}"))

    kb_builder.row(InlineKeyboardButton(text="◀️ Назад в Админ-панель", callback_data="back_admin"))

    await call.message.edit_text("\n".join(text), reply_markup=kb_builder.as_markup(), parse_mode="MarkdownV2")

@dp.callback_query(lambda c: c.data and c.data.startswith("coupon_view:"))
async def coupon_view_cb(call: types.CallbackQuery):
    """Показывает детали купона и кнопки управления."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Просмотр купона...")
    
    try:
        coupon_id = int(call.data.split(":")[1])
    except ValueError:
        return await call.answer("Некорректный ID купона.", show_alert=True)
        
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id, code, type, value, uses_limit, min_amount, is_active, created_at FROM coupons WHERE id = ?", (coupon_id,))
        coupon_data = await cur.fetchone()
        
    if not coupon_data:
        return await call.message.edit_text("Купон не найден.", reply_markup=admin_coupons_kb())

    c_id, code, c_type, value, limit, min_amount, is_active, created_at = coupon_data
    
    uses_count = await get_coupon_use_count(c_id)
    
    status_str = "🟢 Активен" if is_active else "🔴 Неактивен"
    c_type_str = "Процент" if c_type == 'percent' else "Фикс. сумма"
    value_str = f"{value:.2f}%" if c_type == 'percent' else f"{value:,.2f} ₽"
    limit_str = "Безлимитно" if limit == 0 else f"{limit}"
    min_amount_str = "Нет" if min_amount == 0 else f"{min_amount:,.0f} R"

    text = (
        f"🎫 **Купон: {code}** (ID: #{c_id})\n"
        f"───────────────────────────\n"
        f"**Статус:** {status_str}\n"
        f"**Тип:** {c_type_str}\n"
        f"**Скидка:** {value_str}\n"
        f"**Использований:** {uses_count} / {limit_str}\n"
        f"**Мин. Robux:** {min_amount_str}\n"
        f"**Создан:** {format_date(created_at)}"
    )

    kb = InlineKeyboardBuilder()
    
    # Кнопка переключения активности
    new_status = 0 if is_active else 1
    toggle_text = "🔴 Деактивировать" if is_active else "🟢 Активировать"
    kb.row(InlineKeyboardButton(text=toggle_text, callback_data=f"coupon_toggle:{c_id}:{new_status}"))
    
    # Кнопка удаления
    kb.row(InlineKeyboardButton(text="🗑️ Удалить купон", callback_data=f"coupon_delete:{c_id}"))

    kb.row(InlineKeyboardButton(text="◀️ Назад к списку", callback_data="coupon_list"))

    await call.message.edit_text(text, reply_markup=kb.as_markup(), parse_mode="MarkdownV2")

@dp.callback_query(lambda c: c.data and c.data.startswith("coupon_toggle:"))
async def coupon_toggle_cb(call: types.CallbackQuery):
    """Переключает статус активности купона."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Изменение статуса...")
    
    try:
        _, c_id_str, new_status_str = call.data.split(":")
        coupon_id = int(c_id_str)
        new_status = int(new_status_str)
    except ValueError:
        return await call.answer("Некорректные данные в callback.", show_alert=True)
        
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE coupons SET is_active = ? WHERE id = ?", (new_status, coupon_id))
        await db.commit()
    await log_event(call.from_user.id, "COUPON_TOGGLE", f"ID: {coupon_id}, Status: {new_status}")
    
    # Обновляем сообщение (вызываем coupon_view_cb для повторного отображения)
    call.data = f"coupon_view:{coupon_id}"
    await coupon_view_cb(call)

@dp.callback_query(lambda c: c.data and c.data.startswith("coupon_delete:"))
async def coupon_delete_cb(call: types.CallbackQuery):
    """Удаляет купон."""
    if not is_admin(call.from_user.id): return await call.answer("Доступ запрещён.", show_alert=True)
    await call.answer("Удаление купона...")
    
    coupon_id = int(call.data.split(":")[1])
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM coupons WHERE id = ?", (coupon_id,))
        await db.execute("DELETE FROM coupon_uses WHERE coupon_id = ?", (coupon_id,))
        await db.commit()
    
    await log_event(call.from_user.id, "COUPON_DELETE", f"ID: {coupon_id}")
    await call.message.edit_text("✅ Купон успешно удален.", reply_markup=admin_coupons_kb())


# --- User Coupon Activation ---
@dp.callback_query(F.data == "user_coupon_activate")
async def user_coupon_activate_start(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    await state.clear()
    
    user_data = await get_user_data(call.from_user.id)
    active_coupon_id = user_data[4]

    if active_coupon_id:
        # Получаем код текущего активного купона
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "SELECT code FROM coupons WHERE id = ?",
                (active_coupon_id,)
            )
            row = await cur.fetchone()
            coupon_code = row[0] if row else "???"

        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Отменить активный купон", callback_data="user_coupon_deactivate")],
            [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_buy")]
        ])

        return await call.message.edit_text(
            f"🔔 **У вас уже активирован купон:** `{coupon_code}`\n"
            "Вы можете его отменить, чтобы активировать новый.",
            reply_markup=kb,
            parse_mode="MarkdownV2"
        )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]]
    )
    await call.message.edit_text(
        "Введите **КОД КУПОНА** для активации:",
        reply_markup=kb
    )
    await state.set_state(UserCouponStates.enter_code)

async def get_latest_transactions(user_id: int, limit: int = 10) -> list[tuple]:
    """Получает последние транзакции/события пользователя из таблицы logs."""
    async with aiosqlite.connect(DB_PATH) as db:
        # ИСПРАВЛЕНО: 'created_at' заменено на 'timestamp', так как в таблице logs колонка называется timestamp
        query = """
            SELECT event_type, details, timestamp 
            FROM logs 
            WHERE user_id = ? 
            ORDER BY timestamp DESC 
            LIMIT ?
        """
        cursor = await db.execute(query, (user_id, limit))
        return await cursor.fetchall()
    
@dp.callback_query(F.data == "user_coupon_deactivate")
async def user_coupon_deactivate_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer("Купон деактивирован.")
    await set_user_active_coupon(call.from_user.id, None)
    await call.message.edit_text("✅ Активный купон отменен.", reply_markup=buy_menu_kb())
    await state.clear()

@dp.message(UserCouponStates.enter_code)
async def user_coupon_enter_code(message: types.Message, state: FSMContext):
    uid = message.from_user.id
    coupon_code = message.text.strip().upper()
    coupon_data = await get_coupon(coupon_code)

    if not coupon_data:
        return await message.reply("❌ Купон не найден или недействителен.")
        
    c_id, code, c_type, value, limit, min_amount, is_active = coupon_data
    
    if not is_active:
        return await message.reply("❌ Купон неактивен.")
        
    uses_count = await get_coupon_use_count(c_id)
    if limit > 0 and uses_count >= limit:
        return await message.reply("❌ Купон использован максимальное количество раз.")

    if await has_user_used_coupon(uid, c_id):
        return await message.reply("❌ Вы уже использовали этот купон.")
        
    # Купон валиден, активируем
    await set_user_active_coupon(uid, c_id)
    await log_event(message.from_user.id, "COUPON_ACTIVATE", f"Code: {coupon_code}, Min_amount: {min_amount}")

    discount_str = f"{value:.2f} ₽" if c_type == 'fixed' else f"{value:.2f}%"
    min_str = f" (Мин. {min_amount:,.0f} Robux)" if min_amount > 0 else ""
    
    await message.reply(
        f"✅ Купон **{coupon_code}** активирован!\n"
        f"Скидка: **{discount_str}**{min_str}\n"
        "Теперь вы можете перейти к покупке.",
        reply_markup=buy_menu_kb(),
        parse_mode="MarkdownV2"
    )
    await state.clear() # Сбрасываем FSM

# --- Sell Flow (Ad Management) ---
@dp.callback_query(F.data == "sell_create_ad")
async def sell_create_ad_cb(call: types.CallbackQuery, state: FSMContext):
    """Начинает процесс создания объявления."""
    await call.answer()
    await state.clear()
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]])
    await call.message.edit_text(
        "Введите **заголовок/название** объявления (например, 'Продажа через фанпэй'):", 
        reply_markup=kb, 
        parse_mode="MarkdownV2"
    )
    await state.set_state(CreateAdStates.title)

@dp.message(CreateAdStates.title)
async def sell_ad_title(message: types.Message, state: FSMContext):
    await state.update_data(title=message.text.strip())
    await message.reply("Введите **курс** (цену) за 1 Robux в рублях (например, `0.55`):")
    await state.set_state(CreateAdStates.rate)

@dp.message(CreateAdStates.rate)
async def sell_ad_rate(message: types.Message, state: FSMContext):
    try:
        rate = float(message.text.replace(',', '.').strip())
        if rate <= 0: raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите положительное число.")
        
    await state.update_data(rate=rate)
    await message.reply("Введите **минимальное количество Robux** для покупки в вашем объявлении (например, `1000`):")
    await state.set_state(CreateAdStates.min_amount)

@dp.message(CreateAdStates.min_amount)
async def sell_ad_min_amount(message: types.Message, state: FSMContext):
    try:
        min_amount = int(message.text.strip())
        if min_amount < 0: raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите целое число (0 или больше).")
        
    await state.update_data(min_amount=min_amount)
    await message.reply("Введите **максимальное количество Robux** для покупки в вашем объявлении (например, `50000`):")
    await state.set_state(CreateAdStates.max_amount)

@dp.message(CreateAdStates.max_amount)
async def sell_ad_max_amount(message: types.Message, state: FSMContext):
    try:
        max_amount = int(message.text.strip())
        if max_amount < 0: raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите целое число (0 или больше).")

    data = await state.get_data()
    min_amount = data['min_amount']
    if max_amount > 0 and max_amount < min_amount:
        return await message.reply(f"Максимальное количество Robux не может быть меньше минимального ({min_amount:,.0f} R).")
        
    await state.update_data(max_amount=max_amount)
    await message.reply("Введите **доступные методы оплаты** (например, 'Сбербанк, Тинькофф, Qiwi'):")
    await state.set_state(CreateAdStates.payment_methods)

@dp.message(CreateAdStates.payment_methods)
async def sell_ad_payment_methods(message: types.Message, state: FSMContext):
    await state.update_data(payment_methods=message.text.strip())
    await message.reply("Введите **дополнительное описание** объявления (условия, контакты, время выдачи):")
    await state.set_state(CreateAdStates.description)

@dp.message(CreateAdStates.description)
async def sell_ad_description(message: types.Message, state: FSMContext):
    await state.update_data(description=message.text.strip())
    data = await state.get_data()
    
    text = (
        "**Подтвердите создание объявления:**\n"
        "───────────────────────────\n"
        f"**Заголовок:** {data['title']}\n"
        f"**Курс:** {data['rate']:.2f} ₽ / 1 Robux\n"
        f"**Мин. Robux:** {data['min_amount']:,.0f} R\n"
        f"**Макс. Robux:** {data['max_amount']:,.0f} R\n"
        f"**Методы оплаты:** {data['payment_methods']}\n"
        f"**Описание:** {data['description']}"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Опубликовать", callback_data="ad_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="back_main")]
    ])
    
    await message.reply(text, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(CreateAdStates.confirm)

@dp.callback_query(F.data == "ad_confirm", CreateAdStates.confirm)
async def sell_ad_confirm_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer("Публикация объявления...")
    uid = call.from_user.id
    data = await state.get_data()
    
    ad_id = await create_ad(
        user_id=uid,
        title=data['title'],
        rate=data['rate'],
        min_amount=data['min_amount'],
        max_amount=data['max_amount'],
        methods=data['payment_methods'],
        description=data['description']
    )
    
    await call.message.edit_text(
        f"✅ Объявление **#{ad_id}** успешно опубликовано!", 
        reply_markup=sell_menu_kb(), 
        parse_mode="MarkdownV2"
    )
    await log_event(uid, "AD_CREATE", f"Ad ID: {ad_id}, Rate: {data['rate']}")
    await state.clear()

@dp.callback_query(F.data == "sell_my_ads")
async def sell_my_ads_cb(call: types.CallbackQuery):
    """Показывает список объявлений пользователя и кнопки управления."""
    await call.answer("Загрузка ваших объявлений...")
    uid = call.from_user.id
    ads = await get_ads_by_user(uid)
    
    if not ads:
        return await call.message.edit_text("У вас нет созданных объявлений.", reply_markup=sell_menu_kb())
        
    text = ["**📋 Ваши объявления:**\n"]
    kb_builder = InlineKeyboardBuilder()
    
    for ad_data in ads:
        ad_id, user_id, title, rate, min_amount, max_amount, methods, active, desc = ad_data
        status = "🟢 АКТИВНО" if active else "🔴 НЕАКТИВНО"
        
        text.append(f"➖" * 15 + f"\n**#{ad_id}** | {status} | {rate:.2f} ₽/Robux")
        
        # Кнопки для управления
        action_btn_text = "🔴 Деактивировать" if active else "🟢 Активировать"
        new_status = 0 if active else 1
        
        kb_builder.row(
            InlineKeyboardButton(text=action_btn_text, callback_data=f"ad_toggle:{ad_id}:{new_status}"),
            InlineKeyboardButton(text="🗑️ Удалить", callback_data=f"ad_delete:{ad_id}")
        )
        
    kb_builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sell"))

    await call.message.edit_text("\n".join(text), reply_markup=kb_builder.as_markup(), parse_mode="MarkdownV2")

@dp.callback_query(lambda c: c.data and c.data.startswith("ad_toggle:"))
async def ad_toggle_cb(call: types.CallbackQuery):
    """Переключает статус активности объявления."""
    await call.answer()
    uid = call.from_user.id
    try:
        _, ad_id_str, new_status_str = call.data.split(":")
        ad_id = int(ad_id_str)
        new_status = int(new_status_str)
    except ValueError:
        return await call.answer("Некорректные данные в callback.", show_alert=True)
        
    ad_data = await get_ad_data(ad_id)
    if not ad_data or ad_data[1] != uid:
        return await call.answer("Объявление не найдено или принадлежит другому пользователю.", show_alert=True)

    await toggle_ad_active(ad_id, new_status)
    await log_event(uid, "AD_TOGGLE", f"Ad ID: {ad_id}, New status: {new_status}")
    
    # Обновляем список объявлений
    await sell_my_ads_cb(call)

@dp.callback_query(lambda c: c.data and c.data.startswith("ad_delete:"))
async def ad_delete_cb(call: types.CallbackQuery):
    """Удаляет объявление."""
    await call.answer()
    uid = call.from_user.id
    ad_id = int(call.data.split(":")[1])
    
    ad_data = await get_ad_data(ad_id)
    if not ad_data or ad_data[1] != uid:
        return await call.answer("Объявление не найдено или принадлежит другому пользователю.", show_alert=True)

    # Здесь должна быть логика удаления из DB, но мы просто деактивируем для безопасности
    # Реализация удаления:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM ads WHERE id = ?", (ad_id,))
        await db.commit()
        
    await log_event(uid, "AD_DELETE", f"Ad ID: {ad_id}")
    
    await call.answer(f"Объявление #{ad_id} удалено.")
    # Обновляем список объявлений
    await sell_my_ads_cb(call)

@dp.callback_query(F.data == "sell_history")
async def sell_history_cb(call: types.CallbackQuery):
    """Показывает историю продаж (завершенных сделок) продавца."""
    await call.answer("Загрузка истории продаж...")
    uid = call.from_user.id
    
    deals = await get_deals_by_user(uid, is_seller=True, limit=10)
    
    text = ["**📜 Ваша история продаж (P2P):**\n"]
    if not deals:
        text.append("У вас пока нет продаж.")
    else:
        for d_id, amount, rub_amount, status, created_at, buyer_id, seller_id in deals:
            # Показываем только завершенные
            if status == 'completed':
                text.append(f"➖" * 15)
                text.append(f"**Сделка #{d_id}** (от {format_date(created_at)})")
                text.append(f"Продано: **{amount:,.0f} R** | Заработок: **{rub_amount:,.2f} ₽**")
                text.append(f"Покупатель: [User {buyer_id}](tg://user?id={buyer_id})")

    await call.message.edit_text("\n".join(text), reply_markup=sell_menu_kb(), parse_mode="MarkdownV2")

@dp.callback_query(F.data == "sell_profile")
async def sell_profile_cb(call: types.CallbackQuery):
    """Показывает анкету продавца."""
    await call.answer("Загрузка анкеты...")
    uid = call.from_user.id
    
    user_data = await get_user_data(uid)
    username = user_data[0]
    
    avg_rating, review_count = await get_user_rating_avg(uid)
    total_sales_count, total_rub_earned = await get_user_sales_stats(uid)
    
    rating_str = "Нет оценок"
    if review_count > 0:
        rating_str = f"**{avg_rating:.1f}** ⭐"
    
    text = (
        f"👤 **Ваша анкета продавца**\n"
        f"───────────────────────────\n"
        f"Username: @{username}\n"
        f"ID: `{uid}`\n"
        f"⭐ Рейтинг: {rating_str} из 5\n"
        f"📝 Всего отзывов: **{review_count}**\n"
        f"📦 Всего завершенных продаж: **{total_sales_count}**\n"
        f"💰 Общий заработок (RUB): **{total_rub_earned:,.2f} ₽**\n"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⭐ Посмотреть отзывы", callback_data="sell_reviews")],
        [InlineKeyboardButton(text="📜 История продаж", callback_data="sell_history")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="menu_sell")]
    ])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")

@dp.callback_query(F.data == "sell_reviews")
async def sell_reviews_cb(call: types.CallbackQuery):
    """Показывает последние отзывы для продавца."""
    await call.answer("Загрузка отзывов...")
    target_id = call.from_user.id
    
    reviews = await get_reviews_for_user(target_id)
    avg_rating, review_count = await get_user_rating_avg(target_id)
    
    text = [
        f"📝 **Отзывы о вас**\n"
        f"Рейтинг: **{avg_rating:.1f}** ⭐ (Всего: **{review_count}**)\n"
        "───────────────────────────\n"
    ]
    
    if not reviews:
        text.append("Отзывов пока нет.")
    else:
        for reviewer_id, rating, comment, created_at in reviews:
            rating_str = "⭐" * rating
            text.append(f"**От: [User {reviewer_id}](tg://user?id={reviewer_id})** ({format_date(created_at)})")
            text.append(f"Оценка: {rating_str}")
            text.append(f"Комментарий: *{escape_markdown_v2(comment)}*\n")

    await call.message.edit_text("\n".join(text), reply_markup=sell_menu_kb(), parse_mode="MarkdownV2")


# robloxxnadfix.py (предположительно около строки 2290)

@dp.callback_query(F.data == "buy_list_ads")
async def buy_list_ads_cb(call: types.CallbackQuery):  # ОПРЕДЕЛЯЕМ call и async
    """Показывает список активных объявлений."""
    
    # ОПРЕДЕЛЯЕМ ВСЕ ПЕРЕМЕННЫЕ В НАЧАЛЕ ФУНКЦИИ
    await call.answer("Загрузка объявлений...") 
    
    uid = call.from_user.id                 # ОПРЕДЕЛЯЕМ uid
    ads = await get_active_ads()           # ОПРЕДЕЛЯЕМ ads (await требует async def!)
    
    if not ads:
        # Проверяем на пустой список и выходим, если объявлений нет
        return await call.message.edit_text(
            "Активных объявлений о продаже Robux нет.", 
            reply_markup=buy_menu_kb()
        )
        
    text = ["**🛒 Доступные объявления Robux (P2P)**\n"] # ОПРЕДЕЛЯЕМ text
    kb_builder = InlineKeyboardBuilder()                  # ОПРЕДЕЛЯЕМ kb_builder

    # # Здесь может быть логика с купонами, как в вашем исходном коде
    # user_data = await get_user_data(uid)
    # active_coupon_id = user_data[4]
    # ...
    
    # ВАШ ЦИКЛ НАЧИНАЕТСЯ ЗДЕСЬ
    for ad in ads:
        ad_id, seller_id, title, rate, min_amount, max_amount, methods, active, desc = ad
        
        # Экранирование пользовательских данных
        escaped_title = escape_markdown_v2(title)
        escaped_methods = escape_markdown_v2(methods)
        # escaped_desc = escape_markdown_v2(desc) # desc здесь не используется, но оставляем для полноты

        # Этот блок текста, который вы добавили ранее (возможно, лишний)
        # text.append(f"**{escaped_title}** | Курс: *{rate:.2f}*") 
        # text.append(f"Методы: {escaped_methods}")
        # text.append(f"Описание: {escaped_desc}")
        
        # Не показываем свои объявления
        if seller_id == uid:
            continue

        # ВАЖНО: await внутри async функции - это нормально!
        avg_rating, review_count = await get_user_rating_avg(seller_id) 
        rating_str = f"({avg_rating:.1f} ⭐)" if review_count > 0 else "(Нет оценок)"
        
        # Экранируем rating_str (для исправления ошибки TelegramBadRequest)
        escaped_rating_str = escape_markdown_v2(rating_str)
        
        text.append(f"➖" * 15)
        text.append(
            # Используем экранированные переменные
            f"**#{ad_id} - {escaped_title}**\n" 
            f"Продавец: [User {seller_id}](tg://user?id={seller_id}) {escaped_rating_str}\n" 
            f"💵 Курс: **{rate:.2f} ₽ / 1 Robux**\n"
            f"📦 Диапазон: {min_amount:,.0f} - {max_amount:,.0f} R\n"
            f"💳 Методы: {escaped_methods}"
        )
        
        kb_builder.row(InlineKeyboardButton(text=f"Купить у #{ad_id}", callback_data=f"buy_select_ad:{ad_id}"))

    # КОНЕЦ ЦИКЛА
    
    # Заключительные строки функции
    kb_builder.row(InlineKeyboardButton(text="◀️ Назад", callback_data="back_main"))

    # await call.message.edit_text - здесь все переменные определены
    await call.message.edit_text("\n".join(text), reply_markup=kb_builder.as_markup(), parse_mode="MarkdownV2")

# КОНЕЦ ФУНКЦИИ

@dp.callback_query(lambda c: c.data and c.data.startswith("buy_select_ad:"))
async def buy_select_ad_cb(call: types.CallbackQuery, state: FSMContext):
    """Начинает процесс покупки Robux через P2P сделку."""
    await call.answer("Вы выбрали объявление...")
    ad_id = int(call.data.split(":")[1])
    ad_data = await get_ad_data(ad_id)
    uid = call.from_user.id
    
    if not ad_data:
        return await call.message.edit_text("Объявление не найдено или удалено.", reply_markup=buy_menu_kb())
        
    _, seller_id, title, rate, min_amount, max_amount, methods, active, desc = ad_data
    
    if not active:
        return await call.message.edit_text("Объявление неактивно.", reply_markup=buy_menu_kb())

    if seller_id == uid:
        return await call.message.edit_text("Вы не можете создать сделку с самим собой.", reply_markup=buy_menu_kb())
        
    # Проверяем активный купон
    user_data = await get_user_data(uid)
    active_coupon_id = user_data[4]
    coupon_data = None
    if active_coupon_id:
        coupon_data = await get_coupon(active_coupon_id)
        
    await state.clear()
    await state.update_data(
        ad_id=ad_id,
        seller_id=seller_id,
        rate=rate,
        min_amount=min_amount,
        max_amount=max_amount,
        coupon_data=coupon_data # Сохраняем данные купона
    )

    coupon_msg = ""
    if coupon_data:
        _, code, c_type, value, limit, min_a, is_active = coupon_data
        discount_str = f"{value:.2f} ₽" if c_type == 'fixed' else f"{value:.2f}%"
        coupon_msg = f"\n🔔 **Активен купон:** `{code}` ({discount_str})\n"
        if min_a > 0:
            coupon_msg += f" (Мин. Robux для скидки: {min_a:,.0f} R)\n"

    text = (
        f"**Создание сделки: {title}**\n"
        f"Продавец: [User {seller_id}](tg://user?id={seller_id})\n"
        f"Курс: **{rate:.2f} ₽ / 1 Robux**\n"
        f"Диапазон: {min_amount:,.0f} - {max_amount:,.0f} R\n"
        f"Методы оплаты: {methods}\n"
        f"Описание: *{escape_markdown_v2(desc)}*{coupon_msg}"
        "───────────────────────────\n"
        "Введите **количество Robux**, которое вы хотите приобрести:"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]])
    await call.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(CreateDealStates.enter_amount)

@dp.message(CreateDealStates.enter_amount)
async def buy_enter_amount(message: types.Message, state: FSMContext):
    try:
        amount = int(message.text.strip())
        if amount <= 0: raise ValueError
    except ValueError:
        return await message.reply("Некорректное значение. Введите целое число Robux.")

    data = await state.get_data()
    rate = data['rate']
    min_amount = data['min_amount']
    max_amount = data['max_amount']
    coupon_data = data['coupon_data']

    if amount < min_amount:
        return await message.reply(f"Минимальное количество Robux для этой сделки: {min_amount:,.0f} R.")
    if max_amount > 0 and amount > max_amount:
        return await message.reply(f"Максимальное количество Robux для этой сделки: {max_amount:,.0f} R.")
        
    # Расчет суммы к оплате
    original_rub = amount * rate
    rub = original_rub
    discount = 0.0
    discount_str = "Нет (0.00 ₽)"
    coupon_id = None
    coupon_code = None

    if coupon_data:
        c_id, code, c_type, value, _, min_a, _ = coupon_data
        
        # Проверяем минимальный Robux для купона
        if min_a > 0 and amount < min_a:
            discount_str = f"❌ Не применен (мин. {min_a:,.0f} R)"
            # Купон не применился
            await state.update_data(coupon_id=None, coupon_code=None, discount=0.0)
        else:
            coupon_id = c_id
            coupon_code = code
            
            if c_type == 'percent':
                discount = rub * (value / 100.0)
                discount_str = f"{value:.2f}% ({discount:,.2f} ₽)"
            elif c_type == 'fixed':
                discount = value
                discount_str = f"{value:,.2f} ₽"

            rub = max(0.0, rub - discount) # Итоговая сумма не может быть отрицательной
            await state.update_data(coupon_id=coupon_id, coupon_code=coupon_code, discount=discount)


    # Сохраняем данные сделки
    await state.update_data(amount=amount, rub=float(rub))

    text = (
        f"**Сумма к оплате**\n"
        "───────────────────────────\n"
        f"Robux: **{amount:,.0f} R**\n"
        f"Сумма до скидки: **{original_rub:,.2f} ₽**\n"
        f"Скидка (Купон): {discount_str}\n"
        f"Итого к оплате: **{rub:,.2f} ₽**\n"
        "───────────────────────────\n"
        "Введите **ссылку на ваш профиль/аккаунт Roblox** для выдачи Robux:"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]])
    await message.reply(text, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(CreateDealStates.enter_roblox_link)

@dp.message(CreateDealStates.enter_roblox_link)
async def buy_enter_roblox_link(message: types.Message, state: FSMContext):
    """Получает ссылку Roblox и просит подтверждение."""
    roblox_link = message.text.strip()
    
    # Очень простая валидация: URL или содержит 'roblox'
    if not (roblox_link.startswith('http') or 'roblox' in roblox_link.lower()):
        return await message.reply("Не похоже на ссылку или логин Roblox. Пожалуйста, введите корректную ссылку на профиль/аккаунт.")

    await state.update_data(roblox_link=roblox_link)
    data = await state.get_data()
    
    rub = data['rub']
    amount = data['amount']
    seller_id = data['seller_id']
    coupon_code = data.get('coupon_code')
    
    text = (
        "**Подтверждение покупки Robux (P2P)**\n"
        "───────────────────────────\n"
        f"Продавец: [User {seller_id}](tg://user?id={seller_id})\n"
        f"Robux: **{amount:,.0f} R**\n"
        f"Итого к оплате: **{rub:,.2f} ₽**\n"
        f"Аккаунт Roblox: {escape_markdown_v2(roblox_link)}\n"
        f"Купон: {escape_markdown_v2(coupon_code) if coupon_code else 'Нет'}"
        "───────────────────────────\n"
        "Вы уверены, что хотите создать сделку и перейти к оплате?"
    )
    
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"✅ Оплатить {rub:,.2f} ₽", callback_data="deal_confirm_pay")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]
    ])
    
    await message.reply(text, reply_markup=kb, parse_mode="MarkdownV2")
    await state.set_state(CreateDealStates.confirm)

@dp.callback_query(F.data == "deal_confirm_pay", CreateDealStates.confirm)
async def deal_confirm_pay_cb(call: types.CallbackQuery, state: FSMContext, bot: Bot):
    """Создает сделку, генерирует платеж YooKassa и отправляет ссылку."""
    if not YOOKASSA_SHOP_ID or not YOOKASSA_SECRET_KEY or not YOOINSTALLED:
        await state.clear()
        return await call.message.edit_text("❌ Платежная система временно недоступна.", reply_markup=buy_menu_kb())
        
    await call.answer("Генерация счета для оплаты...")
    data = await state.get_data()
    await state.clear() # Очищаем FSM сразу
    
    buyer_id = call.from_user.id
    ad_id = data['ad_id']
    seller_id = data['seller_id']
    amount = data['amount']
    rub = data['rub']
    rate = data['rate']
    roblox_link = data['roblox_link']
    coupon_id = data.get('coupon_id')
    coupon_code = data.get('coupon_code')

    # Промежуточный ID сделки
    deal_id_temp = await create_deal(
        buyer_id=buyer_id, 
        seller_id=seller_id, 
        ad_id=ad_id, 
        amount=amount, 
        price=rate, 
        rub_amount=rub, 
        roblox_link=roblox_link,
        payment_id="Placeholder", # Placeholder
        coupon_id=coupon_id, 
        coupon_code=coupon_code
    )
    
    description = f"P2P Robux Deal #{deal_id_temp} - {amount} R"
    
    try:
        bot_info = await bot.get_me()
    except Exception as e:
        logger.error(f"Error getting bot info: {e}")
        # Если не удалось получить инфо о боте, удаляем сделку и выходим
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM deals WHERE id = ?", (deal_id_temp,))
            await db.commit()
        return await call.message.edit_text("⚠️ Произошла ошибка при получении данных бота. Попробуйте снова.", reply_markup=buy_menu_kb())

    try:
        payment = Payment.create({
            "amount": {
                "value": f"{rub:.2f}",
                "currency": "RUB"
            },
            "confirmation": {
                "type": "redirect",
                "return_url": f"https://t.me/{bot_info.username}?start=deal_{deal_id_temp}"
            },
            "capture": True,
            "description": description,
            "metadata": {
                "deal_id": deal_id_temp,
                "buyer_id": buyer_id,
                "type": "p2p_deal"
            }
        }, os.urandom(12).hex()) # Idempotency Key
        
        confirmation_url = payment.confirmation.confirmation_url
        payment_id = payment.id

        # Обновляем сделку фактическим payment_id и статусом
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE deals SET payment_id = ?, status = 'pending_payment' WHERE id = ?", (payment_id, deal_id_temp))
            await db.commit()
        
        text = (
            f"**Оплата сделки P2P №{deal_id_temp}**\n"
            f"Сумма: **{rub:,.2f} ₽**\n"
            "Нажмите на кнопку ниже, чтобы перейти к оплате. У вас есть 15 минут."
        )
        
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"💳 Оплатить {rub:,.2f} ₽", url=confirmation_url)],
            [InlineKeyboardButton(text="🔄 Проверить оплату", callback_data=f"deal_check_payment:{deal_id_temp}:{payment_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]
        ])
        
        await call.message.edit_text(text, reply_markup=kb, parse_mode="MarkdownV2")
        await log_event(buyer_id, "DEAL_PAYMENT_INIT", f"Deal: {deal_id_temp}, Payment ID: {payment_id}")

    except Exception as e:
        logger.error(f"YooKassa payment creation failed: {e}")
        # Удаляем сделку
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM deals WHERE id = ?", (deal_id_temp,))
            await db.commit()
        await call.message.edit_text("❌ Не удалось создать платеж. Попробуйте позже.", reply_markup=buy_menu_kb())


@dp.callback_query(lambda c: c.data and c.data.startswith("deal_check_payment:"))
async def deal_check_payment_cb(call: types.CallbackQuery, bot: Bot):
    """Повторная проверка статуса платежа через YooKassa API."""
    await call.answer("Проверка статуса платежа...")
    
    try:
        _, deal_id_str, payment_id = call.data.split(":")
        deal_id = int(deal_id_str)
    except ValueError:
        return await call.answer("Некорректные данные.", show_alert=True)
        
    deal_data = await get_deal_data(deal_id)
    if not deal_data:
        return await call.message.edit_text("Сделка не найдена. Попробуйте создать сделку снова.", reply_markup=buy_menu_kb())
        
    try:
        # Получаем статус из YooKassa
        yoo_payment = Payment.find_one(payment_id)
        
        if yoo_payment.status == 'succeeded':
            # Ручное выполнение логики webhook
            await handle_yookassa_success(deal_id, yoo_payment.json())
            await call.message.edit_text(
                f"✅ **Сделка P2P №{deal_id} оплачена!**\n"
                f"Ожидайте выдачи робуксов продавцом. Вам нужно загрузить скриншот оплаты.",
                reply_markup=deal_proof_kb(deal_id),
                parse_mode="MarkdownV2"
            )
        elif yoo_payment.status == 'pending':
            await call.answer("Платеж еще в обработке. Попробуйте через минуту.")
        else: # canceled, waiting_for_capture, etc.
            await call.message.edit_text(
                f"❌ Платеж по сделке №{deal_id} имеет статус: **{yoo_payment.status}**\n"
                "Попробуйте создать новую сделку.",
                reply_markup=buy_menu_kb(),
                parse_mode="MarkdownV2"
            )
            # Очищаем сделку (или помечаем как отмененную)
            await update_deal_status(deal_id, 'cancelled')
            
    except Exception as e:
        logger.error(f"Error checking payment status: {e}")
        await call.answer("Ошибка при проверке статуса. Попробуйте позже.", show_alert=True)

# --- Proof Upload Flow (Buyer) ---
@dp.callback_query(lambda c: c.data and c.data.startswith("deal_upload_proof:"))
async def deal_upload_proof_start_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    deal_id = int(call.data.split(":")[1])
    deal_data = await get_deal_data(deal_id)
    
    if not deal_data or deal_data[1] != call.from_user.id: # Проверка, что покупатель
        return await call.answer("Ошибка доступа.", show_alert=True)
    
    status = deal_data[8]
    if status != 'paid_waiting_proof':
        return await call.answer("Вы уже загрузили скриншот или статус сделки изменился.", show_alert=True)
        
    await state.clear()
    await state.update_data(deal_id=deal_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]])
    await call.message.edit_text(
        f"📸 **Загрузка скриншота оплаты по сделке #{deal_id}**\n\n"
        "Отправьте мне **одним сообщением** скриншот (фото) или документ, подтверждающий оплату.",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(ProofStates.waiting_for_proof)

@dp.message(ProofStates.waiting_for_proof, F.photo | F.document)
async def deal_upload_proof_process(message: types.Message, state: FSMContext, bot: Bot):
    uid = message.from_user.id
    data = await state.get_data()
    deal_id = data['deal_id']
    
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id # Берем самое большое фото
    elif message.document:
        file_id = message.document.file_id
    
    if not file_id:
        return await message.reply("Не удалось найти фото или документ. Пожалуйста, отправьте именно файл или фото.")
        
    # 1. Сохраняем file_id и меняем статус сделки
    await set_deal_proof(deal_id, file_id)
    await log_event(uid, "DEAL_PROOF_UPLOAD", f"Deal: {deal_id}, File ID: {file_id}")
    
    # 2. Уведомление покупателя
    await message.reply(
        f"✅ **Скриншот по сделке #{deal_id} загружен!**\n\n"
        "Продавец уведомлен. Ожидайте выдачи Robux.",
        reply_markup=deal_actions_buyer_kb(deal_id, 'paid_waiting_proof'),
        parse_mode="MarkdownV2"
    )
    
    # 3. Уведомление продавца
    deal_data = await get_deal_data(deal_id)
    if deal_data:
        seller_id = deal_data[2]
        roblox_link = deal_data[6]
        
        seller_msg = (
            f"🔔 **Покупатель загрузил скриншот!**\n"
            f"Сделка P2P №{deal_id} (Покупатель: [User {uid}](tg://user?id={uid}))\n"
            f"Аккаунт: {escape_markdown_v2(roblox_link)}\n\n"
            "**Ваше действие:** Проверьте оплату и выдайте Robux."
        )
        
        try:
            # Отправляем фото продавцу
            await bot.send_photo(
                chat_id=seller_id,
                photo=file_id,
                caption=seller_msg,
                reply_markup=deal_actions_seller_kb(deal_id, 'pending_proof'),
                parse_mode="MarkdownV2"
            )
        except TelegramForbiddenError:
            pass
        except Exception as e:
            logger.error(f"Error sending proof to seller {seller_id}: {e}")
            
    await state.clear()

# --- Dispute Flow (Buyer) ---
@dp.callback_query(lambda c: c.data and c.data.startswith("deal_dispute:"))
async def deal_dispute_start_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    deal_id = int(call.data.split(":")[1])
    deal_data = await get_deal_data(deal_id)
    uid = call.from_user.id
    
    if not deal_data or deal_data[1] != uid:
        return await call.answer("Ошибка доступа.", show_alert=True)
    
    status = deal_data[8]
    if status != 'paid_waiting_proof' and status != 'pending_proof':
        return await call.answer(f"Спор можно открыть только в статусе 'Оплачено' или 'Ожидает выдачи'. Текущий статус: {status.upper()}", show_alert=True)
        
    await state.clear()
    await state.update_data(deal_id=deal_id)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]])
    await call.message.edit_text(
        f"⚠️ **Открытие спора по сделке #{deal_id}**\n\n"
        "Кратко опишите причину открытия спора (например, 'Продавец не выдает Robux'):",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(DealStates.dispute)

@dp.message(DealStates.dispute)
async def deal_dispute_process(message: types.Message, state: FSMContext, bot: Bot):
    uid = message.from_user.id
    data = await state.get_data()
    deal_id = data['deal_id']
    dispute_reason = message.text.strip()
    
    # 1. Меняем статус сделки
    await set_deal_dispute(deal_id, dispute_reason)
    await log_event(uid, "DEAL_DISPUTE_OPEN", f"Deal: {deal_id}, Reason: {dispute_reason}")
    
    # 2. Уведомление покупателя
    await message.reply(
        f"✅ **Спор по сделке #{deal_id} открыт!**\n\n"
        "Ожидайте, пока администратор рассмотрит вашу ситуацию.",
        reply_markup=deal_actions_buyer_kb(deal_id, 'dispute'),
        parse_mode="MarkdownV2"
    )
    
    # 3. Уведомление продавца
    deal_data = await get_deal_data(deal_id)
    if deal_data:
        seller_id = deal_data[2]
        seller_msg = (
            f"⚠️ **Спор открыт!**\n"
            f"Сделка P2P №{deal_id} (Покупатель: [User {uid}](tg://user?id={uid}))\n"
            f"Причина: *{escape_markdown_v2(dispute_reason)}*\n\n"
            "Свяжитесь с администратором для разрешения ситуации."
        )
        try:
            await bot.send_message(seller_id, seller_msg, parse_mode="MarkdownV2", reply_markup=deal_actions_seller_kb(deal_id, 'dispute'))
        except TelegramForbiddenError:
            pass
            
    # 4. Уведомление админов
    admin_msg = (
        f"🚨 **НОВЫЙ СПОР ПО СДЕЛКЕ!**\n"
        f"Сделка: #{deal_id}\n"
        f"Покупатель: [User {uid}](tg://user?id={uid})\n"
        f"Продавец: [User {deal_data[2]}](tg://user?id={deal_data[2]})\n"
        f"Причина: {escape_markdown_v2(dispute_reason)}\n\n"
        "Перейдите в Админ-панель для разрешения."
    )
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_msg, parse_mode="MarkdownV2")
        except Exception:
            pass
            
    await state.clear()


# --- Deal Completion Flow (Seller) ---
@dp.callback_query(lambda c: c.data and c.data.startswith("deal_complete_seller:"))
async def deal_complete_seller_cb(call: types.CallbackQuery, bot: Bot):
    """Подтверждение выдачи робуксов продавцом."""
    await call.answer()
    
    try:
        _, deal_id_str = call.data.split(":")[:2]
        deal_id = int(deal_id_str)
    except ValueError:
        return await call.answer("Некорректные данные.", show_alert=True)
        
    uid = call.from_user.id
    deal_data = await get_deal_data(deal_id)
    
    if not deal_data or deal_data[2] != uid: # Проверка, что продавец
        return await call.answer("Ошибка доступа.", show_alert=True)
        
    status = deal_data[8]
    if status != 'pending_proof' and status != 'dispute':
        return await call.answer("Сделку можно завершить только после загрузки пруфа покупателем или в статусе 'Спор'.", show_alert=True)

    # 1. Обновляем статус сделки
    await update_deal_status(deal_id, 'completed')
    await log_event(uid, "DEAL_COMPLETED", f"Deal: {deal_id}, Seller confirmed")
    
    # 2. Уведомление продавца
    await call.message.edit_text(
        f"✅ **Сделка #{deal_id} успешно завершена!**\n"
        "Спасибо за работу.",
        reply_markup=deal_actions_seller_kb(deal_id, 'completed'),
        parse_mode="MarkdownV2"
    )

    # 3. Уведомление покупателя
    buyer_id = deal_data[1]
    buyer_msg = (
        f"✅ **Сделка P2P №{deal_id} завершена!**\n"
        f"Продавец подтвердил выдачу Robux.\n\n"
        "Пожалуйста, **оставьте отзыв** о продавце, нажав на кнопку ниже."
    )
    try:
        await bot.send_message(buyer_id, buyer_msg, parse_mode="MarkdownV2", reply_markup=deal_actions_buyer_kb(deal_id, 'completed'))
    except TelegramForbiddenError:
        pass
        
    # 4. Уведомление админов
    admin_msg = f"🎉 **Сделка #{deal_id} (P2P) завершена!** Продавец [User {uid}](tg://user?id={uid}) подтвердил выдачу."
    for admin in ADMIN_IDS:
        try:
            await bot.send_message(admin, admin_msg, parse_mode="MarkdownV2")
        except Exception:
            pass


# --- Review Flow (Buyer) ---
@dp.callback_query(lambda c: c.data and c.data.startswith("deal_review:"))
async def review_start_cb(call: types.CallbackQuery, state: FSMContext):
    await call.answer()
    deal_id = int(call.data.split(":")[1])
    deal_data = await get_deal_data(deal_id)
    uid = call.from_user.id
    
    if not deal_data or deal_data[1] != uid: # Проверка, что покупатель
        return await call.answer("Ошибка доступа.", show_alert=True)

    seller_id = deal_data[2]
    
    # 1. Проверяем, был ли уже отзыв
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT id FROM reviews WHERE deal_id = ?", (deal_id,))
        if await cur.fetchone():
            return await call.answer("Вы уже оставили отзыв по этой сделке.", show_alert=True)

    await state.clear()
    await state.update_data(deal_id=deal_id, target_user_id=seller_id)
    
    kb_builder = InlineKeyboardBuilder()
    for rating in range(1, 6):
        kb_builder.add(InlineKeyboardButton(text="⭐" * rating, callback_data=f"review_rating:{rating}"))
    kb_builder.adjust(5)
    
    await call.message.edit_text(
        f"**Отзыв о продавце [User {seller_id}](tg://user?id={seller_id})**\n"
        "Шаг 1/2: Поставьте оценку (1-5 звезд):",
        reply_markup=kb_builder.as_markup(),
        parse_mode="MarkdownV2"
    )
    await state.set_state(LeaveReviewStates.rating)

@dp.callback_query(lambda c: c.data and c.data.startswith("review_rating:"), LeaveReviewStates.rating)
async def review_rating_cb(call: types.CallbackQuery, state: FSMContext):
    """Получает рейтинг и просит комментарий."""
    await call.answer()
    rating = int(call.data.split(":")[1])
    await state.update_data(rating=rating)
    
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отмена", callback_data="menu_buy")]])
    await call.message.edit_text(
        f"**Отзыв о продавце**\n"
        f"Шаг 2/2: Ваша оценка: **{'⭐' * rating}**\n\n"
        "Введите **комментарий** (необязательно, но желательно):",
        reply_markup=kb,
        parse_mode="MarkdownV2"
    )
    await state.set_state(LeaveReviewStates.comment)

@dp.message(LeaveReviewStates.comment)
async def review_comment(message: types.Message, state: FSMContext, bot: Bot):
    uid = message.from_user.id
    data = await state.get_data()
    deal_id = data['deal_id']
    target_id = data['target_user_id']
    rating = data['rating']
    comment = message.text.strip()
    
    # 1. Создаем отзыв
    await create_review(uid, target_id, deal_id, rating, comment)
    await log_event(uid, "REVIEW_LEFT", f"Deal: {deal_id}, Rating: {rating}")
    
    # 2. Уведомление покупателя
    await message.reply(
        "✅ **Спасибо!** Ваш отзыв успешно оставлен.",
        reply_markup=back_main_kb(is_admin(uid)),
        parse_mode="MarkdownV2"
    )
    
    # 3. Уведомление продавца
    try:
        seller_msg = (
            f"🔔 **Новый отзыв!**\n"
            f"По сделке #{deal_id} покупатель [User {uid}](tg://user?id={uid}) оставил оценку:\n"
            f"Оценка: **{'⭐' * rating}**\n"
            f"Комментарий: *{escape_markdown_v2(comment)}*"
        )
        await bot.send_message(target_id, seller_msg, parse_mode="MarkdownV2")
    except TelegramForbiddenError:
        pass
        
    await state.clear()


# --- Фоновый мониторинг сделок (Placeholder) ---
async def deals_monitoring_loop():
    """Фоновый цикл для мониторинга сделок, если это требуется (сейчас только заглушка)"""
    # В этой версии мониторинг не требуется, так как статус меняется через YooKassa webhook
    # или вручную администратором/продавцом.
    
    # Для демонстрации работы фона
    while True:
        await asyncio.sleep(3600) # Ждем 1 час
        # Тут можно добавить логику проверки просроченных сделок, но пока не нужно.


async def main():
    """Основная функция запуска бота и фоновых задач."""
    
    # НОВОЕ: Получаем имя пользователя бота внутри главного асинхронного контекста
    try:
        bot_info = await bot.get_me()
        os.environ['BOT_USERNAME'] = bot_info.username
    except Exception as e:
        logger.error(f"Could not fetch bot username: {e}")
        
    await init_db()
    
    if YOOKASSA_SHOP_ID and YOOKASSA_SECRET_KEY and YOOINSTALLED:
        await setup_yookassa_webhook()

    # Запуск фонового мониторинга сделок
    asyncio.create_task(deals_monitoring_loop())
    
    # Запуск Webhook-сервера, если указан WEBHOOK_HOST
    if WEBHOOK_HOST:
        asyncio.create_task(start_webhook_server())

    logger.info("🤖 Bot starting polling...")
    try:
        await set_bot_commands()
        # Запуск Polling, если Webhook не используется, или просто для надежности, если Webhook настроен
        await dp.start_polling(bot)
    except (KeyboardInterrupt, SystemExit):
        print("🚫 Bot stopped by user.")
    except Exception as e:
        logger.error(f"Polling error: {e}")


if __name__ == "__main__":
    try:
        # Запуск основной функции
        asyncio.run(main())
    except KeyboardInterrupt:
        print("🚫 Program interrupted.")