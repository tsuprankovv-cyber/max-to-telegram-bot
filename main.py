# -*- coding: utf-8 -*-
import os
import sys
import asyncio
import logging
import aiohttp
import json
import hashlib
from aiohttp import web
from typing import List, Dict, Optional, Any

# ===================================================================
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ (МАКСИМАЛЬНАЯ ДЕТАЛИЗАЦИЯ)
# ===================================================================
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot_debug.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ===================================================================
# 2. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '3'))

# Хранилище для защиты от дублей (хэши сообщений)
_processed_hashes = set()

logger.info("=" * 80)
logger.info("🚀 ЗАПУСК БОТА (MAX -> TELEGRAM)")
logger.info(f"📡 Источник (MAX) : {MAX_CHAN}")
logger.info(f"📥 Назначение (TG) : {TG_CHAT}")
logger.info(f"🔗 MAX API Base    : {MAX_BASE}")
logger.info(f"⏱️ Интервал опроса  : {POLL_SEC} сек")
logger.info("=" * 80)

# Проверка наличия переменных
if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ НЕ ХВАТАЕТ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ!")
    logger.critical("Проверьте настройки в Render.")
    sys.exit(1)

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ДАННЫХ)
# ===================================================================

def safe_str(val: Any) -> str:
    """
    Безопасно превращает значение в строку.
    Работает даже если MAX возвращает id как объект {"id": 123} или {"id": "123"}.
    """
    if val is None: return ""
    if isinstance(val, dict):
        # Если пришло вложение вида {"id": "123"}, достаем значение
        for k in ['id', 'value', 'text', 'content', '_id', '$oid']:
            if k in val: return str(val[k]).strip()
        return str(val)
    return str(val).strip()

def safe_list(val: Any) -> List[Dict]:
    """
    Гарантирует возврат списка словарей (вложений).
    """
    if val is None: return []
    if isinstance(val, list): 
        return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        # Ищем список внутри ключей
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

def get_msg_hash(msg: Dict) -> str:
    """
    Создает уникальный хэш сообщения.
    Нужно, чтобы не пересылать одно и то же сообщение дважды,
    даже если у него пустой ID.
    """
    raw = json.dumps(msg, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

# ===================================================================
# 4. КЛИЕНТ TELEGRAM (ОТПРАВКА)
# ===================================================================
class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.LIMIT_BYTES = 50 * 1024 * 1024  # 50 МБ
        self.LIMIT_CAPTION = 1024            # Лимит подписи

    async def init_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def send_text(self, text: str) -> bool:
        """Отправка текста с поддержкой HTML"""
        if not text: return True
        await self.init_session()
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML" 
        }
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"✅ TG Текст отправлен: '{text[:50]}...'")
                    return True
                else:
                    err = await resp.text()
                    logger.error(f"❌ TG Текст ошибка: {resp.status} | {err[:300]}")
                    return False
        except Exception as e:
            logger.error(f"❌ TG Текст исключение: {e}")
            return False

    async def send_media(self, file_bytes: bytes, media_type: str, filename: str, caption: str = "") -> bool:
        """Отправка файлов: Photo, Video, Audio, Document, Voice"""
        if len(file_bytes) > self.LIMIT_BYTES:
            logger.warning(f"⚠️ Файл '{filename}' слишком большой ({len(file_bytes)/1024/1024:.2f} МБ). Лимит TG: 50 МБ. Пропускаю.")
            return False

        await self.init_session()
        url = f"{self.base_url}/send{media_type.capitalize()}"
        
        data = aiohttp.FormData()
        data.add_field("chat_id", self.chat_id)
        data.add_field(media_type.lower(), file_bytes, filename=filename)
        
        if caption:
            safe_caption = caption[:self.LIMIT_CAPTION]
            data.add_field("caption", safe_caption)
            data.add_field("parse_mode", "HTML")

        try:
            async with self.session.post(url, data=data) as resp:
                if resp.status == 200:
                    logger.info(f"✅ TG Медиа ({media_type}) отправлено: {filename}")
                    return True
                else:
                    err = await resp.text()
                    logger.error(f"❌ TG Медиа ошибка: {resp.status} | {err[:300]}")
                    return False
        except Exception as e:
            logger.error(f"❌ TG Медиа исключение: {e}")
            return False

# ===================================================================
# 5. КЛИЕНТ MAX (ПОЛУЧЕНИЕ И СКАЧИВАНИЕ)
# ===================================================================
class MaxClient:
    def __init__(self, token: str, channel_id: str, base_url: str):
        self.token = token
        self.channel_id = channel_id
        self.base_url = base_url
        self.session: Optional[aiohttp.ClientSession] = None

    async def init_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def get_messages(self, limit: int = 5) -> List[Dict]:
        """Запрос списка сообщений из канала MAX"""
        await self.init_session()
        
        # Эндпоинт: /messages?chat_id=...
        url = f"{self.base_url}/messages"
        headers = {"Authorization": self.token}
        params = {"chat_id": self.channel_id, "limit": limit}
        
        logger.debug(f"🔍 Запрос к MAX: GET {url}?chat_id={self.channel_id}")
        
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    raw = await resp.json()
                    logger.debug(f"📦 RAW Ответ MAX: {json.dumps(raw, ensure_ascii=False)[:800]}...")
                    return safe_list(raw)
                else:
                    err = await resp.text()
                    logger.error(f"❌ MAX API Error: {resp.status} | {err[:200]}")
                    return []
        except Exception as e:
            logger.error(f"❌ Ошибка соединения с MAX: {e}")
            return []

    async def download_file(self, file_token: str) -> Optional[bytes]:
        """Скачивание файла по токену"""
        await self.init_session()
        
        # Эндпоинт: /files/{token}/download
        url = f"{self.base_url}/files/{file_token}/download"
        headers = {"Authorization": self.token}
        
        logger.debug(f"📥 Скачивание файла: {url}")
        
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.read()
                else:
                    logger.warning(f"⚠️ Не удалось скачать файл: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания файла: {e}")
            return None

# ===================================================================
# 6. ГЛАВНАЯ ЛОГИКА (ОБРАБОТКА СООБЩЕНИЙ) — ИСПРАВЛЕННАЯ ДЛЯ MAX STRUCTURE
# ===================================================================
tg_client = TelegramClient(TG_TOKEN, TG_CHAT)
max_client = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def process_message(msg: Dict):
    """Обработка одного сообщения из MAX и пересылка в TG"""
    
    # 1. Логирование сырого сообщения (для отладки)
    logger.debug(f"🔍 Обработка сообщения: {json.dumps(msg, ensure_ascii=False)[:500]}")
    
    # 2. Безопасное извлечение полей — С УЧЁТОМ ВЛОЖЕННОСТИ В "body"
    
    # 🔹 ID: MAX кладёт его в body.mid
    msg_id = safe_str(
        msg.get("id") or msg.get("message_id") or msg.get("_id") or 
        msg.get("msgId") or msg.get("uid") or msg.get("messageId") or
        (msg.get("body", {}).get("mid") if isinstance(msg.get("body"), dict) else None)
    )
    
    # 🔹 Текст: MAX кладёт его в body.text
    text = safe_str(
        msg.get("text") or msg.get("content") or msg.get("body") or 
        msg.get("message") or 
        (msg.get("body", {}).get("text") if isinstance(msg.get("body"), dict) else None) or
        (msg.get("payload", {}).get("text") if isinstance(msg.get("payload"), dict) else None)
    )
    
    # 🔹 Вложения
    attachments = safe_list(
        msg.get("attachments") or msg.get("files") or msg.get("media") or 
        (msg.get("payload", {}).get("attachments") if isinstance(msg.get("payload"), dict) else []) or
        (msg.get("body", {}).get("attachments") if isinstance(msg.get("body"), dict) else [])
    )
    
    # 3. Генерация хэша для защиты от дублей
    h = get_msg_hash(msg)
    logger.info(f"🆕 Сообщение | ID:'{msg_id}' | Хэш:'{h}' | Текст:{len(text)}симв | Файлов:{len(attachments)}")
    
    # 4. Проверка: было ли уже обработано?
    if h in _processed_hashes:
        logger.debug(f"⏭ Сообщение с хэшем {h} уже обработано, пропускаю.")
        return
    _processed_hashes.add(h)
    
    # Очистка памяти (храним не более 1000 хэшей)
    if len(_processed_hashes) > 1000:
        _processed_hashes.clear()
        logger.debug("🧹 Очищен кэш хэшей")

    # 5. Пропуск сообщений с пустым ID (служебные)
    if not msg_id:
        logger.debug("⏭ Пропуск: пустой ID (вероятно, служебное сообщение)")
        return

    # 6. Отправка текста
    if text:
        logger.info(f"📤 Отправляю текст: '{text[:100]}...'")
        await tg_client.send_text(text)

    # 7. Отправка вложений
    for att in attachments:
        if not isinstance(att, dict): continue
        
        # Определяем тип файла
        att_type = safe_str(att.get("type") or att.get("media_type") or "file").lower()
        
        # Ищем токен файла
        token = safe_str(att.get("token") or att.get("file_token") or att.get("id"))
        
        # Ищем имя файла
        filename = safe_str(att.get("name") or att.get("filename") or att.get("file_name") or "file.dat")
        
        if not token:
            logger.warning(f"⚠️ Вложение без токена: {att}")
            continue

        logger.info(f"📥 Скачиваю файл: {filename} (тип: {att_type})")
        file_bytes = await max_client.download_file(token)
        
        if not file_bytes:
            logger.error(f"❌ Не удалось скачать файл: {filename}")
            continue

        # Маппинг типов MAX -> Telegram
        tg_type = "Document" # По умолчанию
        
        if att_type in ["image", "photo"]:
            tg_type = "Photo"
        elif att_type == "video":
            tg_type = "Video"
        elif att_type == "audio":
            tg_type = "Audio"
        elif att_type == "voice":
            tg_type = "Voice"
        elif att_type == "file":
            # Если тип "file", смотрим на расширение
            ext = filename.split('.')[-1].lower() if '.' in filename else ''
            if ext in ['mp4', 'mov', 'avi', 'mkv', 'webm']:
                tg_type = "Video"
            elif ext in ['mp3', 'wav', 'ogg', 'm4a', 'flac']:
                tg_type = "Audio"

        logger.info(f"📤 Отправляю файл {filename} как {tg_type}")
        caption = text if tg_type != "Document" else ""
        await tg_client.send_media(file_bytes, tg_type, filename, caption=caption)
        
        # Пауза между файлами
        await asyncio.sleep(0.5)

    logger.info(f"✅ Сообщение {msg_id} полностью обработано")

# ===================================================================
# 7. ЦИКЛ ОПРОСА (POLLING)
# ===================================================================
async def polling_loop():
    logger.info("🔄 Запуск цикла опроса MAX...")
    
    # При старте синхронизируемся: берем последнее сообщение, чтобы не пересылать историю
    logger.info("⏳ Синхронизация (пропуск старых сообщений)...")
    init_msgs = await max_client.get_messages(limit=1)
    if init_msgs:
        h = get_msg_hash(init_msgs[0])
        _processed_hashes.add(h)
        logger.info(f"📍 Стартовый хэш зафиксирован: {h}")
    else:
        logger.warning("⚠️ Не удалось синхронизироваться, буду обрабатывать всё.")

    while True:
        try:
            # Берем 1 самое свежее сообщение
            messages = await max_client.get_messages(limit=1)
            
            if messages:
                # Берем первое (самое новое)
                latest_msg = messages[0]
                await process_message(latest_msg)
            else:
                logger.debug("📭 Новых сообщений нет")
                
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле опроса: {e}", exc_info=True)
        
        # Ждем перед следующим запросом
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 8. WEB СЕРВЕР (ДЛЯ RENDER HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    """Эндпоинт /health для проверки работы бота"""
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-forwarder",
        "processed_count": len(_processed_hashes)
    })

async def run_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("🌐 Веб-сервер запущен на порту 8080 (для UptimeRobot)")
    
    # Запускаем polling параллельно с веб-сервером
    await polling_loop()

# ===================================================================
# 9. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по сигналу...")
    except Exception as e:
        logger.exception(f"💥 Фатальная ошибка: {e}")
        sys.exit(1)
