# -*- coding: utf-8 -*-
import os
import sys
import asyncio
import logging
import aiohttp
import json
from aiohttp import web
from typing import List, Dict, Optional

# ===================================================================
# 1. ЛОГИРОВАНИЕ (ВСЁ ТОЛЬКО В ЛОГИ — НИКАКИХ ОТВЕТОВ В MAX)
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
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
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHANNEL_ID = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_API_BASE = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '3'))

LAST_MSG_ID: Optional[str] = None

logger.info("=" * 70)
logger.info("🚀 MAX -> TELEGRAM FORWARDER (POLLING MODE)")
logger.info(f"📡 MAX Channel : {MAX_CHANNEL_ID}")
logger.info(f"📥 TG Chat     : {TELEGRAM_CHAT_ID}")
logger.info(f"🔗 API Base    : {MAX_API_BASE}")
logger.info(f"⏱  Poll Interval: {POLL_INTERVAL} sec")
logger.info("📜 ОТВЕТЫ В MAX: ОТКЛЮЧЕНЫ (только логи)")
logger.info("=" * 70)

# Проверка обязательных переменных
required = {
    'TELEGRAM_BOT_TOKEN': TELEGRAM_BOT_TOKEN,
    'TELEGRAM_CHAT_ID': TELEGRAM_CHAT_ID,
    'MAX_TOKEN': MAX_TOKEN,
    'MAX_CHANNEL_ID': MAX_CHANNEL_ID
}
missing = [k for k, v in required.items() if not v]
if missing:
    logger.critical(f"❌ Отсутствуют переменные: {', '.join(missing)}")
    sys.exit(1)
logger.info("✅ Переменные окружения проверены")

# ===================================================================
# 3. TELEGRAM CLIENT (ОТПРАВКА)
# ===================================================================
class TelegramSender:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.LIMIT_BYTES = 50 * 1024 * 1024  # 50 МБ — лимит Telegram Bot API
        self.LIMIT_CAPTION = 1024             # Лимит подписи в Telegram

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def _post(self, endpoint: str, **kwargs) -> bool:
        await self.init()
        url = f"{self.base}/{endpoint}"
        try:
            async with self.session.post(url, **kwargs) as resp:
                if resp.status == 200:
                    return True
                err_text = await resp.text()
                logger.error(f"❌ TG API Error ({endpoint}): {resp.status} | {err_text[:300]}")
                return False
        except Exception as e:
            logger.error(f"❌ TG Exception: {e}")
            return False

    async def send_text(self, text: str) -> bool:
        """Отправка текста с поддержкой HTML"""
        if not text:
            return True
        return await self._post("sendMessage", json={
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML"
        })

    async def send_media(self, file_bytes: bytes, media_type: str, filename: str, caption: str = "") -> bool:
        """Отправка медиа: Photo, Video, Audio, Voice, Document"""
        if len(file_bytes) > self.LIMIT_BYTES:
            logger.warning(f"⚠️ Файл >50МБ пропущен: {filename} ({len(file_bytes)/1024/1024:.2f} МБ)")
            return False

        await self.init()
        data = aiohttp.FormData()
        data.add_field("chat_id", self.chat_id)
        data.add_field(media_type.lower(), file_bytes, filename=filename)
        
        if caption:
            safe_caption = caption[:self.LIMIT_CAPTION]
            data.add_field("caption", safe_caption)
            data.add_field("parse_mode", "HTML")

        return await self._post(f"send{media_type.capitalize()}", data=data)

# ===================================================================
# 4. MAX CLIENT (ПОЛУЧЕНИЕ СООБЩЕНИЙ — POLLING)
# ===================================================================
class MaxFetcher:
    def __init__(self, token: str, channel_id: str, base: str):
        self.token = token
        self.channel_id = channel_id
        self.base = base
        self.session: Optional[aiohttp.ClientSession] = None

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def get_latest_messages(self, limit: int = 5) -> List[Dict]:
        """Получение последних сообщений из канала MAX"""
        await self.init()
        
        # 🔹 Эндпоинт: /messages?chat_id={channel_id} (логичное расширение документации)
        url = f"{self.base}/messages"
        headers = {"Authorization": self.token}
        params = {"chat_id": self.channel_id, "limit": limit}
        
        logger.debug(f"🔍 Запрос к MAX: GET {url}?chat_id={self.channel_id}")
        
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                logger.debug(f"📡 Ответ MAX: {resp.status}")
                
                if resp.status == 200:
                    raw = await resp.json()
                    logger.debug(f"📦 Ответ: {str(raw)[:300]}...")
                    
                    # Нормализация ответа: список или объект с ключом
                    if isinstance(raw, list):
                        return raw
                    if isinstance(raw, dict):
                        for key in ['messages', 'items', 'data', 'result', 'message']:
                            if key in raw:
                                val = raw[key]
                                if isinstance(val, list):
                                    return val
                                if isinstance(val, dict):
                                    return [val]
                    return []
                    
                logger.error(f"❌ MAX API Error: {resp.status} | URL: {url}?chat_id={self.channel_id}")
                return []
                
        except Exception as e:
            logger.error(f"❌ Ошибка запроса к MAX: {e}")
            return []

    async def download_file(self, file_token: str) -> Optional[bytes]:
        """Скачивание файла из MAX по токену"""
        await self.init()
        
        # 🔹 Эндпоинт для скачивания (предположительный)
        url = f"{self.base}/files/{file_token}/download"
        headers = {"Authorization": self.token}
        
        logger.debug(f"🔍 Скачать файл: {url}")
        
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.read()
                logger.warning(f"⚠️ Не удалось скачать файл {file_token}: HTTP {resp.status}")
                return None
        except Exception as e:
            logger.error(f"❌ Ошибка скачивания файла: {e}")
            return None

# ===================================================================
# 5. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg_sender = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
max_fetcher = MaxFetcher(MAX_TOKEN, MAX_CHANNEL_ID, MAX_API_BASE)

async def process_message(msg: Dict):
    """Обработка одного сообщения из MAX и отправка в Telegram"""
    global LAST_MSG_ID
    
    msg_id = str(msg.get("id", ""))
    
    # Защита от дублей
    if msg_id == LAST_MSG_ID:
        logger.debug(f"⏭ Сообщение {msg_id} уже обработано, пропускаю")
        return

    text = msg.get("text", "")
    attachments = msg.get("attachments", [])
    
    logger.info(f"🆕 Новое сообщение из MAX | ID:{msg_id} | Текст:{len(text)}симв | Файлов:{len(attachments)}")

    # 1. Отправка текста (если есть)
    if text:
        await tg_sender.send_text(text)

    # 2. Отправка вложений
    for att in attachments:
        att_type = att.get("type", "file").lower()
        token = att.get("token")
        filename = att.get("name", "attachment.dat")
        
        if not token:
            logger.warning("⚠️ Вложение без токена, пропуск")
            continue

        logger.info(f"📥 Скачиваю файл: {filename}")
        file_data = await max_fetcher.download_file(token)
        
        if not file_data:
            logger.error(f"❌ Не удалось скачать файл: {filename}")
            continue

        # Маппинг типов MAX → типы Telegram API
        tg_type = "Document"
        if att_type in ("image", "photo"):
            tg_type = "Photo"
        elif att_type == "video":
            tg_type = "Video"
        elif att_type == "audio":
            tg_type = "Audio"
        elif att_type == "voice":
            tg_type = "Voice"
        elif att_type == "file":
            # Определяем тип по расширению
            ext = filename.split('.')[-1].lower() if '.' in filename else ''
            if ext in ('mp4', 'mov', 'avi', 'mkv', 'webm'):
                tg_type = "Video"
            elif ext in ('mp3', 'wav', 'ogg', 'm4a', 'flac'):
                tg_type = "Audio"

        # Отправка в Telegram
        await tg_sender.send_media(
            file_data, 
            tg_type, 
            filename, 
            caption=text if tg_type != "Document" else ""
        )
        
        # Пауза между файлами, чтобы не получить лимит 429
        await asyncio.sleep(0.5)

    # Обновляем ID последнего обработанного сообщения
    LAST_MSG_ID = msg_id
    logger.info(f"✅ Сообщение {msg_id} полностью обработано")

# ===================================================================
# 6. POLLING LOOP + ИНИЦИАЛИЗАЦИЯ
# ===================================================================
async def polling_loop():
    """Бесконечный цикл опроса MAX API"""
    global LAST_MSG_ID
    logger.info("🔄 Запуск цикла опроса MAX...")
    
    # Синхронизация при старте: запоминаем ID самого свежего сообщения
    logger.info("⏳ Синхронизация с MAX (пропуск старых сообщений)...")
    init_msgs = await max_fetcher.get_latest_messages(limit=1)
    
    if init_msgs:
        LAST_MSG_ID = str(init_msgs[0].get("id", ""))
        logger.info(f"📍 Стартовый ID зафиксирован: {LAST_MSG_ID}")
    else:
        logger.warning("⚠️ Не удалось получить начальный ID. Будут обработаны все новые.")

    while True:
        try:
            messages = await max_fetcher.get_latest_messages(limit=1)
            if messages:
                await process_message(messages[0])
        except Exception as e:
            logger.error(f"❌ Критическая ошибка в цикле опроса: {e}")
        
        # Ждём перед следующим запросом
        await asyncio.sleep(POLL_INTERVAL)

# ===================================================================
# 7. WEB SERVER (ЗДОРОВЬЕ ДЛЯ RENDER / UPTIMEROBOT)
# ===================================================================
async def health_handler(request):
    """Эндпоинт для проверки работоспособности"""
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-forwarder",
        "last_processed_id": LAST_MSG_ID,
        "mode": "polling_logs_only"
    })

app = web.Application()
app.router.add_get('/health', health_handler)

# ===================================================================
# 8. ГЛАВНЫЙ ЗАПУСК
# ===================================================================
async def main():
    """Точка входа: запускаем веб-сервер + polling"""
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("🌐 Веб-сервер запущен на :8080 (для UptimeRobot)")
    
    # Запускаем polling цикл
    await polling_loop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Остановка по сигналу...")
    except Exception as e:
        logger.exception(f"💥 Фатальный сбой: {e}")
        sys.exit(1)
