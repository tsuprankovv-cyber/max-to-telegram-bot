# -*- coding: utf-8 -*-
import os
import sys
import asyncio
import logging
import aiohttp
import json
from aiohttp import web
from typing import Optional, List, Dict

# ===================================================================
# НАСТРОЙКА ЛОГИРОВАНИЯ
# ===================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot_debug.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ===================================================================
# ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ===================================================================
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHANNEL_ID = os.getenv('MAX_CHANNEL_ID', '').strip()
POLL_INTERVAL = int(os.getenv('POLL_INTERVAL', '3'))

logger.info("=" * 80)
logger.info("🚀 ЗАПУСК БОТА-ПЕРЕСЫЛЬЩИКА (MAX -> TELEGRAM) [POLLING]")
logger.info(f"📢 MAX Channel: {MAX_CHANNEL_ID}")
logger.info(f"👥 TG Chat: {TELEGRAM_CHAT_ID}")
logger.info(f"⏱️ Poll Interval: {POLL_INTERVAL} sec")
logger.info("=" * 80)

# 🔹 ПРОВЕРКА ПЕРЕМЕННЫХ
missing = []
if not TELEGRAM_BOT_TOKEN:
    missing.append('TELEGRAM_BOT_TOKEN')
if not TELEGRAM_CHAT_ID:
    missing.append('TELEGRAM_CHAT_ID')
if not MAX_TOKEN:
    missing.append('MAX_TOKEN')
if not MAX_CHANNEL_ID:
    missing.append('MAX_CHANNEL_ID')

if missing:
    logger.error("❌ ОТСУТСТВУЮТ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
    for var in missing:
        logger.error(f"   - {var}")
    raise ValueError(f"Missing: {', '.join(missing)}")

logger.info("✅ Все переменные установлены")
logger.info("=" * 80)

# ===================================================================
# СЕССИЯ
# ===================================================================
session: Optional[aiohttp.ClientSession] = None
last_message_id: Optional[str] = None

async def get_session() -> aiohttp.ClientSession:
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    return session

# ===================================================================
# TELEGRAM CLIENT
# ===================================================================
class TelegramClient:
    def __init__(self, token: str):
        self.token = token
        self.api_url = f"https://api.telegram.org/bot{token}"

    async def send_message(self, chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
        try:
            sess = await get_session()
            data = {"chat_id": chat_id, "text": text, "parse_mode": parse_mode}
            async with sess.post(f"{self.api_url}/sendMessage", json=data) as resp:
                if resp.status == 200:
                    logger.info("✅ Сообщение отправлено в Telegram")
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ send_message: {e}")
            return False

    async def send_photo(self, chat_id: str, file_bytes: bytes, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                return False
            sess = await get_session()
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id)
            data.add_field("photo", file_bytes, filename="photo.jpg")
            if caption:
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")
            async with sess.post(f"{self.api_url}/sendPhoto", data=data) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ send_photo: {e}")
            return False

    async def send_video(self, chat_id: str, file_bytes: bytes, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                return False
            sess = await get_session()
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id)
            data.add_field("video", file_bytes, filename="video.mp4")
            if caption:
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")
            async with sess.post(f"{self.api_url}/sendVideo", data=data) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ send_video: {e}")
            return False

    async def send_document(self, chat_id: str, file_bytes: bytes, filename: str, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                return False
            sess = await get_session()
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id)
            data.add_field("document", file_bytes, filename=filename)
            if caption:
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")
            async with sess.post(f"{self.api_url}/sendDocument", data=data) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ send_document: {e}")
            return False

# ===================================================================
# MAX CLIENT (POLLING)
# ===================================================================
class MaxClient:
    def __init__(self, token: str, channel_id: str):
        self.token = token
        self.channel_id = channel_id
        self.base_url = "https://platform-api.max.ru"

    async def get_messages(self, limit: int = 5) -> Optional[List[Dict]]:
        try:
            sess = await get_session()
            # ⚠️ АДАПТИРУЙТЕ ЭНДПОИНТ ПОД РЕАЛЬНОЕ API MAX
            async with sess.get(
                f"{self.base_url}/channels/{self.channel_id}/messages",
                headers={"Authorization": self.token},
                params={"limit": limit}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    # ⚠️ АДАПТИРУЙТЕ ПОЛЯ ПОД ОТВЕТ API MAX
                    return data.get('messages', []) if isinstance(data, dict) else data
                return None
        except Exception as e:
            logger.error(f"❌ get_messages: {e}")
            return None

    async def download_file(self, file_token: str) -> Optional[bytes]:
        try:
            sess = await get_session()
            # ⚠️ АДАПТИРУЙТЕ ЭНДПОИНТ ПОД РЕАЛЬНОЕ API MAX
            async with sess.get(
                f"{self.base_url}/files/{file_token}/download",
                headers={"Authorization": self.token}
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
                return None
        except Exception as e:
            logger.error(f"❌ download_file: {e}")
            return None

# ===================================================================
# ОБРАБОТКА СООБЩЕНИЯ
# ===================================================================
tg_client = TelegramClient(TELEGRAM_BOT_TOKEN)
max_client = MaxClient(MAX_TOKEN, MAX_CHANNEL_ID)

async def process_message(msg: Dict) -> bool:
    try:
        text = msg.get("text", "")
        attachments = msg.get("attachments", [])
        msg_format = msg.get("format", "HTML")
        msg_id = msg.get("id", "unknown")
        
        logger.info(f"📨 Обработка сообщения #{msg_id}")

        if text:
            await tg_client.send_message(TELEGRAM_CHAT_ID, text, parse_mode=msg_format)
        
        for att in attachments:
            att_type = att.get("type", "")
            caption = text[:1000] if text else ""
            file_bytes = None
            filename = "file"

            if "token" in att:
                file_bytes = await max_client.download_file(att["token"])
                filename = att.get("name", "file")
            elif "url" in att:
                sess = await get_session()
                async with sess.get(att["url"]) as resp:
                    if resp.status == 200:
                        file_bytes = await resp.read()
                        filename = att.get("name", att["url"].split("/")[-1])
            
            if not file_bytes:
                continue

            if att_type == "image":
                await tg_client.send_photo(TELEGRAM_CHAT_ID, file_bytes, caption)
            elif att_type == "video":
                await tg_client.send_video(TELEGRAM_CHAT_ID, file_bytes, caption)
            elif att_type in ["audio", "file"]:
                await tg_client.send_document(TELEGRAM_CHAT_ID, file_bytes, filename, caption)
            
            await asyncio.sleep(0.5)

        return True
    except Exception as e:
        logger.error(f"❌ process_message: {e}")
        return False

# ===================================================================
# POLLING ЦИКЛ
# ===================================================================
async def polling_loop():
    global last_message_id
    logger.info("🔄 Запуск polling цикла...")
    
    while True:
        try:
            messages = await max_client.get_messages(limit=5)
            
            if messages and isinstance(messages, list) and len(messages) > 0:
                latest = messages[0]
                current_id = str(latest.get("id", ""))
                
                if current_id and current_id != last_message_id:
                    logger.info(f"🆕 Новое сообщение: #{current_id}")
                    await process_message(latest)
                    last_message_id = current_id
            
            await asyncio.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logger.error(f"❌ polling_loop: {e}")
            await asyncio.sleep(POLL_INTERVAL)

# ===================================================================
# HEALTH CHECK
# ===================================================================
async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-bot",
        "mode": "polling",
        "last_id": last_message_id
    })

# ===================================================================
# ОЧИСТКА
# ===================================================================
async def cleanup(app):
    logger.info("🧹 Закрытие сессий...")
    if session and not session.closed:
        await session.close()

# ===================================================================
# СОЗДАНИЕ ПРИЛОЖЕНИЯ
# ===================================================================
def create_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    app.on_shutdown.append(cleanup)
    return app

# ===================================================================
# ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Запуск polling сервера на порту 8080...")
        
        async def run():
            app = create_app()
            runner = web.AppRunner(app)
            await runner.setup()
            site = web.TCPSite(runner, '0.0.0.0', 8080)
            await site.start()
            logger.info("✅ Веб-сервер запущен")
            await polling_loop()
        
        asyncio.run(run())
        
    except KeyboardInterrupt:
        logger.info("🛑 Остановка...")
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
