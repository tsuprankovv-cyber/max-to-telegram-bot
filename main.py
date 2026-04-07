# -*- coding: utf-8 -*-
import os
import sys
import asyncio
import logging
import aiohttp
import json
from aiohttp import web
from typing import Optional

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
MAX_WEBHOOK_SECRET = os.getenv('MAX_WEBHOOK_SECRET', '').strip()
BASE_URL = os.getenv('RENDER_EXTERNAL_URL', 'https://max-to-telegram-bot.onrender.com')

logger.info("=" * 80)
logger.info("🚀 ЗАПУСК БОТА-ПЕРЕСЫЛЬЩИКА (MAX -> TELEGRAM)")
logger.info(f"📢 MAX Channel: {MAX_CHANNEL_ID}")
logger.info(f"👥 TG Chat: {TELEGRAM_CHAT_ID}")
logger.info(f"🔗 Webhook URL: {BASE_URL}/webhook")
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
if not MAX_WEBHOOK_SECRET:
    missing.append('MAX_WEBHOOK_SECRET')

if missing:
    logger.error("❌ ОТСУТСТВУЮТ ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ:")
    for var in missing:
        logger.error(f"   - {var}")
    raise ValueError(f"Missing: {', '.join(missing)}")

logger.info("✅ Все переменные установлены")
logger.info("=" * 80)

# ===================================================================
# СЕССИЯ AIOHTTP
# ===================================================================
session: Optional[aiohttp.ClientSession] = None

async def get_session() -> aiohttp.ClientSession:
    global session
    if session is None or session.closed:
        session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    return session

# ===================================================================
# TELEGRAM API CLIENT
# ===================================================================
class TelegramClient:
    def __init__(self, token: str):
        self.token = token
        self.api_url = f"https://api.telegram.org/bot{token}"

    async def send_message(self, chat_id: str, text: str, parse_mode: str = "HTML") -> bool:
        try:
            sess = await get_session()
            data = {
                "chat_id": chat_id,
                "text": text,
                "parse_mode": parse_mode
            }
            async with sess.post(f"{self.api_url}/sendMessage", json=data) as resp:
                if resp.status == 200:
                    logger.info("✅ Сообщение отправлено в Telegram")
                    return True
                else:
                    error = await resp.text()
                    logger.error(f"❌ Telegram API error: {resp.status} - {error[:500]}")
                    return False
        except Exception as e:
            logger.error(f"❌ send_message exception: {e}")
            return False

    async def send_photo(self, chat_id: str, file_bytes: bytes, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                logger.warning(f"⚠️ Фото пропущено (>50MB): {len(file_bytes)} bytes")
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
            logger.error(f"❌ send_photo exception: {e}")
            return False

    async def send_video(self, chat_id: str, file_bytes: bytes, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                logger.warning(f"⚠️ Видео пропущено (>50MB): {len(file_bytes)} bytes")
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
            logger.error(f"❌ send_video exception: {e}")
            return False

    async def send_document(self, chat_id: str, file_bytes: bytes, filename: str, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                logger.warning(f"⚠️ Файл пропущен (>50MB): {len(file_bytes)} bytes")
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
            logger.error(f"❌ send_document exception: {e}")
            return False
            
    async def send_audio(self, chat_id: str, file_bytes: bytes, filename: str, caption: str = "") -> bool:
        try:
            if len(file_bytes) > 50 * 1024 * 1024:
                return False
            
            sess = await get_session()
            data = aiohttp.FormData()
            data.add_field("chat_id", chat_id)
            data.add_field("audio", file_bytes, filename=filename)
            if caption:
                data.add_field("caption", caption)
                data.add_field("parse_mode", "HTML")

            async with sess.post(f"{self.api_url}/sendAudio", data=data) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ send_audio exception: {e}")
            return False

# ===================================================================
# MAX API CLIENT
# ===================================================================
class MaxClient:
    def __init__(self, token: str):
        self.token = token
        self.base_url = "https://platform-api.max.ru"

    async def download_file(self, file_token: str) -> Optional[bytes]:
        try:
            sess = await get_session()
            async with sess.get(
                f"{self.base_url}/files/{file_token}/download",
                headers={"Authorization": self.token}
            ) as resp:
                if resp.status == 200:
                    return await resp.read()
                return None
        except Exception as e:
            logger.error(f"❌ Max download_file exception: {e}")
            return None

# ===================================================================
# ОБРАБОТЧИК
# ===================================================================
tg_client = TelegramClient(TELEGRAM_BOT_TOKEN)
max_client = MaxClient(MAX_TOKEN)

async def process_max_data(data: dict) -> bool:
    try:
        text = data.get("text", "")
        attachments = data.get("attachments", [])
        msg_format = data.get("format", "HTML")
        
        logger.info(f"📨 Получено сообщение от Max: {len(text)} символов, {len(attachments)} вложений")

        # 1. Отправка текста
        if text:
            await tg_client.send_message(TELEGRAM_CHAT_ID, text, parse_mode=msg_format)
        
        # 2. Отправка вложений
        for att in attachments:
            att_type = att.get("type", "")
            caption = text[:1000] if text else ""
            file_bytes = None
            filename = "file"

            # Скачивание файла
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
                logger.warning("⚠️ Не удалось скачать файл")
                continue

            # Отправка в Telegram
            if att_type == "image":
                await tg_client.send_photo(TELEGRAM_CHAT_ID, file_bytes, caption)
            elif att_type == "video":
                await tg_client.send_video(TELEGRAM_CHAT_ID, file_bytes, caption)
            elif att_type == "audio":
                await tg_client.send_audio(TELEGRAM_CHAT_ID, file_bytes, filename, caption)
            elif att_type == "file":
                await tg_client.send_document(TELEGRAM_CHAT_ID, file_bytes, filename, caption)
            else:
                await tg_client.send_document(TELEGRAM_CHAT_ID, file_bytes, filename, caption)
            
            await asyncio.sleep(0.5)

        return True
    except Exception as e:
        logger.error(f"❌ process_max_data exception: {e}")
        return False

# ===================================================================
# WEB SERVER
# ===================================================================
async def webhook_handler(request: web.Request) -> web.Response:
    try:
        # Проверка секретного ключа
        secret = request.headers.get("X-Max-Signature", "")
        if secret != MAX_WEBHOOK_SECRET:
            logger.warning(f"⚠️ Неверный секрет вебхука: {secret}")
            return web.json_response({"ok": False, "error": "Unauthorized"}, status=401)

        data = await request.json()
        logger.info(f"🔔 Webhook от Max: {json.dumps(data, ensure_ascii=False)[:200]}")

        # Проверка канала
        if str(data.get("channel_id")) != str(MAX_CHANNEL_ID):
            logger.warning(f"⚠️ Сообщение из другого канала: {data.get('channel_id')}")
            return web.json_response({"ok": False, "error": "Wrong channel"})

        success = await process_max_data(data)
        return web.json_response({"ok": success})

    except json.JSONDecodeError:
        return web.json_response({"ok": False, "error": "Invalid JSON"}, status=400)
    except Exception as e:
        logger.error(f"❌ webhook_handler exception: {e}")
        return web.json_response({"ok": False, "error": str(e)}, status=500)

async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({"status": "ok", "service": "max-to-telegram-bot"})

async def cleanup(app):
    logger.info("🧹 Закрытие сессий...")
    if session and not session.closed:
        await session.close()

def create_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    app.router.add_post('/webhook', webhook_handler)
    app.on_shutdown.append(cleanup)
    return app

# ===================================================================
# ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Запуск сервера на порту 8080...")
        web.run_app(create_app(), host='0.0.0.0', port=8080)
    except KeyboardInterrupt:
        logger.info("🛑 Остановка...")
    except Exception as e:
        logger.exception(f"❌ Критическая ошибка: {e}")
        sys.exit(1)
