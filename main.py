# -*- coding: utf-8 -*-
import os
import sys
import asyncio
import logging
import aiohttp
import json
from aiohttp import web
from typing import List, Dict, Optional, Any, Union

# ===================================================================
# 1. ЛОГИРОВАНИЕ
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

# Проверка переменных
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
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ПОЛЕЙ)
# ===================================================================
def safe_str(value: Any) -> str:
    """Безопасное преобразование в строку (даже если значение - dict)"""
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        # Пробуем извлечь строковое значение из словаря
        for key in ['id', 'value', 'text', 'content', '_id', '$oid']:
            if key in value:
                return safe_str(value[key])
        return str(value)
    return str(value).strip()

def safe_list(value: Any) -> List[Dict]:
    """Безопасное преобразование в список словарей"""
    if value is None:
        return []
    if isinstance(value, list):
        return [v for v in value if isinstance(v, dict)]
    if isinstance(value, dict):
        # Пробуем найти список внутри словаря
        for key in ['messages', 'items', 'data', 'result', 'message']:
            if key in value:
                return safe_list(value[key])
        return [value]  # Возвращаем как список из одного элемента
    return []

# ===================================================================
# 4. TELEGRAM SENDER
# ===================================================================
class TelegramSender:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.LIMIT_BYTES = 50 * 1024 * 1024
        self.LIMIT_CAPTION = 1024

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
                err = await resp.text()
                logger.error(f"❌ TG Error ({endpoint}): {resp.status} | {err[:300]}")
                return False
        except Exception as e:
            logger.error(f"❌ TG Exception: {e}")
            return False

    async def send_text(self, text: str) -> bool:
        if not text: return True
        return await self._post("sendMessage", json={
            "chat_id": self.chat_id, "text": text, "parse_mode": "HTML"
        })

    async def send_media(self, file_bytes: bytes, media_type: str, filename: str, caption: str = "") -> bool:
        if len(file_bytes) > self.LIMIT_BYTES:
            logger.warning(f"⚠️ Файл >50МБ пропущен: {filename}")
            return False
        await self.init()
        data = aiohttp.FormData()
        data.add_field("chat_id", self.chat_id)
        data.add_field(media_type.lower(), file_bytes, filename=filename)
        if caption:
            data.add_field("caption", caption[:self.LIMIT_CAPTION])
            data.add_field("parse_mode", "HTML")
        return await self._post(f"send{media_type.capitalize()}", data=data)

# ===================================================================
# 5. MAX FETCHER (POLLING)
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
        await self.init()
        url = f"{self.base}/messages"
        headers = {"Authorization": self.token}
        params = {"chat_id": self.channel_id, "limit": limit}
        
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    raw = await resp.json()
                    logger.debug(f"📦 RAW MAX: {json.dumps(raw, ensure_ascii=False)[:500]}")
                    return safe_list(raw)
                logger.error(f"❌ MAX API Error: {resp.status}")
                return []
        except Exception as e:
            logger.error(f"❌ MAX Request Error: {e}")
            return []

    async def download_file(self, file_token: str) -> Optional[bytes]:
        await self.init()
        url = f"{self.base}/files/{file_token}/download"
        headers = {"Authorization": self.token}
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.read()
                return None
        except Exception as e:
            logger.error(f"❌ Download Error: {e}")
            return None

# ===================================================================
# 6. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg = TelegramSender(TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
max_api = MaxFetcher(MAX_TOKEN, MAX_CHANNEL_ID, MAX_API_BASE)

async def process_message(msg: Dict):
    global LAST_MSG_ID
    
    logger.debug(f"🔍 RAW msg: {json.dumps(msg, ensure_ascii=False)[:500]}")
    
    # 🔹 БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ID (работает даже если ID - это dict)
    msg_id = safe_str(
        msg.get("id") or msg.get("message_id") or msg.get("_id") or 
        msg.get("msgId") or msg.get("uid") or msg.get("messageId")
    )
    
    # 🔹 БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ТЕКСТА
    text = safe_str(
        msg.get("text") or msg.get("content") or msg.get("body") or 
        msg.get("message") or 
        (msg.get("payload", {}).get("text") if isinstance(msg.get("payload"), dict) else None)
    )
    
    # 🔹 БЕЗОПАСНОЕ ИЗВЛЕЧЕНИЕ ВЛОЖЕНИЙ
    attachments = safe_list(
        msg.get("attachments") or msg.get("files") or msg.get("media") or 
        (msg.get("payload", {}).get("attachments") if isinstance(msg.get("payload"), dict) else None)
    )
    
    logger.info(f"🆕 Сообщение | ID:'{msg_id}' | Текст:{len(text)}симв | Файлов:{len(attachments)}")
    
    # 🛑 Пропускаем сообщения с пустым ID
    if not msg_id:
        logger.debug("⏭ Пропуск: пустой ID")
        return
    
    # Защита от дублей
    if msg_id == LAST_MSG_ID:
        logger.debug(f"⏭ Уже обработано: {msg_id}")
        return

    # Отправка текста
    if text:
        await tg.send_text(text)

    # Отправка вложений
    for att in attachments:
        if not isinstance(att, dict): continue
        att_type = safe_str(att.get("type") or att.get("media_type") or "file").lower()
        token = safe_str(att.get("token") or att.get("file_token") or att.get("id"))
        filename = safe_str(att.get("name") or att.get("filename") or att.get("file_name") or "file.dat")
        if not token: continue
        
        logger.info(f"📥 Скачиваю: {filename}")
        file_data = await max_api.download_file(token)
        if not file_data: continue
        
        tg_type = "Document"
        if att_type in ("image", "photo"): tg_type = "Photo"
        elif att_type == "video": tg_type = "Video"
        elif att_type == "audio": tg_type = "Audio"
        elif att_type == "voice": tg_type = "Voice"
        elif att_type == "file":
            ext = filename.split('.')[-1].lower() if '.' in filename else ''
            if ext in ('mp4', 'mov', 'avi', 'mkv', 'webm'): tg_type = "Video"
            elif ext in ('mp3', 'wav', 'ogg', 'm4a'): tg_type = "Audio"
        
        await tg.send_media(file_data, tg_type, filename, caption=text if tg_type != "Document" else "")
        await asyncio.sleep(0.5)

    LAST_MSG_ID = msg_id
    logger.info(f"✅ Сообщение {msg_id} обработано")

# ===================================================================
# 7. POLLING LOOP
# ===================================================================
async def polling_loop():
    global LAST_MSG_ID
    logger.info("🔄 Запуск цикла опроса...")
    
    # Синхронизация при старте
    init_msgs = await max_api.get_latest_messages(limit=1)
    if init_msgs:
        first = init_msgs[0]
        LAST_MSG_ID = safe_str(
            first.get("id") or first.get("message_id") or first.get("_id")
        )
        logger.info(f"📍 Стартовый ID: '{LAST_MSG_ID}'")
    
    while True:
        try:
            messages = await max_api.get_latest_messages(limit=1)
            if messages:
                await process_message(messages[0])
        except Exception as e:
            logger.error(f"❌ Ошибка в цикле: {e}")
        await asyncio.sleep(POLL_INTERVAL)

# ===================================================================
# 8. WEB SERVER (HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    return web.json_response({"status": "ok", "service": "max-to-telegram", "last_id": LAST_MSG_ID})

app = web.Application()
app.router.add_get('/health', health_handler)

# ===================================================================
# 9. ЗАПУСК
# ===================================================================
async def main():
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("🌐 Веб-сервер запущен на :8080")
    await polling_loop()

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Остановка...")
    except Exception as e:
        logger.exception(f"💥 Фатальная ошибка: {e}")
        sys.exit(1)
