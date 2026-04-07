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
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ (МАКСИМАЛЬНЫЙ УРОВЕНЬ)
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

# Хранилище для защиты от дублей
_processed_hashes = set()

logger.info("=" * 80)
logger.info("🚀 ЗАПУСК БОТА (MAX -> TELEGRAM) [DUPE-FIX ENABLED]")
logger.info(f"📡 Источник (MAX) : {MAX_CHAN}")
logger.info(f"📥 Назначение (TG) : {TG_CHAT}")
logger.info(f"🔗 MAX API Base    : {MAX_BASE}")
logger.info(f"⏱️ Интервал опроса  : {POLL_SEC} сек")
logger.info("🔒 DUPE-FIX: Hash added to cache BEFORE sending")
logger.info("=" * 80)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ НЕ ХВАТАЕТ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ!")
    sys.exit(1)

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===================================================================
def safe_str(val: Any) -> str:
    if val is None: return ""
    if isinstance(val, dict):
        for k in ['id', 'value', 'text', 'content', '_id', '$oid', 'mid']:
            if k in val: return str(val[k]).strip()
        return str(val)
    return str(val).strip()

def safe_list(val: Any) -> List[Dict]:
    if val is None: return []
    if isinstance(val, list): 
        return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

def get_msg_hash(msg: Dict) -> str:
    raw = json.dumps(msg, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(raw.encode()).hexdigest()[:12]

# ===================================================================
# 4. TELEGRAM CLIENT
# ===================================================================
class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.LIMIT_BYTES = 50 * 1024 * 1024
        self.LIMIT_CAPTION = 1024

    async def init_session(self):
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def send_text(self, text: str) -> bool:
        if not text: return True
        await self.init_session()
        url = f"{self.base_url}/sendMessage"
        payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
        try:
            async with self.session.post(url, json=payload) as resp:
                if resp.status == 200:
                    logger.info(f"✅ TG Текст: '{text[:50]}...'")
                    return True
                err = await resp.text()
                logger.error(f"❌ TG Текст: {resp.status} | {err[:300]}")
                return False
        except Exception as e:
            logger.error(f"❌ TG Текст exception: {e}")
            return False

    async def send_media(self, file_bytes: bytes, media_type: str, filename: str, caption: str = "") -> bool:
        if len(file_bytes) > self.LIMIT_BYTES:
            logger.warning(f"⚠️ Файл >50МБ пропущен: {filename} ({len(file_bytes)/1024/1024:.2f} МБ)")
            return False
        await self.init_session()
        url = f"{self.base_url}/send{media_type.capitalize()}"
        data = aiohttp.FormData()
        data.add_field("chat_id", self.chat_id)
        data.add_field(media_type.lower(), file_bytes, filename=filename)
        if caption:
            data.add_field("caption", caption[:self.LIMIT_CAPTION])
            data.add_field("parse_mode", "HTML")
        try:
            async with self.session.post(url, data=data) as resp:
                if resp.status == 200:
                    logger.info(f"✅ TG Медиа ({media_type}): {filename}")
                    return True
                err = await resp.text()
                logger.error(f"❌ TG Медиа: {resp.status} | {err[:300]}")
                return False
        except Exception as e:
            logger.error(f"❌ TG Медиа exception: {e}")
            return False

# ===================================================================
# 5. MAX CLIENT
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
        await self.init_session()
        url = f"{self.base_url}/messages"
        headers = {"Authorization": self.token}
        params = {"chat_id": self.channel_id, "limit": limit}
        try:
            async with self.session.get(url, headers=headers, params=params) as resp:
                if resp.status == 200:
                    raw = await resp.json()
                    logger.debug(f"📦 MAX raw: {json.dumps(raw, ensure_ascii=False)[:800]}")
                    return safe_list(raw)
                logger.error(f"❌ MAX API: {resp.status}")
                return []
        except Exception as e:
            logger.error(f"❌ MAX request: {e}")
            return []

    async def download_file(self, file_token: str) -> Optional[bytes]:
        await self.init_session()
        url = f"{self.base_url}/files/{file_token}/download"
        headers = {"Authorization": self.token}
        try:
            async with self.session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.read()
                return None
        except Exception as e:
            logger.error(f"❌ Download: {e}")
            return None

# ===================================================================
# 6. ОБРАБОТКА СООБЩЕНИЙ [ИСПРАВЛЕНА: DUPE-FIX]
# ===================================================================
tg_client = TelegramClient(TG_TOKEN, TG_CHAT)
max_client = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def process_message(msg: Dict):
    # [DUPE-FIX LOG] Начало обработки
    logger.info(f"[DUPE-FIX] ▶️ START process_message")
    logger.debug(f"🔍 RAW: {json.dumps(msg, ensure_ascii=False)[:500]}")
    
    # Извлечение полей
    msg_id = safe_str(
        msg.get("id") or msg.get("message_id") or msg.get("_id") or 
        msg.get("msgId") or msg.get("uid") or msg.get("messageId") or
        (msg.get("body", {}).get("mid") if isinstance(msg.get("body"), dict) else None)
    )
    
    text = safe_str(
        msg.get("text") or msg.get("content") or msg.get("body") or 
        msg.get("message") or 
        (msg.get("body", {}).get("text") if isinstance(msg.get("body"), dict) else None) or
        (msg.get("payload", {}).get("text") if isinstance(msg.get("payload"), dict) else None)
    )
    
    attachments = safe_list(
        msg.get("attachments") or msg.get("files") or msg.get("media") or 
        (msg.get("payload", {}).get("attachments") if isinstance(msg.get("payload"), dict) else []) or
        (msg.get("body", {}).get("attachments") if isinstance(msg.get("body"), dict) else [])
    )
    
    # Генерация хэша
    h = get_msg_hash(msg)
    logger.info(f"🆕 MSG | ID:'{msg_id}' | Hash:'{h}' | Text:{len(text)}c | Att:{len(attachments)}")
    
    # ===================================================================
    # 🔒 DUPE-FIX: Добавляем в кэш СРАЗУ, ДО любой отправки!
    # ===================================================================
    if h in _processed_hashes:
        logger.info(f"[DUPE-FIX] ⏭ ALREADY CACHED - Skip: {h}")
        logger.info(f"[DUPE-FIX] ✅ END process_message (skipped)")
        return
    
    # 🔒 КЛЮЧЕВОЙ ФИКС: Добавляем хэш в кэш НЕМЕДЛЕННО
    _processed_hashes.add(h)
    logger.info(f"[DUPE-FIX] 🔒 Hash ADDED to cache BEFORE sending: {h}")
    logger.info(f"[DUPE-FIX] Cache size: {len(_processed_hashes)}")
    
    # Очистка памяти
    if len(_processed_hashes) > 1000:
        _processed_hashes.clear()
        logger.debug("🧹 Cleared old hashes")
    
    # Пропуск служебных сообщений с пустым ID
    if not msg_id:
        logger.info(f"[DUPE-FIX] ⏭ Skip empty ID (hash:{h})")
        logger.info(f"[DUPE-FIX] ✅ END process_message (empty ID)")
        return
    
    # ===================================================================
    # ОТПРАВКА В TELEGRAM
    # ===================================================================
    
    # Отправка текста
    if text:
        logger.info(f"[DUPE-FIX] 📤 SENDING TEXT to TG: '{text[:100]}...' (hash:{h})")
        send_result = await tg_client.send_text(text)
        logger.info(f"[DUPE-FIX] 📤 Text send result: {'✅ OK' if send_result else '❌ FAILED'} (hash:{h})")
    
    # Отправка файлов
    for i, att in enumerate(attachments):
        if not isinstance(att, dict): continue
        att_type = safe_str(att.get("type") or att.get("media_type") or "file").lower()
        token = safe_str(att.get("token") or att.get("file_token") or att.get("id"))
        filename = safe_str(att.get("name") or att.get("filename") or "file.dat")
        if not token: continue
        
        logger.info(f"[DUPE-FIX] 📥 Downloading file #{i+1}: {filename} (hash:{h})")
        file_bytes = await max_client.download_file(token)
        if not file_bytes:
            logger.info(f"[DUPE-FIX] ⚠️ Download failed: {filename} (hash:{h})")
            continue
        
        tg_type = "Document"
        if att_type in ("image", "photo"): tg_type = "Photo"
        elif att_type == "video": tg_type = "Video"
        elif att_type == "audio": tg_type = "Audio"
        elif att_type == "voice": tg_type = "Voice"
        elif att_type == "file":
            ext = filename.split('.')[-1].lower() if '.' in filename else ''
            if ext in ('mp4', 'mov', 'avi', 'mkv', 'webm'): tg_type = "Video"
            elif ext in ('mp3', 'wav', 'ogg', 'm4a'): tg_type = "Audio"
        
        logger.info(f"[DUPE-FIX] 📤 SENDING MEDIA ({tg_type}) to TG: {filename} (hash:{h})")
        media_result = await tg_client.send_media(file_bytes, tg_type, filename, caption=text if tg_type != "Document" else "")
        logger.info(f"[DUPE-FIX] 📤 Media send result: {'✅ OK' if media_result else '❌ FAILED'} (hash:{h})")
        
        await asyncio.sleep(0.5)
    
    # ===================================================================
    # ЗАВЕРШЕНИЕ
    # ===================================================================
    logger.info(f"[DUPE-FIX] ✅ END process_message - Fully processed: {msg_id} (hash:{h})")
    logger.info(f"[DUPE-FIX] Cache size after: {len(_processed_hashes)}")

# ===================================================================
# 7. POLLING LOOP
# ===================================================================
async def polling_loop():
    logger.info("🔄 Polling started...")
    
    # Синхронизация при старте
    logger.info("⏳ Sync: caching last 10 messages...")
    init_msgs = await max_client.get_messages(limit=10)
    for msg in init_msgs:
        h = get_msg_hash(msg)
        _processed_hashes.add(h)
        logger.info(f"[DUPE-FIX] 🔒 Cached at startup: {h}")
    logger.info(f"📍 Total cached at startup: {len(_processed_hashes)}")
    
    poll_count = 0
    while True:
        poll_count += 1
        logger.debug(f"🔄 Poll iteration #{poll_count}")
        try:
            messages = await max_client.get_messages(limit=1)
            logger.debug(f"📬 Got {len(messages)} messages from MAX")
            if messages:
                logger.info(f"[DUPE-FIX] 📨 New message detected, calling process_message...")
                await process_message(messages[0])
            else:
                logger.debug("📭 No new messages")
        except Exception as e:
            logger.error(f"❌ Poll error: {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 8. WEB SERVER (HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    return web.json_response({"status": "ok", "processed": len(_processed_hashes)})

async def run_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    logger.info("🌐 Health server on :8080")
    await polling_loop()

# ===================================================================
# 9. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user")
    except Exception as e:
        logger.exception(f"💥 Fatal error: {e}")
        sys.exit(1)
