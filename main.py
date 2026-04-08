# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, hashlib, mimetypes
from aiohttp import web
from typing import List, Dict, Optional, Any, Union

# ===================================================================
# 1. ЛОГИРОВАНИЕ
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
# 2. ПЕРЕМЕННЫЕ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '1'))

_processed = set()

logger.info("=" * 90)
logger.info("🚀 MAX → TG FORWARDER [ULTIMATE EDITION]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s")
logger.info("🔒 Hash: mid-only | 📎 Media: ALL types | 🧩 Albums: supported")
logger.info("=" * 90)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS! Check Render settings.")
    sys.exit(1)

# ===================================================================
# 3. HELPERS
# ===================================================================
def safe_str(val: Any) -> str:
    if val is None: return ""
    if isinstance(val, dict):
        for k in ['id', 'value', 'text', 'content', '_id', '$oid', 'mid', 'url']:
            if k in val: return str(val[k]).strip()
        return str(val)
    return str(val).strip()

def safe_list(val: Any) -> List[Dict]:
    if val is None: return []
    if isinstance(val, list): return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

def get_hash(msg: Dict) -> str:
    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    mid = body.get("mid") or msg.get("id") or msg.get("message_id") or msg.get("_id") or ""
    return hashlib.md5(str(mid).encode()).hexdigest()[:12] if mid else ""

def guess_media_type(filename: str, att_type: str) -> str:
    att_type = att_type.lower() if att_type else ""
    ext = filename.split('.')[-1].lower() if '.' in filename else ""
    if att_type in ("photo", "image", "picture"): return "photo"
    if att_type == "video": return "video"
    if att_type == "audio": return "audio"
    if att_type == "voice": return "voice"
    if att_type == "sticker": return "document"
    if ext in ("jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff"): return "photo"
    if ext in ("mp4", "mov", "avi", "mkv", "webm", "flv", "m4v"): return "video"
    if ext in ("mp3", "wav", "ogg", "m4a", "flac", "opus", "aac"): return "audio"
    if ext in ("oga", "opus"): return "voice"
    return "document"

# ===================================================================
# 4. TELEGRAM CLIENT
# ===================================================================
class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.MAX_BYTES = 50 * 1024 * 1024
        self.MAX_CAPTION = 1024

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180))

    async def _request(self, method: str, **kwargs) -> bool:
        await self.init()
        url = f"{self.base}/{method}"
        try:
            async with self.session.post(url, **kwargs) as r:
                if r.status == 200:
                    return True
                err = await r.text()
                logger.error(f"❌ TG {method} {r.status}: {err[:300]}")
                return False
        except Exception as e:
            logger.error(f"❌ TG {method} exception: {e}")
            return False

    async def send_text(self, text: str) -> bool:
        if not text: return True
        return await self._request("sendMessage", json={
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        })

    async def send_photo(self,  bytes, caption: str = "") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Photo >50MB skipped")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field("photo", data, filename="photo.jpg")
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        return await self._request("sendPhoto", data=form)

    async def send_video(self, data: bytes, caption: str = "", filename: str = "video.mp4") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Video >50MB skipped: {filename}")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field("video", data, filename=filename)
        form.add_field("supports_streaming", "true")
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        return await self._request("sendVideo", data=form)

    async def send_audio(self,  bytes, caption: str = "", filename: str = "audio.mp3") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Audio >50MB skipped: {filename}")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field("audio", data, filename=filename)
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        return await self._request("sendAudio", data=form)

    async def send_voice(self,  bytes, caption: str = "") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Voice >50MB skipped")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field("voice", data, filename="voice.ogg")
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        return await self._request("sendVoice", data=form)

    async def send_document(self,  bytes, caption: str = "", filename: str = "file.dat") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Document >50MB skipped: {filename}")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field("document", data, filename=filename)
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        return await self._request("sendDocument", data=form)

# ===================================================================
# 5. MAX CLIENT
# ===================================================================
class MaxClient:
    def __init__(self, token: str, cid: str, base: str):
        self.token = token
        self.cid = cid
        self.base = base
        self.session: Optional[aiohttp.ClientSession] = None

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def fetch(self, limit: int = 5) -> List[Dict]:
        await self.init()
        try:
            async with self.session.get(
                f"{self.base}/messages",
                headers={"Authorization": self.token},
                params={"chat_id": self.cid, "limit": limit}
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    logger.debug(f"[MAX] Raw response: {json.dumps(raw, ensure_ascii=False)[:600]}")
                    return safe_list(raw)
                logger.error(f"❌ MAX fetch HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"❌ MAX fetch exception: {e}")
            return []

    async def download(self, token: str) -> Optional[bytes]:
        await self.init()
        try:
            async with self.session.get(
                f"{self.base}/files/{token}/download",
                headers={"Authorization": self.token}
            ) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.debug(f"[MAX] Downloaded {len(data)} bytes")
                    return data
                logger.warning(f"⚠️ MAX download HTTP {r.status}")
                return None
        except Exception as e:
            logger.error(f"❌ MAX download exception: {e}")
            return None

# ===================================================================
# 6. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg = TelegramClient(TG_TOKEN, TG_CHAT)
mx = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle_message(msg: Dict):
    logger.info(f"[HANDLE] Received: {json.dumps(msg, ensure_ascii=False)[:400]}")
    
    h = get_hash(msg)
    if not h:
        logger.warning("[SKIP] Could not extract mid for hashing")
        return
    
    logger.info(f"[HASH] mid hash: {h} | Already processed: {h in _processed}")
    
    if h in _processed:
        logger.info(f"[DUPE] Skip already processed: {h}")
        return
    
    _processed.add(h)
    logger.info(f"[CACHE] Added hash: {h} (cache size: {len(_processed)})")

    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    mid = safe_str(body.get("mid") or msg.get("id") or msg.get("message_id"))
    
    text = safe_str(
        body.get("text") or 
        msg.get("text") or 
        msg.get("content") or 
        (msg.get("body") if isinstance(msg.get("body"), str) else None) or 
        msg.get("message") or
        (msg.get("payload", {}).get("text") if isinstance(msg.get("payload"), dict) else None)
    )
    
    attachments = safe_list(
        body.get("attachments") or 
        msg.get("attachments") or 
        msg.get("files") or 
        msg.get("media") or 
        (msg.get("payload", {}).get("attachments") if isinstance(msg.get("payload"), dict) else []) or
        (body.get("files") if isinstance(body, dict) else [])
    )
    
    logger.info(f"📨 MSG | mid:{mid} | hash:{h} | text:{len(text)}c | attachments:{len(attachments)}")

    if not mid:
        logger.info(f"[SKIP] Empty mid, skipping message")
        return

    if text:
        logger.info(f"[SEND-TEXT] '{text[:150]}{'...' if len(text) > 150 else ''}'")
        await tg.send_text(text)

    for i, att in enumerate(attachments):
        if not isinstance(att, dict):
            logger.warning(f"[SKIP] Attachment #{i+1} is not dict: {type(att)}")
            continue
        
        att_type = safe_str(att.get("type") or att.get("media_type") or att.get("mime_type") or "file")
        token = safe_str(att.get("token") or att.get("file_token") or att.get("id") or att.get("file_id"))
        filename = safe_str(att.get("name") or att.get("filename") or att.get("file_name") or f"file_{i+1}")
        file_size = att.get("size") or att.get("file_size")
        
        logger.info(f"[ATT #{i+1}/{len(attachments)}] type:{att_type} | token:{token[:30]}... | name:{filename} | size:{file_size}")
        
        if not token:
            logger.warning(f"[SKIP] No token in attachment #{i+1}")
            continue

        logger.info(f"[DOWNLOAD] Starting: {filename}")
        file_data = await mx.download(token)
        
        if file_data is None or len(file_data) == 0:
            logger.error(f"[SKIP] Download failed or empty: {filename}")
            continue
        
        logger.info(f"[DOWNLOADED] {filename}: {len(file_data)} bytes")

        tg_type = guess_media_type(filename, att_type)
        logger.info(f"[MEDIA-TYPE] Detected: {tg_type} (from att_type:{att_type}, ext:{filename.split('.')[-1] if '.' in filename else 'none'})")

        caption = text if text and tg_type != "document" else ""
        if caption and len(caption) > tg.MAX_CAPTION:
            caption = caption[:tg.MAX_CAPTION - 3] + "..."
            logger.warning(f"[CAPTION] Truncated to {tg.MAX_CAPTION} chars")

        logger.info(f"[SEND-{tg_type.upper()}] {filename} | caption:{bool(caption)}")
        sent = False
        
        try:
            if tg_type == "photo":
                sent = await tg.send_photo(file_data, caption)
            elif tg_type == "video":
                sent = await tg.send_video(file_data, caption, filename)
            elif tg_type == "audio":
                sent = await tg.send_audio(file_data, caption, filename)
            elif tg_type == "voice":
                sent = await tg.send_voice(file_data, caption)
            else:
                sent = await tg.send_document(file_data, caption, filename)
        except Exception as e:
            logger.error(f"❌ Send exception: {e}")
            sent = False
        
        if sent:
            logger.info(f"✅ Sent {tg_type}: {filename}")
        else:
            logger.error(f"❌ Failed to send {tg_type}: {filename}")
        
        await asyncio.sleep(0.3)

    logger.info(f"✅ DONE: {mid} (hash:{h}) | attachments processed: {len(attachments)}")

# ===================================================================
# 7. POLLING + SERVER
# ===================================================================
async def polling_loop():
    logger.info("🔄 Starting polling loop...")
    logger.info("⏳ Sync: caching recent messages to avoid duplicates...")
    await asyncio.sleep(2)
    
    init_msgs = await mx.fetch(limit=50)
    for m in init_msgs:
        h = get_hash(m)
        if h:
            _processed.add(h)
    logger.info(f"📦 Cached {len(_processed)} message hashes at startup")

    poll_count = 0
    while True:
        poll_count += 1
        logger.debug(f"[POLL] Iteration #{poll_count}")
        
        try:
            messages = await mx.fetch(limit=1)
            logger.debug(f"[POLL] Received {len(messages)} messages from MAX")
            
            if messages:
                logger.info(f"[POLL] Processing newest message...")
                await handle_message(messages[0])
            else:
                logger.debug("[POLL] No new messages")
                
        except Exception as e:
            logger.error(f"❌ Polling loop error: {e}", exc_info=True)
        
        await asyncio.sleep(POLL_SEC)

async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-forwarder",
        "cached_hashes": len(_processed),
        "poll_interval": POLL_SEC
    })

async def run_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    logger.info("🌐 Health server running on :8080 (UptimeRobot compatible)")
    await polling_loop()

if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
