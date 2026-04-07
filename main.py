# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, hashlib
from aiohttp import web
from typing import List, Dict, Optional, Any

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
# 2. ПЕРЕМЕННЫЕ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '1'))  # ← ← ← 1 СЕКУНДА!

_processed = set()  # Кэш обработанных хэшей

logger.info("=" * 70)
logger.info("🚀 MAX → TG FORWARDER [1-SEC POLLING]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Interval: {POLL_SEC}s")
logger.info("=" * 70)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ Missing environment variables. Check Render settings.")
    sys.exit(1)

# ===================================================================
# 3. HELPERS
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
    if isinstance(val, list): return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

def get_hash(msg: Dict) -> str:
    return hashlib.md5(json.dumps(msg, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]

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
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def send_text(self, text: str) -> bool:
        if not text: return True
        await self.init()
        try:
            async with self.session.post(f"{self.base}/sendMessage", json={
                "chat_id": self.chat_id, "text": text, "parse_mode": "HTML"
            }) as r:
                if r.status == 200:
                    logger.info(f"✅ Text sent: '{text[:50]}...'")
                    return True
                logger.error(f"❌ Text error {r.status}: {(await r.text())[:200]}")
                return False
        except Exception as e:
            logger.error(f"❌ Text exception: {e}")
            return False

    async def send_media(self, data: bytes, mtype: str, fname: str, caption: str = "") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ Skipped >50MB: {fname}")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field(mtype.lower(), data, filename=fname)
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        try:
            async with self.session.post(f"{self.base}/send{mtype.capitalize()}", data=form) as r:
                if r.status == 200:
                    logger.info(f"✅ Media ({mtype}) sent: {fname}")
                    return True
                logger.error(f"❌ Media error {r.status}: {(await r.text())[:200]}")
                return False
        except Exception as e:
            logger.error(f"❌ Media exception: {e}")
            return False

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
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def fetch(self, limit: int = 5) -> List[Dict]:
        await self.init()
        try:
            async with self.session.get(f"{self.base}/messages", headers={"Authorization": self.token}, params={"chat_id": self.cid, "limit": limit}) as r:
                if r.status == 200:
                    return safe_list(await r.json())
                return []
        except Exception as e:
            logger.error(f"❌ Max fetch error: {e}")
            return []

    async def download(self, token: str) -> Optional[bytes]:
        await self.init()
        try:
            async with self.session.get(f"{self.base}/files/{token}/download", headers={"Authorization": self.token}) as r:
                return await r.read() if r.status == 200 else None
        except Exception as e:
            logger.error(f"❌ Download error: {e}")
            return None

# ===================================================================
# 6. CORE LOGIC
# ===================================================================
tg = TelegramClient(TG_TOKEN, TG_CHAT)
mx = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg: Dict):
    h = get_hash(msg)
    if h in _processed:
        return  # Already processed
    _processed.add(h)  # 🔒 LOCK IMMEDIATELY

    # Extract fields
    mid = safe_str(msg.get("id") or msg.get("message_id") or msg.get("_id") or 
                   msg.get("msgId") or msg.get("uid") or 
                   (msg.get("body", {}).get("mid") if isinstance(msg.get("body"), dict) else None))
    text = safe_str(msg.get("text") or msg.get("content") or msg.get("body") or 
                    msg.get("message") or 
                    (msg.get("body", {}).get("text") if isinstance(msg.get("body"), dict) else None) or
                    (msg.get("payload", {}).get("text") if isinstance(msg.get("payload"), dict) else None))
    atts = safe_list(msg.get("attachments") or msg.get("files") or msg.get("media") or 
                     (msg.get("payload", {}).get("attachments") if isinstance(msg.get("payload"), dict) else []) or
                     (msg.get("body", {}).get("attachments") if isinstance(msg.get("body"), dict) else []))

    logger.info(f"📨 New msg | ID:{mid} | Hash:{h} | Text:{len(text)}c | Files:{len(atts)}")

    if not mid:
        return  # Skip service messages

    # Send text
    if text:
        await tg.send_text(text)

    # Send files
    for att in atts:
        if not isinstance(att, dict): continue
        atype = safe_str(att.get("type") or att.get("media_type") or "file").lower()
        tok = safe_str(att.get("token") or att.get("file_token") or att.get("id"))
        fn = safe_str(att.get("name") or att.get("filename") or "file.dat")
        if not tok: continue

        data = await mx.download(tok)
        if not data: continue

        tm = "Document"
        if atype in ("image", "photo"): tm = "Photo"
        elif atype == "video": tm = "Video"
        elif atype == "audio": tm = "Audio"
        elif atype == "voice": tm = "Voice"
        elif atype == "file":
            ext = fn.split('.')[-1].lower() if '.' in fn else ''
            if ext in ('mp4','mov','avi','mkv','webm'): tm = "Video"
            elif ext in ('mp3','wav','ogg','m4a'): tm = "Audio"

        await tg.send_media(data, tm, fn, caption=text if tm != "Document" else "")
        await asyncio.sleep(0.3)

    logger.info(f"✅ Done: {mid} (hash:{h})")

# ===================================================================
# 7. POLLING & SERVER
# ===================================================================
async def loop():
    logger.info("⏳ Stabilizing & syncing cache...")
    await asyncio.sleep(2)  # Wait for Render networking
    for m in await mx.fetch(limit=50):
        _processed.add(get_hash(m))
    logger.info(f"📦 Cached {len(_processed)} messages on startup")

    i = 0
    while True:
        i += 1
        try:
            msgs = await mx.fetch(limit=1)
            if msgs:
                await handle(msgs[0])
        except Exception as e:
            logger.error(f"❌ Loop error: {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

async def health(req):
    return web.json_response({"status": "ok", "cached": len(_processed)})

async def run():
    app = web.Application()
    app.router.add_get('/health', health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    logger.info("🌐 Server on :8080 | UptimeRobot ready")
    await loop()

if __name__ == '__main__':
    try: asyncio.run(run())
    except KeyboardInterrupt: logger.info("🛑 Stopped")
    except Exception as e: logger.exception(f"💥 Fatal: {e}"); sys.exit(1)
