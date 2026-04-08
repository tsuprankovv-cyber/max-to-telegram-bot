# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json
from aiohttp import web
from typing import List, Dict, Optional, Any

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
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '3'))

logger.info("=" * 80)
logger.info("🚀 MAX → TG FORWARDER [FINAL FIX]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s")
logger.info("🔒 No seq check | 🎨 All markup | 📎 URL priority")
logger.info("=" * 80)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS!")
    sys.exit(1)

# ===================================================================
# 3. КОНВЕРТЕР РАЗМЕТКИ
# ===================================================================
def apply_markup(text: str, markup: List[Dict]) -> str:
    if not markup or not text:
        return text

    logger.debug(f"[MARKUP] Input: '{text[:50]}...' | items: {len(markup)}")
    logger.debug(f"[MARKUP] Raw: {json.dumps(markup, ensure_ascii=False)}")

    TAGS = {
        "strong": ("<b>", "</b>"), "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"), "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"), "pre": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"), "u": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"), "strike": ("<s>", "</s>"), "s": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }

    sorted_markup = sorted(markup, key=lambda x: int(x.get("from", 0)), reverse=True)
    result = text
    
    for m in sorted_markup:
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length

            if start < 0 or end > len(result) or length <= 0:
                continue

            open_tag, close_tag = "", ""
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
            elif mtype == "link":
                url = m.get("url") or m.get("href") or ""
                if url:
                    url = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url}">'
                    close_tag = "</a>"
            elif mtype in ("mention", "hashtag", "bot_command", "cashtag"):
                continue
            else:
                logger.debug(f"[MARKUP] Unknown: '{mtype}'")
                continue

            if open_tag:
                original = result[start:end]
                result = result[:start] + open_tag + original + close_tag + result[end:]
                logger.debug(f"[MARKUP] Applied '{mtype}' [{start}:{end}]")
                
        except Exception as e:
            logger.warning(f"[MARKUP] Error: {e}")
            continue

    logger.info(f"[MARKUP] Output: '{result[:100]}{'...' if len(result)>100 else ''}'")
    return result

# ===================================================================
# 4. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_data(msg: Dict) -> Dict:
    logger.debug(f"[PARSE] Keys: {list(msg.keys())}")
    
    link = msg.get("link")
    if isinstance(link, dict) and "message" in link:
        logger.info(f"[PARSE] 📩 Found FORWARD")
        inner = link["message"]
        return {
            "source": "link.message",
            "mid": inner.get("mid") or inner.get("id"),
            "text": inner.get("text", ""),
            "markup": inner.get("markup", []),
            "attachments": safe_list(inner.get("attachments") or inner.get("files"))
        }
    
    body = msg.get("body", {})
    if isinstance(body, dict) and ("text" in body or "attachments" in body):
        return {
            "source": "body",
            "mid": body.get("mid"),
            "text": body.get("text", ""),
            "markup": body.get("markup", []),
            "attachments": safe_list(body.get("attachments") or body.get("files"))
        }
    
    return {
        "source": "root",
        "mid": msg.get("id"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files"))
    }

def safe_list(val: Any) -> List[Dict]:
    if val is None: return []
    if isinstance(val, list): return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

# ===================================================================
# 5. TELEGRAM CLIENT
# ===================================================================
class TG:
    def __init__(self, token, chat_id):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
        self.MAX_BYTES = 50 * 1024 * 1024

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))

    async def send(self, method, **kw):
        await self.init()
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                if r.status == 200:
                    logger.info(f"[TG-RESP] {method}: ✅")
                    return True
                logger.error(f"[TG-RESP] {method}: ❌ {r.status} | {txt[:200]}")
                return False
        except Exception as e:
            logger.error(f"[TG-ERR] {method}: {e}")
            return False

    async def text(self, t):
        if not t: return True
        logger.info(f"[TG-SEND] 📝 Text: '{t[:100]}...'")
        return await self.send("sendMessage", json={"chat_id": self.chat_id, "text": t, "parse_mode": "HTML"})

    async def media(self, type_, media, caption="", filename=None, is_url=False):
        if isinstance(media, bytes) and len(media) > self.MAX_BYTES:
            logger.warning(f"[TG-SKIP] {type_} >50MB")
            return False
        
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        field_map = {"photo": "photo", "video": "video", "audio": "audio", "voice": "voice", "document": "document"}
        tg_field = field_map.get(type_, type_)
        
        if is_url:
            form.add_field(tg_field, media)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} (URL)")
        else:
            fname = filename or f"{type_}_file"
            form.add_field(tg_field, media, filename=fname)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} (FILE): {len(media)} bytes")
        
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
        
        return await self.send(f"send{type_.capitalize()}", data=form)

# ===================================================================
# 6. MAX CLIENT
# ===================================================================
class MX:
    def __init__(self, token, cid, base):
        self.token = token
        self.cid = cid
        self.base = base
        self.session = None

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))

    async def fetch(self):
        await self.init()
        try:
            params = {"chat_id": self.cid, "limit": 5}
            async with self.session.get(f"{self.base}/messages", headers={"Authorization": self.token}, params=params) as r:
                if r.status == 200:
                    raw = await r.json()
                    msgs = raw.get("messages", raw) if isinstance(raw, dict) else raw
                    return msgs if isinstance(msgs, list) else []
                logger.error(f"[MAX] HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"[MAX] Err: {e}")
            return []

    async def download(self, token):
        await self.init()
        logger.info(f"[MAX-DL] 🔽 token={token[:30]}...")
        try:
            async with self.session.get(f"{self.base}/files/{token}/download", headers={"Authorization": self.token}) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[MAX-DL] ✅ {len(data)} bytes")
                    return data
                logger.error(f"[MAX-DL] ❌ HTTP {r.status}")
                return None
        except Exception as e:
            logger.error(f"[MAX-DL] Err: {e}")
            return None

# ===================================================================
# 7. ОБРАБОТКА (БЕЗ ПРОВЕРКИ SEQ)
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg):
    logger.info(f"🔍 [RAW-MSG] {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    data = extract_data(msg)
    logger.info(f"[PARSE] {data['source']} | MID: {data['mid']} | Text: {len(data['text'])}c | Markup: {len(data['markup'])} | Att: {len(data['attachments'])}")
    
    mid = data["mid"]
    if not mid:
        logger.warning("[SKIP] No MID")
        return
    
    logger.info(f"🆕 [NEW] Processing MID: {mid}")
    
    text = apply_markup(data["text"], data["markup"]) if data["markup"] else data["text"]
    
    if text:
        logger.info(f"[SEND-TEXT] ▶️ '{text[:100]}...'")
        ok = await tg.text(text)
        logger.info(f"[RESULT] Text: {'✅' if ok else '❌'}")
        await asyncio.sleep(0.2)
    
    for i, att in enumerate(data["attachments"]):
        logger.info(f"[ATT #{i+1}] ▶️")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:400]}")
        
        if not isinstance(att, dict): continue
        
        atype = att.get("type") or att.get("media_type") or "file"
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        url = payload.get("url") or att.get("url")
        token = payload.get("token") or att.get("token") or att.get("id")
        fname = payload.get("filename") or att.get("filename") or f"file_{i+1}"
        
        logger.info(f"[ATT] Type: {atype} | URL: {bool(url)} | Token: {token[:30] if token else None}... | Name: {fname}")
        
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        tg_type = "document"
        if atype in ("image", "photo") or ext in ("jpg","jpeg","png","gif","webp"): tg_type = "photo"
        elif atype == "video" or ext in ("mp4","mov","avi","mkv"): tg_type = "video"
        elif atype == "voice" or ext in ("ogg","opus"): tg_type = "voice"
        elif atype == "audio" or ext in ("mp3","wav","m4a"): tg_type = "audio"
        
        logger.info(f"[TYPE] Detected: {tg_type}")
        
        caption = text if tg_type != "document" else ""
        sent = False
        
        if url and tg_type == "photo":
            logger.info(f"[SEND] 📤 Photo via URL")
            sent = await tg.media(tg_type, url, caption, is_url=True)
        elif token:
            logger.info(f"[SEND] 📤 Downloading...")
            file_data = await mx.download(token)
            # 🔹 ИСПРАВЛЕНИЕ: ПОЛНАЯ СТРОКА (было обрезано)
            if file_data:
                sent = await tg.media(tg_type, file_data, caption, filename=fname)
            else:
                logger.error(f"[SKIP] ❌ Download failed")
        else:
            logger.warning(f"[SKIP] No URL or Token")
        
        logger.info(f"[RESULT] {tg_type}: {'✅' if sent else '❌'}")
        await asyncio.sleep(0.3)
    
    logger.info(f"✅ [DONE] MID: {mid}")

# ===================================================================
# 8. POLLING
# ===================================================================
async def polling_loop():
    logger.info("🔄 Starting loop...")
    await asyncio.sleep(2)
    
    while True:
        try:
            msgs = await mx.fetch()
            logger.debug(f"[POLL] Got {len(msgs)} msgs")
            for msg in msgs:
                await handle(msg)
        except Exception as e:
            logger.error(f"[LOOP] {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 9. SERVER
# ===================================================================
async def health_handler(request):
    return web.json_response({"ok": True})

async def run_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    logger.info("🌐 Server on :8080")
    await polling_loop()

if __name__ == '__main__':
    try:
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 Fatal: {e}")
        sys.exit(1)
