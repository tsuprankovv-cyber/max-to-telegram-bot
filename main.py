# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json
from aiohttp import web
from typing import List, Dict, Optional, Any

# ===================================================================
# 1. ЛОГИРОВАНИЕ (МАКСИМУМ ДЕТАЛЕЙ)
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
# 2. ПЕРЕМЕННЫЕ (БЕЗ last_seq — УБРАЛИ ПРОВЕРКУ ДУБЛЕЙ)
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '3'))

logger.info("=" * 80)
logger.info("🚀 MAX → TG FORWARDER [FINAL - ALL FIXES]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s")
logger.info("🔒 No seq check | 🎨 All markup types | 📎 URL priority")
logger.info("=" * 80)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS!")
    sys.exit(1)

# ===================================================================
# 3. КОНВЕРТЕР РАЗМЕТКИ (ВСЕ ТИПЫ + ЛОГИРОВАНИЕ)
# ===================================================================
def apply_markup(text: str, markup: List[Dict]) -> str:
    """Конвертирует markup MAX → HTML Telegram со всеми типами"""
    if not markup or not text:
        logger.debug("[MARKUP] No markup or empty text, returning as-is")
        return text

    logger.debug(f"[MARKUP] Input text: '{text[:100]}{'...' if len(text)>100 else ''}'")
    logger.debug(f"[MARKUP] Markup items: {len(markup)}")
    logger.debug(f"[MARKUP] Raw markup: {json.dumps(markup, ensure_ascii=False)}")

    # ПОЛНЫЙ МАППИНГ ВСЕХ ТИПОВ
    TAGS = {
        "strong": ("<b>", "</b>"),
        "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"),
        "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"),
        "pre": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"),
        "u": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"),
        "strike": ("<s>", "</s>"),
        "s": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }

    # Сортируем с КОНЦА к началу — чтобы вставка тегов не сдвигала индексы
    sorted_markup = sorted(markup, key=lambda x: int(x.get("from", 0)), reverse=True)
    
    result = text
    applied_count = 0
    
    for m in sorted_markup:
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length

            if start < 0 or end > len(result) or length <= 0:
                logger.warning(f"[MARKUP] Invalid range: {start}-{end} (text_len={len(result)})")
                continue

            open_tag, close_tag = "", ""
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                applied_count += 1
                logger.debug(f"[MARKUP] Applied '{mtype}' at [{start}:{end}]")
            elif mtype == "link":
                url = m.get("url") or m.get("href") or ""
                if url:
                    url = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url}">'
                    close_tag = "</a>"
                    applied_count += 1
                    logger.debug(f"[MARKUP] Applied link at [{start}:{end}]: {url[:50]}...")
                else:
                    logger.warning(f"[MARKUP] Link type without URL")
                    continue
            elif mtype in ("mention", "hashtag", "bot_command", "cashtag"):
                logger.debug(f"[MARKUP] Skipping {mtype} (no HTML tag needed)")
                continue
            else:
                logger.warning(f"[MARKUP] Unknown type: '{mtype}' | item: {m}")
                continue

            if open_tag:
                original = result[start:end]
                result = result[:start] + open_tag + original + close_tag + result[end:]
                
        except Exception as e:
            logger.error(f"[MARKUP] Exception: {e} | markup item: {m}", exc_info=True)
            continue

    logger.info(f"[MARKUP] Applied {applied_count} tags | Output: '{result[:100]}{'...' if len(result)>100 else ''}'")
    return result

# ===================================================================
# 4. ИЗВЛЕЧЕНИЕ ДАННЫХ (ПРИОРИТЕТ LINK.MESSAGE ДЛЯ ПЕРЕСЫЛОК)
# ===================================================================
def extract_data(msg: Dict) -> Dict:
    """Извлекает text, markup, attachments — с приоритетом для пересылок"""
    logger.debug(f"[PARSE] Top-level message keys: {list(msg.keys())}")
    
    # 🔹 1. Проверяем пересылку (link.message) — ЭТО ВАЖНО ДЛЯ ФОРВАРДОВ
    link = msg.get("link")
    if isinstance(link, dict) and "message" in link:
        logger.info(f"[PARSE] 📩 Found FORWARD (link.message)")
        inner_msg = link["message"]
        logger.debug(f"[PARSE] Inner message keys: {list(inner_msg.keys())}")
        
        # Рекурсивно извлекаем из вложенного сообщения
        return {
            "source": "link.message",
            "mid": inner_msg.get("mid") or inner_msg.get("id"),
            "text": inner_msg.get("text", ""),
            "markup": inner_msg.get("markup", []),  # 🔹 БЕРЁМ MARKUP ИЗ INNER!
            "attachments": safe_list(inner_msg.get("attachments") or inner_msg.get("files") or inner_msg.get("media"))
        }
    
    # 🔹 2. Обычное сообщение (body)
    body = msg.get("body", {})
    if isinstance(body, dict) and ("text" in body or "attachments" in body or "markup" in body):
        logger.info(f"[PARSE] 📄 Using body")
        return {
            "source": "body",
            "mid": body.get("mid"),
            "text": body.get("text", ""),
            "markup": body.get("markup", []),
            "attachments": safe_list(body.get("attachments") or body.get("files") or body.get("media"))
        }
    
    # 🔹 3. Резерв: корень сообщения
    logger.info(f"[PARSE] 📦 Using root")
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files") or msg.get("media"))
    }

def safe_list(val: Any) -> List[Dict]:
    """Гарантирует возврат списка словарей"""
    if val is None:
        return []
    if isinstance(val, list):
        return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v]
    return []

# ===================================================================
# 5. TELEGRAM CLIENT (ОТПРАВКА ПО ССЫЛКЕ ИЛИ ФАЙЛОМ)
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
                    logger.info(f"[TG-RESP] {method}: ✅ 200 OK")
                    return True
                logger.error(f"[TG-RESP] {method}: ❌ {r.status} | {txt[:300]}")
                return False
        except Exception as e:
            logger.error(f"[TG-ERR] {method}: {e}", exc_info=True)
            return False

    async def text(self, t):
        if not t:
            logger.debug("[TG-SEND] Empty text, skipping")
            return True
        logger.info(f"[TG-SEND] 📝 Text: '{t[:100]}{'...' if len(t)>100 else ''}'")
        return await self.send("sendMessage", json={
            "chat_id": self.chat_id,
            "text": t,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        })

    async def media(self, type_, media, caption="", filename=None, is_url=False):
        """Универсальная отправка: media = URL (str) или файл (bytes)"""
        if isinstance(media, bytes):
            if len(media) > self.MAX_BYTES:
                logger.warning(f"[TG-SKIP] {type_} >50MB ({len(media)/1024/1024:.2f} MB)")
                return False
        
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        # Маппинг типов
        field_map = {
            "photo": "photo",
            "video": "video",
            "audio": "audio",
            "voice": "voice",
            "document": "document"
        }
        tg_field = field_map.get(type_, type_)
        
        if is_url:
            # 🔹 ОТПРАВКА ПО ССЫЛКЕ (как в TG→MAX боте) — БЫСТРО и НАДЁЖНО
            form.add_field(tg_field, media)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} (URL): {str(media)[:80]}...")
        else:
            # 🔹 ОТПРАВКА ФАЙЛОМ
            fname = filename or f"{type_}_file"
            form.add_field(tg_field, media, filename=fname)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} (FILE): {len(media)} bytes | fname: {fname}")
        
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
            logger.debug(f"[TG-SEND] Caption: '{caption[:50]}{'...' if len(caption)>50 else ''}'")
        
        return await self.send(f"send{type_.capitalize()}", data=form)

# ===================================================================
# 6. MAX CLIENT (БЕЗ since_seq — ДОВЕРЯЕМ MAX)
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
            # 🔹 УБРАЛИ since_seq — MAX сам вернёт последние сообщения
            params = {"chat_id": self.cid, "limit": 10}
            logger.debug(f"[MAX-REQ] Fetching messages (no since_seq)")
            
            async with self.session.get(
                f"{self.base}/messages",
                headers={"Authorization": self.token},
                params=params
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    logger.debug(f"[MAX-RESP] Raw: {json.dumps(raw, ensure_ascii=False)[:800]}")
                    # MAX возвращает {"messages": [...]} или [...]
                    msgs = raw.get("messages", raw) if isinstance(raw, dict) else raw
                    return msgs if isinstance(msgs, list) else []
                logger.error(f"[MAX-ERR] HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"[MAX-ERR] Exception: {e}", exc_info=True)
            return []

    async def download(self, token):
        await self.init()
        logger.info(f"[MAX-DL] 🔽 Request: token={token[:30]}...")
        try:
            async with self.session.get(
                f"{self.base}/files/{token}/download",
                headers={"Authorization": self.token}
            ) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[MAX-DL] ✅ Success: {len(data)} bytes")
                    return data
                err = await r.text()
                logger.error(f"[MAX-DL] ❌ HTTP {r.status}: {err[:200]}")
                return None
        except Exception as e:
            logger.error(f"[MAX-DL] ❌ Exception: {e}", exc_info=True)
            return None

# ===================================================================
# 7. ОБРАБОТКА СООБЩЕНИЙ (БЕЗ ПРОВЕРКИ SEQ)
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg):
    # 🔹 СУПЕР-ЛОГ: полный JSON сообщения
    logger.info(f"🔍 [RAW-MSG] {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    # Извлечение данных
    data = extract_data(msg)
    logger.info(f"[PARSE] Source: {data['source']} | MID: {data['mid']}")
    logger.info(f"[PARSE] Text: {len(data['text'])}c | Markup: {len(data['markup'])} | Attachments: {len(data['attachments'])}")
    
    mid = data["mid"]
    
    if not mid:
        logger.warning("[SKIP] ❌ No MID found")
        return
    
    logger.info(f"🆕 [NEW] Processing MID: {mid}")
    
    # Конвертация разметки
    text = apply_markup(data["text"], data["markup"]) if data["markup"] else data["text"]
    
    # Отправка текста
    if text:
        logger.info(f"[SEND-TEXT] ▶️ '{text[:100]}{'...' if len(text)>100 else ''}'")
        ok = await tg.text(text)
        logger.info(f"[RESULT] Text: {'✅ OK' if ok else '❌ FAIL'}")
        await asyncio.sleep(0.2)
    
    # Отправка вложений
    for i, att in enumerate(data["attachments"]):
        logger.info(f"[ATT #{i+1}/{len(data['attachments'])}] ▶️ Processing")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        if not isinstance(att, dict):
            logger.warning(f"[SKIP] ❌ Attachment #{i+1} not dict: {type(att)}")
            continue
        
        atype = att.get("type") or att.get("media_type") or "file"
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        # 🔹 Ищем URL (приоритет!) и токен
        url = payload.get("url") or att.get("url")
        token = payload.get("token") or att.get("token") or att.get("id") or att.get("file_token")
        fname = payload.get("filename") or att.get("filename") or att.get("name") or f"file_{i+1}"
        
        logger.info(f"[ATT] Type: {atype} | Has URL: {bool(url)} | Token: {token[:30] if token else None}... | Name: {fname}")
        
        # Определение типа для Telegram
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        tg_type = "document"
        
        if atype in ("image", "photo", "picture") or ext in ("jpg", "jpeg", "png", "gif", "webp", "heic"):
            tg_type = "photo"
        elif atype == "video" or ext in ("mp4", "mov", "avi", "mkv", "webm", "flv"):
            tg_type = "video"
        elif atype == "voice" or ext in ("ogg", "opus", "oga"):
            tg_type = "voice"
        elif atype == "audio" or ext in ("mp3", "wav", "m4a", "flac", "aac"):
            tg_type = "audio"
        
        logger.info(f"[TYPE] Detected: {tg_type} (from: {atype}, ext: {ext})")
        
        caption = text if tg_type != "document" else ""
        sent = False
        
        # 🔹 ЛОГИКА ОТПРАВКИ: URL (приоритет) ИЛИ ФАЙЛ
        if url and tg_type == "photo":
            # Фото по прямой ссылке — БЫСТРО и НАДЁЖНО
            logger.info(f"[SEND] 📤 Photo via URL: {url[:80]}...")
            sent = await tg.media(tg_type, url, caption, is_url=True)
        elif token:
            # Скачиваем и отправляем файлом
            logger.info(f"[SEND] 📤 Downloading via token...")
            file_data = await mx.download(token)
            if file_
                logger.info(f"[SEND] 📤 Sending {tg_type}...")
                sent = await tg.media(tg_type, file_data, caption, filename=fname)
            else:
                logger.error(f"[SKIP] ❌ Download failed for {fname}")
        else:
            logger.warning(f"[SKIP] ❌ No URL or Token found")
        
        logger.info(f"[RESULT] {tg_type}: {'✅ OK' if sent else '❌ FAIL'}")
        await asyncio.sleep(0.3)  # Пауза между файлами
    
    logger.info(f"✅ [DONE] MID: {mid}")

# ===================================================================
# 8. POLLING LOOP
# ===================================================================
async def polling_loop():
    logger.info("🔄 Starting polling loop...")
    await asyncio.sleep(2)  # Ждём готовности сети
    
    poll_count = 0
    while True:
        poll_count += 1
        logger.debug(f"[POLL] Iteration #{poll_count}")
        
        try:
            msgs = await mx.fetch()
            logger.debug(f"[POLL] Received {len(msgs)} messages")
            
            if msgs:
                for msg in msgs:
                    await handle(msg)
            else:
                logger.debug("[POLL] No new messages")
                
        except Exception as e:
            logger.error(f"[POLL-ERR] Exception: {e}", exc_info=True)
        
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 9. WEB SERVER (HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-forwarder",
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

# ===================================================================
# 10. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL - ALL FIXES]...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
