# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, hashlib
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
# 2. ПЕРЕМЕННЫЕ И СОСТОЯНИЕ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '1'))

STATE_FILE = "bot_state.json"
_processed_mids = []  # Список обработанных ID
_last_seq = 0         # Последний обработанный seq

def load_state():
    global _processed_mids, _last_seq
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                state = json.load(f)
                _processed_mids = state.get("mids", [])
                _last_seq = state.get("last_seq", 0)
                logger.info(f"📦 [STATE] Loaded: {len(_processed_mids)} mids, seq={_last_seq}")
        except Exception as e:
            logger.warning(f"⚠️ [STATE] Load error: {e}")

def save_state():
    try:
        # Храним только последние 500 ID, чтобы файл не раздувался
        _processed_mids = _processed_mids[-500:]
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"mids": _processed_mids, "last_seq": _last_seq}, f)
    except Exception as e:
        logger.warning(f"⚠️ [STATE] Save error: {e}")

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===================================================================
def safe_str(val: Any) -> str:
    if val is None: return ""
    if isinstance(val, dict):
        for k in ['id', 'value', 'text', 'content', 'mid', 'url', 'token', 'file_id']:
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

def get_mid(msg: Dict) -> str:
    """Извлекает mid из любого уровня вложенности"""
    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    link_msg = msg.get("link", {}).get("message", {}) if isinstance(msg.get("link"), dict) else {}
    mid = body.get("mid") or link_msg.get("mid") or msg.get("id") or msg.get("message_id") or ""
    return str(mid)

# ===================================================================
# 4. КОНВЕРТАЦИЯ РАЗМЕТКИ (ГРАФЕМНАЯ + СТЕК)
# ===================================================================
def apply_markup(text: str, markup: List[Dict]) -> str:
    """Конвертирует разметку MAX в HTML Telegram без потерь"""
    if not markup or not text:
        return text

    logger.debug(f"[MARKUP] Input: '{text[:50]}...' | items: {len(markup)}")
    
    # Маппинг типов
    TAGS = {
        "strong": ("<b>", "</b>"), "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"), "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"),
        "pre": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"),
        "strike": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }

    # Разбиваем текст на символы (графемы), чтобы корректно работать с эмодзи
    chars = list(text)
    n = len(chars)
    
    # Сортируем разметку с конца к началу, чтобы вставка тегов не сбивала индексы
    sorted_markup = sorted(markup, key=lambda x: x.get("from", 0), reverse=True)
    
    for m in sorted_markup:
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length

            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range: {start}-{end} (len={n})")
                continue

            open_tag, close_tag = "", ""
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
            elif mtype == "link":
                url = m.get("url") or m.get("href") or ""
                if url:
                    url = url.replace('"', '&quot;')
                    open_tag = f'<a href="{url}">'
                    close_tag = "</a>"
            
            if open_tag:
                # Вставляем теги в список символов
                chars[end:end] = list(close_tag)
                chars[start:start] = list(open_tag)
                logger.debug(f"[MARKUP] Applied '{mtype}' at {start}:{end}")
        except Exception as e:
            logger.warning(f"[MARKUP] Error: {e}")

    result = "".join(chars)
    logger.debug(f"[MARKUP] Output: '{result[:50]}...'")
    return result

# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ (BODY + LINK.MESSAGE)
# ===================================================================
def extract_data(msg: Dict) -> Dict:
    logger.debug(f"[EXTRACT] Keys: {list(msg.keys())}")
    
    # 1. Обычное сообщение (body)
    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    if body and ("text" in body or "attachments" in body or "markup" in body):
        return {
            "source": "body",
            "mid": body.get("mid"),
            "seq": body.get("seq"),
            "text": body.get("text", ""),
            "markup": body.get("markup", []),
            "attachments": safe_list(body.get("attachments") or body.get("files"))
        }
    
    # 2. Пересланное сообщение (link.message)
    link = msg.get("link", {}) if isinstance(msg.get("link"), dict) else {}
    if link and isinstance(link.get("message"), dict):
        lm = link["message"]
        return {
            "source": "link.message",
            "mid": lm.get("mid"),
            "seq": lm.get("seq"),
            "text": lm.get("text", ""),
            "markup": lm.get("markup", []),
            "attachments": safe_list(lm.get("attachments") or lm.get("files"))
        }
    
    # 3. Резерв (корень)
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id"),
        "seq": msg.get("seq"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files"))
    }

# ===================================================================
# 6. TELEGRAM CLIENT (ОТПРАВКА ПО ССЫЛКЕ ИЛИ ФАЙЛОМ)
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
                logger.info(f"[TG-RESP] {method}: {r.status} | {txt[:200]}")
                return r.status == 200
        except Exception as e:
            logger.error(f"[TG-ERR] {method}: {e}")
            return False

    async def text(self, t):
        if not t: return True
        logger.info(f"[TG-SEND] text: '{t[:100]}...'")
        return await self.send("sendMessage", json={"chat_id": self.chat_id, "text": t, "parse_mode": "HTML"})

    async def photo(self, media, caption="", is_url=False):
        """Отправка фото: media может быть bytes или URL строкой"""
        if isinstance(media, bytes) and len(media) > self.MAX_BYTES:
            logger.warning(f"[TG-SKIP] photo >50MB")
            return False
        
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        if is_url:
            form.add_field("photo", media) # Telegram сам скачает
            logger.info(f"[TG-SEND] photo (URL): {media[:50]}...")
        else:
            form.add_field("photo", media, filename="photo.jpg")
            logger.info(f"[TG-SEND] photo (File): {len(media)} bytes")
            
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
        return await self.send("sendPhoto", data=form)

    async def video(self, media, fname, caption="", is_url=False):
        if isinstance(media, bytes) and len(media) > self.MAX_BYTES: return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        if is_url:
            form.add_field("video", media)
        else:
            form.add_field("video", media, filename=fname)
            
        form.add_field("supports_streaming", "true")
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
        return await self.send("sendVideo", data=form)

    async def doc(self, media, fname, caption="", is_url=False):
        if isinstance(media, bytes) and len(media) > self.MAX_BYTES: return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        if is_url:
            form.add_field("document", media)
        else:
            form.add_field("document", media, filename=fname)
            
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
        return await self.send("sendDocument", data=form)

# ===================================================================
# 7. MAX CLIENT
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

    async def fetch(self, limit=10):
        await self.init()
        try:
            params = {"chat_id": self.cid, "limit": limit}
            # Используем seq для получения только новых сообщений
            if _last_seq > 0:
                params["since_seq"] = _last_seq
            
            logger.debug(f"[MAX-REQ] Fetching since_seq={_last_seq}")
            async with self.session.get(f"{self.base}/messages", headers={"Authorization": self.token}, params=params) as r:
                if r.status == 200:
                    raw = await r.json()
                    return safe_list(raw)
                logger.error(f"[MAX] HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"[MAX] Error: {e}")
            return []

    async def download(self, token):
        await self.init()
        logger.info(f"[MAX-DL] Request: token={token[:30]}...")
        try:
            async with self.session.get(f"{self.base}/files/{token}/download", headers={"Authorization": self.token}) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[MAX-DL] OK: {len(data)} bytes")
                    return data
                err = await r.text()
                logger.error(f"[MAX-DL] HTTP {r.status}: {err[:200]}")
                return None
        except Exception as e:
            logger.error(f"[MAX-DL] Exception: {e}")
            return None

# ===================================================================
# 8. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg):
    logger.info(f"[HANDLE] ▶️ START")
    
    mid = get_mid(msg)
    if not mid:
        logger.warning("[SKIP] No mid")
        return
    
    # Проверка дублей
    if mid in _processed_mids:
        logger.info(f"[DUPE] Skip mid: {mid}")
        return

    logger.info(f"[HANDLE] New mid: {mid}")
    
    data = extract_data(msg)
    logger.info(f"[EXTRACT] src:{data['source']} | text_len:{len(data['text'])} | markup:{len(data['markup'])} | atts:{len(data['attachments'])}")
    
    # Конвертация разметки
    text = apply_markup(data["text"], data["markup"]) if data["markup"] else data["text"]
    
    logger.info(f"📨 MSG | mid:{mid} | text:{len(text)}c | atts:{len(data['attachments'])}")

    # Отправка текста
    if text:
        ok = await tg.text(text)
        logger.info(f"[RESULT] text: {'✅' if ok else '❌'}")
        await asyncio.sleep(0.2)

    # Отправка вложений
    for i, att in enumerate(data['attachments']):
        logger.info(f"[ATT #{i+1}] ▶️")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:300]}")
        
        if not isinstance(att, dict): continue
        
        atype = safe_str(att.get("type") or att.get("media_type") or "file")
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        # Ищем токен и URL
        token = safe_str(att.get("token") or payload.get("token") or att.get("id"))
        url = safe_str(payload.get("url") or att.get("url"))
        fname = safe_str(att.get("name") or payload.get("filename") or f"file_{i+1}")
        
        logger.info(f"[ATT] type:{atype} | has_url:{bool(url)} | token:{token[:30] if token else None}...")
        
        # Определяем тип для TG
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        tg_type = "document"
        if atype in ("image", "photo") or ext in ("jpg","jpeg","png","gif","webp"): tg_type = "photo"
        elif atype == "video" or ext in ("mp4","mov","avi"): tg_type = "video"
        elif atype in ("audio", "voice") or ext in ("mp3","wav","ogg"): tg_type = "audio"
        
        caption = text if tg_type != "document" else ""
        sent = False

        # 🔹 ЛОГИКА ОТПРАВКИ: URL (как в TG->MAX) ИЛИ ФАЙЛ
        if url and tg_type == "photo":
            # Отправляем фото по прямой ссылке (самый надежный способ для MAX)
            logger.info(f"[TG-SEND] Photo via URL: {url[:50]}...")
            sent = await tg.photo(url, caption, is_url=True)
        elif token:
            # Если нет URL, скачиваем и шлем файлом
            logger.info(f"[DOWNLOAD] ▶️ {fname} via token")
            file_data = await mx.download(token)
            if file_data:
                if tg_type == "photo": sent = await tg.photo(file_data, caption)
                elif tg_type == "video": sent = await tg.video(file_data, fname, caption)
                elif tg_type == "audio": sent = await tg.doc(file_data, fname, caption) # TG не любит audio по ссылке без имени
                else: sent = await tg.doc(file_data, fname, caption)
                logger.info(f"[RESULT] {tg_type}: {'✅' if sent else '❌'}")
            else:
                logger.error(f"[SKIP] Download failed")
        else:
            logger.warning("[SKIP] No URL or Token found")

        await asyncio.sleep(0.3)

    # Сохраняем состояние (успешно обработали)
    _processed_mids.append(mid)
    if data['seq']:
        global _last_seq
        if int(data['seq']) > _last_seq:
            _last_seq = int(data['seq'])
    save_state()
    logger.info(f"✅ [HANDLE] DONE: {mid} | seq updated to {_last_seq}")

# ===================================================================
# 9. POLLING
# ===================================================================
async def loop():
    logger.info("🔄 Starting loop...")
    load_state()
    await asyncio.sleep(2)
    
    i = 0
    while True:
        i += 1
        logger.debug(f"[POLL] #{i}")
        try:
            msgs = await mx.fetch(limit=5)
            if msgs:
                for msg in msgs:
                    await handle(msg)
        except Exception as e:
            logger.error(f"[ERR] {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

async def health(req):
    return web.json_response({"ok": True, "seq": _last_seq, "mids": len(_processed_mids)})

async def run():
    app = web.Application()
    app.router.add_get('/health', health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    logger.info("🌐 Server on :8080")
    await loop()

if __name__ == '__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 Fatal: {e}")
        sys.exit(1)
