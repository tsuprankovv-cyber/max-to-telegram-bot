# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, re
from aiohttp import web
from typing import List, Dict, Optional, Any, Tuple
from collections import deque

# ===================================================================
# 1. ЛОГИРОВАНИЕ (МАКСИМУМ — НЕ УДАЛЯТЬ)
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
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '15'))

_processed_mids: deque = deque(maxlen=20)
_last_seq = 0

logger.info("=" * 90)
logger.info("🚀 MAX → TG FORWARDER [FINAL - ALL FIXES + MAX LOGS]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s")
logger.info("🔒 mid deque(20) | 🎨 All markup+graphemes | 📎 URL for ALL media | ⚡ 429+timing")
logger.info("📊 MAX LOGS: every step logged, none removed until release")
logger.info("=" * 90)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS!"); sys.exit(1)

# ===================================================================
# 3. ГРАФЕМНЫЙ СПЛИТТЕР (для корректной работы с эмодзи)
# ===================================================================
def split_into_graphemes(text: str) -> List[str]:
    """Разбивает текст на графемы (визуальные символы) для корректной работы с эмодзи"""
    logger.debug(f"[GRAPH] Splitting text into graphemes: '{text[:50]}...'")
    if not text: return []
    graphemes = []
    i = 0
    while i < len(text):
        char = text[i]
        # Эмодзи и модификаторы
        if '\U0001F300' <= char <= '\U0001F9FF' or '\U00002600' <= char <= '\U000026FF' or '\U00002700' <= char <= '\U000027BF':
            j = i + 1
            while j < len(text):
                nc = text[j]
                if '\U0001F3FB' <= nc <= '\U0001F3FF' or nc == '\u200D' or nc in '\uFE0E\uFE0F':
                    char += nc; j += 1
                else: break
            graphemes.append(char); i = j
        else:
            graphemes.append(char); i += 1
    logger.debug(f"[GRAPH] Result: {len(graphemes)} graphemes")
    return graphemes

# ===================================================================
# 4. КОНВЕРТЕР РАЗМЕТКИ (ВСЕ ТИПЫ + СТЕК + ГРАФЕМЫ + МАКС ЛОГИ)
# ===================================================================
def apply_markup(text: str, markup: List[Dict]) -> str:
    logger.info(f"[MARKUP] ▶️ START | text_len={len(text)} | markup_count={len(markup)}")
    logger.debug(f"[MARKUP] Input text: '{text[:100]}{'...' if len(text)>100 else ''}'")
    logger.debug(f"[MARKUP] Raw markup: {json.dumps(markup, ensure_ascii=False)}")
    
    if not markup or not text:
        logger.debug("[MARKUP] No markup or empty text, returning as-is")
        return text

    # Маппинг ВСЕХ типов
    TAGS = {
        "strong": ("<b>", "</b>"), "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"), "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"), "inline-code": ("<code>", "</code>"),
        "pre": ("<pre>", "</pre>"), "preformatted": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"), "u": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"), "strike": ("<s>", "</s>"), "s": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }

    # 🔹 Разбиваем на графемы
    graphemes = split_into_graphemes(text)
    n = len(graphemes)
    logger.info(f"[MARKUP] Text split into {n} graphemes")

    # 🔹 Создаём события: (позиция, тип, тег, приоритет)
    events = []
    for idx, m in enumerate(markup):
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length
            logger.debug(f"[MARKUP] Item #{idx+1}: type='{mtype}' from={start} len={length} end={end}")
            
            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range: {start}-{end} (grapheme_count={n})"); continue
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                events.append((start, 'open', open_tag, idx))
                events.append((end, 'close', close_tag, idx))
                logger.debug(f"[MARKUP] Added events for '{mtype}' at {start}/{end}")
            elif mtype == "link":
                url = m.get("url") or m.get("href") or ""
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url_safe}">'; close_tag = "</a>"
                    events.append((start, 'open', open_tag, idx))
                    events.append((end, 'close', close_tag, idx))
                    logger.debug(f"[MARKUP] Added link events at {start}/{end} url={url[:50]}...")
                else: logger.warning("[MARKUP] Link without URL"); continue
            elif mtype in ("mention", "hashtag", "bot_command", "cashtag"):
                logger.debug(f"[MARKUP] Skipping {mtype} (no HTML tag)")
                continue
            else:
                logger.warning(f"[MARKUP] Unknown type: '{mtype}' | item: {m}"); continue
        except Exception as e:
            logger.error(f"[MARKUP] Error processing item #{idx+1}: {e} | markup: {m}", exc_info=True); continue

    # 🔹 Сортируем события: по позиции, затем close перед open (чтобы </b><i> а не <i></b>)
    events.sort(key=lambda x: (x[0], 0 if x[1]=='close' else 1, -x[3]))
    logger.debug(f"[MARKUP] Sorted {len(events)} events")

    # 🔹 Обходим графемы, применяя стек тегов
    result = []
    active_tags = []  # стек: (close_tag, priority)
    event_idx = 0
    
    for pos in range(n + 1):
        # Обрабатываем все события на этой позиции
        while event_idx < len(events) and events[event_idx][0] == pos:
            _, etype, tag, priority = events[event_idx]
            if etype == 'close':
                # Закрываем теги (ищем в стеке)
                for i in range(len(active_tags) - 1, -1, -1):
                    if active_tags[i][1] == priority:
                        result.append(active_tags[i][0]); active_tags.pop(i); break
            elif etype == 'open':
                active_tags.append((tag, priority))
            event_idx += 1
        
        # Добавляем саму графему (если не конец)
        if pos < n:
            result.append(graphemes[pos])
    
    # Закрываем все оставшиеся теги
    for tag_close, _ in reversed(active_tags):
        result.append(tag_close)
    
    final_text = "".join(result)
    logger.info(f"[MARKUP] ✅ DONE | Applied tags | Output: '{final_text[:100]}{'...' if len(final_text)>100 else ''}'")
    return final_text

# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ (ФОРВАРДЫ + РЕКУРСИЯ + МАКС ЛОГИ)
# ===================================================================
def extract_data(msg: Dict, depth: int = 0) -> Dict:
    logger.info(f"[PARSE] ▶️ START | depth={depth} | msg_keys={list(msg.keys())}")
    
    # 🔹 Детекция форварда
    link = msg.get("link")
    logger.info(f"[PARSE-FWD] Checking forward: link_exists={link is not None}")
    if isinstance(link, dict):
        logger.info(f"[PARSE-FWD] link.keys()={list(link.keys())} | link.type={link.get('type')}")
        if "message" in link:
            logger.info(f"[PARSE-FWD] ✅ FOUND FORWARD in link.message")
            inner = link["message"]
            logger.info(f"[PARSE-FWD] inner.keys()={list(inner.keys())}")
            # 🔹 Рекурсия на случай вложенных форвардов
            if depth < 3 and "link" in inner and "message" in inner["link"]:
                logger.info(f"[PARSE-FWD] 🔄 Nested forward, recursing to depth {depth+1}")
                return extract_data(inner, depth + 1)
            # 🔹 Извлекаем ВСЕ поля из inner
            return {
                "source": "link.message",
                "mid": inner.get("mid") or inner.get("id"),
                "text": inner.get("text", ""),
                "markup": inner.get("markup", []),
                "attachments": safe_list(inner.get("attachments") or inner.get("files") or inner.get("media")),
                "original_chat_id": link.get("chat_id")
            }
    
    # 🔹 Обычное сообщение (body)
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
    
    # 🔹 Резерв: корень
    logger.info(f"[PARSE] 📦 Using root")
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files") or msg.get("media"))
    }

def safe_list(val: Any) -> List[Dict]:
    logger.debug(f"[SAFE_LIST] Input type: {type(val)}")
    if val is None: logger.debug("[SAFE_LIST] None → []"); return []
    if isinstance(val, list):
        res = [v for v in val if isinstance(v, dict)]; logger.debug(f"[SAFE_LIST] Filtered: {len(res)} items"); return res
    if isinstance(val, dict):
        for k in ['messages','items','data','result','message','attachments','files','media']:
            if k in val:
                v = val[k]; logger.debug(f"[SAFE_LIST] Found key '{k}'"); return v if isinstance(v, list) else [v]
    logger.debug("[SAFE_LIST] No match → []"); return []

# ===================================================================
# 6. TELEGRAM CLIENT (429 + МАКС ЛОГИ)
# ===================================================================
class TG:
    def __init__(s, token, chat_id):
        s.token, s.chat_id, s.base = token, chat_id, f"https://api.telegram.org/bot{token}"
        s.session = None; s.MAX_BYTES = 50*1024*1024
        logger.info(f"[TG] Initialized: chat_id={chat_id}")
    
    async def init(s):
        if not s.session or s.session.closed:
            logger.debug("[TG] Creating new session"); s.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def send(s, method, **kw):
        await s.init()
        logger.debug(f"[TG-REQ] {method} | params: {list(kw.keys())}")
        try:
            async with s.session.post(f"{s.base}/{method}", **kw) as r:
                txt = await r.text()
                if r.status == 200:
                    logger.info(f"[TG-RESP] {method}: ✅ 200 OK | body: {txt[:100]}"); return True
                # 🔹 Обработка 429
                if r.status == 429:
                    try:
                        resp = await r.json()
                        retry = resp.get("parameters", {}).get("retry_after", 10)
                        logger.warning(f"[TG] ⚠️ Rate limit: waiting {retry}s")
                        await asyncio.sleep(retry)
                        logger.info(f"[TG] 🔄 Retrying {method} after rate limit")
                        return await s.send(method, **kw)
                    except Exception as e:
                        logger.error(f"[TG] Error parsing 429: {e}")
                logger.error(f"[TG-RESP] {method}: ❌ {r.status} | body: {txt[:300]}"); return False
        except Exception as e:
            logger.error(f"[TG-ERR] {method}: {e}", exc_info=True); return False
    
    async def text(s, t):
        if not t: logger.debug("[TG-SEND] Empty text, skip"); return True
        logger.info(f"[TG-SEND] 📝 Text: '{t[:100]}{'...' if len(t)>100 else ''}' (len={len(t)})")
        return await s.send("sendMessage", json={"chat_id": s.chat_id, "text": t, "parse_mode": "HTML"})
    
    async def media(s, type_, media, caption="", filename=None, is_url=False):
        logger.info(f"[TG-SEND] 📎 Preparing {type_} | is_url={is_url} | caption_len={len(caption) if caption else 0}")
        if isinstance(media, bytes):
            if len(media) > s.MAX_BYTES:
                logger.warning(f"[TG-SKIP] {type_} >50MB ({len(media)/1024/1024:.2f} MB)"); return False
        await s.init(); form = aiohttp.FormData(); form.add_field("chat_id", s.chat_id)
        field_map = {"photo":"photo","video":"video","audio":"audio","voice":"voice","document":"document"}
        tg_field = field_map.get(type_, type_)
        if is_url:
            form.add_field(tg_field, media)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} via URL: {str(media)[:80]}...")
        else:
            fname = filename or f"{type_}_file"
            form.add_field(tg_field, media, filename=fname)
            logger.info(f"[TG-SEND] 📎 {type_.upper()} via FILE: {len(media)} bytes | fname={fname}")
        if caption:
            form.add_field("caption", caption[:1024]); form.add_field("parse_mode", "HTML")
            logger.debug(f"[TG-SEND] Caption: '{caption[:50]}{'...' if len(caption)>50 else ''}'")
        return await s.send(f"send{type_.capitalize()}", data=form)

# ===================================================================
# 7. MAX CLIENT (МАКС ЛОГИ)
# ===================================================================
class MX:
    def __init__(s, token, cid, base):
        s.token, s.cid, s.base, s.session = token, cid, base, None
        logger.info(f"[MAX] Initialized: cid={cid}")
    
    async def init(s):
        if not s.session or s.session.closed:
            logger.debug("[MAX] Creating new session"); s.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def fetch(s, last_seq: Optional[int] = None):
        await s.init()
        try:
            params = {"chat_id": s.cid, "limit": 1}
            if last_seq is not None:
                params["since_seq"] = last_seq
                logger.debug(f"[MAX-REQ] Fetching since_seq={last_seq}")
            else:
                logger.debug(f"[MAX-REQ] First fetch (no since_seq)")
            async with s.session.get(f"{s.base}/messages", headers={"Authorization": s.token}, params=params) as r:
                if r.status == 200:
                    raw = await r.json()
                    logger.debug(f"[MAX-RESP] Raw: {json.dumps(raw, ensure_ascii=False)[:800]}")
                    msgs = raw.get("messages", raw) if isinstance(raw, dict) else raw
                    return msgs if isinstance(msgs, list) else []
                logger.error(f"[MAX] HTTP {r.status}"); return []
        except Exception as e:
            logger.error(f"[MAX] Exception: {e}", exc_info=True); return []
    
    async def download(s, token):
        await s.init()
        logger.info(f"[MAX-DL] 🔽 Request: token={token[:30]}...")
        try:
            async with s.session.get(f"{s.base}/files/{token}/download", headers={"Authorization": s.token}) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[MAX-DL] ✅ Success: {len(data)} bytes")
                    return data
                err = await r.text()
                logger.error(f"[MAX-DL] ❌ HTTP {r.status}: {err[:200]}"); return None
        except Exception as e:
            logger.error(f"[MAX-DL] Exception: {e}", exc_info=True); return None

# ===================================================================
# 8. ОБРАБОТКА (МАКС ЛОГИ + ВСЕ ФИКСЫ)
# ===================================================================
tg, mx = TG(TG_TOKEN, TG_CHAT), MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg):
    logger.info(f"🔍 [RAW-MSG] {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    data = extract_data(msg)
    logger.info(f"[PARSE-RESULT] source={data['source']} | mid={data['mid']}")
    logger.info(f"[PARSE-RESULT] text_len={len(data['text'])} | markup_count={len(data['markup'])} | attachments_count={len(data['attachments'])}")
    if data['attachments']:
        for i, att in enumerate(data['attachments']):
            logger.info(f"[PARSE-RESULT] att[{i}]: type={att.get('type')}, has_url={bool((att.get('payload',{}) if isinstance(att.get('payload'),dict) else {}).get('url'))}")
    
    mid, seq = data["mid"], data.get("seq")
    if not mid: logger.warning("[SKIP] ❌ No MID found"); return
    if mid in _processed_mids: logger.info(f"[DUPE] ⏭ Skip known mid: {mid}"); return
    logger.info(f"🆕 [NEW] Processing MID: {mid}")
    
    # 🔹 Конвертация разметки
    text = apply_markup(data["text"], data["markup"]) if data["markup"] else data["text"]
    
    # 🔹 Отправка текста
    if text:
        logger.info(f"[SEND-TEXT] ▶️ '{text[:100]}{'...' if len(text)>100 else ''}'")
        ok = await tg.text(text)
        logger.info(f"[RESULT] Text: {'✅ OK' if ok else '❌ FAIL'}")
        await asyncio.sleep(0.2)
    
    # 🔹 Отправка вложений
    for i, att in enumerate(data["attachments"]):
        logger.info(f"[ATT #{i+1}/{len(data['attachments'])}] ▶️ Processing")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:500]}")
        if not isinstance(att, dict): logger.warning("[SKIP] ❌ Attachment not dict"); continue
        
        atype = att.get("type") or att.get("media_type") or "file"
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        # 🔹 Ищем URL и токен
        url = payload.get("url") or att.get("url")
        token = payload.get("token") or att.get("token") or att.get("id") or att.get("file_token")
        fname = payload.get("filename") or att.get("filename") or att.get("name") or f"file_{i+1}"
        
        logger.info(f"[ATT] Type: {atype} | Has URL: {bool(url)} | Token: {token[:30] if token else None}... | Name: {fname}")
        
        # 🔹 Определение типа для TG
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        tg_type = "document"
        if atype in ("image","photo","picture") or ext in ("jpg","jpeg","png","gif","webp","heic"): tg_type = "photo"
        elif atype == "video" or ext in ("mp4","mov","avi","mkv","webm","flv"): tg_type = "video"
        elif atype == "voice" or ext in ("ogg","opus","oga"): tg_type = "voice"
        elif atype == "audio" or ext in ("mp3","wav","m4a","flac","aac"): tg_type = "audio"
        
        logger.info(f"[TYPE] Detected: {tg_type} (from: {atype}, ext: {ext})")
        
        caption = text if tg_type != "document" else ""; sent = False
        
        # 🔹 🔥 КЛЮЧЕВОЕ: ОТПРАВКА ПО ССЫЛКЕ ДЛЯ ВСЕХ ТИПОВ
        if url and tg_type in ("photo", "video", "document", "audio", "voice"):
            logger.info(f"[SEND] 📤 {tg_type} via URL: {url[:80]}...")
            sent = await tg.media(tg_type, url, caption, is_url=True)
        elif token:
            logger.info(f"[SEND] 📤 Downloading via token...")
            file_data = await mx.download(token)
            # 🔹 ЯВНАЯ ПРОВЕРКА (не обрежется):
            if file_data is not None and len(file_data) > 0:
                logger.info(f"[SEND] 📤 Sending {tg_type} (FILE)...")
                sent = await tg.media(tg_type, file_data, caption, filename=fname)
            else:
                logger.error(f"[SKIP] ❌ Download failed (token expired or 404)")
        else:
            logger.warning(f"[SKIP] ❌ No URL or Token found")
        
        logger.info(f"[RESULT] {tg_type}: {'✅ OK' if sent else '❌ FAIL'}")
        
        # 🔹 Тайминги: 0.3с между медиа, +0.2с для голосовых
        delay = 0.3 + (0.2 if tg_type == "voice" else 0)
        logger.debug(f"[DELAY] Sleeping {delay}s after {tg_type}")
        await asyncio.sleep(delay)
    
    # 🔹 Обновляем состояние
    _processed_mids.append(mid)
    if seq:
        global _last_seq
        _last_seq = max(_last_seq, int(seq))
    logger.info(f"✅ [DONE] MID: {mid} | processed: {len(_processed_mids)}/20 | last_seq: {_last_seq}")

# ===================================================================
# 9. POLLING
# ===================================================================
async def polling_loop():
    global _last_seq
    logger.info("🔄 Starting polling loop..."); await asyncio.sleep(2)
    while True:
        try:
            msgs = await mx.fetch(last_seq=_last_seq if _last_seq > 0 else None)
            logger.debug(f"[POLL] Got {len(msgs)} msgs")
            if msgs: await handle(msgs[0])
        except Exception as e:
            logger.error(f"[LOOP] Exception: {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 10. SERVER
# ===================================================================
async def health_handler(request):
    return web.json_response({"ok": True, "processed": len(_processed_mids), "last_seq": _last_seq})

async def run_app():
    app = web.Application(); app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app); await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    logger.info("🌐 Server on :8080 (UptimeRobot compatible)"); await polling_loop()

# ===================================================================
# 11. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL]...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}"); sys.exit(1)
