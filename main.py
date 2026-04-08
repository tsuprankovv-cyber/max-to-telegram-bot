# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, hashlib, re
from aiohttp import web
from typing import List, Dict, Optional, Any, Tuple

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
# 2. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '1'))
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '1'))

_processed = set()
_sent_count = 0  # Счётчик реально отправленных сообщений

logger.info("=" * 100)
logger.info("🚀 MAX → TG FORWARDER [UNICODE FIX + STACK TAGS]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s | 🔁 Retries: {MAX_RETRIES}")
logger.info("🔒 Hash: mid-only | 🎨 Markup: unicode-safe + stack tags | 📎 Attachments: deep search")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS!")
    sys.exit(1)

# ===================================================================
# 3. HELPERS
# ===================================================================
def safe_str(val: Any) -> str:
    if val is None: return ""
    if isinstance(val, dict):
        for k in ['id', 'value', 'text', 'content', '_id', '$oid', 'mid', 'url', 'token', 'file_id']:
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
    link_msg = msg.get("link", {}).get("message", {}) if isinstance(msg.get("link"), dict) else {}
    mid = body.get("mid") or link_msg.get("mid") or msg.get("id") or msg.get("message_id") or msg.get("_id") or ""
    return hashlib.md5(str(mid).encode()).hexdigest()[:12] if mid else ""

def guess_media_type(filename: str, att_type: str, payload: Dict = None) -> str:
    att_type = (att_type or "").lower()
    ext = filename.split('.')[-1].lower() if '.' in filename else ""
    if att_type in ("photo", "image", "picture"): return "photo"
    if att_type == "video": return "video"
    if att_type == "voice": return "voice"
    if att_type == "audio":
        if ext in ("ogg", "opus", "oga"): return "voice"
        return "audio"
    if ext in ("jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "heic"): return "photo"
    if ext in ("mp4", "mov", "avi", "mkv", "webm", "flv", "m4v", "3gp"): return "video"
    if ext in ("ogg", "opus", "oga"): return "voice"
    if ext in ("mp3", "wav", "m4a", "flac", "aac", "wma", "aiff"): return "audio"
    return "document"

# ===================================================================
# 🔎 UNICODE-SAFE MARKUP → HTML CONVERSION (STACK TAGS)
# ===================================================================
def text_to_graphemes(text: str) -> List[str]:
    """Разбивает текст на список графем (с учётом эмодзи, комбинированных символов)"""
    # Простая эвристика: эмодзи и модификаторы тона кожи
    graphemes = []
    i = 0
    while i < len(text):
        char = text[i]
        # Если это эмодзи или модификатор, объединяем с предыдущим
        if '\U0001F300' <= char <= '\U0001F9FF' or '\U00002600' <= char <= '\U000026FF' or '\U00002700' <= char <= '\U000027BF':
            # Проверяем следующие символы на модификаторы (тона кожи, ZWJ-последовательности)
            j = i + 1
            while j < len(text):
                next_char = text[j]
                # Модификаторы тона кожи, ZWJ, вариационные селекторы
                if '\U0001F3FB' <= next_char <= '\U0001F3FF' or next_char == '\u200D' or next_char in '\uFE0E\uFE0F':
                    char += next_char
                    j += 1
                else:
                    break
            graphemes.append(char)
            i = j
        else:
            graphemes.append(char)
            i += 1
    return graphemes

def convert_markup_to_html(text: str, markup: List[Dict]) -> str:
    """
    Unicode-safe конвертация markup в HTML с поддержкой вложенных тегов.
    """
    if not markup or not text:
        return text or ""
    
    logger.debug(f"[MARKUP] Input: text_len={len(text)}, grapheme_count={len(text_to_graphemes(text))}, markup_count={len(markup)}")
    logger.debug(f"[MARKUP] Raw markup: {json.dumps(markup, ensure_ascii=False)[:500]}")
    
    # Разбиваем текст на графемы
    graphemes = text_to_graphemes(text)
    n = len(graphemes)
    
    # Создаём массив тегов для каждой позиции: tags_at[pos] = список открывающих/закрывающих тегов
    tags_at = [[] for _ in range(n + 1)]
    
    # Словарь маппинга типов
    TAG_MAP = {
        "strong": ("<b>", "</b>"),
        "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"),
        "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"),
        "inline-code": ("<code>", "</code>"),
        "pre": ("<pre>", "</pre>"),
        "preformatted": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"),
        "u": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"),
        "s": ("<s>", "</s>"),
        "del": ("<s>", "</s>"),
    }
    
    unknown_types = set()
    
    for i, m in enumerate(markup):
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length
            
            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range #{i+1}: from={start}, length={length}, grapheme_count={n}")
                continue
            
            if mtype in TAG_MAP:
                tag_open, tag_close = TAG_MAP[mtype]
                tags_at[start].append(("open", tag_open, i))
                tags_at[end].append(("close", tag_close, i))
                logger.debug(f"[MARKUP] Added tag #{i+1}: '{mtype}' [{start}:{end}] → {tag_open}...{tag_close}")
            elif mtype in ("link", "url"):
                url = m.get("url") or m.get("href") or ""
                if url:
                    url = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    tag_open = f'<a href="{url}">'
                    tag_close = "</a>"
                    tags_at[start].append(("open", tag_open, i))
                    tags_at[end].append(("close", tag_close, i))
                    logger.debug(f"[MARKUP] Added link #{i+1}: [{start}:{end}] → {tag_open}...{tag_close}")
                else:
                    logger.warning(f"[MARKUP] Link without URL: {m}")
            elif mtype in ("mention", "mention_name", "hashtag", "bot_command", "cashtag"):
                logger.debug(f"[MARKUP] Skipping {mtype} (no HTML tag): {m}")
                continue
            else:
                if mtype not in unknown_types:
                    unknown_types.add(mtype)
                    logger.warning(f"[MARKUP] Unknown type '{mtype}': {m}")
        except Exception as e:
            logger.error(f"[MARKUP] Error processing item #{i+1}: {e} | markup: {m}", exc_info=True)
            continue
    
    if unknown_types:
        logger.info(f"[MARKUP] Summary: {len(markup)} items, {len(unknown_types)} unknown: {unknown_types}")
    
    # Собираем результат, проходя по графемам и применяя теги
    result = []
    active_tags = []  # Стек активных тегов: список (tag_close, markup_index)
    
    for pos in range(n):
        # Сначала закрывающие теги (в обратном порядке добавления, чтобы закрыть внутренние)
        for tag_type, tag, idx in sorted(tags_at[pos], key=lambda x: -x[2]):
            if tag_type == "close":
                # Ищем в стеке и закрываем
                for i in range(len(active_tags) - 1, -1, -1):
                    if active_tags[i][1] == idx:
                        result.append(active_tags[i][0])  # Добавляем закрывающий тег
                        active_tags.pop(i)
                        break
        
        # Добавляем саму графему
        result.append(graphemes[pos])
        
        # Затем открывающие теги (в порядке добавления, чтобы открыть внешние первыми)
        for tag_type, tag, idx in sorted(tags_at[pos + 1], key=lambda x: x[2]):
            if tag_type == "open":
                result.insert(-1, tag)  # Вставляем перед последней графемой
                active_tags.append((tag, idx))  # Сохраняем закрывающий тег и индекс
    
    # Закрываем все оставшиеся теги в конце
    for tag_close, _ in reversed(active_tags):
        result.append(tag_close)
    
    final_text = "".join(result)
    logger.debug(f"[MARKUP] Output: '{final_text[:200]}{'...' if len(final_text)>200 else ''}'")
    return final_text

# ===================================================================
# 🔎 ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ СООБЩЕНИЯ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    logger.debug(f"[EXTRACT] Input msg keys: {list(msg.keys())}")
    
    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    if body and ("text" in body or "attachments" in body or "markup" in body or "mid" in body):
        logger.debug("[EXTRACT] Using body")
        return {
            "source": "body",
            "mid": body.get("mid"),
            "text": body.get("text", ""),
            "markup": body.get("markup", []),
            "attachments": safe_list(body.get("attachments") or body.get("files") or body.get("media"))
        }
    
    link = msg.get("link", {}) if isinstance(msg.get("link"), dict) else {}
    if link and isinstance(link.get("message"), dict):
        link_msg = link["message"]
        logger.debug("[EXTRACT] Using link.message")
        return {
            "source": "link.message",
            "mid": link_msg.get("mid"),
            "text": link_msg.get("text", ""),
            "markup": link_msg.get("markup", []),
            "attachments": safe_list(link_msg.get("attachments") or link_msg.get("files") or link_msg.get("media"))
        }
    
    logger.debug("[EXTRACT] Using root message")
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id") or msg.get("_id"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files") or msg.get("media"))
    }

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
        self.MAX_MEDIA_GROUP = 10

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180))

    async def _request_with_retry(self, method: str, max_retries: int = MAX_RETRIES, **kwargs) -> Tuple[bool, str]:
        url = f"{self.base}/{method}"
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                await self.init()
                async with self.session.post(url, **kwargs) as r:
                    response_text = await r.text()
                    if r.status == 200:
                        return True, f"OK: {response_text[:100]}"
                    logger.error(f"❌ TG {method} (attempt {attempt+1}) {r.status}: {response_text[:300]}")
                    last_error = f"{r.status}: {response_text[:200]}"
                    if 400 <= r.status < 500 and r.status != 429:
                        break
                    if attempt < max_retries:
                        wait = 2 ** attempt
                        logger.info(f"⏳ Retrying {method} in {wait}s...")
                        await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"❌ TG {method} exception (attempt {attempt+1}): {e}")
                last_error = str(e)
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
        
        return False, f"Failed after {max_retries+1} attempts: {last_error}"

    async def send_text(self, text: str) -> bool:
        if not text: return True
        logger.debug(f"[TG-REQ] sendMessage: text_len={len(text)}")
        ok, resp = await self._request_with_retry("sendMessage", json={
            "chat_id": self.chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": False
        })
        logger.info(f"[TG-RESP] sendMessage: {'✅' if ok else '❌'} | {resp[:100]}")
        return ok

    async def send_media_group(self, media_list: List[Dict], caption: str = "") -> bool:
        if not media_list: return False
        logger.debug(f"[TG-REQ] sendMediaGroup: count={len(media_list)}")
        for i, media in enumerate(media_list):
            if i == 0 and caption:
                media["caption"] = caption[:self.MAX_CAPTION]
                media["parse_mode"] = "HTML"
            else:
                media.pop("caption", None)
                media.pop("parse_mode", None)
        ok, resp = await self._request_with_retry("sendMediaGroup", json={"chat_id": self.chat_id, "media": media_list})
        logger.info(f"[TG-RESP] sendMediaGroup: {'✅' if ok else '❌'} | {resp[:100]}")
        return ok

    async def _send_single_media(self, method: str,  bytes, filename: str, caption: str = "") -> bool:
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ {method} >50MB skipped: {filename}")
            return False
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field(method.lower().replace("send", "", 1), data, filename=filename)
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        logger.debug(f"[TG-REQ] {method}: {filename}")
        ok, resp = await self._request_with_retry(method, data=form)
        logger.info(f"[TG-RESP] {method}: {'✅' if ok else '❌'} | {resp[:100]}")
        return ok

    async def send_photo(self,  bytes, caption: str = "") -> bool:
        return await self._send_single_media("sendPhoto", data, "photo.jpg", caption)
    async def send_video(self,  bytes, caption: str = "", filename: str = "video.mp4") -> bool:
        return await self._send_single_media("sendVideo", data, filename, caption)
    async def send_audio(self,  bytes, caption: str = "", filename: str = "audio.mp3") -> bool:
        return await self._send_single_media("sendAudio", data, filename, caption)
    async def send_voice(self,  bytes, caption: str = "") -> bool:
        return await self._send_single_media("sendVoice", data, "voice.ogg", caption)
    async def send_document(self,  bytes, caption: str = "", filename: str = "file.dat") -> bool:
        return await self._send_single_media("sendDocument", data, filename, caption)

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
            async with self.session.get(f"{self.base}/messages", headers={"Authorization": self.token}, params={"chat_id": self.cid, "limit": limit}) as r:
                if r.status == 200:
                    raw = await r.json()
                    logger.debug(f"[MAX] Raw: {json.dumps(raw, ensure_ascii=False)[:800]}")
                    return safe_list(raw)
                logger.error(f"❌ MAX fetch HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"❌ MAX fetch exception: {e}")
            return []

    async def download(self, token: str, photo_id: str = None, max_retries: int = MAX_RETRIES) -> Optional[bytes]:
        await self.init()
        logger.info(f"[DOWNLOAD-ATTEMPT] token:{token[:30]}... photo_id:{photo_id}")
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                async with self.session.get(f"{self.base}/files/{token}/download", headers={"Authorization": self.token}) as r:
                    if r.status == 200:
                        data = await r.read()
                        logger.info(f"[DOWNLOAD-RESULT] ✅ {len(data)} bytes")
                        return data
                    err = await r.text()
                    logger.warning(f"⚠️ MAX download HTTP {r.status} (attempt {attempt+1}): {err[:200]}")
                    last_error = f"HTTP {r.status}: {err[:100]}"
                    if 400 <= r.status < 500 and r.status != 429:
                        break
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"❌ MAX download exception (attempt {attempt+1}): {e}")
                last_error = str(e)
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
        
        logger.error(f"[DOWNLOAD-RESULT] ❌ Failed: {last_error}")
        return None

# ===================================================================
# 6. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg = TelegramClient(TG_TOKEN, TG_CHAT)
mx = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle_message(msg: Dict):
    global _sent_count
    logger.info(f"[HANDLE] ▶️ START")
    logger.info(f"[HANDLE] Full msg: {json.dumps(msg, ensure_ascii=False)[:800]}")
    
    h = get_hash(msg)
    logger.info(f"[HASH] mid hash: '{h}'")
    if not h:
        logger.warning("[SKIP] Could not extract mid")
        return
    if h in _processed:
        logger.info(f"[DUPE] Already processed: {h}")
        return
    _processed.add(h)
    logger.info(f"[CACHE] Added hash: {h} (size: {len(_processed)})")

    data = extract_message_data(msg)
    logger.info(f"[EXTRACT] Source: {data['source']} | mid: {data['mid']}")
    
    raw_text = data["text"]
    markup = data["markup"]
    attachments = data["attachments"]
    
    logger.info(f"[EXTRACT] Raw text: '{raw_text[:150]}{'...' if len(raw_text)>150 else ''}'")
    logger.info(f"[EXTRACT-MARKUP] source:{data['source']} | markup_count:{len(markup)}")
    logger.info(f"[EXTRACT] Attachments count: {len(attachments)}")
    
    if markup:
        text = convert_markup_to_html(raw_text, markup)
        logger.info(f"[MARKUP] Converted: '{raw_text[:50]}...' → '{text[:50]}...'")
    else:
        text = raw_text
    
    logger.info(f"📨 MSG | hash:{h} | text_len:{len(text)} | att_count:{len(attachments)}")

    if not text and not attachments:
        logger.info(f"[SKIP] Empty message")
        return

    # Подготовка медиа
    album_media = []
    standalone_files = []
    
    for i, att in enumerate(attachments):
        logger.info(f"[ATT #{i+1}] ▶️ Processing")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:400]}")
        
        if not isinstance(att, dict):
            logger.warning(f"[SKIP] Attachment #{i+1} not dict")
            continue
        
        att_type = safe_str(att.get("type") or att.get("media_type") or att.get("mime_type") or "file")
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        # 🔹 Глубокий поиск токена
        token = safe_str(
            att.get("token") or payload.get("token") or att.get("file_token") or 
            att.get("id") or att.get("file_id") or payload.get("file_id") or payload.get("url")
        )
        photo_id = payload.get("photo_id") or att.get("photo_id")
        filename = safe_str(att.get("name") or att.get("filename") or att.get("file_name") or payload.get("filename") or f"file_{i+1}")
        
        logger.info(f"[ATT] type:{att_type} | token:{token[:40]}... | photo_id:{photo_id} | name:{filename}")
        
        if not token:
            logger.warning(f"[SKIP] No token found")
            logger.debug(f"[ATT] Available keys: {list(att.keys())}")
            if payload: logger.debug(f"[ATT] Payload keys: {list(payload.keys())}")
            continue

        logger.info(f"[DOWNLOAD] Starting: {filename}")
        file_data = await mx.download(token, photo_id)
        
        if file_data is None or len(file_data) == 0:
            logger.error(f"[SKIP] Download failed: {filename}")
            continue
        
        logger.info(f"[DOWNLOADED] ✅ {filename}: {len(file_data)} bytes")

        tg_type = guess_media_type(filename, att_type, payload)
        logger.info(f"[MEDIA-TYPE] Detected: {tg_type}")

        media_item = {"type": tg_type, "media": f"attach://{filename}", "filename": filename, "data": file_data}
        if tg_type in ("photo", "video") and len(album_media) < tg.MAX_MEDIA_GROUP:
            album_media.append(media_item)
            logger.info(f"[ALBUM] Added: {filename}")
        else:
            standalone_files.append((tg_type, file_data, filename))
            logger.info(f"[STANDALONE] Queued: {filename}")
        
        await asyncio.sleep(0.1)

    # Отправка альбома
    if album_media:
        logger.info(f"[SEND-ALBUM] ▶️ Sending {len(album_media)} media")
        caption = text if text else ""
        media_group = [{"type": item["type"], "media": f"attach://{item['filename']}"} for item in album_media]
        if caption:
            media_group[0]["caption"] = caption[:tg.MAX_CAPTION]
            media_group[0]["parse_mode"] = "HTML"
        
        form = aiohttp.FormData()
        form.add_field("chat_id", tg.chat_id)
        form.add_field("media", json.dumps(media_group))
        for item in album_media:
            form.add_field(f"attach://{item['filename']}", item["data"], filename=item["filename"])
        
        ok, resp = await tg._request_with_retry("sendMediaGroup", data=form)
        logger.info(f"[SEND-ALBUM] {'✅ OK' if ok else '❌ FAIL'} | {resp[:100]}")
        if ok:
            _sent_count += 1
            text = ""  # Не дублировать текст
        await asyncio.sleep(0.3)

    # Отправка текста и файлов
    if text:
        logger.info(f"[SEND-TEXT] ▶️ '{text[:100]}...'")
        ok = await tg.send_text(text)
        if ok: _sent_count += 1
        logger.info(f"[SEND-TEXT] {'✅ OK' if ok else '❌ FAIL'}")
        await asyncio.sleep(0.2)
    
    for tg_type, file_data, filename in standalone_files:
        logger.info(f"[SEND-{tg_type.upper()}] ▶️ {filename}")
        caption = text if tg_type != "document" else ""
        sent = False
        if tg_type == "photo": sent = await tg.send_photo(file_data, caption)
        elif tg_type == "video": sent = await tg.send_video(file_data, caption, filename)
        elif tg_type == "audio": sent = await tg.send_audio(file_data, caption, filename)
        elif tg_type == "voice": sent = await tg.send_voice(file_data, caption)
        else: sent = await tg.send_document(file_data, caption if not text else "", filename)
        
        if sent: _sent_count += 1
        logger.info(f"[SEND-{tg_type.upper()}] {'✅ OK' if sent else '❌ FAIL'}")
        await asyncio.sleep(0.3)

    logger.info(f"✅ [HANDLE] DONE: {data['mid']} (hash:{h}) | sent_count:{_sent_count}")

# ===================================================================
# 7. POLLING + SERVER
# ===================================================================
async def polling_loop():
    logger.info("🔄 Starting polling...")
    await asyncio.sleep(2)
    init_msgs = await mx.fetch(limit=50)
    for m in init_msgs:
        h = get_hash(m)
        if h: _processed.add(h)
    logger.info(f"📦 Cached {len(_processed)} hashes at startup")
    
    i = 0
    while True:
        i += 1
        logger.debug(f"[POLL] #{i}")
        try:
            msgs = await mx.fetch(limit=1)
            if msgs:
                logger.info(f"[POLL] Processing...")
                await handle_message(msgs[0])
        except Exception as e:
            logger.error(f"❌ Loop error: {e}", exc_info=True)
        await asyncio.sleep(POLL_SEC)

async def health_handler(request):
    return web.json_response({"status": "ok", "cached": len(_processed), "sent": _sent_count, "poll_sec": POLL_SEC})

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
        logger.info("🚀 Starting...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 Fatal: {e}")
        sys.exit(1)
