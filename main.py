# -*- coding: utf-8 -*-
import os, sys, asyncio, logging, aiohttp, json, hashlib, mimetypes, re
from aiohttp import web
from typing import List, Dict, Optional, Any, Union

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
MAX_RETRIES = int(os.getenv('MAX_RETRIES', '1'))  # Повторные попытки при ошибке

_processed = set()  # Кэш хэшей для защиты от дублей
_retry_cache = {}   # Кэш для повторных попыток

logger.info("=" * 100)
logger.info("🚀 MAX → TG FORWARDER [IDENTICAL POST MODE]")
logger.info(f"📡 Channel: {MAX_CHAN} | 📥 Chat: {TG_CHAT}")
logger.info(f"🔗 API: {MAX_BASE} | ⏱️ Poll: {POLL_SEC}s | 🔁 Retries: {MAX_RETRIES}")
logger.info("🔒 Hash: mid-only | 🎨 Markup: ALL types + logging | 🖼️ Albums: sendMediaGroup")
logger.info("🎵 Audio/Voice: by extension | 📄 Docs: separate | ⚠️ Errors: retry + log")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ MISSING ENV VARS! Check Render settings.")
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
    """Стабильный хэш ТОЛЬКО по mid (игнорирует views, timestamp, stat)"""
    body = msg.get("body", {}) if isinstance(msg.get("body"), dict) else {}
    link_msg = msg.get("link", {}).get("message", {}) if isinstance(msg.get("link"), dict) else {}
    mid = body.get("mid") or link_msg.get("mid") or msg.get("id") or msg.get("message_id") or msg.get("_id") or ""
    return hashlib.md5(str(mid).encode()).hexdigest()[:12] if mid else ""

def guess_media_type(filename: str, att_type: str, payload: Dict = None) -> str:
    """
    Определяет тип медиа для Telegram API.
    Приоритет: явный тип → расширение файла → эвристика.
    """
    att_type = (att_type or "").lower()
    ext = filename.split('.')[-1].lower() if '.' in filename else ""
    
    # Явные типы от MAX
    if att_type in ("photo", "image", "picture"): return "photo"
    if att_type == "video": return "video"
    if att_type == "voice": return "voice"
    
    # Аудио: определяем по расширению или полю в payload
    if att_type == "audio":
        if ext in ("ogg", "opus", "oga"): return "voice"  # Голосовые обычно в ogg/opus
        return "audio"  # Музыка/подкасты
    
    # Определение по расширению
    if ext in ("jpg", "jpeg", "png", "gif", "webp", "bmp", "tiff", "heic"): return "photo"
    if ext in ("mp4", "mov", "avi", "mkv", "webm", "flv", "m4v", "3gp"): return "video"
    if ext in ("ogg", "opus", "oga"): return "voice"
    if ext in ("mp3", "wav", "m4a", "flac", "aac", "wma", "aiff"): return "audio"
    
    # По умолчанию — документ
    return "document"

# ===================================================================
# 🔎 КОНВЕРТАЦИЯ MARKUP → HTML (ВСЕ ТИПЫ + ПОЛНОЕ ЛОГИРОВАНИЕ)
# ===================================================================
def convert_markup_to_html(text: str, markup: List[Dict]) -> str:
    """
    Конвертирует разметку MAX в HTML для Telegram.
    Поддерживает ВСЕ известные типы + логирует неизвестные.
    """
    if not markup or not text:
        return text or ""
    
    logger.debug(f"[MARKUP] Input: text_len={len(text)}, markup_count={len(markup)}")
    logger.debug(f"[MARKUP] Raw markup: {json.dumps(markup, ensure_ascii=False)[:500]}")
    
    # Сортируем по позиции (от конца к началу, чтобы не ломать индексы при вставке тегов)
    sorted_markup = sorted(markup, key=lambda x: (x.get("from", 0), -x.get("length", 0)), reverse=True)
    result = text
    unknown_types = set()
    
    for i, m in enumerate(sorted_markup):
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length
            
            # Валидация границ
            if start < 0 or end > len(result) or length <= 0:
                logger.warning(f"[MARKUP] Invalid range #{i+1}: from={start}, length={length}, text_len={len(result)}")
                continue
            
            # Определяем теги — ПОЛНЫЙ ПЕРЕБОР ВСЕХ ИЗВЕСТНЫХ ТИПОВ
            if mtype == "strong" or mtype == "bold":
                tag_open, tag_close = "<b>", "</b>"
            elif mtype == "italic" or mtype == "em":
                tag_open, tag_close = "<i>", "</i>"
            elif mtype == "code" or mtype == "inline-code":
                tag_open, tag_close = "<code>", "</code>"
            elif mtype == "pre" or mtype == "preformatted":
                tag_open, tag_close = "<pre>", "</pre>"
            elif mtype == "underline" or mtype == "u":
                tag_open, tag_close = "<u>", "</u>"
            elif mtype == "strikethrough" or mtype == "s" or mtype == "del":
                tag_open, tag_close = "<s>", "</s>"
            elif mtype == "link" or mtype == "url":
                url = m.get("url") or m.get("href") or ""
                if url:
                    url = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    tag_open, tag_close = f'<a href="{url}">', "</a>"
                else:
                    logger.warning(f"[MARKUP] Link without URL: {m}")
                    continue
            elif mtype == "mention" or mtype == "mention_name":
                # Telegram не поддерживает именованные упоминания через HTML, пропускаем
                logger.debug(f"[MARKUP] Skipping mention (not supported in HTML mode): {m}")
                continue
            elif mtype == "hashtag" or mtype == "bot_command" or mtype == "cashtag":
                # Эти типы не требуют тегов в Telegram
                logger.debug(f"[MARKUP] Skipping {mtype} (no tag needed): {m}")
                continue
            else:
                # Неизвестный тип — логируем и пропускаем
                if mtype not in unknown_types:
                    unknown_types.add(mtype)
                    logger.warning(f"[MARKUP] Unknown type '{mtype}': {m}")
                continue
            
            # Вставляем теги
            original = result[start:end]
            result = result[:start] + tag_open + original + tag_close + result[end:]
            logger.debug(f"[MARKUP] Applied #{i+1}: '{original}' → '{tag_open}{original}{tag_close}' (type:{mtype})")
            
        except Exception as e:
            logger.error(f"[MARKUP] Error processing item #{i+1}: {e} | markup: {m}", exc_info=True)
            continue
    
    if unknown_types:
        logger.info(f"[MARKUP] Summary: {len(sorted_markup)} items processed, {len(unknown_types)} unknown types: {unknown_types}")
    
    logger.debug(f"[MARKUP] Output: '{result[:200]}{'...' if len(result)>200 else ''}'")
    return result

# ===================================================================
# 🔎 ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ СООБЩЕНИЯ (body / link.message / root)
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    """
    Извлекает text, markup, attachments, mid из сообщения.
    Поддерживает:
    - Обычные: данные в msg.body
    - Форварды: данные в msg.link.message
    - Резерв: данные в корне msg
    """
    logger.debug(f"[EXTRACT] Input msg keys: {list(msg.keys())}")
    
    # 1. Пробуем body (обычное сообщение)
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
    
    # 2. Пробуем link.message (форвард/пересланное)
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
    
    # 3. Пробуем корень сообщения (резервный вариант)
    logger.debug("[EXTRACT] Using root message")
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id") or msg.get("_id"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files") or msg.get("media"))
    }

# ===================================================================
# 4. TELEGRAM CLIENT (ОТПРАВКА С ПОВТОРНЫМИ ПОПЫТКАМИ)
# ===================================================================
class TelegramClient:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session: Optional[aiohttp.ClientSession] = None
        self.MAX_BYTES = 50 * 1024 * 1024  # 50 МБ — лимит Telegram Bot API
        self.MAX_CAPTION = 1024             # Лимит подписи
        self.MAX_MEDIA_GROUP = 10           # Макс. медиа в одном альбоме

    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=180))

    async def _request_with_retry(self, method: str, max_retries: int = MAX_RETRIES, **kwargs) -> bool:
        """Запрос с повторными попытками при ошибке"""
        url = f"{self.base}/{method}"
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                await self.init()
                async with self.session.post(url, **kwargs) as r:
                    if r.status == 200:
                        return True
                    err = await r.text()
                    logger.error(f"❌ TG {method} (attempt {attempt+1}/{max_retries+1}) {r.status}: {err[:300]}")
                    last_error = f"{r.status}: {err[:200]}"
                    
                    # Не повторяем при клиентских ошибках (400-499), кроме 429 (rate limit)
                    if 400 <= r.status < 500 and r.status != 429:
                        break
                    if attempt < max_retries:
                        wait = 2 ** attempt  # Экспоненциальная задержка: 1s, 2s, 4s...
                        logger.info(f"⏳ Retrying in {wait}s...")
                        await asyncio.sleep(wait)
            except Exception as e:
                logger.error(f"❌ TG {method} exception (attempt {attempt+1}): {e}")
                last_error = str(e)
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
        
        logger.error(f"❌ TG {method} failed after {max_retries+1} attempts: {last_error}")
        return False

    async def send_text(self, text: str) -> bool:
        if not text: return True
        logger.debug(f"[TG-REQ] sendMessage: text_len={len(text)}, chat_id={self.chat_id}")
        return await self._request_with_retry("sendMessage", json={
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        })

    async def send_media_group(self, media_list: List[Dict], caption: str = "") -> bool:
        """Отправка альбома (до 10 фото/видео в одном сообщении)"""
        if not media_list: return False
        logger.debug(f"[TG-REQ] sendMediaGroup: count={len(media_list)}, caption_len={len(caption) if caption else 0}")
        
        # Первая медиа получает подпись, остальные — нет (требование Telegram API)
        for i, media in enumerate(media_list):
            if i == 0 and caption:
                media["caption"] = caption[:self.MAX_CAPTION]
                media["parse_mode"] = "HTML"
            else:
                # Убираем caption у остальных, чтобы не дублировать
                media.pop("caption", None)
                media.pop("parse_mode", None)
        
        return await self._request_with_retry("sendMediaGroup", json={
            "chat_id": self.chat_id,
            "media": media_list
        })

    async def _send_single_media(self, method: str,  bytes, filename: str, caption: str = "") -> bool:
        """Вспомогательный метод для отправки одного медиа-файла"""
        if len(data) > self.MAX_BYTES:
            logger.warning(f"⚠️ {method} >50MB skipped: {filename} ({len(data)/1024/1024:.2f} МБ)")
            return False
        
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        form.add_field(method.lower().replace("send", "", 1), data, filename=filename)
        
        if caption:
            form.add_field("caption", caption[:self.MAX_CAPTION])
            form.add_field("parse_mode", "HTML")
        
        logger.debug(f"[TG-REQ] {method}: {filename}, caption={bool(caption)}")
        return await self._request_with_retry(method, data=form)

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
# 5. MAX CLIENT (ПОЛУЧЕНИЕ + СКАЧИВАНИЕ С ПОВТОРНЫМИ ПОПЫТКАМИ)
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
                    logger.debug(f"[MAX] Raw response keys: {list(raw.keys()) if isinstance(raw, dict) else 'list'}")
                    logger.debug(f"[MAX] Raw: {json.dumps(raw, ensure_ascii=False)[:800]}")
                    return safe_list(raw)
                logger.error(f"❌ MAX fetch HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"❌ MAX fetch exception: {e}")
            return []

    async def download(self, token: str, max_retries: int = MAX_RETRIES) -> Optional[bytes]:
        """Скачивание файла с повторными попытками"""
        await self.init()
        logger.debug(f"[MAX-DL] Requesting token: {token[:30]}...")
        last_error = None
        
        for attempt in range(max_retries + 1):
            try:
                async with self.session.get(
                    f"{self.base}/files/{token}/download",
                    headers={"Authorization": self.token}
                ) as r:
                    if r.status == 200:
                        data = await r.read()
                        logger.debug(f"[MAX-DL] Success: {len(data)} bytes")
                        return data
                    logger.warning(f"⚠️ MAX download HTTP {r.status} (attempt {attempt+1})")
                    last_error = f"HTTP {r.status}"
                    
                    if 400 <= r.status < 500 and r.status != 429:
                        break
                    if attempt < max_retries:
                        await asyncio.sleep(2 ** attempt)
            except Exception as e:
                logger.error(f"❌ MAX download exception (attempt {attempt+1}): {e}")
                last_error = str(e)
                if attempt < max_retries:
                    await asyncio.sleep(2 ** attempt)
        
        logger.error(f"❌ MAX download failed after {max_retries+1} attempts: {last_error}")
        return None

# ===================================================================
# 6. ОБРАБОТКА СООБЩЕНИЙ (МАКСИМУМ ЛОГОВ + АЛЬБОМЫ + ПОВТОРЫ)
# ===================================================================
tg = TelegramClient(TG_TOKEN, TG_CHAT)
mx = MaxClient(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle_message(msg: Dict):
    logger.info(f"[HANDLE] ▶️ START")
    logger.info(f"[HANDLE] Full msg: {json.dumps(msg, ensure_ascii=False)[:800]}")
    
    # 🔒 Хэш для защиты от дублей
    h = get_hash(msg)
    logger.info(f"[HASH] mid hash: '{h}'")
    if not h:
        logger.warning("[SKIP] Could not extract mid for hashing")
        return
    
    if h in _processed:
        logger.info(f"[DUPE] Already processed: {h}")
        return
    
    _processed.add(h)
    logger.info(f"[CACHE] Added hash: {h} (size: {len(_processed)})")

    # 🔹 Извлечение данных (body / link.message / root)
    data = extract_message_data(msg)
    logger.info(f"[EXTRACT] Source: {data['source']} | mid: {data['mid']}")
    
    raw_text = data["text"]
    markup = data["markup"]
    attachments = data["attachments"]
    
    logger.info(f"[EXTRACT] Raw text: '{raw_text[:150]}{'...' if len(raw_text)>150 else ''}'")
    logger.info(f"[EXTRACT] Markup count: {len(markup)}")
    logger.info(f"[EXTRACT] Attachments count: {len(attachments)}")
    
    # 🔹 Конвертация markup → HTML
    if markup:
        text = convert_markup_to_html(raw_text, markup)
        logger.info(f"[MARKUP] Converted: '{raw_text[:100]}...' → '{text[:100]}...'")
    else:
        text = raw_text
    
    logger.info(f"📨 MSG | hash:{h} | text_len:{len(text)} | att_count:{len(attachments)}")

    # 🔹 Пропуск, если нет ни текста, ни вложений
    if not text and not attachments:
        logger.info(f"[SKIP] Empty message (no text, no attachments)")
        return

    # ===================================================================
    # ПОДГОТОВКА: РАЗДЕЛЕНИЕ МЕДИА НА АЛЬБОМ И ОТДЕЛЬНЫЕ ФАЙЛЫ
    # ===================================================================
    album_media = []  # Для sendMediaGroup (только photo/video)
    standalone_files = []  # Для отдельной отправки (документы, аудио, голосовые)
    
    for i, att in enumerate(attachments):
        logger.info(f"[ATT #{i+1}/{len(attachments)}] ▶️ Processing attachment")
        logger.debug(f"[ATT] Raw att: {json.dumps(att, ensure_ascii=False)[:400]}")
        
        if not isinstance(att, dict):
            logger.warning(f"[SKIP] Attachment #{i+1} not dict: {type(att)}")
            continue
        
        att_type = safe_str(att.get("type") or att.get("media_type") or att.get("mime_type") or "file")
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        # 🔹 Извлечение токена: ищем во всех возможных местах
        token = safe_str(
            att.get("token") or 
            payload.get("token") or 
            att.get("file_token") or 
            att.get("id") or 
            att.get("file_id") or
            payload.get("file_id") or
            payload.get("url")  # Резерв: если это прямая ссылка
        )
        
        filename = safe_str(
            att.get("name") or 
            att.get("filename") or 
            att.get("file_name") or 
            payload.get("filename") or 
            f"file_{i+1}"
        )
        
        logger.info(f"[ATT] type:{att_type} | token:{token[:50]}... | name:{filename}")
        
        if not token:
            logger.warning(f"[SKIP] No token found in attachment #{i+1}")
            logger.debug(f"[ATT] Available keys: {list(att.keys())}")
            if payload:
                logger.debug(f"[ATT] Payload keys: {list(payload.keys())}")
            continue

        # 🔹 Скачивание файла
        logger.info(f"[DOWNLOAD] ▶️ Starting: {filename}")
        file_data = await mx.download(token)
        
        if file_data is None:
            logger.error(f"[SKIP] Download returned None: {filename}")
            continue
        if len(file_data) == 0:
            logger.error(f"[SKIP] Download returned empty: {filename}")
            continue
        
        logger.info(f"[DOWNLOADED] ✅ {filename}: {len(file_data)} bytes")

        # 🔹 Определение типа для Telegram
        tg_type = guess_media_type(filename, att_type, payload)
        logger.info(f"[MEDIA-TYPE] Detected: {tg_type} (att_type:{att_type}, ext:{filename.split('.')[-1] if '.' in filename else 'none'})")

        # 🔹 Подготовка медиа для отправки
        media_item = {
            "type": tg_type,
            "media": f"attach://{filename}",  # Специальный формат для sendMediaGroup
            "filename": filename,
            "data": file_data
        }
        
        # Только photo/video могут быть в альбоме
        if tg_type in ("photo", "video") and len(album_media) < tg.MAX_MEDIA_GROUP:
            album_media.append(media_item)
            logger.info(f"[ALBUM] Added to album: {filename} (type:{tg_type})")
        else:
            standalone_files.append((tg_type, file_data, filename))
            logger.info(f"[STANDALONE] Queued for separate send: {filename} (type:{tg_type})")
        
        # Микро-задержка между скачиваниями, чтобы не перегружать MAX API
        await asyncio.sleep(0.1)

    # ===================================================================
    # ОТПРАВКА: АЛЬБОМ (ЕСЛИ ЕСТЬ)
    # ===================================================================
    if album_media:
        logger.info(f"[SEND-ALBUM] ▶️ Sending {len(album_media)} media as album")
        
        # Подпись: текст только для первого медиа в альбоме
        caption = text if text else ""
        
        # Формируем список для sendMediaGroup (только необходимые поля)
        media_group = []
        for item in album_media:
            media_entry = {"type": item["type"], "media": f"attach://{item['filename']}"}
            media_group.append(media_entry)
        
        # Подготовка FormData для sendMediaGroup
        form = aiohttp.FormData()
        form.add_field("chat_id", tg.chat_id)
        form.add_field("media", json.dumps(media_group))
        
        # Добавляем файлы
        for item in album_media:
            form.add_field(f"attach://{item['filename']}", item["data"], filename=item["filename"])
        
        # Добавляем подпись к первому медиа
        if caption:
            first_media = media_group[0]
            first_media["caption"] = caption[:tg.MAX_CAPTION]
            first_media["parse_mode"] = "HTML"
            # Обновляем JSON в form
            form._fields[1] = ("media", json.dumps(media_group), "application/json")
        
        sent = await tg._request_with_retry("sendMediaGroup", data=form)
        logger.info(f"[SEND-ALBUM] {'✅ OK' if sent else '❌ FAIL'}")
        
        # Если альбом отправлен, очищаем текст, чтобы не дублировать его отдельно
        if sent:
            text = ""
        
        # Микро-задержка после отправки альбома
        await asyncio.sleep(0.3)

    # ===================================================================
    # ОТПРАВКА: ОТДЕЛЬНЫЕ ФАЙЛЫ + ТЕКСТ
    # ===================================================================
    
    # Сначала отправляем текст (если он ещё не был использован как подпись к альбому)
    if text:
        logger.info(f"[SEND-TEXT] ▶️ Sending: '{text[:150]}{'...' if len(text)>150 else ''}'")
        sent = await tg.send_text(text)
        logger.info(f"[SEND-TEXT] {'✅ OK' if sent else '❌ FAIL'}")
        # Микро-задержка после текста
        await asyncio.sleep(0.2)
    
    # Затем отправляем остальные файлы по одному
    for tg_type, file_data, filename in standalone_files:
        logger.info(f"[SEND-{tg_type.upper()}] ▶️ {filename}")
        caption = text if tg_type != "document" else ""  # Документы без подписи, если текст уже отправлен
        
        sent = False
        if tg_type == "photo":
            sent = await tg.send_photo(file_data, caption)
        elif tg_type == "video":
            sent = await tg.send_video(file_data, caption, filename)
        elif tg_type == "audio":
            sent = await tg.send_audio(file_data, caption, filename)
        elif tg_type == "voice":
            sent = await tg.send_voice(file_data, caption)
        else:  # document
            sent = await tg.send_document(file_data, caption if not text else "", filename)
        
        logger.info(f"[SEND-{tg_type.upper()}] {'✅ OK' if sent else '❌ FAIL'}: {filename}")
        
        # 🔹 Микро-задержка между файлами (защита от бана за спам)
        await asyncio.sleep(0.3)

    logger.info(f"✅ [HANDLE] DONE: {data['mid']} (hash:{h}) | album:{len(album_media)} standalone:{len(standalone_files)}")

# ===================================================================
# 7. POLLING LOOP
# ===================================================================
async def polling_loop():
    logger.info("🔄 Starting polling loop...")
    logger.info("⏳ Sync: caching recent messages to avoid duplicates...")
    await asyncio.sleep(2)  # Ждём готовности сети Render
    
    # Синхронизация: кешируем последние сообщения, чтобы не пересылать старое
    init_msgs = await mx.fetch(limit=50)
    for m in init_msgs:
        h = get_hash(m)
        if h:
            _processed.add(h)
            logger.debug(f"[STARTUP-CACHE] Cached: {h}")
    logger.info(f"📦 Cached {len(_processed)} message hashes at startup")

    poll_count = 0
    while True:
        poll_count += 1
        logger.debug(f"[POLL] Iteration #{poll_count}")
        
        try:
            # Берём только самое новое сообщение
            messages = await mx.fetch(limit=1)
            logger.debug(f"[POLL] Received {len(messages)} messages")
            
            if messages:
                logger.info(f"[POLL] Processing newest message...")
                await handle_message(messages[0])
            else:
                logger.debug("[POLL] No new messages")
                
        except Exception as e:
            logger.error(f"❌ Polling loop error: {e}", exc_info=True)
            # Не прерываем цикл при ошибке — продолжаем опрос
        
        # 🔹 Интервал опроса (1 секунда по умолчанию)
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 8. WEB SERVER (HEALTH CHECK ДЛЯ UPTIMEROBOT)
# ===================================================================
async def health_handler(request):
    return web.json_response({
        "status": "ok",
        "service": "max-to-telegram-forwarder",
        "cached_hashes": len(_processed),
        "poll_interval": POLL_SEC,
        "max_retries": MAX_RETRIES
    })

async def run_app():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🌐 Health server running on :8080 (UptimeRobot compatible)")
    
    # Запускаем polling
    await polling_loop()

# ===================================================================
# 9. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder [IDENTICAL POST MODE]...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
