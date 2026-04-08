# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
Полная версия с максимальным логированием
"""
import os, sys, asyncio, logging, aiohttp, json, time, re
from aiohttp import web
from typing import List, Dict, Optional, Any, Tuple
from collections import deque

# ===================================================================
# 1. ЛОГИРОВАНИЕ (МАКСИМУМ ДЕТАЛЕЙ — НЕ УДАЛЯТЬ)
# ===================================================================
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bot_debug.log', encoding='utf-8', mode='a')
    ]
)
logger = logging.getLogger(__name__)

# ===================================================================
# 2. ПЕРЕМЕННЫЕ ОКРУЖЕНИЯ + ДЕДУПЛИКАЦИЯ
# ===================================================================
TG_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '').strip()
TG_CHAT  = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN  = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE  = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru')
POLL_SEC  = int(os.getenv('POLL_INTERVAL', '15'))
DEBUG_IGNORE_SEQ = os.getenv('DEBUG_IGNORE_SEQ', '0') == '1'  # Для тестов: игнорировать проверку seq

_last_processed_seq = 0  # Последний обработанный seq для дедупликации

logger.info("=" * 100)
logger.info("🚀 MAX → TELEGRAM FORWARDER [FULL VERSION - MAX LOGGING]")
logger.info("=" * 100)
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 MAX API Base: {MAX_BASE}")
logger.info(f"⏱️  Poll Interval: {POLL_SEC}s")
logger.info(f"🔧 DEBUG_IGNORE_SEQ: {DEBUG_IGNORE_SEQ}")
logger.info("=" * 100)
logger.info("🔒 Deduplication: by seq (strict < comparison)")
logger.info("🎨 Markup: grapheme-aware, all types supported")
logger.info("📎 Media: URL priority for all types")
logger.info("🎤 Voice: .ogg/.opus OR size<2MB → sendVoice")
logger.info("📊 Logging: MAXIMUM detail at every step")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    logger.critical("  Required: TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, MAX_TOKEN, MAX_CHANNEL_ID")
    sys.exit(1)

logger.info("✅ All environment variables present")

# ===================================================================
# 3. ГРАФЕМНЫЙ СПЛИТТЕР (для корректной работы с эмодзи)
# ===================================================================
def split_into_graphemes(text: str) -> List[str]:
    """
    Разбивает текст на графемы (визуальные символы) для корректной работы с эмодзи.
    Эмодзи могут состоять из нескольких кодовых точек (например, 👨‍👩‍👧‍👦 или 🏻).
    """
    logger.debug(f"[GRAPH] Splitting text into graphemes: '{text[:50]}{'...' if len(text)>50 else ''}' (len={len(text)})")
    if not text:
        logger.debug("[GRAPH] Empty text, returning empty list")
        return []
    
    graphemes = []
    i = 0
    while i < len(text):
        char = text[i]
        # Проверяем, является ли символ эмодзи или модификатором
        if ('\U0001F300' <= char <= '\U0001F9FF' or  # Emoticons
            '\U00002600' <= char <= '\U000026FF' or  # Misc symbols
            '\U00002700' <= char <= '\U000027BF'):   # Dingbats
            # Собираем все модификаторы и ZWJ-последовательности
            j = i + 1
            while j < len(text):
                next_char = text[j]
                # Модификаторы тона кожи, ZWJ, вариационные селекторы
                if ('\U0001F3FB' <= next_char <= '\U0001F3FF' or  # Skin tone modifiers
                    next_char == '\u200D' or  # Zero-width joiner
                    next_char in '\uFE0E\uFE0F'):  # Variation selectors
                    char += next_char
                    j += 1
                else:
                    break
            graphemes.append(char)
            i = j
        else:
            graphemes.append(char)
            i += 1
    
    logger.debug(f"[GRAPH] Result: {len(graphemes)} graphemes")
    if len(graphemes) <= 20:
        logger.debug(f"[GRAPH] Graphemes: {graphemes}")
    return graphemes

# ===================================================================
# 4. КОНВЕРТЕР РАЗМЕТКИ MAX → HTML TELEGRAM
# ===================================================================
def apply_markup(text: str, markup: List[Dict]) -> str:
    """
    Конвертирует разметку MAX в HTML для Telegram.
    Поддерживает все типы форматирования и их комбинации.
    Использует графемный подход для корректной работы с эмодзи.
    """
    logger.info(f"[STEP] 🎨 Markup conversion START")
    logger.info(f"[MARKUP] Input text length: {len(text)} characters")
    logger.info(f"[MARKUP] Markup items count: {len(markup)}")
    logger.debug(f"[MARKUP] Input text preview: '{text[:100]}{'...' if len(text)>100 else ''}'")
    logger.debug(f"[MARKUP] Raw markup JSON: {json.dumps(markup, ensure_ascii=False, indent=2)[:500]}")
    
    if not markup:
        logger.debug("[MARKUP] No markup provided, returning text as-is")
        return text
    if not text:
        logger.debug("[MARKUP] Empty text, returning as-is")
        return text

    # Полный маппинг всех типов разметки MAX → HTML теги Telegram
    TAGS = {
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
        "strike": ("<s>", "</s>"),
        "s": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }

    # 🔹 Разбиваем текст на графемы для корректной работы с эмодзи
    graphemes = split_into_graphemes(text)
    n = len(graphemes)
    logger.info(f"[MARKUP] Text split into {n} graphemes")
    if n <= 50:
        logger.debug(f"[MARKUP] Graphemes: {graphemes}")

    # 🔹 Создаём события для каждого элемента разметки
    events = []
    for idx, m in enumerate(markup):
        try:
            start = int(m.get("from", 0))
            length = int(m.get("length", 0))
            mtype = m.get("type", "")
            end = start + length
            
            logger.debug(f"[MARKUP] Processing item #{idx+1}/{len(markup)}:")
            logger.debug(f"  ├─ type: '{mtype}'")
            logger.debug(f"  ├─ from: {start}")
            logger.debug(f"  ├─ length: {length}")
            logger.debug(f"  └─ end: {end}")
            
            # Проверяем валидность диапазона
            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range for item #{idx+1}: {start}-{end} (grapheme_count={n})")
                continue
            
            # Определяем теги по типу разметки
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                events.append((start, 'open', open_tag, idx))
                events.append((end, 'close', close_tag, idx))
                logger.debug(f"[MARKUP] Added events for '{mtype}' at positions {start} (open) and {end} (close)")
            elif mtype == "link":
                url = m.get("url") or m.get("href") or ""
                if url:
                    # Экранируем специальные символы в URL
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url_safe}">'
                    close_tag = "</a>"
                    events.append((start, 'open', open_tag, idx))
                    events.append((end, 'close', close_tag, idx))
                    logger.debug(f"[MARKUP] Added link events at {start}/{end}, URL: {url[:50]}...")
                else:
                    logger.warning(f"[MARKUP] Link type without URL, skipping item #{idx+1}")
                    continue
            elif mtype in ("mention", "hashtag", "bot_command", "cashtag"):
                logger.debug(f"[MARKUP] Skipping {mtype} (no HTML tag needed for Telegram)")
                continue
            else:
                logger.warning(f"[MARKUP] Unknown markup type: '{mtype}' in item #{idx+1}")
                logger.debug(f"[MARKUP] Full item: {json.dumps(m, ensure_ascii=False)}")
                continue
        except Exception as e:
            logger.error(f"[MARKUP] Exception processing item #{idx+1}: {e}", exc_info=True)
            logger.debug(f"[MARKUP] Problematic item: {json.dumps(m, ensure_ascii=False)}")
            continue

    # 🔹 Сортируем события: по позиции, закрывающие теги перед открывающими
    events.sort(key=lambda x: (x[0], 0 if x[1]=='close' else 1, -x[3]))
    logger.debug(f"[MARKUP] Sorted {len(events)} events")

    # 🔹 Обходим графемы, применяя стек тегов
    result = []
    active_tags = []  # Стек активных тегов: (close_tag, priority)
    event_idx = 0
    
    for pos in range(n + 1):
        # Обрабатываем все события на текущей позиции
        while event_idx < len(events) and events[event_idx][0] == pos:
            _, etype, tag, priority = events[event_idx]
            if etype == 'close':
                # Закрываем теги (ищем в стеке по приоритету)
                for i in range(len(active_tags) - 1, -1, -1):
                    if active_tags[i][1] == priority:
                        result.append(active_tags[i][0])
                        active_tags.pop(i)
                        break
            elif etype == 'open':
                # Открываем новый тег
                active_tags.append((tag, priority))
            event_idx += 1
        
        # Добавляем саму графему (если не конец текста)
        if pos < n:
            result.append(graphemes[pos])
    
    # Закрываем все оставшиеся открытые теги
    for tag_close, _ in reversed(active_tags):
        result.append(tag_close)
    
    final_text = "".join(result)
    logger.info(f"[STEP] ✅ Markup conversion DONE")
    logger.info(f"[MARKUP] Output length: {len(final_text)} characters")
    logger.debug(f"[MARKUP] Output preview: '{final_text[:100]}{'...' if len(final_text)>100 else ''}'")
    return final_text

# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ СООБЩЕНИЯ MAX
# ===================================================================
def extract_data(msg: Dict, depth: int = 0) -> Dict:
    """
    Извлекает текст, разметку и вложения из сообщения MAX.
    Поддерживает пересланные сообщения (форварды) с рекурсией.
    """
    indent = "  " * depth
    logger.info(f"[STEP] 📦 Data extraction START | depth={depth}")
    logger.debug(f"{indent}[PARSE] Message top-level keys: {list(msg.keys())}")
    
    # 🔹 Проверяем пересланное сообщение (link.message)
    link = msg.get("link")
    logger.info(f"{indent}[PARSE-FWD] Checking for forward: link_exists={link is not None}")
    if isinstance(link, dict):
        logger.info(f"{indent}[PARSE-FWD] link.keys() = {list(link.keys())}")
        logger.info(f"{indent}[PARSE-FWD] link.type = {link.get('type')}")
        if "message" in link:
            logger.info(f"{indent}[PARSE-FWD] ✅ FOUND FORWARD in link.message")
            inner = link["message"]
            logger.info(f"{indent}[PARSE-FWD] inner.keys() = {list(inner.keys())}")
            
            # Рекурсивная обработка вложенных форвардов (макс. глубина 3)
            if depth < 3 and "link" in inner and "message" in inner["link"]:
                logger.info(f"{indent}[PARSE-FWD] 🔄 Nested forward detected, recursing to depth {depth+1}")
                return extract_data(inner, depth + 1)
            
            # Извлекаем все поля из вложенного сообщения
            logger.info(f"{indent}[PARSE-FWD] Extracting data from inner message")
            return {
                "source": "link.message",
                "mid": inner.get("mid") or inner.get("id"),
                "seq": inner.get("seq"),
                "text": inner.get("text", ""),
                "markup": inner.get("markup", []),
                "attachments": safe_list(inner.get("attachments") or inner.get("files") or inner.get("media")),
                "original_chat_id": link.get("chat_id")
            }
    
    # 🔹 Обычное сообщение (body)
    body = msg.get("body", {})
    if isinstance(body, dict) and ("text" in body or "attachments" in body or "markup" in body):
        logger.info(f"{indent}[PARSE] 📄 Using body")
        return {
            "source": "body",
            "mid": body.get("mid"),
            "seq": body.get("seq"),
            "text": body.get("text", ""),
            "markup": body.get("markup", []),
            "attachments": safe_list(body.get("attachments") or body.get("files") or body.get("media"))
        }
    
    # 🔹 Резерв: корень сообщения
    logger.info(f"{indent}[PARSE] 📦 Using root (fallback)")
    return {
        "source": "root",
        "mid": msg.get("id") or msg.get("message_id"),
        "seq": msg.get("seq"),
        "text": msg.get("text", ""),
        "markup": msg.get("markup", []),
        "attachments": safe_list(msg.get("attachments") or msg.get("files") or msg.get("media"))
    }

def safe_list(val: Any) -> List[Dict]:
    """Гарантирует возврат списка словарей из различных форматов."""
    logger.debug(f"[SAFE_LIST] Input type: {type(val)}")
    if val is None:
        logger.debug("[SAFE_LIST] Value is None, returning []")
        return []
    if isinstance(val, list):
        result = [v for v in val if isinstance(v, dict)]
        logger.debug(f"[SAFE_LIST] Filtered list: {len(result)} items from {len(val)}")
        return result
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                logger.debug(f"[SAFE_LIST] Found key '{k}', returning as list")
                return v if isinstance(v, list) else [v]
    logger.debug("[SAFE_LIST] No match found, returning []")
    return []

# ===================================================================
# 6. TELEGRAM CLIENT С МАКСИМАЛЬНЫМ ЛОГИРОВАНИЕМ
# ===================================================================
class TG:
    """Клиент Telegram Bot API с полным логированием ответов."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
        self.MAX_BYTES = 50 * 1024 * 1024  # 50 MB лимит Telegram
        logger.info(f"[TG] Client initialized: chat_id={chat_id}")
    
    async def init(self):
        """Инициализирует HTTP сессию если нужно."""
        if not self.session or self.session.closed:
            logger.debug("[TG] Creating new aiohttp session")
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def send(self, method: str, **kw) -> bool:
        """
        Отправляет запрос к Telegram API с полным логированием.
        Логирует: запрос, время ответа, заголовки, тело ответа, результат.
        """
        await self.init()
        start_time = time.time()
        logger.info(f"[TG-REQ] ▶️ {method}")
        logger.debug(f"[TG-REQ] Parameters: {list(kw.keys())}")
        logger.debug(f"[TG-REQ] Full params (first 500 chars): {json.dumps(kw, default=str, ensure_ascii=False)[:500]}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                elapsed_ms = (time.time() - start_time) * 1000
                txt = await r.text()
                headers = dict(r.headers)
                
                # 🔹 Логирование базовой информации об ответе
                logger.info(f"[TG-RESP] Status: {r.status} | Duration: {elapsed_ms:.0f}ms | Body size: {len(txt)} bytes")
                
                # 🔹 Логирование важных заголовков
                important_headers = {
                    k: v for k, v in headers.items() 
                    if k.lower() in ['content-type', 'x-ratelimit-limit', 'x-ratelimit-remaining', 'retry-after']
                }
                logger.info(f"[TG-HEADERS] {json.dumps(important_headers, indent=2)}")
                
                # 🔹 МАКСИМАЛЬНОЕ ЛОГИРОВАНИЕ: ПОЛНЫЙ ОТВЕТ ОТ TELEGRAM
                logger.info(f"[TG-BODY] {txt}")
                
                # Парсим JSON ответа для структурированного лога
                try:
                    resp_json = json.loads(txt)
                    
                    if r.status == 200 and resp_json.get("ok"):
                        # ✅ Успешный ответ
                        result = resp_json.get("result", {})
                        msg_id = result.get("message_id")
                        chat = result.get("chat", {})
                        date = result.get("date")
                        
                        logger.info(f"[TG-RESULT] ✅ SUCCESS")
                        logger.info(f"  ├─ message_id: {msg_id}")
                        logger.info(f"  ├─ chat_id: {chat.get('id')}")
                        logger.info(f"  ├─ chat_type: {chat.get('type')}")
                        logger.info(f"  └─ date: {date}")
                        
                        # Логируем информацию о медиа если есть
                        if "photo" in result:
                            photos = result['photo']
                            largest = photos[-1] if photos else {}
                            logger.info(f"  ├─ media_type: photo")
                            logger.info(f"  ├─ photo_sizes: {len(photos)}")
                            logger.info(f"  ├─ largest_width: {largest.get('width')}")
                            logger.info(f"  ├─ largest_height: {largest.get('height')}")
                            logger.info(f"  └─ largest_file_size: {largest.get('file_size')} bytes")
                        
                        if "video" in result:
                            video = result['video']
                            logger.info(f"  ├─ media_type: video")
                            logger.info(f"  ├─ file_size: {video.get('file_size')} bytes")
                            logger.info(f"  ├─ duration: {video.get('duration')}s")
                            logger.info(f"  ├─ width: {video.get('width')}")
                            logger.info(f"  └─ height: {video.get('height')}")
                        
                        if "audio" in result:
                            audio = result['audio']
                            logger.info(f"  ├─ media_type: audio")
                            logger.info(f"  ├─ file_name: {audio.get('file_name')}")
                            logger.info(f"  ├─ file_size: {audio.get('file_size')} bytes")
                            logger.info(f"  └─ duration: {audio.get('duration')}s")
                        
                        if "voice" in result:
                            voice = result['voice']
                            logger.info(f"  ├─ media_type: voice")
                            logger.info(f"  ├─ file_name: {voice.get('file_name')}")
                            logger.info(f"  ├─ file_size: {voice.get('file_size')} bytes")
                            logger.info(f"  └─ duration: {voice.get('duration')}s")
                        
                        if "document" in result:
                            doc = result['document']
                            logger.info(f"  ├─ media_type: document")
                            logger.info(f"  ├─ file_name: {doc.get('file_name')}")
                            logger.info(f"  ├─ mime_type: {doc.get('mime_type')}")
                            logger.info(f"  └─ file_size: {doc.get('file_size')} bytes")
                        
                        return True
                    
                    elif r.status == 429:
                        # ⚠️ Rate limit
                        retry_after = resp_json.get("parameters", {}).get("retry_after", 10)
                        description = resp_json.get("description", "Too Many Requests")
                        logger.warning(f"[TG-429] ⚠️ RATE LIMIT")
                        logger.warning(f"  ├─ retry_after: {retry_after} seconds")
                        logger.warning(f"  └─ description: {description}")
                        logger.info(f"[TG-429] 🔄 Sleeping {retry_after}s then retrying...")
                        await asyncio.sleep(retry_after)
                        return await self.send(method, **kw)  # Повторяем запрос
                    
                    else:
                        # ❌ Ошибка клиента или сервера
                        error_code = resp_json.get("error_code")
                        description = resp_json.get("description")
                        logger.error(f"[TG-ERROR] ❌ FAILED")
                        logger.error(f"  ├─ error_code: {error_code}")
                        logger.error(f"  └─ description: {description}")
                        logger.debug(f"[TG-ERROR] Request params: {json.dumps(kw, default=str)[:300]}")
                        return False
                        
                except json.JSONDecodeError as e:
                    logger.error(f"[TG-PARSE] ❌ Could not parse JSON response: {e}")
                    logger.error(f"[TG-PARSE] Raw response: {txt[:300]}")
                    return r.status == 200
                    
        except Exception as e:
            logger.error(f"[TG-EXCEPTION] ❌ Exception during request: {e}", exc_info=True)
            return False
    
    async def text(self, text: str) -> bool:
        """Отправляет текстовое сообщение."""
        if not text:
            logger.debug("[TG-SEND] Empty text, skipping")
            return True
        logger.info(f"[STEP] 📤 Sending text message")
        logger.info(f"  ├─ length: {len(text)} characters")
        logger.info(f"  └─ preview: '{text[:100]}{'...' if len(text)>100 else ''}'")
        return await self.send("sendMessage", json={
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": False
        })
    
    async def media(self, type_: str, media, caption: str = "", filename: str = None, is_url: bool = False) -> bool:
        """
        Отправляет медиа (фото, видео, аудио, голосовое, документ).
        Поддерживает отправку по URL или файлом.
        """
        logger.info(f"[STEP] 📤 Preparing {type_} media")
        logger.info(f"  ├─ type: {type_}")
        logger.info(f"  ├─ is_url: {is_url}")
        logger.info(f"  ├─ caption_length: {len(caption) if caption else 0}")
        logger.info(f"  └─ filename: {filename or 'N/A'}")
        
        if isinstance(media, bytes):
            file_size_mb = len(media) / 1024 / 1024
            if file_size_mb > self.MAX_BYTES / 1024 / 1024:
                logger.warning(f"[TG-SKIP] ❌ File too large: {file_size_mb:.2f} MB > 50 MB limit")
                return False
            logger.info(f"  └─ file_size: {len(media)} bytes ({file_size_mb:.2f} MB)")
        
        await self.init()
        form = aiohttp.FormData()
        form.add_field("chat_id", self.chat_id)
        
        field_map = {
            "photo": "photo",
            "video": "video",
            "audio": "audio",
            "voice": "voice",
            "document": "document"
        }
        tg_field = field_map.get(type_, type_)
        
        if is_url:
            form.add_field(tg_field, media)
            logger.info(f"[TG-SEND] 📤 Sending {type_.upper()} via URL")
            logger.debug(f"  └─ URL: {str(media)[:100]}...")
        else:
            fname = filename or f"{type_}_file"
            form.add_field(tg_field, media, filename=fname)
            logger.info(f"[TG-SEND] 📤 Sending {type_.upper()} as file")
            logger.debug(f"  └─ filename: {fname}")
        
        if caption:
            form.add_field("caption", caption[:1024])
            form.add_field("parse_mode", "HTML")
            logger.debug(f"  └─ caption: '{caption[:50]}{'...' if len(caption)>50 else ''}'")
        
        return await self.send(f"send{type_.capitalize()}", data=form)

# ===================================================================
# 7. MAX CLIENT
# ===================================================================
class MX:
    """Клиент MAX API для получения сообщений и скачивания файлов."""
    
    def __init__(self, token: str, cid: str, base: str):
        self.token = token
        self.cid = cid
        self.base = base
        self.session = None
        logger.info(f"[MAX] Client initialized: cid={cid}")
    
    async def init(self):
        """Инициализирует HTTP сессию если нужно."""
        if not self.session or self.session.closed:
            logger.debug("[MAX] Creating new aiohttp session")
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def fetch(self, last_seq: Optional[int] = None) -> List[Dict]:
        """Получает сообщения от MAX API."""
        await self.init()
        try:
            params = {"chat_id": self.cid, "limit": 1}
            if last_seq is not None and not DEBUG_IGNORE_SEQ:
                params["since_seq"] = last_seq
                logger.debug(f"[MAX-REQ] Fetching messages since_seq={last_seq}")
            else:
                logger.debug(f"[MAX-REQ] First fetch (no since_seq)")
            
            async with self.session.get(
                f"{self.base}/messages",
                headers={"Authorization": self.token},
                params=params
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    logger.debug(f"[MAX-RESP] Raw response (first 500 chars): {json.dumps(raw, ensure_ascii=False)[:500]}")
                    msgs = raw.get("messages", raw) if isinstance(raw, dict) else raw
                    return msgs if isinstance(msgs, list) else []
                logger.error(f"[MAX] HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"[MAX] Exception: {e}", exc_info=True)
            return []
    
    async def download(self, token: str) -> Optional[bytes]:
        """Скачивает файл по токену."""
        await self.init()
        logger.info(f"[STEP] 🔽 Downloading file from MAX")
        logger.debug(f"  └─ token: {token[:30]}...")
        try:
            async with self.session.get(
                f"{self.base}/files/{token}/download",
                headers={"Authorization": self.token}
            ) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[STEP] ✅ Download success: {len(data)} bytes")
                    return data
                err = await r.text()
                logger.error(f"[STEP] ❌ Download failed: HTTP {r.status}")
                logger.debug(f"  └─ error: {err[:200]}")
                return None
        except Exception as e:
            logger.error(f"[MAX-DL] Exception: {e}", exc_info=True)
            return None

# ===================================================================
# 8. ОБРАБОТКА СООБЩЕНИЙ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)

async def handle(msg: Dict):
    """Обрабатывает одно сообщение от MAX."""
    logger.info(f"[STEP] ▶️ handle() START")
    logger.info(f"🔍 [RAW-MSG] {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    # 🔹 Извлечение данных
    data = extract_data(msg)
    mid = data["mid"]
    seq = data.get("seq")
    
    logger.info(f"[STEP] 📦 Parse RESULT")
    logger.info(f"  ├─ source: {data['source']}")
    logger.info(f"  ├─ mid: {mid}")
    logger.info(f"  ├─ seq: {seq}")
    logger.info(f"  ├─ text_length: {len(data['text'])}")
    logger.info(f"  ├─ markup_count: {len(data['markup'])}")
    logger.info(f"  └─ attachments_count: {len(data['attachments'])}")
    
    # 🔹 ДЕДУПЛИКАЦИЯ ПО SEQ (строгое < вместо <=)
    if seq is None:
        if not DEBUG_IGNORE_SEQ:
            logger.warning("[STEP] ❌ No seq field, skipping message")
            return
        else:
            logger.warning("[STEP] 🔧 DEBUG: No seq but DEBUG_IGNORE_SEQ=1, continuing")
    
    global _last_processed_seq
    if not DEBUG_IGNORE_SEQ and seq < _last_processed_seq:
        logger.info(f"[STEP] ⏭ DUPE: msg_seq={seq} < last_seq={_last_processed_seq} | skipping")
        return
    if DEBUG_IGNORE_SEQ:
        logger.warning(f"[STEP] 🔧 DEBUG: Ignoring seq check | seq={seq}")
    
    logger.info(f"[STEP] ✅ NEW: msg_seq={seq} > last_seq={_last_processed_seq} | processing")
    
    # 🔹 Конвертация разметки
    text = apply_markup(data["text"], data["markup"]) if data["markup"] else data["text"]
    
    # 🔹 Отправка текста
    if text:
        logger.info(f"[STEP] 📤 Sending text to Telegram")
        logger.info(f"  └─ preview: '{text[:100]}{'...' if len(text)>100 else ''}'")
        ok = await tg.text(text)
        logger.info(f"[STEP] ✅ Text send result: {'OK' if ok else 'FAIL'}")
        await asyncio.sleep(0.2)
    
    # 🔹 Отправка вложений
    for i, att in enumerate(data["attachments"]):
        logger.info(f"[STEP] 📎 Processing attachment #{i+1}/{len(data['attachments'])}")
        logger.debug(f"[ATT] Raw: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        if not isinstance(att, dict):
            logger.warning("[SKIP] ❌ Attachment not dict, skipping")
            continue
        
        atype = att.get("type") or att.get("media_type") or "file"
        payload = att.get("payload", {}) if isinstance(att.get("payload"), dict) else {}
        
        url = payload.get("url") or att.get("url")
        token = payload.get("token") or att.get("token") or att.get("id") or att.get("file_token")
        fname = payload.get("filename") or att.get("filename") or att.get("name") or f"file_{i+1}"
        size = payload.get("size") or att.get("size") or 0
        
        logger.info(f"[STEP] 📎 Attachment details")
        logger.info(f"  ├─ type: {atype}")
        logger.info(f"  ├─ has_url: {bool(url)}")
        logger.info(f"  ├─ has_token: {bool(token)}")
        logger.info(f"  ├─ filename: {fname}")
        logger.info(f"  └─ size: {size} bytes ({size/1024:.1f} KB)")
        
        # 🔹 ОПРЕДЕЛЕНИЕ ТИПА МЕДИА (приоритет: расширение → atype → размер)
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        tg_type = "document"
        
        # Правило 1: голосовые по расширению
        if ext in ("ogg", "opus", "oga"):
            tg_type = "voice"
            logger.info(f"[STEP] 🔍 Detected type: voice (by extension .{ext})")
        # Правило 2: аудио по расширению
        elif ext in ("mp3", "wav", "m4a", "flac", "aac"):
            tg_type = "audio"
            logger.info(f"[STEP] 🔍 Detected type: audio (by extension .{ext})")
        # Правило 3: фото по расширению
        elif ext in ("jpg", "jpeg", "png", "gif", "webp", "heic"):
            tg_type = "photo"
            logger.info(f"[STEP] 🔍 Detected type: photo (by extension .{ext})")
        # Правило 4: видео по расширению
        elif ext in ("mp4", "mov", "avi", "mkv", "webm", "flv"):
            tg_type = "video"
            logger.info(f"[STEP] 🔍 Detected type: video (by extension .{ext})")
        # Правило 5: по atype из MAX
        elif atype in ("image", "photo", "picture"):
            tg_type = "photo"
            logger.info(f"[STEP] 🔍 Detected type: photo (by atype={atype})")
        elif atype == "video":
            tg_type = "video"
            logger.info(f"[STEP] 🔍 Detected type: video (by atype={atype})")
        elif atype == "voice":
            tg_type = "voice"
            logger.info(f"[STEP] 🔍 Detected type: voice (by atype={atype})")
        elif atype == "audio":
            # Эвристика: если audio + маленький размер → голосовое
            if size < 2 * 1024 * 1024:  # < 2MB
                tg_type = "voice"
                logger.info(f"[STEP] 🔍 Detected type: voice (by atype=audio + size={size/1024:.0f}KB < 2MB)")
            else:
                tg_type = "audio"
                logger.info(f"[STEP] 🔍 Detected type: audio (by atype=audio + size={size/1024:.0f}KB >= 2MB)")
        else:
            tg_type = "document"
            logger.info(f"[STEP] 🔍 Detected type: document (fallback)")
        
        logger.info(f"[STEP] 🎯 Final media type decision: {tg_type}")
        
        caption = text if tg_type != "document" else ""
        sent = False
        
        # 🔹 ОТПРАВКА ПО ССЫЛКЕ ИЛИ ФАЙЛОМ
        if url and tg_type in ("photo", "video", "document", "audio", "voice"):
            logger.info(f"[STEP] 📤 Sending {tg_type} via URL")
            logger.debug(f"  └─ URL: {url[:80]}...")
            sent = await tg.media(tg_type, url, caption, is_url=True)
        elif token:
            logger.info(f"[STEP] 📤 Downloading via token...")
            file_data = await mx.download(token)
            if file_data is not None and len(file_data) > 0:
                logger.info(f"[STEP] 📤 Sending {tg_type} as file")
                sent = await tg.media(tg_type, file_data, caption, filename=fname)
            else:
                logger.error(f"[STEP] ❌ Download failed (token expired or 404)")
        else:
            logger.warning(f"[STEP] ❌ No URL or Token found, skipping")
        
        logger.info(f"[STEP] ✅ {tg_type} send result: {'OK' if sent else 'FAIL'}")
        
        # 🔹 Тайминги: 0.3с между медиа, +0.2с для голосовых
        delay = 0.3 + (0.2 if tg_type == "voice" else 0)
        logger.debug(f"[STEP] ⏱️ Delay {delay}s after {tg_type}")
        await asyncio.sleep(delay)
    
    # 🔹 Обновляем last_seq ТОЛЬКО после успешной обработки
    _last_processed_seq = seq
    logger.info(f"[STEP] ✅ handle() COMPLETE")
    logger.info(f"  └─ last_seq updated to {_last_processed_seq}")

# ===================================================================
# 9. POLLING LOOP
# ===================================================================
async def polling_loop():
    """Основной цикл опроса MAX API."""
    logger.info("🔄 Starting polling loop...")
    await asyncio.sleep(2)  # Ждём готовности сети
    
    while True:
        try:
            last_seq_for_fetch = _last_processed_seq if _last_processed_seq > 0 and not DEBUG_IGNORE_SEQ else None
            msgs = await mx.fetch(last_seq=last_seq_for_fetch)
            logger.debug(f"[POLL] Got {len(msgs)} messages")
            
            if msgs:
                await handle(msgs[0])
            else:
                logger.debug("[POLL] No new messages")
                
        except Exception as e:
            logger.error(f"[LOOP] Exception: {e}", exc_info=True)
        
        await asyncio.sleep(POLL_SEC)

# ===================================================================
# 10. WEB SERVER (HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    """Эндпоинт для проверки здоровья бота."""
    return web.json_response({
        "ok": True,
        "last_seq": _last_processed_seq,
        "debug_ignore_seq": DEBUG_IGNORE_SEQ
    })

async def run_app():
    """Запускает веб-сервер и polling loop."""
    app = web.Application()
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🌐 Health server running on :8080 (UptimeRobot compatible)")
    await polling_loop()

# ===================================================================
# 11. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder [FULL VERSION]...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user (Ctrl+C)")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
