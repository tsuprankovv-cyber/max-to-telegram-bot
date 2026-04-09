# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ПОЛНАЯ ВЕРСИЯ С РАСШИРЕННОЙ ПОДДЕРЖКОЙ ФОРМАТИРОВАНИЯ, МЕДИА И ЛОГИРОВАНИЯ
"""
import os
import sys
import asyncio
import logging
import json
import time
import re
import tempfile
import subprocess
from typing import List, Dict, Optional, Any, Tuple, Union
from collections import deque
from io import BytesIO

import aiohttp
from aiohttp import web
from mutagen import File as MutagenFile
from mutagen.id3 import ID3
from pydub import AudioSegment

# ===================================================================
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ
# ===================================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()
LOG_RAW_MAX = os.getenv('LOG_RAW_MAX', '1') == '1'
LOG_RAW_TG = os.getenv('LOG_RAW_TG', '1') == '1'
LOG_MARKUP = os.getenv('LOG_MARKUP', '1') == '1'
LOG_MEDIA = os.getenv('LOG_MEDIA', '1') == '1'

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.DEBUG),
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
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
TG_CHAT = os.getenv('TELEGRAM_CHAT_ID', '').strip()
MAX_TOKEN = os.getenv('MAX_TOKEN', '').strip()
MAX_CHAN = os.getenv('MAX_CHANNEL_ID', '').strip()
MAX_BASE = os.getenv('MAX_API_BASE', 'https://platform-api.max.ru').rstrip('/')
POLL_SEC = int(os.getenv('POLL_INTERVAL', '15'))
DEBUG_IGNORE_SEQ = os.getenv('DEBUG_IGNORE_SEQ', '0') == '1'

_last_processed_seq = 0

logger.info("=" * 100)
logger.info("🚀 MAX → TELEGRAM FORWARDER [FULL VERSION]")
logger.info("=" * 100)
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 MAX API Base: {MAX_BASE}")
logger.info(f"⏱️  Poll Interval: {POLL_SEC}s")
logger.info(f"📊 LOG_LEVEL: {LOG_LEVEL}")
logger.info(f"📊 LOG_RAW_MAX: {LOG_RAW_MAX}")
logger.info(f"📊 LOG_RAW_TG: {LOG_RAW_TG}")
logger.info(f"📊 LOG_MARKUP: {LOG_MARKUP}")
logger.info(f"📊 LOG_MEDIA: {LOG_MEDIA}")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

logger.info("✅ All environment variables present")

# ===================================================================
# 3. УТИЛИТЫ ДЛЯ ГРАФЕМ И РАЗМЕТКИ
# ===================================================================
def split_into_graphemes(text: str) -> List[str]:
    """Разбивает текст на графемы для корректной работы с эмодзи."""
    if not text:
        return []
    
    graphemes = []
    i = 0
    while i < len(text):
        char = text[i]
        if ('\U0001F300' <= char <= '\U0001F9FF' or
            '\U00002600' <= char <= '\U000026FF' or
            '\U00002700' <= char <= '\U000027BF'):
            j = i + 1
            while j < len(text):
                next_char = text[j]
                if ('\U0001F3FB' <= next_char <= '\U0001F3FF' or
                    next_char == '\u200D' or
                    next_char in '\uFE0E\uFE0F'):
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


def find_markup_in_message(msg: Dict) -> Tuple[List[Dict], str]:
    """
    Ищет разметку во всех возможных полях сообщения MAX.
    Возвращает (markup_list, source_field).
    """
    # Возможные поля с разметкой
    possible_fields = ['markup', 'entities', 'formats', 'styles', 'annotations', 'text_entities']
    
    # Проверяем body
    body = msg.get('body', {})
    if isinstance(body, dict):
        for field in possible_fields:
            if field in body and body[field]:
                logger.info(f"[MARKUP-DETECT] Found markup in body.{field}")
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP-RAW] body.{field}: {json.dumps(body[field], ensure_ascii=False, indent=2)[:1000]}")
                return body[field], f"body.{field}"
    
    # Проверяем корень
    for field in possible_fields:
        if field in msg and msg[field]:
            logger.info(f"[MARKUP-DETECT] Found markup in root.{field}")
            if LOG_MARKUP:
                logger.debug(f"[MARKUP-RAW] root.{field}: {json.dumps(msg[field], ensure_ascii=False, indent=2)[:1000]}")
            return msg[field], f"root.{field}"
    
    # Проверяем link.message (форвард)
    link = msg.get('link', {})
    if isinstance(link, dict) and 'message' in link:
        inner = link['message']
        if isinstance(inner, dict):
            inner_body = inner.get('body', {})
            if isinstance(inner_body, dict):
                for field in possible_fields:
                    if field in inner_body and inner_body[field]:
                        logger.info(f"[MARKUP-DETECT] Found markup in link.message.body.{field}")
                        return inner_body[field], f"link.message.body.{field}"
            for field in possible_fields:
                if field in inner and inner[field]:
                    logger.info(f"[MARKUP-DETECT] Found markup in link.message.{field}")
                    return inner[field], f"link.message.{field}"
    
    logger.info("[MARKUP-DETECT] No markup found in message")
    return [], "none"


def apply_markup(text: str, markup: List[Dict]) -> str:
    """
    Конвертирует разметку MAX в HTML для Telegram.
    Поддерживает все типы и вложенность.
    """
    if not markup or not text:
        return text
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, markup_items={len(markup)}")
    
    # Маппинг типов разметки MAX → HTML теги Telegram
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
    
    graphemes = split_into_graphemes(text)
    n = len(graphemes)
    
    events = []
    for idx, m in enumerate(markup):
        try:
            # Поддержка разных имён полей
            start = int(m.get("from") or m.get("offset") or 0)
            length = int(m.get("length") or 0)
            mtype = m.get("type") or m.get("tag") or ""
            end = start + length
            
            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range: item={idx}, start={start}, length={length}, end={end}, n={n}")
                continue
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                events.append((start, 'open', open_tag, idx))
                events.append((end, 'close', close_tag, idx))
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP] Item {idx}: {mtype} [{start}:{end}]")
            elif mtype in ("link", "text_link", "url"):
                url = m.get("url") or m.get("href") or ""
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url_safe}">'
                    close_tag = "</a>"
                    events.append((start, 'open', open_tag, idx))
                    events.append((end, 'close', close_tag, idx))
                    if LOG_MARKUP:
                        logger.debug(f"[MARKUP] Item {idx}: link [{start}:{end}] -> {url[:50]}")
            elif mtype in ("mention", "hashtag", "bot_command", "cashtag", "email"):
                # Пропускаем, Telegram сам распознает
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP] Item {idx}: {mtype} (skipped, native TG support)")
                continue
            else:
                logger.warning(f"[MARKUP] Unknown type: '{mtype}' in item {idx}")
        except Exception as e:
            logger.error(f"[MARKUP] Error processing item {idx}: {e}", exc_info=True)
    
    # Сортируем: по позиции, закрывающие перед открывающими
    events.sort(key=lambda x: (x[0], 0 if x[1] == 'close' else 1, -x[3]))
    
    result = []
    active_tags = []
    event_idx = 0
    
    for pos in range(n + 1):
        while event_idx < len(events) and events[event_idx][0] == pos:
            _, etype, tag, priority = events[event_idx]
            if etype == 'close':
                for i in range(len(active_tags) - 1, -1, -1):
                    if active_tags[i][1] == priority:
                        result.append(active_tags[i][0])
                        active_tags.pop(i)
                        break
            else:
                active_tags.append((tag, priority))
            event_idx += 1
        
        if pos < n:
            result.append(graphemes[pos])
    
    for tag_close, _ in reversed(active_tags):
        result.append(tag_close)
    
    final_text = "".join(result)
    logger.info(f"[MARKUP] Conversion complete: output_len={len(final_text)}")
    if LOG_MARKUP:
        logger.debug(f"[MARKUP] Output preview: {final_text[:200]}")
    
    return final_text


# ===================================================================
# 4. ДЕТЕКЦИЯ ФОРВАРДОВ
# ===================================================================
def extract_forward_info(msg: Dict, depth: int = 0) -> Optional[Dict]:
    """
    Извлекает информацию о пересланном сообщении из всех возможных полей.
    """
    if depth > 5:
        logger.warning(f"[FWD] Max recursion depth reached")
        return None
    
    # Возможные поля с форвардом
    fwd_fields = ['link', 'forward', 'fwd', 'forwarded_message', 'reply_to', 'quoted_message']
    
    for field in fwd_fields:
        if field in msg and msg[field]:
            logger.info(f"[FWD-DETECT] Found forward in field: {field}")
            fwd_data = msg[field]
            
            if LOG_RAW_MAX:
                logger.debug(f"[FWD-RAW] {field}: {json.dumps(fwd_data, ensure_ascii=False)[:500]}")
            
            # Тип 1: link.message
            if isinstance(fwd_data, dict) and 'message' in fwd_data:
                inner = fwd_data['message']
                logger.info(f"[FWD] Extracting from link.message")
                return {
                    "source_field": field,
                    "message": inner,
                    "original_chat_id": fwd_data.get('chat_id'),
                    "original_message_id": fwd_data.get('message_id') or inner.get('id')
                }
            
            # Тип 2: сам словарь - это сообщение
            if isinstance(fwd_data, dict) and ('text' in fwd_data or 'body' in fwd_data or 'attachments' in fwd_data):
                logger.info(f"[FWD] Forward is direct message dict")
                return {
                    "source_field": field,
                    "message": fwd_data,
                    "original_chat_id": fwd_data.get('chat_id'),
                    "original_message_id": fwd_data.get('id')
                }
            
            # Тип 3: строка - ID сообщения
            if isinstance(fwd_data, str):
                logger.info(f"[FWD] Forward is message ID string: {fwd_data}")
                return {
                    "source_field": field,
                    "message_id": fwd_data,
                    "type": "reference"
                }
    
    # Рекурсивно проверяем body
    body = msg.get('body', {})
    if isinstance(body, dict):
        result = extract_forward_info(body, depth + 1)
        if result:
            return result
    
    logger.info(f"[FWD-DETECT] No forward found")
    return None


# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ СООБЩЕНИЯ
# ===================================================================
def safe_list(val: Any) -> List[Dict]:
    """Гарантирует возврат списка вложений."""
    if val is None:
        return []
    if isinstance(val, list):
        return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['messages', 'items', 'data', 'result', 'message', 'attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v] if isinstance(v, dict) else []
    return []


def extract_data(msg: Dict) -> Dict:
    """
    Извлекает текст, разметку и вложения из сообщения MAX.
    Поддерживает форварды.
    """
    logger.info(f"[EXTRACT] Starting data extraction")
    
    # Проверяем форвард
    fwd = extract_forward_info(msg)
    if fwd and fwd.get('message'):
        logger.info(f"[EXTRACT] Processing forwarded message")
        inner = fwd['message']
        
        # Извлекаем данные из форварда
        body = inner.get('body', {}) if isinstance(inner, dict) else {}
        
        # Ищем разметку
        markup, markup_source = find_markup_in_message(inner)
        
        return {
            "source": f"forward.{fwd['source_field']}",
            "mid": inner.get('mid') or inner.get('id') or fwd.get('original_message_id'),
            "seq": inner.get('seq') or body.get('seq'),
            "text": inner.get('text', '') or body.get('text', ''),
            "markup": markup,
            "markup_source": markup_source,
            "attachments": safe_list(
                inner.get('attachments') or 
                body.get('attachments') or 
                inner.get('files') or 
                inner.get('media')
            ),
            "is_forward": True,
            "forward_info": {
                "chat_id": fwd.get('original_chat_id'),
                "message_id": fwd.get('original_message_id')
            }
        }
    
    # Обычное сообщение
    body = msg.get('body', {})
    
    # Ищем разметку
    markup, markup_source = find_markup_in_message(msg)
    
    if isinstance(body, dict) and ('text' in body or 'attachments' in body):
        logger.info(f"[EXTRACT] Using body")
        return {
            "source": "body",
            "mid": body.get('mid') or msg.get('mid'),
            "seq": body.get('seq') or msg.get('seq'),
            "text": body.get('text', ''),
            "markup": markup,
            "markup_source": markup_source,
            "attachments": safe_list(
                body.get('attachments') or 
                body.get('files') or 
                body.get('media')
            ),
            "is_forward": False
        }
    
    # Корень сообщения
    logger.info(f"[EXTRACT] Using root")
    return {
        "source": "root",
        "mid": msg.get('id') or msg.get('message_id') or msg.get('mid'),
        "seq": msg.get('seq'),
        "text": msg.get('text', ''),
        "markup": markup,
        "markup_source": markup_source,
        "attachments": safe_list(
            msg.get('attachments') or 
            msg.get('files') or 
            msg.get('media')
        ),
        "is_forward": False
    }


# ===================================================================
# 6. КОНВЕРТЕР МЕДИА
# ===================================================================
class MediaProcessor:
    """Обрабатывает медиа: определяет тип, конвертирует при необходимости."""
    
    # Расширения для разных типов
    PHOTO_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic', 'heif'}
    VIDEO_EXTS = {'mp4', 'mov', 'avi', 'mkv', 'webm', 'flv', 'm4v', '3gp'}
    AUDIO_EXTS = {'mp3', 'wav', 'm4a', 'flac', 'aac', 'ogg', 'opus', 'wma'}
    VOICE_EXTS = {'ogg', 'opus', 'oga'}
    
    def __init__(self):
        self.ffmpeg_available = self._check_ffmpeg()
        logger.info(f"[MEDIA] FFmpeg available: {self.ffmpeg_available}")
    
    def _check_ffmpeg(self) -> bool:
        """Проверяет наличие ffmpeg в системе."""
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            return True
        except (subprocess.SubprocessError, FileNotFoundError):
            return False
    
    def determine_type(self, att: Dict) -> Tuple[str, Dict]:
        """
        Определяет тип медиа для Telegram.
        Возвращает (tg_type, metadata).
        """
        atype = att.get('type') or att.get('media_type') or 'file'
        payload = att.get('payload', {}) if isinstance(att.get('payload'), dict) else {}
        
        fname = payload.get('filename') or att.get('filename') or att.get('name') or ''
        size = payload.get('size') or att.get('size') or 0
        url = payload.get('url') or att.get('url')
        token = payload.get('token') or att.get('token') or att.get('file_token')
        
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        
        logger.info(f"[MEDIA] Determining type: atype={atype}, ext={ext}, size={size}, fname={fname}")
        
        metadata = {
            'filename': fname,
            'size': size,
            'url': url,
            'token': token,
            'ext': ext,
            'original_type': atype
        }
        
        # Правило 1: по расширению
        if ext in self.PHOTO_EXTS:
            return 'photo', metadata
        if ext in self.VIDEO_EXTS:
            return 'video', metadata
        if ext in self.VOICE_EXTS:
            return 'voice', metadata
        if ext in self.AUDIO_EXTS:
            # Проверяем, может это голосовое?
            if size < 2 * 1024 * 1024 and self.ffmpeg_available:
                logger.info(f"[MEDIA] Small audio file, will convert to voice")
                return 'voice', metadata
            return 'audio', metadata
        
        # Правило 2: по atype из MAX
        if atype in ('image', 'photo', 'picture'):
            return 'photo', metadata
        if atype == 'video':
            return 'video', metadata
        if atype == 'voice':
            return 'voice', metadata
        if atype == 'audio':
            if size < 2 * 1024 * 1024 and self.ffmpeg_available:
                return 'voice', metadata
            return 'audio', metadata
        
        # Правило 3: всё остальное - документ
        return 'document', metadata
    
    async def convert_to_voice(self, audio_data: bytes, original_name: str) -> Optional[bytes]:
        """
        Конвертирует аудио в формат OGG Opus для голосовых сообщений Telegram.
        """
        if not self.ffmpeg_available:
            logger.warning("[MEDIA] FFmpeg not available, cannot convert to voice")
            return None
        
        try:
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp_in:
                tmp_in.write(audio_data)
                tmp_in_path = tmp_in.name
            
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_out:
                tmp_out_path = tmp_out.name
            
            # Конвертация: моно, 16-32 kbps, Opus кодек
            cmd = [
                'ffmpeg', '-i', tmp_in_path,
                '-ac', '1',           # моно
                '-ar', '16000',       # 16 kHz (оптимально для голоса)
                '-c:a', 'libopus',
                '-b:a', '16k',
                '-vbr', 'on',
                '-application', 'voip',
                '-y',                 # перезаписать
                tmp_out_path
            ]
            
            logger.info(f"[MEDIA] Converting to voice: {original_name}")
            logger.debug(f"[MEDIA] FFmpeg cmd: {' '.join(cmd)}")
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                logger.error(f"[MEDIA] FFmpeg error: {result.stderr[:500]}")
                return None
            
            with open(tmp_out_path, 'rb') as f:
                ogg_data = f.read()
            
            logger.info(f"[MEDIA] Conversion success: {len(audio_data)} -> {len(ogg_data)} bytes")
            
            # Очистка
            os.unlink(tmp_in_path)
            os.unlink(tmp_out_path)
            
            return ogg_data
            
        except Exception as e:
            logger.error(f"[MEDIA] Conversion error: {e}", exc_info=True)
            return None
    
    def extract_audio_tags(self, audio_data: bytes, filename: str) -> Dict[str, str]:
        """
        Извлекает метаданные аудиофайла (исполнитель, название).
        """
        try:
            audio = AudioSegment.from_file(BytesIO(audio_data))
            duration_ms = len(audio)
            duration = int(duration_ms / 1000)
        except:
            duration = 0
        
        performer = ''
        title = ''
        
        try:
            # Пробуем mutagen
            with tempfile.NamedTemporaryFile(suffix='.mp3', delete=False) as tmp:
                tmp.write(audio_data)
                tmp_path = tmp.name
            
            audio_file = MutagenFile(tmp_path)
            if audio_file is not None:
                if hasattr(audio_file, 'tags') and audio_file.tags:
                    # ID3 теги
                    if 'TPE1' in audio_file.tags:
                        performer = str(audio_file.tags['TPE1'])
                    if 'TIT2' in audio_file.tags:
                        title = str(audio_file.tags['TIT2'])
                elif hasattr(audio_file, 'get'):
                    # Другие форматы
                    performer = audio_file.get('artist', [''])[0] if audio_file.get('artist') else ''
                    title = audio_file.get('title', [''])[0] if audio_file.get('title') else ''
            
            os.unlink(tmp_path)
        except Exception as e:
            logger.debug(f"[MEDIA] Could not read ID3 tags: {e}")
        
        # Fallback: парсим имя файла
        if not performer or not title:
            name_without_ext = filename.rsplit('.', 1)[0] if '.' in filename else filename
            if ' - ' in name_without_ext:
                parts = name_without_ext.split(' - ', 1)
                performer = performer or parts[0].strip()
                title = title or parts[1].strip()
            else:
                title = title or name_without_ext.strip()
                performer = performer or 'Unknown Artist'
        
        performer = performer or 'Unknown Artist'
        title = title or filename or 'Unknown Track'
        
        logger.info(f"[MEDIA] Audio tags: performer='{performer}', title='{title}', duration={duration}s")
        
        return {
            'performer': performer[:64],
            'title': title[:64],
            'duration': duration
        }


# ===================================================================
# 7. TELEGRAM CLIENT
# ===================================================================
class TG:
    """Клиент Telegram Bot API с расширенным логированием."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
        self.MAX_BYTES = 50 * 1024 * 1024
        logger.info(f"[TG] Initialized: chat_id={chat_id}")
    
    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def send(self, method: str, **kw) -> Optional[Dict]:
        """
        Отправляет запрос к Telegram API.
        Возвращает полный JSON ответа или None при ошибке.
        """
        await self.init()
        start_time = time.time()
        logger.info(f"[TG] ▶️ {method}")
        
        if LOG_RAW_TG:
            logger.debug(f"[TG-REQ] Params: {json.dumps(kw, default=str, ensure_ascii=False)[:500]}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                elapsed_ms = (time.time() - start_time) * 1000
                txt = await r.text()
                
                logger.info(f"[TG] Status: {r.status} | Time: {elapsed_ms:.0f}ms | Size: {len(txt)} bytes")
                
                if LOG_RAW_TG:
                    logger.info(f"[TG-RESP] {txt}")
                
                try:
                    resp_json = json.loads(txt)
                except:
                    logger.error(f"[TG] Invalid JSON response: {txt[:300]}")
                    return None
                
                if r.status == 200 and resp_json.get('ok'):
                    result = resp_json.get('result', {})
                    msg_id = result.get('message_id')
                    logger.info(f"[TG] ✅ Success: message_id={msg_id}")
                    return resp_json
                
                elif r.status == 429:
                    retry_after = resp_json.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] ⚠️ Rate limit, waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    return await self.send(method, **kw)
                
                else:
                    error_code = resp_json.get('error_code')
                    description = resp_json.get('description')
                    logger.error(f"[TG] ❌ Error {error_code}: {description}")
                    return resp_json
                    
        except Exception as e:
            logger.error(f"[TG] Exception: {e}", exc_info=True)
            return None
    
    async def text(self, text: str) -> bool:
        """Отправляет текстовое сообщение."""
        if not text or not text.strip():
            logger.debug("[TG] Empty text, skipping")
            return True
        
        logger.info(f"[TG] Sending text: len={len(text)}")
        logger.debug(f"[TG] Text preview: {text[:200]}")
        
        resp = await self.send('sendMessage', json={
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False
        })
        
        return resp is not None and resp.get('ok', False)
    
    async def media(self, type_: str, media_data: Union[str, bytes], 
                    caption: str = "", filename: str = None, 
                    is_url: bool = False, **extra) -> bool:
        """
        Отправляет медиа.
        extra: performer, title, duration для audio
        """
        logger.info(f"[TG] Sending {type_}: is_url={is_url}, caption_len={len(caption)}")
        
        if isinstance(media_data, bytes):
            size_mb = len(media_data) / 1024 / 1024
            if size_mb > 50:
                logger.warning(f"[TG] File too large: {size_mb:.2f} MB")
                return False
            logger.info(f"[TG] File size: {len(media_data)} bytes ({size_mb:.2f} MB)")
        
        await self.init()
        
        # Для sendAudio, sendVoice, sendDocument используем метод, соответствующий типу
        method_map = {
            'photo': 'sendPhoto',
            'video': 'sendVideo',
            'audio': 'sendAudio',
            'voice': 'sendVoice',
            'document': 'sendDocument'
        }
        
        method = method_map.get(type_, 'sendDocument')
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        field_map = {
            'photo': 'photo',
            'video': 'video',
            'audio': 'audio',
            'voice': 'voice',
            'document': 'document'
        }
        tg_field = field_map.get(type_, 'document')
        
        if is_url:
            form.add_field(tg_field, media_data)
        else:
            fname = filename or f"{type_}_file"
            form.add_field(tg_field, media_data, filename=fname)
        
        if caption:
            form.add_field('caption', caption[:1024])
            form.add_field('parse_mode', 'HTML')
        
        # Дополнительные параметры для аудио
        if type_ == 'audio':
            if extra.get('performer'):
                form.add_field('performer', extra['performer'][:64])
            if extra.get('title'):
                form.add_field('title', extra['title'][:64])
            if extra.get('duration'):
                form.add_field('duration', str(extra['duration']))
        
        # Для голосовых тоже можно добавить duration
        if type_ == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
        
        resp = await self.send(method, data=form)
        return resp is not None and resp.get('ok', False)
    
    async def media_group(self, media_items: List[Dict]) -> bool:
        """
        Отправляет группу медиа (до 10 фото/видео).
        media_items: [{'type': 'photo', 'media': url_or_file_id, 'caption': '...'}, ...]
        """
        if not media_items:
            return True
        
        if len(media_items) > 10:
            logger.warning(f"[TG] Media group too large: {len(media_items)}, splitting")
            # Отправляем частями
            for i in range(0, len(media_items), 10):
                chunk = media_items[i:i+10]
                await self.media_group(chunk)
                await asyncio.sleep(0.5)
            return True
        
        logger.info(f"[TG] Sending media group: {len(media_items)} items")
        
        await self.init()
        
        # Для media group нужен JSON
        input_media = []
        for item in media_items:
            media_obj = {
                'type': item['type'],
                'media': item['media']
            }
            if item.get('caption'):
                media_obj['caption'] = item['caption'][:1024]
                media_obj['parse_mode'] = 'HTML'
            input_media.append(media_obj)
        
        resp = await self.send('sendMediaGroup', json={
            'chat_id': self.chat_id,
            'media': input_media
        })
        
        return resp is not None and resp.get('ok', False)


# ===================================================================
# 8. MAX CLIENT
# ===================================================================
class MX:
    """Клиент MAX API."""
    
    def __init__(self, token: str, cid: str, base: str):
        self.token = token
        self.cid = cid
        self.base = base
        self.session = None
        logger.info(f"[MAX] Initialized: cid={cid}")
    
    async def init(self):
        if not self.session or self.session.closed:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def fetch(self, limit: int = 50) -> List[Dict]:
        """Получает последние сообщения от MAX API."""
        await self.init()
        try:
            params = {'chat_id': self.cid, 'limit': limit}
            
            logger.debug(f"[MAX] Fetching messages: limit={limit}")
            
            async with self.session.get(
                f"{self.base}/messages",
                headers={'Authorization': self.token},
                params=params
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    
                    if LOG_RAW_MAX:
                        logger.debug(f"[MAX-RESP] {json.dumps(raw, ensure_ascii=False)[:1000]}")
                    
                    msgs = raw.get('messages', raw) if isinstance(raw, dict) else raw
                    logger.info(f"[MAX] Got {len(msgs) if isinstance(msgs, list) else 0} messages")
                    return msgs if isinstance(msgs, list) else []
                
                logger.error(f"[MAX] HTTP {r.status}")
                return []
        except Exception as e:
            logger.error(f"[MAX] Exception: {e}", exc_info=True)
            return []
    
    async def download(self, token: str) -> Optional[bytes]:
        """Скачивает файл по токену."""
        await self.init()
        logger.info(f"[MAX] Downloading: token={token[:30]}...")
        
        try:
            async with self.session.get(
                f"{self.base}/files/{token}/download",
                headers={'Authorization': self.token}
            ) as r:
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[MAX] Downloaded: {len(data)} bytes")
                    return data
                
                err = await r.text()
                logger.error(f"[MAX] Download failed: HTTP {r.status} - {err[:200]}")
                return None
        except Exception as e:
            logger.error(f"[MAX] Download exception: {e}")
            return None


# ===================================================================
# 9. ОБРАБОТЧИК СООБЩЕНИЙ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_processor = MediaProcessor()


def extract_seq_from_msg(msg: Dict) -> int:
    """Извлекает seq из сообщения."""
    body = msg.get('body', {})
    if isinstance(body, dict) and 'seq' in body:
        return int(body['seq'])
    if 'seq' in msg:
        return int(msg['seq'])
    return 0


async def process_attachment(att: Dict, caption: str = "") -> bool:
    """Обрабатывает одно вложение."""
    logger.info(f"[ATT] Processing: {json.dumps(att, ensure_ascii=False)[:300]}")
    
    if not isinstance(att, dict):
        logger.warning("[ATT] Not a dict, skipping")
        return False
    
    # Определяем тип
    tg_type, meta = media_processor.determine_type(att)
    logger.info(f"[ATT] Determined type: {tg_type}")
    
    # Скачиваем если нет URL
    file_data = None
    is_url = False
    media_input = None
    
    if meta.get('url'):
        media_input = meta['url']
        is_url = True
        logger.info(f"[ATT] Using URL: {meta['url'][:80]}...")
    elif meta.get('token'):
        file_data = await mx.download(meta['token'])
        if file_data:
            media_input = file_data
            is_url = False
        else:
            logger.error("[ATT] Download failed")
            return False
    else:
        logger.warning("[ATT] No URL or token")
        return False
    
    # Конвертация голосовых
    extra = {}
    if tg_type == 'voice' and not is_url and file_data:
        if meta['ext'] not in media_processor.VOICE_EXTS:
            logger.info(f"[ATT] Converting to voice...")
            voice_data = await media_processor.convert_to_voice(file_data, meta['filename'])
            if voice_data:
                media_input = voice_data
                # Определяем длительность
                try:
                    audio = AudioSegment.from_file(BytesIO(voice_data))
                    extra['duration'] = int(len(audio) / 1000)
                except:
                    pass
            else:
                # Fallback: отправляем как аудио
                tg_type = 'audio'
                logger.warning("[ATT] Conversion failed, sending as audio")
    
    # Извлечение тегов для аудио
    if tg_type == 'audio' and not is_url and file_data:
        tags = media_processor.extract_audio_tags(file_data, meta['filename'])
        extra.update(tags)
    
    # Отправка
    success = await tg.media(
        tg_type,
        media_input,
        caption=caption if tg_type != 'document' else '',
        filename=meta['filename'],
        is_url=is_url,
        **extra
    )
    
    logger.info(f"[ATT] Send result: {'OK' if success else 'FAIL'}")
    return success


async def handle_message(msg: Dict):
    """Обрабатывает одно сообщение от MAX."""
    global _last_processed_seq
    
    logger.info("=" * 60)
    logger.info("[HANDLE] Processing message")
    
    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw: {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    # Извлекаем seq
    seq = extract_seq_from_msg(msg)
    mid = msg.get('body', {}).get('mid') or msg.get('mid', 'unknown')
    
    logger.info(f"[HANDLE] mid={mid}, seq={seq}, last_seq={_last_processed_seq}")
    
    # Дедупликация
    if seq and seq <= _last_processed_seq and not DEBUG_IGNORE_SEQ:
        logger.info(f"[HANDLE] ⏭ Skipping duplicate: seq={seq} <= {_last_processed_seq}")
        return
    
    # Извлекаем данные
    data = extract_data(msg)
    
    logger.info(f"[HANDLE] Extracted: source={data['source']}, text_len={len(data['text'])}, attachments={len(data['attachments'])}")
    logger.info(f"[HANDLE] Markup source: {data.get('markup_source', 'none')}, items={len(data['markup'])}")
    logger.info(f"[HANDLE] Is forward: {data['is_forward']}")
    
    # Применяем разметку
    text = data['text']
    if data['markup']:
        text = apply_markup(text, data['markup'])
        logger.info(f"[HANDLE] Markup applied, result_len={len(text)}")
    
    # Добавляем индикатор форварда
    if data['is_forward'] and data.get('forward_info'):
        fwd_info = data['forward_info']
        fwd_prefix = f"🔄 Переслано"
        if fwd_info.get('chat_id'):
            fwd_prefix += f" из {fwd_info['chat_id']}"
        if text:
            text = f"{fwd_prefix}\n\n{text}"
        else:
            text = fwd_prefix
    
    # Отправляем текст
    if text and text.strip():
        ok = await tg.text(text)
        logger.info(f"[HANDLE] Text send: {'OK' if ok else 'FAIL'}")
        await asyncio.sleep(0.2)
    
    # Группируем вложения
    photo_video_items = []
    other_attachments = []
    
    for att in data['attachments']:
        tg_type, meta = media_processor.determine_type(att)
        if tg_type in ('photo', 'video') and meta.get('url'):
            photo_video_items.append({
                'type': tg_type,
                'media': meta['url'],
                'caption': text if len(photo_video_items) == 0 else ''
            })
        else:
            other_attachments.append(att)
    
    # Отправляем группу фото/видео
    if photo_video_items:
        logger.info(f"[HANDLE] Sending media group: {len(photo_video_items)} items")
        ok = await tg.media_group(photo_video_items)
        logger.info(f"[HANDLE] Media group send: {'OK' if ok else 'FAIL'}")
        await asyncio.sleep(0.5)
    
    # Отправляем остальные вложения по одному
    for i, att in enumerate(other_attachments):
        logger.info(f"[HANDLE] Processing attachment {i+1}/{len(other_attachments)}")
        caption = text if i == 0 and not text else ""
        ok = await process_attachment(att, caption)
        delay = 0.3
        if media_processor.determine_type(att)[0] == 'voice':
            delay += 0.2
        await asyncio.sleep(delay)
    
    # Обновляем last_seq
    if seq:
        _last_processed_seq = seq
        logger.info(f"[HANDLE] Updated last_seq to {_last_processed_seq}")
    
    logger.info("[HANDLE] Complete")
    logger.info("=" * 60)


# ===================================================================
# 10. POLLING LOOP
# ===================================================================
async def polling_loop():
    """Основной цикл опроса MAX API."""
    logger.info("🔄 Starting polling loop...")
    await asyncio.sleep(2)
    
    while True:
        try:
            msgs = await mx.fetch(limit=50)
            
            if not msgs:
                logger.debug("[POLL] No messages")
                await asyncio.sleep(POLL_SEC)
                continue
            
            # Сортируем по seq (если есть)
            msgs_with_seq = [(extract_seq_from_msg(m), m) for m in msgs if isinstance(m, dict)]
            msgs_with_seq.sort(key=lambda x: x[0])
            
            # Обрабатываем только новые
            new_count = 0
            for seq, msg in msgs_with_seq:
                if seq > _last_processed_seq or DEBUG_IGNORE_SEQ:
                    await handle_message(msg)
                    new_count += 1
                else:
                    logger.debug(f"[POLL] Skipping seq={seq} (<= {_last_processed_seq})")
            
            logger.info(f"[POLL] Processed {new_count} new messages out of {len(msgs)}")
            
        except Exception as e:
            logger.error(f"[POLL] Exception: {e}", exc_info=True)
        
        await asyncio.sleep(POLL_SEC)


# ===================================================================
# 11. WEB SERVER (HEALTH CHECK)
# ===================================================================
async def health_handler(request):
    """Эндпоинт для проверки здоровья бота."""
    return web.json_response({
        'ok': True,
        'last_seq': _last_processed_seq,
        'debug_ignore_seq': DEBUG_IGNORE_SEQ,
        'ffmpeg_available': media_processor.ffmpeg_available
    })


async def run_app():
    """Запускает веб-сервер и polling loop."""
    app = web.Application()
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    site = web.TCPSite(runner, '0.0.0.0', 8080)
    await site.start()
    
    logger.info("🌐 Health server running on :8080")
    await polling_loop()


# ===================================================================
# 12. ЗАПУСК
# ===================================================================
if __name__ == '__main__':
    try:
        logger.info("🚀 Starting MAX → Telegram Forwarder...")
        asyncio.run(run_app())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
