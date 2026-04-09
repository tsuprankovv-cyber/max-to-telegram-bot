# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
WEBHOOK ВЕРСИЯ - мгновенная доставка без дедупликации
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
import hashlib
from typing import List, Dict, Optional, Any, Tuple, Union
from io import BytesIO
from logging.handlers import RotatingFileHandler

import aiohttp
from aiohttp import web
from mutagen import File as MutagenFile

# ===================================================================
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ (МАКСИМАЛЬНОЕ)
# ===================================================================
LOG_LEVEL = os.getenv('LOG_LEVEL', 'DEBUG').upper()
LOG_RAW_MAX = os.getenv('LOG_RAW_MAX', '1') == '1'
LOG_RAW_TG = os.getenv('LOG_RAW_TG', '1') == '1'
LOG_MARKUP = os.getenv('LOG_MARKUP', '1') == '1'
LOG_MEDIA = os.getenv('LOG_MEDIA', '1') == '1'

file_handler = RotatingFileHandler(
    'bot_debug.log',
    maxBytes=10*1024*1024,
    backupCount=3,
    encoding='utf-8'
)
file_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', '%Y-%m-%d %H:%M:%S'))

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.DEBUG),
    handlers=[console_handler, file_handler]
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
MAX_WEBHOOK_SECRET = os.getenv('MAX_WEBHOOK_SECRET', '').strip()
RENDER_EXTERNAL_URL = os.getenv('RENDER_EXTERNAL_URL', '').strip()

# Для тестирования можно отключить проверку секрета
VERIFY_WEBHOOK_SECRET = os.getenv('VERIFY_WEBHOOK_SECRET', '1') == '1'

logger.info("=" * 100)
logger.info("🚀 MAX → TELEGRAM FORWARDER [WEBHOOK VERSION]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 Webhook URL: {RENDER_EXTERNAL_URL}/webhook")
logger.info(f"🔐 Webhook Secret: {'SET' if MAX_WEBHOOK_SECRET else 'NOT SET'}")
logger.info(f"📊 LOG_LEVEL: {LOG_LEVEL}")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

if not RENDER_EXTERNAL_URL:
    logger.warning("⚠️ RENDER_EXTERNAL_URL not set, webhook registration may fail")

# ===================================================================
# 3. ИСПРАВЛЕНИЕ БИТЫХ HTML ТЕГОВ
# ===================================================================
def fix_broken_html(text: str) -> str:
    """Исправляет незакрытые HTML теги перед отправкой в Telegram."""
    if not text:
        return text
    
    logger.debug(f"[HTML] Input: {text[:100]}...")
    
    tags = ['b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 
            'code', 'pre', 'a', 'tg-spoiler']
    
    fixed = text
    for tag in tags:
        open_pattern = f'<{tag}[^>]*>'
        close_pattern = f'</{tag}>'
        
        open_count = len(re.findall(open_pattern, fixed, re.IGNORECASE))
        close_count = len(re.findall(close_pattern, fixed, re.IGNORECASE))
        
        if open_count > close_count:
            fixed += f'</{tag}>' * (open_count - close_count)
            logger.debug(f"[HTML] Added {open_count - close_count} </{tag}>")
        elif close_count > open_count:
            fixed = re.sub(close_pattern, f'&lt;/{tag}&gt;', fixed, flags=re.IGNORECASE)
            logger.debug(f"[HTML] Escaped extra </{tag}>")
    
    open_a = len(re.findall(r'<a\s+[^>]*>', fixed, re.IGNORECASE))
    close_a = len(re.findall(r'</a>', fixed, re.IGNORECASE))
    if open_a > close_a:
        fixed += '</a>' * (open_a - close_a)
        logger.debug(f"[HTML] Added {open_a - close_a} </a>")
    
    logger.debug(f"[HTML] Output: {fixed[:100]}...")
    return fixed


# ===================================================================
# 4. ГРАФЕМЫ И РАЗМЕТКА
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
    """Ищет разметку во всех возможных полях сообщения MAX."""
    markup_fields = [
        'markup', 'entities', 'formats', 'styles', 
        'annotations', 'text_entities', 'message_entities',
        'rich_text', 'formatting'
    ]
    
    logger.debug(f"[MARKUP] Searching in message keys: {list(msg.keys())}")
    
    # 1. Проверяем body
    body = msg.get('body', {})
    if isinstance(body, dict):
        logger.debug(f"[MARKUP] Body keys: {list(body.keys())}")
        for field in markup_fields:
            if field in body and body[field]:
                logger.info(f"[MARKUP] ✅ Found in body.{field} ({len(body[field])} items)")
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP] Raw: {json.dumps(body[field], ensure_ascii=False)[:500]}")
                return body[field], f"body.{field}"
    
    # 2. Проверяем корень
    for field in markup_fields:
        if field in msg and msg[field]:
            logger.info(f"[MARKUP] ✅ Found in root.{field} ({len(msg[field])} items)")
            if LOG_MARKUP:
                logger.debug(f"[MARKUP] Raw: {json.dumps(msg[field], ensure_ascii=False)[:500]}")
            return msg[field], f"root.{field}"
    
    # 3. Проверяем forward (link.message)
    link = msg.get('link', {})
    if isinstance(link, dict) and 'message' in link:
        inner = link['message']
        logger.debug(f"[MARKUP] Forward message keys: {list(inner.keys())}")
        
        inner_body = inner.get('body', {})
        if isinstance(inner_body, dict):
            for field in markup_fields:
                if field in inner_body and inner_body[field]:
                    logger.info(f"[MARKUP] ✅ Found in forward.body.{field}")
                    return inner_body[field], f"forward.body.{field}"
        
        for field in markup_fields:
            if field in inner and inner[field]:
                logger.info(f"[MARKUP] ✅ Found in forward.{field}")
                return inner[field], f"forward.{field}"
    
    logger.info("[MARKUP] ❌ No markup found in message")
    return [], "none"


def apply_markup(text: str, markup: List[Dict]) -> str:
    """Конвертирует разметку MAX в HTML для Telegram."""
    if not markup or not text:
        return text
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, markup_items={len(markup)}")
    
    TAGS = {
        "strong": ("<b>", "</b>"), "bold": ("<b>", "</b>"),
        "italic": ("<i>", "</i>"), "em": ("<i>", "</i>"),
        "code": ("<code>", "</code>"), "inline-code": ("<code>", "</code>"),
        "pre": ("<pre>", "</pre>"), "preformatted": ("<pre>", "</pre>"),
        "underline": ("<u>", "</u>"), "u": ("<u>", "</u>"),
        "strikethrough": ("<s>", "</s>"), "strike": ("<s>", "</s>"), "s": ("<s>", "</s>"),
        "spoiler": ("<tg-spoiler>", "</tg-spoiler>"),
    }
    
    graphemes = split_into_graphemes(text)
    n = len(graphemes)
    logger.debug(f"[MARKUP] Text split into {n} graphemes")
    
    events = []
    for idx, m in enumerate(markup):
        try:
            start = int(m.get("from") or m.get("offset") or 0)
            length = int(m.get("length") or 0)
            mtype = m.get("type") or m.get("tag") or ""
            end = start + length
            
            if start < 0 or end > n or length <= 0:
                logger.warning(f"[MARKUP] Invalid range: item={idx}, start={start}, length={length}")
                continue
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                events.append((start, 'open', open_tag, idx))
                events.append((end, 'close', close_tag, idx))
                logger.debug(f"[MARKUP] Item {idx}: {mtype} [{start}:{end}]")
            elif mtype in ("link", "text_link", "url"):
                url = m.get("url") or m.get("href") or ""
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    events.append((start, 'open', f'<a href="{url_safe}">', idx))
                    events.append((end, 'close', '</a>', idx))
                    logger.debug(f"[MARKUP] Item {idx}: link [{start}:{end}] -> {url[:50]}")
            else:
                logger.debug(f"[MARKUP] Item {idx}: {mtype} (skipped, type not supported)")
        except Exception as e:
            logger.error(f"[MARKUP] Error processing item {idx}: {e}")
    
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
    logger.info(f"[MARKUP] ✅ Conversion complete: output_len={len(final_text)}")
    logger.debug(f"[MARKUP] Preview: {final_text[:200]}...")
    return final_text


# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ ИЗ СООБЩЕНИЯ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    """Извлекает все данные из сообщения MAX."""
    logger.info(f"[EXTRACT] Starting extraction...")
    logger.debug(f"[EXTRACT] Message keys: {list(msg.keys())}")
    
    # Проверяем forward
    link = msg.get('link', {})
    is_forward = False
    if isinstance(link, dict) and link.get('type') == 'forward' and 'message' in link:
        logger.info(f"[EXTRACT] 📨 This is a FORWARDED message")
        inner = link['message']
        is_forward = True
    else:
        inner = msg
    
    # Извлекаем body
    body = inner.get('body', {})
    if not isinstance(body, dict):
        body = {}
    
    # Текст
    text = body.get('text', '') or inner.get('text', '')
    
    # Разметка
    markup, markup_source = find_markup_in_message(inner)
    
    # Вложения
    attachments = []
    att_list = body.get('attachments') or inner.get('attachments') or []
    if isinstance(att_list, list):
        attachments = [a for a in att_list if isinstance(a, dict)]
    
    # ID сообщения
    mid = body.get('mid') or inner.get('mid') or msg.get('mid', '')
    seq = body.get('seq') or inner.get('seq') or msg.get('seq', 0)
    timestamp = msg.get('timestamp', 0)
    
    result = {
        "mid": mid,
        "seq": seq,
        "timestamp": timestamp,
        "text": text,
        "markup": markup,
        "markup_source": markup_source,
        "attachments": attachments,
        "is_forward": is_forward
    }
    
    logger.info(f"[EXTRACT] ✅ mid={mid}, text_len={len(text)}, attachments={len(attachments)}, markup={len(markup)}, is_forward={is_forward}")
    logger.info(f"[EXTRACT] Markup source: {markup_source}")
    
    return result


# ===================================================================
# 6. АУДИО УТИЛИТЫ
# ===================================================================
def get_audio_duration(file_path: str) -> int:
    """Получает длительность аудио через ffprobe."""
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
               '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            duration = int(float(result.stdout.strip()))
            logger.debug(f"[AUDIO] Duration: {duration}s")
            return duration
    except Exception as e:
        logger.debug(f"[AUDIO] ffprobe error: {e}")
    return 0


def extract_audio_tags(file_data: bytes, filename: str) -> Dict[str, Any]:
    """Извлекает метаданные аудиофайла."""
    logger.info(f"[AUDIO] Extracting tags from: {filename}")
    
    performer = ''
    title = ''
    duration = 0
    
    with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp:
        tmp.write(file_data)
        tmp_path = tmp.name
    
    try:
        audio = MutagenFile(tmp_path)
        if audio:
            if hasattr(audio, 'info') and hasattr(audio.info, 'length'):
                duration = int(audio.info.length)
            if hasattr(audio, 'tags'):
                tags = audio.tags
                if tags:
                    if 'TPE1' in tags:
                        performer = str(tags['TPE1'])
                    elif '©ART' in tags:
                        performer = str(tags['©ART'])
                    if 'TIT2' in tags:
                        title = str(tags['TIT2'])
                    elif '©nam' in tags:
                        title = str(tags['©nam'])
    except Exception as e:
        logger.debug(f"[AUDIO] Mutagen error: {e}")
    
    if duration == 0 and os.path.exists(tmp_path):
        duration = get_audio_duration(tmp_path)
    
    os.unlink(tmp_path)
    
    # Fallback: парсим имя файла
    name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    if ' - ' in name:
        parts = name.split(' - ', 1)
        performer = performer or parts[0].strip()
        title = title or parts[1].strip()
    else:
        title = title or name.strip()
        performer = performer or 'Unknown Artist'
    
    logger.info(f"[AUDIO] ✅ Tags: performer='{performer}', title='{title}', duration={duration}s")
    
    return {
        'performer': performer[:64],
        'title': title[:64],
        'duration': duration
    }


def convert_to_voice(file_data: bytes) -> Optional[bytes]:
    """Конвертирует аудио в OGG Opus для голосовых сообщений."""
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
    except:
        logger.error("[VOICE] ❌ FFmpeg not available")
        return None
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp_in:
            tmp_in.write(file_data)
            tmp_in_path = tmp_in.name
        
        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_out:
            tmp_out_path = tmp_out.name
        
        cmd = [
            'ffmpeg', '-i', tmp_in_path,
            '-ac', '1',
            '-ar', '16000',
            '-c:a', 'libopus',
            '-b:a', '16k',
            '-vbr', 'on',
            '-application', 'voip',
            '-y',
            tmp_out_path
        ]
        
        logger.info(f"[VOICE] 🎤 Converting: {len(file_data)} bytes -> OGG Opus...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            logger.error(f"[VOICE] ❌ FFmpeg error: {result.stderr[:300] if result.stderr else 'unknown'}")
            os.unlink(tmp_in_path)
            return None
        
        with open(tmp_out_path, 'rb') as f:
            ogg_data = f.read()
        
        os.unlink(tmp_in_path)
        os.unlink(tmp_out_path)
        
        logger.info(f"[VOICE] ✅ Converted: {len(file_data)} -> {len(ogg_data)} bytes")
        return ogg_data
        
    except Exception as e:
        logger.error(f"[VOICE] ❌ Exception: {e}")
        return None


# ===================================================================
# 7. MEDIA PROCESSOR
# ===================================================================
class MediaProcessor:
    """Определяет тип медиа и подготавливает к отправке."""
    
    PHOTO_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic', 'heif', 'tiff'}
    VIDEO_EXTS = {'mp4', 'mov', 'avi', 'mkv', 'webm', 'flv', 'm4v', '3gp', 'wmv'}
    AUDIO_EXTS = {'mp3', 'wav', 'm4a', 'flac', 'aac', 'wma', 'alac', 'aiff'}
    VOICE_EXTS = {'ogg', 'opus', 'oga'}
    
    def __init__(self):
        self.ffmpeg_ok = self._check_ffmpeg()
        logger.info(f"[MEDIA] FFmpeg available: {self.ffmpeg_ok}")
    
    def _check_ffmpeg(self) -> bool:
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            return True
        except:
            return False
    
    def determine(self, att: Dict) -> Tuple[str, Dict]:
        """Определяет тип медиа и возвращает метаданные."""
        atype = att.get('type') or att.get('media_type') or 'file'
        payload = att.get('payload', {})
        
        fname = payload.get('filename') or att.get('filename') or ''
        size = payload.get('size') or att.get('size') or 0
        url = payload.get('url')
        token = payload.get('token') or att.get('token') or att.get('file_token')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        
        logger.info(f"[MEDIA] 🔍 Analyzing: atype='{atype}', ext='{ext}', size={size}, filename='{fname}'")
        
        if LOG_MEDIA:
            logger.debug(f"[MEDIA] Full attachment: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        meta = {
            'filename': fname,
            'size': size,
            'url': url,
            'token': token,
            'ext': ext,
            'original_type': atype
        }
        
        # ПРАВИЛО 1: Явный тип от MAX (НАИВЫСШИЙ ПРИОРИТЕТ)
        if atype == 'voice':
            logger.info(f"[MEDIA] ✅ DETERMINED: voice (explicit type from MAX)")
            return 'voice', meta
        if atype == 'audio':
            logger.info(f"[MEDIA] ✅ DETERMINED: audio (explicit type from MAX)")
            return 'audio', meta
        if atype == 'video':
            logger.info(f"[MEDIA] ✅ DETERMINED: video (explicit type from MAX)")
            return 'video', meta
        if atype in ('image', 'photo', 'picture'):
            logger.info(f"[MEDIA] ✅ DETERMINED: photo (explicit type from MAX)")
            return 'photo', meta
        
        # ПРАВИЛО 2: По расширению
        if ext in self.VOICE_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: voice (extension .{ext})")
            return 'voice', meta
        if ext in self.AUDIO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: audio (extension .{ext})")
            return 'audio', meta
        if ext in self.PHOTO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: photo (extension .{ext})")
            return 'photo', meta
        if ext in self.VIDEO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: video (extension .{ext})")
            return 'video', meta
        
        # ПРАВИЛО 3: Всё остальное - документ
        logger.info(f"[MEDIA] 📄 DETERMINED: document (fallback)")
        return 'document', meta


# ===================================================================
# 8. TELEGRAM CLIENT
# ===================================================================
class TG:
    """Клиент Telegram Bot API."""
    
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
        logger.info(f"[TG] Initialized: chat_id={chat_id}")
    
    async def init(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def _request(self, method: str, **kw) -> Optional[Dict]:
        """Отправляет запрос к Telegram API."""
        await self.init()
        
        logger.info(f"[TG] ▶️ {method}")
        if LOG_RAW_TG:
            logger.debug(f"[TG-REQ] {json.dumps(kw, default=str, ensure_ascii=False)[:300]}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                logger.info(f"[TG] Status: {r.status}")
                
                if LOG_RAW_TG:
                    logger.info(f"[TG-RESP] {txt[:500]}")
                
                try:
                    resp = json.loads(txt)
                except json.JSONDecodeError:
                    logger.error(f"[TG] ❌ Invalid JSON: {txt[:200]}")
                    return None
                
                if r.status == 200 and resp.get('ok'):
                    msg_id = resp.get('result', {}).get('message_id')
                    logger.info(f"[TG] ✅ Success: message_id={msg_id}")
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] ⏳ Rate limit, waiting {wait}s...")
                    await asyncio.sleep(wait)
                    return await self._request(method, **kw)
                else:
                    logger.error(f"[TG] ❌ Error {resp.get('error_code')}: {resp.get('description')}")
                    return resp
                    
        except Exception as e:
            logger.error(f"[TG] ❌ Exception: {e}")
            return None
    
    async def send_text(self, text: str) -> bool:
        """Отправляет текстовое сообщение."""
        if not text or not text.strip():
            logger.debug("[TG] Empty text, skipping")
            return True
        
        text = fix_broken_html(text)
        logger.info(f"[TG] 📤 Sending text: {text[:50]}...")
        
        resp = await self._request('sendMessage', json={
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False
        })
        return resp is not None and resp.get('ok', False)
    
    async def send_media(self, media_type: str, media_data: Union[str, bytes],
                         caption: str = "", filename: str = "", 
                         is_url: bool = False, **extra) -> bool:
        """Отправляет медиафайл."""
        method_map = {
            'photo': 'sendPhoto',
            'video': 'sendVideo',
            'audio': 'sendAudio',
            'voice': 'sendVoice',
            'document': 'sendDocument'
        }
        method = method_map.get(media_type, 'sendDocument')
        field = media_type if media_type != 'document' else 'document'
        
        logger.info(f"[TG] 📤 Sending {media_type}: is_url={is_url}, size={len(media_data) if isinstance(media_data, bytes) else 'N/A'}")
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        if is_url:
            form.add_field(field, media_data)
            logger.debug(f"[TG] Using URL: {str(media_data)[:80]}...")
        else:
            fname = filename or f"{media_type}.file"
            form.add_field(field, media_data, filename=fname)
            logger.debug(f"[TG] Using file: {fname}")
        
        if caption and media_type != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption[:1024])
            form.add_field('parse_mode', 'HTML')
            logger.debug(f"[TG] Caption: {caption[:50]}...")
        
        if media_type == 'audio':
            if extra.get('performer'):
                form.add_field('performer', extra['performer'][:64])
            if extra.get('title'):
                form.add_field('title', extra['title'][:64])
            if extra.get('duration'):
                form.add_field('duration', str(extra['duration']))
            logger.debug(f"[TG] Audio extra: {extra}")
        
        if media_type == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
        
        resp = await self._request(method, data=form)
        return resp is not None and resp.get('ok', False)


# ===================================================================
# 9. MAX CLIENT (ТОЛЬКО ДЛЯ СКАЧИВАНИЯ ФАЙЛОВ И РЕГИСТРАЦИИ WEBHOOK)
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
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def register_webhook(self, webhook_url: str, secret: str = "") -> bool:
        """Регистрирует webhook в MAX API."""
        await self.init()
        
        logger.info(f"[MAX] 🔗 Registering webhook: {webhook_url}")
        
        body = {
            "url": webhook_url,
            "update_types": ["message_created", "bot_started"]
        }
        if secret:
            body["secret"] = secret
        
        headers = {
            'Authorization': self.token,
            'Content-Type': 'application/json'
        }
        
        try:
            async with self.session.post(
                f"{self.base}/subscriptions",
                headers=headers,
                json=body
            ) as r:
                text = await r.text()
                logger.info(f"[MAX] Webhook registration response: {r.status}")
                logger.debug(f"[MAX] Response body: {text}")
                
                if r.status == 200:
                    logger.info(f"[MAX] ✅ Webhook registered successfully")
                    return True
                else:
                    logger.error(f"[MAX] ❌ Webhook registration failed: {text}")
                    return False
        except Exception as e:
            logger.error(f"[MAX] ❌ Webhook registration exception: {e}")
            return False
    
    async def download_file(self, token: str) -> Optional[bytes]:
        """Скачивает файл по токену."""
        await self.init()
        
        urls = [
            f"{self.base}/files/{token}/download",
            f"{self.base}/v1/files/{token}/download",
        ]
        
        headers_variants = [
            {'Authorization': self.token},
            {'Authorization': f'Bearer {self.token}'},
        ]
        
        for url in urls:
            for headers in headers_variants:
                try:
                    logger.debug(f"[MAX] 📥 Download attempt: {url[:60]}...")
                    
                    async with self.session.get(url, headers=headers) as r:
                        if r.status == 200:
                            data = await r.read()
                            logger.info(f"[MAX] ✅ Downloaded {len(data)} bytes")
                            return data
                        else:
                            text = await r.text()
                            logger.debug(f"[MAX] Download failed: {r.status} - {text[:100]}")
                except Exception as e:
                    logger.debug(f"[MAX] Download error: {e}")
        
        logger.error(f"[MAX] ❌ All download attempts failed for token: {token[:30]}...")
        return None


# ===================================================================
# 10. ОБРАБОТЧИКИ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


async def process_attachment(att: Dict, caption: str = "") -> bool:
    """Обрабатывает одно вложение."""
    logger.info(f"[ATT] 📎 Processing attachment...")
    
    if not isinstance(att, dict):
        logger.warning("[ATT] ❌ Not a dict, skipping")
        return False
    
    tg_type, meta = media_proc.determine(att)
    
    logger.info(f"[ATT] Type: {tg_type}, filename: {meta.get('filename')}, size: {meta.get('size')}")
    logger.info(f"[ATT] Has URL: {bool(meta.get('url'))}, Has token: {bool(meta.get('token'))}")
    
    media_input = None
    is_url = False
    file_data = None
    
    # Документы, аудио, голосовые - ВСЕГДА через токен
    if tg_type in ('document', 'audio', 'voice'):
        if not meta.get('token'):
            logger.error(f"[ATT] ❌ No token for {tg_type}")
            return False
        
        logger.info(f"[ATT] 📥 Downloading {tg_type} via token...")
        file_data = await mx.download_file(meta['token'])
        
        if not file_data:
            logger.error(f"[ATT] ❌ Download failed")
            return False
        
        logger.info(f"[ATT] ✅ Downloaded {len(file_data)} bytes")
        media_input = file_data
        is_url = False
    
    # Фото/видео - можно по URL
    elif tg_type in ('photo', 'video') and meta.get('url'):
        logger.info(f"[ATT] 📤 Using URL for {tg_type}")
        media_input = meta['url']
        is_url = True
    
    # Fallback - если есть токен
    elif meta.get('token'):
        logger.info(f"[ATT] 📥 Downloading via token (fallback)...")
        file_data = await mx.download_file(meta['token'])
        if not file_data:
            return False
        media_input = file_data
        is_url = False
    
    else:
        logger.error(f"[ATT] ❌ No URL and no token")
        return False
    
    extra = {}
    
    # Конвертация аудио в голосовое (если маленький файл)
    if tg_type == 'audio' and file_data and meta.get('size', 0) < 2 * 1024 * 1024 and media_proc.ffmpeg_ok:
        logger.info(f"[ATT] 🎤 Trying voice conversion (size={meta.get('size')} < 2MB)...")
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type = 'voice'
            media_input = voice_data
            is_url = False
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                tmp_path = tmp.name
            extra['duration'] = get_audio_duration(tmp_path)
            os.unlink(tmp_path)
            logger.info(f"[ATT] ✅ Converted to voice, duration={extra['duration']}s")
        else:
            logger.warning(f"[ATT] ⚠️ Conversion failed, sending as audio")
    
    # Теги для аудио
    if tg_type == 'audio' and file_data:
        tags = extract_audio_tags(file_data, meta.get('filename', ''))
        extra.update(tags)
        logger.info(f"[ATT] Audio tags: {tags}")
    
    # Отправка
    logger.info(f"[ATT] 📤 Sending {tg_type} to Telegram...")
    
    result = await tg.send_media(
        media_type=tg_type,
        media_data=media_input,
        caption=caption if tg_type != 'document' else '',
        filename=meta.get('filename', ''),
        is_url=is_url,
        **extra
    )
    
    logger.info(f"[ATT] {'✅' if result else '❌'} Send result: {'OK' if result else 'FAIL'}")
    return result


async def handle_max_message(msg: Dict):
    """Обрабатывает одно сообщение от MAX."""
    logger.info("=" * 80)
    logger.info("[HANDLE] 🚀 Processing MAX message")
    
    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw message: {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    # Извлекаем данные
    data = extract_message_data(msg)
    
    if not data['text'] and not data['attachments']:
        logger.info("[HANDLE] ⏭ Empty message, skipping")
        return
    
    # Применяем разметку
    text = data['text']
    if data['markup']:
        logger.info(f"[HANDLE] Applying markup from {data['markup_source']}...")
        text = apply_markup(text, data['markup'])
    
    # Отправляем текст
    if text and text.strip():
        ok = await tg.send_text(text)
        if not ok:
            logger.error("[HANDLE] ❌ Failed to send text")
        await asyncio.sleep(0.3)
    
    # Отправляем вложения
    for i, att in enumerate(data['attachments']):
        logger.info(f"[HANDLE] Processing attachment {i+1}/{len(data['attachments'])}")
        caption = text if i == 0 and not text else ""
        await process_attachment(att, caption)
        await asyncio.sleep(0.5)
    
    logger.info("[HANDLE] ✅ Message processing complete")
    logger.info("=" * 80)


# ===================================================================
# 11. WEBHOOK HANDLER
# ===================================================================
async def webhook_handler(request):
    """Принимает webhook от MAX API."""
    logger.info("=" * 60)
    logger.info("[WEBHOOK] 📨 Incoming request")
    
    # Проверяем метод
    if request.method != 'POST':
        logger.warning(f"[WEBHOOK] Invalid method: {request.method}")
        return web.Response(status=405, text="Method Not Allowed")
    
    # Проверяем секретный заголовок
    if VERIFY_WEBHOOK_SECRET and MAX_WEBHOOK_SECRET:
        secret = request.headers.get('X-Max-Bot-Api-Secret')
        if secret != MAX_WEBHOOK_SECRET:
            logger.warning(f"[WEBHOOK] ❌ Invalid secret: {secret[:10] if secret else 'None'}...")
            return web.Response(status=403, text="Forbidden")
        logger.info("[WEBHOOK] ✅ Secret verified")
    else:
        logger.info("[WEBHOOK] ⚠️ Secret verification disabled")
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] Body keys: {list(body.keys())}")
        
        if LOG_RAW_MAX:
            logger.debug(f"[WEBHOOK] Full body: {json.dumps(body, ensure_ascii=False)[:1500]}")
        
        # Определяем тип обновления
        update_type = body.get('update_type', 'unknown')
        logger.info(f"[WEBHOOK] Update type: {update_type}")
        
        if update_type == 'message_created':
            msg = body.get('message', {})
            if msg:
                # Обрабатываем асинхронно, чтобы не блокировать ответ
                asyncio.create_task(handle_max_message(msg))
                logger.info("[WEBHOOK] ✅ Queued for processing")
            else:
                logger.warning("[WEBHOOK] No message in update")
        
        elif update_type == 'bot_started':
            logger.info("[WEBHOOK] Bot was started by user")
        
        else:
            logger.info(f"[WEBHOOK] Unhandled update type: {update_type}")
        
        # Всегда отвечаем 200 OK
        return web.Response(status=200, text="OK")
        
    except json.JSONDecodeError as e:
        logger.error(f"[WEBHOOK] ❌ Invalid JSON: {e}")
        return web.Response(status=400, text="Bad Request")
    except Exception as e:
        logger.error(f"[WEBHOOK] ❌ Exception: {e}", exc_info=True)
        return web.Response(status=500, text="Internal Server Error")
    finally:
        logger.info("=" * 60)


async def health_handler(request):
    """Health check для Render."""
    return web.json_response({
        'ok': True,
        'service': 'MAX → Telegram Forwarder',
        'version': 'webhook'
    })


# ===================================================================
# 12. ЗАПУСК
# ===================================================================
async def main():
    """Точка входа."""
    logger.info("🚀 Starting MAX → Telegram Forwarder [WEBHOOK VERSION]...")
    
    # Регистрируем webhook если есть URL
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        await mx.register_webhook(webhook_url, MAX_WEBHOOK_SECRET)
    else:
        logger.warning("⚠️ RENDER_EXTERNAL_URL not set, skipping webhook registration")
    
    # Запускаем веб-сервер
    app = web.Application()
    app.router.add_post('/webhook', webhook_handler)
    app.router.add_get('/webhook', webhook_handler)  # Для проверки
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.getenv('PORT', 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    
    logger.info(f"🌐 Server running on port {port}")
    logger.info(f"📡 Webhook endpoint: /webhook")
    logger.info(f"💓 Health check: /health")
    logger.info("✅ Ready to receive messages from MAX!")
    
    # Держим сервер запущенным
    await asyncio.Event().wait()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
