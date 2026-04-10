# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
WEBHOOK ВЕРСИЯ - ФИНАЛЬНАЯ
- Исправлено форматирование (комбинации тегов)
- Прямые ссылки для скачивания
- Транслитерация имён файлов
- Максимальное логирование
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
VERIFY_WEBHOOK_SECRET = os.getenv('VERIFY_WEBHOOK_SECRET', '1') == '1'

logger.info("=" * 100)
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL VERSION - FIXED MARKUP]")
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
    
    return fixed


# ===================================================================
# 4. ТРАНСЛИТЕРАЦИЯ
# ===================================================================
def transliterate_ru_to_en(text: str) -> str:
    """Транслитерирует русский текст в латиницу."""
    mapping = {
        'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo',
        'ж': 'zh', 'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm',
        'н': 'n', 'о': 'o', 'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u',
        'ф': 'f', 'х': 'kh', 'ц': 'ts', 'ч': 'ch', 'ш': 'sh', 'щ': 'sch',
        'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya',
        'А': 'A', 'Б': 'B', 'В': 'V', 'Г': 'G', 'Д': 'D', 'Е': 'E', 'Ё': 'Yo',
        'Ж': 'Zh', 'З': 'Z', 'И': 'I', 'Й': 'Y', 'К': 'K', 'Л': 'L', 'М': 'M',
        'Н': 'N', 'О': 'O', 'П': 'P', 'Р': 'R', 'С': 'S', 'Т': 'T', 'У': 'U',
        'Ф': 'F', 'Х': 'Kh', 'Ц': 'Ts', 'Ч': 'Ch', 'Ш': 'Sh', 'Щ': 'Sch',
        'Ъ': '', 'Ы': 'Y', 'Ь': '', 'Э': 'E', 'Ю': 'Yu', 'Я': 'Ya'
    }
    
    result = ''
    for char in text:
        result += mapping.get(char, char)
    
    result = re.sub(r'[^a-zA-Z0-9._-]', '_', result)
    result = re.sub(r'_+', '_', result)
    
    return result


def safe_filename(filename: str) -> str:
    """Создаёт безопасное имя файла с транслитерацией."""
    if not filename:
        return 'file'
    
    if '.' in filename:
        name, ext = filename.rsplit('.', 1)
    else:
        name, ext = filename, ''
    
    safe_name = transliterate_ru_to_en(name)
    
    if not safe_name or safe_name == '_':
        safe_name = 'file'
    
    if len(safe_name) > 100:
        safe_name = safe_name[:100]
    
    safe_name = safe_name.strip('._')
    
    if ext:
        return f"{safe_name}.{ext}"
    return safe_name


# ===================================================================
# 5. ГРАФЕМЫ
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


# ===================================================================
# 6. ПОИСК И КОНВЕРТАЦИЯ РАЗМЕТКИ (ИСПРАВЛЕНО)
# ===================================================================
def find_markup_in_message(msg: Dict) -> Tuple[List[Dict], str]:
    """Ищет разметку в сообщении MAX."""
    logger.info(f"[MARKUP] 🔍 Searching for markup...")
    logger.debug(f"[MARKUP] Message keys: {list(msg.keys())}")
    
    body = msg.get('body', {})
    if isinstance(body, dict):
        logger.debug(f"[MARKUP] Body keys: {list(body.keys())}")
        
        for key in ['entities', 'text_entities', 'markup', 'formats', 'styles']:
            if key in body and body[key]:
                logger.info(f"[MARKUP] body.{key} = {json.dumps(body[key], ensure_ascii=False)[:300]}")
        
        if 'entities' in body and body['entities']:
            logger.info(f"[MARKUP] ✅ Found {len(body['entities'])} entities in body.entities")
            return body['entities'], "body.entities"
        
        if 'text_entities' in body and body['text_entities']:
            logger.info(f"[MARKUP] ✅ Found {len(body['text_entities'])} entities in body.text_entities")
            return body['text_entities'], "body.text_entities"
        
        if 'markup' in body and body['markup']:
            logger.info(f"[MARKUP] ✅ Found {len(body['markup'])} items in body.markup")
            return body['markup'], "body.markup"
    
    for field in ['entities', 'text_entities', 'markup']:
        if field in msg and msg[field]:
            logger.info(f"[MARKUP] ✅ Found {len(msg[field])} items in root.{field}")
            return msg[field], f"root.{field}"
    
    link = msg.get('link', {})
    if isinstance(link, dict) and 'message' in link:
        inner = link['message']
        inner_body = inner.get('body', {})
        if isinstance(inner_body, dict):
            for field in ['entities', 'text_entities', 'markup']:
                if field in inner_body and inner_body[field]:
                    logger.info(f"[MARKUP] ✅ Found in forward.body.{field}")
                    return inner_body[field], f"forward.body.{field}"
    
    logger.warning("[MARKUP] ❌ No markup found")
    return [], "none"


def apply_markup(text: str, entities: List[Dict]) -> str:
    """
    Конвертирует entities в HTML для Telegram.
    Правильно обрабатывает множественные сущности на одном диапазоне.
    """
    if not entities or not text:
        return text
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, entities={len(entities)}")
    start_time = time.time()
    
    TAG_MAP = {
        'bold': 'b', 'strong': 'b',
        'italic': 'i', 'em': 'i',
        'underline': 'u', 'u': 'u',
        'strikethrough': 's', 'strike': 's', 's': 's',
        'code': 'code', 'inline-code': 'code',
        'pre': 'pre', 'preformatted': 'pre',
        'spoiler': 'tg-spoiler',
        'text_link': 'a', 'link': 'a',
    }
    
    graphemes = split_into_graphemes(text)
    n = len(graphemes)
    
    # Для каждой позиции храним множество открывающих и закрывающих тегов
    opens_at = [set() for _ in range(n + 1)]
    closes_at = [set() for _ in range(n + 1)]
    
    # Атрибуты для ссылок
    link_attrs = {}
    
    for idx, entity in enumerate(entities):
        try:
            offset = int(entity.get('offset', 0))
            length = int(entity.get('length', 0))
            etype = entity.get('type', '')
            
            if offset < 0 or length <= 0 or offset + length > n:
                continue
            
            if etype not in TAG_MAP:
                continue
            
            tag_name = TAG_MAP[etype]
            
            opens_at[offset].add(tag_name)
            closes_at[offset + length].add(tag_name)
            
            if etype in ('text_link', 'link'):
                url = entity.get('url') or entity.get('href') or ''
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    link_attrs[(offset, tag_name)] = f' href="{url_safe}"'
            
            logger.debug(f"[MARKUP] {etype}: [{offset}:{offset+length}]")
            
        except Exception as e:
            logger.error(f"[MARKUP] Error: {e}")
    
    # Строим результат
    result = []
    open_stack = []
    
    # Порядок тегов для детерминированного вывода
    tag_order = ['b', 'i', 'u', 's', 'code', 'pre', 'tg-spoiler', 'a']
    
    for pos in range(n + 1):
        # Закрываем теги
        if closes_at[pos]:
            to_close = []
            for tag in reversed(open_stack):
                if tag in closes_at[pos]:
                    to_close.append(tag)
            
            for tag in to_close:
                result.append(f'</{tag}>')
                open_stack.remove(tag)
        
        # Открываем теги
        if opens_at[pos]:
            sorted_tags = sorted(opens_at[pos], key=lambda t: tag_order.index(t) if t in tag_order else 999)
            
            for tag_name in sorted_tags:
                if tag_name not in open_stack:
                    key = (pos, tag_name)
                    attrs = link_attrs.get(key, '')
                    result.append(f'<{tag_name}{attrs}>')
                    open_stack.append(tag_name)
        
        if pos < n:
            result.append(graphemes[pos])
    
    # Закрываем оставшиеся
    for tag_name in reversed(open_stack):
        result.append(f'</{tag_name}>')
    
    final_text = ''.join(result)
    elapsed = time.time() - start_time
    
    logger.info(f"[MARKUP] ✅ Converted in {elapsed:.2f}s: {len(text)} → {len(final_text)} chars")
    logger.debug(f"[MARKUP] Preview: {final_text[:200]}...")
    
    return final_text


# ===================================================================
# 7. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    """Извлекает все данные из сообщения MAX."""
    logger.info(f"[EXTRACT] Starting extraction...")
    logger.debug(f"[EXTRACT] Message keys: {list(msg.keys())}")
    
    link = msg.get('link', {})
    is_forward = False
    if isinstance(link, dict) and link.get('type') == 'forward' and 'message' in link:
        logger.info(f"[EXTRACT] 📨 This is a FORWARDED message")
        inner = link['message']
        is_forward = True
    else:
        inner = msg
    
    body = inner.get('body', {})
    if not isinstance(body, dict):
        body = {}
    
    text = body.get('text', '') or inner.get('text', '')
    markup, markup_source = find_markup_in_message(inner)
    
    attachments = []
    att_list = body.get('attachments') or inner.get('attachments') or []
    if isinstance(att_list, list):
        attachments = [a for a in att_list if isinstance(a, dict)]
    
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
    
    logger.info(f"[EXTRACT] ✅ mid={mid[:30]}..., text_len={len(text)}, attachments={len(attachments)}, markup={len(markup)}")
    logger.info(f"[EXTRACT] Markup source: {markup_source}")
    
    return result


# ===================================================================
# 8. АУДИО УТИЛИТЫ
# ===================================================================
def get_audio_duration(file_path: str) -> int:
    """Получает длительность аудио через ffprobe."""
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
               '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return int(float(result.stdout.strip()))
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
    
    safe_name = safe_filename(filename)
    name = safe_name.rsplit('.', 1)[0] if '.' in safe_name else safe_name
    
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
# 9. СКАЧИВАНИЕ ПО ПРЯМОЙ ССЫЛКЕ
# ===================================================================
async def download_from_url(url: str) -> Optional[bytes]:
    """Скачивает файл по прямой ссылке из payload.url."""
    if not url:
        logger.error("[DOWNLOAD] ❌ Empty URL")
        return None
    
    logger.info(f"[DOWNLOAD] 📥 Downloading from: {url[:100]}...")
    start_time = time.time()
    
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
                elapsed = time.time() - start_time
                
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[DOWNLOAD] ✅ Downloaded {len(data)} bytes in {elapsed:.2f}s")
                    return data
                else:
                    text = await r.text()
                    logger.error(f"[DOWNLOAD] ❌ HTTP {r.status} in {elapsed:.2f}s: {text[:200]}")
                    return None
    except asyncio.TimeoutError:
        logger.error(f"[DOWNLOAD] ❌ Timeout after 120s")
        return None
    except Exception as e:
        logger.error(f"[DOWNLOAD] ❌ Exception: {e}")
        return None


# ===================================================================
# 10. MEDIA PROCESSOR
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
        """Определяет тип медиа с учётом atype от MAX."""
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
        
        # ПРАВИЛО 1: ЯВНЫЙ ТИП ОТ MAX
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
        
        # ПРАВИЛО 2: type=file - смотрим расширение
        if atype == 'file':
            if ext in self.VOICE_EXTS:
                logger.info(f"[MEDIA] ✅ DETERMINED: voice (file with .{ext})")
                return 'voice', meta
            if ext in self.AUDIO_EXTS:
                logger.info(f"[MEDIA] ✅ DETERMINED: audio (file with .{ext})")
                return 'audio', meta
            if ext in self.PHOTO_EXTS:
                logger.info(f"[MEDIA] ✅ DETERMINED: photo (file with .{ext})")
                return 'photo', meta
            if ext in self.VIDEO_EXTS:
                logger.info(f"[MEDIA] ✅ DETERMINED: video (file with .{ext})")
                return 'video', meta
            logger.info(f"[MEDIA] 📄 DETERMINED: document (file with .{ext})")
            return 'document', meta
        
        # ПРАВИЛО 3: ПО РАСШИРЕНИЮ
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
        
        logger.info(f"[MEDIA] 📄 DETERMINED: document (fallback)")
        return 'document', meta


# ===================================================================
# 11. TELEGRAM CLIENT
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
        
        safe_fname = safe_filename(filename) if filename else f"{media_type}.file"
        
        if is_url:
            form.add_field(field, media_data)
            logger.debug(f"[TG] Using URL: {str(media_data)[:80]}...")
        else:
            form.add_field(field, media_data, filename=safe_fname)
            logger.debug(f"[TG] Using file: {safe_fname}")
        
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
# 12. MAX CLIENT
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


# ===================================================================
# 13. ОБРАБОТЧИКИ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


async def process_attachment(att: Dict, caption: str = "") -> bool:
    """Обрабатывает одно вложение через прямую ссылку."""
    start_time = time.time()
    logger.info(f"[ATT] 📎 Processing attachment...")
    
    if not isinstance(att, dict):
        logger.warning("[ATT] ❌ Not a dict, skipping")
        return False
    
    tg_type, meta = media_proc.determine(att)
    
    logger.info(f"[ATT] Type: {tg_type}, filename: {meta.get('filename')}, size: {meta.get('size')}")
    logger.info(f"[ATT] Has URL: {bool(meta.get('url'))}, Has token: {bool(meta.get('token'))}")
    
    direct_url = meta.get('url')
    
    if not direct_url:
        logger.error("[ATT] ❌ No direct URL in payload")
        return False
    
    file_data = None
    
    if tg_type in ('photo', 'video') and direct_url:
        logger.info(f"[ATT] 📤 Using direct URL for {tg_type}")
        result = await tg.send_media(
            media_type=tg_type,
            media_data=direct_url,
            caption=caption,
            filename=safe_filename(meta.get('filename', '')),
            is_url=True
        )
        elapsed = time.time() - start_time
        logger.info(f"[ATT] {'✅' if result else '❌'} Send result in {elapsed:.2f}s")
        return result
    
    logger.info(f"[ATT] 📥 Downloading {tg_type} from direct URL...")
    file_data = await download_from_url(direct_url)
    
    if not file_data:
        logger.error(f"[ATT] ❌ Download failed")
        return False
    
    extra = {}
    
    if tg_type == 'audio' and meta.get('size', 0) < 2 * 1024 * 1024 and media_proc.ffmpeg_ok:
        logger.info(f"[ATT] 🎤 Trying voice conversion (size={meta.get('size')} < 2MB)...")
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type = 'voice'
            file_data = voice_data
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                tmp_path = tmp.name
            extra['duration'] = get_audio_duration(tmp_path)
            os.unlink(tmp_path)
            logger.info(f"[ATT] ✅ Converted to voice, duration={extra['duration']}s")
        else:
            logger.warning(f"[ATT] ⚠️ Conversion failed, sending as audio")
    
    if tg_type == 'audio':
        tags = extract_audio_tags(file_data, meta.get('filename', ''))
        extra.update(tags)
        logger.info(f"[ATT] Audio tags: {tags}")
    
    logger.info(f"[ATT] 📤 Sending {tg_type} to Telegram...")
    
    result = await tg.send_media(
        media_type=tg_type,
        media_data=file_data,
        caption=caption if tg_type != 'document' else '',
        filename=safe_filename(meta.get('filename', '')),
        is_url=False,
        **extra
    )
    
    elapsed = time.time() - start_time
    logger.info(f"[ATT] {'✅' if result else '❌'} Send result in {elapsed:.2f}s")
    return result


async def handle_max_message(msg: Dict):
    """Обрабатывает одно сообщение от MAX."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info(f"[HANDLE] 🚀 Processing MAX message at {time.strftime('%H:%M:%S')}")
    
    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw message: {json.dumps(msg, ensure_ascii=False)[:1500]}")
    
    data = extract_message_data(msg)
    
    if not data['text'] and not data['attachments']:
        logger.info("[HANDLE] ⏭ Empty message, skipping")
        return
    
    text = data['text']
    if data['markup']:
        logger.info(f"[HANDLE] Applying markup from {data['markup_source']}...")
        text = apply_markup(text, data['markup'])
    
    if text and text.strip():
        text_start = time.time()
        ok = await tg.send_text(text)
        text_elapsed = time.time() - text_start
        if not ok:
            logger.error(f"[HANDLE] ❌ Failed to send text in {text_elapsed:.2f}s")
        else:
            logger.info(f"[HANDLE] ✅ Text sent in {text_elapsed:.2f}s")
        await asyncio.sleep(0.3)
    
    for i, att in enumerate(data['attachments']):
        logger.info(f"[HANDLE] Processing attachment {i+1}/{len(data['attachments'])}")
        caption = text if i == 0 and not text else ""
        await process_attachment(att, caption)
        await asyncio.sleep(0.5)
    
    elapsed = time.time() - start_time
    logger.info(f"[HANDLE] ✅ Message processing complete in {elapsed:.2f}s")
    logger.info("=" * 80)


# ===================================================================
# 14. WEBHOOK HANDLER
# ===================================================================
async def webhook_handler(request):
    """Принимает webhook от MAX API."""
    logger.info("=" * 60)
    logger.info("[WEBHOOK] 📨 Incoming request")
    
    if request.method != 'POST':
        logger.warning(f"[WEBHOOK] Invalid method: {request.method}")
        return web.Response(status=405, text="Method Not Allowed")
    
    if VERIFY_WEBHOOK_SECRET and MAX_WEBHOOK_SECRET:
        secret = request.headers.get('X-Max-Bot-Api-Secret')
        if secret != MAX_WEBHOOK_SECRET:
            logger.warning(f"[WEBHOOK] ❌ Invalid secret")
            return web.Response(status=403, text="Forbidden")
        logger.info("[WEBHOOK] ✅ Secret verified")
    else:
        logger.info("[WEBHOOK] ⚠️ Secret verification disabled")
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] Body keys: {list(body.keys())}")
        
        if LOG_RAW_MAX:
            logger.debug(f"[WEBHOOK] Full body: {json.dumps(body, ensure_ascii=False)[:1500]}")
        
        update_type = body.get('update_type', 'unknown')
        logger.info(f"[WEBHOOK] Update type: {update_type}")
        
        if update_type == 'message_created':
            msg = body.get('message', {})
            if msg:
                asyncio.create_task(handle_max_message(msg))
                logger.info("[WEBHOOK] ✅ Queued for processing")
            else:
                logger.warning("[WEBHOOK] No message in update")
        
        elif update_type == 'bot_started':
            logger.info("[WEBHOOK] Bot was started by user")
        
        else:
            logger.info(f"[WEBHOOK] Unhandled update type: {update_type}")
        
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
        'version': 'webhook-final-fixed'
    })


# ===================================================================
# 15. ЗАПУСК
# ===================================================================
async def main():
    """Точка входа."""
    logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL VERSION - FIXED MARKUP]...")
    
    if RENDER_EXTERNAL_URL:
        webhook_url = f"{RENDER_EXTERNAL_URL}/webhook"
        await mx.register_webhook(webhook_url, MAX_WEBHOOK_SECRET)
    else:
        logger.warning("⚠️ RENDER_EXTERNAL_URL not set, skipping webhook registration")
    
    app = web.Application()
    app.router.add_post('/webhook', webhook_handler)
    app.router.add_get('/webhook', webhook_handler)
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
    
    await asyncio.Event().wait()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped by user")
    except Exception as e:
        logger.exception(f"💥 FATAL ERROR: {e}")
        sys.exit(1)
