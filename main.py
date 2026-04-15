# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ
- Правильная обработка вложенных и пересекающихся тегов (LIFO)
- Прямые offset без графем (эмодзи не сбивают)
- Исправлены дубли при отправке одиночного фото
- Гибридная отправка медиа (старая/новая схема)
- Приоритет имени файла для аудио
- Транслитерация кириллицы
- Полное логирование ответов Telegram
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
# 1. НАСТРОЙКА ЛОГИРОВАНИЯ
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
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL VERSION - LIFO MARKUP]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 Webhook URL: {RENDER_EXTERNAL_URL}/webhook")
logger.info(f"🔐 Webhook Secret: {'SET' if MAX_WEBHOOK_SECRET else 'NOT SET'}")
logger.info(f"📊 LOG_LEVEL: {LOG_LEVEL}")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
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
        elif close_count > open_count:
            fixed = re.sub(close_pattern, f'&lt;/{tag}&gt;', fixed, flags=re.IGNORECASE)
    
    open_a = len(re.findall(r'<a\s+[^>]*>', fixed, re.IGNORECASE))
    close_a = len(re.findall(r'</a>', fixed, re.IGNORECASE))
    if open_a > close_a:
        fixed += '</a>' * (open_a - close_a)
    
    return fixed


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
    return result.strip('._')


def safe_filename(filename: str) -> str:
    """Создаёт безопасное имя файла с транслитерацией."""
    if not filename:
        return 'file'
    if '.' in filename:
        name, ext = filename.rsplit('.', 1)
    else:
        name, ext = filename, ''
    safe_name = transliterate_ru_to_en(name)
    if not safe_name:
        safe_name = 'file'
    if len(safe_name) > 100:
        safe_name = safe_name[:100]
    if ext:
        return f"{safe_name}.{ext}"
    return safe_name


# ===================================================================
# 4. КОНВЕРТАЦИЯ РАЗМЕТКИ MAX → HTML (СТЕКОВАЯ ОБРАБОТКА)
# ===================================================================
MAX_TAG_MAP = {
    "strong": "b", "bold": "b", "b": "b",
    "emphasized": "i", "italic": "i", "em": "i", "i": "i", "emphasis": "i",
    "underline": "u", "u": "u", "ins": "u",
    "strikethrough": "s", "strike": "s", "s": "s", "del": "s",
    "code": "code", "inline-code": "code", "pre": "pre", "preformatted": "pre",
    "spoiler": "tg-spoiler",
    "link": "a", "text_link": "a", "url": "a",
}


def parse_markdown_to_html(text: str) -> str:
    if not text:
        return text
    logger.info(f"[MARKDOWN] Parsing markdown...")
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'\+\+(.+?)\+\+', r'<u>\1</u>', text)
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    return text


def apply_markup(text: str, markup: List[Dict]) -> str:
    """
    Конвертирует разметку MAX в HTML для Telegram.
    Использует стековую обработку для правильной вложенности тегов.
    """
    if not markup or not text:
        return text
    
    all_from_zero = all(m.get('from', 0) == 0 for m in markup)
    max_length = max((m.get('length', 0) for m in markup), default=0)
    
    if all_from_zero and max_length < len(text):
        logger.warning("[MARKUP] ⚠️ Broken markup, switching to Markdown.")
        return parse_markdown_to_html(text)
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, entities={len(markup)}")
    start_time = time.time()
    
    # Создаём события открытия и закрытия тегов
    events = []
    for idx, entity in enumerate(markup):
        try:
            offset = int(entity.get('from', 0))
            length = int(entity.get('length', 0))
            etype = entity.get('type', '')
            
            if offset < 0 or length <= 0 or offset + length > len(text):
                continue
            
            if etype not in MAX_TAG_MAP:
                continue
            
            tag_name = MAX_TAG_MAP[etype]
            
            if etype in ('link', 'text_link', 'url'):
                url = entity.get('url') or entity.get('href') or ''
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<{tag_name} href="{url_safe}">'
                else:
                    open_tag = f'<{tag_name}>'
            else:
                open_tag = f'<{tag_name}>'
            
            close_tag = f'</{tag_name}>'
            
            events.append((offset, 'open', open_tag, close_tag, idx))
            events.append((offset + length, 'close', open_tag, close_tag, idx))
            
            logger.debug(f"[MARKUP] {etype}: [{offset}:{offset+length}] -> <{tag_name}>")
            
        except Exception as e:
            logger.error(f"[MARKUP] Error: {e}")
    
    # Сортируем: сначала по позиции, закрывающие перед открывающими
    events.sort(key=lambda x: (x[0], 0 if x[1] == 'close' else 1, -x[4]))
    
    result = []
    open_stack = []  # Стек открытых тегов: [(close_tag, priority), ...]
    event_idx = 0
    n = len(text)
    
    for pos in range(n + 1):
        # Обрабатываем все события на текущей позиции
        while event_idx < len(events) and events[event_idx][0] == pos:
            _, etype, open_tag, close_tag, priority = events[event_idx]
            if etype == 'close':
                # Ищем соответствующий открывающий тег в стеке
                for i in range(len(open_stack) - 1, -1, -1):
                    if open_stack[i][0] == close_tag and open_stack[i][1] == priority:
                        result.append(close_tag)
                        open_stack.pop(i)
                        break
            else:
                open_stack.append((close_tag, priority))
                result.append(open_tag)
            event_idx += 1
        
        # Добавляем символ
        if pos < n:
            result.append(text[pos])
    
    # Закрываем оставшиеся теги
    for close_tag, _ in reversed(open_stack):
        result.append(close_tag)
    
    final_text = ''.join(result)
    elapsed = time.time() - start_time
    logger.info(f"[MARKUP] ✅ Converted in {elapsed:.2f}s: {len(text)} → {len(final_text)} chars")
    logger.debug(f"[MARKUP] Preview: {final_text[:200]}...")
    
    return final_text


# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    logger.info(f"[EXTRACT] Starting extraction...")
    
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
    markup = body.get('markup', []) or inner.get('markup', [])
    
    attachments = []
    att_list = body.get('attachments') or inner.get('attachments') or []
    if isinstance(att_list, list):
        attachments = [a for a in att_list if isinstance(a, dict)]
    
    mid = body.get('mid') or inner.get('mid') or msg.get('mid', '')
    
    return {
        "mid": mid,
        "text": text,
        "markup": markup,
        "attachments": attachments,
        "is_forward": is_forward
    }


# ===================================================================
# 6. АУДИО УТИЛИТЫ
# ===================================================================
def get_audio_duration(file_path: str) -> int:
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
               '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0:
            return int(float(result.stdout.strip()))
    except:
        pass
    return 0


def safe_audio_tag(tag: str) -> str:
    if not tag:
        return tag
    if re.search(r'[а-яА-Я]', tag):
        return transliterate_ru_to_en(tag)
    return tag


def extract_audio_tags(file_data: bytes, filename: str) -> Dict[str, Any]:
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
    except:
        pass
    
    if duration == 0 and os.path.exists(tmp_path):
        duration = get_audio_duration(tmp_path)
    
    os.unlink(tmp_path)
    
    performer_tag = safe_audio_tag(performer)
    title_tag = safe_audio_tag(title)
    
    base_filename = safe_filename(filename)
    base_name = base_filename.rsplit('.', 1)[0] if '.' in base_filename else base_filename
    
    if performer_tag and title_tag:
        final_performer = base_name
        final_title = f"{title_tag} ({performer_tag})"
    elif performer_tag:
        final_performer = base_name
        final_title = performer_tag
    elif title_tag:
        final_performer = base_name
        final_title = title_tag
    else:
        if ' - ' in base_name:
            parts = base_name.split(' - ', 1)
            final_performer = parts[0].strip()
            final_title = parts[1].strip()
        else:
            final_performer = 'Unknown Artist'
            final_title = base_name
    
    logger.info(f"[AUDIO] ✅ Final: performer='{final_performer}', title='{final_title}', duration={duration}s")
    
    return {
        'performer': final_performer[:64],
        'title': final_title[:64],
        'duration': duration
    }


def convert_to_voice(file_data: bytes) -> Optional[bytes]:
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
            '-ac', '1', '-ar', '16000', '-c:a', 'libopus',
            '-b:a', '16k', '-vbr', 'on', '-application', 'voip',
            '-y', tmp_out_path
        ]
        
        logger.info(f"[VOICE] 🎤 Converting: {len(file_data)} bytes -> OGG Opus...")
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        
        if result.returncode != 0:
            logger.error(f"[VOICE] ❌ FFmpeg error")
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
# 7. СКАЧИВАНИЕ ПО ПРЯМОЙ ССЫЛКЕ
# ===================================================================
async def download_from_url(url: str) -> Optional[bytes]:
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
# 8. MEDIA PROCESSOR
# ===================================================================
class MediaProcessor:
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
        
        if atype == 'voice':
            return 'voice', meta
        if atype == 'audio':
            return 'audio', meta
        if atype == 'video':
            return 'video', meta
        if atype in ('image', 'photo', 'picture'):
            return 'photo', meta
        
        if ext in self.VOICE_EXTS:
            return 'voice', meta
        if ext in self.AUDIO_EXTS:
            return 'audio', meta
        if ext in self.PHOTO_EXTS:
            return 'photo', meta
        if ext in self.VIDEO_EXTS:
            return 'video', meta
        
        return 'document', meta


# ===================================================================
# 9. TELEGRAM CLIENT (С ПОЛНЫМ ЛОГИРОВАНИЕМ)
# ===================================================================
class TG:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
    
    async def init(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def _request(self, method: str, **kw) -> Optional[Dict]:
        await self.init()
        logger.info(f"[TG] ▶️ {method}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                logger.info(f"[TG] Status: {r.status}")
                
                if LOG_RAW_TG:
                    logger.info(f"[TG-RESP] {txt}")  # ПОЛНЫЙ ОТВЕТ БЕЗ ОБРЕЗКИ
                
                try:
                    resp = json.loads(txt)
                except:
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
        if not text or not text.strip():
            return True
        
        text = fix_broken_html(text)
        logger.info(f"[TG] 📤 Sending text: {text[:100]}...")
        
        resp = await self._request('sendMessage', json={
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': 'HTML',
            'disable_web_page_preview': False
        })
        return resp is not None and resp.get('ok', False)
    
    async def send_media_group_direct(self, media_items: List[Dict]) -> bool:
        """Старая схема: отправка медиа-группы по прямым URL."""
        if not media_items:
            return True
        
        if len(media_items) > 10:
            media_items = media_items[:10]
        
        logger.info(f"[TG] 📤 Sending media group (direct URLs): {len(media_items)} items")
        
        input_media = []
        for i, item in enumerate(media_items):
            media_obj = {
                'type': item['type'],
                'media': item['media']
            }
            if i == 0 and item.get('caption'):
                media_obj['caption'] = fix_broken_html(item['caption'])[:1024]
                media_obj['parse_mode'] = 'HTML'
            input_media.append(media_obj)
        
        resp = await self._request('sendMediaGroup', json={
            'chat_id': self.chat_id,
            'media': input_media
        })
        
        if resp and isinstance(resp, dict) and resp.get('ok'):
            results = resp.get('result', [])
            msg_ids = [res.get('message_id') for res in results if isinstance(res, dict)]
            logger.info(f"[TG] ✅ Media group sent: message_ids={msg_ids}")
            return True
        
        logger.error(f"[TG] ❌ Media group failed")
        return False
    
    async def send_media_group_via_download(self, media_items: List[Dict], caption: str = None) -> bool:
        """Новая схема: скачанные файлы -> загрузка в Telegram -> media group по file_id."""
        if not media_items:
            return True
        
        if len(media_items) > 10:
            logger.warning(f"[TG] Media group too large ({len(media_items)}), truncating to 10")
            media_items = media_items[:10]
        
        types = [item['type'] for item in media_items]
        logger.info(f"[TG] 📤 Sending media group (via download): {len(media_items)} items, types={types}")
        
        input_media = []
        for i, item in enumerate(media_items):
            if item['type'] == 'photo':
                form = aiohttp.FormData()
                form.add_field('chat_id', self.chat_id)
                form.add_field('photo', item['data'], filename=item.get('filename', 'photo.jpg'))
                upload_resp = await self._request('sendPhoto', data=form)
                if upload_resp and upload_resp.get('ok'):
                    file_id = upload_resp['result']['photo'][0]['file_id']
                else:
                    logger.error(f"[TG] Failed to upload photo for media group")
                    return False
            elif item['type'] == 'video':
                form = aiohttp.FormData()
                form.add_field('chat_id', self.chat_id)
                form.add_field('video', item['data'], filename=item.get('filename', 'video.mp4'))
                upload_resp = await self._request('sendVideo', data=form)
                if upload_resp and upload_resp.get('ok'):
                    file_id = upload_resp['result']['video']['file_id']
                else:
                    logger.error(f"[TG] Failed to upload video for media group")
                    return False
            else:
                continue
            
            media_obj = {
                'type': item['type'],
                'media': file_id
            }
            if i == 0 and caption:
                media_obj['caption'] = fix_broken_html(caption)[:1024]
                media_obj['parse_mode'] = 'HTML'
            input_media.append(media_obj)
            
            if upload_resp:
                msg_id = upload_resp['result']['message_id']
                await self._request('deleteMessage', json={'chat_id': self.chat_id, 'message_id': msg_id})
        
        resp = await self._request('sendMediaGroup', json={
            'chat_id': self.chat_id,
            'media': input_media
        })
        
        if resp and isinstance(resp, dict) and resp.get('ok'):
            results = resp.get('result', [])
            msg_ids = [res.get('message_id') for res in results if isinstance(res, dict)]
            logger.info(f"[TG] ✅ Media group sent: message_ids={msg_ids}")
            return True
        
        logger.error(f"[TG] ❌ Media group failed")
        return False
    
    async def send_media(self, media_type: str, media_data: Union[str, bytes],
                         caption: str = "", filename: str = "", 
                         is_url: bool = False, **extra) -> bool:
        method_map = {
            'photo': 'sendPhoto', 'video': 'sendVideo',
            'audio': 'sendAudio', 'voice': 'sendVoice',
            'document': 'sendDocument'
        }
        method = method_map.get(media_type, 'sendDocument')
        field = media_type if media_type != 'document' else 'document'
        
        logger.info(f"[TG] 📤 Sending {media_type}: is_url={is_url}")
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        safe_fname = safe_filename(filename) if filename else f"{media_type}.file"
        
        if is_url:
            form.add_field(field, media_data)
        else:
            form.add_field(field, media_data, filename=safe_fname)
        
        if caption and media_type != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption[:1024])
            form.add_field('parse_mode', 'HTML')
        
        if media_type == 'audio':
            if extra.get('performer'):
                form.add_field('performer', extra['performer'][:64])
            if extra.get('title'):
                form.add_field('title', extra['title'][:64])
            if extra.get('duration'):
                form.add_field('duration', str(extra['duration']))
        
        if media_type == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
        
        resp = await self._request(method, data=form)
        return resp is not None and resp.get('ok', False)


# ===================================================================
# 10. MAX CLIENT
# ===================================================================
class MX:
    def __init__(self, token: str, cid: str, base: str):
        self.token = token
        self.cid = cid
        self.base = base
        self.session = None
    
    async def init(self):
        if not self.session:
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def register_webhook(self, webhook_url: str, secret: str = "") -> bool:
        await self.init()
        logger.info(f"[MAX] 🔗 Registering webhook: {webhook_url}")
        
        body = {"url": webhook_url, "update_types": ["message_created"]}
        if secret:
            body["secret"] = secret
        
        headers = {'Authorization': self.token, 'Content-Type': 'application/json'}
        
        try:
            async with self.session.post(f"{self.base}/subscriptions", headers=headers, json=body) as r:
                text = await r.text()
                logger.info(f"[MAX] Webhook registration response: {r.status}")
                
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
# 11. ОБРАБОТЧИКИ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


async def process_attachment(att: Dict, caption: str = "") -> bool:
    start_time = time.time()
    logger.info(f"[ATT] 📎 Processing attachment...")
    
    if not isinstance(att, dict):
        logger.warning("[ATT] ❌ Not a dict, skipping")
        return False
    
    tg_type, meta = media_proc.determine(att)
    
    logger.info(f"[ATT] Type: {tg_type}, filename: {meta.get('filename')}, size: {meta.get('size')}")
    
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
        logger.info(f"[ATT] 🎤 Trying voice conversion...")
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
    """Обрабатывает одно сообщение от MAX с гибридной отправкой медиа."""
    start_time = time.time()
    logger.info("=" * 80)
    logger.info(f"[HANDLE] 🚀 Processing MAX message at {time.strftime('%H:%M:%S')}")

    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw message: {json.dumps(msg, ensure_ascii=False)[:2000]}")

    data = extract_message_data(msg)

    if not data['text'] and not data['attachments']:
        logger.info("[HANDLE] ⏭ Empty message, skipping")
        return

    text = data['text']
    if data['markup']:
        logger.info(f"[HANDLE] Applying markup ({len(data['markup'])} items)...")
        text = apply_markup(text, data['markup'])
    elif text and ('*' in text or '_' in text or '[' in text):
        logger.info("[HANDLE] 📝 Using Markdown parser...")
        text = parse_markdown_to_html(text)

    media_items = []
    other_attachments = []

    for att in data['attachments']:
        tg_type, meta = media_proc.determine(att)
        if tg_type in ('photo', 'video'):
            media_items.append({'type': tg_type, 'attachment': att, 'meta': meta})
        else:
            other_attachments.append(att)

    logger.info(f"[HANDLE] Media items: {len(media_items)}, Other: {len(other_attachments)}")

    use_new_scheme = False
    if len(media_items) > 1:
        has_video = any(item['type'] == 'video' for item in media_items)
        has_photo = any(item['type'] == 'photo' for item in media_items)
        video_count = sum(1 for item in media_items if item['type'] == 'video')
        
        if has_video and (has_photo or video_count >= 2):
            use_new_scheme = True
            logger.info(f"[HANDLE] 🔄 Using NEW scheme (download + file_id) for mixed/video group")
        else:
            logger.info(f"[HANDLE] 📷 Using OLD scheme (direct URLs) for photo collage")

    if media_items:
        if len(media_items) == 1:
            # ОДИНОЧНОЕ ФОТО/ВИДЕО — без media group!
            item = media_items[0]
            logger.info(f"[HANDLE] 📷 Single media, sending via send_media...")
            caption = text
            await process_attachment(item['attachment'], caption)
        elif use_new_scheme:
            downloaded_media = []
            for item in media_items:
                att = item['attachment']
                meta = item['meta']
                direct_url = meta.get('url')
                if not direct_url:
                    logger.warning(f"[HANDLE] No URL for {item['type']}, skipping")
                    continue

                logger.info(f"[HANDLE] 📥 Downloading {item['type']} for reliable group...")
                file_data = await download_from_url(direct_url)
                if file_data:
                    downloaded_media.append({
                        'type': item['type'],
                        'data': file_data,
                        'filename': safe_filename(meta.get('filename', ''))
                    })
                else:
                    logger.error(f"[HANDLE] Failed to download {item['type']}")

            if downloaded_media:
                ok = await tg.send_media_group_via_download(downloaded_media, text)
                if ok:
                    logger.info(f"[HANDLE] ✅ Media group sent successfully (new scheme)")
                else:
                    logger.warning(f"[HANDLE] Media group failed, sending individually...")
                    for i, item in enumerate(downloaded_media):
                        caption = text if i == 0 else ""
                        await tg.send_media(item['type'], item['data'], caption=caption, filename=item['filename'])
                        await asyncio.sleep(0.3)
        else:
            group_items = [
                {
                    'type': item['type'],
                    'media': item['meta'].get('url'),
                    'caption': text if i == 0 else None
                }
                for i, item in enumerate(media_items)
                if item['meta'].get('url')
            ]
            
            if group_items:
                logger.info(f"[HANDLE] 📸 Sending media group with {len(group_items)} items (old scheme)...")
                ok = await tg.send_media_group_direct(group_items)
                if ok:
                    logger.info(f"[HANDLE] ✅ Media group sent successfully (old scheme)")
                else:
                    logger.warning(f"[HANDLE] Media group failed, sending individually...")
                    for i, item in enumerate(media_items):
                        caption = text if i == 0 else ""
                        await process_attachment(item['attachment'], caption)
                        await asyncio.sleep(0.3)
    else:
        if text and text.strip():
            await tg.send_text(text)
            await asyncio.sleep(0.3)

    for i, att in enumerate(other_attachments):
        logger.info(f"[HANDLE] Processing other attachment {i+1}/{len(other_attachments)}")
        await process_attachment(att, "")
        await asyncio.sleep(0.5)

    elapsed = time.time() - start_time
    logger.info(f"[HANDLE] ✅ Message processing complete in {elapsed:.2f}s")
    logger.info("=" * 80)


# ===================================================================
# 12. WEBHOOK HANDLER
# ===================================================================
async def webhook_handler(request):
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
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] 🔑 Keys: {list(body.keys())}")
        
        if LOG_RAW_MAX:
            logger.debug(f"[WEBHOOK] Full body: {json.dumps(body, ensure_ascii=False)[:3000]}")
        
        update_type = body.get('update_type', 'unknown')
        logger.info(f"[WEBHOOK] Update type: {update_type}")
        
        if update_type == 'message_created':
            msg = body.get('message', {})
            if msg:
                asyncio.create_task(handle_max_message(msg))
                logger.info("[WEBHOOK] ✅ Queued for processing")
            else:
                logger.warning("[WEBHOOK] No message in update")
        
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
    return web.json_response({
        'ok': True,
        'service': 'MAX → Telegram Forwarder',
        'version': 'final-lifo'
    })


# ===================================================================
# 13. ЗАПУСК
# ===================================================================
async def main():
    logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL LIFO VERSION]...")
    
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
