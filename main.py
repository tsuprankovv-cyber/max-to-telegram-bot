# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ С МАКСИМАЛЬНЫМ ЛОГИРОВАНИЕМ
- Гарантированный LIFO порядок закрытия тегов
- Полное логирование webhook, markup, медиа, Telegram API
- Гибридная отправка медиа
- Транслитерация имён файлов
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
    backupCount=5,
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
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL WITH MAX LOGGING]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 Webhook URL: {RENDER_EXTERNAL_URL}/webhook")
logger.info(f"🔐 Webhook Secret: {'SET' if MAX_WEBHOOK_SECRET else 'NOT SET'}")
logger.info(f"📊 LOG_LEVEL: {LOG_LEVEL}")
logger.info(f"📊 LOG_RAW_MAX: {LOG_RAW_MAX}")
logger.info(f"📊 LOG_RAW_TG: {LOG_RAW_TG}")
logger.info(f"📊 LOG_MARKUP: {LOG_MARKUP}")
logger.info(f"📊 LOG_MEDIA: {LOG_MEDIA}")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===================================================================
def fix_broken_html(text: str) -> str:
    if not text: return text
    logger.debug(f"[HTML-FIX] Input: {text[:100]}...")
    tags = ['b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 'code', 'pre', 'a', 'tg-spoiler']
    fixed = text
    for tag in tags:
        open_pattern = f'<{tag}[^>]*>'
        close_pattern = f'</{tag}>'
        open_count = len(re.findall(open_pattern, fixed, re.IGNORECASE))
        close_count = len(re.findall(close_pattern, fixed, re.IGNORECASE))
        if open_count > close_count:
            fixed += f'</{tag}>' * (open_count - close_count)
            logger.debug(f"[HTML-FIX] Added {open_count - close_count} </{tag}>")
        elif close_count > open_count:
            fixed = re.sub(close_pattern, f'&lt;/{tag}&gt;', fixed, flags=re.IGNORECASE)
            logger.debug(f"[HTML-FIX] Escaped extra </{tag}>")
    open_a = len(re.findall(r'<a\s+[^>]*>', fixed, re.IGNORECASE))
    close_a = len(re.findall(r'</a>', fixed, re.IGNORECASE))
    if open_a > close_a:
        fixed += '</a>' * (open_a - close_a)
        logger.debug(f"[HTML-FIX] Added {open_a - close_a} </a>")
    return fixed

def transliterate_ru_to_en(text: str) -> str:
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
    result = ''.join(mapping.get(c, c) for c in text)
    result = re.sub(r'[^a-zA-Z0-9._-]', '_', result)
    result = re.sub(r'_+', '_', result)
    logger.debug(f"[TRANS] '{text}' -> '{result}'")
    return result.strip('._')

def safe_filename(filename: str) -> str:
    if not filename: return 'file'
    name, ext = (filename.rsplit('.', 1) + [''])[:2]
    safe_name = transliterate_ru_to_en(name) or 'file'
    if len(safe_name) > 100: safe_name = safe_name[:100]
    return f"{safe_name}.{ext}" if ext else safe_name

# ===================================================================
# 4. КОНВЕРТАЦИЯ РАЗМЕТКИ (ГАРАНТИРОВАННЫЙ LIFO)
# ===================================================================
MAX_TAG_MAP = {
    "strong": "b", "bold": "b", "b": "b",
    "emphasized": "i", "italic": "i", "em": "i", "i": "i",
    "underline": "u", "u": "u", "ins": "u",
    "strikethrough": "s", "strike": "s", "s": "s", "del": "s",
    "code": "code", "inline-code": "code", "pre": "pre",
    "spoiler": "tg-spoiler",
    "link": "a", "text_link": "a", "url": "a",
}

def parse_markdown_to_html(text: str) -> str:
    if not text: return text
    logger.info("[MARKDOWN] Parsing markdown...")
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'\+\+(.+?)\+\+', r'<u>\1</u>', text)
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    logger.debug(f"[MARKDOWN] Result: {text[:200]}...")
    return text

def apply_markup(text: str, markup: List[Dict]) -> str:
    """
    Конвертирует разметку MAX в HTML для Telegram.
    ГАРАНТИРОВАННЫЙ LIFO порядок закрытия тегов.
    """
    if not markup or not text:
        return text
    
    logger.info(f"[MARKUP] ========== START CONVERSION ==========")
    logger.info(f"[MARKUP] Text length: {len(text)}")
    logger.info(f"[MARKUP] Text preview: {text[:100]}...")
    logger.info(f"[MARKUP] Entities count: {len(markup)}")
    
    if LOG_MARKUP:
        logger.debug(f"[MARKUP] Raw markup: {json.dumps(markup, ensure_ascii=False, indent=2)}")
    
    start_time = time.time()
    
    # 1. Сортируем сущности по начальной позиции
    sorted_markup = sorted(markup, key=lambda m: (m.get('from', 0), m.get('length', 0)))
    
    # 2. Присваиваем каждой сущности уникальный порядковый номер открытия
    for idx, entity in enumerate(sorted_markup):
        entity['_open_order'] = idx
    
    # 3. Строим карту тегов
    tag_starts = {}
    tag_ends = {}
    
    for entity in sorted_markup:
        offset = entity.get('from', 0)
        length = entity.get('length', 0)
        etype = entity.get('type', '')
        open_order = entity.get('_open_order', 0)
        
        if etype not in MAX_TAG_MAP:
            logger.warning(f"[MARKUP] Unknown type: '{etype}', skipping")
            continue
        
        tag_name = MAX_TAG_MAP[etype]
        
        if etype in ('link', 'text_link', 'url'):
            url = entity.get('url', '').replace('"', '&quot;')
            open_tag = f'<{tag_name} href="{url}">' if url else f'<{tag_name}>'
        else:
            open_tag = f'<{tag_name}>'
        
        close_tag = f'</{tag_name}>'
        
        if offset not in tag_starts:
            tag_starts[offset] = []
        tag_starts[offset].append((open_tag, open_order))
        
        end_pos = offset + length
        if end_pos not in tag_ends:
            tag_ends[end_pos] = []
        tag_ends[end_pos].append((close_tag, open_order))
        
        logger.info(f"[MARKUP] Entity {open_order}: {etype} -> <{tag_name}> [{offset}:{end_pos}]")
    
    logger.info(f"[MARKUP] Tag starts at positions: {sorted(tag_starts.keys())}")
    logger.info(f"[MARKUP] Tag ends at positions: {sorted(tag_ends.keys())}")
    
    # 4. Проходим по тексту
    result = []
    open_tags = []  # Список (close_tag, open_order)
    
    for i, char in enumerate(text):
        # Логируем каждые 10 символов для отладки
        if i % 50 == 0 and (i in tag_starts or i in tag_ends):
            logger.debug(f"[MARKUP] Position {i}: char='{char}'")
        
        # Сначала закрываем теги
        if i in tag_ends:
            closing = sorted(tag_ends[i], key=lambda x: -x[1])
            logger.debug(f"[MARKUP] Position {i}: closing {len(closing)} tags (order: {[c[1] for c in closing]})")
            for close_tag, open_order in closing:
                for j in range(len(open_tags) - 1, -1, -1):
                    if open_tags[j][1] == open_order:
                        result.append(close_tag)
                        open_tags.pop(j)
                        logger.debug(f"[MARKUP]   Closed {close_tag} (order={open_order})")
                        break
        
        # Затем открываем новые
        if i in tag_starts:
            opening = sorted(tag_starts[i], key=lambda x: x[1])
            logger.debug(f"[MARKUP] Position {i}: opening {len(opening)} tags (order: {[o[1] for o in opening]})")
            for open_tag, open_order in opening:
                open_tags.append((open_tag, open_order))
                result.append(open_tag)
                logger.debug(f"[MARKUP]   Opened {open_tag} (order={open_order})")
        
        result.append(char)
    
    # Закрываем оставшиеся
    if open_tags:
        logger.info(f"[MARKUP] Closing {len(open_tags)} remaining tags")
        for close_tag, open_order in reversed(open_tags):
            result.append(close_tag)
            logger.debug(f"[MARKUP]   Closed remaining {close_tag} (order={open_order})")
    
    final_text = ''.join(result)
    elapsed = time.time() - start_time
    
    logger.info(f"[MARKUP] ✅ Converted in {elapsed:.2f}s")
    logger.info(f"[MARKUP] Output length: {len(final_text)}")
    logger.info(f"[MARKUP] Preview: {final_text[:200]}...")
    logger.info(f"[MARKUP] ========== END CONVERSION ==========")
    
    return final_text

# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    logger.info("[EXTRACT] ========== START EXTRACTION ==========")
    logger.debug(f"[EXTRACT] Raw message keys: {list(msg.keys())}")
    
    link = msg.get('link', {})
    is_forward = isinstance(link, dict) and link.get('type') == 'forward' and 'message' in link
    if is_forward:
        logger.info("[EXTRACT] 📨 This is a FORWARDED message")
    inner = link['message'] if is_forward else msg
    
    body = inner.get('body', {})
    text = body.get('text', '') or inner.get('text', '')
    markup = body.get('markup', []) or inner.get('markup', [])
    
    att_list = body.get('attachments') or inner.get('attachments') or []
    attachments = [a for a in att_list if isinstance(a, dict)]
    
    result = {
        "mid": body.get('mid') or inner.get('mid', ''),
        "text": text,
        "markup": markup,
        "attachments": attachments,
        "is_forward": is_forward
    }
    
    logger.info(f"[EXTRACT] mid: {result['mid'][:30]}...")
    logger.info(f"[EXTRACT] text_len: {len(text)}, markup: {len(markup)}, attachments: {len(attachments)}")
    logger.info("[EXTRACT] ========== END EXTRACTION ==========")
    
    return result

# ===================================================================
# 6. АУДИО УТИЛИТЫ
# ===================================================================
def get_audio_duration(file_path: str) -> int:
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0: return int(float(result.stdout.strip()))
    except: pass
    return 0

def extract_audio_tags(file_data: bytes, filename: str) -> Dict[str, Any]:
    logger.info(f"[AUDIO] Extracting tags from: {filename}")
    performer, title, duration = '', '', 0
    with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp:
        tmp.write(file_data)
        tmp_path = tmp.name
    
    try:
        audio = MutagenFile(tmp_path)
        if audio and hasattr(audio, 'info') and audio.info: duration = int(audio.info.length or 0)
        if audio and hasattr(audio, 'tags') and audio.tags:
            tags = audio.tags
            performer = str(tags.get('TPE1', tags.get('©ART', '')))
            title = str(tags.get('TIT2', tags.get('©nam', '')))
            logger.debug(f"[AUDIO] Raw tags: performer='{performer}', title='{title}'")
    except Exception as e:
        logger.debug(f"[AUDIO] Mutagen error: {e}")
    
    if not duration: duration = get_audio_duration(tmp_path)
    os.unlink(tmp_path)
    
    base_name = safe_filename(filename).rsplit('.', 1)[0]
    final_performer, final_title = base_name, ''
    if performer and title: final_title = f"{title} ({performer})"
    elif performer: final_title = performer
    elif title: final_title = title
    elif ' - ' in base_name:
        parts = base_name.split(' - ', 1)
        final_performer, final_title = parts[0].strip(), parts[1].strip()
    
    logger.info(f"[AUDIO] ✅ Final: performer='{final_performer}', title='{final_title}', duration={duration}s")
    return {'performer': final_performer[:64], 'title': final_title[:64], 'duration': duration}

def convert_to_voice(file_data: bytes) -> Optional[bytes]:
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
    except:
        logger.error("[VOICE] ❌ FFmpeg not available")
        return None
    
    with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp_in, \
         tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_out:
        tmp_in.write(file_data)
        tmp_in_path, tmp_out_path = tmp_in.name, tmp_out.name
        
    cmd = ['ffmpeg', '-i', tmp_in_path, '-ac', '1', '-ar', '16000', '-c:a', 'libopus', '-b:a', '16k', '-vbr', 'on', '-application', 'voip', '-y', tmp_out_path]
    logger.info(f"[VOICE] 🎤 Converting: {len(file_data)} bytes -> OGG Opus...")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    
    if result.returncode != 0:
        logger.error(f"[VOICE] ❌ FFmpeg error: {result.stderr[:200]}")
        os.unlink(tmp_in_path)
        return None
    
    with open(tmp_out_path, 'rb') as f: ogg_data = f.read()
    os.unlink(tmp_in_path); os.unlink(tmp_out_path)
    logger.info(f"[VOICE] ✅ Converted: {len(file_data)} -> {len(ogg_data)} bytes")
    return ogg_data

# ===================================================================
# 7. СКАЧИВАНИЕ ПО URL
# ===================================================================
async def download_from_url(url: str) -> Optional[bytes]:
    if not url: return None
    logger.info(f"[DOWNLOAD] 📥 Downloading: {url[:100]}...")
    start_time = time.time()
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
                elapsed = time.time() - start_time
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[DOWNLOAD] ✅ {len(data)} bytes in {elapsed:.2f}s")
                    return data
                logger.error(f"[DOWNLOAD] ❌ HTTP {r.status} in {elapsed:.2f}s")
                return None
    except Exception as e:
        logger.error(f"[DOWNLOAD] ❌ {e}")
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
        try: subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True); return True
        except: return False
    
    def determine(self, att: Dict) -> Tuple[str, Dict]:
        atype = att.get('type', 'file')
        payload = att.get('payload', {})
        fname = payload.get('filename') or att.get('filename', '')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        meta = {'filename': fname, 'size': payload.get('size', 0), 'url': payload.get('url'), 'token': payload.get('token'), 'ext': ext, 'original_type': atype}
        
        logger.info(f"[MEDIA] 🔍 atype='{atype}', ext='{ext}', size={meta['size']}, filename='{fname}'")
        if LOG_MEDIA: logger.debug(f"[MEDIA] Full attachment: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        if atype == 'voice':
            logger.info(f"[MEDIA] ✅ DETERMINED: voice")
            return 'voice', meta
        if atype == 'audio':
            logger.info(f"[MEDIA] ✅ DETERMINED: audio")
            return 'audio', meta
        if atype == 'video':
            logger.info(f"[MEDIA] ✅ DETERMINED: video")
            return 'video', meta
        if atype in ('image', 'photo'):
            logger.info(f"[MEDIA] ✅ DETERMINED: photo")
            return 'photo', meta
        
        if ext in self.VOICE_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: voice (ext .{ext})")
            return 'voice', meta
        if ext in self.AUDIO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: audio (ext .{ext})")
            return 'audio', meta
        if ext in self.PHOTO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: photo (ext .{ext})")
            return 'photo', meta
        if ext in self.VIDEO_EXTS:
            logger.info(f"[MEDIA] ✅ DETERMINED: video (ext .{ext})")
            return 'video', meta
        
        logger.info(f"[MEDIA] 📄 DETERMINED: document")
        return 'document', meta

# ===================================================================
# 9. TELEGRAM CLIENT
# ===================================================================
class TG:
    def __init__(self, token: str, chat_id: str):
        self.token, self.chat_id = token, chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
        logger.info(f"[TG] Initialized: chat_id={chat_id}")
    
    async def init(self):
        if not self.session: 
            self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def _request(self, method: str, **kw) -> Optional[Dict]:
        await self.init()
        logger.info(f"[TG] ▶️ {method}")
        if LOG_RAW_TG: 
            logger.debug(f"[TG-REQ] {json.dumps(kw, default=str, ensure_ascii=False)[:500]}")
        
        start_time = time.time()
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                elapsed = time.time() - start_time
                logger.info(f"[TG] Status: {r.status} in {elapsed:.2f}s")
                logger.info(f"[TG-RESP] {txt}")  # ПОЛНЫЙ ОТВЕТ
                
                resp = json.loads(txt)
                if r.status == 200 and resp.get('ok'):
                    logger.info(f"[TG] ✅ Success: msg_id={resp.get('result', {}).get('message_id')}")
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] ⏳ Rate limit, waiting {wait}s")
                    await asyncio.sleep(wait)
                    return await self._request(method, **kw)
                else:
                    logger.error(f"[TG] ❌ {resp.get('description')}")
                    return resp
        except Exception as e:
            logger.error(f"[TG] ❌ Exception: {e}")
            return None

    async def send_text(self, text: str) -> bool:
        if not text: return True
        text = fix_broken_html(text)
        logger.info(f"[TG] 📤 Sending text: {text[:100]}...")
        resp = await self._request('sendMessage', json={'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML'})
        return resp and resp.get('ok', False)

    async def send_media_group_direct(self, items: List[Dict]) -> bool:
        if not items: return True
        if len(items) > 10: items = items[:10]
        logger.info(f"[TG] 📤 Media group (direct URLs): {len(items)} items")
        
        input_media = []
        for i, item in enumerate(items):
            obj = {'type': item['type'], 'media': item['media']}
            if i == 0 and item.get('caption'):
                obj['caption'] = fix_broken_html(item['caption'])[:1024]
                obj['parse_mode'] = 'HTML'
            input_media.append(obj)
            logger.debug(f"[TG]   Item {i}: {item['type']}, media={str(item['media'])[:50]}...")
        
        resp = await self._request('sendMediaGroup', json={'chat_id': self.chat_id, 'media': input_media})
        if resp and isinstance(resp, dict) and resp.get('ok'):
            msg_ids = [r.get('message_id') for r in resp.get('result', [])]
            logger.info(f"[TG] ✅ Media group sent: message_ids={msg_ids}")
            return True
        return False

    async def send_media_group_via_download(self, items: List[Dict], caption: str = None) -> bool:
        if not items: return True
        if len(items) > 10: items = items[:10]
        logger.info(f"[TG] 📤 Media group (via download): {len(items)} items")
        
        input_media = []
        for i, item in enumerate(items):
            form = aiohttp.FormData()
            form.add_field('chat_id', self.chat_id)
            field = 'photo' if item['type'] == 'photo' else 'video'
            form.add_field(field, item['data'], filename=item.get('filename', f'{field}.jpg'))
            upload_resp = await self._request(f'send{field.capitalize()}', data=form)
            
            if not upload_resp or not upload_resp.get('ok'):
                logger.error(f"[TG] Failed to upload {field}")
                return False
            
            file_id = upload_resp['result'][field][0]['file_id'] if field == 'photo' else upload_resp['result']['video']['file_id']
            obj = {'type': item['type'], 'media': file_id}
            if i == 0 and caption:
                obj['caption'] = fix_broken_html(caption)[:1024]
                obj['parse_mode'] = 'HTML'
            input_media.append(obj)
            
            # Удаляем временное сообщение
            msg_id = upload_resp['result']['message_id']
            await self._request('deleteMessage', json={'chat_id': self.chat_id, 'message_id': msg_id})
            logger.debug(f"[TG]   Uploaded {field}, file_id={file_id}, deleted temp msg {msg_id}")
        
        resp = await self._request('sendMediaGroup', json={'chat_id': self.chat_id, 'media': input_media})
        if resp and isinstance(resp, dict) and resp.get('ok'):
            msg_ids = [r.get('message_id') for r in resp.get('result', [])]
            logger.info(f"[TG] ✅ Media group sent: message_ids={msg_ids}")
            return True
        return False

    async def send_media(self, media_type: str, media_data, caption="", filename="", is_url=False, **extra) -> bool:
        method_map = {'photo': 'sendPhoto', 'video': 'sendVideo', 'audio': 'sendAudio', 'voice': 'sendVoice', 'document': 'sendDocument'}
        method = method_map.get(media_type, 'sendDocument')
        field = media_type if media_type != 'document' else 'document'
        
        logger.info(f"[TG] 📤 Sending {media_type}: is_url={is_url}, size={len(media_data) if isinstance(media_data, bytes) else 'N/A'}")
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        safe_fname = safe_filename(filename) if filename else f"{media_type}.file"
        
        if is_url:
            form.add_field(field, media_data)
            logger.debug(f"[TG]   URL: {str(media_data)[:80]}...")
        else:
            form.add_field(field, media_data, filename=safe_fname)
            logger.debug(f"[TG]   File: {safe_fname}")
        
        if caption and media_type != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption[:1024])
            form.add_field('parse_mode', 'HTML')
            logger.debug(f"[TG]   Caption: {caption[:50]}...")
        
        if media_type == 'audio':
            if extra.get('performer'): form.add_field('performer', extra['performer'][:64])
            if extra.get('title'): form.add_field('title', extra['title'][:64])
            if extra.get('duration'): form.add_field('duration', str(extra['duration']))
            logger.debug(f"[TG]   Audio extra: {extra}")
        if media_type == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
            
        resp = await self._request(method, data=form)
        return resp and resp.get('ok', False)

# ===================================================================
# 10. MAX CLIENT
# ===================================================================
class MX:
    def __init__(self, token: str, cid: str, base: str):
        self.token, self.cid, self.base = token, cid, base
        self.session = None
        logger.info(f"[MAX] Initialized: cid={cid}")
    
    async def init(self):
        if not self.session: self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def register_webhook(self, webhook_url: str, secret: str = "") -> bool:
        await self.init()
        logger.info(f"[MAX] 🔗 Registering webhook for chat {self.cid}: {webhook_url}")
        
        body = {
            "url": webhook_url,
            "chat_id": self.cid,
            "update_types": ["message_created"]
        }
        if secret:
            body["secret"] = secret
        
        headers = {'Authorization': self.token, 'Content-Type': 'application/json'}
        logger.debug(f"[MAX] Request body: {json.dumps(body, ensure_ascii=False)}")
        
        try:
            async with self.session.post(f"{self.base}/subscriptions", headers=headers, json=body) as r:
                text = await r.text()
                logger.info(f"[MAX] Response: {r.status}")
                logger.info(f"[MAX] Body: {text}")
                return r.status == 200
        except Exception as e:
            logger.error(f"[MAX] ❌ {e}")
            return False

# ===================================================================
# 11. ОБРАБОТЧИКИ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()

async def process_attachment(att: Dict, caption: str = "") -> bool:
    start_time = time.time()
    logger.info("[ATT] ========== PROCESSING ATTACHMENT ==========")
    
    if not isinstance(att, dict):
        logger.warning("[ATT] ❌ Not a dict, skipping")
        return False
    
    tg_type, meta = media_proc.determine(att)
    logger.info(f"[ATT] Type: {tg_type}, filename: {meta.get('filename')}, size: {meta.get('size')}")
    
    direct_url = meta.get('url')
    if not direct_url:
        logger.error("[ATT] ❌ No direct URL in payload")
        return False
    
    if tg_type in ('photo', 'video') and direct_url:
        logger.info(f"[ATT] 📤 Using direct URL for {tg_type}")
        res = await tg.send_media(tg_type, direct_url, caption, meta.get('filename', ''), is_url=True)
        elapsed = time.time() - start_time
        logger.info(f"[ATT] {'✅' if res else '❌'} Completed in {elapsed:.2f}s")
        return res
        
    logger.info(f"[ATT] 📥 Downloading {tg_type} from direct URL...")
    file_data = await download_from_url(direct_url)
    if not file_data:
        logger.error("[ATT] ❌ Download failed")
        return False
    
    extra = {}
    if tg_type == 'audio' and meta.get('size', 0) < 2*1024*1024 and media_proc.ffmpeg_ok:
        logger.info(f"[ATT] 🎤 Trying voice conversion...")
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type, file_data = 'voice', voice_data
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                extra['duration'] = get_audio_duration(tmp.name)
                os.unlink(tmp.name)
            logger.info(f"[ATT] ✅ Converted to voice, duration={extra['duration']}s")
    
    if tg_type == 'audio':
        extra.update(extract_audio_tags(file_data, meta.get('filename', '')))
    
    logger.info(f"[ATT] 📤 Sending {tg_type} to Telegram...")
    res = await tg.send_media(tg_type, file_data, caption if tg_type != 'document' else '', meta.get('filename', ''), False, **extra)
    elapsed = time.time() - start_time
    logger.info(f"[ATT] {'✅' if res else '❌'} Completed in {elapsed:.2f}s")
    return res

async def handle_max_message(msg: Dict):
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
        logger.info(f"[HANDLE] Applying markup ({len(data['markup'])} items)")
        text = apply_markup(text, data['markup'])
    elif text and ('*' in text or '_' in text or '[' in text):
        logger.info("[HANDLE] 📝 Using Markdown parser...")
        text = parse_markdown_to_html(text)

    media_items, other = [], []
    for att in data['attachments']:
        t, m = media_proc.determine(att)
        (media_items if t in ('photo', 'video') else other).append({'type': t, 'attachment': att, 'meta': m})

    logger.info(f"[HANDLE] Media items: {len(media_items)}, Other: {len(other)}")

    use_new = False
    if len(media_items) > 1:
        has_v = any(i['type']=='video' for i in media_items)
        has_p = any(i['type']=='photo' for i in media_items)
        if has_v and (has_p or sum(1 for i in media_items if i['type']=='video') >= 2):
            use_new = True
            logger.info("[HANDLE] 🔄 Using NEW scheme (mixed/video group)")

    if media_items:
        if len(media_items) == 1:
            logger.info("[HANDLE] 📷 Single media, sending directly")
            await process_attachment(media_items[0]['attachment'], text)
        elif use_new:
            downloaded = []
            for item in media_items:
                url = item['meta'].get('url')
                if not url: continue
                data = await download_from_url(url)
                if data: 
                    downloaded.append({'type': item['type'], 'data': data, 'filename': safe_filename(item['meta'].get('filename', ''))})
            if downloaded:
                ok = await tg.send_media_group_via_download(downloaded, text)
                if not ok:
                    logger.warning("[HANDLE] Media group failed, sending individually...")
                    for i, item in enumerate(downloaded):
                        await tg.send_media(item['type'], item['data'], caption=text if i==0 else "", filename=item['filename'])
                        await asyncio.sleep(0.3)
        else:
            items = [{'type': i['type'], 'media': i['meta'].get('url'), 'caption': text if idx==0 else None} 
                     for idx, i in enumerate(media_items) if i['meta'].get('url')]
            if items:
                ok = await tg.send_media_group_direct(items)
                if not ok:
                    logger.warning("[HANDLE] Media group failed, sending individually...")
                    for i, item in enumerate(media_items):
                        await process_attachment(item['attachment'], text if i==0 else "")
                        await asyncio.sleep(0.3)
    elif text:
        await tg.send_text(text)
        await asyncio.sleep(0.3)

    for att in other:
        await process_attachment(att, "")
        await asyncio.sleep(0.5)

    elapsed = time.time() - start_time
    logger.info(f"[HANDLE] ✅ Complete in {elapsed:.2f}s")
    logger.info("=" * 80)

# ===================================================================
# 12. WEBHOOK HANDLER (МАКСИМАЛЬНОЕ ЛОГИРОВАНИЕ)
# ===================================================================
async def webhook_handler(request):
    logger.info("=" * 60)
    logger.info(f"[WEBHOOK] 📨 {request.method} from {request.remote}")
    logger.info(f"[WEBHOOK] Headers: {dict(request.headers)}")
    
    if request.method != 'POST':
        logger.warning(f"[WEBHOOK] Invalid method: {request.method}")
        return web.Response(status=405)
    
    if VERIFY_WEBHOOK_SECRET and MAX_WEBHOOK_SECRET:
        secret = request.headers.get('X-Max-Bot-Api-Secret')
        if secret != MAX_WEBHOOK_SECRET:
            logger.warning(f"[WEBHOOK] ❌ Invalid secret: {secret[:10] if secret else 'None'}...")
            return web.Response(status=403)
        logger.info("[WEBHOOK] ✅ Secret verified")
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] Keys: {list(body.keys())}")
        logger.info(f"[WEBHOOK] Update type: {body.get('update_type')}")
        logger.info(f"[WEBHOOK] FULL BODY:\n{json.dumps(body, ensure_ascii=False, indent=2)}")
        
        if body.get('update_type') == 'message_created' and (msg := body.get('message')):
            logger.info(f"[WEBHOOK] ✅ Message received, queuing for processing...")
            asyncio.create_task(handle_max_message(msg))
        else:
            logger.info(f"[WEBHOOK] ⏭ Skipping (not message_created)")
        
        return web.Response(status=200)
    except json.JSONDecodeError as e:
        logger.error(f"[WEBHOOK] ❌ Invalid JSON: {e}")
        return web.Response(status=400)
    except Exception as e:
        logger.error(f"[WEBHOOK] ❌ Exception: {e}", exc_info=True)
        return web.Response(status=500)
    finally:
        logger.info("=" * 60)

async def health_handler(request):
    return web.json_response({'ok': True, 'version': 'final-max-logging'})

# ===================================================================
# 13. ЗАПУСК
# ===================================================================
async def main():
    logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL MAX LOGGING]...")
    
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
    await web.TCPSite(runner, '0.0.0.0', port).start()
    
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
