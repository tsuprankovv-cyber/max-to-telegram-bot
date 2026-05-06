# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ
- Кнопки из inline_keyboard (бот-постер)
- Кнопки отдельным сообщением после медиа (короткий текст/без текста)
- Кнопки у последней части разделённого длинного текста
- Замена определённой ссылки в кнопках
- Умное разделение длинных текстов
- Удаление пустых HTML-тегов
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
    maxBytes=20*1024*1024,
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
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"🔗 Webhook URL: {RENDER_EXTERNAL_URL}/webhook")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

# ===================================================================
# 3. ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ===================================================================
def remove_empty_tags(text: str) -> str:
    if not text: return text
    for tag in ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']:
        text = re.sub(f'<{tag}></{tag}>', '', text)
        text = re.sub(f'<{tag} [^>]*></{tag}>', '', text)
    return text


def fix_broken_html(text: str) -> str:
    if not text: return text
    tags = ['b', 'i', 'u', 's', 'code', 'pre', 'a', 'tg-spoiler']
    for tag in tags:
        open_count = text.count(f'<{tag}>') + len(re.findall(f'<{tag} [^>]*>', text))
        close_count = text.count(f'</{tag}>')
        if open_count > close_count:
            text += f'</{tag}>' * (open_count - close_count)
            logger.warning(f"[HTML-FIX] Added {open_count - close_count} </{tag}>")
    text = remove_empty_tags(text)
    return text


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
    return result.strip('._')


def safe_filename(filename: str) -> str:
    if not filename: return 'file'
    name, ext = (filename.rsplit('.', 1) + [''])[:2]
    safe_name = transliterate_ru_to_en(name) or 'file'
    if len(safe_name) > 100: safe_name = safe_name[:100]
    return f"{safe_name}.{ext}" if ext else safe_name


def split_smart_text(text: str, max_len: int = 1000) -> List[str]:
    if len(text) <= max_len:
        return [text]
    
    logger.info(f"[TEXT] Splitting {len(text)} chars (max={max_len})")
    
    parts = []
    current = ""
    paragraphs = text.split('\n\n')
    
    for para in paragraphs:
        if len(current) + len(para) + 2 <= max_len:
            current = (current + '\n\n' + para) if current else para
        else:
            if current:
                parts.append(current)
            if len(para) > max_len:
                sentences = re.split(r'(?<=[.!?])\s+', para)
                current = ""
                for sent in sentences:
                    if len(current) + len(sent) + 1 <= max_len:
                        current = (current + ' ' + sent) if current else sent
                    else:
                        if current:
                            parts.append(current)
                        current = sent
            else:
                current = para
    
    if current:
        parts.append(current)
    
    logger.info(f"[TEXT] Split into {len(parts)} parts")
    for i, part in enumerate(parts):
        logger.info(f"[TEXT] Part {i+1}: {len(part)} chars")
    
    return parts


# ===================================================================
# 4. КОНВЕРТАЦИЯ КНОПОК MAX → TELEGRAM (С ЗАМЕНОЙ ССЫЛКИ)
# ===================================================================
OLD_FORM_URL = 'https://forms.yandex.ru/cloud/680f8114505690020a036f30/'
NEW_FORM_URL = 'https://forms.yandex.ru/cloud/680f5f5ce010db158f8b7610'


def replace_button_urls(buttons: List[List[Dict]]) -> List[List[Dict]]:
    for row in buttons:
        for btn in row:
            if btn.get('url') == OLD_FORM_URL:
                btn['url'] = NEW_FORM_URL
                logger.info(f"[BUTTONS] 🔄 Replaced URL in '{btn.get('text', '')}'")
    return buttons


def convert_max_buttons(reply_markup: Dict) -> Optional[Dict]:
    if not reply_markup: return None
    keyboard = reply_markup.get('inline_keyboard') or reply_markup.get('keyboard')
    if not keyboard or not isinstance(keyboard, list): return None
    telegram_keyboard = []
    for row in keyboard:
        telegram_row = []
        for button in row:
            if button.get('type') == 'url' or button.get('url'):
                url = button.get('url', '')
                if url == OLD_FORM_URL:
                    url = NEW_FORM_URL
                    logger.info(f"[BUTTONS] 🔄 Replaced URL in '{button.get('text', '')}'")
                telegram_row.append({'text': button.get('text', 'Button'), 'url': url})
        if telegram_row:
            telegram_keyboard.append(telegram_row)
    logger.info(f"[BUTTONS] ✅ Converted {len(telegram_keyboard)} rows")
    return {'inline_keyboard': telegram_keyboard} if telegram_keyboard else None


def extract_keyboard_from_attachments(attachments: List[Dict]) -> Optional[Dict]:
    for att in attachments:
        if att.get('type') == 'inline_keyboard':
            payload = att.get('payload', {})
            buttons = payload.get('buttons', [])
            if buttons:
                buttons = replace_button_urls(buttons)
                logger.info(f"[BUTTONS] 🎛️ Found inline_keyboard: {len(buttons)} rows")
                return convert_max_buttons({'inline_keyboard': buttons})
    return None


# ===================================================================
# 5. УНИВЕРСАЛЬНАЯ КОРРЕКЦИЯ OFFSET ЧЕРЕЗ UTF-16
# ===================================================================
def normalize_max_offset(text: str, max_offset: int, max_length: int = None) -> Tuple[int, int]:
    python_offset = 0
    utf16_pos = 0
    for i, char in enumerate(text):
        if utf16_pos >= max_offset:
            python_offset = i
            break
        utf16_pos += len(char.encode('utf-16-le')) // 2
    else:
        python_offset = len(text)
    
    if max_length is not None:
        python_length = 0
        utf16_end = max_offset + max_length
        utf16_pos = max_offset
        for i in range(python_offset, len(text)):
            if utf16_pos >= utf16_end:
                break
            utf16_pos += len(text[i].encode('utf-16-le')) // 2
            python_length += 1
        if python_offset != max_offset or python_length != max_length:
            logger.warning(f"[MARKUP] 🔧 Offset corrected: MAX=[{max_offset}:{max_offset+max_length}] -> Python=[{python_offset}:{python_offset+python_length}]")
        return python_offset, python_length
    return python_offset, max_length


# ===================================================================
# 6. ФИЛЬТРАЦИЯ ВЛОЖЕННЫХ СУЩНОСТЕЙ
# ===================================================================
def filter_overlapping_same_type(markup: List[Dict]) -> List[Dict]:
    if not markup: return markup
    filtered = []
    for i, entity in enumerate(markup):
        etype = entity.get('type', '')
        offset = entity.get('from', 0)
        length = entity.get('length', 0)
        end = offset + length
        is_nested = False
        for j, other in enumerate(markup):
            if i == j: continue
            if other.get('type') != etype: continue
            other_offset = other.get('from', 0)
            other_end = other_offset + other.get('length', 0)
            if other_offset <= offset and other_end >= end and (other_offset < offset or other_end > end):
                is_nested = True
                break
        if not is_nested:
            filtered.append(entity)
    return filtered


# ===================================================================
# 7. КОНВЕРТАЦИЯ РАЗМЕТКИ
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

TAG_ORDER = {'a': 1, 'u': 2, 's': 3, 'b': 4, 'i': 5, 'code': 6, 'pre': 7, 'tg-spoiler': 8}


def parse_markdown_to_html(text: str) -> str:
    if not text: return text
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'\+\+(.+?)\+\+', r'<u>\1</u>', text)
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    return text


def apply_markup(text: str, markup: List[Dict]) -> str:
    if not markup or not text: return text
    
    logger.info(f"[MARKUP] Input text length: {len(text)}, entities: {len(markup)}")
    markup = filter_overlapping_same_type(markup)
    
    corrected_markup = []
    for entity in markup:
        entity = entity.copy()
        max_offset = entity.get('from', 0)
        max_length = entity.get('length', 0)
        python_offset, python_length = normalize_max_offset(text, max_offset, max_length)
        entity['from'] = python_offset
        entity['length'] = python_length
        corrected_markup.append(entity)
    
    sorted_markup = sorted(corrected_markup, key=lambda m: (m.get('from', 0), m.get('length', 0)))
    tag_starts = {}
    tag_ends = {}
    
    for entity in sorted_markup:
        offset = entity.get('from', 0)
        length = entity.get('length', 0)
        etype = entity.get('type', '')
        if etype not in MAX_TAG_MAP: continue
        tag_name = MAX_TAG_MAP[etype]
        if etype in ('link', 'text_link', 'url'):
            url = entity.get('url', '').replace('"', '&quot;')
            open_tag = f'<{tag_name} href="{url}">' if url else f'<{tag_name}>'
        else:
            open_tag = f'<{tag_name}>'
        tag_starts.setdefault(offset, []).append(open_tag)
        tag_ends.setdefault(offset + length, []).append(open_tag)
    
    result = []
    open_tags = []
    for i, char in enumerate(text):
        if i in tag_ends:
            for open_tag in reversed(open_tags):
                if open_tag in tag_ends[i]:
                    open_tags.remove(open_tag)
                    close_tag = f'</{open_tag.split()[0].strip("<>")}>'
                    result.append(close_tag)
        if i in tag_starts:
            sorted_tags = sorted(tag_starts[i], key=lambda t: TAG_ORDER.get(t.split()[0].strip('<>'), 99))
            for open_tag in sorted_tags:
                open_tags.append(open_tag)
                result.append(open_tag)
        result.append(char)
    for open_tag in reversed(open_tags):
        result.append(f'</{open_tag.split()[0].strip("<>")}>')
    
    final_text = ''.join(result)
    logger.info(f"[MARKUP] Output length: {len(final_text)}")
    return final_text


# ===================================================================
# 8. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    link = msg.get('link', {})
    is_forward = isinstance(link, dict) and link.get('type') == 'forward' and 'message' in link
    inner = link['message'] if is_forward else msg
    
    body = inner.get('body', {})
    text = body.get('text', '') or inner.get('text', '')
    markup = body.get('markup', []) or inner.get('markup', [])
    attachments = [a for a in (body.get('attachments') or inner.get('attachments') or []) if isinstance(a, dict)]
    reply_markup = inner.get('reply_markup') or msg.get('reply_markup')
    
    logger.info(f"[EXTRACT] text_len: {len(text)}, markup: {len(markup)}, attachments: {len(attachments)}")
    return {"mid": body.get('mid', ''), "text": text, "markup": markup, "attachments": attachments, "is_forward": is_forward, "reply_markup": reply_markup}


# ===================================================================
# 9. АУДИО УТИЛИТЫ
# ===================================================================
def get_audio_duration(file_path: str) -> int:
    try:
        cmd = ['ffprobe', '-v', 'error', '-show_entries', 'format=duration', '-of', 'default=noprint_wrappers=1:nokey=1', file_path]
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        if result.returncode == 0: return int(float(result.stdout.strip()))
    except: pass
    return 0


def extract_audio_tags(file_data: bytes, filename: str) -> Dict[str, Any]:
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
    except: pass
    if not duration: duration = get_audio_duration(tmp_path)
    os.unlink(tmp_path)
    base_name = safe_filename(filename).rsplit('.', 1)[0]
    final_performer, final_title = base_name, ''
    if performer and title: final_title = f"{title} ({performer})"
    elif performer: final_title = performer
    elif title: final_title = title
    logger.info(f"[AUDIO] Final: performer='{final_performer}', title='{final_title}', duration={duration}s")
    return {'performer': final_performer[:64], 'title': final_title[:64], 'duration': duration}


def convert_to_voice(file_data: bytes) -> Optional[bytes]:
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
    except:
        return None
    with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp_in, \
         tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_out:
        tmp_in.write(file_data)
        tmp_in_path, tmp_out_path = tmp_in.name, tmp_out.name
    cmd = ['ffmpeg', '-i', tmp_in_path, '-ac', '1', '-ar', '16000', '-c:a', 'libopus', '-b:a', '16k', '-vbr', 'on', '-application', 'voip', '-y', tmp_out_path]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        os.unlink(tmp_in_path)
        return None
    with open(tmp_out_path, 'rb') as f: ogg_data = f.read()
    os.unlink(tmp_in_path); os.unlink(tmp_out_path)
    return ogg_data


# ===================================================================
# 10. СКАЧИВАНИЕ ПО URL
# ===================================================================
async def download_from_url(url: str) -> Optional[bytes]:
    if not url: return None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=300)) as r:
                if r.status == 200:
                    return await r.read()
                return None
    except: return None


# ===================================================================
# 11. MEDIA PROCESSOR
# ===================================================================
class MediaProcessor:
    VOICE_EXTS = {'ogg', 'opus', 'oga'}
    AUDIO_EXTS = {'mp3', 'wav', 'm4a', 'flac', 'aac', 'wma', 'alac', 'aiff'}
    
    def __init__(self):
        self.ffmpeg_ok = self._check_ffmpeg()
    
    def _check_ffmpeg(self) -> bool:
        try: subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True); return True
        except: return False
    
    def determine(self, att: Dict) -> Tuple[str, Dict]:
        atype = att.get('type', 'file')
        payload = att.get('payload', {})
        fname = payload.get('filename') or att.get('filename', '')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        meta = {'filename': fname, 'size': payload.get('size', 0), 'url': payload.get('url'), 'token': payload.get('token'), 'ext': ext, 'original_type': atype}
        
        if atype == 'voice': return 'voice', meta
        if atype == 'audio': return 'audio', meta
        if atype == 'video': return 'video', meta
        if atype in ('image', 'photo'): return 'photo', meta
        if atype == 'share': return 'document', meta
        if atype == 'inline_keyboard': return 'keyboard', meta
        
        if ext in self.VOICE_EXTS: return 'voice', meta
        if ext in self.AUDIO_EXTS: return 'audio', meta
        
        return 'document', meta


# ===================================================================
# 12. TELEGRAM CLIENT
# ===================================================================
class TG:
    def __init__(self, token: str, chat_id: str):
        self.token, self.chat_id = token, chat_id
        self.base = f"https://api.telegram.org/bot{token}"
        self.session = None
    
    async def init(self):
        if not self.session: self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120))
    
    async def _request(self, method: str, **kw) -> Optional[Dict]:
        await self.init()
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                resp = json.loads(txt)
                if r.status == 200 and resp.get('ok'):
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    await asyncio.sleep(wait)
                    return await self._request(method, **kw)
                return None
        except: return None

    async def send_text(self, text: str, reply_markup: Optional[Dict] = None) -> bool:
        text = fix_broken_html(text) if text else text
        payload = {'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML', 'disable_web_page_preview': False}
        if reply_markup:
            payload['reply_markup'] = reply_markup
        resp = await self._request('sendMessage', json=payload)
        return resp and resp.get('ok', False)

    async def send_media_group_via_download(self, items: List[Dict], caption: str = None) -> bool:
        if not items: return True
        if len(items) > 10: items = items[:10]
        
        input_media = []
        for i, item in enumerate(items):
            form = aiohttp.FormData()
            form.add_field('chat_id', self.chat_id)
            field = 'photo' if item['type'] == 'photo' else 'video'
            form.add_field(field, item['data'], filename=item.get('filename', f'{field}.jpg'))
            upload_resp = await self._request(f'send{field.capitalize()}', data=form)
            
            if not upload_resp or not upload_resp.get('ok'): return False
            
            file_id = upload_resp['result'][field][0]['file_id'] if field == 'photo' else upload_resp['result']['video']['file_id']
            obj = {'type': item['type'], 'media': file_id}
            if i == 0 and caption:
                obj['caption'] = fix_broken_html(caption)
                obj['parse_mode'] = 'HTML'
            input_media.append(obj)
            
            await self._request('deleteMessage', json={'chat_id': self.chat_id, 'message_id': upload_resp['result']['message_id']})
        
        resp = await self._request('sendMediaGroup', json={'chat_id': self.chat_id, 'media': input_media})
        return resp and resp.get('ok', False)

    async def send_media(self, media_type: str, media_data, caption="", filename="", is_url=False, **extra) -> bool:
        method_map = {'photo': 'sendPhoto', 'video': 'sendVideo', 'audio': 'sendAudio', 'voice': 'sendVoice', 'document': 'sendDocument'}
        method = method_map.get(media_type, 'sendDocument')
        field = media_type if media_type != 'document' else 'document'
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        if is_url:
            form.add_field(field, media_data)
        else:
            form.add_field(field, media_data, filename=safe_filename(filename) if filename else f"{media_type}.file")
        
        if caption and media_type != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption)
            form.add_field('parse_mode', 'HTML')
        
        if media_type == 'audio':
            if extra.get('performer'): form.add_field('performer', extra['performer'][:64])
            if extra.get('title'): form.add_field('title', extra['title'][:64])
            if extra.get('duration'): form.add_field('duration', str(extra['duration']))
        if media_type == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
            
        resp = await self._request(method, data=form)
        return resp and resp.get('ok', False)


# ===================================================================
# 13. MAX CLIENT
# ===================================================================
class MX:
    def __init__(self, token: str, cid: str, base: str):
        self.token, self.cid, self.base = token, cid, base
        self.session = None
    
    async def init(self):
        if not self.session: self.session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=60))
    
    async def register_webhook(self, webhook_url: str, secret: str = "") -> bool:
        await self.init()
        body = {"url": webhook_url, "chat_id": self.cid, "update_types": ["message_created"]}
        if secret: body["secret"] = secret
        try:
            async with self.session.post(f"{self.base}/subscriptions", headers={'Authorization': self.token}, json=body) as r:
                return r.status == 200
        except: return False


# ===================================================================
# 14. ОБРАБОТЧИКИ
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


async def process_attachment(att: Dict, caption: str = "") -> bool:
    if not isinstance(att, dict): return False
    
    tg_type, meta = media_proc.determine(att)
    
    if tg_type == 'keyboard':
        return True
    
    direct_url = meta.get('url') or att.get('payload', {}).get('url')
    if not direct_url: return False
    
    if tg_type in ('photo', 'video') and direct_url:
        return await tg.send_media(tg_type, direct_url, caption, meta.get('filename', ''), is_url=True)
    
    file_data = await download_from_url(direct_url)
    if not file_data: return False
    
    extra = {}
    if meta.get('original_type') == 'voice' and media_proc.ffmpeg_ok:
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type, file_data = 'voice', voice_data
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                extra['duration'] = get_audio_duration(tmp.name)
                os.unlink(tmp.name)
    
    if tg_type == 'audio':
        extra.update(extract_audio_tags(file_data, meta.get('filename', '')))
    
    return await tg.send_media(tg_type, file_data, caption if tg_type != 'document' else '', meta.get('filename', ''), False, **extra)


async def send_media_group(media_items: List[Dict], caption: str = "") -> bool:
    downloaded = []
    for item in media_items:
        url = item['meta'].get('url')
        if not url: continue
        data = await download_from_url(url)
        if data:
            downloaded.append({'type': item['type'], 'data': data, 'filename': safe_filename(item['meta'].get('filename', ''))})
    
    if downloaded:
        ok = await tg.send_media_group_via_download(downloaded, caption)
        if not ok:
            for i, item in enumerate(downloaded):
                await tg.send_media(item['type'], item['data'], caption=caption if i == 0 else "", filename=item['filename'])
                await asyncio.sleep(0.3)
        return ok
    return False


async def handle_max_message(msg: Dict):
    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw: {json.dumps(msg, ensure_ascii=False)[:2000]}")

    data = extract_message_data(msg)
    
    if not data['text'] and not data['attachments']:
        return

    text = data['text']
    logger.info(f"[HANDLE] Raw text length: {len(text)}")
    
    if data['markup']:
        text = apply_markup(text, data['markup'])
    elif text and ('*' in text or '_' in text or '[' in text or 'http' in text):
        text = parse_markdown_to_html(text)
    
    logger.info(f"[HANDLE] Final text length: {len(text)}")

    media_items, other = [], []
    for att in data['attachments']:
        if att.get('type') == 'inline_keyboard':
            continue
        t, m = media_proc.determine(att)
        item = {'type': t, 'attachment': att, 'meta': m}
        (media_items if t in ('photo', 'video') else other).append(item)

    # Извлекаем кнопки
    reply_markup = convert_max_buttons(data.get('reply_markup', {}))
    if not reply_markup:
        reply_markup = extract_keyboard_from_attachments(data['attachments'])

    if media_items:
        text_parts = split_smart_text(text, max_len=1000) if text else [""]
        caption = text_parts[0] if text_parts[0] else ""
        
        if len(media_items) == 1:
            await process_attachment(media_items[0]['attachment'], caption)
        else:
            await send_media_group(media_items, caption)
        
        # Остальные части текста — кнопки к последней
        for i, part in enumerate(text_parts[1:]):
            is_last = (i == len(text_parts) - 2)
            await tg.send_text(part, reply_markup=reply_markup if is_last else None)
        
        # Кнопки отдельным сообщением после медиа
        if reply_markup and len(text_parts) <= 1:
            await tg.send_text("\u200B", reply_markup=reply_markup)
            logger.info("[HANDLE] 🎛️ Buttons sent as separate message after media")
            
    elif text:
        text_parts = split_smart_text(text, max_len=4000) if len(text) > 4000 else [text]
        for i, part in enumerate(text_parts):
            is_last = (i == len(text_parts) - 1)
            await tg.send_text(part, reply_markup=reply_markup if is_last else None)

    for item in other:
        await process_attachment(item['attachment'], "")
        await asyncio.sleep(0.5)


# ===================================================================
# 15. WEBHOOK HANDLER
# ===================================================================
async def webhook_handler(request):
    if request.method != 'POST':
        return web.Response(status=405)
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] Update type: {body.get('update_type')}")
        logger.info(f"[WEBHOOK] FULL BODY:\n{json.dumps(body, ensure_ascii=False, indent=2)}")
        
        if body.get('update_type') == 'message_created' and (msg := body.get('message')):
            asyncio.create_task(handle_max_message(msg))
        
        return web.Response(status=200)
    except: return web.Response(status=500)


async def health_handler(request):
    return web.json_response({'ok': True, 'version': 'final-buttons'})


# ===================================================================
# 16. ЗАПУСК
# ===================================================================
async def main():
    if RENDER_EXTERNAL_URL:
        await mx.register_webhook(f"{RENDER_EXTERNAL_URL}/webhook", MAX_WEBHOOK_SECRET)
    
    app = web.Application()
    app.router.add_post('/webhook', webhook_handler)
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv('PORT', 8080))).start()
    await asyncio.Event().wait()


if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 FATAL: {e}")
        sys.exit(1)
