# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ
- Ссылки работают (Markdown + голые URL)
- Голосовые определяются по расширению (.ogg, .opus, .oga)
- Фото/видео как файл → отправляется как документ
- Коллажи через надёжную схему со скачиванием
- Кнопки-ссылки (если есть в webhook)
- Коррекция offset через UTF-16
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
def fix_broken_html(text: str) -> str:
    if not text: return text
    tags = ['b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 'code', 'pre', 'a', 'tg-spoiler']
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

# ===================================================================
# 4. КОНВЕРТАЦИЯ КНОПОК MAX → TELEGRAM
# ===================================================================
def convert_max_buttons(reply_markup: Dict) -> Optional[Dict]:
    if not reply_markup: return None
    keyboard = reply_markup.get('inline_keyboard') or reply_markup.get('keyboard')
    if not keyboard or not isinstance(keyboard, list): return None
    telegram_keyboard = []
    for row in keyboard:
        telegram_row = []
        for button in row:
            if button.get('type') == 'url' or button.get('url'):
                telegram_row.append({'text': button.get('text', 'Button'), 'url': button.get('url', '')})
        if telegram_row:
            telegram_keyboard.append(telegram_row)
    return {'inline_keyboard': telegram_keyboard} if telegram_keyboard else None

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
# 7. КОНВЕРТАЦИЯ РАЗМЕТКИ (ИСПРАВЛЕННЫЙ LIFO)
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
    text = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', text)
    text = re.sub(r'\*(.+?)\*', r'<i>\1</i>', text)
    text = re.sub(r'\+\+(.+?)\+\+', r'<u>\1</u>', text)
    text = re.sub(r'~~(.+?)~~', r'<s>\1</s>', text)
    text = re.sub(r'\[(.+?)\]\((.+?)\)', r'<a href="\2">\1</a>', text)
    text = re.sub(r'(?<!["\'>])(https?://[^\s<>\[\]()]+)', r'<a href="\1">\1</a>', text)
    return text

def apply_markup(text: str, markup: List[Dict]) -> str:
    if not markup or not text: return text
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
    
    sorted_markup = sorted(corrected_markup, key=lambda m: (m.get('from', 0), -m.get('length', 0)))
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
                    result.append(open_tag.replace('<', '</'))
        if i in tag_starts:
            for open_tag in tag_starts[i]:
                open_tags.append(open_tag)
                result.append(open_tag)
        result.append(char)
    for open_tag in reversed(open_tags):
        result.append(open_tag.replace('<', '</'))
    return ''.join(result)

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
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=120)) as r:
                if r.status == 200: return await r.read()
                return None
    except: return None

# ===================================================================
# 11. MEDIA PROCESSOR (ИСПРАВЛЕНО: ФОТО/ВИДЕО КАК ФАЙЛ → ДОКУМЕНТ)
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
        
        # ЯВНЫЕ ТИПЫ ОТ MAX
        if atype == 'voice': return 'voice', meta
        if atype == 'audio': return 'audio', meta
        if atype == 'video': return 'video', meta
        if atype in ('image', 'photo'): return 'photo', meta
        
        # ТОЛЬКО ГОЛОСОВЫЕ И АУДИО ПО РАСШИРЕНИЮ
        if ext in self.VOICE_EXTS: return 'voice', meta
        if ext in self.AUDIO_EXTS: return 'audio', meta
        
        # ВСЁ ОСТАЛЬНОЕ (включая фото/видео как файл) — ДОКУМЕНТ
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
                resp = json.loads(await r.text())
                if r.status == 200 and resp.get('ok'): return resp
                elif r.status == 429:
                    await asyncio.sleep(resp.get('parameters', {}).get('retry_after', 10))
                    return await self._request(method, **kw)
                return None
        except: return None

    async def send_text(self, text: str, reply_markup: Optional[Dict] = None) -> bool:
        if not text: return True
        payload = {'chat_id': self.chat_id, 'text': fix_broken_html(text), 'parse_mode': 'HTML'}
        if reply_markup: payload['reply_markup'] = reply_markup
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
                obj['caption'] = fix_broken_html(caption)[:1024]
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
        safe_fname = safe_filename(filename) if filename else f"{media_type}.file"
        if is_url: form.add_field(field, media_data)
        else: form.add_field(field, media_data, filename=safe_fname)
        if caption and media_type != 'document':
            form.add_field('caption', fix_broken_html(caption)[:1024])
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
    tg_type, meta = media_proc.determine(att)
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

async def handle_max_message(msg: Dict):
    data = extract_message_data(msg)
    if not data['text'] and not data['attachments']: return

    text = data['text']
    if data['markup']: text = apply_markup(text, data['markup'])
    elif text and ('*' in text or '_' in text or '[' in text): text = parse_markdown_to_html(text)

    media_items, other = [], []
    for att in data['attachments']:
        t, m = media_proc.determine(att)
        (media_items if t in ('photo', 'video') else other).append({'type': t, 'attachment': att, 'meta': m})

    reply_markup = convert_max_buttons(data.get('reply_markup', {}))

    if media_items:
        if len(media_items) == 1:
            await process_attachment(media_items[0]['attachment'], text)
        else:
            downloaded = []
            for item in media_items:
                url = item['meta'].get('url')
                if not url: continue
                data = await download_from_url(url)
                if data: downloaded.append({'type': item['type'], 'data': data, 'filename': safe_filename(item['meta'].get('filename', ''))})
            if downloaded:
                ok = await tg.send_media_group_via_download(downloaded, text)
                if not ok:
                    for i, item in enumerate(downloaded):
                        await tg.send_media(item['type'], item['data'], caption=text if i==0 else "", filename=item['filename'])
                        await asyncio.sleep(0.3)
    elif text:
        await tg.send_text(text, reply_markup)

    for att in other:
        await process_attachment(att, "")

# ===================================================================
# 15. WEBHOOK HANDLER
# ===================================================================
async def webhook_handler(request):
    if request.method != 'POST': return web.Response(status=405)
    try:
        body = await request.json()
        if body.get('update_type') == 'message_created' and (msg := body.get('message')):
            asyncio.create_task(handle_max_message(msg))
        return web.Response(status=200)
    except: return web.Response(status=500)

async def health_handler(request):
    return web.json_response({'ok': True, 'version': 'final'})

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
    asyncio.run(main())
