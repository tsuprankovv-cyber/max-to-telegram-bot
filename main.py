# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ С ПОЛНЫМ ЛОГИРОВАНИЕМ
- Исправлена сортировка закрывающих тегов (LIFO)
- Прямые offset без графем (эмодзи не сбивают)
- Гибридная отправка медиа
- Полное логирование запросов и ответов
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
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL FIXED VERSION]")
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
# 4. КОНВЕРТАЦИЯ РАЗМЕТКИ (ИСПРАВЛЕННАЯ СОРТИРОВКА)
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
    return text

def apply_markup(text: str, markup: List[Dict]) -> str:
    if not markup or not text: return text
    
    all_from_zero = all(m.get('from', 0) == 0 for m in markup)
    max_length = max((m.get('length', 0) for m in markup), default=0)
    if all_from_zero and max_length < len(text):
        logger.warning("[MARKUP] ⚠️ Broken markup, switching to Markdown.")
        return parse_markdown_to_html(text)
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, entities={len(markup)}")
    start_time = time.time()
    
    events = []
    for idx, entity in enumerate(markup):
        try:
            offset = int(entity.get('from', 0))
            length = int(entity.get('length', 0))
            etype = entity.get('type', '')
            if offset < 0 or length <= 0 or offset + length > len(text): continue
            if etype not in MAX_TAG_MAP: continue
            
            tag_name = MAX_TAG_MAP[etype]
            if etype in ('link', 'text_link', 'url'):
                url = entity.get('url', '').replace('"', '&quot;')
                open_tag = f'<{tag_name} href="{url}">' if url else f'<{tag_name}>'
            else:
                open_tag = f'<{tag_name}>'
            close_tag = f'</{tag_name}>'
            
            events.append((offset, 'open', open_tag, close_tag, idx))
            events.append((offset + length, 'close', open_tag, close_tag, idx))
            logger.debug(f"[MARKUP] {etype}: [{offset}:{offset+length}] -> <{tag_name}>")
        except Exception as e:
            logger.error(f"[MARKUP] Error: {e}")

    # Сортируем события: сначала по позиции, закрывающие перед открывающими
    events.sort(key=lambda x: (x[0], 0 if x[1] == 'close' else 1, -x[4]))
    
    result = []
    open_stack = []
    event_idx = 0
    n = len(text)
    
    for pos in range(n + 1):
        # Собираем все события на текущей позиции
        pos_events = []
        while event_idx < len(events) and events[event_idx][0] == pos:
            pos_events.append(events[event_idx])
            event_idx += 1
        
        # ВАЖНО: Сортируем события на одной позиции
        # 1. Сначала ВСЕ закрывающие (в обратном порядке приоритета = LIFO)
        # 2. Потом ВСЕ открывающие
        close_events = sorted([e for e in pos_events if e[1] == 'close'], key=lambda x: -x[4])
        open_events = sorted([e for e in pos_events if e[1] == 'open'], key=lambda x: -x[4])
        
        # Обрабатываем закрывающие
        for _, _, _, close_tag, priority in close_events:
            for i in range(len(open_stack) - 1, -1, -1):
                if open_stack[i][0] == close_tag and open_stack[i][1] == priority:
                    result.append(close_tag)
                    open_stack.pop(i)
                    break
        
        # Обрабатываем открывающие
        for _, _, open_tag, close_tag, priority in open_events:
            open_stack.append((close_tag, priority))
            result.append(open_tag)
        
        if pos < n:
            result.append(text[pos])
    
    # Закрываем оставшиеся теги
    for close_tag, _ in reversed(open_stack):
        result.append(close_tag)
    
    final_text = ''.join(result)
    logger.info(f"[MARKUP] ✅ Converted in {time.time() - start_time:.2f}s")
    logger.debug(f"[MARKUP] Preview: {final_text[:200]}...")
    return final_text

# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def extract_message_data(msg: Dict) -> Dict:
    logger.info("[EXTRACT] Starting extraction...")
    link = msg.get('link', {})
    is_forward = isinstance(link, dict) and link.get('type') == 'forward' and 'message' in link
    inner = link['message'] if is_forward else msg
    
    body = inner.get('body', {})
    text = body.get('text', '') or inner.get('text', '')
    markup = body.get('markup', []) or inner.get('markup', [])
    
    att_list = body.get('attachments') or inner.get('attachments') or []
    attachments = [a for a in att_list if isinstance(a, dict)]
    
    return {
        "mid": body.get('mid') or inner.get('mid', ''),
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
                if r.status == 200:
                    data = await r.read()
                    logger.info(f"[DOWNLOAD] ✅ {len(data)} bytes in {time.time() - start_time:.2f}s")
                    return data
                logger.error(f"[DOWNLOAD] ❌ HTTP {r.status}")
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
        logger.info(f"[MEDIA] FFmpeg: {self.ffmpeg_ok}")
    
    def _check_ffmpeg(self) -> bool:
        try: subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True); return True
        except: return False
    
    def determine(self, att: Dict) -> Tuple[str, Dict]:
        atype = att.get('type', 'file')
        payload = att.get('payload', {})
        fname = payload.get('filename') or att.get('filename', '')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        meta = {'filename': fname, 'size': payload.get('size', 0), 'url': payload.get('url'), 'token': payload.get('token'), 'ext': ext, 'original_type': atype}
        
        logger.info(f"[MEDIA] 🔍 atype='{atype}', ext='{ext}'")
        if LOG_MEDIA: logger.debug(f"[MEDIA] Full: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        if atype == 'voice': return 'voice', meta
        if atype == 'audio': return 'audio', meta
        if atype == 'video': return 'video', meta
        if atype in ('image', 'photo'): return 'photo', meta
        
        if ext in self.VOICE_EXTS: return 'voice', meta
        if ext in self.AUDIO_EXTS: return 'audio', meta
        if ext in self.PHOTO_EXTS: return 'photo', meta
        if ext in self.VIDEO_EXTS: return 'video', meta
        return 'document', meta

# ===================================================================
# 9. TELEGRAM CLIENT
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
        logger.info(f"[TG] ▶️ {method}")
        if LOG_RAW_TG: logger.debug(f"[TG-REQ] {json.dumps(kw, default=str, ensure_ascii=False)[:500]}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                logger.info(f"[TG] Status: {r.status}")
                logger.info(f"[TG-RESP] {txt}")  # ПОЛНЫЙ ОТВЕТ
                
                resp = json.loads(txt)
                if r.status == 200 and resp.get('ok'):
                    logger.info(f"[TG] ✅ Success: msg_id={resp.get('result', {}).get('message_id')}")
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] ⏳ Rate limit, wait {wait}s")
                    await asyncio.sleep(wait)
                    return await self._request(method, **kw)
                else:
                    logger.error(f"[TG] ❌ {resp.get('description')}")
                    return resp
        except Exception as e:
            logger.error(f"[TG] ❌ {e}")
            return None

    async def send_text(self, text: str) -> bool:
        if not text: return True
        text = fix_broken_html(text)
        logger.info(f"[TG] 📤 Text: {text[:100]}...")
        resp = await self._request('sendMessage', json={'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML'})
        return resp and resp.get('ok', False)

    async def send_media_group_direct(self, items: List[Dict]) -> bool:
        if not items: return True
        if len(items) > 10: items = items[:10]
        logger.info(f"[TG] 📤 Media group (direct): {len(items)} items")
        
        input_media = []
        for i, item in enumerate(items):
            obj = {'type': item['type'], 'media': item['media']}
            if i == 0 and item.get('caption'):
                obj['caption'] = fix_broken_html(item['caption'])[:1024]
                obj['parse_mode'] = 'HTML'
            input_media.append(obj)
        
        resp = await self._request('sendMediaGroup', json={'chat_id': self.chat_id, 'media': input_media})
        return resp and isinstance(resp, dict) and resp.get('ok', False)

    async def send_media_group_via_download(self, items: List[Dict], caption: str = None) -> bool:
        if not items: return True
        if len(items) > 10: items = items[:10]
        logger.info(f"[TG] 📤 Media group (download): {len(items)} items")
        
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
            await self._request('deleteMessage', json={'chat_id': self.chat_id, 'message_id': upload_resp['result']['message_id']})
        
        resp = await self._request('sendMediaGroup', json={'chat_id': self.chat_id, 'media': input_media})
        return resp and isinstance(resp, dict) and resp.get('ok', False)

    async def send_media(self, media_type: str, media_data, caption="", filename="", is_url=False, **extra) -> bool:
        method_map = {'photo': 'sendPhoto', 'video': 'sendVideo', 'audio': 'sendAudio', 'voice': 'sendVoice', 'document': 'sendDocument'}
        method = method_map.get(media_type, 'sendDocument')
        field = media_type if media_type != 'document' else 'document'
        
        logger.info(f"[TG] 📤 {media_type}: is_url={is_url}")
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        if is_url: form.add_field(field, media_data)
        else: form.add_field(field, media_data, filename=safe_filename(filename))
        
        if caption and media_type != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption[:1024])
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
# 10. MAX CLIENT
# ===================================================================
class MX:
    def __init__(self, token: str, cid: str, base: str):
        self.token, self.cid, self.base = token, cid, base
        self.session = None
    
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
    logger.info(f"[ATT] 📎 Processing...")
    tg_type, meta = media_proc.determine(att)
    logger.info(f"[ATT] Type: {tg_type}, file: {meta.get('filename')}")
    
    direct_url = meta.get('url')
    if not direct_url: return False
    
    if tg_type in ('photo', 'video') and direct_url:
        logger.info(f"[ATT] 📤 Using direct URL")
        res = await tg.send_media(tg_type, direct_url, caption, meta.get('filename', ''), is_url=True)
        logger.info(f"[ATT] {'✅' if res else '❌'} in {time.time() - start_time:.2f}s")
        return res
        
    file_data = await download_from_url(direct_url)
    if not file_data: return False
    
    extra = {}
    if tg_type == 'audio' and meta.get('size', 0) < 2*1024*1024 and media_proc.ffmpeg_ok:
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type, file_data = 'voice', voice_data
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                extra['duration'] = get_audio_duration(tmp.name)
                os.unlink(tmp.name)
    
    if tg_type == 'audio':
        extra.update(extract_audio_tags(file_data, meta.get('filename', '')))
    
    res = await tg.send_media(tg_type, file_data, caption if tg_type != 'document' else '', meta.get('filename', ''), False, **extra)
    logger.info(f"[ATT] {'✅' if res else '❌'} in {time.time() - start_time:.2f}s")
    return res

async def handle_max_message(msg: Dict):
    start_time = time.time()
    logger.info("=" * 80)
    logger.info(f"[HANDLE] 🚀 Processing...")
    if LOG_RAW_MAX: logger.debug(f"[HANDLE] Raw: {json.dumps(msg, ensure_ascii=False)[:2000]}")

    data = extract_message_data(msg)
    if not data['text'] and not data['attachments']: return

    text = data['text']
    if data['markup']:
        logger.info(f"[HANDLE] Applying markup ({len(data['markup'])} items)")
        text = apply_markup(text, data['markup'])
    elif text and ('*' in text or '_' in text or '[' in text):
        text = parse_markdown_to_html(text)

    media_items, other = [], []
    for att in data['attachments']:
        t, _ = media_proc.determine(att)
        (media_items if t in ('photo', 'video') else other).append({'type': t, 'attachment': att, 'meta': _})

    logger.info(f"[HANDLE] Media: {len(media_items)}, Other: {len(other)}")

    use_new = False
    if len(media_items) > 1:
        has_v, has_p = any(i['type']=='video' for i in media_items), any(i['type']=='photo' for i in media_items)
        if has_v and (has_p or sum(1 for i in media_items if i['type']=='video') >= 2):
            use_new = True
            logger.info("[HANDLE] 🔄 NEW scheme (mixed/video group)")

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
                if data: downloaded.append({'type': item['type'], 'data': data, 'filename': safe_filename(item['meta'].get('filename', ''))})
            if downloaded:
                ok = await tg.send_media_group_via_download(downloaded, text)
                if not ok:
                    for i, item in enumerate(downloaded):
                        await tg.send_media(item['type'], item['data'], caption=text if i==0 else "", filename=item['filename'])
                        await asyncio.sleep(0.3)
        else:
            items = [{'type': i['type'], 'media': i['meta'].get('url'), 'caption': text if idx==0 else None} for idx, i in enumerate(media_items) if i['meta'].get('url')]
            if items:
                ok = await tg.send_media_group_direct(items)
                if not ok:
                    for i, item in enumerate(media_items):
                        await process_attachment(item['attachment'], text if i==0 else "")
                        await asyncio.sleep(0.3)
    elif text:
        await tg.send_text(text)
        await asyncio.sleep(0.3)

    for att in other:
        await process_attachment(att, "")
        await asyncio.sleep(0.5)

    logger.info(f"[HANDLE] ✅ Complete in {time.time() - start_time:.2f}s")
    logger.info("=" * 80)

# ===================================================================
# 12. WEBHOOK HANDLER (МАКСИМАЛЬНОЕ ЛОГИРОВАНИЕ)
# ===================================================================
async def webhook_handler(request):
    logger.info("=" * 60)
    logger.info(f"[WEBHOOK] 📨 {request.method} from {request.remote}")
    if request.method != 'POST':
        return web.Response(status=405)
    
    if VERIFY_WEBHOOK_SECRET and MAX_WEBHOOK_SECRET:
        if request.headers.get('X-Max-Bot-Api-Secret') != MAX_WEBHOOK_SECRET:
            logger.warning("[WEBHOOK] ❌ Invalid secret")
            return web.Response(status=403)
    
    try:
        body = await request.json()
        logger.info(f"[WEBHOOK] Keys: {list(body.keys())}, Type: {body.get('update_type')}")
        logger.info(f"[WEBHOOK] FULL BODY:\n{json.dumps(body, ensure_ascii=False, indent=2)}")
        
        if body.get('update_type') == 'message_created' and (msg := body.get('message')):
            asyncio.create_task(handle_max_message(msg))
            logger.info("[WEBHOOK] ✅ Queued")
        return web.Response(status=200)
    except Exception as e:
        logger.error(f"[WEBHOOK] ❌ {e}", exc_info=True)
        return web.Response(status=500)
    finally:
        logger.info("=" * 60)

async def health_handler(request):
    return web.json_response({'ok': True, 'version': 'final-fixed'})

# ===================================================================
# 13. ЗАПУСК
# ===================================================================
async def main():
    logger.info("🚀 Starting MAX → Telegram Forwarder [FINAL FIXED]...")
    if RENDER_EXTERNAL_URL:
        await mx.register_webhook(f"{RENDER_EXTERNAL_URL}/webhook", MAX_WEBHOOK_SECRET)
    
    app = web.Application()
    app.router.add_post('/webhook', webhook_handler)
    app.router.add_get('/health', health_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', int(os.getenv('PORT', 8080))).start()
    logger.info(f"🌐 Ready on port {os.getenv('PORT', 8080)}")
    await asyncio.Event().wait()

if __name__ == '__main__':
    asyncio.run(main())
