# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ФИНАЛЬНАЯ ВЕРСИЯ: дедупликация через set() в памяти
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
POLL_SEC = int(os.getenv('POLL_INTERVAL', '15'))

LAST_SEQ_FILE = 'last_seq.txt'

# ДЕДУПЛИКАЦИЯ В ПАМЯТИ
_processed_seqs = set()

logger.info("=" * 100)
logger.info("🚀 MAX → TELEGRAM FORWARDER [FINAL VERSION]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"⏱️  Poll Interval: {POLL_SEC}s")
logger.info(f"📊 LOG_LEVEL: {LOG_LEVEL}")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

# ===================================================================
# 3. РАБОТА С last_seq (СОХРАНЕНИЕ В ФАЙЛ)
# ===================================================================
def load_last_seq() -> int:
    """Загружает последний обработанный seq из файла."""
    try:
        with open(LAST_SEQ_FILE, 'r') as f:
            seq = int(f.read().strip())
            logger.info(f"[SEQ] Loaded last_seq = {seq}")
            return seq
    except FileNotFoundError:
        logger.info("[SEQ] No last_seq file, starting from 0")
        return 0
    except Exception as e:
        logger.error(f"[SEQ] Error loading last_seq: {e}")
        return 0

def save_last_seq(seq: int):
    """Сохраняет последний обработанный seq в файл."""
    try:
        with open(LAST_SEQ_FILE, 'w') as f:
            f.write(str(seq))
        logger.debug(f"[SEQ] Saved last_seq = {seq}")
    except Exception as e:
        logger.error(f"[SEQ] Error saving last_seq: {e}")

# ===================================================================
# 4. ИСПРАВЛЕНИЕ БИТЫХ HTML ТЕГОВ
# ===================================================================
def fix_broken_html(text: str) -> str:
    """Исправляет незакрытые HTML теги."""
    if not text:
        return text
    
    tags = ['b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 
            'code', 'pre', 'a', 'tg-spoiler']
    
    for tag in tags:
        open_pattern = f'<{tag}[^>]*>'
        close_pattern = f'</{tag}>'
        
        open_count = len(re.findall(open_pattern, text, re.IGNORECASE))
        close_count = len(re.findall(close_pattern, text, re.IGNORECASE))
        
        if open_count > close_count:
            text += f'</{tag}>' * (open_count - close_count)
            logger.debug(f"[HTML] Added {open_count - close_count} </{tag}>")
        elif close_count > open_count:
            text = re.sub(close_pattern, f'&lt;/{tag}&gt;', text, flags=re.IGNORECASE)
            logger.debug(f"[HTML] Escaped extra </{tag}>")
    
    open_a = len(re.findall(r'<a\s+[^>]*>', text, re.IGNORECASE))
    close_a = len(re.findall(r'</a>', text, re.IGNORECASE))
    if open_a > close_a:
        text += '</a>' * (open_a - close_a)
        logger.debug(f"[HTML] Added {open_a - close_a} </a>")
    
    return text


# ===================================================================
# 5. ГРАФЕМЫ И РАЗМЕТКА
# ===================================================================
def split_into_graphemes(text: str) -> List[str]:
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
    """Ищет разметку во всех возможных полях."""
    markup_fields = [
        'markup', 'entities', 'formats', 'styles', 
        'annotations', 'text_entities', 'message_entities'
    ]
    
    logger.debug(f"[MARKUP] Message keys: {list(msg.keys())}")
    
    body = msg.get('body', {})
    if isinstance(body, dict):
        logger.debug(f"[MARKUP] Body keys: {list(body.keys())}")
        for field in markup_fields:
            if field in body and body[field]:
                logger.info(f"[MARKUP] ✅ Found in body.{field}")
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP] Raw: {json.dumps(body[field], ensure_ascii=False)[:500]}")
                return body[field], f"body.{field}"
    
    for field in markup_fields:
        if field in msg and msg[field]:
            logger.info(f"[MARKUP] ✅ Found in root.{field}")
            if LOG_MARKUP:
                logger.debug(f"[MARKUP] Raw: {json.dumps(msg[field], ensure_ascii=False)[:500]}")
            return msg[field], f"root.{field}"
    
    link = msg.get('link', {})
    if isinstance(link, dict) and 'message' in link:
        inner = link['message']
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
    
    logger.info("[MARKUP] ❌ No markup found")
    return [], "none"


def apply_markup(text: str, markup: List[Dict]) -> str:
    """Применяет разметку MAX к тексту."""
    if not markup or not text:
        return text
    
    logger.info(f"[MARKUP] Converting: {len(text)} chars, {len(markup)} items")
    
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
    
    events = []
    for idx, m in enumerate(markup):
        try:
            start = int(m.get("from") or m.get("offset") or 0)
            length = int(m.get("length") or 0)
            mtype = m.get("type") or m.get("tag") or ""
            end = start + length
            
            if start < 0 or end > n or length <= 0:
                continue
            
            if mtype in TAGS:
                open_tag, close_tag = TAGS[mtype]
                events.append((start, 'open', open_tag, idx))
                events.append((end, 'close', close_tag, idx))
                logger.debug(f"[MARKUP] {mtype} [{start}:{end}]")
            elif mtype in ("link", "text_link", "url"):
                url = m.get("url") or m.get("href") or ""
                if url:
                    url_safe = url.replace('"', '&quot;')
                    events.append((start, 'open', f'<a href="{url_safe}">', idx))
                    events.append((end, 'close', '</a>', idx))
                    logger.debug(f"[MARKUP] link [{start}:{end}] -> {url[:50]}")
        except Exception as e:
            logger.error(f"[MARKUP] Error item {idx}: {e}")
    
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
    logger.info(f"[MARKUP] Result preview: {final_text[:100]}...")
    return final_text


# ===================================================================
# 6. ФОРВАРДЫ
# ===================================================================
def extract_forward_info(msg: Dict, depth: int = 0, visited_ids: set = None) -> Optional[Dict]:
    if visited_ids is None:
        visited_ids = set()
    
    if depth > 10:
        return None
    
    msg_id = msg.get('mid') or msg.get('id')
    if msg_id and msg_id in visited_ids:
        return None
    if msg_id:
        visited_ids.add(msg_id)
    
    fwd_fields = ['link', 'forward', 'fwd', 'forwarded_message', 'reply_to']
    
    for field in fwd_fields:
        if field in msg and msg[field]:
            fwd_data = msg[field]
            logger.info(f"[FWD] Found in {field}")
            
            if isinstance(fwd_data, dict) and 'message' in fwd_data:
                return {"message": fwd_data['message']}
            
            if isinstance(fwd_data, dict) and ('text' in fwd_data or 'body' in fwd_data):
                return {"message": fwd_data}
    
    body = msg.get('body', {})
    if isinstance(body, dict):
        return extract_forward_info(body, depth + 1, visited_ids)
    
    return None


# ===================================================================
# 7. ИЗВЛЕЧЕНИЕ ДАННЫХ
# ===================================================================
def safe_list(val: Any) -> List[Dict]:
    if val is None:
        return []
    if isinstance(val, list):
        return [v for v in val if isinstance(v, dict)]
    if isinstance(val, dict):
        for k in ['attachments', 'files', 'media']:
            if k in val:
                v = val[k]
                return v if isinstance(v, list) else [v] if isinstance(v, dict) else []
    return []


def extract_data(msg: Dict) -> Dict:
    fwd = extract_forward_info(msg)
    if fwd and fwd.get('message'):
        inner = fwd['message']
        body = inner.get('body', {})
        markup, source = find_markup_in_message(inner)
        
        return {
            "source": f"forward",
            "mid": inner.get('mid') or inner.get('id'),
            "seq": inner.get('seq') or body.get('seq'),
            "text": inner.get('text', '') or body.get('text', ''),
            "markup": markup,
            "attachments": safe_list(inner.get('attachments') or body.get('attachments')),
            "is_forward": True
        }
    
    body = msg.get('body', {})
    markup, source = find_markup_in_message(msg)
    
    if isinstance(body, dict):
        return {
            "source": "body",
            "mid": body.get('mid') or msg.get('mid'),
            "seq": body.get('seq') or msg.get('seq'),
            "text": body.get('text', ''),
            "markup": markup,
            "attachments": safe_list(body.get('attachments')),
            "is_forward": False
        }
    
    return {
        "source": "root",
        "mid": msg.get('mid'),
        "seq": msg.get('seq'),
        "text": msg.get('text', ''),
        "markup": markup,
        "attachments": safe_list(msg.get('attachments')),
        "is_forward": False
    }


# ===================================================================
# 8. АУДИО УТИЛИТЫ
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


def extract_audio_tags(file_data: bytes, filename: str) -> Dict[str, Any]:
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
    
    name = filename.rsplit('.', 1)[0] if '.' in filename else filename
    if ' - ' in name:
        parts = name.split(' - ', 1)
        performer = performer or parts[0].strip()
        title = title or parts[1].strip()
    else:
        title = title or name.strip()
        performer = performer or 'Unknown'
    
    logger.info(f"[AUDIO] Tags: {performer} - {title} ({duration}s)")
    return {'performer': performer[:64], 'title': title[:64], 'duration': duration}


def convert_to_voice(file_data: bytes) -> Optional[bytes]:
    try:
        subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
    except:
        logger.error("[VOICE] FFmpeg not available")
        return None
    
    try:
        with tempfile.NamedTemporaryFile(suffix='.tmp', delete=False) as tmp_in:
            tmp_in.write(file_data)
            tmp_in_path = tmp_in.name
        
        with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp_out:
            tmp_out_path = tmp_out.name
        
        cmd = ['ffmpeg', '-i', tmp_in_path, '-ac', '1', '-ar', '16000',
               '-c:a', 'libopus', '-b:a', '16k', '-vbr', 'on',
               '-application', 'voip', '-y', tmp_out_path]
        
        logger.info(f"[VOICE] Converting: {len(file_data)} bytes...")
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        
        if result.returncode != 0:
            logger.error(f"[VOICE] FFmpeg error: {result.stderr[:300] if result.stderr else 'unknown'}")
            os.unlink(tmp_in_path)
            return None
        
        with open(tmp_out_path, 'rb') as f:
            ogg_data = f.read()
        
        os.unlink(tmp_in_path)
        os.unlink(tmp_out_path)
        
        logger.info(f"[VOICE] Converted: {len(file_data)} -> {len(ogg_data)} bytes")
        return ogg_data
    except Exception as e:
        logger.error(f"[VOICE] Exception: {e}")
        return None


# ===================================================================
# 9. MEDIA PROCESSOR
# ===================================================================
class MediaProcessor:
    PHOTO_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp', 'heic', 'heif'}
    VIDEO_EXTS = {'mp4', 'mov', 'avi', 'mkv', 'webm', 'flv', 'm4v', '3gp'}
    AUDIO_EXTS = {'mp3', 'wav', 'm4a', 'flac', 'aac', 'wma'}
    VOICE_EXTS = {'ogg', 'opus', 'oga'}
    
    def __init__(self):
        self.ffmpeg_ok = self._check_ffmpeg()
        logger.info(f"[MEDIA] FFmpeg available: {self.ffmpeg_ok}")
    
    def _check_ffmpeg(self):
        try:
            subprocess.run(['ffmpeg', '-version'], capture_output=True, check=True)
            return True
        except:
            return False
    
    def determine(self, att: Dict) -> Tuple[str, Dict]:
        """Определяет тип медиа с детальным логированием."""
        atype = att.get('type') or att.get('media_type') or 'file'
        payload = att.get('payload', {})
        
        fname = payload.get('filename') or att.get('filename') or ''
        size = payload.get('size') or att.get('size') or 0
        url = payload.get('url')
        token = payload.get('token') or att.get('token') or att.get('file_token')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        
        logger.info(f"[MEDIA] atype={atype}, ext={ext}, size={size}")
        
        if LOG_MEDIA:
            logger.info(f"[MEDIA] Full attachment: {json.dumps(att, ensure_ascii=False)[:500]}")
        
        meta = {
            'filename': fname,
            'size': size,
            'url': url,
            'token': token,
            'ext': ext,
            'original_type': atype
        }
        
        # ЯВНЫЕ ТИПЫ ОТ MAX
        if atype == 'voice':
            logger.info(f"[MEDIA] ✅ voice (explicit type)")
            return 'voice', meta
        if atype == 'audio':
            logger.info(f"[MEDIA] ✅ audio (explicit type)")
            return 'audio', meta
        if atype == 'video':
            logger.info(f"[MEDIA] ✅ video (explicit type)")
            return 'video', meta
        if atype in ('image', 'photo', 'picture'):
            logger.info(f"[MEDIA] ✅ photo (explicit type)")
            return 'photo', meta
        
        # ПО РАСШИРЕНИЮ
        if ext in self.VOICE_EXTS:
            logger.info(f"[MEDIA] ✅ voice (extension .{ext})")
            return 'voice', meta
        if ext in self.AUDIO_EXTS:
            logger.info(f"[MEDIA] ✅ audio (extension .{ext})")
            return 'audio', meta
        if ext in self.PHOTO_EXTS:
            logger.info(f"[MEDIA] ✅ photo (extension .{ext})")
            return 'photo', meta
        if ext in self.VIDEO_EXTS:
            logger.info(f"[MEDIA] ✅ video (extension .{ext})")
            return 'video', meta
        
        logger.info(f"[MEDIA] 📄 document (fallback)")
        return 'document', meta


# ===================================================================
# 10. TELEGRAM CLIENT
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
    
    async def send(self, method: str, **kw) -> Optional[Dict]:
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
                except:
                    logger.error(f"[TG] Invalid JSON: {txt[:200]}")
                    return None
                
                if r.status == 200 and resp.get('ok'):
                    msg_id = resp.get('result', {}).get('message_id')
                    logger.info(f"[TG] ✅ message_id={msg_id}")
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] ⏳ Rate limit, wait {wait}s")
                    await asyncio.sleep(wait)
                    return await self.send(method, **kw)
                else:
                    logger.error(f"[TG] ❌ {resp.get('description')}")
                    return resp
        except Exception as e:
            logger.error(f"[TG] Exception: {e}")
            return None
    
    async def text(self, text: str) -> bool:
        if not text or not text.strip():
            return True
        
        text = fix_broken_html(text)
        logger.info(f"[TG] Sending text: {text[:50]}...")
        
        resp = await self.send('sendMessage', json={
            'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML'
        })
        return resp and resp.get('ok', False)
    
    async def media(self, type_: str, media_data: Union[str, bytes],
                    caption: str = "", filename: str = "", is_url: bool = False,
                    **extra) -> bool:
        
        method_map = {'photo': 'sendPhoto', 'video': 'sendVideo',
                      'audio': 'sendAudio', 'voice': 'sendVoice',
                      'document': 'sendDocument'}
        method = method_map.get(type_, 'sendDocument')
        field = type_ if type_ != 'document' else 'document'
        
        logger.info(f"[TG] Sending {type_}: is_url={is_url}")
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        if is_url:
            form.add_field(field, media_data)
        else:
            fname = filename or f"{type_}.file"
            form.add_field(field, media_data, filename=fname)
        
        if caption and type_ != 'document':
            caption = fix_broken_html(caption)
            form.add_field('caption', caption[:1024])
            form.add_field('parse_mode', 'HTML')
        
        if type_ == 'audio':
            if extra.get('performer'):
                form.add_field('performer', extra['performer'][:64])
            if extra.get('title'):
                form.add_field('title', extra['title'][:64])
            if extra.get('duration'):
                form.add_field('duration', str(extra['duration']))
        
        if type_ == 'voice' and extra.get('duration'):
            form.add_field('duration', str(extra['duration']))
        
        resp = await self.send(method, data=form)
        return resp and resp.get('ok', False)


# ===================================================================
# 11. MAX CLIENT
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
    
    async def fetch(self, limit: int = 10) -> List[Dict]:
        await self.init()
        try:
            params = {'chat_id': self.cid, 'limit': limit}
            logger.debug(f"[MAX] Fetching last {limit} messages")
            
            async with self.session.get(
                f"{self.base}/messages",
                headers={'Authorization': self.token},
                params=params
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    if LOG_RAW_MAX:
                        logger.debug(f"[MAX-RESP] {json.dumps(raw, ensure_ascii=False)[:500]}")
                    msgs = raw.get('messages', [])
                    logger.info(f"[MAX] Got {len(msgs)} messages")
                    return msgs
                else:
                    text = await r.text()
                    logger.error(f"[MAX] HTTP {r.status}: {text[:200]}")
                    return []
        except Exception as e:
            logger.error(f"[MAX] Exception: {e}")
            return []
    
    async def download(self, token: str) -> Optional[bytes]:
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
                    logger.debug(f"[MAX-DL] Trying: {url[:60]}...")
                    
                    async with self.session.get(url, headers=headers) as r:
                        if r.status == 200:
                            data = await r.read()
                            logger.info(f"[MAX-DL] ✅ Downloaded {len(data)} bytes")
                            return data
                        else:
                            text = await r.text()
                            logger.debug(f"[MAX-DL] Failed: {r.status} - {text[:100]}")
                except Exception as e:
                    logger.debug(f"[MAX-DL] Error: {e}")
        
        logger.error(f"[MAX-DL] ❌ All download attempts failed")
        return None


# ===================================================================
# 12. ОБРАБОТЧИК
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


def extract_seq(msg: Dict) -> int:
    body = msg.get('body', {})
    seq = body.get('seq') or msg.get('seq')
    return int(seq) if seq else 0


async def process_attachment(att: Dict, caption: str = "") -> bool:
    tg_type, meta = media_proc.determine(att)
    
    logger.info(f"[ATT] Processing: type={tg_type}, filename={meta.get('filename')}, size={meta.get('size')}")
    logger.info(f"[ATT] Has URL: {bool(meta.get('url'))}, Has token: {bool(meta.get('token'))}")
    
    media_input = None
    is_url = False
    file_data = None
    
    # Документы, аудио, голосовые - всегда через токен
    if tg_type in ('document', 'audio', 'voice'):
        if not meta.get('token'):
            logger.error(f"[ATT] ❌ No token for {tg_type}")
            return False
        
        logger.info(f"[ATT] 📥 Downloading {tg_type}...")
        file_data = await mx.download(meta['token'])
        
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
        logger.info(f"[ATT] 📥 Downloading (fallback)...")
        file_data = await mx.download(meta['token'])
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
        logger.info(f"[ATT] 🎤 Trying voice conversion...")
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
            logger.warning(f"[ATT] Conversion failed, sending as audio")
    
    # Теги для аудио
    if tg_type == 'audio' and file_data:
        tags = extract_audio_tags(file_data, meta.get('filename', ''))
        extra.update(tags)
        logger.info(f"[ATT] Audio tags: {tags}")
    
    # Отправка
    logger.info(f"[ATT] 📤 Sending {tg_type} to Telegram...")
    
    result = await tg.media(
        type_=tg_type,
        media_data=media_input,
        caption=caption if tg_type != 'document' else '',
        filename=meta.get('filename', ''),
        is_url=is_url,
        **extra
    )
    
    logger.info(f"[ATT] {'✅' if result else '❌'} Send result: {'OK' if result else 'FAIL'}")
    return result


async def handle_message(msg: Dict):
    seq = extract_seq(msg)
    mid = msg.get('body', {}).get('mid') or msg.get('mid', 'unknown')
    
    logger.info("=" * 60)
    logger.info(f"[HANDLE] mid={mid}, seq={seq}")
    
    if LOG_RAW_MAX:
        logger.debug(f"[HANDLE] Raw: {json.dumps(msg, ensure_ascii=False)[:1000]}")
    
    data = extract_data(msg)
    logger.info(f"[HANDLE] source={data['source']}, text_len={len(data['text'])}, att={len(data['attachments'])}")
    logger.info(f"[HANDLE] markup: {len(data['markup'])} items, is_forward={data['is_forward']}")
    
    text = data['text']
    if data['markup']:
        text = apply_markup(text, data['markup'])
    
    if text and text.strip():
        ok = await tg.text(text)
        if not ok:
            logger.error(f"[HANDLE] Failed to send text")
        await asyncio.sleep(0.3)
    
    for i, att in enumerate(data['attachments']):
        caption = text if i == 0 and not text else ""
        await process_attachment(att, caption)
        await asyncio.sleep(0.5)
    
    logger.info("=" * 60)


# ===================================================================
# 13. POLLING LOOP (С ДЕДУПЛИКАЦИЕЙ В ПАМЯТИ)
# ===================================================================
async def polling_loop():
    global _processed_seqs
    last_seq = load_last_seq()
    logger.info(f"🔄 Polling started, last_seq={last_seq}, processed_count={len(_processed_seqs)}")
    await asyncio.sleep(2)
    
    while True:
        try:
            msgs = await mx.fetch(limit=10)
            
            if not msgs:
                logger.debug("[POLL] No messages")
                await asyncio.sleep(POLL_SEC)
                continue
            
            msgs.sort(key=extract_seq)
            
            new_count = 0
            for msg in msgs:
                seq = extract_seq(msg)
                
                # ПРОПУСКАЕМ УЖЕ ОБРАБОТАННЫЕ
                if seq in _processed_seqs:
                    logger.debug(f"[POLL] ⏭ Skip already processed seq={seq}")
                    continue
                
                logger.info(f"[POLL] Processing NEW seq={seq}")
                await handle_message(msg)
                
                _processed_seqs.add(seq)
                new_count += 1
                
                if seq > last_seq:
                    last_seq = seq
                    save_last_seq(last_seq)
                
                await asyncio.sleep(2.0)
            
            if new_count > 0:
                logger.info(f"[POLL] Processed {new_count} new messages")
            else:
                logger.debug(f"[POLL] No new messages in batch")
            
            # Ограничиваем размер set
            if len(_processed_seqs) > 1000:
                sorted_seqs = sorted(_processed_seqs)
                _processed_seqs = set(sorted_seqs[-500:])
                logger.debug(f"[POLL] Cleaned processed set, now {len(_processed_seqs)} items")
            
        except Exception as e:
            logger.error(f"[POLL] {e}", exc_info=True)
        
        await asyncio.sleep(POLL_SEC)


# ===================================================================
# 14. WEB SERVER
# ===================================================================
async def health(request):
    return web.json_response({
        'ok': True,
        'last_seq': load_last_seq(),
        'processed_count': len(_processed_seqs)
    })

async def reset_handler(request):
    global _processed_seqs
    save_last_seq(0)
    _processed_seqs.clear()
    logger.warning("[RESET] last_seq=0, processed_seqs cleared")
    return web.json_response({'ok': True, 'last_seq': 0, 'processed_cleared': True})


async def run():
    app = web.Application()
    app.router.add_get('/health', health)
    app.router.add_get('/reset', reset_handler)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    
    logger.info("🌐 Server on :8080 (/health, /reset)")
    await polling_loop()


if __name__ == '__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 FATAL: {e}")
        sys.exit(1)
