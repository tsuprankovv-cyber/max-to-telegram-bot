# -*- coding: utf-8 -*-
"""
MAX → Telegram Forwarder
ИСПРАВЛЕННАЯ ВЕРСИЯ: форматирование, документы, голосовые, аудио
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
logger.info("🚀 MAX → TELEGRAM FORWARDER [FIXED VERSION]")
logger.info(f"📡 MAX Channel: {MAX_CHAN}")
logger.info(f"📥 Telegram Chat: {TG_CHAT}")
logger.info(f"⏱️  Poll Interval: {POLL_SEC}s")
logger.info("=" * 100)

if not all([TG_TOKEN, TG_CHAT, MAX_TOKEN, MAX_CHAN]):
    logger.critical("❌ FATAL: Missing required environment variables!")
    sys.exit(1)

# ===================================================================
# 3. ГРАФЕМЫ И РАЗМЕТКА
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
    possible_fields = ['markup', 'entities', 'formats', 'styles', 'annotations', 'text_entities']
    
    body = msg.get('body', {})
    if isinstance(body, dict):
        for field in possible_fields:
            if field in body and body[field]:
                logger.info(f"[MARKUP] Found in body.{field}")
                if LOG_MARKUP:
                    logger.debug(f"[MARKUP-RAW] {json.dumps(body[field], ensure_ascii=False)[:500]}")
                return body[field], f"body.{field}"
    
    for field in possible_fields:
        if field in msg and msg[field]:
            logger.info(f"[MARKUP] Found in root.{field}")
            return msg[field], f"root.{field}"
    
    return [], "none"


def apply_markup(text: str, markup: List[Dict]) -> str:
    if not markup or not text:
        return text
    
    logger.info(f"[MARKUP] Converting: text_len={len(text)}, items={len(markup)}")
    
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
            elif mtype in ("link", "text_link", "url"):
                url = m.get("url") or m.get("href") or ""
                if url:
                    url_safe = url.replace('"', '&quot;').replace('<', '&lt;').replace('>', '&gt;')
                    open_tag = f'<a href="{url_safe}">'
                    close_tag = "</a>"
                    events.append((start, 'open', open_tag, idx))
                    events.append((end, 'close', close_tag, idx))
        except Exception as e:
            logger.error(f"[MARKUP] Error: {e}")
    
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
    
    return "".join(result)


# ===================================================================
# 4. ФОРВАРДЫ (без префикса)
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
                inner = fwd_data['message']
                return {
                    "message": inner,
                    "original_chat_id": fwd_data.get('chat_id')
                }
            
            if isinstance(fwd_data, dict) and ('text' in fwd_data or 'body' in fwd_data):
                return {"message": fwd_data}
    
    body = msg.get('body', {})
    if isinstance(body, dict):
        return extract_forward_info(body, depth + 1, visited_ids)
    
    return None


# ===================================================================
# 5. ИЗВЛЕЧЕНИЕ ДАННЫХ
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
        markup, _ = find_markup_in_message(inner)
        
        return {
            "source": "forward",
            "mid": inner.get('mid') or inner.get('id'),
            "seq": inner.get('seq') or body.get('seq'),
            "text": inner.get('text', '') or body.get('text', ''),
            "markup": markup,
            "attachments": safe_list(inner.get('attachments') or body.get('attachments')),
            "is_forward": True
        }
    
    body = msg.get('body', {})
    markup, _ = find_markup_in_message(msg)
    
    if isinstance(body, dict):
        return {
            "source": "body",
            "mid": body.get('mid'),
            "seq": body.get('seq'),
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
    except:
        pass
    finally:
        os.unlink(tmp_path)
    
    if duration == 0:
        duration = get_audio_duration(tmp_path) if os.path.exists(tmp_path) else 0
    
    # Fallback: имя файла
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
        
        result = subprocess.run(cmd, capture_output=True, timeout=60)
        
        if result.returncode != 0:
            os.unlink(tmp_in_path)
            return None
        
        with open(tmp_out_path, 'rb') as f:
            ogg_data = f.read()
        
        os.unlink(tmp_in_path)
        os.unlink(tmp_out_path)
        
        logger.info(f"[VOICE] Converted: {len(file_data)} -> {len(ogg_data)} bytes")
        return ogg_data
    except:
        return None


# ===================================================================
# 7. MEDIA PROCESSOR
# ===================================================================
class MediaProcessor:
    PHOTO_EXTS = {'jpg', 'jpeg', 'png', 'gif', 'webp', 'bmp'}
    VIDEO_EXTS = {'mp4', 'mov', 'avi', 'mkv', 'webm'}
    AUDIO_EXTS = {'mp3', 'wav', 'm4a', 'flac', 'aac', 'ogg', 'opus'}
    
    def __init__(self):
        self.ffmpeg_ok = self._check_ffmpeg()
        logger.info(f"[MEDIA] FFmpeg: {self.ffmpeg_ok}")
    
    def _check_ffmpeg(self):
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
        token = payload.get('token') or att.get('token')
        ext = fname.split('.')[-1].lower() if '.' in fname else ''
        
        logger.info(f"[MEDIA] atype={atype}, ext={ext}, size={size}")
        
        meta = {'filename': fname, 'size': size, 'url': url, 'token': token, 'ext': ext}
        
        # ЯВНЫЕ ТИПЫ ОТ MAX
        if atype == 'voice':
            return 'voice', meta
        if atype == 'audio':
            return 'audio', meta
        if atype == 'video':
            return 'video', meta
        if atype in ('image', 'photo'):
            return 'photo', meta
        
        # ПО РАСШИРЕНИЮ
        if ext in self.PHOTO_EXTS:
            return 'photo', meta
        if ext in self.VIDEO_EXTS:
            return 'video', meta
        if ext in self.AUDIO_EXTS:
            return 'audio', meta
        
        return 'document', meta


# ===================================================================
# 8. TELEGRAM CLIENT
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
        logger.info(f"[TG] {method}")
        
        try:
            async with self.session.post(f"{self.base}/{method}", **kw) as r:
                txt = await r.text()
                
                if LOG_RAW_TG:
                    logger.info(f"[TG-RESP] {txt[:500]}")
                
                try:
                    resp = json.loads(txt)
                except:
                    return None
                
                if r.status == 200 and resp.get('ok'):
                    logger.info(f"[TG] ✅ message_id={resp.get('result', {}).get('message_id')}")
                    return resp
                elif r.status == 429:
                    wait = resp.get('parameters', {}).get('retry_after', 10)
                    logger.warning(f"[TG] Rate limit, wait {wait}s")
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
        resp = await self.send('sendMessage', json={
            'chat_id': self.chat_id, 'text': text, 'parse_mode': 'HTML'
        })
        return resp and resp.get('ok', False)
    
    async def media(self, type_: str, data: Union[str, bytes],
                    caption: str = "", filename: str = "", **extra) -> bool:
        
        method_map = {'photo': 'sendPhoto', 'video': 'sendVideo',
                      'audio': 'sendAudio', 'voice': 'sendVoice',
                      'document': 'sendDocument'}
        method = method_map.get(type_, 'sendDocument')
        field = type_ if type_ != 'document' else 'document'
        
        form = aiohttp.FormData()
        form.add_field('chat_id', self.chat_id)
        
        if isinstance(data, str):
            form.add_field(field, data)
        else:
            fname = filename or f"{type_}.file"
            form.add_field(field, data, filename=fname)
        
        if caption and type_ != 'document':
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
# 9. MAX CLIENT
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
    
    async def fetch(self, limit: int = 50) -> List[Dict]:
        await self.init()
        try:
            async with self.session.get(
                f"{self.base}/messages",
                headers={'Authorization': self.token},
                params={'chat_id': self.cid, 'limit': limit}
            ) as r:
                if r.status == 200:
                    raw = await r.json()
                    msgs = raw.get('messages', [])
                    logger.info(f"[MAX] Got {len(msgs)} messages")
                    return msgs
                return []
        except:
            return []
    
    async def download(self, token: str) -> Optional[bytes]:
        await self.init()
        try:
            async with self.session.get(
                f"{self.base}/files/{token}/download",
                headers={'Authorization': self.token}
            ) as r:
                if r.status == 200:
                    return await r.read()
                return None
        except:
            return None


# ===================================================================
# 10. ОБРАБОТЧИК
# ===================================================================
tg = TG(TG_TOKEN, TG_CHAT)
mx = MX(MAX_TOKEN, MAX_CHAN, MAX_BASE)
media_proc = MediaProcessor()


def extract_seq(msg: Dict) -> int:
    body = msg.get('body', {})
    return int(body.get('seq', 0) or msg.get('seq', 0))


async def process_attachment(att: Dict, caption: str = "") -> bool:
    tg_type, meta = media_proc.determine(att)
    logger.info(f"[ATT] Type: {tg_type}")
    
    file_data = None
    is_url = False
    
    # Документы и аудио ВСЕГДА скачиваем через токен
    if tg_type in ('document', 'audio', 'voice') and meta.get('token'):
        file_data = await mx.download(meta['token'])
        if not file_data:
            return False
    elif meta.get('url'):
        # URL только для фото/видео
        return await tg.media(tg_type, meta['url'], caption=caption, is_url=True)
    elif meta.get('token'):
        file_data = await mx.download(meta['token'])
        if not file_data:
            return False
    else:
        return False
    
    extra = {}
    
    # Конвертация mp3 → голосовое (если маленький файл)
    if tg_type == 'audio' and meta['size'] < 2 * 1024 * 1024 and media_proc.ffmpeg_ok:
        logger.info(f"[ATT] Trying voice conversion...")
        voice_data = convert_to_voice(file_data)
        if voice_data:
            tg_type = 'voice'
            file_data = voice_data
            # Получаем длительность
            with tempfile.NamedTemporaryFile(suffix='.ogg', delete=False) as tmp:
                tmp.write(voice_data)
                tmp_path = tmp.name
            extra['duration'] = get_audio_duration(tmp_path)
            os.unlink(tmp_path)
    
    # Теги для аудио
    if tg_type == 'audio':
        extra.update(extract_audio_tags(file_data, meta['filename']))
    
    return await tg.media(tg_type, file_data, caption=caption,
                          filename=meta['filename'], **extra)


async def handle_message(msg: Dict):
    global _last_processed_seq
    
    logger.info("=" * 60)
    
    seq = extract_seq(msg)
    logger.info(f"[HANDLE] seq={seq}, last={_last_processed_seq}")
    
    if seq and seq <= _last_processed_seq and not DEBUG_IGNORE_SEQ:
        logger.info("[HANDLE] ⏭ Skipping (duplicate)")
        return
    
    data = extract_data(msg)
    logger.info(f"[HANDLE] text_len={len(data['text'])}, att={len(data['attachments'])}")
    
    text = data['text']
    if data['markup']:
        text = apply_markup(text, data['markup'])
    
    # НЕ добавляем префикс "Переслано"
    
    if text and text.strip():
        await tg.text(text)
        await asyncio.sleep(0.3)
    
    for i, att in enumerate(data['attachments']):
        caption = text if i == 0 and not text else ""
        await process_attachment(att, caption)
        await asyncio.sleep(0.5)
    
    if seq:
        _last_processed_seq = seq
        logger.info(f"[HANDLE] Updated last_seq={seq}")
    
    logger.info("=" * 60)


# ===================================================================
# 11. POLLING
# ===================================================================
async def polling_loop():
    global _last_processed_seq
    logger.info("🔄 Polling started")
    await asyncio.sleep(2)
    
    while True:
        try:
            msgs = await mx.fetch(limit=10)
            if not msgs:
                await asyncio.sleep(POLL_SEC)
                continue
            
            # Сортируем и обрабатываем только новые
            msgs.sort(key=extract_seq)
            
            for msg in msgs:
                seq = extract_seq(msg)
                if seq > _last_processed_seq or DEBUG_IGNORE_SEQ:
                    await handle_message(msg)
                    await asyncio.sleep(2.0)  # Защита от rate limit
                else:
                    logger.debug(f"[POLL] Skip seq={seq}")
            
        except Exception as e:
            logger.error(f"[POLL] {e}")
        
        await asyncio.sleep(POLL_SEC)


# ===================================================================
# 12. WEB SERVER
# ===================================================================
async def health(request):
    return web.json_response({'ok': True, 'last_seq': _last_processed_seq})


async def run():
    app = web.Application()
    app.router.add_get('/health', health)
    
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, '0.0.0.0', 8080).start()
    
    logger.info("🌐 Health server on :8080")
    await polling_loop()


if __name__ == '__main__':
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("🛑 Stopped")
    except Exception as e:
        logger.exception(f"💥 FATAL: {e}")
        sys.exit(1)
