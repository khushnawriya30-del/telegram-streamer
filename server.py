import os
import re
import math
import shutil
import mimetypes
import logging
import asyncio
import tempfile
from typing import Dict, Any, Optional

# Ensure an asyncio event loop exists on Python 3.11/3.12/3.13+ before importing/initializing clients
try:
    loop = asyncio.get_event_loop()
except RuntimeError:
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

from aiohttp import web
from hydrogram import Client, filters
from hydrogram.types import Message
import aiohttp
import aiofiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("telegram-streamer")

API_ID = int(os.environ.get("API_ID", "32313888"))
API_HASH = os.environ.get("API_HASH", "a2ca24e548f99aedd831a9f6072a57f4")
SESSION_STRING = os.environ.get(
    "SESSION_STRING",
    "BQHtEiAArqzRUo0FLR_kwvuppvQut35s6hquS9v6Xa9TZ2Rap7YyIKkcKJ29kR-YbSW16krPUdlbMbz7E45s-HrAqDsgyxpWZEBrNOsNcJpZUAtNOFEoEIJzP69O9he392ZhxGCuWBL7lxbgukPReoidPcnE2kibR8cAdapdmhYDLoixD_iOBgsFsoH4ft2OjDleDT_s5LHu5oEfkBuuGCtRiPWp20RL8hY8m_tka7HbWZkkfQujcraMxed33bhz-belOblCamOJ5q5wLClmGMXeANw1QlZ_GLwRFWqYbst_6ONUbEdJP8sC0zYrIjyMOV5J6OfBOMX9Hsga47hsGowngU26wwAAAAIJTGGIAQ"
)
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1004457425617"))
PORT = int(os.environ.get("PORT", 10000))
FQDN = os.environ.get("FQDN", "https://hindianime-telegram-streamer.onrender.com")

bot = Client(
    "stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=10,
    in_memory=True
)

DURATION_CACHE: Dict[int, float] = {}
SEG_DURATION: float = 6.0
PREFETCH_SEMAPHORE = asyncio.Semaphore(3)

async def get_file_properties(message: Message) -> Dict[str, Any]:
    media = message.video or message.document or message.audio or message.animation
    if not media:
        return {}
    file_name = getattr(media, "file_name", None) or f"video_{message.id}.mp4"
    file_size = getattr(media, "file_size", 0)
    duration = getattr(media, "duration", 0) or 0
    mime_type = getattr(media, "mime_type", None) or mimetypes.guess_type(file_name)[0] or "video/mp4"
    return {
        "file_name": file_name,
        "file_size": file_size,
        "duration": float(duration),
        "mime_type": mime_type,
        "media": media
    }

async def get_video_duration(message_id: int, file_props: Dict[str, Any]) -> float:
    if message_id in DURATION_CACHE and DURATION_CACHE[message_id] > 0:
        return DURATION_CACHE[message_id]

    dur = file_props.get("duration", 0.0)
    if dur and dur > 0:
        DURATION_CACHE[message_id] = float(dur)
        return float(dur)

    # Probe duration via ffprobe using local stream URL
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1",
            f"http://127.0.0.1:{PORT}/stream/{message_id}"
        ]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=8.0)
        out_str = stdout.decode().strip()
        if out_str:
            probed_dur = float(out_str)
            if probed_dur > 0:
                DURATION_CACHE[message_id] = probed_dur
                logger.info(f"Probed duration for msg {message_id}: {probed_dur}s")
                return probed_dur
    except Exception as e:
        logger.warning(f"ffprobe duration probe error: {e}")

    # Fallback estimate based on file size (~1.5 Mbps anime stream)
    file_size = file_props.get("file_size", 0)
    est_dur = max(60.0, float(file_size) / (180 * 1024))
    DURATION_CACHE[message_id] = est_dur
    return est_dur

def get_hls_dir(message_id: int) -> str:
    base_tmp = os.path.join(tempfile.gettempdir(), "hls_cache")
    path = os.path.join(base_tmp, str(message_id))
    os.makedirs(path, exist_ok=True)
    return path

async def generate_segment(message_id: int, index: int) -> Optional[str]:
    hls_dir = get_hls_dir(message_id)
    seg_path = os.path.join(hls_dir, f"seg_{index}.ts")
    if os.path.exists(seg_path) and os.path.getsize(seg_path) > 1000:
        return seg_path

    start_time = index * SEG_DURATION
    stream_source = f"http://127.0.0.1:{PORT}/stream/{message_id}"
    tmp_out = f"{seg_path}.tmp_{os.getpid()}_{index}.ts"

    cmd = [
        "ffmpeg",
        "-ss", f"{start_time:.3f}",
        "-t", f"{SEG_DURATION:.3f}",
        "-i", stream_source,
        "-c", "copy",
        "-f", "mpegts",
        "-avoid_negative_ts", "1",
        "-v", "error",
        "-y",
        tmp_out
    ]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        _, stderr = await asyncio.wait_for(proc.communicate(), timeout=12.0)
        if proc.returncode == 0 and os.path.exists(tmp_out) and os.path.getsize(tmp_out) > 0:
            os.replace(tmp_out, seg_path)
            return seg_path
        else:
            logger.warning(f"ffmpeg seg_{index} failed: {stderr.decode()[:200]}")
    except Exception as e:
        logger.warning(f"Error generating segment {index}: {e}")
    finally:
        if os.path.exists(tmp_out):
            try:
                os.remove(tmp_out)
            except Exception:
                pass
    return None

async def prefetch_segment(message_id: int, index: int, total_segs: int):
    if index >= total_segs:
        return
    async with PREFETCH_SEMAPHORE:
        await generate_segment(message_id, index)

# HLS Master VOD Playlist Handler
async def hls_master_handler(request: web.Request) -> web.Response:
    try:
        message_id = int(request.match_info["message_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

    try:
        msg = await bot.get_messages(BIN_CHANNEL, message_id)
        file_props = await get_file_properties(msg)
        if not file_props:
            return web.Response(status=404, text="No video found in this message")
    except Exception as e:
        return web.Response(status=404, text=f"Message error: {e}")

    duration = await get_video_duration(message_id, file_props)
    num_segs = math.ceil(duration / SEG_DURATION)

    lines = [
        "#EXTM3U",
        "#EXT-X-VERSION:3",
        f"#EXT-X-TARGETDURATION:{int(SEG_DURATION + 1)}",
        "#EXT-X-MEDIA-SEQUENCE:0",
        "#EXT-X-PLAYLIST-TYPE:VOD"
    ]

    for i in range(num_segs):
        dur = min(SEG_DURATION, duration - (i * SEG_DURATION))
        if dur <= 0:
            dur = SEG_DURATION
        lines.append(f"#EXTINF:{dur:.3f},")
        lines.append(f"seg_{i}.ts")

    lines.append("#EXT-X-ENDLIST")
    content = "\n".join(lines)

    # Pre-generate first segment in background so playback starts instantly
    asyncio.create_task(generate_segment(message_id, 0))

    return web.Response(
        text=content,
        content_type="application/vnd.apple.mpegurl",
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
            "Cache-Control": "public, max-age=3600"
        }
    )

# HLS Video Segment Handler
async def hls_seg_handler(request: web.Request) -> web.Response:
    try:
        message_id = int(request.match_info["message_id"])
        seg_index = int(request.match_info["index"])
    except ValueError:
        return web.Response(status=400, text="Invalid parameters")

    seg_path = await generate_segment(message_id, seg_index)
    if not seg_path or not os.path.exists(seg_path):
        return web.Response(status=500, text="Segment generation error")

    # Trigger prefetch for next segment
    duration = DURATION_CACHE.get(message_id, 3600.0)
    total_segs = math.ceil(duration / SEG_DURATION)
    asyncio.create_task(prefetch_segment(message_id, seg_index + 1, total_segs))

    headers = {
        "Content-Type": "video/mp2t",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Cache-Control": "public, max-age=86400"
    }

    try:
        async with aiofiles.open(seg_path, "rb") as f:
            data = await f.read()
            return web.Response(body=data, headers=headers)
    except Exception as e:
        logger.error(f"Error reading seg_{seg_index}.ts: {e}")
        return web.Response(status=500, text="Segment read error")

# Web Watch Player Page (High-Performance HLS + Smooth Seekbar)
async def watch_handler(request: web.Request) -> web.Response:
    try:
        message_id = int(request.match_info["message_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

    try:
        msg = await bot.get_messages(BIN_CHANNEL, message_id)
        file_props = await get_file_properties(msg)
        if not file_props:
            return web.Response(status=404, text="No video found in this message")
        file_name = file_props.get("file_name", "Anime Video")
        size_mb = round(file_props.get("file_size", 0) / (1024 * 1024), 2)
    except Exception as e:
        return web.Response(status=404, text=f"Message not found: {e}")

    stream_url = f"/stream/{message_id}"
    hls_url = f"/hls/{message_id}/master.m3u8"
    download_url = f"/stream/{message_id}?download=1"

    html = f"""<!DOCTYPE html>
<html lang="hi">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{file_name} - HindiAnime Player</title>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.8/dist/hls.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: 'Outfit', sans-serif; }}
    body {{ background: #0b0f19; color: #f1f5f9; min-height: 100vh; display: flex; flex-direction: column; }}
    header {{ padding: 14px 24px; background: #0f172a; border-bottom: 1px solid #1e293b; display: flex; justify-content: space-between; align-items: center; }}
    .logo {{ font-size: 18px; font-weight: 700; color: #ff640a; display: flex; align-items: center; gap: 8px; text-decoration: none; }}
    .player-wrap {{ width: 100%; max-width: 1050px; margin: 24px auto; padding: 0 16px; flex: 1; }}
    .artplayer-app {{ width: 100%; aspect-ratio: 16 / 9; border-radius: 12px; overflow: hidden; background: #000; box-shadow: 0 12px 35px rgba(0,0,0,0.6); }}
    .meta-box {{ margin-top: 16px; background: #0f172a; padding: 18px 22px; border-radius: 10px; border: 1px solid #1e293b; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 14px; }}
    .title-box h1 {{ font-size: 17px; font-weight: 600; color: #f8fafc; margin-bottom: 5px; }}
    .title-box span {{ font-size: 13px; color: #94a3b8; }}
    .badge-stream {{ display: inline-flex; align-items: center; gap: 6px; background: #166534; color: #4ade80; font-size: 12px; padding: 3px 10px; border-radius: 12px; font-weight: 600; }}
    .btn-group {{ display: flex; gap: 10px; flex-wrap: wrap; }}
    .btn {{ display: inline-flex; align-items: center; gap: 6px; padding: 9px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; text-decoration: none; cursor: pointer; border: none; transition: 0.2s; }}
    .btn-primary {{ background: #ff640a; color: #fff; }}
    .btn-primary:hover {{ background: #ea580c; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; }}
    .btn-secondary:hover {{ background: #334155; color: #fff; }}
    .toast {{ position: fixed; bottom: 24px; right: 24px; background: #22c55e; color: #fff; padding: 12px 20px; border-radius: 8px; font-size: 13px; display: none; z-index: 999; box-shadow: 0 4px 15px rgba(0,0,0,0.4); }}
  </style>
</head>
<body>
  <header>
    <a href="https://www.hindianime.site" class="logo" target="_blank">
      <i class="fa-solid fa-play"></i> HindiAnime Web Player
    </a>
    <span class="badge-stream"><i class="fa-solid fa-bolt"></i> Smooth Seek Active</span>
  </header>

  <div class="player-wrap">
    <div class="artplayer-app" id="artplayer"></div>
    <div class="meta-box">
      <div class="title-box">
        <h1>{file_name}</h1>
        <span>File Size: <strong>{size_mb} MB</strong> &bull; Zero Buffering &bull; Full Seek Support</span>
      </div>
      <div class="btn-group">
        <button onclick="togglePlayerMode()" id="modeToggleBtn" class="btn btn-secondary" title="Toggle between HLS Seek Mode and Direct Stream">
          <i class="fa-solid fa-arrows-rotate"></i> <span id="modeText">Direct MP4 Mode</span>
        </button>
        <a href="{download_url}" class="btn btn-secondary">
          <i class="fa-solid fa-download"></i> Download
        </a>
        <button onclick="copyEmbedCode()" class="btn btn-secondary" title="Copy iframe embed code for website">
          <i class="fa-solid fa-code"></i> Embed
        </button>
        <button onclick="copyStreamLink()" class="btn btn-primary">
          <i class="fa-solid fa-link"></i> Copy Link
        </button>
      </div>
    </div>
  </div>

  <div id="toast" class="toast">Link copied to clipboard!</div>

  <script>
    let currentMode = 'hls';
    const hlsUrl = '{hls_url}';
    const directUrl = '{stream_url}';

    const art = new Artplayer({{
      container: '#artplayer',
      url: hlsUrl,
      type: 'm3u8',
      title: '{file_name}',
      volume: 0.8,
      autoplay: true,
      pip: true,
      screenshot: true,
      setting: true,
      playbackRate: true,
      aspectRatio: true,
      hotkey: true,
      theme: '#ff640a',
      fullscreen: true,
      fullscreenWeb: true,
      fastSeek: true,
      customType: {{
        m3u8: function(video, url, player) {{
          if (Hls.isSupported()) {{
            if (player.hls) player.hls.destroy();
            const hls = new Hls({{
              enableWorker: true,
              lowLatencyMode: false,
              backBufferLength: 90,
              maxBufferLength: 30
            }});
            hls.loadSource(url);
            hls.attachMedia(video);
            player.hls = hls;
            player.on('destroy', () => hls.destroy());
          }} else if (video.canPlayType('application/vnd.apple.mpegurl')) {{
            video.src = url;
          }} else {{
            player.notice.show = 'Browser does not support HLS stream';
          }}
        }}
      }}
    }});
    window.art = art;

    function togglePlayerMode() {{
      if (currentMode === 'hls') {{
        currentMode = 'direct';
        if (window.art.hls) {{
          window.art.hls.destroy();
          window.art.hls = null;
        }}
        window.art.switchUrl(directUrl);
        document.getElementById('modeText').innerText = 'Smooth HLS Mode';
        showToast('Switched to Direct MP4 Mode');
      }} else {{
        currentMode = 'hls';
        window.art.switchUrl(hlsUrl);
        document.getElementById('modeText').innerText = 'Direct MP4 Mode';
        showToast('Switched to Smooth HLS Mode');
      }}
    }}

    function copyStreamLink() {{
      const link = window.location.origin + directUrl;
      navigator.clipboard.writeText(link).then(() => {{
        showToast('Direct Stream Link copied!');
      }});
    }}

    function copyEmbedCode() {{
      const embed = '<iframe src="' + window.location.href + '" width="100%" height="100%" frameborder="0" allowfullscreen></iframe>';
      navigator.clipboard.writeText(embed).then(() => {{
        showToast('Embed Code copied! Paste on website');
      }});
    }}

    function showToast(text) {{
      const t = document.getElementById('toast');
      t.innerText = text;
      t.style.display = 'block';
      setTimeout(() => t.style.display = 'none', 2500);
    }}
  </script>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html")

async def stream_handler(request: web.Request) -> web.StreamResponse:
    try:
        message_id = int(request.match_info["message_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

    accept_header = request.headers.get("Accept", "")
    range_header = request.headers.get("Range")
    is_download = request.query.get("download") == "1"

    if "text/html" in accept_header and not range_header and not is_download:
        raise web.HTTPFound(f"/watch/{message_id}")

    try:
        msg = await bot.get_messages(BIN_CHANNEL, message_id)
    except Exception as e:
        logger.error(f"Message retrieval error: {e}")
        return web.Response(status=404, text=f"Message not found: {e}")

    file_props = await get_file_properties(msg)
    if not file_props:
        return web.Response(status=404, text="No streamable media found in this message")

    file_size = file_props["file_size"]
    file_name = file_props["file_name"]
    mime_type = file_props["mime_type"]

    from_bytes = 0
    until_bytes = file_size - 1

    status_code = 200
    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            status_code = 206
            from_bytes = int(range_match.group(1))
            if range_match.group(2):
                until_bytes = min(int(range_match.group(2)), file_size - 1)

    length = until_bytes - from_bytes + 1

    disposition_type = "attachment" if is_download else "inline"
    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f'{disposition_type}; filename="{file_name}"',
        "Accept-Ranges": "bytes",
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
        "Access-Control-Allow-Headers": "Range, Content-Type",
        "Content-Length": str(length),
    }

    if status_code == 206:
        headers["Content-Range"] = f"bytes {from_bytes}-{until_bytes}/{file_size}"

    response = web.StreamResponse(status=status_code, headers=headers)
    await response.prepare(request)

    if request.method == "HEAD":
        await response.write_eof()
        return response

    part_size = 1024 * 1024
    first_part = from_bytes // part_size
    last_part = until_bytes // part_size
    offset_chunks = first_part
    limit_chunks = (last_part - first_part) + 1

    bytes_to_skip = from_bytes % part_size
    remaining_bytes = length

    try:
        async for chunk in bot.stream_media(msg, offset=offset_chunks, limit=limit_chunks):
            if remaining_bytes <= 0:
                break
            if bytes_to_skip > 0:
                if len(chunk) <= bytes_to_skip:
                    bytes_to_skip -= len(chunk)
                    continue
                chunk = chunk[bytes_to_skip:]
                bytes_to_skip = 0

            if len(chunk) > remaining_bytes:
                chunk = chunk[:remaining_bytes]

            await response.write(chunk)
            await response.drain()
            remaining_bytes -= len(chunk)
    except (ConnectionResetError, aiohttp.ClientConnectionResetError):
        pass
    except Exception as e:
        logger.error(f"Streaming write error: {e}")

    await response.write_eof()
    return response

async def health_handler(request: web.Request) -> web.Response:
    return web.json_response({
        "status": "online",
        "service": "HindiAnime Telegram Streamer",
        "channel": BIN_CHANNEL,
        "ffmpeg": shutil.which("ffmpeg"),
        "version": "4.0"
    }, headers={"Access-Control-Allow-Origin": "*"})

# Periodic background cleaner for old HLS temporary segment files
async def periodic_cache_cleaner():
    while True:
        try:
            await asyncio.sleep(1800)  # Clean every 30 minutes
            base_tmp = os.path.join(tempfile.gettempdir(), "hls_cache")
            if os.path.exists(base_tmp):
                for root, dirs, files in os.walk(base_tmp):
                    for f in files:
                        fp = os.path.join(root, f)
                        if os.path.getmtime(fp) < (loop.time() - 7200):
                            try:
                                os.remove(fp)
                            except Exception:
                                pass
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.warning(f"Cache cleaner error: {e}")

# When user starts bot in DM
@bot.on_message(filters.private & filters.command("start"))
async def on_start(client: Client, message: Message):
    await message.reply_text(
        "👋 **Namaste! HindiAnime Streamer Bot me aapka swagat hai.**\n\n"
        "⚡ Mujhe koi bhi Anime Video (MP4/MKV/TS) send ya forward kijiye.\n"
        "Main turant aapko **Smooth Seek Web Player**, **Direct Stream Link**, aur **Website Embed Code** de dunga!\n\n"
        "✨ Seekbar status bar bina kisi problem ke aage-peeche smoothly chalegi!"
    )

# When user sends or forwards video to bot in private DM
@bot.on_message(filters.private & (filters.video | filters.document | filters.audio | filters.animation))
async def on_private_media(client: Client, message: Message):
    status_msg = await message.reply_text("⏳ *Generating Fast Stream Link...*", quote=True)
    try:
        forwarded = await message.forward(BIN_CHANNEL)
        stream_url = f"{FQDN}/stream/{forwarded.id}"
        watch_url = f"{FQDN}/watch/{forwarded.id}"
        hls_url = f"{FQDN}/hls/{forwarded.id}/master.m3u8"
        file_props = await get_file_properties(message)
        file_name = file_props.get("file_name", "Anime Video")
        size_mb = round(file_props.get("file_size", 0) / (1024 * 1024), 2)

        caption = (
            f"🎬 **File:** `{file_name}`\n"
            f"📦 **Size:** `{size_mb} MB`\n\n"
            f"📺 **Watch Online (Smooth Seekbar Player):**\n`{watch_url}`\n\n"
            f"🔗 **Direct Stream Link:**\n`{stream_url}`\n\n"
            f"📡 **HLS Stream (m3u8):**\n`{hls_url}`\n\n"
            f"📋 **Website Embed Code:**\n"
            f"`<iframe src=\"{watch_url}\" width=\"100%\" height=\"100%\" frameborder=\"0\" allowfullscreen></iframe>`"
        )
        await status_msg.edit_text(caption, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Private media error: {e}")
        await status_msg.edit_text(f"❌ Error: {e}")

cleaner_task = None

async def on_startup(app):
    global cleaner_task
    await bot.start()
    cleaner_task = asyncio.create_task(periodic_cache_cleaner())
    logger.info(f"Telegram Bot started as @{bot.me.username} with HLS engine active")

async def on_cleanup(app):
    global cleaner_task
    if cleaner_task:
        cleaner_task.cancel()
    await bot.stop()
    logger.info("Telegram Bot stopped")

def create_app():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/watch/{message_id}", watch_handler)
    app.router.add_get(r"/hls/{message_id:\d+}/master.m3u8", hls_master_handler)
    app.router.add_get(r"/hls/{message_id:\d+}/seg_{index:\d+}.ts", hls_seg_handler)
    app.router.add_route("*", "/stream/{message_id}", stream_handler)
    return app

if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
