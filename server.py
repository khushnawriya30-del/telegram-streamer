import os
import re
import mimetypes
import logging
import asyncio
from typing import Dict, Any

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

logging.basicConfig(level=logging.INFO)
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

# Initialize client using permanent pre-authenticated session_string (zero flood wait)
bot = Client(
    "stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    session_string=SESSION_STRING,
    workers=10,
    in_memory=True
)

async def get_file_properties(message: Message) -> Dict[str, Any]:
    media = message.video or message.document or message.audio or message.animation
    if not media:
        return {}
    file_name = getattr(media, "file_name", None) or f"video_{message.id}.mp4"
    file_size = getattr(media, "file_size", 0)
    mime_type = getattr(media, "mime_type", None) or mimetypes.guess_type(file_name)[0] or "video/mp4"
    return {
        "file_name": file_name,
        "file_size": file_size,
        "mime_type": mime_type,
        "media": media
    }

# Web Watch Player Page (Plays MP4, MKV, TS smoothly directly in any browser)
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
    download_url = f"/stream/{message_id}?download=1"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{file_name} - HindiAnime Streamer</title>
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
  <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600;700&display=swap" rel="stylesheet">
  <script src="https://cdn.jsdelivr.net/npm/hls.js@1.5.8/dist/hls.min.js"></script>
  <script src="https://cdn.jsdelivr.net/npm/artplayer/dist/artplayer.js"></script>
  <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; font-family: 'Outfit', sans-serif; }}
    body {{ background: #0b0f19; color: #f1f5f9; min-height: 100vh; display: flex; flex-direction: column; }}
    header {{ padding: 14px 24px; background: #0f172a; border-bottom: 1px solid #1e293b; display: flex; justify-content: space-between; align-items: center; }}
    .logo {{ font-size: 18px; font-weight: 700; color: #ff640a; display: flex; align-items: center; gap: 8px; text-decoration: none; }}
    .player-wrap {{ width: 100%; max-width: 1000px; margin: 24px auto; padding: 0 16px; flex: 1; }}
    .artplayer-app {{ width: 100%; aspect-ratio: 16 / 9; border-radius: 12px; overflow: hidden; background: #000; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }}
    .meta-box {{ margin-top: 16px; background: #0f172a; padding: 16px 20px; border-radius: 10px; border: 1px solid #1e293b; display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }}
    .title-box h1 {{ font-size: 16px; font-weight: 600; color: #f8fafc; margin-bottom: 4px; }}
    .title-box span {{ font-size: 13px; color: #94a3b8; }}
    .btn-group {{ display: flex; gap: 10px; }}
    .btn {{ display: inline-flex; align-items: center; gap: 6px; padding: 8px 16px; border-radius: 6px; font-size: 13px; font-weight: 600; text-decoration: none; cursor: pointer; border: none; transition: 0.2s; }}
    .btn-primary {{ background: #ff640a; color: #fff; }}
    .btn-primary:hover {{ background: #ea580c; }}
    .btn-secondary {{ background: #1e293b; color: #cbd5e1; }}
    .btn-secondary:hover {{ background: #334155; color: #fff; }}
    .toast {{ position: fixed; bottom: 24px; right: 24px; background: #22c55e; color: #fff; padding: 10px 18px; border-radius: 6px; font-size: 13px; display: none; }}
  </style>
</head>
<body>
  <header>
    <a href="https://www.hindianime.site" class="logo" target="_blank">
      <i class="fa-solid fa-play"></i> HindiAnime Web Player
    </a>
    <span style="font-size: 12px; color: #64748b; background: #1e293b; padding: 4px 10px; border-radius: 20px;">24x7 Telegram Stream</span>
  </header>

  <div class="player-wrap">
    <div class="artplayer-app" id="artplayer"></div>
    <div class="meta-box">
      <div class="title-box">
        <h1>{file_name}</h1>
        <span>File Size: <strong>{size_mb} MB</strong> &bull; Direct Cloud Stream</span>
      </div>
      <div class="btn-group">
        <a href="{download_url}" class="btn btn-secondary">
          <i class="fa-solid fa-download"></i> Download
        </a>
        <button onclick="copyStreamLink()" class="btn btn-primary">
          <i class="fa-solid fa-link"></i> Copy Link
        </button>
      </div>
    </div>
  </div>

  <div id="toast" class="toast">Link copied to clipboard!</div>

  <script>
    const art = new Artplayer({{
      container: '#artplayer',
      url: '{stream_url}',
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
    }});

    function copyStreamLink() {{
      const link = window.location.origin + '{stream_url}';
      navigator.clipboard.writeText(link).then(() => {{
        const t = document.getElementById('toast');
        t.style.display = 'block';
        setTimeout(() => t.style.display = 'none', 2500);
      }});
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

    # If browser visits stream URL directly with HTML accept header and no range, redirect to beautiful watch player
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

    # HEAD requests only need headers
    if request.method == "HEAD":
        await response.write_eof()
        return response

    # 1 MiB chunk size for Telegram media streaming
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
        "version": "3.0"
    }, headers={"Access-Control-Allow-Origin": "*"})

# When user starts bot in DM
@bot.on_message(filters.private & filters.command("start"))
async def on_start(client: Client, message: Message):
    await message.reply_text(
        "👋 **Namaste! HindiAnime Streamer Bot me aapka swagat hai.**\n\n"
        "⚡ Mujhe koi bhi Anime Video (MP4/MKV) send ya forward kijiye.\n"
        "Main turant aapko **Watch Online Page** aur **Direct High-Speed Stream Link** de dunga!"
    )

# When user sends or forwards video to bot in private DM
@bot.on_message(filters.private & (filters.video | filters.document | filters.audio | filters.animation))
async def on_private_media(client: Client, message: Message):
    status_msg = await message.reply_text("⏳ *Generating Fast Stream Link...*", quote=True)
    try:
        forwarded = await message.forward(BIN_CHANNEL)
        stream_url = f"{FQDN}/stream/{forwarded.id}"
        watch_url = f"{FQDN}/watch/{forwarded.id}"
        file_props = await get_file_properties(message)
        file_name = file_props.get("file_name", "Anime Video")
        size_mb = round(file_props.get("file_size", 0) / (1024 * 1024), 2)

        caption = (
            f"🎬 **File:** `{file_name}`\n"
            f"📦 **Size:** `{size_mb} MB`\n\n"
            f"📺 **Watch Online (Browser Player):**\n`{watch_url}`\n\n"
            f"🔗 **Direct Stream/Embed Link:**\n`{stream_url}`\n\n"
            f"👉 Copy this link for your HindiAnime website player!"
        )
        await status_msg.edit_text(caption, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Private media error: {e}")
        await status_msg.edit_text(f"❌ Error: {e}")

async def on_startup(app):
    await bot.start()
    logger.info(f"Telegram Bot started as @{bot.me.username}")

async def on_cleanup(app):
    await bot.stop()
    logger.info("Telegram Bot stopped")

def create_app():
    app = web.Application()
    app.on_startup.append(on_startup)
    app.on_cleanup.append(on_cleanup)
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.router.add_get("/watch/{message_id}", watch_handler)
    app.router.add_route("*", "/stream/{message_id}", stream_handler)
    return app

if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
