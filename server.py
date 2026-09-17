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
BOT_TOKEN = os.environ.get("BOT_TOKEN", "8982179760:AAHSjueoPpgQmBJfjIlfRebxbA45m0y595w")
BIN_CHANNEL = int(os.environ.get("BIN_CHANNEL", "-1004457425617"))
PORT = int(os.environ.get("PORT", 10000))
FQDN = os.environ.get("FQDN", "")

bot = Client(
    "stream_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
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

async def stream_handler(request: web.Request) -> web.StreamResponse:
    try:
        message_id = int(request.match_info["message_id"])
    except ValueError:
        return web.Response(status=400, text="Invalid message ID")

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

    range_header = request.headers.get("Range")
    from_bytes = 0
    until_bytes = file_size - 1

    status_code = 200
    if range_header:
        range_match = re.match(r"bytes=(\d+)-(\d*)", range_header)
        if range_match:
            status_code = 206
            from_bytes = int(range_match.group(1))
            if range_match.group(2):
                until_bytes = int(range_match.group(2))

    length = until_bytes - from_bytes + 1

    headers = {
        "Content-Type": mime_type,
        "Content-Disposition": f'inline; filename="{file_name}"',
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

    try:
        async for chunk in bot.stream_media(msg, offset=from_bytes, limit=length):
            await response.write(chunk)
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
        "version": "2.0"
    }, headers={"Access-Control-Allow-Origin": "*"})

@bot.on_message(filters.chat(BIN_CHANNEL) & (filters.video | filters.document))
async def on_channel_video(client: Client, message: Message):
    base_url = FQDN or "https://hindianime-telegram-streamer.onrender.com"
    stream_url = f"{base_url}/stream/{message.id}"
    file_props = await get_file_properties(message)
    file_name = file_props.get("file_name", "Anime Video")
    size_mb = round(file_props.get("file_size", 0) / (1024 * 1024), 2)

    caption = (
        f"🎬 **{file_name}**\n"
        f"📦 Size: `{size_mb} MB`\n\n"
        f"🔗 **Fast Web Stream Link:**\n`{stream_url}`\n\n"
        f"👉 Use this link in HindiAnime website episode player!"
    )
    try:
        await message.reply_text(caption, disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Reply error: {e}")

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
    app.router.add_get("/stream/{message_id}", stream_handler)
    return app

if __name__ == "__main__":
    app = create_app()
    web.run_app(app, host="0.0.0.0", port=PORT)
