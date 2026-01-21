from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 39397868
api_hash = "ca736b2e1556cb9132427ba36c1b6c58"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("TG_USER_SESSION=", client.session.save(), sep="")
