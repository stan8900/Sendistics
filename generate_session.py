from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 34959777
api_hash = "43c6b5c50ce2d7851112955024af1007"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("TG_USER_SESSION=", client.session.save(), sep="")
