from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 38716906
api_hash = "792fef6147d502f29e5c8996d1a68a42"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("TG_USER_SESSION=", client.session.save(), sep="")
