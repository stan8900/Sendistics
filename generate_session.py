from telethon.sync import TelegramClient
from telethon.sessions import StringSession

api_id = 36603621
api_hash = "ef983c005ec07eaca5cc801e1960506b"

with TelegramClient(StringSession(), api_id, api_hash) as client:
    print("TG_USER_SESSION=", client.session.save(), sep="")
