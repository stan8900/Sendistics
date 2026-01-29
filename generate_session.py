import getpass

from telethon.sync import TelegramClient
from telethon.sessions import StringSession


def main() -> None:
    print("Telethon session generator")
    api_id_raw = input("API ID: ").strip()
    if not api_id_raw.isdigit():
        raise ValueError("API ID must be digits.")
    api_id = int(api_id_raw)
    api_hash = getpass.getpass("API Hash: ").strip()
    if not api_hash:
        raise ValueError("API hash is required.")
    with TelegramClient(StringSession(), api_id, api_hash) as client:
        print("\nTG_USER_SESSION =", client.session.save())


if __name__ == "__main__":
    main()
