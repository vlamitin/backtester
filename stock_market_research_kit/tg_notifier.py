import os

import requests
from dotenv import load_dotenv

load_dotenv()

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
SESSIONS_STAT_CHANNEL_ID = os.environ["SESSIONS_STAT_CHANNEL_ID"]
SESSIONS_SIGNALS_CHANNEL_ID = os.environ["SESSIONS_SIGNALS_CHANNEL_ID"]

TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"


def post_stat_notification(text):
    requests.post(TG_URL, json={
        "chat_id": SESSIONS_STAT_CHANNEL_ID,
        "text": text,
        "disable_notification": True
    }, headers={
        "Content-Type": "application/json"
    })


def post_signal_notification(text):
    requests.post(TG_URL, json={
        "chat_id": SESSIONS_SIGNALS_CHANNEL_ID,
        "text": text,
        "disable_notification": True
    }, headers={
        "Content-Type": "application/json"
    })


if __name__ == "__main__":
    try:
        post_stat_notification("test1")
        post_signal_notification("test2")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
