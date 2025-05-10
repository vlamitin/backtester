import os
import time
from collections import defaultdict, deque
from threading import Lock

import requests
from dotenv import load_dotenv

load_dotenv()

TG_BOT_TOKEN = os.environ["TG_BOT_TOKEN"]
SESSIONS_STAT_CHANNEL_ID = os.environ["SESSIONS_STAT_CHANNEL_ID"]
SESSIONS_SIGNALS_CHANNEL_ID = os.environ["SESSIONS_SIGNALS_CHANNEL_ID"]

TG_URL = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"


class TelegramThrottler:
    _instance = None
    _lock = Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
                cls._instance._init()
            return cls._instance

    def _init(self):
        self.message_history = defaultdict(lambda: deque(maxlen=20))
        self.last_message_time = defaultdict(lambda: 0)
        self.chat_locks = defaultdict(Lock)

    def send_signal_message(self, text: str):
        with self.chat_locks[SESSIONS_SIGNALS_CHANNEL_ID]:
            now = time.time()

            # Enforce 1 message per second per chat
            time_since_last = now - self.last_message_time[SESSIONS_SIGNALS_CHANNEL_ID]
            if time_since_last < 1:
                time.sleep(1 - time_since_last)

            # Enforce 20 messages per 60 seconds
            history = self.message_history[SESSIONS_SIGNALS_CHANNEL_ID]
            if len(history) == 20 and now - history[0] < 60:
                sleep_time = 60 - (now - history[0])
                time.sleep(sleep_time)

            # Send the request
            response = requests.post(TG_URL, json={
                "chat_id": SESSIONS_SIGNALS_CHANNEL_ID,
                "text": text,
                "disable_notification": True
            }, headers={
                "Content-Type": "application/json"
            })

            if response.status_code != 200:
                print(f"Failed to send: {response.status_code} - {response.text}")
            else:
                print(f"Sent to {SESSIONS_SIGNALS_CHANNEL_ID}: {text}")

            # Update state
            now = time.time()
            self.message_history[SESSIONS_SIGNALS_CHANNEL_ID].append(now)
            self.last_message_time[SESSIONS_SIGNALS_CHANNEL_ID] = now


def post_stat_notification(html_formatted_text):
    requests.post(TG_URL, json={
        "chat_id": SESSIONS_STAT_CHANNEL_ID,
        "parse_mode": "HTML",
        "text": html_formatted_text,
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
        for i in range(25):
            TelegramThrottler().send_signal_message(f"Test throttled message {i + 1}/25")

#         post_stat_notification("""Next <b>AVAXUSDT</b> session is <b>Early session</b>, chances for 2024:
#
# Asia Open__BTS -> London Open__REJECTION_BULL -> <b>Early session__INDECISION: 1/1 100.0%</b>
# London Open__REJECTION_BULL -> <b>Early session__INDECISION: 4/15 26.67%</b>
# London Open__REJECTION_BULL -> <b>Early session__COMPRESSION: 3/15 20.0%</b>
# London Open__REJECTION_BULL -> <b>Early session__REJECTION_BULL: 3/15 20.0%</b> (LOW_TO_BODY_REVERSAL 1/15 6.67%)
# London Open__REJECTION_BULL -> <b>Early session__DOJI: 3/15 20.0%</b>
# London Open__REJECTION_BULL -> <b>Early session__BEAR: 1/15 6.67%</b>
# London Open__REJECTION_BULL -> <b>Early session__BULL: 1/15 6.67%</b>
# <b>Early session__COMPRESSION: 71/262 27.1%</b> (LOW_TO_BODY_REVERSAL 1/262 0.38%, FORWARD_BODY_BUILDER 1/262 0.38%, HIGH_TO_BODY_REVERSAL 1/262 0.38%)
# <b>Early session__INDECISION: 63/262 24.05%</b> (LOW_WICK_BUILDER 2/262 0.76%, BACKWARD_BODY_BUILDER 2/262 0.76%, FORWARD_BODY_BUILDER 1/262 0.38%, HIGH_TO_BODY_REVERSAL 1/262 0.38%, DAILY_HIGH 1/262 0.38%)
# <b>Early session__BEAR: 45/262 17.18%</b> (HIGH_TO_BODY_REVERSAL 7/262 2.67%, FORWARD_BODY_BUILDER 5/262 1.91%, LOW_WICK_BUILDER 2/262 0.76%, DAILY_LOW 1/262 0.38%, BACKWARD_BODY_BUILDER 1/262 0.38%)
# <b>Early session__BULL: 26/262 9.92%</b> (DAILY_LOW 1/262 0.38%, LOW_TO_BODY_REVERSAL 1/262 0.38%, LOW_WICK_BUILDER 1/262 0.38%)
# <b>Early session__DOJI: 24/262 9.16%</b> (DAILY_HIGH 2/262 0.76%, LOW_TO_BODY_REVERSAL 1/262 0.38%)
# <b>Early session__STB: 6/262 2.29%</b>
# <b>Early session__REJECTION_BEAR: 6/262 2.29%</b> (HIGH_TO_BODY_REVERSAL 1/262 0.38%, BACKWARD_BODY_BUILDER 1/262 0.38%)
# <b>Early session__REJECTION_BULL: 6/262 2.29%</b> (LOW_TO_BODY_REVERSAL 1/262 0.38%, FORWARD_BODY_BUILDER 1/262 0.38%)
# <b>Early session__FLASH_CRASH: 4/262 1.53%</b> (DAILY_LOW 1/262 0.38%, FORWARD_BODY_BUILDER 1/262 0.38%, BACKWARD_BODY_BUILDER 1/262 0.38%)
# <b>Early session__BTS: 4/262 1.53%</b> (DAILY_HIGH 1/262 0.38%, FORWARD_BODY_BUILDER 1/262 0.38%)
# <b>Early session__TO_THE_MOON: 4/262 1.53%</b> (LOW_TO_BODY_REVERSAL 1/262 0.38%, DAILY_LOW 1/262 0.38%)
# <b>Early session__V_SHAPE: 3/262 1.15%</b> (DAILY_LOW 1/262 0.38%)
# """)
        # post_signal_notification("test2")
    except KeyboardInterrupt:
        print(f"KeyboardInterrupt, exiting ...")
        quit(0)
