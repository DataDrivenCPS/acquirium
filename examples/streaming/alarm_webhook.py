"""Post to a webhook whenever the high-mean alarm is raised or cleared.

Deploy the apps from docs/how-to/write-soft-sensors.md, then:

    WEBHOOK=https://example.com/hook uv run --locked python examples/streaming/alarm_webhook.py

It saves its position in alarm-cursor.json and resumes from there. On its first
run it reads through the history without posting, so nobody is paged for alarms
that are long over.
"""

import json
import os
import time
from pathlib import Path

import requests

import acquirium as aq
from acquirium.core import END_OF_EPOCH

WEBHOOK = os.environ["WEBHOOK"]
SAVED = Path("alarm-cursor.json")

client = aq.Acquirium(server_url="127.0.0.1", server_port=8000)
alarm = client.query().measurement(alias="alarm", app="high-mean")
stream = alarm.data().stream_info()["stream_uri"][0]

if SAVED.exists():
    cursor = json.loads(SAVED.read_text())
    catching_up = False
else:
    cursor = {"epoch": 0, "ordinal": 0}
    catching_up = True

waiting = []
saved_through = cursor["epoch"]
while True:
    page = client.changes(**cursor, limit=1000)
    cursor = page["cursor"]
    waiting += page["changes"]

    done = page["complete_through"]
    ready = [change for change in waiting if change["epoch"] <= done]
    waiting = [change for change in waiting if change["epoch"] > done]

    # Collect the alarm's changes, by the epoch and the timestamp they touch.
    touched = {}
    for change in ready:
        key = change["row"]["key"]
        if key["stream"] == stream:
            touched.setdefault((change["epoch"], key["time"]), []).append(change)

    for (epoch, alarm_time), changes in touched.items():
        if catching_up:
            continue
        weights = [change["weight"] for change in changes]
        if weights == [1]:
            event = "raised"
        elif weights == [-1]:
            event = "cleared"
        else:
            continue  # retracted and inserted again: the alarm's value was revised

        requests.post(
            WEBHOOK,
            json={
                "id": f"{epoch}/{alarm_time}",
                "event": event,
                "time": alarm_time,
                "value": changes[-1]["row"]["data"]["value"],
            },
            timeout=10,
        ).raise_for_status()

    if done > saved_through:
        SAVED.write_text(json.dumps({"epoch": done, "ordinal": END_OF_EPOCH}))
        saved_through = done

    if not page["has_more"]:
        catching_up = False
        time.sleep(1)
