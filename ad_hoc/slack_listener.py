import json
import logging
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import requests

from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

BOT_TOKEN      = os.getenv("SLACK_BOT_TOKEN")
APP_TOKEN      = os.getenv("SLACK_APP_TOKEN")
TARGET_CHANNEL = os.getenv("SLACK_TARGET_CHANNEL")
TARGET_USER    = os.getenv("SLACK_TARGET_USER")

DOWNLOAD_DIR = Path.home() / "Downloads"
PY_SCRIPT    = PROJECT_ROOT / "weekly_processing" / "run_all.py"
STATE_FILE   = PROJECT_ROOT / "processed.json"

now = datetime.now()
monday_date = (now - timedelta(days=now.weekday())).date()
monday_iso  = monday_date.strftime("%Y-%m-%d")
monday_dmy  = monday_date.strftime("d_%m_%Y")

# Expected file name regexes (strict)
RE_DATASET = re.compile(
    rf"^dataset_facebook-groups-scraper_{re.escape(monday_iso)}_\d{{2}}-\d{{2}}-\d{{2}}-\d{{3}}\.xlsx$"
)
RE_FULL = re.compile(
    rf"^full_data-{re.escape(monday_dmy)}\.xlsx$"
)

DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
processed = set()
try:
    processed = set(json.loads(STATE_FILE.read_text()))
except (FileNotFoundError, json.JSONDecodeError):
    processed = set()


def save_state():
    STATE_FILE.write_text(json.dumps(sorted(processed)))


def _download_file(file_obj, out_dir: Path, auth_headers: dict) -> str:
    url = file_obj.get("url_private_download") or file_obj.get("url_private")
    name = file_obj.get("name") or file_obj.get("id", "file.xlsx")
    r = requests.get(url, headers=auth_headers, timeout=120)
    r.raise_for_status()
    out_path = out_dir / name
    out_path.write_bytes(r.content)
    return str(out_path)


def notify(title, text):
    subprocess.run(
        ["osascript", "-e", f'display notification "{text}" with title "{title}"'],
        check=False,
    )


app = App(token=BOT_TOKEN)
_auth_headers = {"Authorization": f"Bearer {BOT_TOKEN}"}


@app.event("message")
def handle_message_events(body, event, logger):  # body is required by slack_bolt's signature inspection
    try:
        if event.get("subtype") == "bot_message":
            return
        if event.get("channel") != TARGET_CHANNEL:
            return
        if event.get("user") != TARGET_USER:
            logger.info("Message not from target user")
            return

        ts = event.get("ts")
        if not ts or ts in processed:
            return

        files = event.get("files", [])
        if len(files) != 2:
            logger.info(f"Ignoring ts={ts}: expected 2 files, got {len(files)}")
            return

        names = [(f.get("name") or "") for f in files]
        matches = {
            "dataset": [i for i, n in enumerate(names) if RE_DATASET.match(n)],
            "full":    [i for i, n in enumerate(names) if RE_FULL.match(n)],
        }
        if len(matches["dataset"]) != 1 or len(matches["full"]) != 1:
            logger.info(
                f"Skip ts={ts}: names don't match Monday patterns "
                f"(ISO={monday_iso}, DMY={monday_dmy}) -> {names}"
            )
            return

        notify("Tagging Automation", "Detected Ford files on Slack — downloading and running pipeline...")

        downloaded_dataset = _download_file(files[matches["dataset"][0]], DOWNLOAD_DIR, _auth_headers)
        downloaded_full    = _download_file(files[matches["full"][0]],    DOWNLOAD_DIR, _auth_headers)
        logger.info(f"Downloaded:\n  {downloaded_dataset}\n  {downloaded_full}")

        cmd = [sys.executable, str(PY_SCRIPT)]
        logger.info(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)
        logger.info(
            f"Pipeline finished (exit={result.returncode})\n"
            f"STDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )

        if result.returncode == 0:
            notify("Tagging Automation", "Pipeline complete!")
        else:
            notify("Tagging Automation", f"Pipeline failed (exit {result.returncode}) — check logs.")

        processed.add(ts)
        save_state()

        logger.info("Done for this week — exiting listener.")
        logging.shutdown()
        os._exit(0)

    except Exception:
        logger.exception("Handler error")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info(
        f"Listening — expecting files for Monday {monday_iso} / {monday_dmy} "
        f"in channel {TARGET_CHANNEL} from user {TARGET_USER}"
    )
    SocketModeHandler(app, APP_TOKEN).start()
