
import os, sys, subprocess, json, pathlib, logging, requests, re
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
APP_TOKEN = os.getenv("APP_TOKEN")
TARGET_CHANNEL = os.getenv("TARGET_CHANNEL")
TARGET_USER = os.getenv("TARGET_USER")

# will need editing on another person's machine
DOWNLOAD_DIR     = "/Users/seb.smith/Downloads"
PY_SCRIPT        = "run_all.py"
STATE_FILE       = "./processed.json"

now = datetime.now()
monday_date = (now - timedelta(days=now.weekday())).date()
monday_iso = monday_date.strftime("%Y-%m-%d")
monday_dmy = monday_date.strftime("d_%m_%Y")

# Expected file name regexes (strict)
RE_DATASET = re.compile(
    rf"^dataset_facebook-groups-scraper_{re.escape(monday_iso)}_\d{{2}}-\d{{2}}-\d{{2}}-\d{{3}}\.xlsx$"
)
RE_FULL = re.compile(
    rf"^full_data-{re.escape(monday_dmy)}\.xlsx$"
)

pathlib.Path(DOWNLOAD_DIR).mkdir(parents=True, exist_ok=True)
processed = set()
if pathlib.Path(STATE_FILE).exists():
    try:
        processed = set(json.loads(pathlib.Path(STATE_FILE).read_text()))
    except Exception:
        processed = set()

def save_state():
    pathlib.Path(STATE_FILE).write_text(json.dumps(sorted(processed)))
    
def _download_file(file_obj, out_dir: pathlib.Path) -> str:
    url = file_obj.get("url_private_download") or file_obj.get("url_private")
    name = file_obj.get("name") or file_obj.get("id", "file.xlsx")
    r = requests.get(url, headers=headers, timeout=120)
    r.raise_for_status()
    # Prefix with message ts later to avoid collisions if needed
    out_path = out_dir / name
    out_path.write_bytes(r.content)
    return str(out_path)

def notify(title, text):
    os.system("""
              osascript -e 'display notification "{}" with title "{}"'
              """.format(text, title))

app = App(token=BOT_TOKEN)
headers = {"Authorization": f"Bearer {BOT_TOKEN}"}  # needed for url_private download

@app.event("message")
def handle_message_events(body, event, logger):
    try:
        print("Message received...")
        if event.get("subtype") == "bot_message":
            return
        if event.get("channel") != TARGET_CHANNEL:
            return
        if event.get("user") != TARGET_USER:
            logger.info("Message not from target user")
            return

        # Tracks messages that have been timestamped
        ts = event.get("ts")
        if not ts or ts in processed:
            return

        files = event.get("files", [])
        if len(files) != 2:
            logger.info(f"Ignoring ts={ts}: expected 2 files, got {len(files)}")
            return
        
        names = [(f.get("name") or "") for f in files]
        # Must be one dataset_* and one full_data-* for this week's Monday
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

        notify("Tagging Automation", "Detected Ford Files on Slack, Downloading and triggering filtering script...")
        idx_dataset = matches["dataset"][0]
        idx_full = matches["full"][0]

        downloaded_dataset = _download_file(files[idx_dataset], pathlib.Path(DOWNLOAD_DIR))
        downloaded_full    = _download_file(files[idx_full],    pathlib.Path(DOWNLOAD_DIR))

        logger.info(f"Downloaded:\n- {downloaded_dataset}\n- {downloaded_full}")

        # # Run python script
        # cmd = [sys.executable, PY_SCRIPT]
        # logger.info(f"Running: {' '.join(cmd)}")
        # result = subprocess.run(cmd, capture_output=True, text=True)
        # logger.info(
        #     "Process finished "
        #     f"(exit={result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        # )

        # # Mark processed (avoid dupes if you start it again before exiting)
        # processed.add(ts)
        # save_state()

        # Exit immediately after the run
        logger.info("Done for this week — exiting listener now.")
        logging.shutdown()
        os._exit(0)
    except Exception:
        logger.exception("Handler error")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.info(
        f"Expecting file names for Monday {monday_iso} / {monday_dmy} in {TARGET_CHANNEL} from {TARGET_USER}"
    )
    SocketModeHandler(app, APP_TOKEN).start()