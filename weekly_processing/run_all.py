from pathlib import Path
import subprocess, sys

ROOT = Path(__file__).resolve().parent
files = ["move_files.py", "get_ownership_database.py", "add_facebook_names.py", "google_sheet_editing.py"]

for f in files:
    print(f"▶ Running {f}")
    subprocess.run([sys.executable, str(ROOT / f)], check=True)
print("All done! Hooray!")
