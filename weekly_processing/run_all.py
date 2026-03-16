import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))          # weekly_processing/ — for importing sibling scripts
sys.path.insert(0, str(ROOT.parent))   # project root — for shared modules (auth, helper_functions, etc.)

from move_files import main as run_move_files
from get_ownership_database import main as run_ownership_db
from add_facebook_names import main as run_add_facebook_names
from google_sheet_editing import main as run_google_sheet_editing
from autovalidation import main as run_autovalidation

steps = [
    ("move_files", run_move_files),
    ("get_ownership_database", run_ownership_db),
    ("add_facebook_names", run_add_facebook_names),
]

for name, fn in steps:
    print(f"▶ {name}")
    fn()

print("▶ google_sheet_editing")
spreadsheet_id = run_google_sheet_editing()

print("▶ autovalidation")
run_autovalidation(spreadsheet_id)

print("All done! Hooray!")
