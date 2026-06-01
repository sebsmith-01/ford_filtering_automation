import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from helper_functions import get_monday_str

GROUPS_FILE = "ford_facebook_groups.xlsx"
MONDAY_STR = get_monday_str()
INPUT_FILE = f"weekly_data/{MONDAY_STR}/added_facebook_names_{MONDAY_STR}.xlsx"
OUTPUT_FILE = INPUT_FILE  # overwrite in place


def build_slug_to_name(groups_file: str) -> dict[str, str]:
    groups = pd.read_excel(groups_file)
    mapping = {}
    for _, row in groups.iterrows():
        url = str(row["facebook_group_urls"]).rstrip("/")
        slug = url.split("/")[-1]
        mapping[slug] = row["group_name"]
    return mapping


def fix_thread_titles(input_file: str, output_file: str, slug_to_name: dict[str, str]) -> None:
    df = pd.read_excel(input_file)

    original = df["thread_title"].copy()
    df["thread_title"] = df["thread_title"].map(
        lambda v: slug_to_name.get(str(v), v) if pd.notna(v) else v
    )

    changed = (df["thread_title"] != original).sum()
    print(f"Replaced {changed} thread_title values")

    df.to_excel(output_file, index=False)
    print(f"Saved to {output_file}")


if __name__ == "__main__":
    slug_to_name = build_slug_to_name(GROUPS_FILE)
    print(f"Loaded {len(slug_to_name)} group mappings")
    fix_thread_titles(INPUT_FILE, OUTPUT_FILE, slug_to_name)
