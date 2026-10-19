# ═══════════════════════════════════════════════════════════════════════════
# CELL 1 — Colab setup  (run once per session, before the module cell)
# ═══════════════════════════════════════════════════════════════════════════
import time
import logging

import gspread
from google.colab import auth, drive, userdata
from google.auth import default
from gspread_dataframe import set_with_dataframe

drive.mount('/content/drive', force_remount=False)
auth.authenticate_user()
creds, _ = default()
gc = gspread.authorize(creds)

logging.basicConfig(level=logging.INFO, format='%(levelname)s  %(message)s')
logger = logging.getLogger(__name__)


# ── OpenAI key, from the Colab secrets manager ──────────────────────────────
# Add it once via the key icon in the left sidebar: name OPENAI_API_KEY, then
# toggle "Notebook access" on for this notebook. It is never written into the
# notebook, so it does not travel when the .ipynb is shared or committed.

OPENAI_SECRET_NAME = 'OPENAI_API_KEY'


def load_openai_key(secret_name=OPENAI_SECRET_NAME):
    try:
        key = userdata.get(secret_name)
    except userdata.SecretNotFoundError:
        raise RuntimeError(
            f"Secret '{secret_name}' does not exist. Open the key icon in the left "
            f"sidebar, add a secret named {secret_name} with your OpenAI key, and "
            f"enable Notebook access."
        ) from None
    except userdata.NotebookAccessError:
        raise RuntimeError(
            f"Secret '{secret_name}' exists but this notebook cannot read it. Open the "
            f"key icon in the left sidebar and turn on Notebook access for it."
        ) from None

    key = (key or '').strip()
    if not key:
        raise RuntimeError(f"Secret '{secret_name}' is empty.")
    # Report only the shape, never the value.
    logger.info("OpenAI key loaded from secrets (%d chars, ends %s).", len(key), key[-4:])
    return key


# ==========================================================================
# CELL 2 - module   (autovalidation v2)
# siblings OFF | owner DB ON | EXCLUDE_USED_CAR ON | non-vehicle-cause OFF
# ==========================================================================
import json
import asyncio
import csv
import re
from string import Template

import aiohttp
import nest_asyncio
import logging
import pandas as pd
from tqdm.asyncio import tqdm

logger = logging.getLogger(__name__)

# ──────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ──────────────────────────────────────────────────────────────────────────────

SPREADSHEET_NAME = "1lDJqQGUIQK5XSa8OR4859E33v1tAHkRYtC8dUcP7n8M"
WORKSHEET_NAMES = ["R4"]
FILE_NAME = "/content/drive/My Drive/For report_2026-08-24"
# Model specs are read LIVE from the sheet on every run, so edits there take effect
# without re-exporting anything.
SPECS_SHEET_ID = "1g_Vba8LpRPcmqz_xgpeJrklY0Nn5ce6_BYqUmS9cHbQ"
SPECS_GID      = "1332278035"

# The existing run stays untouched. The new prompt writes to a parallel _v2 block
# placed immediately to the right of it, so the sheet reads before | after.
V1_COLS = ['validation_auto', 'confidence', 'reasoning',
           'is_malfunction_auto', 'model_comparison_auto']

V2_COLS = [c + '_v2' for c in V1_COLS] + [
    'autoval_status_v2',   # llm | prefilter_spam | prefilter_irrelevant |
                       # prefilter_not_owner | prefilter_off_brand | no_text | error
    'validation_delta',    # same | Hit->Miss | Miss->Hit | ... | new | (blank)
]

# the five model-written fields of the v2 block, in V1 order
V2_CORE = V2_COLS[:5]

EMPTY_RESULT = {c: '' for c in V2_COLS}

# ─── PER-TAB CONFIG ─────────────────────────────────────────────────────────
# The rulebook is shared across every tab. Only two inputs vary, because only
# these two are things the model cannot infer:
#
#   name     plain-language vehicle name. NEVER internal jargon — passing the bare
#            tab name "Puma MCA" made the model reject a real hit with "identifies a
#            different vehicle (Ford Puma), which is not the target vehicle (Puma MCA)".
#   spec_key row in the model-specs sheet, or None.

TAB_CONFIG = {
    "Explorer": dict(
        name="Ford Explorer EV (all-electric SUV, Europe, 2024-)",
        spec_key="Explorer"),
    "Tesla Y": dict(
        name="Tesla Model Y",
        spec_key=None),
    "Capri": dict(
        name="Ford Capri EV (all-electric coupe-SUV, 2024-)",
        spec_key=None),
    "Mach-E": dict(
        name="Ford Mustang Mach-E (all-electric SUV)",
        spec_key=None),
    "Puma Gen-E": dict(
        name="Ford Puma Gen-E (all-electric Puma)",
        spec_key=None),
    "Puma MCA": dict(
        name="Ford Puma (petrol / mild-hybrid, post-facelift)",
        spec_key=None),
    "Kuga MCA": dict(
        name="Ford Kuga (post-facelift, petrol / hybrid / plug-in hybrid)",
        spec_key="Kuga"),
    "Transit": dict(
        name="Ford Transit Custom (2nd generation van, 2023-)",
        spec_key="Ford Transit Custom (V710)"),
    "Ranger": dict(
        name="Ford Ranger pickup (T6.2, 2022-)",
        spec_key="Ford Ranger (T6.2)"),
    "Transporter": dict(name="Volkswagen Transporter", spec_key="VW Transporter (T7)"),
    "Trafic":      dict(name="Renault Trafic",          spec_key="Renault Trafic"),
    "Vito":        dict(name="Mercedes-Benz Vito",      spec_key="Mercedes Vito"),
    "Expert":      dict(name="Peugeot Expert",          spec_key="Peugeot Expert"),
    "R4":          dict(name="Renault 4 E-Tech (all-electric, 2025-)", spec_key=None),
    "R5":          dict(name="Renault 5 E-Tech (all-electric, 2024-)", spec_key=None),
}

# ─── SIBLING / TWIN NOTES  (DISABLED — measured regression) ──────────────────
# Added in run7 to disambiguate the tabs that share a nameplate with a real-world
# twin (the US petrol Explorer, the 1969-86 Capri, the petrol Puma). It backfired.
# On the four EV tabs, switching them on took recall 92.9% -> 81.7%, i.e. 12 -> 31
# true hits lost, for a queue that got no smaller. run8's condition rewrite recovered
# only about half of that (87.6%), which is what identifies the notes rather than the
# conditions as the cause.
#
# The 255-mention delivered-report sample re-confirmed it from the other direction:
# residual Explorer losses still quote the note's own escape hatch back at us —
# "refers to a petrol Ford Explorer" — on posts an analyst had already shipped.
# Every note was written default-IN-SCOPE and the model still read it as permission
# to exclude, across two rewrites. Same conclusion as the off-brand prefilter: a
# rule the model can turn into a reason to reject is not worth its upside.
#
# Kept as data, not deleted, so one tab can be re-tested in isolation later.
USE_SIBLING_NOTES = False

SIBLING_NOTES = {
    "Explorer": "A petrol Ford Explorer SUV is sold in the US. If a post is clearly "
                "about that — US location, V6, petrol engine, towing 2+ tonnes — it is "
                "out of scope. BUT this data is European and almost every Explorer post "
                "in it is the electric one. Default to IN SCOPE: unless the text clearly "
                "shows a US petrol model, it is the target vehicle. Do not infer 'petrol' "
                "from a 12V socket, a tyre pump, horsepower figures, or any feature you "
                "think the EV lacks.",
    "Capri": "The Ford Capri of 1969-1986 is a classic petrol coupe. A post clearly "
             "about a restoration, a carburettor or a 1970s car is out of scope. BUT "
             "almost every Capri post in this data is the new electric one. Default to "
             "IN SCOPE unless the text clearly places it in the classic era.",
    "Mach-E": "The petrol Ford Mustang coupe is a different vehicle. But a post saying "
              "only 'Mustang' or 'Mach-E' in this data is the electric Mach-E. Default "
              "to IN SCOPE unless the text clearly describes a petrol V8 coupe.",
    "Puma Gen-E": "A petrol / mild-hybrid Ford Puma exists and is tracked on a separate "
                  "tab. A post clearly about a petrol engine, gears or fuel economy is out "
                  "of scope here. BUT this tab's data was collected for the electric Gen-E: "
                  "default to IN SCOPE unless the text clearly shows a combustion Puma.",
    "Puma MCA": "The all-electric Ford Puma Gen-E is tracked on a separate tab. A post "
                "clearly about charging, kWh or electric range is out of scope here. BUT a "
                "post saying only 'Ford Puma' most likely IS this vehicle — default to IN "
                "SCOPE unless the text clearly shows the electric Gen-E.",
    "Transit": "The larger 2-tonne Ford Transit is a different vehicle — weight codes "
               "350E/470E and the 10-speed automatic belong to it. BUT this tab's data "
               "was collected for the Custom: default to IN SCOPE unless the text clearly "
               "shows the bigger Transit.",
}


# ─── USED / SECOND-HAND VEHICLES ────────────────────────────────────────────
# Three distinct rules under one flag so the whole thing can be measured and pulled
# as a unit. Measured before building, on 3,217 mentions:
#
#   mentions with used-car language        34  (1.1%)
#   ...currently in the v2 review queue     9   <- the most (a) and (b) can remove
#   ...of those, analyst-confirmed hits     1   <- 9 removed per hit lost
#
# 9 mentions is 1.0% of an 882-mention queue, i.e. below the 10-13% churn between
# identical re-runs, so (a) and (b) are not independently measurable. They are here
# because they are near-free, not because they will show up in the numbers.
#
# (c) is the expensive one and is a SCOPE decision, not an accuracy fix: it rejects
# owners who report firsthand experience of a car they bought used. In the delivered
# report — mentions Ford has already accepted — Explorer's one used-car mention is
# exactly this case ("since we bought a used BEV at the end of June") and was kept by
# an analyst. Expect this rule to remove mentions the client has previously been
# happy with, and confirm the scope with them before it ships.
#
# The rule asks for the phrase "used vehicle" in the reasoning so its cost can be
# counted from the output without a schema change.
EXCLUDE_USED_CAR = True

USED_CAR_BLOCK = """
USED / SECOND-HAND VEHICLES — three separate cases. Apply each ONLY on an explicit
statement in the text. Never infer any of them from mileage, an older model year, a
service history, a low price, or the car simply not being new. When you apply any of
them, use the words "used vehicle" in your reasoning.

  a) A used-car ADVERT — a for-sale, wanted or dealer-stock post offering a
     second-hand vehicle. Miss, as with any sales listing.
  b) A SHOPPER weighing up a used one who has never driven it: "is a used one worth
     it?", "what should I check on a second-hand example?". Miss — but because there
     is no own experience, not because the car is used.
  c) A SECOND-HAND OWNER — the author says THIS car was bought used, pre-owned,
     ex-demo or second-hand. Miss: the report covers first-hand buyers only.
     This needs an explicit statement about how they acquired it. "I bought it in
     June", "I've had it two years", high mileage, or an older model year are NOT
     evidence of a used purchase — if how they got the car is not stated, this rule
     does not apply and the mention stands.

A used vehicle that is not the $target_vehicle is beside the point: someone
part-exchanging a second-hand Golf is still a Hit about the target vehicle.
"""


# ─── CAUSE NOT THE VEHICLE  (DISABLED — too nuanced for a blanket rule) ─────
# Would exclude a mention when what is described was caused by something outside the
# vehicle. Built, then switched off before it ever ran. Measured on 3,217 mentions:
#
#                        matched   in v2 queue   true hits there   trade
#   aftermarket          76 (2.4%)      14              2           7:1
#   external damage      61 (1.9%)      12              3           4:1
#
# Both sit below the 20:1 bar other rules were held to, and 26 mentions is ~3% of an
# 882-mention queue — under the 10-13% churn between identical re-runs, so the effect
# would not have been separable from noise anyway.
#
# The deciding evidence is that analysts do NOT treat these as exclusions: 25% and 19%
# Hit rates against a 24.3% baseline. A real exclusion looks like 'Not an Owner' —
# 930 mentions, 0 hits. Both of Explorer's delivered examples would have been cut:
# a tow bar "retrofitted at Ford" (a genuine accessory, not aftermarket), and an
# infotainment "crash with automatic restart" (software, not a collision).
#
# That is the nuance the rule cannot encode. Whether a cause is really external turns
# on who fitted the part, whether the complaint is about the part or the car, and
# which sense of "crash" is meant — per-case judgement, not a blanket rule. Same
# failure mode as the sibling notes: a rule the model can turn into a reason to
# reject. Left as data in case it is ever wanted per-tab.
#
# Still open and NOT addressed by turning this off: 5 of the 12 queued accident
# mentions are flagged is_malfunction_auto = TRUE, i.e. collision damage counted as
# a Ford defect. Fixing that belongs in FIELD 4 and costs no recall.
EXCLUDE_NON_VEHICLE_CAUSE = False

NON_VEHICLE_CAUSE_BLOCK = """
MISS — THE CAUSE IS NOT THE VEHICLE. This report covers the vehicle's own design,
build and behaviour. Exclude the mention when what is described was caused by
something outside it. Apply this ONLY where the text says so — where the cause is not
stated the rule does NOT apply and the mention stands. Say "external cause" in your
reasoning when you use it.

  a) AFTERMARKET — a part, accessory or software that the owner or a third party
     added, where the complaint is about THAT part. Factory options and genuine
     accessories fitted by Ford or its dealer are NOT aftermarket and stay in scope.
     If an aftermarket item is merely mentioned in passing and the complaint is about
     the vehicle, this does not apply.
  b) EXTERNAL DAMAGE — a road traffic collision, another vehicle or object striking
     the car, a kerb or pothole, weather, hail, flood, vandalism or theft.
     "Crash" alone is NOT evidence of this. In this data "crash", "crashed", "froze"
     and "restart" almost always describe SOFTWARE — the infotainment or display
     dropping out and coming back. Read them as a software fault, which is a HIT,
     unless the text clearly describes a real-world impact involving the vehicle.
  c) USER ERROR OR MISUSE — the author states the outcome came from their own
     mistake or from using the vehicle outside its intended use.

Aftersales is NOT an external cause. Contacting Ford, a dealer, a workshop or
customer services about the car remains a Hit, as stated above.
"""


def tab_config(worksheet_name):
    cfg = TAB_CONFIG.get(worksheet_name)
    if cfg is None:
        logger.warning("Tab '%s' is not in TAB_CONFIG — falling back to the bare tab "
                       "name with no specs. Add an entry.", worksheet_name)
        cfg = dict(name=worksheet_name, spec_key=None)
    return dict(cfg,
                siblings=SIBLING_NOTES.get(worksheet_name) if USE_SIBLING_NOTES else None)


# ─── DETERMINISTIC SCOPE FILTER ──────────────────────────────────────────────
# Scope questions the columns can already answer are decided in code, not in the
# prompt. Two reasons: the columns are free, and they are reproducible — across two
# identical re-runs the prefiltered rows were byte-identical while prompt-driven
# verdicts flipped on 10.4% of rows.
#
# Cost measured on Explorer/Mach-E/Capri/Puma Gen-E (783-mention review queue,
# 162 true hits). "per hit" = mentions removed for each true hit lost:
#   ownership_status = Not an Owner       91 removed,  0 hits lost      (perfect)
#   mention_relevance = Sales / Spam       1 removed,  0 hits lost      (perfect)
#   mention_relevance = Irrelevant       114 removed,  1 hit  lost      (114 per hit)
# Superseded by the wider 8-tab measurement next to PREFILTER_OWNERSHIP below, which
# is what the current rules are set from.
#
# Deliberately NOT included — these cost recall faster than they shrink the queue,
# which is the wrong side of the FN/FP trade:
#   thread reply (8.6 per hit) · model_citation = No Vehicle Mentioned (7.7)
#   ownership_status = Pre-Ownership (5.2, and it is ownership-journey) · <80 chars (5.1)

PREFILTER_RELEVANCE = {
    "sales / spam": "prefilter_spam",
    "sales/spam":   "prefilter_spam",
    "irrelevant":   "prefilter_irrelevant",
}

# Measured on 3,226 mentions / 253 true hits across EV, MCA and CV tabs:
#   ownership_status = Not an Owner      930 mentions, 0 true hits   -> FREE, keep
#   ownership_status = Showing Interest   96 mentions, 3 true hits   -> 32 per hit, DROPPED
# 'Showing Interest' is also a legitimate tracked category in the owner database, so
# it is left to the model rather than filtered out.
PREFILTER_OWNERSHIP = {
    "not an owner": "prefilter_not_owner",
}

# vehicle_ownership_status is database-backed — it records what the author is
# registered as. It is a POSITIVE signal, never a filter:
#   Owner            781 mentions, 17.0% hit rate  (2.2x base)
#   Pre-Ownership    165 mentions,  8.5%           (1.1x)
#   blank          2,217 mentions,  4.7%  but holds 105 true hits
# Filtering on blank would cost 105 of 253 hits — absence is a missing record, not
# evidence against ownership. So it is passed to the model as corroboration instead.

# Target brand per tab, for the off-brand check. A mention is dropped only when the
# pipeline named a brand AND it is not this one — UNSURE or blank always passes.
# Retained for reference; the off-brand prefilter is disabled (see prefilter()).
TAB_TO_BRAND = {
    "Explorer": "Ford", "Capri": "Ford", "Mach-E": "Ford", "Puma Gen-E": "Ford",
    "R4": "Renault", "R5": "Renault", "Trafic": "Renault",
    "Transporter": "Volkswagen", "Vito": "Mercedes", "Expert": "Peugeot",
}

# ─── RE-RUN BEHAVIOUR ────────────────────────────────────────────────────────
# False  = resume. Rows with a complete v2 block are left alone, so an interrupted
#          run can be restarted without paying for the rows already scored.
# True   = overwrite. Every row is scored again and the v2 block is replaced, with
#          no need to clear the columns in the sheet first.
# Set it here, or per call: bd.run_async_processing(df, tab, overwrite=True)
OVERWRITE_V2 = False

# Truncation budgets (characters) — keeps prompt cost bounded on long threads
MAX_TEXT = 4000
MAX_PREV = 1200
MAX_META = 400

# ──────────────────────────────────────────────────────────────────────────────
# VEHICLE SPECS  — parsed from the model-specs sheet
# ──────────────────────────────────────────────────────────────────────────────

_GEN_FIELDS = ['volume', 'current_gen', 'new_only_specs', 'removed_specs', 'visual', 'keywords']

_GEN_LABELS = {
    'current_gen':    'Current generation (context only — earlier generations of\n'
                      'this same vehicle also count as the target vehicle)',
    'new_only_specs': 'Only the current generation has these',
    'removed_specs':  'Older-generation kit (context only — an earlier generation of\n'
                      'this vehicle STILL COUNTS, do not exclude on it)',
    'visual':         'Visual identifiers of the current generation',
    'keywords':       'Terms owners use for the current generation',
}

# Rows of a comparison table that carry scope meaning, in the order we render them
_CMP_ORDER = ['Model Names', 'Model Options', 'Engine Options', 'Engine',
              'New Technologies', 'New Hardware', 'Specs', 'Size and Weight',
              'Color Options', 'Colour', 'Differentiating Keywords',
              'Shared Features', 'Shared Hardware', 'Exclusions']


def _norm(s):
    return re.sub(r'\s+', ' ', (s or '').strip().lower())


def fetch_specs_rows(sheet_id=None, gid=None, gc=None):
    """Read the specs sheet live. Prefers gspread (works on private sheets using the
    Colab credentials already set up); falls back to the public CSV export."""
    sheet_id = sheet_id or SPECS_SHEET_ID
    gid = str(gid or SPECS_GID)

    if gc is not None:
        try:
            ss = gc.open_by_key(sheet_id)
            ws = next((w for w in ss.worksheets() if str(w.id) == gid), None)
            if ws is None:
                ws = ss.get_worksheet(0)
                logger.warning("gid %s not found in specs sheet — using first tab '%s'.",
                               gid, ws.title)
            logger.info("Specs read live from '%s' via gspread.", ws.title)
            return ws.get_all_values()
        except Exception as e:
            logger.warning("gspread read of specs sheet failed (%s) — trying CSV export.", e)

    import urllib.request
    url = (f"https://docs.google.com/spreadsheets/d/{sheet_id}"
           f"/export?format=csv&gid={gid}")
    try:
        with urllib.request.urlopen(url, timeout=60) as resp:
            text = resp.read().decode('utf-8', errors='replace')
        logger.info("Specs read live via CSV export.")
        return list(csv.reader(text.splitlines()))
    except Exception as e:
        logger.error("Could not read the specs sheet (%s). Running without specs.", e)
        return []


def _discover_generation_tables(header):
    """Locate every 'Model | … | Current Gen …' table by its header signature rather
    than a fixed column offset, so a column inserted in the sheet cannot silently
    shift the parse onto the wrong data."""
    cols = []
    for c, v in enumerate(header):
        if _norm(v) != 'model':
            continue
        if any('current gen' in _norm(x) for x in header[c + 1:c + 8]):
            cols.append(c)
    return cols


def _discover_comparison_tables(header):
    """Locate 'Category | <ModelA> | <ModelB> …' tables."""
    out = []
    for c, v in enumerate(header):
        if _norm(v) != 'category':
            continue
        models = []
        for c2 in range(c + 1, min(c + 6, len(header))):
            name = header[c2].strip()
            if not name:
                break
            models.append((c2, name))
        if models:
            out.append((c, models))
    return out


def load_vehicle_specs(rows=None, gc=None):
    """Returns {model_name: {...}}. Each entry carries _kind = generation|comparison."""
    rows = rows if rows is not None else fetch_specs_rows(gc=gc)
    if not rows:
        return {}

    width = max(len(r) for r in rows)
    rows = [list(r) + [''] * (width - len(r)) for r in rows]
    header = rows[0]
    specs = {}

    for c0 in _discover_generation_tables(header):
        for r in rows[1:]:
            name = r[c0].strip()
            if not name:
                continue
            entry = {'_kind': 'generation'}
            for i, f in enumerate(_GEN_FIELDS):
                if c0 + 1 + i < width:
                    entry[f] = r[c0 + 1 + i].strip()
            specs[name] = entry

    for c0, models in _discover_comparison_tables(header):
        for col, name in models:
            fields = {}
            for r in rows[1:]:
                k, v = r[c0].strip(), r[col].strip()
                if k and v:
                    fields[k] = v
            if fields:
                specs[name] = {'_kind': 'comparison', **fields}

    logger.info("Specs loaded for %d vehicles: %s", len(specs), ", ".join(sorted(specs)))
    return specs


def build_vehicle_context(spec_key, specs):
    """Render the scope block that tells the model which vehicle/generation is in play."""
    s = specs.get(spec_key) if spec_key else None
    if not s:
        return ("(No specification entry for this vehicle in the model-specs sheet — "
                "judge on the text alone, and be strict about the vehicle actually "
                "being the target.)")

    # 'removed_specs' lists older-generation kit. It is NOT an exclusion: an earlier
    # generation of the same vehicle still counts, so it renders as context only.
    OUT_KEYS = {'Exclusions'}
    inside, outside = [], []

    if s.get('_kind') == 'generation':
        for f, label in _GEN_LABELS.items():
            if s.get(f):
                (outside if f in OUT_KEYS else inside).append(f"{label}:\n  {s[f]}")
    else:
        seen = set()
        for k in _CMP_ORDER:
            if s.get(k):
                (outside if k in OUT_KEYS else inside).append(f"{k}:\n  {s[k]}")
                seen.add(k)
        for k, v in s.items():
            if k != '_kind' and k not in seen and v:
                (outside if k in OUT_KEYS else inside).append(f"{k}:\n  {v}")

    blocks = []
    if inside:
        blocks.append("--- IN SCOPE — all of this IS the target vehicle ---\n\n"
                      + "\n\n".join(inside))
    if outside:
        blocks.append("--- OUT OF SCOPE — a post describing these is a different vehicle ---\n\n"
                      + "\n\n".join(outside))
    return "\n\n".join(blocks) if blocks else "(Specification entry found but empty.)"


# ──────────────────────────────────────────────────────────────────────────────
# PROMPT
# ──────────────────────────────────────────────────────────────────────────────
# string.Template ($placeholders) rather than str.format, so the JSON braces in
# the worked examples need no escaping.

PROMPT_TEMPLATE = Template("""
You are validating social-listening mentions for a report about the **$target_vehicle**.

Return a JSON object with exactly 5 fields.

===============================  VEHICLE IN SCOPE  ===============================
Read the two halves of this block differently.

Everything under IN SCOPE is the $target_vehicle — including every trim, variant,
engine and feature named there. A post mentioning one of those trims IS about the
target vehicle, not a different one. It is not a checklist: a post need not mention
any of it, and most genuine posts will mention none of it. Never treat absence as
evidence of a different vehicle.

Only the OUT OF SCOPE half identifies vehicles to exclude.

$vehicle_context

==============================  MENTION TO JUDGE  ===============================
VERBATIM:
$text

PRECEDING COMMENT IN THE THREAD (context only — may be empty):
$previous_comment

$siblings_block
OWNER DATABASE — our own records for this author, from the owner database. This is
verified, not inferred. A status of Owner or Pre-Ownership SETTLES condition 1: the
author does own or awaits this vehicle, so do not argue that they might not. A blank
means only that we hold no record for them — it is NOT evidence against ownership and
must never count against the mention:
$ownership

WHERE IT CAME FROM — a single-model forum, a thread title or metadata naming the
vehicle is REASONABLE EVIDENCE the post is about it. Owners on a dedicated forum
rarely name their own car again, so do not require the verbatim to name it:
$signals
=================================================================================

### FIELD 1 — validation_auto

Missing a genuine mention costs far more than passing a weak one. When the call is
close, return the MORE inclusive answer.

  Judge the two conditions SEPARATELY. Condition 1 asks only "is this person
  speaking from their own experience of a car?" — answer it WITHOUT considering
  which car. Condition 2 handles the vehicle. Never write off a firsthand report
  because it does not name the vehicle; that is condition 2's job, and condition 2
  passes by default.

  1. OWN EXPERIENCE — the text reflects personal ownership, firsthand experience, or
     direct interaction with SOME car. Be generous here:
       - If the OWNER DATABASE below records this author as Owner or Pre-Ownership,
         condition 1 is SATISFIED. Do not re-argue it.
       - Explicit ownership or use: "I just got mine and drove it 500km."
       - Firsthand experience: "My infotainment screen froze yesterday."
       - A personal problem or its solution, a service visit, a warranty claim
       - A purchase decision from personal testing: "I test drove both."
       - Personal context inside a question: "My trip counter resets. Is this normal?"
       - Ordering, production, delivery and waiting updates — all part of ownership
       - A reply that adds the author's own experience to a discussion
       - "Mine does that too", "same here on mine" — a comparison to their own car
       - Contacting Ford, a dealer or customer services about their vehicle
       - An opinion on colours, options, pricing or spec from someone choosing or
         living with the car
       - Explaining how a feature behaves from having used it

  2. TARGET VEHICLE — assume the post IS about the $target_vehicle. This condition
     PASSES by default: when the vehicle is never named, when the text is vague, and
     whenever you are unsure. Fail it ONLY when the text definitely identifies some
     other vehicle as the thing being described. When in doubt, keep it.

Return "Hit" when both hold. Return "Maybe" when own experience is genuinely unclear
— never because the vehicle is unnamed, which passes condition 2. Otherwise "Miss".

MISS — no firsthand basis at all:
  - General buying advice or speculation from someone who has not used the vehicle
  - Hearsay and market chatter: "I read that charging speed is slower." (This is about
    information the author did not witness — nothing to do with second-hand cars.)
  - Purely technical explanation or spec recitation with no personal tie-in
  - Hypothetical scenarios: "What if the range is worse in winter?"
  - News, press coverage or a review written about the vehicle by a third party

MISS — DIFFERENT VEHICLE. Apply this ONLY when another vehicle is the SUBJECT of the
experience being described. Merely naming another car is not enough — people compare,
mention what a neighbour drives, or say what they are trading in, and those are still
Hits about the target vehicle. Never infer this from silence, from an unnamed vehicle,
from missing feature names, or from the post not matching the scope block:
  - A different make or model is explicitly named as the thing being described.
    A TRIM or VARIANT of the target vehicle is NOT a different model — "Explorer
    Select", "Explorer Premium", "ST-Line" and the like are the target vehicle.
  - The author says it is their previous car, a rental, a courtesy car, or someone
    else's car
  - NOT model year or generation. An older or newer model year of the target vehicle
    is still the target vehicle. "I have the older 2024 model" is a HIT.
  - An unrelated product entirely (a bicycle, a phone, a different appliance)
If none of those is explicit in the text, this rule does NOT apply.

MISS — SALES LISTING OR ADVERT. A classified ad rather than a report of experience:
for-sale or wanted posts, dealer stock, price/mileage/spec listed as selling points.
(Most of these are already removed upstream; this is a backstop.)
$used_car_block
$non_vehicle_cause_block
MISS — PURE NOISE. No experience and no opinion of any kind: a bare "thanks", an
emoji, a one-line joke. Judge this narrowly — a short post that still reports
something the author saw, felt or did is a Hit.

STILL A HIT — these are the cases most often got wrong. Do not penalise them:
  - A brief post that nonetheless reports something firsthand.
  - A post naming another car in passing while describing the target vehicle.
  - Aftersales: contacting Ford, a dealer, a workshop or customer services about the
    car. "I wrote to the customer centre and got a fast reply" is firsthand experience.
  - Build slots, production dates and delivery tracking, including comparing dates
    with other owners in the thread.
  - A setting, sound or feature behaving oddly on "the car" / "mine", with no model
    name anywhere in the post.
  - Any model year or generation of the target vehicle, old or new.
  - A reply in a thread that carries the author's own experience.
  - A question that carries personal context: "I noticed my trip counter resets. Is
    this normal?" — this is firsthand and counts.
  - A reply that adds the author's own experience to the discussion.
  - An owner who never names the vehicle. "My tow hitch no longer swings out" is a
    Hit. So is any firsthand post that simply does not say which car it is about.
  - A post that mentions none of the features, trims or specs in the scope block.
  - Service, warranty, dealer and aftersales experience with the vehicle.
  - Ordering, production, delivery and waiting updates.

### FIELD 2 — confidence
Integer 0-100: how certain you are of the validation_auto value you chose, judged on
the evidence in the text alone. Do NOT tie this to which label you picked — a Miss can
be certain (100) and a Hit can be marginal (60). Use the full range.

### FIELD 3 — reasoning
One sentence. State which of the three conditions decided it, and for a Miss say which
MISS rule applied.

### FIELD 4 — is_malfunction_auto
Boolean. TRUE only when a defect is clearly confirmed:
  - The author states something is broken, failed, or not working
  - A concrete defect described from firsthand experience
  - A repair, service visit or warranty claim because of a defect
FALSE when: the issue was resolved ("rebooting fixed it"); it is speculation, hearsay
or a hypothetical; it is a quirk, preference or annoyance rather than a failure; no
defect is mentioned; or the author is unsure whether it is actually broken.

### FIELD 5 — model_comparison_auto
Boolean. TRUE when the author compares this vehicle to a previous or different model
they owned or drove, or signals they are switching from one. FALSE when no other model
is mentioned, or a model name appears only as a passing reference with no comparison.

===============================  WORKED EXAMPLES  ===============================
Roughly one mention in eight is a genuine Hit, so most of these are Misses.

Input: "Picked mine up in March. The 8-speed auto is much smoother than I expected on
        the motorway, though it hunts a bit in town."
Output: {"validation_auto": "Hit", "confidence": 93, "reasoning": "Firsthand ownership of the target vehicle with a specific driving observation.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "My door handle completely stopped working. Dealer confirmed it needs replacement."
Output: {"validation_auto": "Hit", "confidence": 96, "reasoning": "Confirmed defect from firsthand experience with dealer involvement.", "is_malfunction_auto": true, "model_comparison_auto": false}

Input: "Hate these vans. Was working fine last week, went away for a week, now it will
        not start. Turns over but no ignition, no codes on the reader."
Output: {"validation_auto": "Miss", "confidence": 88, "reasoning": "Firsthand and a real fault, but the vehicle described is not the target vehicle.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Selling my 2023, 45,000 km, full service history, tow bar and roof bars
        included. Negotiable at 13,500."
Output: {"validation_auto": "Miss", "confidence": 97, "reasoning": "Classified listing rather than a report of experience.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "I have the ones from teslabs."
Output: {"validation_auto": "Miss", "confidence": 90, "reasoning": "Ownership implied but no experience, opinion or outcome described.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Yeah same here, agreed."
Output: {"validation_auto": "Miss", "confidence": 92, "reasoning": "Bare thread reply that adds no experience of its own.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "I read that the charging speed is slower than expected."
Output: {"validation_auto": "Miss", "confidence": 89, "reasoning": "Secondhand information with no firsthand basis.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Mine has the old halogen lights and the pre-facelift dashboard. Still going
        strong at 180,000 km."
Output: {"validation_auto": "Miss", "confidence": 85, "reasoning": "Firsthand ownership, but the specs described belong to an out-of-scope earlier generation.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Coming from the previous shape, the new cabin feels a generation ahead — the
        digital cockpit alone is worth it."
Output: {"validation_auto": "Hit", "confidence": 88, "reasoning": "Firsthand experience of the current generation with an explicit prior-model comparison.", "is_malfunction_auto": false, "model_comparison_auto": true}

Input: "Hello everyone, my tow hitch no longer swings out. Any ideas what to do?"
        [source: a forum dedicated to the $target_vehicle]
Output: {"validation_auto": "Hit", "confidence": 88, "reasoning": "Firsthand fault on a dedicated forum for the target vehicle; the author need not name it.", "is_malfunction_auto": true, "model_comparison_auto": false}

Input: "My rear flap wouldn't hold a roll of toilet paper and a hat. You also notice
        the reduced acoustic insulation. I transported water crates today."
Output: {"validation_auto": "Hit", "confidence": 87, "reasoning": "Firsthand use with specific observations; the vehicle is never named but nothing indicates a different one, so condition 2 passes.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Mine was finally built yesterday. Should be here by the end of the month."
Output: {"validation_auto": "Hit", "confidence": 84, "reasoning": "Firsthand ownership-journey update; build and delivery stages count as experience.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "5 year service plan and Ford assist. I queried years 3-4 with the dealer as no
        service is scheduled, and they confirmed cover continues."
Output: {"validation_auto": "Hit", "confidence": 86, "reasoning": "Firsthand aftersales experience with the target vehicle's service plan.", "is_malfunction_auto": false, "model_comparison_auto": false}

Input: "Does it support wireless CarPlay?"
Output: {"validation_auto": "Maybe", "confidence": 55, "reasoning": "Feature question with no personal context; possible owner but no experience described.", "is_malfunction_auto": false, "model_comparison_auto": false}
=================================================================================

Output ONLY a valid JSON object with exactly these 5 keys:
validation_auto, confidence, reasoning, is_malfunction_auto, model_comparison_auto.
""")


# ──────────────────────────────────────────────────────────────────────────────
# COLUMN RESOLUTION  — by name, with positional fallback
# ──────────────────────────────────────────────────────────────────────────────
# The old code used row.iloc[10] for the verbatim. That silently breaks the moment a
# column is inserted upstream, so resolve by header name and fall back to position.

COLUMN_FALLBACKS = {
    'translated_text':       10,
    'post_text':             1,
    'url':                   3,
    'mention_metadata':      5,
    'domain':                6,
    'previous_comment_text': 7,
}


def resolve(df, name):
    if name in df.columns:
        return name
    pos = COLUMN_FALLBACKS.get(name)
    if pos is not None and pos < len(df.columns):
        logger.warning("Column '%s' not found by name — falling back to position %d ('%s').",
                       name, pos, df.columns[pos])
        return df.columns[pos]
    return None


def get(row, df, name, default=''):
    col = resolve(df, name)
    if col is None:
        return default
    val = row.get(col, default)
    return '' if pd.isna(val) else str(val).strip()


def clip(s, n):
    s = (s or '').strip()
    return s if len(s) <= n else s[:n] + ' …[truncated]'


def build_ownership(row, df):
    """Database-backed ownership, rendered as corroboration."""
    vos = get(row, df, 'vehicle_ownership_status')
    infer = get(row, df, 'ownership_status')
    bits = []
    if vos:
        bits.append(f"  Recorded in the owner database as: {vos}")
    else:
        bits.append("  No database record for this author (this means nothing either way).")
    if infer:
        bits.append(f"  Pipeline's own reading of the text: {infer}")
    return "\n".join(bits)


def build_signals(row, df):
    """Weak provenance hints. Explicitly labelled as hints so the model does not treat
    a vehicle-specific forum or thread title as proof the author owns that vehicle."""
    bits = []
    for label, col in (("Source / domain", 'domain'),
                       ("Thread title",    'thread_title'),
                       ("Mention type",    'mention_type'),
                       ("Data source",     'data_source')):
        v = get(row, df, col)
        if v:
            bits.append(f"  {label}: {v}")

    url = get(row, df, 'url')
    if url:
        bits.append(f"  URL: {clip(url, 200)}")

    meta = get(row, df, 'mention_metadata')
    if meta:
        bits.append(f"  Mention metadata: {clip(meta, MAX_META)}")

    return "\n".join(bits) if bits else "  (none available)"


# ──────────────────────────────────────────────────────────────────────────────
# BEFORE / AFTER
# ──────────────────────────────────────────────────────────────────────────────

def _label(v):
    v = ('' if v is None or (isinstance(v, float) and pd.isna(v)) else str(v)).strip()
    return v


def compute_delta(v1, v2):
    """One sortable value describing what the rewrite did to this row."""
    a, b = _label(v1), _label(v2)
    if not b:
        return ''
    if not a:
        return 'new'
    if a == b:
        return 'same'
    return f'{a}->{b}'


def place_v2_beside_v1(df):
    """Move the _v2 block so it sits immediately after model_comparison_auto,
    rather than at the far right of the sheet."""
    cols = list(df.columns)
    block = [c for c in V2_COLS if c in cols]
    if not block:
        return df
    rest = [c for c in cols if c not in block]
    if 'model_comparison_auto' in rest:
        at = rest.index('model_comparison_auto') + 1
    elif 'validation_auto' in rest:
        at = rest.index('validation_auto') + 1
    else:
        at = len(rest)          # no v1 present — append
    return df[rest[:at] + block + rest[at:]]


def comparison_summary(df, worksheet_name):
    """Log the before/after picture for this tab."""
    if 'validation_delta' not in df.columns:
        return
    scored = df[df['validation_auto_v2'].map(_label) != '']
    if scored.empty:
        logger.info("%s: nothing scored in v2.", worksheet_name)
        return

    n = len(scored)
    counts = scored['validation_delta'].value_counts()
    same = int(counts.get('same', 0))
    new = int(counts.get('new', 0))
    changed = n - same - new

    logger.info("%s — v1 vs v2 on %d rows: %d unchanged, %d changed, %d newly scored",
                worksheet_name, n, same, changed, new)

    moves = counts[[i for i in counts.index if '->' in str(i)]]
    for move, c in moves.items():
        logger.info("      %-14s %5d  (%.1f%%)", move, c, 100 * c / n)

    v1_hit = int((scored['validation_auto'].map(_label) == 'Hit').sum())
    v2_hit = int((scored['validation_auto_v2'].map(_label) == 'Hit').sum())
    if v1_hit:
        logger.info("      auto-Hit volume: %d -> %d  (%+.1f%%)",
                    v1_hit, v2_hit, 100 * (v2_hit - v1_hit) / v1_hit)

    pre = int((scored['autoval_status_v2'].map(_label).str.startswith('prefilter')).sum())
    if pre:
        logger.info("      of which pre-filtered without an API call: %d", pre)


# ──────────────────────────────────────────────────────────────────────────────
# LLM
# ──────────────────────────────────────────────────────────────────────────────

class LLMBase:
    def __init__(self, openai_api_key, max_concurrent_requests=10, retries=2, timeout=60):
        key = (openai_api_key or '').strip()
        if not key:
            raise ValueError("No OpenAI API key supplied. Load it from the Colab "
                             "secrets manager rather than hard-coding it.")
        # Kept only inside the header dict. Never logged, never echoed, and not put
        # on an attribute of its own so a stray repr(self) cannot surface it.
        self.headers = {
            "Authorization": f"Bearer {key}",
            "Content-Type": "application/json",
        }
        self._key_tail = key[-4:]        # for identifying which key is in use
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        nest_asyncio.apply()

    async def async_request(self, prompt, session):
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 800,
            "temperature": 0,
            "seed": 42,          # temperature 0 alone is not deterministic here
            "response_format": {"type": "json_object"},
        }
        for attempt in range(self.retries + 1):
            try:
                async with self.semaphore:
                    async with session.post(
                        "https://api.openai.com/v1/chat/completions",
                        headers=self.headers, json=payload,
                    ) as response:
                        if response.status == 200:
                            result = await response.json()
                            return json.loads(result['choices'][0]['message']['content'])
                        body = await response.text()
                        if response.status in (429, 500, 502, 503, 504) and attempt < self.retries:
                            wait = 2 ** attempt
                            logger.warning("HTTP %s, retrying in %ss", response.status, wait)
                            await asyncio.sleep(wait)
                            continue
                        logger.error("HTTP %s: %s", response.status, body[:400])
                        return None
            except Exception as e:
                if attempt < self.retries:
                    # log the exception TYPE only — aiohttp messages can embed request
                    # detail, and this is the one place credentials are in scope
                    logger.warning("Request failed (%d/%d): %s", attempt + 1,
                                   self.retries, type(e).__name__)
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error("Request failed after %d attempts: %s",
                                 self.retries, type(e).__name__)
                    return None
        return None


class VehicleProcessor(LLMBase):
    def __init__(self, openai_api_key, specs=None, gc=None, overwrite=None,
                 max_concurrent_requests=20, retries=2, timeout=60):
        super().__init__(openai_api_key, max_concurrent_requests, retries, timeout)
        self.specs = specs if specs is not None else load_vehicle_specs(gc=gc)
        self.overwrite = OVERWRITE_V2 if overwrite is None else bool(overwrite)

    # ---- prompt assembly -----------------------------------------------------

    def generate_prompt(self, row, df, target_vehicle, vehicle_context, siblings=None):
        text = get(row, df, 'translated_text') or get(row, df, 'post_text')
        sib = (f"SAME NAME, DIFFERENT VEHICLE — read this carefully:\n  {siblings}\n"
               if siblings else "")
        # USED_CAR_BLOCK carries its own $target_vehicle, so it is rendered before
        # being passed in as a value — substitute() does not recurse into values.
        used = (Template(USED_CAR_BLOCK).substitute(target_vehicle=target_vehicle)
                if EXCLUDE_USED_CAR else "")
        cause = NON_VEHICLE_CAUSE_BLOCK if EXCLUDE_NON_VEHICLE_CAUSE else ""
        return PROMPT_TEMPLATE.substitute(
            target_vehicle=target_vehicle,
            vehicle_context=vehicle_context,
            siblings_block=sib,
            used_car_block=used,
            non_vehicle_cause_block=cause,
            ownership=build_ownership(row, df),
            text=clip(text, MAX_TEXT),
            previous_comment=clip(get(row, df, 'previous_comment_text'), MAX_PREV) or "(none — this is a standalone post)",
            signals=build_signals(row, df),
        )

    # ---- pre-filter ----------------------------------------------------------

    @staticmethod
    def mention_scope(df):
        """Scope fields vary between the rows of one mention — a post can have one
        feedback row tagged 'Not an Owner' and another tagged 'Owner'. Judging per
        row drops the whole mention on the say-so of its weakest row, which cost 14
        true hits in testing. Resolve each field across the mention first, taking the
        most inclusive value, and key it by id."""
        OWN_RANK = {'owner': 0, 'pre-ownership': 1, 'showing interest': 2,
                    'not an owner': 3, '': 4}
        REL_RANK = {'relevant': 0, 'needs more info': 1, 'news': 2,
                    'irrelevant': 3, 'sales / spam': 4, 'sales/spam': 4, '': 1}
        idc  = resolve(df, 'id')
        ownc = resolve(df, 'ownership_status')
        relc = resolve(df, 'mention_relevance')
        brc  = resolve(df, 'brand')
        if idc is None:
            return {}

        scope = {}
        for mid, grp in df.groupby(idc):
            def most_inclusive(col, rank):
                if col is None:
                    return ''
                vals = [str(v).strip() for v in grp[col] if str(v).strip()]
                return min(vals, key=lambda v: rank.get(v.lower(), 1)) if vals else ''
            brands = {str(v).strip() for v in (grp[brc] if brc is not None else [])
                      if str(v).strip() and str(v).strip().upper() != 'UNSURE'}
            scope[str(mid).strip()] = {
                'own':    most_inclusive(ownc, OWN_RANK),
                'rel':    most_inclusive(relc, REL_RANK),
                'brands': brands,
            }
        return scope

    @staticmethod
    def prefilter(row, df, target_brand=None, scope=None):
        """Return a status string if this row should skip the LLM, else None.
        Every rule here is a scope question, never a judgement about whether the
        author is describing real experience — that stays with the model."""
        text = get(row, df, 'translated_text') or get(row, df, 'post_text')
        if not text:
            return 'no_text'

        sc = (scope or {}).get(get(row, df, 'id'), None)
        rel = sc['rel'] if sc else get(row, df, 'mention_relevance')
        own = sc['own'] if sc else get(row, df, 'ownership_status')

        status = PREFILTER_RELEVANCE.get(rel.lower())
        if status:
            return status

        status = PREFILTER_OWNERSHIP.get(own.lower())
        if status:
            return status

        # The off-brand filter was removed. It dropped 21 mentions for 2 true hits,
        # and both losses were authors naming a brand they do NOT own — a neighbour's
        # BMW, a Skoda they currently drive while ordering a Capri. The brand column
        # records any brand mentioned, not the subject of the experience, so it
        # cannot carry this decision. Left to the model, which can read the sentence.
        return None

    # ---- main ----------------------------------------------------------------

    async def process_dataframe(self, df, target_vehicle, vehicle_context, target_brand=None,
                                siblings=None, overwrite=False):
        scope = self.mention_scope(df)
        # v1 columns are read-only here. Create them empty only if the tab has
        # never been validated, so the comparison column still has something to say.
        for col in V1_COLS:
            if col not in df.columns:
                df[col] = ""
        for col in V2_COLS:
            if col not in df.columns:
                df[col] = ""

        async with aiohttp.ClientSession(timeout=self.timeout) as session:

            async def process_row(row):
                # Resume on the v2 block, never on v1 — a populated v1 must not stop
                # the rewrite from scoring the row. With overwrite=True nothing is
                # kept, so the columns need not be cleared in the sheet first.
                if not overwrite and all(_label(row.get(c, '')) != '' for c in V2_CORE):
                    return {c: row.get(c, '') for c in V2_COLS}

                status = self.prefilter(row, df, target_brand, scope)
                if status == 'no_text':
                    return {**EMPTY_RESULT, 'autoval_status_v2': 'no_text'}
                if status:
                    label = {
                        'prefilter_spam':       'sales listing / spam',
                        'prefilter_irrelevant': 'flagged irrelevant',
                        'prefilter_not_owner':  'author is not an owner of this vehicle',
                        'prefilter_off_brand':  'resolved brand is not the target brand',
                    }.get(status, status)
                    return {
                        'validation_auto_v2': 'Miss',
                        'confidence_v2': 99,
                        'reasoning_v2': f'Scope pre-filter, no model call: {label}.',
                        'is_malfunction_auto_v2': False,
                        'model_comparison_auto_v2': False,
                        'autoval_status_v2': status,
                        'validation_delta': '',
                    }

                prompt = self.generate_prompt(row, df, target_vehicle, vehicle_context, siblings)
                response = await self.async_request(prompt, session)
                if not response:
                    return {**EMPTY_RESULT, 'autoval_status_v2': 'error'}

                out = {v2: response.get(v1, '') for v1, v2 in zip(V1_COLS, V2_CORE)}
                out['autoval_status_v2'] = 'llm'
                out['validation_delta'] = ''
                return out

            tasks = [process_row(row) for _, row in df.iterrows()]
            results = await tqdm.gather(*tasks, desc=f"Validating {target_vehicle}")

        results = [r if isinstance(r, dict) else dict(EMPTY_RESULT) for r in results]
        results_df = pd.DataFrame(results).reindex(columns=V2_COLS, fill_value='')
        results_df.index = df.index
        df[V2_COLS] = results_df[V2_COLS]

        # before/after label for every scored row
        df['validation_delta'] = [
            compute_delta(a, b)
            for a, b in zip(df['validation_auto'], df['validation_auto_v2'])
        ]

        return place_v2_beside_v1(df)

    def run_async_processing(self, df, worksheet_name, overwrite=None):
        cfg = tab_config(worksheet_name)
        display_name, spec_key, siblings = cfg['name'], cfg['spec_key'], cfg.get('siblings')
        vehicle_context = build_vehicle_context(spec_key, self.specs)
        logger.info("%s -> %r | specs: %s | sibling note: %s",
                    worksheet_name, display_name, spec_key or 'none',
                    'yes' if siblings else 'no')

        target_brand = TAB_TO_BRAND.get(worksheet_name)
        if target_brand is None:
            logger.warning("No target brand mapped for '%s' — off-brand filter disabled.",
                           worksheet_name)

        ow = self.overwrite if overwrite is None else bool(overwrite)

        # how much work this run implies, before spending anything on it
        already = 0
        if all(c in df.columns for c in V2_CORE):
            already = int(df.apply(
                lambda r: all(_label(r.get(c, '')) != '' for c in V2_CORE), axis=1).sum())
        if ow:
            logger.info("%s: OVERWRITE — re-scoring all %d rows, replacing %d existing "
                        "v2 verdicts (no need to clear the columns).",
                        worksheet_name, len(df), already)
        elif already:
            logger.info("%s: resume — %d of %d rows already have a v2 verdict and will "
                        "be skipped. Pass overwrite=True to re-score them.",
                        worksheet_name, already, len(df))

        scope = self.mention_scope(df)
        stats = {}
        for _, r in df.iterrows():
            st = self.prefilter(r, df, target_brand, scope)
            if st:
                stats[st] = stats.get(st, 0) + 1
        skipped = sum(stats.values())
        logger.info("%s: %d rows, %d skip the model (%.1f%%) — %s",
                    worksheet_name, len(df), skipped,
                    100 * skipped / max(len(df), 1),
                    ", ".join(f"{k.replace('prefilter_','')} {v}" for k, v in sorted(stats.items())) or "none")

        df = asyncio.run(self.process_dataframe(df, display_name, vehicle_context,
                                                target_brand, siblings, ow))
        comparison_summary(df, worksheet_name)
        return df


# ═══════════════════════════════════════════════════════════════════════════
# CELL 3 — sheet I/O + execution   (run AFTER the module cell)
# ═══════════════════════════════════════════════════════════════════════════

MAX_SHEET_RETRIES = 3


def get_data_from_sheet(sheet_name, attempt=1):
    """Read a worksheet into a DataFrame. Bounded retries — the original version
    recursed without a limit, so a persistent API error looped forever."""
    try:
        sheet = gc.open_by_key(SPREADSHEET_NAME).worksheet(sheet_name)
        data = sheet.get_all_values()
        if not data:
            logger.warning("%s is empty.", sheet_name)
            return pd.DataFrame()
        df = pd.DataFrame(data[1:], columns=data[0])
        logger.info("%s: read %d rows x %d cols", sheet_name, len(df), len(df.columns))
        return df
    except gspread.exceptions.APIError as e:
        if attempt >= MAX_SHEET_RETRIES:
            logger.error("Giving up reading %s after %d attempts: %s",
                         sheet_name, attempt, e)
            raise
        wait = 5 * attempt
        logger.warning("API error reading %s (attempt %d/%d), retrying in %ss: %s",
                       sheet_name, attempt, MAX_SHEET_RETRIES, wait, e)
        time.sleep(wait)
        return get_data_from_sheet(sheet_name, attempt + 1)


def update_sheet_with_results(sheet_name, df, attempt=1):
    """Write back. resize=True is required — the v2 block adds 7 columns, and
    without it gspread writes into the existing grid and errors once the frame
    is wider than the sheet."""
    try:
        sheet = gc.open_by_key(SPREADSHEET_NAME).worksheet(sheet_name)
        set_with_dataframe(sheet, df.fillna(''), resize=True)
        logger.info("%s: wrote %d rows x %d cols", sheet_name, len(df), len(df.columns))
    except gspread.exceptions.APIError as e:
        if attempt >= MAX_SHEET_RETRIES:
            logger.error("Giving up writing %s after %d attempts: %s",
                         sheet_name, attempt, e)
            raise
        wait = 5 * attempt
        logger.warning("API error writing %s (attempt %d/%d), retrying in %ss: %s",
                       sheet_name, attempt, MAX_SHEET_RETRIES, wait, e)
        time.sleep(wait)
        return update_sheet_with_results(sheet_name, df, attempt + 1)


# ── execution ──────────────────────────────────────────────────────────────

# Never paste the key here. load_openai_key() reads it from the Colab secrets
# manager, so nothing sensitive is stored in the notebook itself.
# gc is passed so the specs sheet is read with your credentials (works even if
# it is private). Without it, it falls back to the public CSV export.
# OVERWRITE = True  -> re-score every row and replace the v2 columns. Use this when
#                      iterating on the prompt; you no longer need to clear columns.
# OVERWRITE = False -> resume. Rows that already hold a v2 verdict are skipped, so an
#                      interrupted run can be picked up without paying twice.
OVERWRITE = True

bd = VehicleProcessor(load_openai_key(), gc=gc, overwrite=OVERWRITE)

for sheet_name in WORKSHEET_NAMES:
    print(f"\n=== {sheet_name} ===")
    df = get_data_from_sheet(sheet_name)
    if df.empty:
        continue
    # NOTE: the worksheet name is now required — it selects the target vehicle
    # and its spec block for the prompt. overwrite can also be set per tab here.
    df = bd.run_async_processing(df, sheet_name)
    update_sheet_with_results(sheet_name, df)

print("\nProcessing complete. v2 columns written alongside v1 in each sheet.")
