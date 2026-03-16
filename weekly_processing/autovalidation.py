import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

import json
import asyncio
import logging
import os

import aiohttp
import nest_asyncio
import pandas as pd
from dotenv import load_dotenv
from tqdm.asyncio import tqdm

from auth import get_creds
from google_sheet_processor import GoogleSheetProcessor, extract_file_id

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

OUTPUT_COLS = ["validation_auto", "confidence", "reasoning", "is_malfunction_auto", "model_comparison_auto"]

EMPTY_RESULT = {
    "validation_auto": "", "confidence": "", "reasoning": "",
    "is_malfunction_auto": "", "model_comparison_auto": "",
}

PROMPT_TEMPLATE = """
Analyze the following verbatim text and return a JSON object with exactly 5 fields.

{text}
---

### FIELD 1 — validation_auto
Classify into "Hit", "Miss", or "Maybe".

Hit (85-100 confidence): Text reflects personal ownership, firsthand experience, or direct interaction with the product.
  - Explicit ownership or use: "I just got my car X and drove it 500km."
  - Firsthand experience: "My infotainment screen froze yesterday."
  - Personal problem/solution: "I had a tire blowout, roadside assistance was quick."
  - Purchase decision from personal testing: "I test drove both and found the 80 much better."
  - Personal context in questions: "I noticed my trip counter resets. Is this normal?"

Miss (85-100 confidence): Does NOT reflect real-world personal experience.
  - General buying advice or speculation without firsthand basis
  - Secondhand reports, market trends, or hearsay: "I read that charging speed is slower."
  - Purely technical explanations or facts with no personal tie-in
  - Hypothetical scenarios: "What if the range is worse in winter?"

Maybe (confidence <85): Conflicting indicators that prevent a clear decision.
  - General praise/criticism without describing a personal experience
  - Feature/spec discussion without personal use: "Does the car support wireless CarPlay?"

---

### FIELD 2 — confidence
Integer 0-100 reflecting certainty in the validation_auto classification.

---

### FIELD 3 — reasoning
Brief explanation of why the classification was made.

---

### FIELD 4 — is_malfunction_auto
Boolean (true/false). Indicates whether the verbatim describes a confirmed malfunction or broken feature.

Stay SKEPTICAL — only mark true when malfunction is clearly confirmed:
  TRUE only when:
    - The speaker explicitly states something is broken, not working, failed, or malfunctioning.
    - A concrete defect is described from firsthand experience (e.g. "my screen froze and never recovered", "the door handle stopped working").
    - A repair, service visit, or warranty claim is mentioned due to a defect.
  FALSE when:
    - The issue was resolved (e.g. "it froze but a reboot fixed it") — resolved issues are NOT malfunctions.
    - The speaker is speculating, asking hypothetically, or describing something they heard.
    - It is a software quirk, preference, or minor annoyance that is not a break/failure.
    - There is no mention of any defect or problem at all.
    - The speaker is unsure if it is actually broken.

---

### FIELD 5 — model_comparison_auto
Boolean (true/false). Indicates whether the verbatim contains a comparison with a previous or different model, or signals the owner is coming from a previous model.

  TRUE when:
    - The speaker explicitly compares this vehicle to a previous model they owned or drove (e.g. "compared to my old Model 3", "coming from a Model Y", "my previous car had better range").
    - The speaker mentions switching from or upgrading from another model or brand.
    - The speaker directly contrasts features, performance, or experience between two models.
  FALSE when:
    - No other model name or prior vehicle is mentioned.
    - A model name is mentioned only as a general reference with no comparison or ownership signal.

---

### Examples

Input: "I test drove the car X and found the suspension too stiff."
Output: {{"validation_auto": "Hit", "confidence": 92, "reasoning": "Direct firsthand test drive experience described.", "is_malfunction_auto": false, "model_comparison_auto": false}}

Input: "My door handle completely stopped working. Dealer confirmed it needs replacement."
Output: {{"validation_auto": "Hit", "confidence": 97, "reasoning": "Confirmed hardware failure with dealer involvement.", "is_malfunction_auto": true, "model_comparison_auto": false}}

Input: "Coming from a Model 3, the acceleration on the X is insane."
Output: {{"validation_auto": "Hit", "confidence": 95, "reasoning": "Firsthand experience with explicit prior model comparison.", "is_malfunction_auto": false, "model_comparison_auto": true}}

Input: "My screen froze yesterday but rebooting fixed it completely."
Output: {{"validation_auto": "Hit", "confidence": 90, "reasoning": "Personal experience described; issue was self-resolved.", "is_malfunction_auto": false, "model_comparison_auto": false}}

Input: "I read that the charging speed is slower than expected."
Output: {{"validation_auto": "Miss", "confidence": 88, "reasoning": "Secondhand information, no personal experience.", "is_malfunction_auto": false, "model_comparison_auto": false}}

---
Output ONLY a valid JSON object with exactly these 5 keys: validation_auto, confidence, reasoning, is_malfunction_auto, model_comparison_auto.
"""


class LLMBase:
    def __init__(self, openai_api_key, max_concurrent_requests=10, retries=2, timeout=60):
        self.headers = {
            "Authorization": f"Bearer {openai_api_key}",
            "Content-Type": "application/json",
        }
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.retries = retries
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        nest_asyncio.apply()

    async def async_request(self, prompt):
        payload = {
            "model": "gpt-4o-mini",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 5000,
            "temperature": 0,
            "response_format": {"type": "json_object"},
        }

        for attempt in range(self.retries + 1):
            try:
                async with self.semaphore:
                    async with aiohttp.ClientSession(timeout=self.timeout) as session:
                        async with session.post(
                            "https://api.openai.com/v1/chat/completions",
                            headers=self.headers,
                            json=payload,
                        ) as response:
                            if response.status == 200:
                                result = await response.json()
                                return json.loads(result["choices"][0]["message"]["content"])
                            else:
                                error = await response.text()
                                logger.error(f"Error: {error}")
                                response.raise_for_status()
            except Exception as e:
                if attempt < self.retries:
                    logger.warning(f"Request failed (attempt {attempt + 1}/{self.retries}), retrying... Error: {e}")
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"Request failed after {self.retries} attempts. Error: {e}")
                    return None

    async def process_dataframe(self, df):
        for col in OUTPUT_COLS:
            if col not in df.columns:
                df[col] = ""

        async def process_row(row):
            has_input = bool(row.iloc[10])
            needs_processing = has_input and any(str(row.get(col, "")).strip() == "" for col in OUTPUT_COLS)
            if needs_processing:
                response = await self.async_request(row["prompt"])
                if response:
                    for key in OUTPUT_COLS:
                        if key not in response:
                            response[key] = ""
                    return response
                return dict(EMPTY_RESULT)
            return {col: row.get(col, "") for col in OUTPUT_COLS}

        tasks = [process_row(row) for _, row in df.iterrows()]
        results = await tqdm.gather(*tasks, desc="Processing Prompts")

        for i in range(len(results)):
            if not isinstance(results[i], dict):
                results[i] = dict(EMPTY_RESULT)
            else:
                for key in OUTPUT_COLS:
                    if key not in results[i]:
                        results[i][key] = ""

        results_df = pd.DataFrame(results).reindex(range(len(df)), fill_value="")
        df[OUTPUT_COLS] = results_df[OUTPUT_COLS]
        return df

    def run_async_processing_df(self, df):
        return asyncio.run(self.process_dataframe(df))


class VehicleProcessor(LLMBase):
    def __init__(self, openai_api_key, max_concurrent_requests=1000, retries=2, timeout=60):
        super().__init__(openai_api_key, max_concurrent_requests, retries, timeout)

    def generate_prompt(self, text):
        return PROMPT_TEMPLATE.format(text=text)

    def run_async_processing(self, df):
        df["prompt"] = df.iloc[:, 10].apply(self.generate_prompt)
        return self.run_async_processing_df(df)


def main(spreadsheet_id: str | None = None):
    """Run autovalidation on all vehicle tabs. spreadsheet_id can be passed directly
    from the pipeline or will fall back to reading recent_spreadsheet_link.txt."""
    if spreadsheet_id is None:
        with open(PROJECT_ROOT / "recent_spreadsheet_link.txt") as f:
            spreadsheet_id = extract_file_id(f.read().strip())

    creds = get_creds()
    sheet = GoogleSheetProcessor(spreadsheet_id, creds)
    processor = VehicleProcessor(OPENAI_API_KEY)

    vehicle_ids = pd.read_csv(PROJECT_ROOT / "vehicle_ids.csv")
    worksheet_names = vehicle_ids["vehicle_name"].tolist()

    for tab_name in worksheet_names:
        print(f"Processing tab: {tab_name}")
        df = sheet.read_tab(tab_name)
        if df.empty:
            print(f"  Skipping {tab_name} — tab is empty.")
            continue
        df = processor.run_async_processing(df)
        sheet.write_dataframe(tab_name, df)
        print(f"  Updated {tab_name} with autovalidation results.")

    print("Autovalidation complete.")


if __name__ == "__main__":
    main()
