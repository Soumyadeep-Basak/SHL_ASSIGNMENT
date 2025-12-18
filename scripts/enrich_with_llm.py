"""
Enrich SHL Catalog with LLM-Generated Metadata (GROQ — ULTRA SAFE)
=================================================================
Uses Groq (llama-3.1-8b-instant) to enrich assessment descriptions.

Safety-first design:
- Small batches (3 rows)
- Long delays
- Periodic cooldowns
- Resume-safe CSV caching
- No manual tuning required

Groq Free Tier (practical):
- ~30 RPM
- ~6k tokens/min
"""

import os
import json
import time
import logging
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# ---------------- DEPENDENCIES ---------------- #

try:
    from groq import Groq
except ImportError:
    raise RuntimeError("Install groq first: pip install groq")

load_dotenv()

# ---------------- LOGGING ---------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------- FILE PATHS ---------------- #

INPUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "shl_catalog_enriched.csv"
OUTPUT_FILE = INPUT_FILE  # in-place update

# ---------------- GROQ CONFIG ---------------- #

GROQ_API_KEY = os.getenv("GROQ_API_KEY")
if not GROQ_API_KEY:
    raise RuntimeError("❌ GROQ_API_KEY not set")

MODEL_NAME = "llama-3.1-8b-instant"

# ---------------- SAFETY CONTROLS ---------------- #

BATCH_SIZE = 3              # safest size for TPM
REQUEST_DELAY = 4.0         # seconds between requests
COOLDOWN_EVERY = 10          # batches
COOLDOWN_SECONDS = 30.0     # pause to reset minute window

# ---------------- PROMPTS ---------------- #

SYSTEM_PROMPT = """You are an expert HR assessment analyst.

Rules:
- Only extract information explicitly stated or strongly implied
- Do NOT hallucinate
- Use controlled vocabulary
- Keep output compact
"""

USER_PROMPT_TEMPLATE = """
You will be given a list of assessments.
For EACH assessment, extract metadata.

Return STRICT JSON ONLY as a list, in the SAME ORDER.

Each item MUST be exactly:

{{
  "skills_covered": [],
  "skill_domains": [],
  "assessment_category": "",
  "job_roles": [],
  "seniority_levels": [],
  "assessment_focus": "",
  "keywords": []
}}

Constraints:
- assessment_category ∈ ["Technical", "Behavioral", "Cognitive", "Mixed"]
- Max 10 skills, 5 domains, 5 roles, 10 keywords
- If unclear → empty lists or empty string
- DO NOT invent skills, tools, or roles

Assessments:
{items}
"""

# ---------------- UTILITIES ---------------- #

def is_row_enriched(row):
    return (
        isinstance(row.get("skills_covered"), str)
        and row["skills_covered"].strip()
        and isinstance(row.get("assessment_category"), str)
        and row["assessment_category"].strip()
    )

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# ---------------- MAIN PIPELINE ---------------- #

def enrich_catalog_data():
    logger.info("=" * 70)
    logger.info("SHL Catalog Enrichment — GROQ (SAFE + COMPLETE)")
    logger.info("=" * 70)

    if not INPUT_FILE.exists():
        logger.error(f"File not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows")

    # Ensure columns exist
    new_cols = [
        "skills_covered", "skill_domains", "assessment_category",
        "job_roles", "seniority_levels", "assessment_focus", "keywords"
    ]
    for c in new_cols:
        if c not in df.columns:
            df[c] = ""

    client = Groq(api_key=GROQ_API_KEY)

    pending_indices = [
        i for i, row in df.iterrows()
        if not is_row_enriched(row)
    ]

    logger.info(f"Rows pending enrichment: {len(pending_indices)}")

    batches_done = 0

    for batch_idxs in chunk_list(pending_indices, BATCH_SIZE):

        payload = []
        for idx in batch_idxs:
            payload.append({
                "name": df.at[idx, "name"],
                "description": df.at[idx, "description"]
            })

        prompt = USER_PROMPT_TEMPLATE.format(
            items=json.dumps(payload, ensure_ascii=False)
        )

        logger.info(f"Processing batch {batches_done + 1} ({len(batch_idxs)} rows)")

        try:
            response = client.chat.completions.create(
                model=MODEL_NAME,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt}
                ],
                temperature=0
            )

            text = response.choices[0].message.content.strip()
            data = json.loads(text)

            if len(data) != len(batch_idxs):
                raise ValueError("Response length mismatch")

            for idx, meta in zip(batch_idxs, data):
                df.at[idx, "skills_covered"] = json.dumps(meta.get("skills_covered", []))
                df.at[idx, "skill_domains"] = json.dumps(meta.get("skill_domains", []))
                df.at[idx, "assessment_category"] = meta.get("assessment_category", "")
                df.at[idx, "job_roles"] = json.dumps(meta.get("job_roles", []))
                df.at[idx, "seniority_levels"] = json.dumps(meta.get("seniority_levels", []))
                df.at[idx, "assessment_focus"] = meta.get("assessment_focus", "")
                df.at[idx, "keywords"] = json.dumps(meta.get("keywords", []))

            df.to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
            logger.info("✅ Batch saved successfully")

        except Exception as e:
            logger.error(f"❌ Batch failed: {e}")

        batches_done += 1

        # Cooldown every N batches
        if batches_done % COOLDOWN_EVERY == 0:
            logger.info(f"⏸ Cooling down for {COOLDOWN_SECONDS}s")
            time.sleep(COOLDOWN_SECONDS)
        else:
            time.sleep(REQUEST_DELAY)

    logger.info("=" * 70)
    logger.info("DONE — All possible rows processed")
    logger.info(f"Batches processed: {batches_done}")
    logger.info("=" * 70)

# ---------------- ENTRY ---------------- #

if __name__ == "__main__":
    enrich_catalog_data()