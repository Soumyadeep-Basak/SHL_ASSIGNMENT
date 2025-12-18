import pandas as pd
import re
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

INPUT_FILE = Path(__file__).parent.parent / "data" / "raw" / "shl_catalog.csv"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "shl_catalog_enriched.csv"


def normalize_whitespace(text):
    if pd.isna(text):
        return ""
    return re.sub(r"\s+", " ", str(text)).strip()


def extract_duration_minutes(text):
    if pd.isna(text):
        return None
    m = re.search(r"(\d+)", str(text))
    return int(m.group(1)) if m else None


def normalize_boolean(v):
    if pd.isna(v):
        return "Unknown"
    v = str(v).lower()
    if v == "yes":
        return "Yes"
    if v == "no":
        return "No"
    return "Unknown"


def clean():
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows")

    if "name" in df:
        df["name"] = df["name"].apply(normalize_whitespace)

    if "description" in df:
        df["description"] = df["description"].apply(normalize_whitespace)

    if "test_type" in df:
        df["test_type"] = df["test_type"].apply(normalize_whitespace)

    if "duration" in df:
        df["duration_minutes"] = df["duration"].apply(extract_duration_minutes)
        df.drop(columns=["duration"], inplace=True)

    if "remote_testing" in df:
        df["remote_testing"] = df["remote_testing"].apply(normalize_boolean)

    if "adaptive_irt" in df:
        df["adaptive_irt"] = df["adaptive_irt"].apply(normalize_boolean)

    df = df.drop_duplicates(subset=["name"]).reset_index(drop=True)

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_FILE, index=False)

    logger.info(f"Saved cleaned CSV to {OUTPUT_FILE}")


if __name__ == "__main__":
    clean()
