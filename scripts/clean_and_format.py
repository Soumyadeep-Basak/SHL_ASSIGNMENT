"""
Clean and Format SHL Catalog Data
==================================
Cleans raw scraped data and converts to structured CSV format.

Input: data/raw/shl_catalog_raw.json
Output: data/processed/shl_catalog_enriched.csv

Cleaning operations:
- Remove HTML tags and artifacts
- Normalize whitespace
- Standardize casing
- Remove special characters
- Handle missing values
"""

import json
import pandas as pd
import re
from pathlib import Path
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# File paths
INPUT_FILE = Path(__file__).parent.parent / "data" / "raw" / "shl_catalog_raw.json"
OUTPUT_FILE = Path(__file__).parent.parent / "data" / "processed" / "shl_catalog_enriched.csv"


def clean_html(text):
    """Remove HTML tags and decode HTML entities."""
    if not text or pd.isna(text):
        return ""
    
    text = str(text)
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    # Decode common HTML entities
    html_entities = {
        '&nbsp;': ' ',
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
        '&apos;': "'",
        '&mdash;': '—',
        '&ndash;': '–',
        '&hellip;': '...',
    }
    
    for entity, char in html_entities.items():
        text = text.replace(entity, char)
    
    return text


def normalize_whitespace(text):
    """Normalize whitespace - remove extra spaces, newlines, tabs."""
    if not text or pd.isna(text):
        return ""
    
    text = str(text)
    
    # Replace multiple whitespace with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Remove leading/trailing whitespace
    text = text.strip()
    
    return text


def clean_text_field(text):
    """Apply all text cleaning operations."""
    if not text or pd.isna(text):
        return ""
    
    # Convert to string
    text = str(text)
    
    # Remove HTML
    text = clean_html(text)
    
    # Normalize whitespace
    text = normalize_whitespace(text)
    
    # Remove markdown-style artifacts
    text = re.sub(r'\*\*', '', text)  # Remove bold markers
    text = re.sub(r'__', '', text)    # Remove underline markers
    text = re.sub(r'\[|\]', '', text)  # Remove brackets
    
    # Remove URLs from text (keep URLs in url column)
    text = re.sub(r'http[s]?://\S+', '', text)
    
    # Remove excessive punctuation
    text = re.sub(r'\.{2,}', '.', text)  # Multiple dots to single
    text = re.sub(r',{2,}', ',', text)   # Multiple commas to single
    
    # Final whitespace cleanup
    text = normalize_whitespace(text)
    
    return text


def standardize_name(name):
    """Standardize assessment names."""
    if not name or pd.isna(name):
        return ""
    
    name = clean_text_field(name)
    
    # Remove common suffixes that don't add value
    name = re.sub(r'\s*-\s*SHL$', '', name, flags=re.IGNORECASE)
    name = re.sub(r'\s*\|\s*SHL$', '', name, flags=re.IGNORECASE)
    
    return name.strip()


def extract_duration_minutes(duration_text):
    """Extract duration in minutes from text."""
    if not duration_text or pd.isna(duration_text):
        return None
    
    duration_text = str(duration_text).lower()
    
    # Try to extract number of minutes
    # Patterns: "30 minutes", "30 mins", "30min", "0:30", etc.
    
    # Pattern 1: "X minutes/mins"
    match = re.search(r'(\d+)\s*(?:minute|min)', duration_text)
    if match:
        return int(match.group(1))
    
    # Pattern 2: "X hours Y minutes"
    hour_match = re.search(r'(\d+)\s*(?:hour|hr)', duration_text)
    min_match = re.search(r'(\d+)\s*(?:minute|min)', duration_text)
    
    if hour_match:
        hours = int(hour_match.group(1))
        minutes = int(min_match.group(1)) if min_match else 0
        return hours * 60 + minutes
    
    # Pattern 3: Time format "0:30" or "1:30"
    time_match = re.search(r'(\d+):(\d+)', duration_text)
    if time_match:
        hours = int(time_match.group(1))
        minutes = int(time_match.group(2))
        return hours * 60 + minutes
    
    return None


def normalize_boolean(value):
    """Normalize boolean-like values to Yes/No/Unknown."""
    if not value or pd.isna(value):
        return "Unknown"
    
    value = str(value).lower().strip()
    
    # Positive values
    if value in ['yes', 'true', 'supported', 'available', 'enabled', '1']:
        return "Yes"
    
    # Negative values
    if value in ['no', 'false', 'not supported', 'unavailable', 'disabled', '0']:
        return "No"
    
    # Default
    return "Unknown"


def clean_and_format_data():
    """Main function to clean and format the data."""
    logger.info("=" * 70)
    logger.info("SHL Catalog Data Cleaning and Formatting")
    logger.info("=" * 70)
    
    # Load raw data
    logger.info(f"Loading raw data from: {INPUT_FILE}")
    
    if not INPUT_FILE.exists():
        logger.error(f"❌ Input file not found: {INPUT_FILE}")
        return
    
    with open(INPUT_FILE, 'r', encoding='utf-8') as f:
        raw_data = json.load(f)
    
    logger.info(f"Loaded {len(raw_data)} assessments")
    
    # Convert to DataFrame
    df = pd.DataFrame(raw_data)
    
    logger.info(f"Original columns: {list(df.columns)}")
    
    # Clean text fields
    logger.info("Cleaning text fields...")
    
    if 'name' in df.columns:
        df['name'] = df['name'].apply(standardize_name)
    
    if 'description' in df.columns:
        df['description'] = df['description'].apply(clean_text_field)
    
    if 'test_type' in df.columns:
        df['test_type'] = df['test_type'].apply(clean_text_field)
    
    # Process duration
    if 'duration' in df.columns:
        logger.info("Extracting duration in minutes...")
        df['duration_minutes'] = df['duration'].apply(extract_duration_minutes)
        df['duration_text'] = df['duration'].apply(clean_text_field)
        df = df.drop('duration', axis=1)
    
    # Normalize boolean fields
    if 'adaptive_support' in df.columns:
        df['adaptive_support'] = df['adaptive_support'].apply(normalize_boolean)
    
    if 'remote_support' in df.columns:
        df['remote_support'] = df['remote_support'].apply(normalize_boolean)
    
    # Ensure URL column is clean
    if 'url' in df.columns:
        df['url'] = df['url'].apply(lambda x: str(x).strip() if x else "")
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    # Remove duplicate assessments (by name)
    if 'name' in df.columns:
        logger.info(f"Removing duplicates based on assessment name...")
        original_count = len(df)
        df = df.drop_duplicates(subset=['name'], keep='first')
        removed = original_count - len(df)
        if removed > 0:
            logger.info(f"  Removed {removed} duplicate(s)")
    
    # Sort by name
    if 'name' in df.columns:
        df = df.sort_values('name').reset_index(drop=True)
    
    # Reorder columns for better readability
    column_order = [
        'name',
        'description',
        'test_type',
        'duration_text',
        'duration_minutes',
        'adaptive_support',
        'remote_support',
        'url'
    ]
    
    # Only include columns that exist
    available_columns = [col for col in column_order if col in df.columns]
    df = df[available_columns]
    
    # Data quality report
    logger.info("\n" + "=" * 70)
    logger.info("Data Quality Report")
    logger.info("=" * 70)
    logger.info(f"Total assessments: {len(df)}")
    
    for col in df.columns:
        non_empty = df[col].notna().sum()
        empty = len(df) - non_empty
        percentage = (non_empty / len(df)) * 100
        logger.info(f"  {col}: {non_empty}/{len(df)} filled ({percentage:.1f}%)")
    
    # Save to CSV
    logger.info("\n" + "=" * 70)
    logger.info(f"Saving cleaned data to: {OUTPUT_FILE}")
    
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_csv(OUTPUT_FILE, index=False, encoding='utf-8')
    
    logger.info(f"✅ Success! Saved {len(df)} assessments to CSV")
    logger.info("=" * 70)
    
    # Display sample
    logger.info("\nSample of cleaned data (first 3 rows):")
    print("\n" + df.head(3).to_string(index=False))


if __name__ == "__main__":
    clean_and_format_data()
