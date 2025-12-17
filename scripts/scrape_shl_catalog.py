import pandas as pd
import random
import time
import os
from playwright.sync_api import sync_playwright

INPUT_CSV = r"c:\Users\safal\Desktop\shl_assignment\shl_catalogue.csv"
OUTPUT_DIR = r"c:\Users\safal\Desktop\shl_assignment\data\raw"
OUTPUT_CSV = os.path.join(OUTPUT_DIR, "shl_catalogue.csv")


def scrape_description(page, url):
    try:
        print(f"Visiting: {url}")
        page.goto(url, timeout=60000, wait_until="domcontentloaded")

        time.sleep(1)

        description = ""

        content_selectors = [
            ".product-description",
            ".catalog-product-view__description",
            "div[itemprop='description']",
            ".product-detail__description"
        ]

        for selector in content_selectors:
            if page.locator(selector).count() > 0:
                description = page.locator(selector).inner_text()
                break

        if not description:
            headers = page.locator("h2, h3, h4, strong").filter(has_text="Description")
            for i in range(headers.count()):
                header = headers.nth(i)
                text = header.inner_text().strip()

                if text.lower() == "description":
                    next_sibling = header.locator("xpath=following-sibling::*[1]")
                    if next_sibling.count() > 0:
                        candidate = next_sibling.inner_text().strip()
                        if len(candidate) > 10:
                            description = candidate
                            break

                    parent = header.locator("xpath=..")
                    candidate = parent.inner_text().replace("Description", "", 1).strip()
                    if len(candidate) > 10:
                        description = candidate
                        break

        return description.strip()

    except Exception as e:
        print(f"Error scraping {url}: {e}")
        return ""


def main():
    if os.path.exists(OUTPUT_CSV):
        print(f"Resuming from {OUTPUT_CSV}")
        df = pd.read_csv(OUTPUT_CSV)
    elif os.path.exists(INPUT_CSV):
        print(f"Starting fresh from {INPUT_CSV}")
        df = pd.read_csv(INPUT_CSV)
    else:
        print(f"Input file not found: {INPUT_CSV}")
        return

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"Loaded {len(df)} rows")

    if 'description' not in df.columns:
        df['description'] = ""

    df['description'] = df['description'].fillna("").astype(str)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = context.new_page()

        for index, row in df.iterrows():
            url = row.get('assessment_url')

            if not isinstance(url, str) or not url.startswith("http"):
                continue

            if df.at[index, 'description'].strip():
                continue

            desc = scrape_description(page, url)

            if desc.startswith("Description"):
                desc = desc[11:].strip()

            df.at[index, 'description'] = desc

            if index % 10 == 0:
                df.to_csv(OUTPUT_CSV, index=False)
                print(f"Progress saved at index {index}")

            time.sleep(random.uniform(1.0, 2.0))

        df.to_csv(OUTPUT_CSV, index=False)
        print(f"Completed. Saved to {OUTPUT_CSV}")

        browser.close()


if __name__ == "__main__":
    main()
