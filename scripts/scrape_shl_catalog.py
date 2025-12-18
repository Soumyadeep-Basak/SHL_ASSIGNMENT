import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import re

BASE_URL = "https://www.shl.com/solutions/products/product-catalog/"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0 Safari/537.36"
}

LIST_DELAY = 1.0
DETAIL_DELAY = 1.5
MAX_RETRIES = 3


# ---------------- SESSION ---------------- #

session = requests.Session()
session.headers.update(HEADERS)


# ---------------- DETAIL PAGE ---------------- #

def fetch_assessment_details(assessment):
    url = assessment["url"]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = session.get(url, timeout=20)
            r.raise_for_status()

            soup = BeautifulSoup(r.content, "html.parser")
            description = ""
            duration = ""

            product_module = soup.find(
                "div", class_=lambda x: x and "product-catalogue" in x
            )

            if product_module:
                rows = product_module.find_all(
                    "div",
                    class_=lambda x: x and "product-catalogue-training-calendar__row" in x
                )

                for row in rows:
                    h4 = row.find("h4")
                    p = row.find("p")
                    if not h4 or not p:
                        continue

                    title = h4.text.lower()
                    text = p.text.strip()

                    if "description" in title:
                        description = text
                    elif "assessment length" in title or "duration" in title:
                        m = re.search(r'(\d+)', text)
                        if m:
                            duration = f"{m.group(1)} minutes"

            assessment["description"] = description
            assessment["duration"] = duration
            break

        except Exception as e:
            if attempt == MAX_RETRIES:
                print(f"❌ Failed after retries: {url}")
            else:
                time.sleep(2 * attempt)

    time.sleep(DETAIL_DELAY)
    return assessment


# ---------------- LIST PAGE ---------------- #

def scrape_table(table):
    assessments = []
    rows = table.find_all("tr")[1:]

    for row in rows:
        cols = row.find_all("td")
        if len(cols) < 4:
            continue

        name_tag = cols[0].find("a")
        if not name_tag:
            continue

        assessments.append({
            "name": name_tag.text.strip(),
            "url": "https://www.shl.com" + name_tag["href"],
            "description": "",
            "duration": "",
            "test_type": ", ".join(
                k.text.strip() for k in cols[3].find_all("span", class_="product-catalogue__key")
            ),
            "remote_testing": "Yes" if cols[1].find("span", class_="-yes") else "No",
            "adaptive_irt": "Yes" if cols[2].find("span", class_="-yes") else "No",
        })

    return assessments


def scrape_pages(type_param=1, max_pages=32):
    all_assessments = []

    for page in range(max_pages):
        start = page * 12
        url = f"{BASE_URL}?start={start}&type={type_param}"
        print(f"Fetching list page: start={start}")

        try:
            r = session.get(url, timeout=20)
            if r.status_code != 200:
                break

            soup = BeautifulSoup(r.content, "html.parser")
            tables = soup.find_all("table")
            if not tables:
                break

            page_assessments = scrape_table(tables[-1])
            if not page_assessments:
                break

            all_assessments.extend(page_assessments)

        except Exception:
            break

        time.sleep(LIST_DELAY)

    return all_assessments


# ---------------- MAIN ---------------- #

def scrape():
    assessments = scrape_pages()
    print(f"Found {len(assessments)} assessments. Fetching details...")

    for i, a in enumerate(assessments, 1):
        fetch_assessment_details(a)
        if i % 10 == 0:
            print(f"Progress: {i}/{len(assessments)}")

    return pd.DataFrame(assessments)


def save_to_csv(df, filename="data/raw/shl_catalog.csv"):
    df.to_csv(filename, index=False)
    print(f"Saved {len(df)} rows to {filename}")


if __name__ == "__main__":
    df = scrape()
    save_to_csv(df)
