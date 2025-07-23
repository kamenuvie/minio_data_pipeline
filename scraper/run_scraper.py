import requests 
from bs4 import BeautifulSoup
import json
import re
import logging
import csv
import os
from datetime import datetime
from time import sleep
from dotenv import load_dotenv
import boto3
from botocore.exceptions import BotoCoreError, NoCredentialsError

# Load .env if available
load_dotenv()

# Logging config
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Constants
BASE_URL = "http://books.toscrape.com/catalogue/"
MAX_RETRIES = 3
RETRY_WAIT = 2

# Secrets (from GitHub or .env)
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_ROOT_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")

def make_request_with_retry(url):
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            res = requests.get(url, timeout=10)
            res.encoding = 'utf-8'
            if res.status_code == 200:
                return res
            logging.warning(f"⚠️ Received {res.status_code} for {url}. Attempt {attempt}/{MAX_RETRIES}")
        except requests.RequestException as e:
            logging.warning(f"❌ Request error: {e}. Attempt {attempt}/{MAX_RETRIES}")
        sleep(RETRY_WAIT)
    logging.error(f"❌ Failed to fetch {url} after {MAX_RETRIES} attempts.")
    return None

def clean_price(raw_price):
    try:
        clean = raw_price.encode('utf-8').decode('utf-8', 'ignore')  # Removes weird encodings
        match = re.search(r'[\d.]+', clean)
        return float(match.group()) if match else 0.0
    except Exception as e:
        logging.warning(f"❗ Price cleaning failed for '{raw_price}': {e}")
        return 0.0

def clean_availability(text):
    return int(re.search(r'\d+', text).group()) if re.search(r'\d+', text) else 0

def clean_rating(tag):
    rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
    classes = tag.get("class", [])
    for cls in classes:
        if cls in rating_map:
            return rating_map[cls]
    return 0

def scrap_books():
    books = []
    page = 1
    logging.info("📘 Starting book scraping...")

    while True:
        url = f"{BASE_URL}page-{page}.html"
        res = make_request_with_retry(url)
        if not res:
            break

        soup = BeautifulSoup(res.text, 'html.parser')
        book_tags = soup.select("article.product_pod")

        if not book_tags:
            break

        for book in book_tags:
            try:
                title = book.h3.a["title"]
                rel_url = book.h3.a["href"]
                book_url = BASE_URL + rel_url

                detail_res = make_request_with_retry(book_url)
                if not detail_res:
                    continue

                detail_soup = BeautifulSoup(detail_res.text, 'html.parser')
                desc_tag = detail_soup.select_one("#product_description ~ p")
                description = desc_tag.text.strip() if desc_tag else ""

                book_data = {
                    "title": title,
                    "price": clean_price(book.select_one(".price_color").text),
                    "availability": clean_availability(book.select_one(".availability").text),
                    "rating": clean_rating(book.select_one("p.star-rating")),
                    "description": description,
                    "url": book_url
                }
                books.append(book_data)

            except Exception as e:
                logging.error(f"❌ Failed to parse book: {e}")

        logging.info(f"✅ Page {page} scraped. Total books so far: {len(books)}")
        page += 1

    logging.info(f"🎯 Scraping finished. Total books scraped: {len(books)}")
    return books

def save_json(data):
    os.makedirs("scraper/output", exist_ok=True)
    filename = f"scraper/output/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    logging.info(f"💾 JSON file saved: {filename}")
    return filename

def save_csv(data):
    os.makedirs("scraper/output", exist_ok=True)
    filename = f"scraper/output/books_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline='', encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    logging.info(f"📁 CSV file saved: {filename}")
    return filename

def upload_to_minio(file_path):
    if not (MINIO_ENDPOINT and MINIO_ACCESS_KEY and MINIO_ROOT_PASSWORD and MINIO_BUCKET_NAME):
        logging.warning("🚫 Skipping upload: Missing MinIO config.")
        return

    try:
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_ROOT_PASSWORD
        )
        filename = os.path.basename(file_path)
        s3.upload_file(file_path, MINIO_BUCKET_NAME, filename)
        logging.info(f"🚀 Uploaded {filename} to MinIO bucket '{MINIO_BUCKET_NAME}'")
    except (BotoCoreError, NoCredentialsError, Exception) as e:
        logging.error(f"❌ Failed to upload to MinIO: {e}")

if __name__ == "__main__":
    try:
        books = scrap_books()
        if not books:
            raise Exception("No books scraped.")

        print("\n🔍 Preview of scraped books:")
        for book in books[:3]:
            print(json.dumps(book, indent=2, ensure_ascii=False))
            print("-" * 40)

        json_path = save_json(books)
        csv_path = save_csv(books)

        upload_to_minio(json_path)
        upload_to_minio(csv_path)

    except Exception as e:
        logging.error(f"🔥 Unexpected error: {e}")
