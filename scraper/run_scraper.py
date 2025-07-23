# scraper/run_scraper.py
import requests
from bs4 import BeautifulSoup
import json
import re
import logging
import csv
import os
from datetime import datetime

BASE_URL = "http://books.toscrape.com/catalogue/"
START_URL = "http://books.toscrape.com/catalogue/page-1.html"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_price(raw_price):
    return float(raw_price.strip().lstrip('£').replace(',', ''))

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
        url = f"http://books.toscrape.com/catalogue/page-{page}.html"
        res = requests.get(url)
        res.encoding = 'utf-8'
        if res.status_code != 200:
            break

        soup = BeautifulSoup(res.text, 'html.parser')
        book_tags = soup.select("article.product_pod")

        if not book_tags:
            break

        for book in book_tags:
            title = book.h3.a["title"]
            rel_url = book.h3.a["href"]
            book_url = BASE_URL + rel_url

            detail_res = requests.get(book_url)
            detail_res.encoding = 'utf-8'
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

if __name__ == "__main__":
    books = scrap_books()

    # Preview top 3
    print("\n🔍 Preview of scraped books:")
    for book in books[:3]:
        print(json.dumps(book, indent=2, ensure_ascii=False))
        print("-" * 40)

    # Save results
    save_json(books)
    save_csv(books)
