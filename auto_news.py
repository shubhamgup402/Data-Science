import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
import time
from langdetect import detect
from googletrans import Translator
import os
from sqlalchemy import create_engine, String, Date
from dotenv import load_dotenv
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import random
from requests.exceptions import HTTPError

# --- Thread-local storage & Globals ---
thread_local = threading.local()
print_lock = threading.Lock()  # For synchronized console output

# --- Database Engine Setup (Shared) ---
_shared_db_engine = None
_engine_init_lock = threading.Lock()

# Replace DATABASE_URL_FALLBACK with your Supabase connection string
DATABASE_URL_FALLBACK = "postgresql://postgres:root123@db.ajknmvzwurzxfdforzyr.supabase.co:5432/postgres"

def get_shared_engine():
    """Initializes and returns a shared SQLAlchemy Engine."""
    global _shared_db_engine
    if _shared_db_engine is None:
        with _engine_init_lock:
            if _shared_db_engine is None:  # Double-check after acquiring lock
                load_dotenv()
                db_url = os.getenv("DATABASE_URL", DATABASE_URL_FALLBACK)
                if not db_url:
                    raise ValueError("DATABASE_URL is not set in .env or fallback.")
                # pool_pre_ping checks connection validity before handing it out
                # pool_recycle prevents connections from becoming too old
                _shared_db_engine = create_engine(
                    db_url,
                    pool_recycle=3600,
                    pool_pre_ping=True
                )
    return _shared_db_engine

# --- Translator Setup (Thread-Local) ---
def get_translator():
    """Get a thread-local translator instance."""
    if not hasattr(thread_local, "translator"):
        thread_local.translator = Translator()
    return thread_local.translator

# --- Global Rate Limiting for Requests ---
global_request_lock = threading.Lock()
# Initialize with a time in the past to allow the first request immediately
global_last_request_time = time.time() - 5  # Allow first request

# Adjust these if you want to be more conservative
MIN_REQUEST_INTERVAL = 5.0  # seconds between each Google News page
RANDOM_JITTER_MAX = 2.0     # seconds of random jitter

def wait_for_google_news_request_slot():
    """Enforces a global rate limit for Google News requests."""
    global global_last_request_time
    with global_request_lock:
        while True:
            current_time = time.time()
            elapsed_since_last = current_time - global_last_request_time

            if elapsed_since_last >= MIN_REQUEST_INTERVAL:
                # Slot is available
                global_last_request_time = current_time + random.uniform(0, RANDOM_JITTER_MAX)
                break
            else:
                # Calculate remaining wait time + small jitter
                wait_needed = MIN_REQUEST_INTERVAL - elapsed_since_last
                sleep_duration = wait_needed + random.uniform(0, RANDOM_JITTER_MAX / 2)
                time.sleep(sleep_duration)

# --- Safe GET to handle 429 backoff ---
def safe_get(url, headers, timeout=10, max_retries=2):
    """
    Wrapper around requests.get that:
      - If it gets a 429, sleeps for 30 minutes and then retries once.
      - Returns None if it ultimately fails.
    """
    attempt = 0
    while attempt < max_retries:
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            resp.raise_for_status()
            return resp
        except HTTPError as he:
            status = he.response.status_code if he.response else None
            if status == 429:
                wait_secs = 30 * 60  # 30 minutes
                with print_lock:
                    print(f" Received 429 for URL {url}. Sleeping {wait_secs//60} minutes…")
                time.sleep(wait_secs)
                attempt += 1
                continue
            else:
                # Re-raise any non-429 HTTP error
                raise
        except requests.RequestException as e:
            with print_lock:
                print(f" Network error fetching URL {url}: {e}")
            return None

    with print_lock:
        print(" Max retries hit for URL:", url)
    return None

# --- Database Operations ---
def store_news_in_db(news_data, company, engine):
    """Store news articles in the database using the shared engine."""
    if not news_data:
        return 0

    records = []
    for item in news_data:
        record = {
            "company": company,
            "date": datetime.strptime(item["date"], "%d %b %Y").date(),
            "title": item["title"],
            "summary": item["summary"],
            "url": item["url"],
        }
        records.append(record)

    df = pd.DataFrame(records)
    try:
        df.to_sql(
            "news",
            engine,
            if_exists="append",
            index=False,
            schema="public",
            method="multi",
            dtype={
                "company": String,
                "date": Date,
                "title": String,
                "summary": String,
                "url": String,
            },
        )
        return len(records)
    except Exception as e:
        with print_lock:
            print(f"Error storing news for {company}: {str(e)}")
        return 0

# --- Company Keywords ---
COMPANY_KEYWORDS = {
    "Adani Energy": ["adani energy", "adani energy ltd", "adani energy limited"],
    "Adani Green": ["adani green", "agel", "adani green energy", "adani green energy ltd"],
    "Adani Power": ["adani power", "adani power ltd", "apl"],
    "Asian Paints": ["asian paints", "asian paints ltd", "asianpaint"],
    "Axis Bank": ["axis bank", "axis", "axis bank ltd"],
    "Bajaj Finance": ["bajaj finance", "bajaj finance ltd", "bajfinance", "bfl"],
    "Bharti Airtel": ["bharti airtel", "airtel", "bharti airtel ltd"],
    "HCL Technologies": ["hcl", "hcl tech", "hcltech", "hcl technologies ltd"],
    "HDFC": ["hdfc", "housing development finance corporation", "hdfc bank", "hdfc bank ltd"],
    "Hindustan Unilever": ["hindustan unilever", "hul", "hindustan unilever ltd"],
    "ICICI": ["icici", "icici bank", "icici bank ltd"],
    "Infosys": ["infosys", "infy", "infosys ltd", "infosys limited"],
    "ITC": ["itc", "itc ltd", "indian tobacco company"],
    "JSW Energy": ["jsw energy", "jsw", "jsw energy ltd"],
    "Larsen & Toubro": [
        "larsen",
        "l&t",
        "lnt",
        "larsen & toubro ltd",
        "larsen and toubro",
    ],
    "Maruti Suzuki": ["maruti", "maruti suzuki", "msil", "maruti suzuki india ltd"],
    "NHPC": ["nhpc", "nhpc ltd", "national hydroelectric power corporation"],
    "NTPC Green Energy": [
        "ntpc green",
        "ntpc green energy ltd",
        "ntpc green energy limited",
    ],
    "NTPC": ["ntpc", "ntpc ltd", "national thermal power corporation"],
    "Power Grid": ["powergrid", "power grid", "pgcil", "power grid corporation"],
    "Reliance": ["reliance", "ril", "reliance industries", "reliance industries ltd"],
    "State Bank of India": ["sbi", "state bank", "state bank of india"],
    "Sun Pharmaceutical": ["sun pharma", "sunpharma", "sun pharmaceutical industries"],
    "Tata Consultancy Services": [
        "tcs",
        "tata consultancy",
        "tata consultancy services ltd",
    ],
    "Tata Power": ["tata power", "tatapower", "tata power company ltd"],
    "Titan": ["titan", "titan company", "titan company ltd"],
    "Torrent Power": ["torrent power", "torrent power ltd"],
    "UltraTech": ["ultratech", "ultratech cement", "ultratech cement ltd"],
    "Wipro": ["wipro", "wipro ltd", "wipro limited"],
}

# --- Translation and Language Detection ---
def is_english(text):
    try:
        return detect(text) == "en"
    except Exception:
        return False

def translate_text_to_english(text):
    """Translates text to English using a thread-local translator."""
    if not text or text.isspace() or is_english(text):
        return text

    translator_instance = get_translator()
    try:
        translated = translator_instance.translate(text, dest="en")
        return translated.text
    except Exception as e:
        with print_lock:
            print(f"Translation error for text '{text[:50]}...': {str(e)}. Returning original.")
        return text

# --- News Fetching Logic ---
def format_date_for_gnews(date_obj):
    return date_obj.strftime("%m/%d/%Y")

def fetch_news_for_company_date(company_name, target_date, headers, company_keywords):
    articles = []
    start_index = 0
    date_str_gnews = format_date_for_gnews(target_date)

    while True:
        wait_for_google_news_request_slot()  # Enforce global rate limit

        query = f'"{company_name}" news'
        url = (
            f"https://www.google.com/search?q={query}&tbm=nws"
            f"&tbs=cdr:1,cd_min:{date_str_gnews},cd_max:{date_str_gnews}"
            f"&hl=en&start={start_index}"
        )

        response = safe_get(url, headers, timeout=10)
        if response is None:
            break

        soup = BeautifulSoup(response.text, "html.parser")
        results = soup.find_all("div", class_="SoaBEf")
        if not results:
            break

        page_articles_found = 0
        for result in results:
            title_tag = result.find("div", class_="n0jPhd ynAwRc MBeuO nDgy9d")
            snippet_tag = result.find("div", class_="GI74Re nDgy9d")
            link_tag = result.find("a")

            if title_tag and link_tag:
                title_raw = title_tag.get_text(strip=True)
                summary_raw = snippet_tag.get_text(strip=True) if snippet_tag else "No summary available"
                article_url = link_tag.get("href")
                if article_url and not article_url.startswith("http"):
                    article_url = "https://www.google.com" + article_url

                # Translate if needed
                title_en = translate_text_to_english(title_raw)
                summary_en = translate_text_to_english(summary_raw)

                # Check relevance by keywords
                if any(keyword.lower() in title_en.lower() for keyword in company_keywords):
                    articles.append({
                        "date": target_date.strftime("%d %b %Y"),
                        "title": title_en,
                        "summary": summary_en,
                        "url": article_url,
                    })
                    page_articles_found += 1

        if page_articles_found == 0 and start_index > 0:
            break

        start_index += 10
        time.sleep(random.uniform(0.1, 0.3))

    return articles

# --- Worker Function ---
def process_task_for_company_date(company_official_name, target_date, shared_engine):
    """
    Worker function: Fetches, processes, and stores news for a single company on a single date.
    """
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )
    }
    company_keywords = COMPANY_KEYWORDS.get(company_official_name, [company_official_name.lower()])

    with print_lock:
        print(f"⚙️ Processing: {company_official_name} for date: {target_date.strftime('%Y-%m-%d')}")

    daily_articles = fetch_news_for_company_date(company_official_name, target_date, headers, company_keywords)

    num_stored = 0
    if daily_articles:
        num_stored = store_news_in_db(daily_articles, company_official_name, shared_engine)

    with print_lock:
        print(
            f"✔️ Done: {company_official_name} for {target_date.strftime('%Y-%m-%d')}. "
            f"Found: {len(daily_articles)}, Stored: {num_stored}"
        )
    return num_stored

# --- Parallel Processing Orchestration ---
def run_parallel_news_scraping(start_date, end_date, max_threads=3):
    """
    Scrapes news for all companies in parallel, with each (company, date) pair as a task.
    """
    shared_engine = get_shared_engine()
    total_articles_stored = 0

    tasks = []
    current_date = start_date
    while current_date <= end_date:
        for company_name in COMPANY_KEYWORDS.keys():
            tasks.append((company_name, current_date))
        current_date += timedelta(days=1)

    random.shuffle(tasks)  # Distribute load

    with print_lock:
        print(f"\n📊 Starting news collection for {len(COMPANY_KEYWORDS)} companies.")
        print(f"🗓️ Date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
        print(f"🔩 Total tasks to process: {len(tasks)} with {max_threads} worker threads.")

    with ThreadPoolExecutor(max_workers=max_threads) as executor:
        future_to_task = {
            executor.submit(process_task_for_company_date, company, date, shared_engine): (company, date)
            for company, date in tasks
        }

        for future in as_completed(future_to_task):
            task_company, task_date = future_to_task[future]
            try:
                stored_count = future.result()
                total_articles_stored += stored_count
            except Exception as e:
                with print_lock:
                    print(f"❌❌ CRITICAL ERROR for task ({task_company}, {task_date.strftime('%Y-%m-%d')}): {e}")

    return total_articles_stored

# === MAIN ===
if __name__ == "__main__":
    print("Parallel News Scraper and Database Storage - Improved")
    print("=" * 60)

    # Define your desired date range
    start_date = datetime(2024, 1, 1)  # Example: Jan 1, 2024
    end_date = datetime(2025, 6, 2)    # Example: Jun 2, 2025 (today)

    if end_date > datetime.today():
        end_date = datetime.today()

    with print_lock:
        print(f"\nEffective scraping range: {start_date.strftime('%d-%b-%Y')} to {end_date.strftime('%d-%b-%Y')}")
    print("=" * 60)

    # Adjust max_threads based on your system and network
    num_worker_threads = 8  # Good starting point

    total_stored = run_parallel_news_scraping(start_date, end_date, max_threads=num_worker_threads)

    print("\n Scraping Process Complete")
    print("=" * 60)
    print(f"Total Articles Stored in Database: {total_stored}")
    print("=" * 60)
