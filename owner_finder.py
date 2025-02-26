import os
import time
import random
import pandas as pd
import requests
import asyncio
import aiohttp
import sys
import asyncio
import time
from tqdm import tqdm  # Import tqdm for the progress bar
from fuzzywuzzy import fuzz, process
from datetime import datetime
from bs4 import BeautifulSoup

# 🛠 Fix for Windows AsyncIO Event Loop Issues
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# File Paths
file_path = r"C:\Users\rexha\OneDrive\Documents\HVAC_and_Plumber_Aggregator\HVAC_Plumbing_Businesses.xlsx"
output_file = r"C:\Users\rexha\OneDrive\Documents\HVAC_and_Plumber_Aggregator\HVAC_Plumbing_Owners_Final.xlsx"
log_success = r"C:\Users\rexha\OneDrive\Documents\HVAC_and_Plumber_Aggregator\success_log.txt"
log_failures = r"C:\Users\rexha\OneDrive\Documents\HVAC_and_Plumber_Aggregator\failure_log.txt"
log_errors = r"C:\Users\rexha\OneDrive\Documents\HVAC_and_Plumber_Aggregator\error_log.txt"

# **Google Search API Keys (Replace with yours)**
GOOGLE_API_KEY = "AIzaSyCrbXMPzZxYZie-33v4-mIyG6yKWeBMEN4"
GOOGLE_CSE_ID = "04f960734dc1b4823"  # Your Custom Search Engine ID

# **SC SOS Business Search URL**
SC_SOS_SEARCH_URL = "https://businessfilings.sc.gov/BusinessFiling/Entity/Search"

# Load Business Data
df = pd.read_excel(file_path)

# **Logging Function**
def log_message(message, log_type="success"):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    print(f"[{timestamp}] {message}")

    log_file = {
        "success": log_success,
        "failure": log_failures,
        "error": log_errors
    }[log_type]

    with open(log_file, "a", encoding="utf-8") as f:
        f.write(log_entry)

log_message("🔍 Starting business owner lookup script...")

# **Helper Function: Advanced Fuzzy Matching**
def fuzzy_match(query, choices, threshold=70):
    query_normalized = query.lower().replace(" llc", "").replace(" inc", "").replace(" corp", "").replace(" ltd", "").strip()
    best_match = None
    highest_score = 0
    for choice in choices:
        choice_normalized = choice.lower().replace(" llc", "").replace(" inc", "").replace(" corp", "").replace(" ltd", "").strip()
        score_1 = fuzz.partial_ratio(query_normalized, choice_normalized)
        score_2 = fuzz.token_sort_ratio(query_normalized, choice_normalized)
        score_3 = fuzz.token_set_ratio(query_normalized, choice_normalized)
        weighted_score = (score_1 * 0.4) + (score_2 * 0.3) + (score_3 * 0.3)

        if weighted_score > highest_score and weighted_score >= threshold:
            highest_score = weighted_score
            best_match = choice

    return best_match, highest_score

# **1️⃣ Extract Business Owner from Website**
async def search_business_website(session, business_website):
    try:
        if not business_website.startswith("http"):
            business_website = "https://" + business_website

        log_message(f"🌍 Checking business website: {business_website}")
        async with session.get(business_website) as response:
            html = await response.text()

        soup = BeautifulSoup(html, "html.parser")
        potential_links = ["about", "team", "leadership", "company", "who-we-are"]
        for link in soup.find_all("a", href=True):
            href = link["href"].lower()
            if any(keyword in href for keyword in potential_links):
                log_message(f"📖 Found About Page: {href}")
                return href

        return "Not Found"

    except Exception as e:
        log_message(f"⚠️ Website Scraping Error: {e}", "error")
        return "Error"

# **2️⃣ Google Custom Search API for Owner Lookup (With Adaptive Rate Limiting)**
async def search_google(session, business_name, retries=5):
    base_delay = 10  # Start with a 10-second delay

    for attempt in range(retries):
        try:
            search_query = f"Owner of {business_name}"
            google_url = f"https://www.googleapis.com/customsearch/v1?q={search_query}&key={GOOGLE_API_KEY}&cx={GOOGLE_CSE_ID}"
            
            async with session.get(google_url) as response:
                data = await response.json()
                
            if "error" in data:
                error_message = data["error"].get("message", "Unknown error")
                log_message(f"❌ Google API Error: {error_message}", "error")

                if "API key not valid" in error_message:
                    log_message("❌ Fix: Verify that your API key & CSE ID are correct.", "error")
                    return "API Key Error"

                if "quotaExceeded" in error_message:
                    log_message(f"⏳ Quota exceeded. Waiting {base_delay} seconds before retrying...", "error")
                    time.sleep(base_delay)  # Wait before retrying
                    base_delay *= 2  # Double the wait time (Exponential Backoff)
                    continue  # Retry request
                
                return "API Error"

            if "items" in data and len(data["items"]) > 0:
                for item in data["items"]:
                    if "snippet" in item:
                        owner_info = item["snippet"]
                        log_message(f"✅ Google Found: {owner_info}")
                        return owner_info

                return "No Owner Info Found"

            return "Not Found"

        except Exception as e:
            log_message(f"⚠️ Google API Request Error: {e}", "error")
            time.sleep(base_delay)  # Wait before retrying
            base_delay *= 2
            continue

    return "Error"

# **3️⃣ Improved Search BBB for Owner Name**
async def search_bbb(session, business_name):
    try:
        search_url = f"https://www.bbb.org/search?find_text={business_name.replace(' ', '+')}"
        log_message(f"🔎 Searching BBB: {business_name}")

        # Fetch the BBB search results page
        async with session.get(search_url, headers={"User-Agent": "Mozilla/5.0"}) as response:
            soup = BeautifulSoup(await response.text(), "html.parser")

        # Find the first business profile link
        business_link = None
        for link in soup.find_all("a", href=True):
            if "/us/" in link["href"] and "profile" in link["href"]:
                business_link = "https://www.bbb.org" + link["href"]
                break  # Stop at the first valid profile link

        if not business_link:
            log_message(f"⚠️ No BBB profile found for {business_name}", "failure")
            return "Not Found"

        log_message(f"📄 Found BBB profile: {business_link}")

        # Fetch BBB profile page
        async with session.get(business_link, headers={"User-Agent": "Mozilla/5.0"}) as response:
            profile_soup = BeautifulSoup(await response.text(), "html.parser")

        # **Check 'Business Management' section**
        owner_name = "Not Found"
        management_section = profile_soup.find("h2", string="Business Management")
        if management_section:
            owner_details = management_section.find_next("p")  # Usually the owner name
            if owner_details:
                owner_name = owner_details.get_text(strip=True)

        # **Check 'Principal Contacts' section (Fallback if Business Management is missing)**
        if owner_name == "Not Found":
            principal_contacts_section = profile_soup.find("h2", string="Principal Contacts")
            if principal_contacts_section:
                owner_details = principal_contacts_section.find_next("p")
                if owner_details:
                    owner_name = owner_details.get_text(strip=True)

        log_message(f"✅ BBB Found: {owner_name}")
        return owner_name

    except Exception as e:
        log_message(f"⚠️ BBB Search Error: {e}", "error")
        return "Error"

# **Parallel Execution Using AsyncIO**
async def process_business(row):
    async with aiohttp.ClientSession() as session:
        business_name = row["Name"]
        website = row["Website"] if pd.notna(row["Website"]) else None

        website_owner = await search_business_website(session, website) if website else "No Website"
        google_owner = await search_google(session, business_name)
        bbb_owner = await search_bbb(session, business_name)

        return [website_owner, google_owner, bbb_owner]

async def main():
    tasks = []
    async with aiohttp.ClientSession() as session:
        # Initialize progress bar (keeps it at the bottom after completion)
        progress_bar = tqdm(total=len(df), desc="🔄 Progress", unit="business", position=0, leave=True)

        for _, row in df.iterrows():
            tasks.append(process_business(row))

        results = await asyncio.gather(*tasks)

        # Ensure progress bar stays visible even after completion
        for _ in range(len(df)):
            progress_bar.update(1)

        tqdm.write("\n✅ Scraping completed. Results saved to output file.\n")

    # Ensure DataFrame update matches results structure
    df[["Website Owner", "Google Owner", "BBB Owner"]] = results
    df.to_excel(output_file, index=False)
    log_message(f"✅ Scraping completed. Results saved to {output_file}")

    # Prevents tqdm from removing the progress bar
    while True:
        time.sleep(60)  # Keeps the script running to keep the bar visible

if __name__ == "__main__":
    asyncio.run(main())
