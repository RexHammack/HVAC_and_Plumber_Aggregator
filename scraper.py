import os
import googlemaps
import pandas as pd
import time
from tqdm import tqdm  # Progress bar for tracking

# 🔒 Load API Key
GMAPS_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")

if not GMAPS_API_KEY:
    raise ValueError("🚨 Missing API Key! Set GOOGLE_PLACES_API_KEY in your environment.")

# 🔥 Initialize Google Maps Client
gmaps = googlemaps.Client(key=GMAPS_API_KEY)

# 🔍 Define Cities & Search Keywords
cities = ["Columbia, SC", "Lexington, SC", "Blythewood, SC", "Cayce, SC", "West Columbia, SC", "Irmo, SC", "Chapin, SC"]
keywords = ["HVAC contractor", "Plumbing contractor"]

# 📦 Storage for Business Data
businesses = []

# 🔥 Function to Get Place Details (Phone Number & Website)
def get_place_details(place_id):
    """ Fetches phone number & website for a given place ID. """
    try:
        details = gmaps.place(place_id=place_id, fields=["formatted_phone_number", "website"])
        return details.get("result", {})
    except Exception as e:
        print(f"⚠️ Error fetching details for place {place_id}: {e}")
        return {}

# 🔍 Fetch Businesses with Progress Tracking
total_queries = len(cities) * len(keywords)
query_count = 0  # Keep track of progress

for city in cities:
    for keyword in keywords:
        query_count += 1
        print(f"\n🔎 [{query_count}/{total_queries}] Searching for '{keyword}' in {city}...")

        # Fetch places from Google Places API
        try:
            results = gmaps.places(query=f"{keyword} in {city}")
            places = results.get("results", [])
            print(f"✅ Found {len(places)} results.")

            # Progress bar for business processing
            for place in tqdm(places, desc=f"Processing {keyword} in {city}", ncols=80):
                place_id = place.get("place_id")
                details = get_place_details(place_id) if place_id else {}

                # Store business data
                businesses.append({
                    "Name": place.get("name"),
                    "Address": place.get("formatted_address"),
                    "Category": keyword,
                    "Phone": details.get("formatted_phone_number", "N/A"),
                    "Website": details.get("website", "N/A")
                })

                # 🌍 Avoid hitting API rate limits
                time.sleep(1)

        except Exception as e:
            print(f"❌ Error fetching data for {keyword} in {city}: {e}")

# 📄 Save to Excel
output_file = "HVAC_Plumbing_Businesses.xlsx"
df = pd.DataFrame(businesses)
df.to_excel(output_file, index=False)

print(f"\n✅ Data successfully scraped and saved to {output_file}!")
