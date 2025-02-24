import os
import googlemaps
import pandas as pd
import folium

# 🔒 Secure API Key Handling - Load from Environment Variable
GMAPS_API_KEY = os.getenv("GOOGLE_PLACES_API_KEY")  

if not GMAPS_API_KEY:
    raise ValueError("🚨 Missing Google Places API Key! Set GOOGLE_PLACES_API_KEY in your environment.")

# Initialize Google Maps Client
gmaps = googlemaps.Client(key=GMAPS_API_KEY)

# Load business data
file_path = "HVAC_Plumbing_Businesses.xlsx"

try:
    df = pd.read_excel(file_path)
except FileNotFoundError:
    print("🚨 Data file not found! Make sure you run the scraper first.")
    exit()

# Extract addresses
addresses = df["Address"].dropna().tolist()

# 🔥 Set Starting Location (Home or Office)
starting_location = "123 Main Street, Columbia, SC"  # Replace with your actual starting point

# 🔥 Optimize the Route Using Google Maps API
def get_optimized_route(start, destinations):
    if len(destinations) < 2:
        print("🚨 Not enough locations for route optimization!")
        return None

    # Google Maps Directions API (Optimized Route)
    directions = gmaps.directions(
        start,
        destinations[-1],  # End at last location
        waypoints=destinations[:-1],  # Stops in between
        optimize_waypoints=True,  # 🔥 Optimize route
        mode="driving"
    )

    return directions

# Get optimized route
route = get_optimized_route(starting_location, addresses)

# 🔥 [A] Extract Estimated Drive Time & Distance
route_summary = []
if route:
    for step in route[0]['legs']:
        start_address = step['start_address']
        end_address = step['end_address']
        distance = step['distance']['text']
        duration = step['duration']['text']
        route_summary.append([start_address, end_address, distance, duration])

    # Convert to DataFrame
    route_df = pd.DataFrame(route_summary, columns=["Start", "End", "Distance", "Duration"])
    print("🚗 Optimized Route Summary:\n", route_df)

    # Save to CSV for mobile app integration
    route_df.to_csv("Optimized_Route.csv", index=False)
    print("✅ Route saved to Optimized_Route.csv")

    # 🔥 [B] Show Route on an Interactive Map
    m = folium.Map(location=[34.0007, -81.0348], zoom_start=10)  # Centered in Columbia, SC

    for _, row in route_df.iterrows():
        folium.Marker(
            location=gmaps.geocode(row["Start"])[0]['geometry']['location'].values(),
            popup=f"Start: {row['Start']}\nEnd: {row['End']}\n{row['Distance']} ({row['Duration']})",
            icon=folium.Icon(color="blue")
        ).add_to(m)

    m.save("Optimized_Route_Map.html")
    print("✅ Interactive Map saved as Optimized_Route_Map.html")

    # 🔥 [C] Generate a Google Maps Link for Mobile Navigation
    google_maps_link = f"https://www.google.com/maps/dir/{starting_location.replace(' ', '+')}/" + "/".join(
        [addr.replace(" ", "+") for addr in addresses])
    print(f"📱 Open this link on your phone for navigation:\n{google_maps_link}")

else:
    print("🚨 Could not optimize route.")
