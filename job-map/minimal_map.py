#!/usr/bin/env python3
"""
Script to filter jobs to only include specific AI companies and add lon, lat coordinates using a hardcoded map.
Handles office-specific locations (e.g., "San Francisco Office") and saves unmatched entries to offices.txt.
"""

import pandas as pd
import re
from pathlib import Path

# Hardcoded coordinates map (based on cityCoordinates.ts and jobs_map.csv)
LOCATION_COORDINATES = {
    # United States - Major Cities
    "San Francisco, California, United States": (37.7749, -122.4194),
    "San Francisco, CA, United States": (37.7749, -122.4194),
    "San Francisco": (37.7749, -122.4194),
    "New York, New York, United States": (40.7128, -74.006),
    "New York, NY, United States": (40.7128, -74.006),
    "New York": (40.7128, -74.006),
    "Los Angeles, California, United States": (34.0522, -118.2437),
    "Los Angeles, CA, United States": (34.0522, -118.2437),
    "Los Angeles": (34.0522, -118.2437),
    "Chicago, Illinois, United States": (41.8781, -87.6298),
    "Chicago, IL, United States": (41.8781, -87.6298),
    "Chicago": (41.8781, -87.6298),
    "Seattle, Washington, United States": (47.6062, -122.3321),
    "Seattle, WA, United States": (47.6062, -122.3321),
    "Seattle": (47.6062, -122.3321),
    "Austin, Texas, United States": (30.2672, -97.7431),
    "Austin, TX, United States": (30.2672, -97.7431),
    "Austin": (30.2672, -97.7431),
    "Boston, Massachusetts, United States": (42.3601, -71.0589),
    "Boston, MA, United States": (42.3601, -71.0589),
    "Boston": (42.3601, -71.0589),
    "Denver, Colorado, United States": (39.7392, -104.9903),
    "Denver, CO, United States": (39.7392, -104.9903),
    "Denver": (39.7392, -104.9903),
    "Washington, District of Columbia, United States": (38.9072, -77.0369),
    "Washington, DC, United States": (38.9072, -77.0369),
    "Washington": (38.9072, -77.0369),
    "Miami, Florida, United States": (25.7617, -80.1918),
    "Miami, FL, United States": (25.7617, -80.1918),
    "Miami": (25.7617, -80.1918),
    "Portland, Oregon, United States": (45.5152, -122.6784),
    "Portland, OR, United States": (45.5152, -122.6784),
    "Portland": (45.5152, -122.6784),
    "Atlanta, Georgia, United States": (33.749, -84.388),
    "Atlanta, GA, United States": (33.749, -84.388),
    "Atlanta": (33.749, -84.388),
    "Dallas, Texas, United States": (32.7767, -96.797),
    "Dallas, TX, United States": (32.7767, -96.797),
    "Dallas": (32.7767, -96.797),
    "Phoenix, Arizona, United States": (33.4484, -112.074),
    "Phoenix, AZ, United States": (33.4484, -112.074),
    "Phoenix": (33.4484, -112.074),
    "San Diego, California, United States": (32.7157, -117.1611),
    "San Diego, CA, United States": (32.7157, -117.1611),
    "San Diego": (32.7157, -117.1611),
    "Philadelphia, Pennsylvania, United States": (39.9526, -75.1652),
    "Philadelphia, PA, United States": (39.9526, -75.1652),
    "Philadelphia": (39.9526, -75.1652),
    "Palo Alto, California, United States": (37.4419, -122.1430),
    "Palo Alto, CA, United States": (37.4419, -122.1430),
    "Palo Alto": (37.4419, -122.1430),
    "Mountain View, California, United States": (37.3861, -122.0839),
    "Mountain View, CA, United States": (37.3861, -122.0839),
    "Mountain View": (37.3861, -122.0839),
    "Novato, California, United States": (38.1074, -122.5697),
    "Novato, CA, United States": (38.1074, -122.5697),
    "Novato": (38.1074, -122.5697),
    "Sparks Glencoe, Maryland, United States": (39.5401, -76.6447),
    "Sparks Glencoe, MD, United States": (39.5401, -76.6447),
    "Moorpark, California, United States": (34.2856, -118.8820),
    "Moorpark, CA, United States": (34.2856, -118.8820),
    # Canada
    "Toronto, Ontario, Canada": (43.6532, -79.3832),
    "Toronto": (43.6532, -79.3832),
    "Vancouver, British Columbia, Canada": (49.2827, -123.1207),
    "Vancouver": (49.2827, -123.1207),
    "Montréal, Quebec, Canada": (45.5017, -73.5673),
    "Montreal, Quebec, Canada": (45.5017, -73.5673),
    "Montreal": (45.5017, -73.5673),
    "Calgary, Alberta, Canada": (51.0447, -114.0719),
    "Calgary": (51.0447, -114.0719),
    "Ottawa, Ontario, Canada": (45.4215, -75.6972),
    "Ottawa": (45.4215, -75.6972),
    "Nova Scotia, Canada": (44.6820, -63.7443),
    "Quebec, Canada": (46.8139, -71.2080),
    # United Kingdom
    "London, England, United Kingdom": (51.5074, -0.1278),
    "London, United Kingdom": (51.5074, -0.1278),
    "London": (51.5074, -0.1278),
    "Manchester, England, United Kingdom": (53.4808, -2.2426),
    "Manchester": (53.4808, -2.2426),
    "Edinburgh, Scotland, United Kingdom": (55.9533, -3.1883),
    "Edinburgh": (55.9533, -3.1883),
    "Birmingham, England, United Kingdom": (52.4862, -1.8904),
    "Birmingham": (52.4862, -1.8904),
    # Europe
    "Berlin, Germany": (52.52, 13.405),
    "Berlin": (52.52, 13.405),
    "Paris, France": (48.8566, 2.3522),
    "Paris": (48.8566, 2.3522),
    "Amsterdam, Netherlands": (52.3676, 4.9041),
    "Amsterdam": (52.3676, 4.9041),
    "Barcelona, Spain": (41.3851, 2.1734),
    "Barcelona": (41.3851, 2.1734),
    "Madrid, Spain": (40.4168, -3.7038),
    "Madrid": (40.4168, -3.7038),
    "Rome, Italy": (41.9028, 12.4964),
    "Rome": (41.9028, 12.4964),
    "Milan, Italy": (45.4642, 9.19),
    "Milan": (45.4642, 9.19),
    "Vienna, Austria": (48.2082, 16.3738),
    "Vienna": (48.2082, 16.3738),
    "Zurich, Switzerland": (47.3769, 8.5417),
    "Zurich": (47.3769, 8.5417),
    "Stockholm, Sweden": (59.3293, 18.0686),
    "Stockholm": (59.3293, 18.0686),
    "Copenhagen, Denmark": (55.6761, 12.5683),
    "Copenhagen": (55.6761, 12.5683),
    "Dublin, Ireland": (53.3498, -6.2603),
    "Dublin": (53.3498, -6.2603),
    "Brussels, Belgium": (50.8503, 4.3517),
    "Brussels": (50.8503, 4.3517),
    "Lisbon, Portugal": (38.7223, -9.1393),
    "Lisbon": (38.7223, -9.1393),
    "Prague, Czech Republic": (50.0755, 14.4378),
    "Prague": (50.0755, 14.4378),
    "Warsaw, Poland": (52.2297, 21.0122),
    "Warsaw": (52.2297, 21.0122),
    "Munich, Germany": (48.1351, 11.5820),
    "Munich": (48.1351, 11.5820),
    "Luxembourg": (49.6116, 6.1319),
    "Luxembourg, Luxembourg": (49.6116, 6.1319),
    # Asia-Pacific
    "Singapore, Singapore": (1.3521, 103.8198),
    "Singapore": (1.3521, 103.8198),
    "Tokyo, Japan": (35.6762, 139.6503),
    "Tokyo": (35.6762, 139.6503),
    "Seoul, Korea": (37.5665, 126.978),
    "Seoul, South Korea": (37.5665, 126.978),
    "Seoul": (37.5665, 126.978),
    "Hong Kong, Hong Kong": (22.3193, 114.1694),
    "Hong Kong": (22.3193, 114.1694),
    "Shanghai, China": (31.2304, 121.4737),
    "Shanghai": (31.2304, 121.4737),
    "Beijing, China": (39.9042, 116.4074),
    "Beijing": (39.9042, 116.4074),
    "Bangalore, India": (12.9716, 77.5946),
    "Bangalore": (12.9716, 77.5946),
    "Mumbai, India": (19.076, 72.8777),
    "Mumbai": (19.076, 72.8777),
    "Delhi, India": (28.7041, 77.1025),
    "Delhi": (28.7041, 77.1025),
    "Sydney, Australia": (-33.8688, 151.2093),
    "Sydney": (-33.8688, 151.2093),
    "Melbourne, Australia": (-37.8136, 144.9631),
    "Melbourne": (-37.8136, 144.9631),
    "Auckland, New Zealand": (-36.8485, 174.7633),
    "Auckland": (-36.8485, 174.7633),
    # Latin America
    "São Paulo, Brazil": (-23.5505, -46.6333),
    "São Paulo": (-23.5505, -46.6333),
    "Mexico City, Mexico": (19.4326, -99.1332),
    "Mexico City": (19.4326, -99.1332),
    "Buenos Aires, Argentina": (-34.6037, -58.3816),
    "Buenos Aires": (-34.6037, -58.3816),
    "Argentina": (-34.6037, -58.3816),
    "Bogotá, Colombia": (4.711, -74.0721),
    "Bogotá": (4.711, -74.0721),
    "Santiago, Chile": (-33.4489, -70.6693),
    "Santiago": (-33.4489, -70.6693),
    "Casablanca, Morocco": (33.5731, -7.5898),
    "Casablanca": (33.5731, -7.5898),
    # Middle East
    "Dubai, United Arab Emirates": (25.2048, 55.2708),
    "Dubai": (25.2048, 55.2708),
    "Tel Aviv, Israel": (32.0853, 34.7818),
    "Tel Aviv": (32.0853, 34.7818),
    # Africa
    "Cape Town, South Africa": (-33.9249, 18.4241),
    "Cape Town": (-33.9249, 18.4241),
    "Johannesburg, South Africa": (-26.2041, 28.0473),
    "Johannesburg": (-26.2041, 28.0473),
    "Lagos, Nigeria": (6.5244, 3.3792),
    "Lagos": (6.5244, 3.3792),
    "Cairo, Egypt": (30.0444, 31.2357),
    "Cairo": (30.0444, 31.2357),
    # Additional cities for office locations
    "Pune, India": (18.5204, 73.8567),
    "Pune": (18.5204, 73.8567),
    "Bengaluru, India": (12.9716, 77.5946),
    "Bengaluru": (12.9716, 77.5946),
    "Bengaluru, Karnataka, India": (12.9716, 77.5946),
    "Bengaluru, Karnataka": (12.9716, 77.5946),
    "Hyderabad, India": (17.3850, 78.4867),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai, India": (13.0827, 80.2707),
    "Chennai": (13.0827, 80.2707),
    "Taipei, Taiwan": (25.0330, 121.5654),
    "Taipei": (25.0330, 121.5654),
    "Bangkok, Thailand": (13.7563, 100.5018),
    "Bangkok": (13.7563, 100.5018),
    "Guangzhou, China": (23.1291, 113.2644),
    "Guangzhou": (23.1291, 113.2644),
    "Shenzhen, China": (22.5431, 114.0579),
    "Shenzhen": (22.5431, 114.0579),
    "Oakland, California, United States": (37.8044, -122.2711),
    "Oakland, CA, United States": (37.8044, -122.2711),
    "Oakland": (37.8044, -122.2711),
    "Santa Clara, California, United States": (37.3541, -121.9552),
    "Santa Clara, CA, United States": (37.3541, -121.9552),
    "Santa Clara": (37.3541, -121.9552),
    "Redwood City, California, United States": (37.4852, -122.2364),
    "Redwood City, CA, United States": (37.4852, -122.2364),
    "Redwood City": (37.4852, -122.2364),
    "Bristol, England, United Kingdom": (51.4545, -2.5879),
    "Bristol": (51.4545, -2.5879),
    "Tampa, Florida, United States": (27.9506, -82.4572),
    "Tampa, FL, United States": (27.9506, -82.4572),
    "Tampa": (27.9506, -82.4572),
    "Manila, Philippines": (14.5995, 120.9842),
    "Manila": (14.5995, 120.9842),
    "Kyiv, Ukraine": (50.4501, 30.5234),
    "Kyiv": (50.4501, 30.5234),
    "Kiev, Ukraine": (50.4501, 30.5234),
    "Kiev": (50.4501, 30.5234),
    "Belgrade, Serbia": (44.7866, 20.4489),
    "Belgrade": (44.7866, 20.4489),
    "Riyadh, Saudi Arabia": (24.7136, 46.6753),
    "Riyadh": (24.7136, 46.6753),
    "Sao Paulo, Brazil": (
        -23.5505,
        -46.6333,
    ),  # Note: "São Paulo, Brazil" already defined above
    "Sao Paulo": (-23.5505, -46.6333),
    # Special/Regional locations (handles remote and regional locations)
    "Remote": (39.8283, -98.5795),  # Geographic center of US (for remote jobs)
    "Remote - US": (39.8283, -98.5795),  # Geographic center of US
    "East Coast": (40.7128, -74.006),  # New York City (representative of East Coast)
    "Bay Area or Remote": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "Bay Area": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "Europe": (50.8503, 4.3517),  # Brussels (central point of Europe)
    "São Paolo": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "São Paolo, Brazil": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "India - Remote": (28.7041, 77.1025),  # Delhi (center of India)
    "Remote - India": (28.7041, 77.1025),  # Delhi (center of India)
}


def extract_city_from_office_location(location: str) -> str | None:
    """
    Extract city name from office-specific locations like "San Francisco Office" or "Bangalore Office".
    Returns the city name if found, None otherwise.
    """
    location_lower = location.lower()

    # Common patterns: "City Office", "City, Country Office", "Office - City"
    patterns = [
        r"office\s*-\s*([^,;]+)",  # "Office - City"
        r"([^,;]+)\s+office",  # "City Office"
        r"office,\s*([^,;]+)",  # "Office, City"
        r"([a-z\s]+),\s*[a-z]+\s+office",  # "City, Country Office"
    ]

    for pattern in patterns:
        match = re.search(pattern, location_lower)
        if match:
            city = match.group(1).strip()
            # Remove common suffixes
            city = re.sub(
                r"\s*(office|location|offices)\s*$", "", city, flags=re.IGNORECASE
            )
            if city:
                return city.strip()

    # Try to find city names in the location string
    location_lower_clean = re.sub(r"\s*office\s*", " ", location_lower)
    for city_key in LOCATION_COORDINATES.keys():
        city_name = city_key.split(",")[0].strip().lower()
        if city_name in location_lower_clean and len(city_name) > 2:
            return city_name

    return None


def get_coordinates(location: str) -> tuple[float | None, float | None]:
    """
    Get coordinates for a location from the hardcoded map.
    Handles office-specific locations by extracting city names.
    Returns (lat, lon) or (None, None) if not found.
    """
    if pd.isna(location):
        return None, None

    location_str = str(location).strip()

    # Fix common typos
    location_str = location_str.replace("Sao Paolo", "São Paulo")
    location_str = location_str.replace("Sao Paulo", "São Paulo")
    location_str = location_str.replace("São Paolo", "São Paulo")  # Fix typo variant

    # Direct match
    if location_str in LOCATION_COORDINATES:
        lat, lon = LOCATION_COORDINATES[location_str]
        return lat, lon

    # Case-insensitive match
    location_lower = location_str.lower()
    for key, (lat, lon) in LOCATION_COORDINATES.items():
        if key.lower() == location_lower:
            return lat, lon

    # Try to match if location contains the key
    for key, (lat, lon) in LOCATION_COORDINATES.items():
        key_lower = key.lower()
        # Check if the key city name is in the location
        city_name = key_lower.split(",")[0].strip()
        if city_name in location_lower or location_lower in key_lower:
            return lat, lon

    # Try to extract city from office location
    extracted_city = extract_city_from_office_location(location_str)
    if extracted_city:
        # Try to match the extracted city
        for key, (lat, lon) in LOCATION_COORDINATES.items():
            key_lower = key.lower()
            city_name = key_lower.split(",")[0].strip()
            if (
                city_name == extracted_city.lower()
                or extracted_city.lower() in key_lower
            ):
                return lat, lon

    return None, None


def main():
    """Main function to filter jobs to only include specified companies and add coordinates."""
    script_dir = Path(__file__).parent
    input_file = script_dir / "public" / "jobs.csv"
    output_file = script_dir / "public" / "jobs_minimal.csv"
    offices_file = script_dir / "offices.txt"

    # List of some top AI companies
    included_companies = {
        "openai",
        "mistral",
        "anthropic",
        "deepmind",
        "cohere",
        "huggingface",
        "perplexity",
        "character",
        "inflection",
        "anyscale",
        "modal",
        "together",
        "togetherai",
        "runwayml",
        "runway",
        "scaleai",
        "scale",
        "stability",
        "stabilityai",
        "midjourney",
        "replicate",
        "lightning",
        "fal",
        "adept",
    }

    print(f"Reading jobs from {input_file}...")
    df = pd.read_csv(input_file)

    print(f"Total jobs: {len(df)}")

    # Filter to only include specified companies (case-insensitive)
    original_count = len(df)
    df_filtered = df[df["company"].str.lower().isin(included_companies)].copy()
    filtered_count = len(df_filtered)

    print(f"Included {filtered_count} jobs from specified companies")
    print(f"Filtered out {original_count - filtered_count} jobs from other companies")

    # Add coordinates
    print("Adding coordinates...")
    coordinates = df_filtered["location"].apply(get_coordinates)
    df_filtered["lat"] = [coord[0] for coord in coordinates]
    df_filtered["lon"] = [coord[1] for coord in coordinates]

    # Count how many locations were found
    found_coords = df_filtered["lat"].notna().sum()
    missing_coords = df_filtered["lat"].isna().sum()

    print(f"Found coordinates for {found_coords} jobs")
    print(f"Missing coordinates for {missing_coords} jobs")

    # Collect all office locations (both matched and unmatched) for lookup
    all_offices = []
    office_pattern = re.compile(r"office", re.IGNORECASE)

    for idx, row in df_filtered.iterrows():
        location = str(row["location"])
        if office_pattern.search(location):
            all_offices.append(
                {
                    "company": str(row["company"]),
                    "location": location,
                    "title": str(row.get("title", "")),
                    "url": str(row.get("url", "")),
                    "has_coordinates": not pd.isna(row.get("lat")),
                }
            )

    if missing_coords > 0:
        print("\nSample locations without coordinates:")
        missing_df = df_filtered[df_filtered["lat"].isna()]
        missing_locations = missing_df["location"].unique()[:10]
        for loc in missing_locations:
            print(f"  - {loc}")

    # Save all office locations to file for lookup
    if all_offices:
        print(f"\nSaving {len(all_offices)} office locations to {offices_file}...")
        with open(offices_file, "w", encoding="utf-8") as f:
            f.write("# Office locations from AI companies\n")
            f.write("# Format: Company | Location | Title | URL | Has Coordinates\n")
            f.write(
                "# Use this file to look up office locations and add coordinates if missing\n\n"
            )
            for office in sorted(
                all_offices, key=lambda x: (x["company"], x["location"])
            ):
                coords_status = "YES" if office["has_coordinates"] else "NO"
                f.write(
                    f"{office['company']} | {office['location']} | {office['title']} | {office['url']} | {coords_status}\n"
                )
        print(f"Saved {len(all_offices)} office locations to {offices_file}")
        unmatched_count = sum(1 for o in all_offices if not o["has_coordinates"])
        if unmatched_count > 0:
            print(f"  - {unmatched_count} office locations still need coordinates")

    # Save to new file
    print(f"\nSaving filtered jobs with coordinates to {output_file}...")
    df_filtered.to_csv(output_file, index=False)

    print(f"Done! Saved {len(df_filtered)} jobs to {output_file}")


if __name__ == "__main__":
    main()
