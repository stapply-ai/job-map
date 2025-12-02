#!/usr/bin/env python3
"""
AI-powered job data gatherer.
Gathers comprehensive job data (including salaries) from different companies
across multiple ATS systems based on company name.

Usage:
    python ai.py "Company Name"
    python ai.py "Company Name" --ats greenhouse
    python ai.py "Company Name" "Another Company" --ats ashby
"""

import json
import sys
import argparse
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from urllib.parse import urlparse, unquote
import csv
import re
from datetime import date, datetime, timezone
import asyncio
from glob import glob

# Import models
ROOT_DIR = Path(__file__).resolve().parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from models.ashby import AshbyApiResponse
from models.gh import GreenhouseJob
from models.lever import LeverJob
from models.workable import WorkableJob
from pydantic import ValidationError

# Hardcoded coordinates map (from minimal_map.py)
LOCATION_COORDINATES = {
    # United States - Major Cities
    "San Francisco, California, United States": (37.7749, -122.4194),
    "San Francisco, CA, United States": (37.7749, -122.4194),
    "San Francisco": (37.7749, -122.4194),
    "New York, New York, United States": (40.7128, -74.006),
    "New York, NY, United States": (40.7128, -74.006),
    "New York": (40.7128, -74.006),
    "NYC": (40.7128, -74.006),
    "New York City": (40.7128, -74.006),
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
    "Cambridge, Massachusetts, United States": (42.3736, -71.1097),
    "Cambridge, Massachusetts, US": (42.3736, -71.1097),
    "Cambridge, MA, United States": (42.3736, -71.1097),
    "Cambridge, MA": (42.3736, -71.1097),
    "Cambridge": (42.3736, -71.1097),
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
    "Memphis, Tennessee, United States": (35.1495, -90.049),
    "Memphis, TN, United States": (35.1495, -90.049),
    "Memphis, TN": (35.1495, -90.049),
    "Memphis": (35.1495, -90.049),
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
    "Québec City, Quebec, Canada": (46.8139, -71.2080),
    "Québec City, QC, Canada": (46.8139, -71.2080),
    "Québec City, QC": (46.8139, -71.2080),
    "Québec City, QC - Data Center": (46.8139, -71.2080),
    "Quebec City, Quebec, Canada": (46.8139, -71.2080),
    "Quebec City, QC, Canada": (46.8139, -71.2080),
    "Quebec City, QC": (46.8139, -71.2080),
    "Quebec City": (46.8139, -71.2080),
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
    "Anywhere in France": (46.2276, 2.2137),  # Geographic center of France
    "France": (46.2276, 2.2137),
    "Amsterdam, Netherlands": (52.3676, 4.9041),
    "Amsterdam": (52.3676, 4.9041),
    "Barcelona, Spain": (41.3851, 2.1734),
    "Barcelona": (41.3851, 2.1734),
    "Madrid, Spain": (40.4168, -3.7038),
    "Madrid": (40.4168, -3.7038),
    "Anywhere in Spain": (40.4168, -3.7038),  # Madrid (center of Spain)
    "Spain": (40.4168, -3.7038),
    "Rome, Italy": (41.9028, 12.4964),
    "Rome": (41.9028, 12.4964),
    "Milan, Italy": (45.4642, 9.19),
    "Milan": (45.4642, 9.19),
    "Vienna, Austria": (48.2082, 16.3738),
    "Vienna": (48.2082, 16.3738),
    "Zurich, Switzerland": (47.3769, 8.5417),
    "Zurich": (47.3769, 8.5417),
    "Zürich, Switzerland": (47.3769, 8.5417),
    "Zürich, CH": (47.3769, 8.5417),
    "Zürich": (47.3769, 8.5417),
    "Stockholm, Sweden": (59.3293, 18.0686),
    "Stockholm": (59.3293, 18.0686),
    "Malmö, Sweden": (55.6059, 13.0007),
    "Malmö": (55.6059, 13.0007),
    "Malmoe, Sweden": (55.6059, 13.0007),
    "Malmoe": (55.6059, 13.0007),
    "Copenhagen, Denmark": (55.6761, 12.5683),
    "Copenhagen": (55.6761, 12.5683),
    "Dublin, Ireland": (53.3498, -6.2603),
    "Dublin": (53.3498, -6.2603),
    "Brussels, Belgium": (50.8503, 4.3517),
    "Brussels": (50.8503, 4.3517),
    "Anywhere in Belgium": (50.8503, 4.3517),  # Brussels (center of Belgium)
    "Belgium": (50.8503, 4.3517),
    "Lisbon, Portugal": (38.7223, -9.1393),
    "Lisbon": (38.7223, -9.1393),
    "Prague, Czech Republic": (50.0755, 14.4378),
    "Prague": (50.0755, 14.4378),
    "Bratislava, Slovakia": (48.1486, 17.1077),
    "Slovakia": (48.1486, 17.1077),
    "Cologne, Germany": (50.9375, 6.9603),
    "Cologne": (50.9375, 6.9603),
    "Köln, Germany": (50.9375, 6.9603),
    "Köln": (50.9375, 6.9603),
    "Warsaw, Poland": (52.2297, 21.0122),
    "Warsaw": (52.2297, 21.0122),
    "Sofia, Bulgaria": (42.6977, 23.3219),
    "Sofia": (42.6977, 23.3219),
    "Munich, Germany": (48.1351, 11.5820),
    "Munich": (48.1351, 11.5820),
    "Luxembourg": (49.6116, 6.1319),
    "Luxembourg, Luxembourg": (49.6116, 6.1319),
    "Budapest, Hungary": (47.4979, 19.0402),
    "Budapest": (47.4979, 19.0402),
    # Asia-Pacific
    "Singapore, Singapore": (1.3521, 103.8198),
    "Singapore": (1.3521, 103.8198),
    "APAC": (1.3521, 103.8198),  # Singapore (representative center of APAC region)
    "Asia-Pacific": (1.3521, 103.8198),
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
    "UAE": (24.4539, 54.3773),  # Abu Dhabi (capital, center of UAE)
    "United Arab Emirates": (24.4539, 54.3773),
    "Doha, Qatar": (25.2854, 51.5310),
    "Doha": (25.2854, 51.5310),
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
    "Sunnyvale, California, United States": (37.3688, -122.0363),
    "Sunnyvale, CA, United States": (37.3688, -122.0363),
    "Sunnyvale, CA - US": (37.3688, -122.0363),
    "Sunnyvale, CA": (37.3688, -122.0363),
    "Sunnyvale": (37.3688, -122.0363),
    "San Jose, California, United States": (37.3382, -121.8863),
    "San Jose, CA, United States": (37.3382, -121.8863),
    "San Jose, CA - US": (37.3382, -121.8863),
    "San Jose, CA": (37.3382, -121.8863),
    "San Jose": (37.3382, -121.8863),
    "San Jose Office": (37.3382, -121.8863),
    "Foster City, California, United States": (37.5585, -122.2711),
    "Foster City, CA, United States": (37.5585, -122.2711),
    "Foster City, CA - US": (37.5585, -122.2711),
    "Foster City, CA": (37.5585, -122.2711),
    "Foster City": (37.5585, -122.2711),
    "Tulsa, Oklahoma, United States": (36.1540, -95.9928),
    "Tulsa, OK, United States": (36.1540, -95.9928),
    "Tulsa, OK - US": (36.1540, -95.9928),
    "Tulsa, OK": (36.1540, -95.9928),
    "Tulsa": (36.1540, -95.9928),
    "Abilene, Texas, United States": (32.4487, -99.7331),
    "Abilene, TX, United States": (32.4487, -99.7331),
    "Abilene, TX - US": (32.4487, -99.7331),
    "Abilene, TX": (32.4487, -99.7331),
    "Abilene": (32.4487, -99.7331),
    "Cheyenne, Wyoming, United States": (41.1400, -104.8197),
    "Cheyenne, WY, United States": (41.1400, -104.8197),
    "Cheyenne, WY - US": (41.1400, -104.8197),
    "Cheyenne, WY": (41.1400, -104.8197),
    "Cheyenne": (41.1400, -104.8197),
    "Arvada, Colorado, United States": (39.8028, -105.0875),
    "Arvada, CO, United States": (39.8028, -105.0875),
    "Arvada, CO - US": (39.8028, -105.0875),
    "Arvada, CO": (39.8028, -105.0875),
    "Arvada": (39.8028, -105.0875),
    "Ponchatoula, Louisiana, United States": (30.4391, -90.4415),
    "Ponchatoula, LA, United States": (30.4391, -90.4415),
    "Ponchatoula, LA - US": (30.4391, -90.4415),
    "Ponchatoula, LA": (30.4391, -90.4415),
    "Ponchatoula": (30.4391, -90.4415),
    "Amarillo, Texas, United States": (35.2220, -101.8313),
    "Amarillo, TX, United States": (35.2220, -101.8313),
    "Amarillo, TX - US": (35.2220, -101.8313),
    "Amarillo, TX": (35.2220, -101.8313),
    "Amarillo": (35.2220, -101.8313),
    "Springfield, Ohio, United States": (39.9242, -83.8088),
    "Springfield, OH, United States": (39.9242, -83.8088),
    "Springfield, OH - US": (39.9242, -83.8088),
    "Springfield, OH": (39.9242, -83.8088),
    "Bellevue, Washington, United States": (47.6101, -122.2015),
    "Bellevue, WA, United States": (47.6101, -122.2015),
    "Bellevue, WA - US": (47.6101, -122.2015),
    "Bellevue, WA": (47.6101, -122.2015),
    "Bellevue": (47.6101, -122.2015),
    "Quincy, Washington, United States": (47.2343, -119.8526),
    "Quincy, WA, United States": (47.2343, -119.8526),
    "Quincy, WA - US": (47.2343, -119.8526),
    "Quincy, WA": (47.2343, -119.8526),
    "Quincy, WA - Data Center": (47.2343, -119.8526),
    "Quincy": (47.2343, -119.8526),
    "Bluffdale, Utah, United States": (40.4847, -111.9388),
    "Bluffdale, UT, United States": (40.4847, -111.9388),
    "Bluffdale, UT - US": (40.4847, -111.9388),
    "Bluffdale, UT": (40.4847, -111.9388),
    "Bluffdale, UT - Data Center": (40.4847, -111.9388),
    "Bluffdale": (40.4847, -111.9388),
    "Ashburn, Virginia, United States": (39.0438, -77.4874),
    "Ashburn, VA, United States": (39.0438, -77.4874),
    "Ashburn, VA - US": (39.0438, -77.4874),
    "Ashburn, VA": (39.0438, -77.4874),
    "Ashburn, VA - Data Center": (39.0438, -77.4874),
    "Ashburn": (39.0438, -77.4874),
    "Elk Grove Village, Illinois, United States": (42.0039, -87.9703),
    "Elk Grove Village, IL, United States": (42.0039, -87.9703),
    "Elk Grove Village, IL - US": (42.0039, -87.9703),
    "Elk Grove Village, IL": (42.0039, -87.9703),
    "Elk Grove Village, IL - Data Center": (42.0039, -87.9703),
    "Elk Grove Village": (42.0039, -87.9703),
    "Kansas City, Missouri, United States": (39.0997, -94.5786),
    "Kansas City, MO, United States": (39.0997, -94.5786),
    "Kansas City, MO - US": (39.0997, -94.5786),
    "Kansas City, MO": (39.0997, -94.5786),
    "Kansas City, MO - Data Center": (39.0997, -94.5786),
    "Kansas City": (39.0997, -94.5786),
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
    # Iceland
    "Reykjanesbaer, Iceland": (63.9981, -22.5618),
    "Reykjanesbaer, IS": (63.9981, -22.5618),
    "Reykjanesbaer - IS": (63.9981, -22.5618),
    "Reykjanesbaer": (63.9981, -22.5618),
    "Reykjanesbær, Iceland": (63.9981, -22.5618),
    "Reykjanesbær, IS": (63.9981, -22.5618),
    "Reykjanesbær - IS": (63.9981, -22.5618),
    "Reykjanesbær": (63.9981, -22.5618),
    "Sao Paulo, Brazil": (
        -23.5505,
        -46.6333,
    ),  # Note: "São Paulo, Brazil" already defined above
    "Sao Paulo": (-23.5505, -46.6333),
    # Special/Regional locations (handles remote and regional locations)
    "Remote": (39.8283, -98.5795),  # Geographic center of US (for remote jobs)
    "Remote - US": (39.8283, -98.5795),  # Geographic center of US
    "USA": (39.8283, -98.5795),  # Geographic center of US
    "USA | Relocate": (39.8283, -98.5795),  # Geographic center of US
    "United States": (39.8283, -98.5795),
    "East Coast": (40.7128, -74.006),  # New York City (representative of East Coast)
    "Bay Area or Remote": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "Bay Area": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "Europe": (50.8503, 4.3517),  # Brussels (central point of Europe)
    "São Paolo": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "São Paolo, Brazil": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "India - Remote": (28.7041, 77.1025),  # Delhi (center of India)
    "Remote - India": (28.7041, 77.1025),  # Delhi (center of India)
}


def extract_city_from_office_location(location: str) -> Optional[str]:
    """
    Extract city name from office-specific locations like "San Francisco Office" or "Bangalore Office".
    Returns the city name if found, None otherwise.
    """
    location_lower = location.lower()

    # Common patterns: "City Office", "City, Country Office", "Office - City", "City, State Office"
    patterns = [
        r"office\s*-\s*([^,;]+)",  # "Office - City"
        r"([^,;]+)\s+office",  # "City Office" or "City, State Office"
        r"office,\s*([^,;]+)",  # "Office, City"
        r"([a-z\s]+),\s*[a-z]+\s+office",  # "City, Country Office"
    ]

    for pattern in patterns:
        match = re.search(pattern, location_lower, re.IGNORECASE)
        if match:
            city = match.group(1).strip()
            # Remove common suffixes
            city = re.sub(
                r"\s*(office|location|offices)\s*$", "", city, flags=re.IGNORECASE
            )
            if city:
                return city.strip()

    # Try to extract "City, State" pattern before parentheses or other text
    # e.g., "Foster City, CA (Hybrid) In office M,W,F" -> "Foster City, CA"
    city_state_match = re.search(r"([A-Za-z\s]+,\s*[A-Z]{2})", location, re.IGNORECASE)
    if city_state_match:
        city_state = city_state_match.group(1).strip()
        return city_state

    # Try to find city names in the location string
    location_lower_clean = re.sub(r"\s*office\s*", " ", location_lower)
    for city_key in LOCATION_COORDINATES.keys():
        city_name = city_key.split(",")[0].strip().lower()
        if city_name in location_lower_clean and len(city_name) > 2:
            return city_name

    return None


def get_coordinates(location: str) -> Tuple[Optional[float], Optional[float]]:
    """
    Get coordinates for a location from the hardcoded map.
    Handles office-specific locations by extracting city names.
    Returns (lat, lon) or (None, None) if not found.
    """
    if not location or location.strip() == "":
        return None, None

    location_str = str(location).strip()

    # Fix common typos
    location_str = location_str.replace("Sao Paolo", "São Paulo")
    location_str = location_str.replace("Sao Paulo", "São Paulo")
    location_str = location_str.replace("São Paolo", "São Paulo")  # Fix typo variant

    # Handle pipe-separated locations (e.g., "USA | Relocate" -> "USA")
    if " | " in location_str:
        # Take the first part before the pipe
        location_str = location_str.split(" | ")[0].strip()

    # Direct match
    if location_str in LOCATION_COORDINATES:
        lat, lon = LOCATION_COORDINATES[location_str]
        return lat, lon

    # Case-insensitive match
    location_lower = location_str.lower()
    for key, (lat, lon) in LOCATION_COORDINATES.items():
        if key.lower() == location_lower:
            return lat, lon

    # Try to extract city from complex office location strings
    # Extract "City, State" pattern before parentheses, "- Data Center", or other text
    city_state_match = re.search(r"([A-Za-z\s]+,\s*[A-Z]{2})", location_str)
    if city_state_match:
        city_state = city_state_match.group(1).strip()
        if city_state in LOCATION_COORDINATES:
            lat, lon = LOCATION_COORDINATES[city_state]
            return lat, lon
        # Try case-insensitive
        for key, (lat, lon) in LOCATION_COORDINATES.items():
            if key.lower() == city_state.lower():
                return lat, lon

    # Try to match locations with "- Data Center" suffix
    if " - Data Center" in location_str:
        base_location = location_str.replace(" - Data Center", "").strip()
        if base_location in LOCATION_COORDINATES:
            lat, lon = LOCATION_COORDINATES[base_location]
            return lat, lon
        # Try case-insensitive
        for key, (lat, lon) in LOCATION_COORDINATES.items():
            if key.lower() == base_location.lower():
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


# AI companies map: company name -> ATS type (None means search all ATS systems)
AI_COMPANIES = {
    "openai": None,  # Will search all ATS
    "mistral": None,
    "anthropic": None,
    "deepmind": None,
    "cohere": None,
    "huggingface": None,
    "perplexity": None,
    "character": None,
    "inflection": None,
    "anyscale": None,
    "modal": None,
    "together": None,
    "togetherai": None,
    "runwayml": None,
    "runway": None,
    "scaleai": None,
    "scale": None,
    "stability": None,
    "stabilityai": None,
    "midjourney": None,
    "replicate": None,
    "fal": None,
    "adept": None,
    "xai": None,
    "anysphere": None,
    "openrouter": None,
    "applied compute": None,
    "alan": None,
    "attio": None,
    "cartesia": None,
    "cognition": None,
    "crusoe": None,
    "decagon": None,
    "deepgram": None,
    "deepl": None,
    "dust": None,
    "elevenlabs": None,
    "exa": None,
    "factory": None,
    "firecrawl": None,
    "gigaml": None,
    "gladia": None,
    "granola": None,
    "graphite": None,
    "hcompany": None,
    "juicebox": None,
    "jua": None,
    "lambda": None,
    "langchain": None,
    "legora": None,
    "lindy": None,
    "livekit": None,
    "lovable": None,
    "mercor": None,
    "n8n": None,
    "parallel": None,
    "peec": None,
    "photoroom": None,
    "physicalintelligence": None,
    "primeintellect": None,
    "replit": None,
    "notion": None,
    "ramp": None,
}

# ATS configurations
ATS_CONFIGS = {
    "ashby": {
        "companies_csv": ROOT_DIR / "ashby" / "companies.csv",
        "companies_dir": ROOT_DIR / "ashby" / "companies",
        "url_column": "url",
        "name_column": "name",
        "model": AshbyApiResponse,
    },
    "greenhouse": {
        "companies_csv": ROOT_DIR / "greenhouse" / "greenhouse_companies.csv",
        "companies_dir": ROOT_DIR / "greenhouse" / "companies",
        "url_column": "url",
        "name_column": "name",
        "model": None,  # Greenhouse uses dict directly
    },
    "lever": {
        "companies_csv": ROOT_DIR / "lever" / "lever_companies.csv",
        "companies_dir": ROOT_DIR / "lever" / "companies",
        "url_column": "url",
        "name_column": "name",
        "model": None,  # Lever uses list directly
    },
    "workable": {
        "companies_csv": ROOT_DIR / "workable" / "workable_companies.csv",
        "companies_dir": ROOT_DIR / "workable" / "companies",
        "url_column": "url",
        "name_column": "name",
        "model": None,  # Workable uses dict directly
    },
    "rippling": {
        "companies_csv": ROOT_DIR / "rippling" / "rippling_companies.csv",
        "companies_dir": ROOT_DIR / "rippling" / "companies",
        "url_column": "url",
        "name_column": "name",
        "model": None,  # Rippling uses dict directly
    },
}


def normalize_datetime_to_utc_iso(dt: Optional[datetime]) -> Optional[str]:
    """
    Normalize a datetime to UTC ISO 8601 string (e.g. 2025-03-10T14:32:00Z).
    Accepts naive or aware datetimes; returns None if dt is falsy.
    """
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def posted_at_from_source(
    ats_type: str,
    raw_job: Dict,
) -> Optional[str]:
    """
    Compute posted_at ISO datetime from a raw job dict based on ATS type.

    This mirrors the logic used in backfill_posted_at.py:
    - ashby:      job['publishedAt']
    - greenhouse: job['updated_at'] (fallback to job['first_published'])
    - lever:      job['createdAt'] (epoch ms)
    - rippling:   job['created_on']
    - workable:   published_on (fallback created_at)
    """
    try:
        if ats_type == "ashby":
            published = raw_job.get("publishedAt")
            if published:
                try:
                    dt = datetime.fromisoformat(published)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None

        elif ats_type == "greenhouse":
            ts = raw_job.get("updated_at") or raw_job.get("first_published")
            if ts:
                try:
                    dt = datetime.fromisoformat(ts)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None

        elif ats_type == "lever":
            created_at = raw_job.get("createdAt")
            if isinstance(created_at, (int, float)):
                try:
                    dt = datetime.fromtimestamp(created_at / 1000.0, tz=timezone.utc)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None
            if isinstance(created_at, str):
                try:
                    dt = datetime.fromisoformat(created_at)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None

        elif ats_type == "rippling":
            created_on = raw_job.get("created_on")
            if created_on:
                try:
                    dt = datetime.fromisoformat(created_on)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None

        elif ats_type == "workable":
            from datetime import datetime as _dt

            published_on = raw_job.get("published_on")
            created_at = raw_job.get("created_at")

            def _date_to_iso(d: str) -> Optional[str]:
                try:
                    dtd = _dt.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    return normalize_datetime_to_utc_iso(dtd)
                except Exception:
                    return None

            if published_on:
                iso = _date_to_iso(published_on)
                if iso:
                    return iso
            if created_at:
                return _date_to_iso(created_at)

    except Exception:
        return None

    return None


def normalize_company_name(name: str) -> str:
    """Normalize company name for matching."""
    # Remove common suffixes and normalize
    name = name.strip()
    # Remove common suffixes
    for suffix in [
        " Inc",
        " Inc.",
        " LLC",
        " Ltd",
        " Ltd.",
        " Corp",
        " Corp.",
        " Co",
        " Co.",
    ]:
        if name.endswith(suffix):
            name = name[: -len(suffix)].strip()
    return name.lower()


def find_companies_by_name(
    company_name: str, ats_type: Optional[str] = None
) -> List[Tuple[str, str, str]]:
    """
    Find companies matching the given name across ATS systems using exact matching.
    Returns list of (ats_type, company_slug, company_name) tuples.
    """
    matches = []
    normalized_search = normalize_company_name(company_name)

    ats_to_search = [ats_type] if ats_type else ATS_CONFIGS.keys()

    for ats in ats_to_search:
        if ats not in ATS_CONFIGS:
            continue

        config = ATS_CONFIGS[ats]
        companies_csv = config["companies_csv"]

        if not companies_csv.exists():
            continue

        try:
            with open(companies_csv, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    csv_name = row.get(config["name_column"], "").strip()
                    url = row.get(config["url_column"], "").strip()

                    if not csv_name or not url:
                        continue

                    # Normalize CSV company name
                    normalized_csv = normalize_company_name(csv_name)

                    # Exact match (case-insensitive after normalization)
                    if normalized_search == normalized_csv:
                        # Extract slug from URL
                        slug = extract_slug_from_url(url, ats)
                        matches.append((ats, slug, csv_name))
        except Exception as e:
            print(f"Error reading {companies_csv}: {e}", file=sys.stderr)
            continue

    # Remove duplicates (same company across multiple ATS)
    seen = set()
    unique_matches = []
    for ats, slug, name in matches:
        key = (ats, slug.lower())
        if key not in seen:
            seen.add(key)
            unique_matches.append((ats, slug, name))

    return unique_matches


def extract_slug_from_url(url: str, ats_type: str) -> str:
    """Extract company slug from URL based on ATS type."""
    parsed = urlparse(url)
    path = parsed.path.lstrip("/")

    if ats_type == "rippling":
        # https://ats.rippling.com/{slug}/jobs
        slug = path.split("/")[0] if path else "unknown"
    elif ats_type == "ashby":
        # https://jobs.ashbyhq.com/{slug}
        slug = path
    elif ats_type == "greenhouse":
        # https://job-boards.greenhouse.io/{slug}
        slug = path
    elif ats_type == "lever":
        # https://jobs.lever.co/{slug}
        slug = path
    elif ats_type == "workable":
        # https://apply.workable.com/{slug}
        slug = path
    else:
        slug = path.split("/")[0] if path else "unknown"

    return unquote(slug)


def extract_compensation_data(compensation: Optional[Dict]) -> Dict[str, Optional[str]]:
    """Extract compensation data from compensation object."""
    if not compensation:
        return {
            "salary_min": None,
            "salary_max": None,
            "salary_currency": None,
            "salary_period": None,
            "salary_summary": None,
        }

    result = {
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
        "salary_summary": None,
    }

    # Helper function to get value with both camelCase and snake_case support
    def get_field(obj: Dict, camel_key: str, snake_key: str = None):
        """Get field value supporting both camelCase and snake_case."""
        if snake_key is None:
            # Convert camelCase to snake_case
            snake_key = "".join(
                ["_" + c.lower() if c.isupper() else c for c in camel_key]
            ).lstrip("_")
        return obj.get(camel_key) or obj.get(snake_key)

    # Try to get summary first (prefer scrapeableCompensationSalarySummary as it's cleaner)
    summary = get_field(
        compensation,
        "scrapeableCompensationSalarySummary",
        "scrapeable_compensation_salary_summary",
    ) or get_field(compensation, "compensationTierSummary", "compensation_tier_summary")
    if summary:
        result["salary_summary"] = str(summary)

    # Extract from summary components (Ashby format) - these are at the top level
    summary_components = (
        get_field(compensation, "summaryComponents", "summary_components") or []
    )
    if summary_components:
        for component in summary_components:
            comp_type = (
                get_field(component, "compensationType", "compensation_type") or ""
            ).lower()
            # Only extract from Salary components, ignore EquityCashValue and others
            if comp_type == "salary":
                min_val = get_field(component, "minValue", "min_value")
                max_val = get_field(component, "maxValue", "max_value")
                if min_val is not None:
                    result["salary_min"] = (
                        str(int(min_val))
                        if isinstance(min_val, float)
                        else str(min_val)
                    )
                if max_val is not None:
                    result["salary_max"] = (
                        str(int(max_val))
                        if isinstance(max_val, float)
                        else str(max_val)
                    )
                currency = get_field(component, "currencyCode", "currency_code")
                if currency:
                    result["salary_currency"] = currency
                interval = get_field(component, "interval")
                if interval:
                    result["salary_period"] = interval
                # Found salary component, break to avoid overwriting
                break

    # Extract from compensation tiers if we didn't find salary in summaryComponents
    if not result["salary_min"]:
        tiers = get_field(compensation, "compensationTiers", "compensation_tiers") or []
        if tiers:
            for tier in tiers:
                components = get_field(tier, "components") or []
                if not components:
                    continue
                for component in components:
                    comp_type = (
                        get_field(component, "compensationType", "compensation_type")
                        or ""
                    ).lower()
                    # Only extract from Salary components
                    if comp_type == "salary":
                        min_val = get_field(component, "minValue", "min_value")
                        max_val = get_field(component, "maxValue", "max_value")
                        if min_val is not None:
                            result["salary_min"] = (
                                str(int(min_val))
                                if isinstance(min_val, float)
                                else str(min_val)
                            )
                        if max_val is not None:
                            result["salary_max"] = (
                                str(int(max_val))
                                if isinstance(max_val, float)
                                else str(max_val)
                            )
                        currency = get_field(component, "currencyCode", "currency_code")
                        if currency:
                            result["salary_currency"] = currency
                        interval = get_field(component, "interval")
                        if interval:
                            result["salary_period"] = interval
                        # Found salary component, break both loops
                        break
                # If we found salary in this tier, break outer loop
                if result["salary_min"]:
                    break

    return result


def extract_ashby_jobs(json_file: Path, company_name: str) -> List[Dict]:
    """Extract jobs from Ashby JSON file."""
    jobs = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        parsed = AshbyApiResponse(**data)

        # Build a mapping from jobUrl/applyUrl to raw job dict so we can
        # compute posted_at consistently from source timestamps.
        raw_jobs_by_url: Dict[str, Dict] = {}
        for raw in data.get("jobs", []):
            url = raw.get("jobUrl") or raw.get("applyUrl")
            if url:
                raw_jobs_by_url[url] = raw

        for job in parsed.jobs:
            # Use model_dump to get snake_case keys (by_alias=False)
            comp_dict = None
            if job.compensation:
                if hasattr(job.compensation, "model_dump"):
                    comp_dict = job.compensation.model_dump(by_alias=False)
                else:
                    comp_dict = job.compensation.dict()
            comp_data = extract_compensation_data(comp_dict)

            # Get coordinates for location
            location_str = job.location or ""
            lat, lon = get_coordinates(location_str)

            job_url = job.job_url or job.apply_url or ""
            posted_at = None
            if job_url:
                raw_job = raw_jobs_by_url.get(job_url, {})
                posted_at = posted_at_from_source("ashby", raw_job)

            jobs.append(
                {
                    "url": job_url,
                    "title": job.title or "",
                    "location": location_str,
                    "company": company_name,
                    "ats_id": job.id,
                    "ats_type": "ashby",
                    "salary_min": comp_data["salary_min"],
                    "salary_max": comp_data["salary_max"],
                    "salary_currency": comp_data["salary_currency"],
                    "salary_period": comp_data["salary_period"],
                    "salary_summary": comp_data["salary_summary"],
                    "employment_type": job.employment_type or "",
                    "is_remote": str(job.is_remote)
                    if job.is_remote is not None
                    else "",
                    "lat": lat,
                    "lon": lon,
                    "posted_at": posted_at,
                }
            )
    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_greenhouse_jobs(json_file: Path, company_name: str) -> List[Dict]:
    """Extract jobs from Greenhouse JSON file."""
    jobs = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", [])
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            try:
                job = GreenhouseJob(**job_data)
            except ValidationError:
                continue

            location_str = (
                job.location.name if job.location and job.location.name else ""
            )

            # Get coordinates for location
            lat, lon = get_coordinates(location_str)

            # Greenhouse doesn't have compensation in the standard API response
            # but we can try to extract from metadata if available
            salary_summary = None
            if job.metadata:
                for meta in job.metadata:
                    if meta.name and "salary" in meta.name.lower():
                        salary_summary = str(meta.value)
                        break

            posted_at = posted_at_from_source("greenhouse", job_data)

            jobs.append(
                {
                    "url": job.absolute_url or "",
                    "title": job.title or "",
                    "location": location_str,
                    "company": company_name,
                    "ats_id": str(job.id) if job.id is not None else "",
                    "ats_type": "greenhouse",
                    "salary_min": None,
                    "salary_max": None,
                    "salary_currency": None,
                    "salary_period": None,
                    "salary_summary": salary_summary,
                    "employment_type": "",
                    "is_remote": "",
                    "lat": lat,
                    "lon": lon,
                    "posted_at": posted_at,
                }
            )
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_lever_jobs(json_file: Path, company_name: str) -> List[Dict]:
    """Extract jobs from Lever JSON file."""
    jobs = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = (
            data
            if isinstance(data, list)
            else data.get("postings", []) or data.get("jobs", [])
        )
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            try:
                job = LeverJob(**job_data)
            except ValidationError:
                continue

            location_str = ""
            if job.categories:
                if job.categories.location:
                    location_str = job.categories.location
                elif job.categories.allLocations:
                    location_str = ", ".join(
                        loc for loc in job.categories.allLocations if loc
                    )
            if not location_str:
                location_str = job.country or ""

            # Lever doesn't have compensation in the model, try to extract from raw data if available
            comp_data = {
                "salary_min": None,
                "salary_max": None,
                "salary_currency": None,
                "salary_period": None,
                "salary_summary": None,
            }

            # Get coordinates for location
            lat, lon = get_coordinates(location_str)

            posted_at = posted_at_from_source("lever", job_data)

            jobs.append(
                {
                    "url": job.hostedUrl or job.applyUrl or "",
                    "title": job.text or "",
                    "location": location_str,
                    "company": company_name,
                    "ats_id": job.id or "",
                    "ats_type": "lever",
                    "salary_min": comp_data["salary_min"],
                    "salary_max": comp_data["salary_max"],
                    "salary_currency": comp_data["salary_currency"],
                    "salary_period": comp_data["salary_period"],
                    "salary_summary": comp_data["salary_summary"],
                    "employment_type": job.categories.commitment
                    if job.categories
                    else "",
                    "is_remote": str(
                        job.workplaceType == "remote" if job.workplaceType else ""
                    ),
                    "lat": lat,
                    "lon": lon,
                    "posted_at": posted_at,
                }
            )
    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_workable_jobs(json_file: Path, company_name: str) -> List[Dict]:
    """Extract jobs from Workable JSON file."""
    jobs = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = (
            data
            if isinstance(data, list)
            else data.get("results", []) or data.get("jobs", [])
        )
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            try:
                job = WorkableJob(**job_data)
            except ValidationError:
                continue

            # Extract location from locations list
            location_str = ""
            if job.locations:
                location_parts = []
                for loc in job.locations:
                    if isinstance(loc, dict):
                        # Location is a dict with city, country, etc.
                        parts = [loc.get("city"), loc.get("region"), loc.get("country")]
                        location_parts.append(", ".join(p for p in parts if p))
                    else:
                        location_parts.append(str(loc))
                location_str = ", ".join(location_parts)
            elif job.city or job.state or job.country:
                location_parts = [job.city, job.state, job.country]
                location_str = ", ".join(p for p in location_parts if p)

            # Workable doesn't have salary in the model, set to None
            comp_data = {
                "salary_min": None,
                "salary_max": None,
                "salary_currency": None,
                "salary_period": None,
                "salary_summary": None,
            }

            # Get coordinates for location
            lat, lon = get_coordinates(location_str)

            posted_at = posted_at_from_source("workable", job_data)

            jobs.append(
                {
                    "url": job.url or job.application_url or "",
                    "title": job.title or "",
                    "location": location_str,
                    "company": company_name,
                    "ats_id": str(job.shortcode or job.code or ""),
                    "ats_type": "workable",
                    "salary_min": comp_data["salary_min"],
                    "salary_max": comp_data["salary_max"],
                    "salary_currency": comp_data["salary_currency"],
                    "salary_period": comp_data["salary_period"],
                    "salary_summary": comp_data["salary_summary"],
                    "employment_type": job.employment_type or "",
                    "is_remote": str(job.telecommuting)
                    if job.telecommuting is not None
                    else "",
                    "lat": lat,
                    "lon": lon,
                    "posted_at": posted_at,
                }
            )
    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def check_json_freshness(json_file: Path, max_age_hours: float = 0.25) -> bool:
    """
    Check if JSON file is fresh (less than max_age_hours old).
    Returns True if fresh, False if stale or doesn't exist.
    """
    if not json_file.exists():
        return False

    try:
        # First try to check last_scraped field in JSON
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)
            last_scraped_str = data.get("last_scraped")
            if last_scraped_str:
                try:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now() - last_scraped
                    ).total_seconds() / 3600
                    return hours_elapsed < max_age_hours
                except (ValueError, TypeError):
                    pass

        # Fallback to file modification time
        file_mtime = json_file.stat().st_mtime
        hours_elapsed = (datetime.now().timestamp() - file_mtime) / 3600
        return hours_elapsed < max_age_hours
    except Exception:
        return False


def fetch_fresh_data(company_name: str, ats_type: str, slug: str) -> bool:
    """
    Fetch fresh data for a company by calling the appropriate ATS scraping function.
    Returns True if data was fetched/updated, False otherwise.
    """
    try:
        if ats_type == "ashby":
            from ashby.main import scrape_ashby_jobs

            result = asyncio.run(
                scrape_ashby_jobs(slug, force=False, company_name=company_name)
            )
            return result is not None and result[2]  # result[2] is was_scraped flag
        elif ats_type == "greenhouse":
            from greenhouse.main import scrape_greenhouse_jobs

            result = asyncio.run(
                scrape_greenhouse_jobs(slug, force=False, company_name=company_name)
            )
            return result is not None and result[2]  # result[2] is was_scraped flag
        elif ats_type == "lever":
            from lever.main import scrape_lever_jobs

            result = asyncio.run(
                scrape_lever_jobs(slug, force=False, company_name=company_name)
            )
            return result is not None and result[2]  # result[2] is was_scraped flag
        elif ats_type == "workable":
            from workable.main import scrape_workable_jobs

            result = asyncio.run(
                scrape_workable_jobs(slug, force=False, company_name=company_name)
            )
            return result is not None and result[2]  # result[2] is was_scraped flag
        elif ats_type == "rippling":
            from rippling.main import scrape_company_jobs

            # Rippling uses company_url instead of slug, need to construct URL
            # Try to find the URL from the companies CSV
            config = ATS_CONFIGS[ats_type]
            companies_csv = config["companies_csv"]
            if companies_csv.exists():
                with open(companies_csv, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        csv_name = row.get(config["name_column"], "").strip()
                        url = row.get(config["url_column"], "").strip()
                        if normalize_company_name(csv_name) == normalize_company_name(
                            company_name
                        ):
                            result = scrape_company_jobs(
                                url, force=False, company_name=company_name
                            )
                            return result is not None
            return False
        else:
            print(f"Unknown ATS type: {ats_type}")
            return False
    except Exception as e:
        print(
            f"Error fetching fresh data for {company_name} ({ats_type}): {e}",
            file=sys.stderr,
        )
        return False


def extract_rippling_jobs(json_file: Path, company_name: str) -> List[Dict]:
    """Extract jobs from Rippling JSON file."""
    jobs = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Rippling structure varies, try common patterns
        job_list = data.get("jobs", []) or data.get("results", []) or []
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            # Rippling doesn't have a standard model, extract manually
            url = job_data.get("url") or job_data.get("applyUrl") or ""
            title = job_data.get("title") or job_data.get("name") or ""
            location = job_data.get("location") or job_data.get("city") or ""
            ats_id = str(job_data.get("id", ""))

            # Try to extract compensation
            comp_data = extract_compensation_data(job_data.get("compensation"))

            # Get coordinates for location
            lat, lon = get_coordinates(location)

            posted_at = posted_at_from_source("rippling", job_data)

            jobs.append(
                {
                    "url": url,
                    "title": title,
                    "location": location,
                    "company": company_name,
                    "ats_id": ats_id,
                    "ats_type": "rippling",
                    "salary_min": comp_data["salary_min"],
                    "salary_max": comp_data["salary_max"],
                    "salary_currency": comp_data["salary_currency"],
                    "salary_period": comp_data["salary_period"],
                    "salary_summary": comp_data["salary_summary"],
                    "employment_type": job_data.get("employmentType") or "",
                    "is_remote": str(job_data.get("remote"))
                    if job_data.get("remote") is not None
                    else "",
                    "lat": lat,
                    "lon": lon,
                    "posted_at": posted_at,
                }
            )
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def gather_jobs_for_companies(
    company_names: List[str], ats_type: Optional[str] = None
) -> List[Dict]:
    """Gather all jobs for the given company names."""
    all_jobs = []
    all_matches = []

    # Find all matching companies
    for company_name in company_names:
        matches = find_companies_by_name(company_name, ats_type)
        all_matches.extend(matches)
        print(f"Found {len(matches)} match(es) for '{company_name}':")
        for ats, slug, name in matches:
            print(f"  - {name} ({ats})")

    if not all_matches:
        print(f"No companies found matching: {', '.join(company_names)}")
        return all_jobs

    # Extract jobs from each match
    for ats, slug, company_name in all_matches:
        config = ATS_CONFIGS[ats]
        companies_dir = config["companies_dir"]

        # Find JSON file (handle URL encoding in filename)
        json_file = companies_dir / f"{slug}.json"
        if not json_file.exists():
            # Try URL-encoded version
            from urllib.parse import quote

            encoded_slug = quote(slug, safe="")
            json_file = companies_dir / f"{encoded_slug}.json"

        if not json_file.exists():
            print(
                f"Warning: JSON file not found for {company_name} ({ats}): {json_file.name}"
            )
            continue

        # Check if JSON is fresh (less than 1 hour old)
        if not check_json_freshness(json_file, max_age_hours=1.0):
            print(
                f"JSON file for {company_name} ({ats}) is stale, fetching fresh data..."
            )
            fetch_fresh_data(company_name, ats, slug)
            # Re-check the file path in case it was created/updated
            json_file = companies_dir / f"{slug}.json"
            if not json_file.exists():
                encoded_slug = quote(slug, safe="")
                json_file = companies_dir / f"{encoded_slug}.json"

        print(f"Extracting jobs from {company_name} ({ats})...")

        # Extract jobs based on ATS type
        if ats == "ashby":
            jobs = extract_ashby_jobs(json_file, company_name)
        elif ats == "greenhouse":
            jobs = extract_greenhouse_jobs(json_file, company_name)
        elif ats == "lever":
            jobs = extract_lever_jobs(json_file, company_name)
        elif ats == "workable":
            jobs = extract_workable_jobs(json_file, company_name)
        elif ats == "rippling":
            jobs = extract_rippling_jobs(json_file, company_name)
        else:
            print(f"Unknown ATS type: {ats}")
            continue

        all_jobs.extend(jobs)
        print(f"  Extracted {len(jobs)} jobs")

    return all_jobs


def find_most_recent_ai_csv(exclude_today: bool = True) -> Optional[Path]:
    """
    Find the most recent ai-{date}.csv file in the root directory.
    Returns None if no such file exists.
    """
    today = date.today()
    today_str = today.strftime("%d-%m-%Y")

    # Find all ai-*.csv files in root
    pattern = str(ROOT_DIR / "ai-*.csv")
    csv_files = [Path(f) for f in glob(pattern) if Path(f).is_file()]

    if not csv_files:
        return None

    # Filter out today's file if requested
    if exclude_today:
        csv_files = [f for f in csv_files if f.name != f"ai-{today_str}.csv"]

    if not csv_files:
        return None

    # Return the most recent one by modification time
    most_recent = max(csv_files, key=lambda f: f.stat().st_mtime)
    return most_recent


def find_new_jobs(current_csv: Path, previous_csv: Path) -> List[Dict]:
    """
    Compare two CSV files and return jobs from current_csv that don't exist in previous_csv.
    Jobs are compared by URL.
    """
    # Read previous CSV URLs
    previous_urls = set()
    try:
        with open(previous_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                if url:
                    previous_urls.add(url)
    except Exception as e:
        print(f"Error reading previous CSV {previous_csv}: {e}", file=sys.stderr)
        return []

    # Read current CSV and find new jobs
    new_jobs = []
    try:
        with open(current_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                if url and url not in previous_urls:
                    new_jobs.append(row)
    except Exception as e:
        print(f"Error reading current CSV {current_csv}: {e}", file=sys.stderr)
        return []

    return new_jobs


def main():
    parser = argparse.ArgumentParser(
        description="Gather job data (including salaries) from companies by name"
    )
    parser.add_argument(
        "companies",
        nargs="*",
        help="Company name(s) to search for (if not provided, uses AI companies list)",
    )
    parser.add_argument(
        "--ai-companies",
        action="store_true",
        help="Use the predefined AI companies list",
    )
    parser.add_argument(
        "--ats",
        choices=list(ATS_CONFIGS.keys()),
        help="Limit search to specific ATS system",
    )
    parser.add_argument(
        "--output",
        "-o",
        default="map/public/ai.csv",
        help="Output CSV file path (default: map/public/ai.csv)",
    )
    args = parser.parse_args()

    # Determine which companies to search for
    if args.ai_companies or not args.companies:
        companies_to_search = list(AI_COMPANIES.keys())
        print(f"Using AI companies list ({len(companies_to_search)} companies)")
        # If using AI companies, respect the ATS mapping (but allow override with --ats)
        if not args.ats:
            # Process companies grouped by ATS type for efficiency
            companies_by_ats = {}
            for company_name in companies_to_search:
                ats_type = AI_COMPANIES.get(company_name)
                if ats_type not in companies_by_ats:
                    companies_by_ats[ats_type] = []
                companies_by_ats[ats_type].append(company_name)

            # Gather jobs for each ATS group
            all_jobs = []
            for ats_type, company_list in companies_by_ats.items():
                jobs = gather_jobs_for_companies(company_list, ats_type)
                all_jobs.extend(jobs)
            jobs = all_jobs
        else:
            # --ats override: search all companies in specified ATS
            jobs = gather_jobs_for_companies(companies_to_search, args.ats)
    else:
        companies_to_search = args.companies
        jobs = gather_jobs_for_companies(companies_to_search, args.ats)

    if not jobs:
        print("No jobs found.")
        return

    # Write to CSV (make path relative to script directory if not absolute)
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "url",
        "title",
        "location",
        "company",
        "ats_id",
        "ats_type",
        "salary_min",
        "salary_max",
        "salary_currency",
        "salary_period",
        "salary_summary",
        "employment_type",
        "is_remote",
        "lat",
        "lon",
        "posted_at",
    ]

    # Write to the specified output path
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)

    print(f"\n✅ Saved {len(jobs)} jobs to {output_path}")

    # Also save to root as ai-{date}.csv
    today = date.today()
    date_str = today.strftime("%d-%m-%Y")
    root_output_path = ROOT_DIR / f"ai-{date_str}.csv"
    with open(root_output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs)

    print(f"✅ Also saved {len(jobs)} jobs to {root_output_path}")

    # Read current CSV to get all active job URLs
    current_urls = set()
    try:
        with open(root_output_path, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                if url:
                    current_urls.add(url)
    except Exception as e:
        print(f"Error reading current CSV: {e}", file=sys.stderr)
        current_urls = set()

    # Find most recent ai-{date}.csv (excluding today's)
    previous_csv = find_most_recent_ai_csv(exclude_today=True)
    new_ai_path = ROOT_DIR / "new_ai.csv"
    today_str = date.today().strftime("%d-%m-%Y-%H-%M")

    if previous_csv:
        print(f"Comparing with previous CSV: {previous_csv.name}")
        new_jobs = find_new_jobs(root_output_path, previous_csv)

        # Read existing new_ai.csv if it exists
        existing_new_jobs = {}
        if new_ai_path.exists():
            try:
                with open(new_ai_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get("url", "").strip()
                        if (
                            url and url in current_urls
                        ):  # Only keep jobs that still exist
                            existing_new_jobs[url] = row
            except Exception as e:
                print(f"Error reading existing new_ai.csv: {e}", file=sys.stderr)

        # Add new jobs with today's date
        for job in new_jobs:
            url = job.get("url", "").strip()
            if url:
                job["date_added"] = today_str
                existing_new_jobs[url] = job

        if existing_new_jobs:
            # Write updated new_ai.csv with date_added column
            new_fieldnames = fieldnames + ["date_added"]
            with open(new_ai_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=new_fieldnames)
                writer.writeheader()
                writer.writerows(existing_new_jobs.values())

            new_count = len(
                [
                    j
                    for j in existing_new_jobs.values()
                    if j.get("date_added") == today_str
                ]
            )
            existing_count = len(existing_new_jobs) - new_count
            print(
                f"✅ Updated new_ai.csv: {new_count} new jobs added today, {existing_count} existing jobs still active"
            )
            print(
                f"   Saved {len(existing_new_jobs)} total active jobs to {new_ai_path}"
            )
        else:
            print("✅ No new jobs found compared to previous CSV")
    else:
        # No previous CSV, but check if new_ai.csv exists and validate it
        if new_ai_path.exists():
            print(
                "ℹ️  No previous dated CSV found, but validating existing new_ai.csv..."
            )
            existing_new_jobs = {}
            try:
                with open(new_ai_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get("url", "").strip()
                        if (
                            url and url in current_urls
                        ):  # Only keep jobs that still exist
                            existing_new_jobs[url] = row

                if existing_new_jobs:
                    # Ensure date_added column exists
                    new_fieldnames = fieldnames + ["date_added"]
                    # Add date_added if missing
                    for job in existing_new_jobs.values():
                        if "date_added" not in job:
                            job["date_added"] = today_str  # Use today as fallback

                    with open(new_ai_path, "w", encoding="utf-8", newline="") as f:
                        writer = csv.DictWriter(f, fieldnames=new_fieldnames)
                        writer.writeheader()
                        writer.writerows(existing_new_jobs.values())

                    print(
                        f"✅ Validated new_ai.csv: {len(existing_new_jobs)} jobs still active"
                    )
                else:
                    print("ℹ️  No active jobs found in existing new_ai.csv")
            except Exception as e:
                print(f"Error validating new_ai.csv: {e}", file=sys.stderr)
        else:
            print("ℹ️  No previous CSV found for comparison (this may be the first run)")


if __name__ == "__main__":
    main()
