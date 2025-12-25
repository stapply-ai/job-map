#!/usr/bin/env python3
"""
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
import html
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

# Import extraction functions from extract_salary_experience.py
from extract_salary_experience import (
    get_job_description_fast,
    extract_salary_from_description,
    extract_experience_from_description,
    parse_salary,
)

# File to log Cloudflare location extraction failures
CLOUDFLARE_FAILURES_FILE = ROOT_DIR / "cloudflare_location_failures.jsonl"

# Hardcoded coordinates map (from minimal_map.py)
LOCATION_COORDINATES = {
    # United States - Major Cities
    "San Francisco, California, United States": (37.7749, -122.4194),
    "San Francisco, CA, United States": (37.7749, -122.4194),
    "San Francisco": (37.7749, -122.4194),
    "San Fransisco, California, United States": (
        37.7749,
        -122.4194,
    ),  # Handle typo variant
    "San Fransisco, CA, United States": (37.7749, -122.4194),  # Handle typo variant
    "San Fransisco": (37.7749, -122.4194),  # Handle typo variant
    "New York, New York, United States": (40.7128, -74.006),
    "New York, NY, United States": (40.7128, -74.006),
    "New York": (40.7128, -74.006),
    "NYC": (40.7128, -74.006),
    "New York City": (40.7128, -74.006),
    "Mapbox US": (37.7749, -122.4194),  # San Francisco (default for Mapbox US)
    "Los Angeles, California, United States": (34.0522, -118.2437),
    "Los Angeles, CA, United States": (34.0522, -118.2437),
    "Los Angeles": (34.0522, -118.2437),
    "Hollywood, California, United States": (34.09256, -118.32888),
    "Hollywood, CA, United States": (34.09256, -118.32888),
    "Hollywood, CA": (34.09256, -118.32888),
    "Hollywood": (34.09256, -118.32888),
    "Chicago, Illinois, United States": (41.8781, -87.6298),
    "Chicago, IL, United States": (41.8781, -87.6298),
    "Chicago": (41.8781, -87.6298),
    "Seattle, Washington, United States": (47.6062, -122.3321),
    "Seattle, WA, United States": (47.6062, -122.3321),
    "Seattle": (47.6062, -122.3321),
    "Austin, Texas, United States": (30.2672, -97.7431),
    "Austin, TX, United States": (30.2672, -97.7431),
    "Austin": (30.2672, -97.7431),
    "San Antonio, Texas, United States": (29.4241, -98.4936),
    "San Antonio, TX, United States": (29.4241, -98.4936),
    "San Antonio, TX": (29.4241, -98.4936),
    "San Antonio, US": (29.4241, -98.4936),
    "San Antonio": (29.4241, -98.4936),
    "Boston, Massachusetts, United States": (42.3601, -71.0589),
    "Boston, MA, United States": (42.3601, -71.0589),
    "Boston": (42.3601, -71.0589),
    "Cambridge, Massachusetts, United States": (42.3736, -71.1097),
    "Cambridge, Massachusetts, US": (42.3736, -71.1097),
    "Cambridge, MA, United States": (42.3736, -71.1097),
    "Cambridge, MA": (42.3736, -71.1097),
    "Cambridge": (42.3736, -71.1097),
    "Needham, Massachusetts, United States": (42.2811, -71.2364),
    "Needham, MA, United States": (42.2811, -71.2364),
    "Needham, MA": (42.2811, -71.2364),
    "Needham": (42.2811, -71.2364),
    "Denver, Colorado, United States": (39.7392, -104.9903),
    "Denver, CO, United States": (39.7392, -104.9903),
    "Denver": (39.7392, -104.9903),
    "Washington, District of Columbia, United States": (38.9072, -77.0369),
    "Washington, DC, United States": (38.9072, -77.0369),
    "Washington": (38.9072, -77.0369),
    "Miami, Florida, United States": (25.7617, -80.1918),
    "Miami, FL, United States": (25.7617, -80.1918),
    "Miami": (25.7617, -80.1918),
    "Orlando, Florida, United States": (28.5383, -81.3792),
    "Orlando, FL, United States": (28.5383, -81.3792),
    "Orlando, FL": (28.5383, -81.3792),
    "Orlando": (28.5383, -81.3792),
    "Lake Mary, Florida, United States": (28.7589, -81.3178),
    "Lake Mary, FL, United States": (28.7589, -81.3178),
    "Lake Mary, FL": (28.7589, -81.3178),
    "Lake Mary": (28.7589, -81.3178),
    "Portland, Oregon, United States": (45.5152, -122.6784),
    "Portland, OR, United States": (45.5152, -122.6784),
    "Portland": (45.5152, -122.6784),
    "Atlanta, Georgia, United States": (33.749, -84.388),
    "Atlanta, GA, United States": (33.749, -84.388),
    "Atlanta": (33.749, -84.388),
    "Dallas, Texas, United States": (32.7767, -96.797),
    "Dallas, TX, United States": (32.7767, -96.797),
    "Dallas": (32.7767, -96.797),
    "Plano, Texas, United States": (33.0198, -96.6989),
    "Plano, TX, United States": (33.0198, -96.6989),
    "Plano, TX": (33.0198, -96.6989),
    "Plano": (33.0198, -96.6989),
    "Westlake, Texas, United States": (32.9912, -97.1950),
    "Westlake, TX, United States": (32.9912, -97.1950),
    "Westlake, TX": (32.9912, -97.1950),
    "Westlake": (32.9912, -97.1950),
    "Houston, Texas, United States": (29.7604, -95.3698),
    "Houston, TX, United States": (29.7604, -95.3698),
    "Houston, TX": (29.7604, -95.3698),
    "Houston": (29.7604, -95.3698),
    "Detroit, Michigan, United States": (42.3314, -83.0458),
    "Detroit, MI, United States": (42.3314, -83.0458),
    "Detroit, MI": (42.3314, -83.0458),
    "Detroit": (42.3314, -83.0458),
    "St. Louis, Missouri, United States": (38.6270, -90.1994),
    "St. Louis, MO, United States": (38.6270, -90.1994),
    "St. Louis, MO": (38.6270, -90.1994),
    "St. Louis": (38.6270, -90.1994),
    "Redmond, Washington, United States": (47.6740, -122.1215),
    "Redmond, WA, United States": (47.6740, -122.1215),
    "Redmond, WA": (47.6740, -122.1215),
    "Redmond": (47.6740, -122.1215),
    "North Bend, Washington, United States": (47.4953, -121.7868),
    "North Bend, WA, United States": (47.4953, -121.7868),
    "North Bend, WA": (47.4953, -121.7868),
    "North Bend": (47.4953, -121.7868),
    "Honolulu, Hawaii, United States": (21.3099, -157.8581),
    "Honolulu, HI, United States": (21.3099, -157.8581),
    "Honolulu, HI": (21.3099, -157.8581),
    "Honolulu": (21.3099, -157.8581),
    "Colorado Springs, Colorado, United States": (38.8339, -104.8214),
    "Colorado Springs, CO, United States": (38.8339, -104.8214),
    "Colorado Springs, CO": (38.8339, -104.8214),
    "Colorado Springs": (38.8339, -104.8214),
    "Bastrop, Texas, United States": (30.1104, -97.3153),
    "Bastrop, TX, United States": (30.1104, -97.3153),
    "Bastrop, TX": (30.1104, -97.3153),
    "Bastrop": (30.1104, -97.3153),
    "Columbia, Maryland, United States": (39.2037, -76.8610),
    "Columbia, MD, United States": (39.2037, -76.8610),
    "Columbia, MD": (39.2037, -76.8610),
    "Columbia": (39.2037, -76.8610),
    "Southaven, Mississippi, United States": (34.9910, -90.0026),
    "Southaven, MS, United States": (34.9910, -90.0026),
    "Southaven, MS": (34.9910, -90.0026),
    "Southaven": (34.9910, -90.0026),
    "North Carolina, United States": (35.7596, -79.0193),  # Geographic center
    "North Carolina": (35.7596, -79.0193),
    "Raleigh, North Carolina, United States": (35.7796, -78.6382),
    "Raleigh, NC, United States": (35.7796, -78.6382),
    "Raleigh, NC": (35.7796, -78.6382),
    "Raleigh": (35.7796, -78.6382),
    "Huntsville, Alabama, United States": (34.7304, -86.5861),
    "Huntsville, AL, United States": (34.7304, -86.5861),
    "Huntsville, AL": (34.7304, -86.5861),
    "Huntsville": (34.7304, -86.5861),
    "Nebraska, United States": (41.4925, -99.9018),  # Geographic center
    "Nebraska": (41.4925, -99.9018),
    "Virginia, United States": (37.7693, -78.1697),  # Geographic center
    "Virginia": (37.7693, -78.1697),
    "Virgina": (37.7693, -78.1697),  # Handle typo
    "Virgina, United States": (37.7693, -78.1697),  # Handle typo
    "Southern California": (
        34.0522,
        -118.2437,
    ),  # Los Angeles (representative of Southern California)
    "Northern California": (
        38.58,
        -121.49,
    ),  # Sacramento (representative of Northern California)
    "Indiana, United States": (39.8494, -86.2583),  # Geographic center
    "Indiana": (39.8494, -86.2583),
    "Ohio, United States": (
        40.4553,
        -82.7733,
    ),  # Geographic center (center of population)
    "Ohio": (40.4553, -82.7733),
    "Tennessee, United States": (35.8594, -86.3619),  # Geographic center
    "Tennessee": (35.8594, -86.3619),
    "Tennesse": (35.8594, -86.3619),  # Handle typo variant
    "DC Metro Preferred": (38.9072, -77.0369),  # Washington DC
    "Clay, New York, United States": (43.1848, -76.1727),
    "Clay, NY, United States": (43.1848, -76.1727),
    "Clay, NY": (43.1848, -76.1727),
    "Clay HQ": (40.7406, -73.9964),  # 111 W 19th Street, 5th Floor, New York, NY 10011
    "Clay": (43.1848, -76.1727),
    "Pacific Northwest": (
        47.6062,
        -122.3321,
    ),  # Seattle (representative of Pacific Northwest)
    "Pacific Northwest OR Arizona": (
        37.7749,
        -122.4194,
    ),  # San Francisco (midpoint between PNW and Arizona)
    "Memphis, Tennessee, United States": (35.1495, -90.049),
    "Memphis, TN, United States": (35.1495, -90.049),
    "Memphis, TN": (35.1495, -90.049),
    "Memphis": (35.1495, -90.049),
    "Idaho Falls, Idaho, United States": (43.49, -112.04),
    "Idaho Falls, ID, United States": (43.49, -112.04),
    "Idaho Falls, ID": (43.49, -112.04),
    "Idaho Falls": (43.49, -112.04),
    "Phoenix, Arizona, United States": (33.4484, -112.074),
    "Phoenix, AZ, United States": (33.4484, -112.074),
    "Phoenix": (33.4484, -112.074),
    "San Diego, California, United States": (32.7157, -117.1611),
    "San Diego, CA, United States": (32.7157, -117.1611),
    "San Diego": (32.7157, -117.1611),
    "Philadelphia, Pennsylvania, United States": (39.9526, -75.1652),
    "Philadelphia, PA, United States": (39.9526, -75.1652),
    "Philadelphia": (39.9526, -75.1652),
    "Pittsburgh, Pennsylvania, United States": (40.4406, -79.9959),
    "Pittsburgh, PA, United States": (40.4406, -79.9959),
    "Pittsburgh, PA": (40.4406, -79.9959),
    "Pittsburgh": (40.4406, -79.9959),
    "Irvine, California, United States": (33.6846, -117.8265),
    "Irvine, CA, United States": (33.6846, -117.8265),
    "Irvine, CA": (33.6846, -117.8265),
    "Irvine": (33.6846, -117.8265),
    "Palo Alto, California, United States": (37.4419, -122.1430),
    "Palo Alto, CA, United States": (37.4419, -122.1430),
    "Palo Alto": (37.4419, -122.1430),
    "Menlo Park, California, United States": (37.4538, -122.182),
    "Menlo Park, CA, United States": (37.4538, -122.182),
    "Menlo Park, CA": (37.4538, -122.182),
    "Menlo Park": (37.4538, -122.182),
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
    "Kitchener-Waterloo, Ontario, Canada": (43.4516, -80.4925),
    "Kitchener-Waterloo, ON, Canada": (43.4516, -80.4925),
    "Kitchener-Waterloo, ON": (43.4516, -80.4925),
    "Kitchener-Waterloo": (43.4516, -80.4925),
    "Nova Scotia, Canada": (44.6820, -63.7443),
    "Quebec, Canada": (46.8139, -71.2080),
    "Canada": (56.1304, -106.3468),  # Geographic center
    # United Kingdom
    "London, England, United Kingdom": (51.5074, -0.1278),
    "London, United Kingdom": (51.5074, -0.1278),
    "London": (51.5074, -0.1278),
    "London, UK": (51.5074, -0.1278),
    "Mapbox UK": (51.5074, -0.1278),  # London
    "UK": (51.5074, -0.1278),  # London (representative of UK)
    "United Kingdom": (51.5074, -0.1278),  # London (representative of UK)
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
    "Saint-Denis, France": (48.9356, 2.3539),
    "Saint-Denis": (48.9356, 2.3539),
    "Aubervilliers, France": (48.9131, 2.3831),
    "Aubervilliers": (48.9131, 2.3831),
    "Nantes, France": (47.2184, -1.5536),
    "Nantes": (47.2184, -1.5536),
    "Lille, France": (50.6372, 3.0633),
    "Lille": (50.6372, 3.0633),
    "Marseille, France": (43.2965, 5.3698),
    "Marseille": (43.2965, 5.3698),
    "Montpellier, France": (43.6112, 3.8767),
    "Montpellier": (43.6112, 3.8767),
    "Anywhere in France": (46.2276, 2.2137),  # Geographic center of France
    "France": (46.2276, 2.2137),
    "Amsterdam, Netherlands": (52.3676, 4.9041),
    "Amsterdam": (52.3676, 4.9041),
    "Netherlands": (52.1326, 5.2913),  # Geographic center
    "Barcelona, Spain": (41.3851, 2.1734),
    "Barcelona": (41.3851, 2.1734),
    "Santa Oliva, Spain": (41.2533, 1.5514),
    "Santa Oliva, Tarragona, Spain": (41.2533, 1.5514),
    "Santa Oliva (Tarragona)": (41.2533, 1.5514),
    "Santa Oliva": (41.2533, 1.5514),
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
    "Lausanne, Switzerland": (46.5197, 6.6323),
    "Lausanne, CH": (46.5197, 6.6323),
    "Lausanne": (46.5197, 6.6323),
    "Geneva, Switzerland": (46.2044, 6.1432),
    "Geneva, CH": (46.2044, 6.1432),
    "Geneva": (46.2044, 6.1432),
    "Switzerland": (46.8182, 8.2275),  # Geographic center
    "Stockholm, Sweden": (59.3293, 18.0686),
    "Stockholm": (59.3293, 18.0686),
    "Sweden": (60.1282, 18.6435),  # Geographic center
    "Malmö, Sweden": (55.6059, 13.0007),
    "Malmö": (55.6059, 13.0007),
    "Helsinki, Finland": (60.1699, 24.9384),
    "Helsinki": (60.1699, 24.9384),
    "Malmoe, Sweden": (55.6059, 13.0007),
    "Malmoe": (55.6059, 13.0007),
    "Copenhagen, Denmark": (55.6761, 12.5683),
    "Copenhagen": (55.6761, 12.5683),
    "Aarhus, Denmark": (56.1629, 10.2039),
    "Aarhus": (56.1629, 10.2039),
    "Dublin, Ireland": (53.3498, -6.2603),
    "Dublin": (53.3498, -6.2603),
    "Brussels, Belgium": (50.8503, 4.3517),
    "Brussels": (50.8503, 4.3517),
    "Anywhere in Belgium": (50.8503, 4.3517),  # Brussels (center of Belgium)
    "Belgium": (50.8503, 4.3517),
    "Lisbon, Portugal": (38.7223, -9.1393),
    "Portugal": (39.3999, -8.2245),  # Geographic center
    "Lisbon": (38.7223, -9.1393),
    "Prague, Czech Republic": (50.0755, 14.4378),
    "Prague": (50.0755, 14.4378),
    "Bratislava, Slovakia": (48.1486, 17.1077),
    "Slovakia": (48.1486, 17.1077),
    "Cologne, Germany": (50.9375, 6.9603),
    "Cologne": (50.9375, 6.9603),
    "Köln, Germany": (50.9375, 6.9603),
    "Köln": (50.9375, 6.9603),
    "Bochum, Germany": (51.4817, 7.2165),
    "Bochum": (51.4817, 7.2165),
    "Hamburg, Germany": (53.5511, 9.9937),
    "Hamburg": (53.5511, 9.9937),
    "Warsaw, Poland": (52.2297, 21.0122),
    "Warsaw": (52.2297, 21.0122),
    "Krakow, Poland": (50.0647, 19.9449),
    "Wrocław, Poland": (51.1, 17.0333),
    "Wroclaw, Poland": (51.1, 17.0333),
    "Wrocław": (51.1, 17.0333),
    "Wroclaw": (51.1, 17.0333),
    "Kraków, Poland": (50.0647, 19.9449),
    "Krakow": (50.0647, 19.9449),
    "Kraków": (50.0647, 19.9449),
    "Mapbox Poland": (52.2297, 21.0122),  # Warsaw
    "Vilnius, Lithuania": (54.6872, 25.2797),
    "Vilnius": (54.6872, 25.2797),
    "Minsk, Belarus": (53.9045, 27.5615),
    "Minsk": (53.9045, 27.5615),
    "Mapbox Minsk": (53.9045, 27.5615),
    "Sofia, Bulgaria": (42.6977, 23.3219),
    "Sofia": (42.6977, 23.3219),
    "Skopje, North Macedonia": (41.9973, 21.4280),
    "Skopje, Macedonia": (41.9973, 21.4280),
    "Skopje": (41.9973, 21.4280),
    "Munich, Germany": (48.1351, 11.5820),
    "Munich": (48.1351, 11.5820),
    "Frankfurt, Germany": (50.1109, 8.6821),
    "Frankfurt": (50.1109, 8.6821),
    "Germany": (51.1657, 10.4515),  # Geographic center
    "Germany, North": (53.5511, 9.9937),  # Hamburg (representative of North)
    "Germany, West": (50.9375, 6.9603),  # Cologne (representative of West)
    "Luxembourg": (49.6116, 6.1319),
    "Luxembourg, Luxembourg": (49.6116, 6.1319),
    "Budapest, Hungary": (47.4979, 19.0402),
    "Budapest": (47.4979, 19.0402),
    "Bucharest, Romania": (44.4268, 26.1025),
    "Bucharest": (44.4268, 26.1025),
    "Iasi, Romania": (47.1585, 27.6014),
    "Iasi": (47.1585, 27.6014),
    "Iasi Office": (47.1585, 27.6014),
    "Croatia": (45.1000, 15.2000),  # Geographic center
    "Bosnia & Herzegovina": (43.9159, 17.6791),  # Geographic center
    "Bosnia and Herzegovina": (43.9159, 17.6791),
    "Italy": (41.8719, 12.5674),  # Rome (representative center)
    "Limerick, Ireland": (52.6638, -8.6267),
    "Limerick": (52.6638, -8.6267),
    "Ireland, United Kingdom": (
        53.4129,
        -8.2439,
    ),  # Dublin (though Ireland is not in UK, using Dublin coordinates)
    # Asia-Pacific
    "Singapore, Singapore": (1.3521, 103.8198),
    "Singapore": (1.3521, 103.8198),
    "APAC": (1.3521, 103.8198),  # Singapore (representative center of APAC region)
    "Asia-Pacific": (1.3521, 103.8198),
    "Tokyo, Japan": (35.6762, 139.6503),
    "Tokyo": (35.6762, 139.6503),
    "Mapbox Japan": (35.6762, 139.6503),  # Tokyo
    "Osaka, Japan": (34.6937, 135.5023),
    "Osaka": (34.6937, 135.5023),
    "Seoul, Korea": (37.5665, 126.978),
    "Seoul, South Korea": (37.5665, 126.978),
    "Seoul": (37.5665, 126.978),
    "Hong Kong, Hong Kong": (22.3193, 114.1694),
    "Hong Kong": (22.3193, 114.1694),
    "Shanghai, China": (31.2304, 121.4737),
    "Shanghai": (31.2304, 121.4737),
    "Beijing, China": (39.9042, 116.4074),
    "Beijing": (39.9042, 116.4074),
    "Chengdu, China": (30.6624, 104.0633),
    "Chengdu": (30.6624, 104.0633),
    "China": (35.8617, 104.1954),  # Geographic center
    "Bangalore, India": (12.9716, 77.5946),
    "Bangalore": (12.9716, 77.5946),
    "Mumbai, India": (19.076, 72.8777),
    "Mumbai": (19.076, 72.8777),
    "Delhi, India": (28.7041, 77.1025),
    "Delhi": (28.7041, 77.1025),
    "India": (20.5937, 78.9629),  # Geographic center
    "Karachi, Pakistan": (24.8607, 67.0011),
    "Karachi": (24.8607, 67.0011),
    "Lahore, Pakistan": (31.5204, 74.3587),
    "Lahore": (31.5204, 74.3587),
    "Islamabad, Pakistan": (33.6844, 73.0479),
    "Islamabad": (33.6844, 73.0479),
    "Sydney, Australia": (-33.8688, 151.2093),
    "Sydney": (-33.8688, 151.2093),
    "Melbourne, Australia": (-37.8136, 144.9631),
    "Melbourne": (-37.8136, 144.9631),
    "Brisbane, Australia": (-27.4698, 153.0251),
    "Brisbane": (-27.4698, 153.0251),
    "Australia": (-25.2744, 133.7751),  # Geographic center
    "Auckland, New Zealand": (-36.8485, 174.7633),
    "Auckland": (-36.8485, 174.7633),
    "New Zealand": (-40.9006, 174.8860),  # Geographic center
    "Wellington, New Zealand": (-41.2865, 174.7762),
    "Wellington": (-41.2865, 174.7762),
    # Latin America
    "LATAM": (-23.5505, -46.6333),  # São Paulo (representative center of Latin America)
    "Americas": (
        39.8283,
        -98.5795,
    ),  # Geographic center of US (representative of Americas)
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
    "Sharjah, United Arab Emirates": (25.35, 55.42),
    "Sharjah": (25.35, 55.42),
    "UAE": (24.4539, 54.3773),  # Abu Dhabi (capital, center of UAE)
    "United Arab Emirates": (24.4539, 54.3773),
    "Abu Dhabi": (24.4539, 54.3773),
    "Abu Dhabi, United Arab Emirates": (24.4539, 54.3773),
    "Beirut, Lebanon": (33.8938, 35.5018),
    "Beirut": (33.8938, 35.5018),
    "Middle East": (29.2985, 42.5509),  # Geographic center (Saudi Arabia)
    "Turkey": (38.9637, 35.2433),  # Geographic center
    "Doha, Qatar": (25.2854, 51.5310),
    "Doha": (25.2854, 51.5310),
    "Amman, Jordan": (31.9539, 35.9106),
    "Amman": (31.9539, 35.9106),
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
    "Alexandria, Egypt": (31.2001, 29.9187),
    "Alexandria": (31.2001, 29.9187),
    # Additional cities for office locations
    "Pune, India": (18.5204, 73.8567),
    "Pune": (18.5204, 73.8567),
    "Gurugram, India": (28.4089, 77.0378),
    "Gurugram": (28.4089, 77.0378),
    "Kuala Lumpur, Malaysia": (3.1390, 101.6869),
    "Kuala Lumpur": (3.1390, 101.6869),
    "Indonesia": (-0.7893, 113.9213),  # Geographic center
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
    "Taiwan, Hsinchu": (24.8036, 120.9686),
    "Hsinchu, Taiwan": (24.8036, 120.9686),
    "Hsinchu": (24.8036, 120.9686),
    "BR, SP, Cajamar": (-23.3550, -46.8789),
    "IN, TS, Virtual": (17.3850, 78.4867),
    "FR, Satolas-et-bonce": (45.7310, 5.0910),
    "PL, Gdansk": (54.3520, 18.6466),
    "DE, TH, Erfurt": (50.9848, 11.0299),
    "CR, H, Heredia": (9.9986, -84.1170),
    "FR, Courbevoie": (48.8976, 2.2567),
    "IN, HR, Gurgaon": (28.4595, 77.0266),
    "BR, MG, Betim": (-19.9670, -44.1970),
    "FR, Clichy": (48.9039, 2.3060),
    "MX, MEX, Cuautitlan Izcalli": (19.6460, -99.2470),
    "CR, Virtual": (9.9281, -84.0907),
    "ES, Zaragoza": (41.6488, -0.8891),
    "FR, Augny": (49.0760, 6.1290),
    "JP, 14, Sagamihara": (35.5710, 139.3730),
    "DE, Helmstedt": (52.2270, 11.0090),
    "FR, Bretigny Sur Orge": (48.6100, 2.3070),
    "GB, NTH, Kettering": (52.3980, -0.7270),
    "GB, Tilbury": (51.4630, 0.3580),
    "DE, Kaiserslautern": (49.4400, 7.7490),
    "GB, Minworth": (52.5300, -1.7800),
    "JP, 11, Saitama": (35.8617, 139.6455),
    "JP, 27, Sakai": (34.5733, 135.4828),
    "NL, Den Haag": (52.0705, 4.3007),
    "PL, Swiebodzin": (52.2470, 15.5330),
    "DE, NW, Horn-bad Meinberg": (51.8700, 8.9830),
    "ES, Figueres": (42.2670, 2.9610),
    "FR, Senlis": (49.2070, 2.5860),
    "JP, 11, Sayama": (35.8520, 139.4130),
    "SE, Eskilstuna": (59.3710, 16.5090),
    "BR, SP, Osasco": (-23.5320, -46.7910),
    "DE, HE, Bad Hersfeld": (50.8720, 9.7080),
    "DE, NW, Dortmund": (51.5136, 7.4653),
    "GB, NTT, Mansfield": (53.1430, -1.1990),
    "IN, GJ, Ahmedabad": (23.0225, 72.5714),
    "IN, MH, Thane": (19.2183, 72.9781),
    "JP, 12, Ichikawa": (35.7190, 139.9310),
    "JP, 12, Nagareyama-shi": (35.8560, 139.9020),
    "MX, Cuauhtémoc": (19.4270, -99.1670),
    "AU, NSW, Kemps Creek": (-33.8490, 150.7640),
    "DE, BE, Mittenwalde": (52.2570, 13.5360),
    "DE, BW, Pforzheim": (48.8910, 8.6980),
    "FR, 42, Montluel": (45.8500, 5.0500),
    "GB, NBL, Northampton": (52.2405, -0.9027),
    "IN, UP, Noida": (28.5355, 77.3910),
    "MX, NLE, Apodaca": (25.7800, -100.1880),
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
    "6105 Tennyson Pkwy, Suite 300, Plano TX 75024": (
        33.0198,
        -96.6989,
    ),  # Plano, TX coordinates
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
    "Any location": (
        39.8283,
        -98.5795,
    ),  # Geographic center of US (for flexible location jobs)
    "Multiple Locations": (
        39.8283,
        -98.5795,
    ),  # Geographic center of US (for jobs in multiple locations)
    "USA": (39.8283, -98.5795),  # Geographic center of US
    "USA | Relocate": (39.8283, -98.5795),  # Geographic center of US
    "United States": (39.8283, -98.5795),
    "United Stated": (39.8283, -98.5795),  # Handle typo variant
    "US": (39.8283, -98.5795),  # Short form of United States
    "North America": (
        39.8283,
        -98.5795,
    ),  # Geographic center of US (representative of North America)
    "East Coast": (40.7128, -74.006),  # New York City (representative of East Coast)
    "West Coast": (37.7749, -122.4194),  # San Francisco (representative of West Coast)
    "Western Region": (
        37.7749,
        -122.4194,
    ),  # San Francisco (representative of Western Region)
    "Bay Area or Remote": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "Bay Area": (37.7749, -122.4194),  # San Francisco (Bay Area)
    "NY or SF": (39.0608, -98.3284),  # Midpoint between New York and San Francisco
    "SF or NY": (39.0608, -98.3284),  # Midpoint between New York and San Francisco
    "Ohio or Tennesse": (38.1574, -84.5681),  # Midpoint between Ohio and Tennessee
    "Europe": (50.8503, 4.3517),  # Brussels (central point of Europe)
    "EMEA": (
        50.8503,
        4.3517,
    ),  # Brussels (representative center of Europe, Middle East, and Africa)
    "São Paolo": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "São Paolo, Brazil": (-23.5505, -46.6333),  # São Paulo (fix typo variant)
    "India - Remote": (28.7041, 77.1025),  # Delhi (center of India)
    "Remote - India": (28.7041, 77.1025),  # Delhi (center of India)
    "Hawthorne, CA": (33.9164, -118.3526),
    "Starbase, TX": (25.9971, -97.1566),
    "Cape Canaveral, FL": (28.3922, -80.6077),
    "Vandenberg, CA": (34.7420, -120.5724),
    "Woodinville, WA": (47.7543, -122.1635),
    "McGregor, TX": (31.4438, -97.4094),
    # Missing locations from missing_locations.json
    "Belo Horizonte, State of Minas Gerais, Brazil": (-19.9167, -43.9345),
    "Ho Chi Minh City, Vietnam": (10.8231, 106.6297),
    "Costa Rica, San José, San José": (9.9281, -84.0907),
    "Hanoi, Vietnam": (21.0285, 105.8542),
    "Greece, Attica, Athens": (37.9838, 23.7275),
    "Haifa, Israel": (32.7940, 34.9896),
    "Nong Yai, Nong Yai District, Chon Buri, Thailand": (13.1614, 101.0025),
    "Vietnam, Ho Chi Minh City, Ho Chi Minh City": (10.8231, 106.6297),
    "Estonia, Harjumaa, Tallinn": (59.4370, 24.7536),
    "Norway, Oslo, Oslo": (59.9139, 10.7522),
    "Malaysia, Selangor, Cyberjaya": (2.9213, 101.6559),
    "Mexico, Querétaro, Querétaro City": (20.5888, -100.3899),
    "Türkiye, Istanbul, Istanbul": (41.0082, 28.9784),
    "Zhubei, Zhubei City, Hsinchu County, Taiwan": (24.8297, 121.0115),
    "Arlington, VA": (38.8816, -77.0910),
    "Canandaigua, NY": (42.8875, -77.2814),
    "Inzai, Chiba, Japan": (35.8349, 140.1444),
    "Istanbul, İstanbul, Türkiye": (41.0082, 28.9784),
    "Kuwait City, Kuwait": (29.3759, 47.9774),
    "Long Beach, CA": (33.7701, -118.1937),
    "Sungai Buloh, Selangor, Malaysia": (3.2098, 101.5760),
    "Vietnam, Ha Noi - Capital, Hanoi": (21.0285, 105.8542),
    "Costa Rica": (9.7489, -83.7534),  # Geographic center
    "Malaysia, Johor, Johor Bahru": (1.4927, 103.7414),
    "Nairobi, Kenya": (-1.2921, 36.8219),
    "Antwerp": (51.2194, 4.4025),
    "Athens, Greece": (37.9838, 23.7275),
    "Berkeley, California": (37.8715, -122.2730),
    "Brazil, Distrito Federal, Brasilia": (-15.7942, -47.8822),
    "Centurion, South Africa": (-25.8603, 28.1896),
    "Clearwater, FL": (27.9659, -82.8001),
    "Colombia, Distrito Capital, Bogota": (4.7110, -74.0721),
    "Fort Meade, MD": (39.1084, -76.7436),
    "Guam": (13.4443, 144.7937),  # Geographic center
    "Hamina, Finland": (60.5702, 27.1979),
    "Hanoi": (21.0285, 105.8542),
    "Ho Chi Minh City, Ho Chi Minh City, Vietnam": (10.8231, 106.6297),
    "Ho Chi Minh, Ho Chi Minh City, Vietnam": (10.8231, 106.6297),
    "Israel, Haifa, Haifa": (32.7940, 34.9896),
    "Kenya, Nairobi City, Nairobi": (-1.2921, 36.8219),
    "Latvia, Riga, Riga": (56.9496, 24.1052),
    "Oslo, Norway": (59.9139, 10.7522),
    "Peru, Lima, Lima": (-12.0464, -77.0428),
    "Puyan, Puyan Township, Changhua County, Taiwan": (24.0167, 120.5167),
    "Ramat Gan, Israel": (32.0809, 34.8142),
    "Saint Vulbas": (45.8333, 5.2833),
    "Saudi Arabia, Eastern Province, Dammam": (26.4207, 50.0888),
    "Skien, Norway": (59.2096, 9.6090),
    "South Africa, KwaZulu-Natal, Durban": (-29.8587, 31.0218),
    "Viby, Denmark": (56.1278, 10.1606),
    "Vietnam, Ho Chi Minh City, Ho Chi Minh City, Vietnam, Ha Noi - Capital, Hanoi": (
        10.8231,
        106.6297,
    ),  # Using Ho Chi Minh City coordinates
    "Xianxi, Xianxi Township, Changhua County, Taiwan": (24.0167, 120.5167),
    "Colombia, Distrito Capital, Bogota, Colombia, Antioquia, Medellín, Peru, Lima, Lima, Ecuador, Pichincha, Quito": (
        4.7110,
        -74.0721,
    ),  # Using Bogota coordinates as primary
    # Missing locations from terminal output
    "Beaverton": (45.4871, -122.8038),
    "Beaverton, Oregon": (45.4871, -122.8038),
    "Beaverton, OR": (45.4871, -122.8038),
    "Herzliya": (32.1624, 34.8447),
    "Herzliya, Israel": (32.1624, 34.8447),
    "Culver City": (34.0211, -118.3965),
    "Culver City, California": (34.0211, -118.3965),
    "Culver City, CA": (34.0211, -118.3965),
    "Waltham": (42.3765, -71.2356),
    "Waltham, Massachusetts": (42.3765, -71.2356),
    "Waltham, MA": (42.3765, -71.2356),
    "Cork": (51.8985, -8.4756),
    "Cork, Ireland": (51.8985, -8.4756),
    "Suzhou": (31.2989, 120.5853),
    "Suzhou, China": (31.2989, 120.5853),
    "SF / NY": (39.0608, -98.3284),  # Midpoint between SF and NY
    "Cary": (35.7915, -78.7811),
    "Cary, North Carolina": (35.7915, -78.7811),
    "Cary, NC": (35.7915, -78.7811),
    "Longtan": (24.8647, 121.2075),
    "Longtan, Taiwan": (24.8647, 121.2075),
    "New Albany, OH": (40.0817, -82.8088),
    "New Albany, Ohio": (40.0817, -82.8088),
    "Rayville, LA": (32.4774, -91.7554),
    "Rayville, Louisiana": (32.4774, -91.7554),
    "Montgomery, AL": (32.3668, -86.3000),
    "Montgomery, Alabama": (32.3668, -86.3000),
    "Montgomery": (32.3668, -86.3000),
    "Yokohama": (35.4437, 139.6380),
    "Yokohama, Japan": (35.4437, 139.6380),
    "Macao": (22.1987, 113.5439),
    "Macau": (22.1987, 113.5439),
    "Newton County, GA": (33.5557, -83.8502),
    "Newton County, Georgia": (33.5557, -83.8502),
    "SF Office - 171 2nd, 4th floor": (37.7749, -122.4194),  # San Francisco
    "Fort Worth, TX": (32.7555, -97.3308),
    "Fort Worth, Texas": (32.7555, -97.3308),
    "Fort Worth": (32.7555, -97.3308),
    "Kuna, ID": (43.4918, -116.4200),
    "Kuna, Idaho": (43.4918, -116.4200),
    "Kuna": (43.4918, -116.4200),
    "Los Lunas, NM": (34.8068, -106.7333),
    "Los Lunas, New Mexico": (34.8068, -106.7333),
    "Los Lunas": (34.8068, -106.7333),
    "Makati City": (14.5547, 121.0244),
    "Makati City, Philippines": (14.5547, 121.0244),
    "Turkiye": (38.9637, 35.2433),  # Geographic center
    "Altoona, IA": (41.6472, -93.4647),
    "Altoona, Iowa": (41.6472, -93.4647),
    "Altoona": (41.6472, -93.4647),
    "El Paso, TX": (31.7619, -106.4850),
    "El Paso, Texas": (31.7619, -106.4850),
    "El Paso": (31.7619, -106.4850),
    "ID, Jakarta": (-6.2088, 106.8456),
    "Jakarta": (-6.2088, 106.8456),
    "Jakarta, Indonesia": (-6.2088, 106.8456),
    "Livorno": (43.5500, 10.3100),
    "Livorno, Italy": (43.5500, 10.3100),
    "PH, Pasay City": (14.5378, 120.9972),
    "Pasay City": (14.5378, 120.9972),
    "Pasay City, Philippines": (14.5378, 120.9972),
    "Papillion, NE": (41.1544, -96.0425),
    "Papillion, Nebraska": (41.1544, -96.0425),
    "Papillion": (41.1544, -96.0425),
    "Aiken, SC": (33.5604, -81.7195),
    "Aiken, South Carolina": (33.5604, -81.7195),
    "Aiken": (33.5604, -81.7195),
    "CO, Bogota": (4.7110, -74.0721),
    "Henrico, VA": (37.5407, -77.4360),
    "Henrico, Virginia": (37.5407, -77.4360),
    "Henrico": (37.5407, -77.4360),
    "IT, BG, Cividate Al Piano": (45.5833, 9.8333),
    "Cividate Al Piano": (45.5833, 9.8333),
    "Cividate Al Piano, Italy": (45.5833, 9.8333),
    "Nabern": (48.6167, 9.5167),
    "Nabern, Germany": (48.6167, 9.5167),
    "SA, Jeddah": (21.4858, 39.1925),
    "Jeddah": (21.4858, 39.1925),
    "Jeddah, Saudi Arabia": (21.4858, 39.1925),
}


def normalize_location_by_company(location_str: str, company_name: str) -> str:
    """
    Normalize location string based on company-specific rules.

    Args:
        location_str: Original location string
        company_name: Company name

    Returns:
        Normalized location string
    """
    if not location_str or not company_name:
        return location_str

    location_lower = location_str.lower().strip()
    company_lower = company_name.lower().strip()

    # Tavily: "All Locations - On Site" -> "New York"
    if company_lower == "tavily" and location_lower == "all locations - on site":
        return "New York"

    return location_str


def split_locations(location_str: str) -> List[str]:
    """
    Split location string by semicolon or pipe to handle multiple locations.
    Returns a list of location strings, trimmed of whitespace.
    """
    if not location_str:
        return [""]

    # Split by semicolon or pipe and strip whitespace from each location
    # Handle both ";" and "|" as separators
    if "|" in location_str:
        locations = [loc.strip() for loc in location_str.split("|") if loc.strip()]
    else:
        locations = [loc.strip() for loc in location_str.split(";") if loc.strip()]

    return locations if locations else [""]


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

    # Try to match locations with workplace type suffix like " (Hybrid)", " (In-Office)", " (Distributed)"
    workplace_type_match = re.search(
        r"^(.+?)\s*\((?:Hybrid|In-Office|In Office|Distributed)\)$",
        location_str,
        re.IGNORECASE,
    )
    if workplace_type_match:
        base_location = workplace_type_match.group(1).strip()
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


# AI companies default map: normalized company name -> ATS type
# (None means search all ATS systems; ATS type limits search to that system)
AI_COMPANIES_DEFAULT = {
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
    "physical intelligence": None,
    "prime intellect": None,
    "replit": None,
    "notion": None,
    "ramp": None,
    "browserbase": None,
    "anything": None,
    "astral": None,
    "axiom": None,
    "baseten": None,
    "braintrust": None,
    "claylabs": None,
    "clerk": None,
    "cluely": None,
    "codegen": None,
    "coder": None,
    "compound": None,
    "confluent": None,
    "convex dev": None,
    "david ai": None,
    "deel": None,
    "delve": None,
    "docker": None,
    "eightsleep": None,
    "fyxer": None,
    "greptile": None,
    "gumloop": None,
    "harvey": None,
    "inkeep": None,
    "interation": None,
    "julius": None,
    "kilocode": None,
    "langdock": None,
    "langfuse": None,
    "lime": None,
    "magicpatterns": None,
    "mapbox": None,
    "mem0": None,
    "mintlify": None,
    "posthog": None,
    "profound": None,
    "pylon labs": None,
    "retell ai": None,
    "revenuecat": None,
    "sentry": None,
    "sfcompute": None,
    "sierra": None,
    "statista": None,
    "stytch": None,
    "substack": None,
    "supabase": None,
    "tavily": None,
    "telli": None,
    "taktile": None,
    "the browser company": None,
    "vapi": None,
    "vizcom": None,
    "warp": None,
    "wordware": None,
    "airbnb": None,
    "algolia": None,
    "baselayer": None,
    "beyondtrust": None,
    "bitly": None,
    "boxinc": None,
    "brave": None,
    "brex": None,
    "careem": None,
    "cloudflare": None,
    "coursera": None,
    "dataiku": None,
    "databricks": None,
    "duolingo": None,
    "faire": None,
    "figma": None,
    "figure ai": None,
    "gitlab": None,
    "intercom": None,
    "Isomorphic labs": None,
    "jane street": None,
    "neuralink": None,
    "nintendo": None,
    "pagerduty": None,
    "planet scale": None,
    "postman": None,
    "proton": None,
    "reddit": None,
    "stackblitz": None,
    "strava": None,
    "synthesia": None,
    "thinking machines": None,
    "twilio": None,
    "twitch": None,
    "whoop": None,
    "stripe": None,
    "snapchat": None,
    "shopify": None,
    "slack": None,
    "square": None,
    "sumup": None,
    "space x": None,
    "Optiver": None,
    "oklo": None,
    "ngrok": None,
    "newrelic": None,
    "netlify": None,
    "neon": None,
    "mozilla": None,
    "passes": None,
    "paypaycard": None,
    "redis": None,
    "reliant": None,
    "samsung research": None,
    "starcloud": None,
    "tripadvisor": None,
    "typeform": None,
    "vercel": None,
    "1password": None,
    "Alice Bob": None,
    "daedalean": None,
    "deepjudge": None,
    "nominal": None,
    "pigment": None,
    "plaid": None,
    "quantco": None,
    "scaleway": None,
    "sonar": None,
    "veepee": None,
    "wahed": None,
    "workos": None,
    "sana": None,
    "sanity": None,
    "sardine": None,
    "sieve": None,
    "speckle": None,
    "stack ai": None,
    "statsig": None,
    "zip": None,
    "yazio": None,
    "voodoo": None,
    "twenty": None,
    "turbopuffer": None,
    "tldraw": None,
    "tabs": None,
    "synthflow": None,
    "svix": None,
    "superhuman": None,
    "roo code": None,
    "riza": None,
    "resend": None,
    "render": None,
    "reacher": None,
    "ravio": None,
    "quora": None,
    "polar": None,
    "phare": None,
    "magic.dev": None,
    "magentic": None,
    "lottie": None,
    "lmarena": None,
    "llamaindex": None,
    "hud": None,
    "gptzero": None,
    "general intelligence": None,
    "fizz": None,
    # Big tech custom scrapers
    "google": None,
    "microsoft": None,
    "nvidia": None,
    "amazon": None,
    "cursor": None,
    "meta": None,
    "apple": None,
    "uber": None,
}

AI_COMPANIES_FILE = ROOT_DIR / "ai_companies.json"


def _normalize_ai_company_map(
    raw_map: Dict[str, Optional[str]],
) -> Dict[str, Optional[str]]:
    normalized: Dict[str, Optional[str]] = {}
    for name, ats in raw_map.items():
        key = normalize_company_name(name)
        normalized[key] = ats
    return normalized


def load_ai_companies() -> Dict[str, Optional[str]]:
    mapping = _normalize_ai_company_map(AI_COMPANIES_DEFAULT)
    if AI_COMPANIES_FILE.exists():
        try:
            with open(AI_COMPANIES_FILE, "r", encoding="utf-8") as f:
                user_map = json.load(f)
            if isinstance(user_map, dict):
                for name, ats in user_map.items():
                    key = normalize_company_name(str(name))
                    mapping[key] = ats
        except Exception as e:
            print(f"Error loading {AI_COMPANIES_FILE}: {e}", file=sys.stderr)
    return mapping


def save_ai_companies(mapping: Dict[str, Optional[str]]) -> None:
    try:
        AI_COMPANIES_FILE.write_text(
            json.dumps(mapping, indent=2, ensure_ascii=False), encoding="utf-8"
        )
    except Exception as e:
        print(f"Error saving {AI_COMPANIES_FILE}: {e}", file=sys.stderr)


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
    - amazon:     job['createdDate'] (Unix timestamp in seconds)
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

        elif ats_type == "amazon":
            created_date = raw_job.get("createdDate")
            if created_date:
                try:
                    timestamp = float(created_date)
                    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    return normalize_datetime_to_utc_iso(dt)
                except Exception:
                    return None

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

            # Handle multiple locations separated by semicolon
            location_str = job.location or ""
            location_str = normalize_location_by_company(location_str, company_name)
            locations = split_locations(location_str)

            job_url = job.job_url or job.apply_url or ""
            posted_at = None
            if job_url:
                raw_job = raw_jobs_by_url.get(job_url, {})
                posted_at = posted_at_from_source("ashby", raw_job)

            # Create a job entry for each location
            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": job_url,
                        "title": (job.title or "").strip(),
                        "location": loc,
                        "company": company_name,
                        "ats_id": job.id,
                        "ats_type": "ashby",
                        "salary_currency": comp_data["salary_currency"],
                        "salary_period": comp_data["salary_period"],
                        "salary_summary": comp_data["salary_summary"],
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except (json.JSONDecodeError, ValidationError) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def log_cloudflare_extraction_failure(
    job_url: str,
    job_title: str,
    original_location: str,
    workplace_type: str,
    description: Optional[str],
    job: Optional[GreenhouseJob] = None,
) -> None:
    """
    Log Cloudflare location extraction failures to a file for analysis.

    Args:
        job_url: Job URL
        job_title: Job title
        original_location: Original location string from job
        workplace_type: Workplace type (Hybrid, In-Office, Distributed)
        description: Job description content
        job: Optional GreenhouseJob object for metadata/offices info
    """
    try:
        # Extract a snippet of the description (first 500 chars) for analysis
        description_snippet = ""
        if description:
            decoded = html.unescape(description)
            # Remove HTML tags for cleaner snippet
            clean_desc = re.sub(r"<[^>]+>", " ", decoded)
            description_snippet = clean_desc[:500].strip()

        # Extract metadata information
        metadata_info = None
        offices_info = None
        if job:
            # Get "Job Posting Location" from metadata
            if job.metadata:
                for meta in job.metadata:
                    if meta.name and meta.name.lower() == "job posting location":
                        metadata_info = {
                            "name": meta.name,
                            "value": meta.value,
                            "value_type": meta.value_type,
                        }
                        break

            # Get offices information
            if job.offices:
                offices_info = []
                for office in job.offices:
                    offices_info.append(
                        {
                            "id": office.id,
                            "name": office.name,
                            "location": office.location,
                        }
                    )

        failure_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "job_url": job_url,
            "job_title": job_title,
            "original_location": original_location,
            "workplace_type": workplace_type,
            "description_snippet": description_snippet,
            "description_length": len(description) if description else 0,
            "metadata_job_posting_location": metadata_info,
            "offices": offices_info,
        }

        # Append to JSONL file (one JSON object per line)
        with open(CLOUDFLARE_FAILURES_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(failure_data, ensure_ascii=False) + "\n")
    except Exception as e:
        # Don't fail the main process if logging fails
        print(
            f"Warning: Failed to log Cloudflare extraction failure: {e}",
            file=sys.stderr,
        )


def extract_cloudflare_location_from_metadata(
    job: GreenhouseJob,
) -> Optional[str]:
    """
    Extract location from Cloudflare job metadata or offices fields.

    First tries to get location from metadata field with name "Job Posting Location",
    then falls back to offices field.

    Args:
        job: GreenhouseJob object

    Returns:
        Extracted location string or None if not found
    """
    # Try metadata field first - look for "Job Posting Location"
    if job.metadata:
        for meta in job.metadata:
            if meta.name and meta.name.lower() == "job posting location":
                if meta.value:
                    # Value can be a list or string
                    if isinstance(meta.value, list):
                        # Join multiple locations with semicolon
                        locations = [
                            str(v).strip() for v in meta.value if v and str(v).strip()
                        ]
                        if locations:
                            return "; ".join(locations)
                    elif isinstance(meta.value, str):
                        location = meta.value.strip()
                        if location:
                            return location

    # Fall back to offices field
    if job.offices:
        office_locations = []
        for office in job.offices:
            # Prefer office.location if available, otherwise use office.name
            if office.location:
                office_locations.append(office.location.strip())
            elif office.name:
                office_locations.append(office.name.strip())

        if office_locations:
            return "; ".join(office_locations)

    return None


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

            # Special handling for Cloudflare jobs with generic workplace types
            # Check if location contains any of these workplace types (handles cases like "Distributed; Hybrid")
            location_lower = location_str.lower().strip()
            is_generic_workplace = any(
                wt in location_lower
                for wt in ["hybrid", "in-office", "in office", "distributed"]
            )
            if company_name.lower() == "cloudflare" and is_generic_workplace:
                # Extract workplace type from location string
                # Handle cases like "Distributed; Hybrid" by taking the first one
                workplace_type = location_str.split(";")[0].strip()
                # Normalize "in office" to "In-Office"
                if workplace_type.lower() == "in office":
                    workplace_type = "In-Office"
                elif workplace_type.lower() == "in-office":
                    workplace_type = "In-Office"
                # Capitalize first letter for consistency (but preserve existing capitalization for known types)
                elif workplace_type and workplace_type.lower() in [
                    "hybrid",
                    "distributed",
                ]:
                    workplace_type = (
                        workplace_type[0].upper() + workplace_type[1:].lower()
                    )

                # Extract location from structured metadata/offices fields
                extracted_location = extract_cloudflare_location_from_metadata(job)
                if extracted_location:
                    # Split multiple locations and format each as "City (Workplace Type)"
                    locations_list = split_locations(extracted_location)
                    formatted_locations = [
                        f"{loc} ({workplace_type})" for loc in locations_list if loc
                    ]
                    location_str = (
                        "; ".join(formatted_locations)
                        if formatted_locations
                        else extracted_location
                    )
                else:
                    # Try description as fallback
                    fallback_location = None
                    if job.content:
                        decoded = html.unescape(job.content)
                        pattern = r"Available\s+Location(?:s)?\s*:\s*([^<]+?)(?:</[^>]+>|</strong>|</p>|$)"
                        match = re.search(
                            pattern, decoded, re.IGNORECASE | re.MULTILINE | re.DOTALL
                        )
                        if match:
                            location = match.group(1).strip()
                            location = re.sub(r"<[^>]+>", "", location)
                            location = html.unescape(location).strip()
                            location = location.rstrip(".,;")
                            if location:
                                fallback_location = location
                                # Split multiple locations and format each as "City (Workplace Type)"
                                locations_list = split_locations(location)
                                formatted_locations = [
                                    f"{loc} ({workplace_type})"
                                    for loc in locations_list
                                    if loc
                                ]
                                location_str = (
                                    "; ".join(formatted_locations)
                                    if formatted_locations
                                    else location
                                )

                    # Log failure if both metadata and description parsing failed
                    if not extracted_location and not fallback_location:
                        # location_str still contains the original "Hybrid"/"Distributed" value
                        # Log this failure for analysis
                        print(
                            f"⚠️  Cloudflare location extraction failed for: {job.title or 'Unknown'} - {job.absolute_url or 'No URL'}",
                            file=sys.stderr,
                        )
                        log_cloudflare_extraction_failure(
                            job_url=job.absolute_url or "",
                            job_title=job.title or "",
                            original_location=location_str,  # This is still "Hybrid"/"Distributed"
                            workplace_type=workplace_type,
                            description=job.content,
                            job=job,
                        )
                        # Note: location_str remains as "Hybrid"/"Distributed" and will show as missing coordinates

            # Normalize location based on company-specific rules
            location_str = normalize_location_by_company(location_str, company_name)

            # Handle multiple locations separated by semicolon
            locations = split_locations(location_str)

            # Greenhouse doesn't have compensation in the standard API response
            # but we can try to extract from metadata if available
            salary_summary = None
            if job.metadata:
                for meta in job.metadata:
                    if meta.name and "salary" in meta.name.lower():
                        salary_summary = str(meta.value)
                        break

            posted_at = posted_at_from_source("greenhouse", job_data)

            # Create a job entry for each location
            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": job.absolute_url or "",
                        "title": (job.title or "").strip(),
                        "location": loc,
                        "company": company_name,
                        "ats_id": str(job.id) if job.id is not None else "",
                        "ats_type": "greenhouse",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": salary_summary,
                        "experience": None,
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

            # Normalize location based on company-specific rules
            location_str = normalize_location_by_company(location_str, company_name)

            # Handle multiple locations separated by semicolon
            locations = split_locations(location_str)

            # Lever doesn't have compensation in the model, try to extract from raw data if available
            comp_data = {
                "salary_min": None,
                "salary_max": None,
                "salary_currency": None,
                "salary_period": None,
                "salary_summary": None,
            }

            posted_at = posted_at_from_source("lever", job_data)

            # Create a job entry for each location
            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": job.hostedUrl or job.applyUrl or "",
                        "title": (job.text or "").strip(),
                        "location": loc,
                        "company": company_name,
                        "ats_id": job.id or "",
                        "ats_type": "lever",
                        "salary_currency": comp_data["salary_currency"],
                        "salary_period": comp_data["salary_period"],
                        "salary_summary": comp_data["salary_summary"],
                        "experience": None,
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

            # Normalize location based on company-specific rules
            location_str = normalize_location_by_company(location_str, company_name)

            # Handle multiple locations separated by semicolon
            locations = split_locations(location_str)

            # Workable doesn't have salary in the model, set to None
            comp_data = {
                "salary_min": None,
                "salary_max": None,
                "salary_currency": None,
                "salary_period": None,
                "salary_summary": None,
            }

            posted_at = posted_at_from_source("workable", job_data)

            # Create a job entry for each location
            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": job.url or job.application_url or "",
                        "title": (job.title or "").strip(),
                        "location": loc,
                        "company": company_name,
                        "ats_id": str(job.shortcode or job.code or ""),
                        "ats_type": "workable",
                        "salary_currency": comp_data["salary_currency"],
                        "salary_period": comp_data["salary_period"],
                        "salary_summary": comp_data["salary_summary"],
                        "experience": None,
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


def fetch_fresh_data(
    company_name: str, ats_type: str, slug: str, force: bool = True
) -> bool:
    """
    Fetch fresh data for a company by calling the appropriate ATS scraping function.
    Returns True if data was fetched/updated, False otherwise.

    Args:
        company_name: Name of the company
        ats_type: Type of ATS (ashby, greenhouse, lever, workable, rippling)
        slug: Company slug/identifier
        force: If True, force scraping even if data was recently scraped (default: True)
    """
    try:
        if ats_type == "ashby":
            from ashby.main import scrape_ashby_jobs

            result = asyncio.run(
                scrape_ashby_jobs(slug, force=force, company_name=company_name)
            )
            was_scraped = (
                result is not None and result[2]
            )  # result[2] is was_scraped flag
            if was_scraped:
                print(
                    f"  ✓ Successfully fetched fresh data for {company_name} ({ats_type})"
                )
            else:
                print(
                    f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - data was scraped recently"
                )
            return was_scraped
        elif ats_type == "greenhouse":
            from greenhouse.main import scrape_greenhouse_jobs

            result = asyncio.run(
                scrape_greenhouse_jobs(slug, force=force, company_name=company_name)
            )
            was_scraped = (
                result is not None and result[2]
            )  # result[2] is was_scraped flag
            if was_scraped:
                print(
                    f"  ✓ Successfully fetched fresh data for {company_name} ({ats_type})"
                )
            else:
                print(
                    f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - data was scraped recently"
                )
            return was_scraped
        elif ats_type == "lever":
            from lever.main import scrape_lever_jobs

            result = asyncio.run(
                scrape_lever_jobs(slug, force=force, company_name=company_name)
            )
            was_scraped = (
                result is not None and result[2]
            )  # result[2] is was_scraped flag
            if was_scraped:
                print(
                    f"  ✓ Successfully fetched fresh data for {company_name} ({ats_type})"
                )
            else:
                print(
                    f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - data was scraped recently"
                )
            return was_scraped
        elif ats_type == "workable":
            from workable.main import scrape_workable_jobs

            result = asyncio.run(
                scrape_workable_jobs(slug, force=force, company_name=company_name)
            )
            was_scraped = (
                result is not None and result[2]
            )  # result[2] is was_scraped flag
            if was_scraped:
                print(
                    f"  ✓ Successfully fetched fresh data for {company_name} ({ats_type})"
                )
            else:
                print(
                    f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - data was scraped recently"
                )
            return was_scraped
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
                                url, force=force, company_name=company_name
                            )
                            was_scraped = result is not None
                            if was_scraped:
                                print(
                                    f"  ✓ Successfully fetched fresh data for {company_name} ({ats_type})"
                                )
                            else:
                                print(
                                    f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - data was scraped recently"
                                )
                            return was_scraped
            print(
                f"  ⊘ Skipped fetching for {company_name} ({ats_type}) - company URL not found"
            )
            return False
        else:
            print(f"Unknown ATS type: {ats_type}")
            return False
    except Exception as e:
        print(
            f"  ✗ Error fetching fresh data for {company_name} ({ats_type}): {e}",
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
            title = (job_data.get("title") or job_data.get("name") or "").strip()
            location = job_data.get("location") or job_data.get("city") or ""
            ats_id = str(job_data.get("id", ""))

            # Try to extract compensation
            comp_data = extract_compensation_data(job_data.get("compensation"))

            # Normalize location based on company-specific rules
            location = normalize_location_by_company(location, company_name)

            # Handle multiple locations separated by semicolon
            locations = split_locations(location)

            posted_at = posted_at_from_source("rippling", job_data)

            # Create a job entry for each location
            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": ats_id,
                        "ats_type": "rippling",
                        "salary_currency": comp_data["salary_currency"],
                        "salary_period": comp_data["salary_period"],
                        "salary_summary": comp_data["salary_summary"],
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except (json.JSONDecodeError, Exception) as e:
        print(f"Error parsing {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_google_jobs(json_file: Path, company_name: str = "Google") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            location = (job_data.get("location") or "").strip()

            if not url or not title:
                continue

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": url,
                        "ats_type": "google",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": None,
                    }
                )
    except Exception as e:
        print(f"Error parsing Google jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_tiktok_jobs(json_file: Path, company_name: str = "TikTok") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            location = (job_data.get("location") or "").strip()

            if not url or not title:
                continue

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": url,
                        "ats_type": "tiktok",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": None,
                    }
                )
    except Exception as e:
        print(f"Error parsing TikTok jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_microsoft_jobs(
    json_file: Path, company_name: str = "Microsoft"
) -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()

            if not url or not title:
                continue

            raw_locations = job_data.get("locations") or []
            if isinstance(raw_locations, list):
                location = (
                    " | ".join([str(x).strip() for x in raw_locations if x]) or ""
                )
            else:
                location = str(raw_locations).strip()

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            posted_at = job_data.get("posted_at")

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": job_data.get("eightfold_id") or url,
                        "ats_type": "microsoft",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except Exception as e:
        print(f"Error parsing Microsoft jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_nvidia_jobs(json_file: Path, company_name: str = "NVIDIA") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()

            if not url or not title:
                continue

            # NVIDIA stores locations as an array, same as Microsoft
            raw_locations = job_data.get("locations") or []
            if isinstance(raw_locations, list):
                location = (
                    " | ".join([str(x).strip() for x in raw_locations if x]) or ""
                )
            else:
                location = str(raw_locations).strip()

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            posted_at = job_data.get("posted_at")

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": url,
                        "ats_type": "nvidia",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except Exception as e:
        print(f"Error parsing NVIDIA jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_amazon_jobs(json_file: Path, company_name: str = "Amazon") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("urlNextStep") or "").strip()
            title = (job_data.get("title") or "").strip()
            location = (job_data.get("location") or "").strip()

            if not url or not title:
                continue

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            posted_at = posted_at_from_source("amazon", job_data)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": url,
                        "ats_type": "amazon",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except Exception as e:
        print(f"Error parsing Amazon jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_meta_jobs(json_file: Path, company_name: str = "Meta") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            ats_id = str(job_data.get("id") or url or "").strip()

            raw_locations = job_data.get("locations") or job_data.get("location")
            if isinstance(raw_locations, list):
                location_values = [str(loc).strip() for loc in raw_locations if loc]
            elif isinstance(raw_locations, str):
                location_values = [raw_locations.strip()]
            else:
                location_values = [
                    ""
                ]  # Let downstream missing-location handling catch it

            for raw_loc in location_values:
                location_str = normalize_location_by_company(raw_loc, company_name)
                for loc in split_locations(location_str):
                    lat, lon = get_coordinates(loc)
                    jobs.append(
                        {
                            "url": url,
                            "title": title,
                            "location": loc,
                            "company": company_name,
                            "ats_id": ats_id,
                            "ats_type": "meta",
                            "salary_currency": None,
                            "salary_period": None,
                            "salary_summary": None,
                            "experience": None,
                            "lat": lat,
                            "lon": lon,
                            "posted_at": job_data.get("updated_time"),
                        }
                    )
    except Exception as e:
        print(f"Error parsing Meta jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_cursor_jobs(json_file: Path, company_name: str = "Cursor") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            location = (job_data.get("location") or "").strip()

            if not url or not title:
                continue

            location_str = normalize_location_by_company(location, company_name)
            locations = split_locations(location_str)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": url,
                        "ats_type": "cursor",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": None,
                    }
                )
    except Exception as e:
        print(f"Error parsing Cursor jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_apple_jobs(json_file: Path, company_name: str = "Apple") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            ats_id = str(
                job_data.get("positionId") or job_data.get("id") or url
            ).strip()

            if not url or not title:
                continue

            # Handle multiple locations - prefer locations array, fallback to location string
            raw_locations = job_data.get("locations", [])
            if isinstance(raw_locations, list) and raw_locations:
                location_str = "; ".join(
                    [str(loc).strip() for loc in raw_locations if loc]
                )
            else:
                location_str = (job_data.get("location") or "").strip()

            if not location_str:
                location_str = "N/A"

            # Parse posted_at from postingDate
            posted_at = None
            posting_date = job_data.get("postingDate")
            if posting_date:
                try:
                    # Try ISO format first, then common formats
                    if "T" in posting_date or posting_date.endswith("Z"):
                        dt = datetime.fromisoformat(posting_date.replace("Z", "+00:00"))
                    else:
                        # Try common date formats
                        for fmt in [
                            "%Y-%m-%d",
                            "%m/%d/%Y",
                            "%d/%m/%Y",
                            "%Y-%m-%d %H:%M:%S",
                        ]:
                            try:
                                dt = datetime.strptime(posting_date, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            raise ValueError(f"Unknown date format: {posting_date}")
                    posted_at = normalize_datetime_to_utc_iso(dt)
                except Exception:
                    # If parsing fails, leave as None
                    pass

            location_str = normalize_location_by_company(location_str, company_name)
            locations = split_locations(location_str)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": ats_id,
                        "ats_type": "apple",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except Exception as e:
        print(f"Error parsing Apple jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def extract_uber_jobs(json_file: Path, company_name: str = "Uber") -> List[Dict]:
    jobs: List[Dict] = []
    try:
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        job_list = data.get("jobs", []) if isinstance(data, dict) else data
        if not isinstance(job_list, list):
            return jobs

        for job_data in job_list:
            url = (job_data.get("url") or "").strip()
            title = (job_data.get("title") or "").strip()
            ats_id = str(job_data.get("id") or url).strip()

            if not url or not title:
                continue

            # Handle multiple locations - prefer locations array, fallback to location string
            raw_locations = job_data.get("locations", [])
            if isinstance(raw_locations, list) and raw_locations:
                location_str = "; ".join(
                    [str(loc).strip() for loc in raw_locations if loc]
                )
            else:
                location_str = (job_data.get("location") or "").strip()

            if not location_str:
                location_str = "N/A"

            # Parse posted_at from creation_date or updated_date
            posted_at = None
            creation_date = job_data.get("creation_date") or job_data.get(
                "creationDate"
            )
            if creation_date:
                try:
                    # Try ISO format first
                    if "T" in creation_date or creation_date.endswith("Z"):
                        dt = datetime.fromisoformat(
                            creation_date.replace("Z", "+00:00")
                        )
                    else:
                        # Try common date formats
                        for fmt in [
                            "%Y-%m-%d",
                            "%m/%d/%Y",
                            "%d/%m/%Y",
                            "%Y-%m-%d %H:%M:%S",
                        ]:
                            try:
                                dt = datetime.strptime(creation_date, fmt)
                                break
                            except ValueError:
                                continue
                        else:
                            raise ValueError(f"Unknown date format: {creation_date}")
                    posted_at = normalize_datetime_to_utc_iso(dt)
                except Exception:
                    # If parsing fails, leave as None
                    pass

            location_str = normalize_location_by_company(location_str, company_name)
            locations = split_locations(location_str)

            for loc in locations:
                lat, lon = get_coordinates(loc)
                jobs.append(
                    {
                        "url": url,
                        "title": title,
                        "location": loc,
                        "company": company_name,
                        "ats_id": ats_id,
                        "ats_type": "uber",
                        "salary_currency": None,
                        "salary_period": None,
                        "salary_summary": None,
                        "experience": None,
                        "lat": lat,
                        "lon": lon,
                        "posted_at": posted_at,
                    }
                )
    except Exception as e:
        print(f"Error parsing Uber jobs from {json_file}: {e}", file=sys.stderr)

    return jobs


def gather_jobs_for_companies(
    company_names: List[str], ats_type: Optional[str] = None
) -> tuple[List[Dict], List[str]]:
    """Gather all jobs for the given company names.

    Returns:
        Tuple of (list of jobs, list of company names with no ATS matches)
    """
    all_jobs = []
    all_matches = []
    companies_without_ats = []

    # Predefined special-source companies (handled outside ATS via dedicated scrapers)
    special_source_files = {
        "google": ROOT_DIR / "google" / "google.json",
        "microsoft": ROOT_DIR / "microsoft" / "microsoft.json",
        "nvidia": ROOT_DIR / "nvidia" / "nvidia.json",
        "amazon": ROOT_DIR / "amazon" / "amazon.json",
        "meta": ROOT_DIR / "meta" / "meta.json",
        "cursor": ROOT_DIR / "cursor" / "cursor.json",
        "apple": ROOT_DIR / "apple" / "apple.json",
        "uber": ROOT_DIR / "uber" / "uber.json",
    }

    # Find all matching companies
    for company_name in company_names:
        matches = find_companies_by_name(company_name, ats_type)
        all_matches.extend(matches)
        if matches:
            print(f"Found {len(matches)} match(es) for '{company_name}':")
            for ats, slug, name in matches:
                print(f"  - {name} ({ats})")
        else:
            normalized = normalize_company_name(company_name)
            special_path = special_source_files.get(normalized)
            # Treat existing special-source JSON as a "match" so logs and
            # summaries reflect that this company is handled elsewhere.
            if special_path and special_path.exists():
                print(f"Found 1 match(es) for '{company_name}':")
                print(f"  - {company_name} (special-source)")
                continue

            # Track companies with no matches in any ATS or special source
            print(f"Found 0 match(es) for '{company_name}':")
            companies_without_ats.append(company_name)

    if not all_matches:
        print(f"No companies found matching: {', '.join(company_names)}")
        return all_jobs, companies_without_ats

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
                f"JSON file for {company_name} ({ats}) is stale (older than 1 hour), attempting to fetch fresh data..."
            )
            was_fetched = fetch_fresh_data(company_name, ats, slug)

            # Re-check the file path in case it was created/updated
            json_file = companies_dir / f"{slug}.json"
            if not json_file.exists():
                encoded_slug = quote(slug, safe="")
                json_file = companies_dir / f"{encoded_slug}.json"

            # Read and log the last_scraped timestamp to confirm it was updated
            if json_file.exists():
                try:
                    with open(json_file, "r", encoding="utf-8") as f:
                        data = json.load(f)
                        last_scraped_str = data.get("last_scraped")
                        if last_scraped_str:
                            try:
                                last_scraped = datetime.fromisoformat(last_scraped_str)
                                hours_ago = (
                                    datetime.now() - last_scraped
                                ).total_seconds() / 3600
                                if was_fetched:
                                    print(
                                        f"  → Data file updated with last_scraped: {last_scraped_str} ({hours_ago:.2f} hours ago)"
                                    )
                                else:
                                    print(
                                        f"  → Using existing data with last_scraped: {last_scraped_str} ({hours_ago:.2f} hours ago)"
                                    )
                            except (ValueError, TypeError):
                                if was_fetched:
                                    print(
                                        f"  → Data file updated (last_scraped field: {last_scraped_str})"
                                    )
                                else:
                                    print(
                                        f"  → Using existing data (last_scraped field: {last_scraped_str})"
                                    )
                        else:
                            if was_fetched:
                                print(
                                    f"  → Data file updated (no last_scraped field found)"
                                )
                            else:
                                print(
                                    f"  → Using existing data (no last_scraped field found)"
                                )
                except Exception as e:
                    print(
                        f"  ⚠ Warning: Could not read last_scraped from {json_file.name}: {e}",
                        file=sys.stderr,
                    )

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

        # Filter out dirty data: ignore listings where title contains "TEST" and company is nintendo
        jobs = [
            job
            for job in jobs
            if not (
                "TEST" in job.get("title", "").strip()
                and job.get("company", "").strip().lower() == "nintendo"
            )
        ]

        all_jobs.extend(jobs)
        print(f"  Extracted {len(jobs)} jobs")

    return all_jobs, companies_without_ats


def gather_special_source_jobs(company_names: List[str]) -> List[Dict]:
    jobs: List[Dict] = []
    normalized_map = {normalize_company_name(name): name for name in company_names}

    special_sources = {
        "google": {
            "module": "google.main",
            "scrape_func": "scrape_google_jobs",
            "extract_func": extract_google_jobs,
            "company_name": "Google",
        },
        "microsoft": {
            "module": "microsoft.main",
            "scrape_func": "scrape_microsoft_jobs",
            "extract_func": extract_microsoft_jobs,
            "company_name": "Microsoft",
        },
        "nvidia": {
            "module": "nvidia.main",
            "scrape_func": "scrape_nvidia_jobs",
            "extract_func": extract_nvidia_jobs,
            "company_name": "NVIDIA",
        },
        "amazon": {
            "module": "amazon.main",
            "scrape_func": "scrape_amazon_jobs",
            "extract_func": extract_amazon_jobs,
            "company_name": "Amazon",
        },
        "meta": {
            "module": "meta.main",
            "scrape_func": "scrape_meta",
            "extract_func": extract_meta_jobs,
            "company_name": "Meta",
        },
        "tiktok": {
            "module": "tiktok.main",
            "scrape_func": "scrape_tiktok_jobs",
            "extract_func": extract_tiktok_jobs,
            "company_name": "TikTok",
        },
        "cursor": {
            "module": "cursor.main",
            "scrape_func": "scrape_cursor_jobs",
            "extract_func": extract_cursor_jobs,
            "company_name": "Cursor",
        },
        "apple": {
            "module": "apple.main",
            "scrape_func": "scrape_apple_jobs",
            "extract_func": extract_apple_jobs,
            "company_name": "Apple",
        },
        "uber": {
            "module": "uber.main",
            "scrape_func": "scrape_uber_jobs",
            "extract_func": extract_uber_jobs,
            "company_name": "Uber",
        },
    }

    for normalized_name, original_name in normalized_map.items():
        if normalized_name not in special_sources:
            continue
        cfg = special_sources[normalized_name]
        try:
            module = __import__(cfg["module"], fromlist=[cfg["scrape_func"]])
            scrape_func = getattr(module, cfg["scrape_func"])
            json_path_str, _, _ = scrape_func(force=False)
            if json_path_str:
                json_path = Path(json_path_str)
                if json_path.exists():
                    extract_func = cfg["extract_func"]
                    jobs.extend(extract_func(json_path, cfg["company_name"]))
        except Exception as e:
            print(
                f"Error gathering {cfg['company_name']} jobs: {e}",
                file=sys.stderr,
            )

    return jobs


def enrich_jobs_with_description_data(jobs: List[Dict]) -> List[Dict]:
    """
    Enrich jobs with salary (if missing) and experience (always) extracted from descriptions.

    Args:
        jobs: List of job dictionaries

    Returns:
        List of enriched job dictionaries with salary (if missing) and experience fields
    """
    total_jobs = len(jobs)
    print(
        f"\n🔍 Enriching {total_jobs} jobs with salary and experience from descriptions..."
    )

    enriched_count = 0
    salary_extracted_count = 0
    experience_extracted_count = 0

    for idx, job in enumerate(jobs):
        if (idx + 1) % 100 == 0:
            print(f"  Processing job {idx + 1}/{total_jobs}...")

        # Coerce core fields to strings to avoid attribute errors when some
        # scrapers store ids as integers or other types.
        job_url = str(job.get("url", "") or "").strip()
        company = str(job.get("company", "") or "").strip()
        title = str(job.get("title", "") or "").strip()
        ats_id = str(job.get("ats_id", "") or "").strip()
        ats_type = str(job.get("ats_type", "") or "").strip()

        if not job_url or not company or not title:
            continue

        # Get job description
        description, _ = get_job_description_fast(
            job_url, company, title, ats_id, ats_type
        )

        if not description:
            # Initialize experience as None if description not found
            job["experience"] = None
            continue

        # Extract salary if summary is missing
        salary_summary = job.get("salary_summary")

        if not salary_summary and description:
            salary_str, _ = extract_salary_from_description(description)
            if salary_str:
                # Use the extracted salary string as the summary
                job["salary_summary"] = salary_str
                # Also extract currency if not set
                parsed_min, parsed_max, parsed_currency = parse_salary(salary_str)
                if parsed_currency and not job.get("salary_currency"):
                    job["salary_currency"] = parsed_currency
                salary_extracted_count += 1

        # Always extract experience
        experience_years, _ = extract_experience_from_description(description)
        job["experience"] = (
            str(experience_years) if experience_years is not None else None
        )
        if experience_years is not None:
            experience_extracted_count += 1

        enriched_count += 1

    print(f"✅ Enriched {enriched_count} jobs:")
    print(f"   - Extracted salary for {salary_extracted_count} jobs")
    print(f"   - Extracted experience for {experience_extracted_count} jobs")

    return jobs


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


def find_removed_jobs(current_csv: Path, previous_csv: Path) -> List[Dict]:
    """
    Compare two CSV files and return jobs from previous_csv that don't exist in current_csv.
    Jobs are compared by URL. This is the reverse of find_new_jobs().
    """
    # Read current CSV URLs
    current_urls = set()
    try:
        with open(current_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                if url:
                    current_urls.add(url)
    except Exception as e:
        print(f"Error reading current CSV {current_csv}: {e}", file=sys.stderr)
        return []

    # Read previous CSV and find removed jobs
    removed_jobs = []
    try:
        with open(previous_csv, "r", encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get("url", "").strip()
                if url and url not in current_urls:
                    removed_jobs.append(row)
    except Exception as e:
        print(f"Error reading previous CSV {previous_csv}: {e}", file=sys.stderr)
        return []

    return removed_jobs


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
    ai_companies_map: Dict[str, Optional[str]] = {}
    if args.ai_companies or not args.companies:
        ai_companies_map = load_ai_companies()
        companies_to_search = list(ai_companies_map.keys())
        print(f"Using AI companies list ({len(companies_to_search)} companies)")
        # If using AI companies, respect the ATS mapping (but allow override with --ats)
        if not args.ats:
            # Process companies grouped by ATS type for efficiency
            companies_by_ats: Dict[Optional[str], List[str]] = {}
            for company_name in companies_to_search:
                ats_type = ai_companies_map.get(normalize_company_name(company_name))
                if ats_type not in companies_by_ats:
                    companies_by_ats[ats_type] = []
                companies_by_ats[ats_type].append(company_name)

            # Gather jobs for each ATS group
            all_jobs: List[Dict] = []
            all_companies_without_ats: List[str] = []
            for ats_type, company_list in companies_by_ats.items():
                jobs_group, companies_without_ats_group = gather_jobs_for_companies(
                    company_list, ats_type
                )
                all_jobs.extend(jobs_group)
                all_companies_without_ats.extend(companies_without_ats_group)
            jobs = all_jobs
            companies_without_ats = all_companies_without_ats
        else:
            # --ats override: search all companies in specified ATS
            jobs, companies_without_ats = gather_jobs_for_companies(
                companies_to_search, args.ats
            )
    else:
        companies_to_search = args.companies
        jobs, companies_without_ats = gather_jobs_for_companies(
            companies_to_search, args.ats
        )

    # Add jobs from special non-ATS sources like Google Careers and TikTok
    if companies_to_search:
        special_jobs = gather_special_source_jobs(companies_to_search)
        if special_jobs:
            jobs.extend(special_jobs)

        # Remove special-source companies from the "without ATS" summary
        normalized_without = {
            normalize_company_name(name): name for name in companies_without_ats
        }
        for special in (
            "google",
            "microsoft",
            "nvidia",
            "amazon",
            "meta",
            "tiktok",
            "apple",
            "uber",
        ):
            normalized_without.pop(special, None)
        companies_without_ats = list(normalized_without.values())

    # If we used the AI companies list, learn ATS mappings from the jobs we actually found
    if (args.ai_companies or not args.companies) and jobs:
        if not ai_companies_map:
            ai_companies_map = load_ai_companies()
        ats_by_company: Dict[str, set[str]] = {}
        for job in jobs:
            company = job.get("company", "").strip()
            ats = (job.get("ats_type") or "").strip()
            if not company or not ats or ats not in ATS_CONFIGS:
                continue
            key = normalize_company_name(company)
            if key not in ats_by_company:
                ats_by_company[key] = set()
            ats_by_company[key].add(ats)
        for key, ats_set in ats_by_company.items():
            if len(ats_set) == 1:
                ats_value: Optional[str] = next(iter(ats_set))
            else:
                ats_value = None
            ai_companies_map[key] = ats_value
        if ai_companies_map:
            save_ai_companies(ai_companies_map)

    if not jobs:
        print("No jobs found.")
        # Print summary of companies without ATS even when no jobs found
        if companies_without_ats:
            print(
                f"\n📊 Summary: {len(companies_without_ats)} company/companies without ATS found:"
            )
            for company in sorted(companies_without_ats):
                print(f"   - {company}")
        else:
            print("\n✅ All companies found matching ATS systems")
        return

    # Enrich jobs with salary (if missing) and experience (always) from descriptions
    jobs = enrich_jobs_with_description_data(jobs)

    # Count jobs with missing locations
    missing_location_jobs = [
        job for job in jobs if job.get("lat") is None or job.get("lon") is None
    ]
    missing_locations = len(missing_location_jobs)

    if missing_locations > 0:
        print(f"\n⚠️  {missing_locations} job(s) with missing location coordinates")

        # Collect unique missing location strings and track empty/null separately
        unique_missing_locations = {}
        empty_null_location_jobs = []
        for job in missing_location_jobs:
            location = job.get("location", "").strip()
            if location:
                if location not in unique_missing_locations:
                    unique_missing_locations[location] = 0
                unique_missing_locations[location] += 1
            else:
                # Track jobs with empty/null locations
                empty_null_location_jobs.append(job)

        empty_null_count = len(empty_null_location_jobs)

        # Print sample of jobs with empty/null locations
        if empty_null_count > 0:
            print(
                f"\n   📋 Sample of {min(10, empty_null_count)} jobs with empty/null locations:"
            )
            for i, job in enumerate(empty_null_location_jobs[:10], 1):
                company = job.get("company", "Unknown")
                title = job.get("title", "Unknown")
                url = job.get("url", "N/A")
                ats_type = job.get("ats_type", "N/A")
                print(f"     {i}. {company} - {title} ({ats_type})")
                print(
                    f"        URL: {url[:80]}..."
                    if len(url) > 80
                    else f"        URL: {url}"
                )
            if empty_null_count > 10:
                print(
                    f"     ... and {empty_null_count - 10} more jobs with empty/null locations"
                )

        if unique_missing_locations:
            print(f"\n   Missing locations ({len(unique_missing_locations)} unique):")
            # Sort by count (descending) then by location name
            sorted_locations = sorted(
                unique_missing_locations.items(), key=lambda x: (-x[1], x[0])
            )
            # Show top 50 most common missing locations
            for location, count in sorted_locations[:50]:
                print(f"     - {location} ({count} job(s))")
            if len(sorted_locations) > 20:
                print(
                    f"     ... and {len(sorted_locations) - 20} more unique locations"
                )

        # Write missing locations to file
        missing_locations_file = ROOT_DIR / "missing_locations.json"
        missing_data = {
            "total_jobs_with_missing_locations": missing_locations,
            "jobs_with_empty_null_locations": empty_null_count,
            "unique_missing_locations_count": len(unique_missing_locations),
            "locations": [
                {"location": location, "count": count}
                for location, count in sorted(
                    unique_missing_locations.items(), key=lambda x: (-x[1], x[0])
                )
            ],
            "sample_empty_null_jobs": [
                {
                    "company": job.get("company", ""),
                    "title": job.get("title", ""),
                    "url": job.get("url", ""),
                    "ats_type": job.get("ats_type", ""),
                }
                for job in empty_null_location_jobs[:20]
            ],
        }
        try:
            with open(missing_locations_file, "w", encoding="utf-8") as f:
                json.dump(missing_data, f, indent=2, ensure_ascii=False)
            print(f"\n   💾 Saved missing locations to {missing_locations_file}")
        except Exception as e:
            print(
                f"   ⚠️  Failed to save missing locations: {e}",
                file=sys.stderr,
            )
    else:
        print(f"\n✅ All {len(jobs)} jobs have location coordinates")

    # Write to CSV (make path relative to script directory if not absolute)
    output_path = Path(args.output)
    if not output_path.is_absolute():
        output_path = ROOT_DIR / output_path
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Preserve existing dates from output CSV if it exists
    existing_dates = {}
    if output_path.exists():
        try:
            with open(output_path, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("url", "").strip()
                    date_value = row.get("date", "").strip()
                    if url and date_value:
                        existing_dates[url] = date_value
        except Exception as e:
            print(
                f"Error reading existing dates from {output_path}: {e}", file=sys.stderr
            )

    previous_csv = find_most_recent_ai_csv(exclude_today=False)
    if previous_csv and previous_csv.exists():
        try:
            with open(previous_csv, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    url = row.get("url", "").strip()
                    date_value = row.get("date", "").strip()
                    if url and date_value and url not in existing_dates:
                        existing_dates[url] = date_value
        except Exception as e:
            print(
                f"Error reading existing dates from {previous_csv}: {e}",
                file=sys.stderr,
            )

    # Set date for all jobs: preserve existing or set to current datetime
    current_datetime = normalize_datetime_to_utc_iso(datetime.now(timezone.utc))
    for job in jobs:
        url = job.get("url", "").strip()
        if url and url in existing_dates:
            # Preserve existing date
            job["date"] = existing_dates[url]
        else:
            # Set to current datetime for new jobs
            job["date"] = current_datetime

    fieldnames = [
        "url",
        "title",
        "location",
        "company",
        "ats_id",
        "ats_type",
        "salary_currency",
        "salary_period",
        "salary_summary",
        "experience",
        "lat",
        "lon",
        "posted_at",
        "date",
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
        removed_jobs = find_removed_jobs(root_output_path, previous_csv)

        # Generate rm_ai.csv with removed jobs (cumulative, like new_ai.csv)
        rm_ai_path = ROOT_DIR / "rm_ai.csv"

        # Read existing rm_ai.csv if it exists
        existing_removed_jobs = {}
        if rm_ai_path.exists():
            try:
                with open(rm_ai_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        url = row.get("url", "").strip()
                        if url:
                            # Keep jobs that are still removed (not in current CSV)
                            if url not in current_urls:
                                # Remove deprecated fields
                                row.pop("employment_type", None)
                                row.pop("is_remote", None)
                                row.pop("salary_min", None)
                                row.pop("salary_max", None)
                                existing_removed_jobs[url] = row
            except Exception as e:
                print(f"Error reading existing rm_ai.csv: {e}", file=sys.stderr)

        # Add newly removed jobs from this run
        for job in removed_jobs:
            url = job.get("url", "").strip()
            if url:
                # Remove deprecated fields
                job.pop("employment_type", None)
                job.pop("is_remote", None)
                job.pop("salary_min", None)
                job.pop("salary_max", None)
                existing_removed_jobs[url] = job

        # Write updated rm_ai.csv with all removed jobs (cumulative)
        if existing_removed_jobs:
            with open(rm_ai_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(existing_removed_jobs.values())

            newly_removed_count = len(removed_jobs)
            total_removed_count = len(existing_removed_jobs)
            existing_count = total_removed_count - newly_removed_count
            print(
                f"✅ Updated rm_ai.csv: {newly_removed_count} newly removed jobs, "
                f"{existing_count} previously removed jobs still pending deletion"
            )
            print(f"   Saved {total_removed_count} total removed jobs to {rm_ai_path}")
        else:
            # No removed jobs at all, delete the file
            if rm_ai_path.exists():
                rm_ai_path.unlink()
                print("✅ No removed jobs, deleted existing rm_ai.csv")
            else:
                print("✅ No removed jobs found")

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
                            # Remove deprecated fields
                            row.pop("employment_type", None)
                            row.pop("is_remote", None)
                            row.pop("salary_min", None)
                            row.pop("salary_max", None)
                            existing_new_jobs[url] = row
            except Exception as e:
                print(f"Error reading existing new_ai.csv: {e}", file=sys.stderr)

        # Add new jobs with today's date
        for job in new_jobs:
            url = job.get("url", "").strip()
            if url:
                # Remove deprecated fields
                job.pop("employment_type", None)
                job.pop("is_remote", None)
                job.pop("salary_min", None)
                job.pop("salary_max", None)
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
                            # Remove deprecated fields
                            row.pop("employment_type", None)
                            row.pop("is_remote", None)
                            row.pop("salary_min", None)
                            row.pop("salary_max", None)
                            existing_new_jobs[url] = row

                if existing_new_jobs:
                    # Ensure date_added column exists
                    new_fieldnames = fieldnames + ["date_added"]
                    # Add date_added if missing and remove deprecated fields
                    for job in existing_new_jobs.values():
                        job.pop("employment_type", None)
                        job.pop("is_remote", None)
                        job.pop("salary_min", None)
                        job.pop("salary_max", None)
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

    # Print summary of companies without ATS
    if companies_without_ats:
        print(
            f"\n📊 Summary: {len(companies_without_ats)} company/companies without ATS found:"
        )
        for company in sorted(companies_without_ats):
            print(f"   - {company}")
    else:
        print("\n✅ All companies found matching ATS systems")


if __name__ == "__main__":
    main()
