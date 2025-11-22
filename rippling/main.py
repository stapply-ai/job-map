"""Utility script to fetch Rippling company job data directly from HTML."""

from __future__ import annotations

import argparse
import csv
import importlib
import json
import os
import sys
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

import requests

sys.path.append(str(Path(__file__).parent.parent))
from models.rippling import RipplingJob, RipplingJobBoard, RipplingCompanyData

cloudscraper = None
try:  # pragma: no cover
    cloudscraper = importlib.import_module("cloudscraper")
except ImportError:
    pass

REPO_DIR = Path(__file__).resolve().parent
DEFAULT_COMPANIES_CSV = REPO_DIR / "rippling_companies.csv"
COMPANIES_DIR = REPO_DIR / "companies"
COMPANIES_DIR.mkdir(exist_ok=True)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36"
)


def scrape():
    # take all divs with css-aapqz6 class and get the href attribute
    # exaple: <div class="css-aapqz6"><div><div><a href="/3rd-street-youth-center-clinic/jobs/a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a" font-size="20" class="css-1a75djn-Anchor epvls060">Behavioral Health Clinician - Community</a></div><div class="css-mwvv03"><div data-testid="HStack" class="css-srz1l9"><span data-icon="DEPARTMENTS_OUTLINE" color="#6f6f72" type="&quot;\e73b&quot;" size="20" aria-hidden="true" class="css-36tj2e-StyledIcon egn5bfn0"></span><p class="css-htb71u-Body1Element">Behavioral Health</p></div><div><div data-testid="HStack" class="css-srz1l9"><span data-icon="LOCATION_OUTLINE" color="#6f6f72" type="&quot;\e8ef&quot;" size="20" aria-hidden="true" class="css-5pguwy-StyledIcon egn5bfn0"></span><p class="css-htb71u-Body1Element">San Francisco, CA</p></div></div></div></div><a href="/3rd-street-youth-center-clinic/jobs/a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a"><div class="css-1oteowz"><button data-disabled="false" data-testid="Apply" type="button" class="css-1bh0fgi-Container epknpyv1"><span class="css-fiz9cs-Content epknpyv0"><span class="css-1dmm0gl-css-css">Apply</span><span data-icon="ARROW_RIGHT" color="#ffffff" type="&quot;\e9ff&quot;" aria-hidden="true" class="css-1j9pqmm-StyledIcon egn5bfn0"></span></span></button></div></a></div>
    # script data for a single job:         <script id="__NEXT_DATA__" type="application/json">
    """
    {
        "props": {
            "pageProps": {
                "apiData": {
                    "job": {
                        "id": "a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a",
                        "title": "Behavioral Health Clinician - Community",
            {
                "props": {
                    "pageProps": {
                        "apiData": {
                            "jobBoard": {
                                "boardType": "RIPPLING",
                                "slug": "3rd-street-youth-center-clinic",
                                "logo": {
                                    "name": "3rdStYouth-Logo-web.png",
                                    "url": "https://prod-images.rippling.com/1127ff13fcde84ab58c8b9a413bda14d53326fa4.png?Expires=1763818431\u0026Signature=KXJBzXKZQ9h0rAIP55PivkRfX~T8ZSJ~fojHuQMVEmbLAtsbMjUEqaC0kTdDVz~wYjdJ53xS2-sN8kY2qsRQsOwI1yE0ALJ0Xu-4bGDc5drI51SH5GFN0qYHDV6P0TcRovw7qMucUfi8XCqJMuZW2NmwGTVdUZIfmjYyuA49gezsOcfajWdeNz7OgBsZaBYVxwzpttP-7wdV9xuvDGMTgxGg4KTjbykA--RZScwPFcYRp~oe0e3JpKE4M7SVArsc-jlqIP8IjSul4b8ifMQpTfx6uhP9120VvPLsJF7ax-4DSLyQW-9nYvOt1noMXiEnCWnA2JXWOi5-jKCsYPrsvw__\u0026Key-Pair-Id=K2Y26R2ZPP26PH",
                                    "type": "image/png"
                                },
                                "banner": {
                                    "name": "",
                                    "url": null,
                                    "type": ""
                                },
                                "title": "3rd Street Youth Center \u0026 Clinic",
                                "subtitle": "3rd Street is Hiring ",
                                "boardURL": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs",
                                "fontType": "VERDANA_SANS_SERIF",
                                "linkColor": null,
                                "buttonColor": null,
                                "buttonTextColor": null,
                                "legalNotice": null,
                                "groupJobsByLocation": false,
                                "showBoardLogoOnJobPost": false,
                                "showCompanyInfoUnderJobPost": false
                            },
                            "jobPost": {
                                "uuid": "a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a",
                                "name": "Behavioral Health Clinician - Community",
                                "description": {
                                    "company": "\u003cmeta\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e3rd Street Youth Center \u0026amp; Clinic is a community-based agency that provides a range of services to youth in Bayview Hunters Point. 3rd Street’s goal is to provide services so youth have the resources and information to support their wellness and health and is built on the foundation that young people have the intrinsic strengths and resilience to flourish in the face of systemic barriers.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e3rd Street Youth Center \u0026amp; Clinic originated as a community-driven effort to meet the needs of the Bayview Hunters Point neighborhood and maintains its deep connections to the community to this day. 3rd Street prioritizes youth-responsive support across all of its services, which include medical care, youth development programming, TAY Navigation Center, housing (including rapid rehousing and case management), and mental health services (including counseling and case management).\u003c/span\u003e\u003c/p\u003e",
                                    "role": "\u003cmeta\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;text-align:left;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eWe are looking for collaborative and committed Behavioral Health Clinicians in our Behavioral Health Department. You will join a team of creative, culturally responsive, and passionate clinicians to provide outpatient behavioral health services to youth ages 12-27. Community clinicians will provide services both remotely and from a range of office settings, including medical settings.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;text-align:left;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eThis is a time of expansion for the Behavioral Health Department. We are looking for individuals who can sit with the complexities of change and help iterate a growing program. Individual and group supervision is available and the Behavioral Health Department prioritizes creating a supportive work environment amongst all team members.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eDuties \u0026amp; Responsibilities\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eThe following is not designed to cover or contain a comprehensive listing of activities, duties or\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eresponsibilities that are required of the Behavioral Health Clinician. Duties, responsibilities, and activities may change, or new ones may be assigned as needed.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Provide clinical services to youth ages 12-27, including engagement, assessment, and treatment planning.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Collaborate with other providers in young people’s lives to provide meaningful services.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Maintain client records and documentation in accordance with state and federal health\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eregulations.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Work collaboratively with 3rd Street colleagues, multi-disciplinary teams, community resources\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eand organizations.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Contribute to program development efforts.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Engage in a community-oriented approach to mental health services.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eRequirements\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cbr\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eKnowledge, Skills, and Abilities:\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Experience and direct contact with youth between the ages of 12-18, preferably in a school\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003esetting. Experience may come from volunteering, employment, or school training.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Experience working with and/or lived experience of individuals and communities facing systemic barriers and who have been pushed away from resources.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● A strong understanding of the cultural and social factors that affect individuals and communities.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Knowledge of current theories, practices, and principles and practices for providing mental\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003ehealth services to youth.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Experience with wellness, recovery and resiliency-oriented strategies, dual recovery/co-occurring disorder treatment, screening and assessment tools.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Understanding of community needs, resources and organizations related to behavioral health\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003ecare.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Good communication skills, both verbal and written.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Strong interpersonal skills, including humility, empathy and conflict resolution.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eEducation, Certifications, and Licenses:\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Possession of a Master's degree from a recognized college or university in social work, marriage and family therapy, clinical or educational psychology or in a closely related field.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Possession of a valid registration as an Associate Social Worker, Marriage and Family Therapist\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eIntern, or Psychological Assistant from the applicable licensing authority: California Board of\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003eBehavioral Sciences or California Board of Psychology or proof of licensure.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● In compliance with the Administrative Simplification provision of the Health Insurance Portability and Accountability Act of 1996 (HIPAA), employees in this classification are required to possess a National Provider Identifier (NPI) number prior to their first day of employment.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Must possess a valid California Motor Vehicle Operator's license.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eGeneral info\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● This position is an exempt and full-time role.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● This position is primarily based in a school setting, although may be required to occasionally\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003ework from different locations.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cb\u003e\u003cstrong style=\"white-space:pre-wrap;\"\u003eCompensation and benefits\u003c/strong\u003e\u003c/b\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● This position has an annual salary of $75,000.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● We offer a comprehensive benefits package, including health, dental, vision, 403b, generous\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003etime off (vacation, sick, and holidays), and more!\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003cspan style=\"white-space:pre-wrap;\"\u003e● Individual and group supervision is provided.\u003c/span\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\"\u003e\u003cbr\u003e\u003c/p\u003e\u003cp style=\"font-style:normal;font-weight:400;font-size:11pt;line-height:1.38;font-family:\u0026quot;Basel Grotesk\u0026quot;,Arial,sans-serif;margin-right:0px;padding:0px;margin-top:0px;margin-bottom:0px;\" dir=\"ltr\"\u003e\u003ci\u003e\u003cem style=\"white-space:pre-wrap;\"\u003e3rd Street is an Equal Opportunity Employer (EOE). Qualified applicants are considered for employment without regard to race, color, creed, religion, sexual orientation, partnership status, gender and/or gender identity or expression, marital, parental or familial status, national origin, ethnicity, alienage or citizenship status, veteran or military status, age, disability, or any other legally protected basis. Pursuant to the San Francisco Fair Chance Ordinance we will consider for employment qualified candidates with arrest and conviction records.\u003c/em\u003e\u003c/i\u003e\u003c/p\u003e"
                                },
                                "workLocations": [
                                    "San Francisco, CA"
                                ],
                                "department": {
                                    "name": "Behavioral Health",
                                    "base_department": "Behavioral Health",
                                    "department_tree": [
                                        "Behavioral Health"
                                    ]
                                },
                                "employmentType": {
                                    "label": "SALARIED_FT",
                                    "id": "Salaried, full-time"
                                },
                                "createdOn": "2025-06-20T15:46:41.749000-07:00",
                                "activeJobApplication": {
                                    "customQuestions": {
                                        "fields": [
                                            {
                                                "title": "First name",
                                                "fieldType": "SHORT_ANSWER",
                                                "fieldData": {
                                                },
                                                "oid": "first_name",
                                                "required": true
                                            },
                                            {
                                                "title": "Last name",
                                                "fieldType": "SHORT_ANSWER",
                                                "fieldData": {
                                                },
                                                "oid": "last_name",
                                                "required": true
                                            },
                                            {
                                                "title": "Email",
                                                "fieldType": "SHORT_ANSWER",
                                                "fieldData": {
                                                },
                                                "oid": "email",
                                                "required": true
                                            },
                                            {
                                                "title": "Pronouns",
                                                "fieldType": "PRONOUN",
                                                "fieldData": {
                                                },
                                                "oid": "pronouns",
                                                "required": false
                                            },
                                            {
                                                "title": "Current company",
                                                "fieldType": "SHORT_ANSWER",
                                                "fieldData": {
                                                },
                                                "oid": "current_company",
                                                "required": false
                                            },
                                            {
                                                "title": "Phone number",
                                                "fieldType": "PHONE_NUMBER",
                                                "fieldData": {
                                                },
                                                "oid": "phone_number",
                                                "required": true
                                            },
                                            {
                                                "title": "Location (city only)",
                                                "fieldType": "SHORT_ANSWER",
                                                "fieldData": {
                                                },
                                                "oid": "location",
                                                "required": true
                                            },
                                            {
                                                "title": "Resume",
                                                "fieldType": "FILE",
                                                "fieldData": {
                                                },
                                                "oid": "resume",
                                                "required": true
                                            },
                                            {
                                                "title": "Cover letter",
                                                "fieldType": "FILE",
                                                "fieldData": {
                                                },
                                                "oid": "cover_letter",
                                                "required": true
                                            }
                                        ]
                                    },
                                    "additionalQuestions": [
                                        {
                                            "form": {
                                                "questions": [
                                                    {
                                                        "uniqueKey": "026684fa-c175-4fb6-a6e7-e3ebad2b8490",
                                                        "title": "Why do you want to work at 3rd Street Youth Center \u0026 Clinic",
                                                        "description": "",
                                                        "tags": [
                                                        ],
                                                        "dataType": "Text",
                                                        "questionType": "LONG_ANSWER",
                                                        "isRequired": true,
                                                        "allowComments": false,
                                                        "canEdit": false,
                                                        "isPrivate": false,
                                                        "strChoices": [
                                                        ],
                                                        "intChoices": [
                                                        ],
                                                        "isMultiSelectEnabled": false,
                                                        "isOtherEnabled": false
                                                    },
                                                    {
                                                        "uniqueKey": "4f654a70-5970-42cb-8a2e-3a793df4267c",
                                                        "title": "Why are you interested in this position? What would you like to achieve?",
                                                        "tags": [
                                                        ],
                                                        "dataType": "Text",
                                                        "questionType": "LONG_ANSWER",
                                                        "isRequired": true,
                                                        "allowComments": false,
                                                        "canEdit": false,
                                                        "isPrivate": false,
                                                        "strChoices": [
                                                        ],
                                                        "intChoices": [
                                                        ],
                                                        "isMultiSelectEnabled": false,
                                                        "isOtherEnabled": false
                                                    }
                                                ],
                                                "deletedQuestions": [
                                                    {
                                                        "uniqueKey": "e3772cf1-7c43-486a-8b4c-9f625df73d66",
                                                        "title": "Do you have experience working directly with clients between the ages of 12 to 27?",
                                                        "tags": [
                                                        ],
                                                        "dataType": "Number",
                                                        "questionType": "YES_NO_SCALE_4",
                                                        "isRequired": true,
                                                        "allowComments": false,
                                                        "canEdit": false,
                                                        "isPrivate": false,
                                                        "strChoices": [
                                                        ],
                                                        "intChoices": [
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ],
                                                        "isMultiSelectEnabled": false,
                                                        "isOtherEnabled": false
                                                    },
                                                    {
                                                        "uniqueKey": "b9ea53c9-985a-434d-92b2-9e9e75851955",
                                                        "title": "Do you have a Master's degree in social work, marriage and family therapy, clinical or educational psychology or in a closely related field?",
                                                        "tags": [
                                                        ],
                                                        "dataType": "Number",
                                                        "questionType": "YES_NO_SCALE_4",
                                                        "isRequired": true,
                                                        "allowComments": false,
                                                        "canEdit": false,
                                                        "isPrivate": false,
                                                        "strChoices": [
                                                        ],
                                                        "intChoices": [
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ],
                                                        "isMultiSelectEnabled": false,
                                                        "isOtherEnabled": false
                                                    },
                                                    {
                                                        "uniqueKey": "690e9f4a-f156-4510-9304-b51844b261cc",
                                                        "title": "Do you have a valid registration as an Associate Social Worker, Marriage and Family Therapy Intern, or Psychological Assistant?",
                                                        "tags": [
                                                        ],
                                                        "dataType": "Number",
                                                        "questionType": "YES_NO_SCALE_4",
                                                        "isRequired": true,
                                                        "allowComments": false,
                                                        "canEdit": false,
                                                        "isPrivate": false,
                                                        "strChoices": [
                                                        ],
                                                        "intChoices": [
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ],
                                                        "isMultiSelectEnabled": false,
                                                        "isOtherEnabled": false
                                                    }
                                                ],
                                                "sections": [
                                                ],
                                                "deletedSections": [
                                                ],
                                                "skipLogic": [
                                                ]
                                            },
                                            "name": "General",
                                            "id": "685ae969574b9a6a74837db3"
                                        }
                                    ]
                                },
                                "eeocQuestionnaireEnabled": true,
                                "eeocQuestionnaireEnabledForJobPost": true,
                                "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a",
                                "board": {
                                    "boardType": "RIPPLING",
                                    "slug": "3rd-street-youth-center-clinic",
                                    "logo": {
                                        "name": "3rdStYouth-Logo-web.png",
                                        "url": "https://prod-images.rippling.com/1127ff13fcde84ab58c8b9a413bda14d53326fa4.png?Expires=1763818431\u0026Signature=KXJBzXKZQ9h0rAIP55PivkRfX~T8ZSJ~fojHuQMVEmbLAtsbMjUEqaC0kTdDVz~wYjdJ53xS2-sN8kY2qsRQsOwI1yE0ALJ0Xu-4bGDc5drI51SH5GFN0qYHDV6P0TcRovw7qMucUfi8XCqJMuZW2NmwGTVdUZIfmjYyuA49gezsOcfajWdeNz7OgBsZaBYVxwzpttP-7wdV9xuvDGMTgxGg4KTjbykA--RZScwPFcYRp~oe0e3JpKE4M7SVArsc-jlqIP8IjSul4b8ifMQpTfx6uhP9120VvPLsJF7ax-4DSLyQW-9nYvOt1noMXiEnCWnA2JXWOi5-jKCsYPrsvw__\u0026Key-Pair-Id=K2Y26R2ZPP26PH",
                                        "type": "image/png"
                                    },
                                    "banner": {
                                        "name": "",
                                        "url": null,
                                        "type": ""
                                    },
                                    "title": "3rd Street Youth Center \u0026 Clinic",
                                    "subtitle": "3rd Street is Hiring ",
                                    "boardURL": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs",
                                    "fontType": "VERDANA_SANS_SERIF",
                                    "linkColor": null,
                                    "buttonColor": null,
                                    "buttonTextColor": null,
                                    "legalNotice": null,
                                    "groupJobsByLocation": false,
                                    "showBoardLogoOnJobPost": false,
                                    "showCompanyInfoUnderJobPost": false
                                },
                                "payRangeDetails": [
                                ],
                                "applicationConfirmationTemplate": null,
                                "hasAIEvaluationsEnabled": false,
                                "companyName": "3rd Street Youth Center \u0026 Clinic"
                            },
                            "workLocations": [
                                "San Francisco, CA"
                            ],
                            "department": {
                                "name": "Behavioral Health",
                                "base_department": "Behavioral Health",
                                "department_tree": [
                                    "Behavioral Health"
                                ]
                            },
                            "payRangeDetails": [
                            ]
                        },
                        "_sentryTraceData": "2a0a30a3a0764f5a8ebfdac2374947a3-b73e93e67a87da6c-0",
                        "_sentryBaggage": "sentry-environment=production,sentry-release=5ccavxJdPZ6lHvxp04Hv0,sentry-public_key=908ccf35bffc4247823e66133cba73de,sentry-trace_id=2a0a30a3a0764f5a8ebfdac2374947a3,sentry-transaction=%2F%5BjobBoardSlug%5D%2Fjobs%2F%5BjobId%5D,sentry-sampled=false"
                    },
                    "__N_SSP": true
                },
                "page": "/[jobBoardSlug]/jobs/[jobId]",
                "query": {
                    "jobBoardSlug": "3rd-street-youth-center-clinic",
                    "jobId": "a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a"
                },
                "buildId": "5ccavxJdPZ6lHvxp04Hv0",
                "assetPrefix": "https://ats.us1.rippling.com",
                "isFallback": false,
                "isExperimentalCompile": false,
                "gssp": true,
                "locale": "en-US",
                "locales": [
                    "en-US",
                    "de-DE",
                    "en-AU",
                    "en-CA",
                    "en-GB",
                    "es-419",
                    "es-ES",
                    "fr-CA",
                    "fr-FR",
                    "nl-NL",
                    "pl-PL",
                    "pt-BR",
                    "pt-PT"
                ],
                "defaultLocale": "en-US",
                "scriptLoader": [
                ]
            }
        }
    </script>
    """

    # for all jobs:
    """
     <script id="__NEXT_DATA__" type="application/json">
            {
                "props": {
                    "pageProps": {
                        "apiData": {
                            "jobBoard": {
                                "boardType": "RIPPLING",
                                "slug": "3rd-street-youth-center-clinic",
                                "logo": {
                                    "name": "3rdStYouth-Logo-web.png",
                                    "url": "https://prod-images.rippling.com/1127ff13fcde84ab58c8b9a413bda14d53326fa4.png?Expires=1763818691\u0026Signature=WFk3TmsXMdJB2RLLy5Rx~A0GU6rCjYxgGUeA25-ezrMUHZXc~Ce2CsSYk069OxeOVH~8NqIB~dh~6r3ZwxWVv4V8V13~msuDMPW1vzY0TcrxIojI836ygfrdgpSaXAjyhXf~95cvg-ogC0EtIQGdOnCYbDcSjgLGhnXaLD60GKyJVximo445CQLQifxlw3VQcK0nivepaAVQmYrr26UVOtNYLbbIg147z7iSdI-AmJ9Zm4JR~N9czW-FHRZ6psNtagdJwLUNIRrBYi0IQSOwjUUplqxUxi3RIEx3xM9tZcCmta-NdfdxcildTSIVQjrhT-yt8v-6feEbLvmu7dTVEg__\u0026Key-Pair-Id=K2Y26R2ZPP26PH",
                                    "type": "image/png"
                                },
                                "banner": {
                                    "name": "",
                                    "url": null,
                                    "type": ""
                                },
                                "title": "3rd Street Youth Center \u0026 Clinic",
                                "subtitle": "3rd Street is Hiring ",
                                "boardURL": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs",
                                "fontType": "VERDANA_SANS_SERIF",
                                "linkColor": null,
                                "buttonColor": null,
                                "buttonTextColor": null,
                                "legalNotice": null,
                                "groupJobsByLocation": false,
                                "showBoardLogoOnJobPost": false,
                                "showCompanyInfoUnderJobPost": false
                            },
                            "jobBoardSlug": "3rd-street-youth-center-clinic",
                            "filtersConfig": {
                                "workLocations": [
                                ],
                                "departments": [
                                ]
                            }
                        },
                        "dehydratedState": {
                            "mutations": [
                            ],
                            "queries": [
                                {
                                    "state": {
                                        "data": {
                                            "items": [
                                                {
                                                    "id": "a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a",
                                                    "name": "Behavioral Health Clinician - Community",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/a661d3ef-2dd4-4aee-8c0a-fb48ae545a7a",
                                                    "department": {
                                                        "name": "Behavioral Health"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "76e5d543-573b-4780-ae38-e4129271694a",
                                                    "name": "Behavioral Health Clinician - TAY Navigation Center",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/76e5d543-573b-4780-ae38-e4129271694a",
                                                    "department": {
                                                        "name": "Behavioral Health @ The Nav"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "75db887b-2e60-40a2-bcdc-d58cef1f9f8e",
                                                    "name": "Emergency Housing Voucher (EHV) Case Manager",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/75db887b-2e60-40a2-bcdc-d58cef1f9f8e",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "976a445f-0a33-40da-a4fa-98f7d7725a3a",
                                                    "name": "Housing Case Manager - Rapid Rehousing for Parenting Transitional Aged Youth (TAY)",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/976a445f-0a33-40da-a4fa-98f7d7725a3a",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "b9abb703-c04b-4646-b01b-36b17a06b70e",
                                                    "name": "Housing Case Manager - Rapid Rehousing for Transitional Aged Youth (TAY)",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/b9abb703-c04b-4646-b01b-36b17a06b70e",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "c93b5de8-d865-44e9-a1af-e89e9c4621b1",
                                                    "name": "Housing Case Manager – Rising Up",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/c93b5de8-d865-44e9-a1af-e89e9c4621b1",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "a90ec186-3670-42ac-b607-25feb09185ef",
                                                    "name": "Program Monitor",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/a90ec186-3670-42ac-b607-25feb09185ef",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "3628c092-331a-4e46-8739-c234e67d64b6",
                                                    "name": "Youth Access Point (YAP) Case Manager",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/3628c092-331a-4e46-8739-c234e67d64b6",
                                                    "department": {
                                                        "name": "Housing"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                },
                                                {
                                                    "id": "79742c34-e6c4-413f-8e4b-940ff16245c3",
                                                    "name": "Youth Leadership \u0026 Policy Analyst",
                                                    "url": "https://ats.rippling.com/3rd-street-youth-center-clinic/jobs/79742c34-e6c4-413f-8e4b-940ff16245c3",
                                                    "department": {
                                                        "name": "Policy"
                                                    },
                                                    "locations": [
                                                        {
                                                            "name": "San Francisco, CA",
                                                            "country": "United States",
                                                            "countryCode": "US",
                                                            "state": "California",
                                                            "stateCode": "CA",
                                                            "city": "San Francisco",
                                                            "workplaceType": "ON_SITE"
                                                        }
                                                    ],
                                                    "language": "en-US"
                                                }
                                            ],
                                            "page": 0,
                                            "pageSize": 20,
                                            "totalItems": 9,
                                            "totalPages": 1
                                        },
                                        "dataUpdateCount": 1,
                                        "dataUpdatedAt": 1763732291960,
                                        "error": null,
                                        "errorUpdateCount": 0,
                                        "errorUpdatedAt": 0,
                                        "fetchFailureCount": 0,
                                        "fetchFailureReason": null,
                                        "fetchMeta": null,
                                        "isInvalidated": false,
                                        "status": "success",
                                        "fetchStatus": "idle"
                                    },
                                    "queryKey": [
                                        "board",
                                        "3rd-street-youth-center-clinic",
                                        "job-posts",
                                        false,
                                        {
                                            "searchQuery": "",
                                            "departments": [
                                            ],
                                            "workplaceType": null,
                                            "country": "",
                                            "state": "",
                                            "city": "",
                                            "page": 0,
                                            "pageSize": 20
                                        }
                                    ],
                                    "queryHash": "[\"board\",\"3rd-street-youth-center-clinic\",\"job-posts\",false,{\"city\":\"\",\"country\":\"\",\"departments\":[],\"page\":0,\"pageSize\":20,\"searchQuery\":\"\",\"state\":\"\",\"workplaceType\":null}]"
                                },
                                {
                                    "state": {
                                        "data": {
                                            "items": [
                                                {
                                                    "name": "San Francisco, CA",
                                                    "country": "United States",
                                                    "countryCode": "US",
                                                    "state": "California",
                                                    "stateCode": "CA",
                                                    "city": "San Francisco",
                                                    "workplaceType": "ON_SITE"
                                                }
                                            ],
                                            "page": 0,
                                            "pageSize": 1,
                                            "totalItems": 1,
                                            "totalPages": 1
                                        },
                                        "dataUpdateCount": 1,
                                        "dataUpdatedAt": 1763732291947,
                                        "error": null,
                                        "errorUpdateCount": 0,
                                        "errorUpdatedAt": 0,
                                        "fetchFailureCount": 0,
                                        "fetchFailureReason": null,
                                        "fetchMeta": null,
                                        "isInvalidated": false,
                                        "status": "success",
                                        "fetchStatus": "idle"
                                    },
                                    "queryKey": [
                                        "board",
                                        "3rd-street-youth-center-clinic",
                                        "locations"
                                    ],
                                    "queryHash": "[\"board\",\"3rd-street-youth-center-clinic\",\"locations\"]"
                                },
                                {
                                    "state": {
                                        "data": {
                                            "items": [
                                                {
                                                    "name": "Behavioral Health"
                                                },
                                                {
                                                    "name": "Behavioral Health @ The Nav"
                                                },
                                                {
                                                    "name": "Housing"
                                                },
                                                {
                                                    "name": "Policy"
                                                }
                                            ],
                                            "page": 0,
                                            "pageSize": 4,
                                            "totalItems": 4,
                                            "totalPages": 1
                                        },
                                        "dataUpdateCount": 1,
                                        "dataUpdatedAt": 1763732291954,
                                        "error": null,
                                        "errorUpdateCount": 0,
                                        "errorUpdatedAt": 0,
                                        "fetchFailureCount": 0,
                                        "fetchFailureReason": null,
                                        "fetchMeta": null,
                                        "isInvalidated": false,
                                        "status": "success",
                                        "fetchStatus": "idle"
                                    },
                                    "queryKey": [
                                        "board",
                                        "3rd-street-youth-center-clinic",
                                        "departments"
                                    ],
                                    "queryHash": "[\"board\",\"3rd-street-youth-center-clinic\",\"departments\"]"
                                }
                            ]
                        },
                        "_sentryTraceData": "f39f262a39104aa3b0afd266b0d72eb3-aa2632ce32be25dd-0",
                        "_sentryBaggage": "sentry-environment=production,sentry-release=5ccavxJdPZ6lHvxp04Hv0,sentry-public_key=908ccf35bffc4247823e66133cba73de,sentry-trace_id=f39f262a39104aa3b0afd266b0d72eb3,sentry-transaction=%2F%5BjobBoardSlug%5D%2Fjobs,sentry-sampled=false"
                    },
                    "__N_SSP": true
                },
                "page": "/[jobBoardSlug]/jobs",
                "query": {
                    "jobBoardSlug": "3rd-street-youth-center-clinic"
                },
                "buildId": "5ccavxJdPZ6lHvxp04Hv0",
                "assetPrefix": "https://ats.us1.rippling.com",
                "isFallback": false,
                "isExperimentalCompile": false,
                "gssp": true,
                "locale": "en-US",
                "locales": [
                    "en-US",
                    "de-DE",
                    "en-AU",
                    "en-CA",
                    "en-GB",
                    "es-419",
                    "es-ES",
                    "fr-CA",
                    "fr-FR",
                    "nl-NL",
                    "pl-PL",
                    "pt-BR",
                    "pt-PT"
                ],
                "defaultLocale": "en-US",
                "scriptLoader": [
                ]
            }</script>
            """


class NextDataExtractor(HTMLParser):
    """Minimal parser to capture the __NEXT_DATA__ script contents."""

    def __init__(self) -> None:
        super().__init__()
        self._capture = False
        self._chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs) -> None:  # type: ignore[override]
        if tag.lower() != "script":
            return
        attr_dict = dict(attrs)
        if attr_dict.get("id") == "__NEXT_DATA__":
            self._capture = True

    def handle_endtag(self, tag: str) -> None:  # type: ignore[override]
        if tag.lower() == "script" and self._capture:
            self._capture = False

    def handle_data(self, data: str) -> None:  # type: ignore[override]
        if self._capture:
            self._chunks.append(data)

    def payload(self) -> str:
        return "".join(self._chunks).strip()


def fetch_company_html(url: str, timeout: int = 30) -> Optional[str]:
    headers = {
        "User-Agent": DEFAULT_USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }
    session = requests.Session()
    response = session.get(url, headers=headers, timeout=timeout)

    # Handle 404 - return None to indicate page not found
    if response.status_code == 404:
        return None

    if response.status_code in {403, 409} and cloudscraper is not None:
        scraper = cloudscraper.create_scraper(
            browser={
                "browser": "chrome",
                "platform": "mac",
                "mobile": False,
            },
            delay=10,
        )
        response = scraper.get(url, headers=headers, timeout=timeout)
        # Check 404 again after cloudscraper retry
        if response.status_code == 404:
            return None

    response.raise_for_status()
    return response.text


def extract_next_data(html: str) -> dict:
    parser = NextDataExtractor()
    parser.feed(html)
    payload = parser.payload()
    if not payload:
        raise ValueError("Failed to locate __NEXT_DATA__ script in HTML.")
    return json.loads(payload)


def extract_detailed_job(next_data: dict) -> Optional[dict]:
    """Extract detailed job data from individual job page __NEXT_DATA__."""
    props = next_data.get("props", {})
    page_props = props.get("pageProps", {})
    api_data = page_props.get("apiData", {})

    job_post = api_data.get("jobPost") or api_data.get("job")
    if not job_post:
        return None

    return job_post


def extract_job_summaries(next_data: dict) -> list[dict]:
    props = next_data.get("props", {})
    page_props = props.get("pageProps", {})
    api_data = page_props.get("apiData", {})

    # Individual job page
    job_post = api_data.get("jobPost") or api_data.get("job")
    if job_post:
        return [
            {
                "id": job_post.get("uuid") or job_post.get("id"),
                "title": job_post.get("name") or job_post.get("title"),
                "url": job_post.get("url")
                or job_post.get("boardURL")
                or job_post.get("jobURL"),
                "department": (job_post.get("department") or {}).get("name"),
                "locations": job_post.get("workLocations")
                or [loc.get("name") for loc in job_post.get("locations", [])],
            }
        ]

    dehydrated = page_props.get("dehydratedState", {})
    queries = dehydrated.get("queries", [])
    for query in queries:
        state = query.get("state", {})
        data = state.get("data", {})
        items = data.get("items")
        if items:
            summaries = []
            for item in items:
                summaries.append(
                    {
                        "id": item.get("id"),
                        "title": item.get("name"),
                        "url": item.get("url"),
                        "department": (item.get("department") or {}).get("name"),
                        "locations": [
                            loc.get("name")
                            for loc in item.get("locations", [])
                            if isinstance(loc, dict) and loc.get("name")
                        ],
                    }
                )
            if summaries:
                return summaries

    return []


def extract_company_slug(url: str) -> str:
    """Extract company slug from Rippling job board URL."""
    parsed = urlparse(url)
    # Extract slug from path like /company-slug/jobs
    path_parts = [p for p in parsed.path.split("/") if p]
    if path_parts and path_parts[-1] == "jobs":
        return path_parts[-2] if len(path_parts) > 1 else path_parts[0]
    return path_parts[0] if path_parts else "unknown"


def fetch_detailed_job(job_url: str) -> Optional[dict]:
    """Fetch detailed job data from an individual job page."""
    try:
        html = fetch_company_html(job_url)
        if html is None:
            return None
        next_data = extract_next_data(html)
        return extract_detailed_job(next_data)
    except Exception as e:
        print(f"  Error fetching job {job_url}: {e}")
        return None


def scrape_company_jobs(
    company_url: str, force: bool = False
) -> Optional[RipplingCompanyData]:
    """Scrape all detailed jobs for a company."""
    company_slug = extract_company_slug(company_url)
    file_path = COMPANIES_DIR / f"{company_slug}.json"

    # Check if we should skip scraping
    if not force and file_path.exists():
        try:
            with file_path.open() as f:
                existing_data = json.load(f)
                last_scraped_str = existing_data.get("last_scraped")
                if last_scraped_str:
                    last_scraped = datetime.fromisoformat(last_scraped_str)
                    hours_elapsed = (
                        datetime.now() - last_scraped
                    ).total_seconds() / 3600
                    if hours_elapsed < 12:
                        print(
                            f"Skipping {company_slug} (scraped {hours_elapsed:.1f} hours ago)"
                        )
                        return None
        except Exception:
            pass

    print(f"Fetching job board: {company_url}")
    html = fetch_company_html(company_url)
    if html is None:
        print(f"  Company '{company_slug}' not found (404), skipping...")
        return None

    try:
        next_data = extract_next_data(html)
    except ValueError as e:
        print(
            f"  Failed to parse job board data for '{company_slug}': {e}, skipping..."
        )
        return None

    # Extract job board info
    props = next_data.get("props", {})
    page_props = props.get("pageProps", {})
    api_data = page_props.get("apiData", {})
    job_board_data = api_data.get("jobBoard", {})

    # Extract job summaries from board page
    job_summaries = extract_job_summaries(next_data)
    if not job_summaries:
        print(f"  No jobs found for {company_slug}")
        return None

    print(f"  Found {len(job_summaries)} job(s), fetching details...")

    # Fetch detailed data for each job
    detailed_jobs = []
    for i, summary in enumerate(job_summaries, 1):
        job_url = summary.get("url")
        if not job_url:
            continue

        # Ensure URL is absolute
        if not job_url.startswith("http"):
            job_url = f"https://ats.rippling.com{job_url}"

        print(f"  [{i}/{len(job_summaries)}] Fetching {job_url}")
        job_data = fetch_detailed_job(job_url)
        if job_data:
            detailed_jobs.append(job_data)

    # Build company data object
    company_data = RipplingCompanyData(
        company_slug=company_slug,
        job_board=RipplingJobBoard(**job_board_data) if job_board_data else None,
        jobs=[RipplingJob(**job) for job in detailed_jobs],
        last_scraped=datetime.now().isoformat(),
    )

    # Save to file
    with file_path.open("w") as f:
        json.dump(company_data.model_dump(mode="json", exclude_none=True), f, indent=2)

    print(f"  Saved {len(detailed_jobs)} jobs to {file_path}")
    return company_data


def read_company_urls(csv_path: Path) -> list[str]:
    if not csv_path.exists():
        raise FileNotFoundError(f"Company CSV not found at '{csv_path}'.")

    with csv_path.open(encoding="utf-8", newline="") as csv_file:
        reader = csv.reader(csv_file)
        rows = [row for row in reader if row]

    if not rows:
        raise ValueError(f"No rows found in '{csv_path}'.")

    header, *data = rows
    if "rippling_url" in header:
        idx = header.index("rippling_url")
    else:
        idx = 0

    urls = [row[idx].strip() for row in data if len(row) > idx and row[idx].strip()]
    if not urls:
        raise ValueError(f"No URLs found in '{csv_path}'.")
    return urls


def run_html_sample(company_url: str, max_jobs: int) -> list[dict]:
    print(f"[html-sample] Fetching {company_url}")
    html = fetch_company_html(company_url)
    next_data = extract_next_data(html)
    jobs = extract_job_summaries(next_data)
    if not jobs:
        print("[html-sample] No jobs found in parsed data.")
        return []

    print(f"[html-sample] Found {len(jobs)} job(s). Showing up to {max_jobs} entries:")
    for job in jobs[:max_jobs]:
        title = job.get("title") or "<untitled>"
        url = job.get("url") or "<no url>"
        dept = job.get("department") or "Unknown department"
        location = ", ".join(job.get("locations") or []) or "Unknown location"
        print(f"  - {title} | {dept} | {location} | {url}")
    return jobs[:max_jobs]


def process_companies(
    urls: list[str],
    max_jobs: int,
    html_sample: bool = False,
    force: bool = False,
) -> None:
    for company_url in urls:
        if html_sample:
            run_html_sample(company_url, max_jobs)
        else:
            scrape_company_jobs(company_url, force=force)


def parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Rippling job data directly from the job board HTML."
    )
    parser.add_argument(
        "--csv",
        default=os.environ.get("RIPPLING_COMPANIES_CSV", str(DEFAULT_COMPANIES_CSV)),
        help="Path to rippling_companies.csv.",
    )
    parser.add_argument(
        "--url",
        default=os.environ.get("RIPPLING_URL"),
        help="Process only this company URL (otherwise processes all companies from CSV).",
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=int(os.environ.get("RIPPLING_MAX_JOBS", 5)),
        help="Max job listings to print per company.",
    )
    parser.add_argument(
        "--html-sample",
        action="store_true",
        help="Show job summaries only (no detailed scraping).",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-scraping even if data was recently scraped.",
    )
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()

    if args.url:
        if args.html_sample:
            run_html_sample(args.url, args.max_jobs)
        else:
            scrape_company_jobs(args.url, force=args.force)
        return

    urls = read_company_urls(Path(args.csv))

    if args.html_sample:
        run_html_sample(urls[0], args.max_jobs)
        return

    process_companies(
        urls, args.max_jobs, html_sample=args.html_sample, force=args.force
    )


if __name__ == "__main__":
    main()
