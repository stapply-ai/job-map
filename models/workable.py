from typing import List, Optional
from pydantic import BaseModel


class Location(BaseModel):
    country: Optional[str]
    countryCode: Optional[str]
    city: Optional[str]
    region: Optional[str]
    hidden: Optional[bool]


class WorkableJob(BaseModel):
    title: Optional[str]
    shortcode: Optional[str]
    code: Optional[str]
    employment_type: Optional[str]
    telecommuting: Optional[bool]
    department: Optional[str]
    url: Optional[str]
    shortlink: Optional[str]
    application_url: Optional[str]
    published_on: Optional[str]
    created_at: Optional[str]
    country: Optional[str]
    city: Optional[str]
    state: Optional[str]
    education: Optional[str]
    experience: Optional[str]
    function: Optional[str]
    industry: Optional[str]
    locations: Optional[List[Location]]

