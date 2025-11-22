from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime


class Department(BaseModel):
    name: Optional[str] = None
    base_department: Optional[str] = Field(alias="base_department", default=None)
    department_tree: Optional[List[str]] = Field(alias="department_tree", default_factory=list)


class EmploymentType(BaseModel):
    label: Optional[str] = None
    id: Optional[str] = None


class Location(BaseModel):
    name: Optional[str] = None
    country: Optional[str] = None
    country_code: Optional[str] = Field(alias="countryCode", default=None)
    state: Optional[str] = None
    state_code: Optional[str] = Field(alias="stateCode", default=None)
    city: Optional[str] = None
    workplace_type: Optional[str] = Field(alias="workplaceType", default=None)


class PayRangeDetail(BaseModel):
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    currency: Optional[str] = None
    interval: Optional[str] = None


class JobDescription(BaseModel):
    company: Optional[str] = None
    role: Optional[str] = None


class RipplingJob(BaseModel):
    uuid: Optional[str] = None
    id: Optional[str] = None
    name: Optional[str] = None
    title: Optional[str] = None
    url: Optional[str] = None
    description: Optional[Dict[str, str]] = None  # Can be dict with "company" and "role" keys
    work_locations: Optional[List[str]] = Field(alias="workLocations", default_factory=list)
    locations: Optional[List[Location]] = Field(default_factory=list)
    department: Optional[Dict[str, Any]] = None  # Can be dict or Department object
    employment_type: Optional[Dict[str, Any]] = Field(alias="employmentType", default=None)  # Can be dict or EmploymentType object
    created_on: Optional[str] = Field(alias="createdOn", default=None)
    company_name: Optional[str] = Field(alias="companyName", default=None)
    pay_range_details: Optional[List[Dict[str, Any]]] = Field(alias="payRangeDetails", default_factory=list)
    eeoc_questionnaire_enabled: Optional[bool] = Field(alias="eeocQuestionnaireEnabled", default=None)
    eeoc_questionnaire_enabled_for_job_post: Optional[bool] = Field(alias="eeocQuestionnaireEnabledForJobPost", default=None)

    class Config:
        populate_by_name = True
        extra = "allow"


class RipplingJobBoard(BaseModel):
    board_type: Optional[str] = Field(alias="boardType", default=None)
    slug: Optional[str] = None
    title: Optional[str] = None
    subtitle: Optional[str] = None
    board_url: Optional[str] = Field(alias="boardURL", default=None)


class RipplingCompanyData(BaseModel):
    """Container for all jobs from a Rippling company job board."""
    company_slug: str
    job_board: Optional[RipplingJobBoard] = None
    jobs: List[RipplingJob] = Field(default_factory=list)
    last_scraped: Optional[str] = None

