from pydantic import BaseModel, Field
from typing import List, Optional, Dict
from datetime import datetime


class Address(BaseModel):
    postal_address: Optional[Dict[str, str]] = Field(alias="postalAddress")


class CompensationComponent(BaseModel):
    id: str
    summary: str
    compensation_type: str = Field(alias="compensationType")
    interval: str
    currency_code: str = Field(alias="currencyCode")
    min_value: Optional[float] = Field(alias="minValue")
    max_value: Optional[float] = Field(alias="maxValue")


class CompensationTier(BaseModel):
    id: str
    tier_summary: str = Field(alias="tierSummary")
    title: Optional[str] = None
    additional_information: Optional[str] = Field(alias="additionalInformation")
    components: List[CompensationComponent]


class Compensation(BaseModel):
    compensation_tier_summary: str = Field(alias="compensationTierSummary")
    scrapeable_compensation_salary_summary: Optional[str] = Field(
        alias="scrapeableCompensationSalarySummary"
    )
    compensation_tiers: List[CompensationTier] = Field(alias="compensationTiers")
    summary_components: List[CompensationComponent] = Field(alias="summaryComponents")


class AshbyJob(BaseModel):
    id: str
    title: str
    department: str
    team: str
    employment_type: str = Field(alias="employmentType")
    location: str
    should_display_compensation_on_job_postings: bool = Field(
        alias="shouldDisplayCompensationOnJobPostings"
    )
    secondary_locations: List[str] = Field(alias="secondaryLocations")
    published_at: datetime = Field(alias="publishedAt")
    is_listed: bool = Field(alias="isListed")
    is_remote: bool = Field(alias="isRemote")
    address: Optional[Address] = None
    job_url: str = Field(alias="jobUrl")
    apply_url: str = Field(alias="applyUrl")
    description_html: str = Field(alias="descriptionHtml")
    description_plain: str = Field(alias="descriptionPlain")
    compensation: Optional[Compensation] = None


class AshbyApiResponse(BaseModel):
    jobs: List[AshbyJob]
    api_version: str = Field(alias="apiVersion")


# Example usage:
# response_data = {...}  # Your JSON data
# ashby_response = AshbyApiResponse(**response_data)
# print(f"Found {len(ashby_response.jobs)} jobs")
# for job in ashby_response.jobs:
#     print(f"- {job.title} at {job.location}")
