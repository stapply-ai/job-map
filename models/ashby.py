from pydantic import BaseModel, Field, field_validator
from typing import List, Optional, Dict, Any, Union
from datetime import datetime


class Address(BaseModel):
    postal_address: Optional[Dict[str, str]] = Field(
        alias="postalAddress", default=None
    )


class CompensationComponent(BaseModel):
    id: Optional[str] = None
    summary: Optional[str] = None
    compensation_type: str = Field(alias="compensationType")
    interval: Optional[str] = None
    currency_code: Optional[str] = Field(alias="currencyCode", default=None)
    min_value: Optional[float] = Field(alias="minValue", default=None)
    max_value: Optional[float] = Field(alias="maxValue", default=None)


class CompensationTier(BaseModel):
    id: str
    tier_summary: str = Field(alias="tierSummary")
    title: Optional[str] = None
    additional_information: Optional[str] = Field(
        alias="additionalInformation", default=None
    )
    components: List[CompensationComponent]


class Compensation(BaseModel):
    compensation_tier_summary: Optional[str] = Field(
        alias="compensationTierSummary", default=None
    )
    scrapeable_compensation_salary_summary: Optional[str] = Field(
        alias="scrapeableCompensationSalarySummary", default=None
    )
    compensation_tiers: List[CompensationTier] = Field(
        alias="compensationTiers", default_factory=list
    )
    summary_components: List[CompensationComponent] = Field(
        alias="summaryComponents", default_factory=list
    )


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
    secondary_locations: List[Union[str, Dict[str, Any]]] = Field(
        alias="secondaryLocations", default_factory=list
    )
    published_at: datetime = Field(alias="publishedAt")
    is_listed: bool = Field(alias="isListed")
    is_remote: Optional[bool] = Field(alias="isRemote", default=None)
    address: Optional[Address] = None
    job_url: str = Field(alias="jobUrl")
    apply_url: str = Field(alias="applyUrl")
    description_html: str = Field(alias="descriptionHtml")
    description_plain: str = Field(alias="descriptionPlain")
    compensation: Optional[Compensation] = None

    @field_validator("secondary_locations", mode="before")
    @classmethod
    def convert_secondary_locations(cls, v):
        """Convert dict locations to strings."""
        if not v:
            return []
        result = []
        for item in v:
            if isinstance(item, dict):
                # Extract location string from dict
                result.append(item.get("location", ""))
            else:
                result.append(item)
        return result


class AshbyApiResponse(BaseModel):
    jobs: List[AshbyJob]
    api_version: str = Field(alias="apiVersion")


# Example usage:
# response_data = {...}  # Your JSON data
# ashby_response = AshbyApiResponse(**response_data)
# print(f"Found {len(ashby_response.jobs)} jobs")
# for job in ashby_response.jobs:
#     print(f"- {job.title} at {job.location}")
