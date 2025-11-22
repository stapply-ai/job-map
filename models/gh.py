from pydantic import BaseModel, Field
from typing import List, Optional, Any, Union, Dict

class DataComplianceItem(BaseModel):
    type: Optional[str]
    requires_consent: Optional[bool]
    requires_processing_consent: Optional[bool]
    requires_retention_consent: Optional[bool]
    retention_period: Optional[Any]
    demographic_data_consent_applies: Optional[bool]

class Location(BaseModel):
    name: Optional[str]

class MetadataItem(BaseModel):
    id: Optional[int]
    name: Optional[str]
    value: Optional[Union[str, List[str], Dict[str, Any], bool]]
    value_type: Optional[str]

class Department(BaseModel):
    id: Optional[int]
    name: Optional[str]
    child_ids: Optional[List[int]]
    parent_id: Optional[Union[int, None]]

class Office(BaseModel):
    id: Optional[int]
    name: Optional[str]
    location: Optional[str]
    child_ids: Optional[List[int]]
    parent_id: Optional[Union[int, None]]

class GreenhouseJob(BaseModel):
    absolute_url: Optional[str]
    data_compliance: Optional[List[DataComplianceItem]]
    internal_job_id: Optional[int]
    location: Optional[Location]
    metadata: Optional[List[MetadataItem]]
    id: Optional[int]
    updated_at: Optional[str]
    requisition_id: Optional[str]
    title: Optional[str]
    company_name: Optional[str]
    first_published: Optional[str]
    content: Optional[str]
    departments: Optional[List[Department]]
    offices: Optional[List[Office]]
