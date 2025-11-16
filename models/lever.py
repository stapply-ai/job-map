from pydantic import BaseModel
from typing import List, Optional


class Categories(BaseModel):
    location: Optional[str] = None
    team: Optional[str] = None
    commitment: Optional[str] = None
    allLocations: Optional[List[str]] = None


class LeverJob(BaseModel):
    additional: Optional[str] = None
    additionalPlain: Optional[str] = None
    categories: Optional[Categories] = None
    createdAt: Optional[int] = None
    descriptionPlain: Optional[str] = None
    description: Optional[str] = None
    id: Optional[str] = None
    lists: Optional[List] = None
    text: Optional[str] = None
    country: Optional[str] = None
    workplaceType: Optional[str] = None
    opening: Optional[str] = None
    openingPlain: Optional[str] = None
    descriptionBody: Optional[str] = None
    descriptionBodyPlain: Optional[str] = None
    hostedUrl: Optional[str] = None
    applyUrl: Optional[str] = None

