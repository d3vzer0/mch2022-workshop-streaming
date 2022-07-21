from datetime import datetime
from typing import List, Dict, Optional
from pydantic import BaseModel, validator, root_validator


class ItemModel(BaseModel):
    cve: Dict
    configurations: Optional[Dict]
    impact: Optional[Dict]
    publishedDate: datetime
    lastModifiedDate: datetime


class ResultModel(BaseModel):
    CVE_data_timestamp: datetime
    CVE_data_type: str
    CVE_Items: List[ItemModel]

    @validator('CVE_data_type')
    def fixed_type(cls, v):
        assert v == 'CVE', 'Must be of type CVE'
        return v


class ResponseModel(BaseModel):
    resultsPerPage: int
    startIndex: int
    totalResults: int
    result: ResultModel