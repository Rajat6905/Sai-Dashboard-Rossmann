from datetime import datetime
from pydantic import BaseModel
from typing import Optional,Union, List


class AuthDetails(BaseModel):
    username: str
    password: str

class PresignedUrlRequest(BaseModel):
    object_name: str
    expires_in: int = 60

class UploadTransactionDetailsRequestModel(BaseModel):
    store_ids: Optional[List[int]] = []
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    nudge_type: str = "all"

class UploadTransactionToDbRequestModel(BaseModel):
    transaction_id: str
    store_id: int
    description: str = None
    clubcard: str = None
    nudge_type: str = "all"

class UploadTransactionSkipRequestModel(BaseModel):
    transaction_id: str

class UploadTransactionStatusDetailsRequestModel(BaseModel):
    store_ids: Optional[List[int]] = []
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    nudge_type: str = "all"