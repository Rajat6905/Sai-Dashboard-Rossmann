from pydantic import BaseModel


class AuthDetails(BaseModel):
    username: str
    password: str

class PresignedUrlRequest(BaseModel):
    object_name: str
    expires_in: int = 60