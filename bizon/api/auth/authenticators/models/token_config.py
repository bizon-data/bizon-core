from pydantic import BaseModel

class TokenAuthConfig(BaseModel):
    token: str
    auth_method: str = "Bearer"
    auth_header: str = "Authorization"