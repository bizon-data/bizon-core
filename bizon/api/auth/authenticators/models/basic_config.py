from pydantic import BaseModel


class BasicAuthConfig(BaseModel):
    username: str
    password: str = ""
    auth_method: str = "Basic"
    auth_header: str = "Authorization"
