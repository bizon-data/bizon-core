from typing import Any, Mapping, Optional

from pydantic import BaseModel, Field
from requests import PreparedRequest
from requests.auth import AuthBase


class CookiesAuthParams(BaseModel):
    cookies: Mapping[str, str] = Field(..., description="Cookies to be attached to the request")
    headers: Optional[Mapping[str, Any]] = Field({}, description="Headers to be attached to the request")


class CookiesAuthenticator(AuthBase):
    def __init__(self, params: CookiesAuthParams):
        super().__init__()
        self.cookies = params.cookies
        self.headers = params.headers

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.prepare_cookies(self.cookies)

        if self.headers:
            request.headers.update(self.headers)
        return request
