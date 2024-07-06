import requests


class BaseBackoffException(requests.exceptions.HTTPError):
    def __init__(self, request: requests.PreparedRequest, response: requests.Response, error_message: str = ""):
        error_message = (
            error_message
            or f"Request URL: {request.url}, Response Code: {response.status_code}, Response Text: {response.text}"
        )
        super().__init__(error_message, request=request, response=response)


class DefaultBackoffException(BaseBackoffException):
    pass
