from pydantic import Field

from bizon.source.config import SourceConfig


class GsheetsSourceConfig(SourceConfig):
    name: str = Field("googlesheets", description="Name of the source")
    stream_name: str = Field("worksheet", description="Name of the stream")
    worksheet_name: str = Field(description="Name of the worksheet to fetch data from", default=...)
    spreadsheet_url: str = Field(description="URL of the spreadsheet", default=...)
    service_account_key: str = Field(
        description="Service Account Key JSON string. If empty it will be infered",
        default="",
    )
