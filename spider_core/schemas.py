"""Pydantic request/response schemas."""

from pydantic import BaseModel
from ranking_utils import COLLECTION_TYPE_ACTOR


class TaskConfig(BaseModel):
    start_url: str
    cookie: str = ""
    user_agent: str = ""
    filename: str = ""
    proxies: str = None
    crawl_mode: str = ""
    collection_type: str = COLLECTION_TYPE_ACTOR
    ranking_category: str = ""
    ranking_period: str = ""
    remember_cookie: bool = False

class RuntimeConfig(BaseModel):
    cookie: str = ""
    remember_cookie: bool = False
    user_agent: str = ""
    proxies: str = ""
    trackers: list[str] = []

class CookieConfig(BaseModel):
    cookie: str = ""

class ModeConfig(BaseModel):
    mode: str

class TagConfigRequest(BaseModel):
    url: str
    cookie: str = ""
    user_agent: str = ""
    proxies: str = None

class DeleteRequest(BaseModel):
    filenames: list[str]

class SelectMagnetRequest(BaseModel):
    magnet_id: int


class AutoSelectRequest(BaseModel):
    """Request body for automatic magnet selection."""

    filenames: list[str]
