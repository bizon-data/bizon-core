from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests import PreparedRequest
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://slack.com/api"

# Without a read timeout a stalled socket never becomes a retryable event.
TIMEOUT = (10, 60)

# conversations.members returns bare user IDs, so a large page keeps the per-channel fan-out cheap.
MEMBERS_PAGE_SIZE = 1000

# users.info errors scoped to one user ID; anything else is workspace-wide and fails the stream.
PER_USER_INFO_ERRORS = frozenset({"user_not_found", "user_not_visible"})

# conversations.members errors scoped to one channel degrade to an empty member list. Anything else
# would hit every channel, so degrading it would publish members: [] everywhere on a "successful" run.
PER_CHANNEL_MEMBER_ERRORS = frozenset(
    {
        "channel_not_found",
        "fetch_members_failed",
        "method_not_supported_for_channel_type",
    }
)


class SlackApiError(RuntimeError):
    """An HTTP 200 ``ok: false`` body. Callers branch on ``.error``, the only per-channel vs per-token signal."""

    def __init__(self, error: Optional[str]):
        self.error = error or "unknown_error"
        super().__init__(f"Slack API error: {self.error}")


class SlackApiKeyAuth(AuthBase):
    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request: PreparedRequest) -> PreparedRequest:
        request.headers["Authorization"] = f"Bearer {self.api_key}"
        return request


class SlackStreams(str, Enum):
    USERS = "users"
    PUBLIC_CHANNELS = "public_channels"
    EXTERNAL_USERS = "external_users"


class SlackSourceConfig(SourceConfig):
    stream: SlackStreams

    api_key: str = Field(
        ...,
        description=(
            "Slack token. Needs users:read (users "
            "stream), channels:read (public_channels + external_users streams) and "
            "users:read.email (emails on external_users rows)."
        ),
    )

    page_size: int = Field(
        default=200,
        ge=1,
        le=1000,
        description="Number of results per page.",
    )

    http: Optional[HttpConfig] = Field(default_factory=HttpConfig, description="HTTP timeout/retry policy")


class SlackSource(AbstractSource):
    def __init__(self, config: SlackSourceConfig):
        super().__init__(config)
        self.config: SlackSourceConfig = config

        self.session.headers.update({"Content-Type": "application/json"})

        # external_users caches: keep users.info calls proportional to distinct external users, not memberships.
        self._internal_user_ids: Optional[set] = None
        self._seen_external_ids: set = set()

    def get_authenticator(self) -> AuthBase:
        return SlackApiKeyAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in SlackStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return SlackSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            response = self.session.get(f"{BASE_URL}/users.list", params={"limit": 1}, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()
            if not data.get("ok"):
                return False, data.get("error")
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def get_users(self, pagination: dict = None) -> SourceIteration:
        params = {"limit": self.config.page_size}
        if pagination and pagination.get("cursor"):
            params["cursor"] = pagination["cursor"]

        response = self.session.get(f"{BASE_URL}/users.list", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            raise SlackApiError(data.get("error"))

        records = [SourceRecord(id=str(m["id"]), data=m) for m in data.get("members", [])]

        # Slack signals the last page with an empty next_cursor; that must become {}, not {"cursor": ""}.
        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        next_pagination = {"cursor": cursor} if cursor else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def _get_channel_members(self, channel_id: str) -> List[str]:
        """Drain every page of a channel's members; the caller decides whether a SlackApiError is per-channel."""
        members: List[str] = []
        cursor = ""

        while True:
            params = {"channel": channel_id, "limit": MEMBERS_PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor

            response = self.session.get(f"{BASE_URL}/conversations.members", params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if not data.get("ok"):
                raise SlackApiError(data.get("error"))

            members.extend(str(m) for m in data.get("members", []))

            cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
            if not cursor:
                return members

    def get_public_channels(self, pagination: dict = None) -> SourceIteration:
        """``is_private``/``is_channel`` are re-checked so an API quirk cannot leak a private conversation."""
        params = {
            "types": "public_channel",
            "exclude_archived": "false",
            "limit": self.config.page_size,
        }
        if pagination and pagination.get("cursor"):
            params["cursor"] = pagination["cursor"]

        response = self.session.get(f"{BASE_URL}/conversations.list", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            raise SlackApiError(data.get("error"))

        records = []
        for channel in data.get("channels", []):
            if channel.get("is_private") or not channel.get("is_channel"):
                logger.warning(
                    f"Skipping non-public conversation {channel.get('id')} returned by "
                    f"types=public_channel (is_private={channel.get('is_private')}, "
                    f"is_channel={channel.get('is_channel')})"
                )
                continue

            channel_id = str(channel["id"])
            try:
                member_ids = self._get_channel_members(channel_id)
            except SlackApiError as e:
                if e.error not in PER_CHANNEL_MEMBER_ERRORS:
                    logger.error(
                        f"conversations.members failed workspace-wide with {e.error!r} "
                        f"(first seen on channel {channel_id}) -- failing the stream rather "
                        f"than publishing empty member lists"
                    )
                    raise
                logger.warning(f"conversations.members failed for channel {channel_id}: {e.error}")
                member_ids = []

            channel["members"] = member_ids
            channel["num_members_fetched"] = len(member_ids)
            records.append(SourceRecord(id=channel_id, data=channel))

        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        next_pagination = {"cursor": cursor} if cursor else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def _load_internal_user_ids(self) -> set:
        """users.list returns only the token's own team, so any channel member missing from it is external."""
        if self._internal_user_ids is not None:
            return self._internal_user_ids

        ids = set()
        cursor = ""
        while True:
            params = {"limit": self.config.page_size}
            if cursor:
                params["cursor"] = cursor

            response = self.session.get(f"{BASE_URL}/users.list", params=params, timeout=TIMEOUT)
            response.raise_for_status()
            data = response.json()

            if not data.get("ok"):
                raise SlackApiError(data.get("error"))

            ids.update(str(m["id"]) for m in data.get("members", []))

            cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
            if not cursor:
                break

        logger.info(f"Loaded {len(ids)} internal user IDs from users.list")
        self._internal_user_ids = ids
        return ids

    def _get_user_info(self, user_id: str) -> Optional[dict]:
        """Only users.info returns a Slack Connect user's profile; users.profile.get says user_not_found."""
        response = self.session.get(f"{BASE_URL}/users.info", params={"user": user_id}, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            error = data.get("error")
            if error in PER_USER_INFO_ERRORS:
                logger.warning(f"users.info could not resolve {user_id}: {error}")
                return None
            raise SlackApiError(error)

        return data.get("user")

    def get_external_users(self, pagination: dict = None) -> SourceIteration:
        """One record per distinct Slack Connect user; only externally-shared channels can hold one."""
        internal_ids = self._load_internal_user_ids()

        params = {
            "types": "public_channel",
            "exclude_archived": "false",
            "limit": self.config.page_size,
        }
        if pagination and pagination.get("cursor"):
            params["cursor"] = pagination["cursor"]

        response = self.session.get(f"{BASE_URL}/conversations.list", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        data = response.json()

        if not data.get("ok"):
            raise SlackApiError(data.get("error"))

        records = []
        for channel in data.get("channels", []):
            if channel.get("is_private") or not channel.get("is_channel"):
                continue
            if not (channel.get("is_ext_shared") or channel.get("is_pending_ext_shared")):
                continue

            channel_id = str(channel["id"])
            try:
                member_ids = self._get_channel_members(channel_id)
            except SlackApiError as e:
                if e.error not in PER_CHANNEL_MEMBER_ERRORS:
                    raise
                logger.warning(f"conversations.members failed for channel {channel_id}: {e.error}")
                continue

            for member_id in member_ids:
                if member_id in internal_ids or member_id in self._seen_external_ids:
                    continue
                self._seen_external_ids.add(member_id)

                user = self._get_user_info(member_id)
                if user is None:
                    continue

                # Lets `users` UNION `external_users` form a complete identity table.
                user["is_external"] = True
                user["first_seen_channel_id"] = channel_id
                records.append(SourceRecord(id=str(user["id"]), data=user))

        cursor = (data.get("response_metadata") or {}).get("next_cursor") or ""
        next_pagination = {"cursor": cursor} if cursor else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == SlackStreams.USERS:
            return self.get_users(pagination)

        if self.config.stream == SlackStreams.PUBLIC_CHANNELS:
            return self.get_public_channels(pagination)

        if self.config.stream == SlackStreams.EXTERNAL_USERS:
            return self.get_external_users(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Slack")
