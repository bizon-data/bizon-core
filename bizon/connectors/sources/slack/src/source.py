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

# Without a read timeout a stalled socket never becomes a retryable event and the run hangs.
TIMEOUT = (10, 60)

# conversations.members is Tier 4 (~100 req/min) and returns bare user IDs, so a
# large page keeps the per-channel fan-out to as few calls as possible.
MEMBERS_PAGE_SIZE = 1000

# users.info errors scoped to one user ID rather than to the token. A member ID that
# cannot be resolved is skipped; anything else (missing_scope, invalid_auth, ...) is
# workspace-wide and must fail the stream.
PER_USER_INFO_ERRORS = frozenset({"user_not_found", "user_not_visible"})

# conversations.members errors scoped to ONE channel degrade to an empty member list.
# Anything else (missing_scope, invalid_auth, ratelimited, ...) would hit every channel,
# so degrading it would publish members: [] everywhere on a "successful" run.
PER_CHANNEL_MEMBER_ERRORS = frozenset(
    {
        "channel_not_found",
        "fetch_members_failed",
        "method_not_supported_for_channel_type",
    }
)


class SlackApiError(RuntimeError):
    """A Slack ``{"ok": false, "error": ...}`` body, carrying the raw error code.

    Slack signals failures with HTTP 200 plus an ``ok: false`` body, so the error code
    is the only thing that distinguishes "this one channel is unreadable" from "this
    token cannot read any channel". Callers branch on ``.error``, never on substrings.
    """

    def __init__(self, error: Optional[str]):
        self.error = error or "unknown_error"
        super().__init__(f"Slack API error: {self.error}")


class SlackApiKeyAuth(AuthBase):
    """Bearer-token auth for the Slack API, from the ``api_key`` source config field."""

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

        # Per-run caches for external_users: instance state survives across get() calls, which keeps
        # the users.info cost proportional to distinct external users rather than to memberships.
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
        """Test connection with a cheap, single-user users.list call."""
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
        """Fetch all users in the Slack workspace."""
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
        """Fetch every member user ID of a channel, following conversations.members cursors.

        Large channels exceed a single page, so it is drained here rather than truncated.

        An ``ok: false`` body raises SlackApiError and transport failures propagate; deciding
        whether a failure is per-channel or must fail the stream is the caller's job.
        """
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
        """Fetch the workspace's public channels, each with its list of member user IDs.

        ``types=public_channel`` already excludes private conversations, but ``is_channel`` /
        ``is_private`` are re-checked so an API quirk cannot leak a private one into this stream.
        """
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
        """Page users.list once per run and cache the workspace's own user IDs.

        users.list returns only members of the token's own team, so it doubles as the
        "is this person external?" oracle: any channel member missing from it is a
        Slack Connect user from another workspace.
        """
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
        """Resolve one user via users.info, or None if that ID alone is unresolvable.

        users.info is the only endpoint that returns a profile (including the email,
        via the users:read.email scope) for a Slack Connect "stranger" --
        users.profile.get answers user_not_found for them.
        """
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
        """Emit one record per distinct external (Slack Connect) user in a public channel.

        Only externally-shared channels are walked: a stranger can only be in a channel shared
        with their workspace, so this cuts the members fan-out to the Slack Connect channels.

        Deduplication is per-run and in-process (``_seen_external_ids``): the same person appears
        in many shared channels, and emitting them once per channel would multiply rows.
        """
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

                # Marks the row as external so `users` UNION `external_users` gives a
                # complete identity table without re-deriving membership.
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
