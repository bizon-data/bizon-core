import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any, List, Optional, Tuple

from loguru import logger
from pydantic import Field
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.graphql import (
    ErrorClass,
    TransientGraphQLError,
    build_page_shrink_sequence,
    errors_match,
    graphql_post,
    graphql_post_adaptive,
)
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

GRAPHQL_URL = "https://api.linear.app/graphql"


# Matched against `extensions.type`/`extensions.code`; Linear doesn't populate the top-level `type`.
# RATE_LIMITED is here because Linear can also send one as an HTTP-200 `errors[]` entry.
_TRANSIENT_GRAPHQL_ERROR_TYPES = ("INTERNAL", "SERVICE_UNAVAILABLE", "RATE_LIMITED")
_TRANSIENT_GRAPHQL_TYPE_FIELDS = ("extensions.type", "extensions.code")

# "internal server error" is Linear's signature for a transient resolver failure on an HTTP 200.
_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES = (
    "internal server error",
    "something went wrong",
    "timeout",
    "timed out",
    "service unavailable",
)

# A history page starts at 100, so the transport's default ladder doesn't fit.
HISTORY_SHRINK_STEPS = (100, 50, 10, 1)


def _classify_linear_graphql(response, body: dict) -> Optional[ErrorClass]:
    """Linear rate-limits with HTTP 400 + a RATELIMITED extension, not a 429. Other non-OK statuses are left
    unclaimed (the adapter already retried 5xx), and only the signatures above are TRANSIENT so a query bug
    fails fast."""
    errors = body.get("errors") or []
    if response.status_code == 400 and errors_match(
        errors, types=("RATELIMITED",), type_fields=_TRANSIENT_GRAPHQL_TYPE_FIELDS
    ):
        return ErrorClass.RATE_LIMITED
    if not response.ok or not errors:
        return None
    if errors_match(
        errors,
        types=_TRANSIENT_GRAPHQL_ERROR_TYPES,
        message_signatures=_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES,
        type_fields=_TRANSIENT_GRAPHQL_TYPE_FIELDS,
    ):
        return ErrorClass.TRANSIENT
    return ErrorClass.FATAL


PROJECTS_QUERY = """
query Projects($first: Int!, $after: String, $filter: ProjectFilter) {
  projects(first: $first, after: $after, filter: $filter, includeArchived: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      description
      slugId
      url
      state
      color
      icon
      content
      priority
      priorityLabel
      sortOrder
      health
      progress
      scope
      targetDate
      startDate
      trashed
      slackIssueComments
      slackIssueStatuses
      slackNewIssue
      createdAt
      updatedAt
      archivedAt
      canceledAt
      completedAt
      startedAt
      healthUpdatedAt
      scopeHistory
      issueCountHistory
      completedScopeHistory
      completedIssueCountHistory
      inProgressScopeHistory
      lead { id name email }
      creator { id name email }
      status { id name color type }
      teams(first: 20) { nodes { id name key } }
      members(first: 50) { nodes { id name email } }
      labels(first: 20) { nodes { id name color } }
      projectMilestones(first: 20) { nodes { id name description targetDate sortOrder } }
    }
  }
}
"""

PROJECT_LABELS_QUERY = """
query ProjectLabels($first: Int!, $after: String) {
  projectLabels(first: $first, after: $after, includeArchived: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      name
      color
      description
      createdAt
      updatedAt
      archivedAt
      creator { id name email }
      parent { id name }
    }
  }
}
"""

ISSUES_QUERY = """
query Issues($first: Int!, $after: String, $filter: IssueFilter) {
  issues(first: $first, after: $after, filter: $filter, includeArchived: true) {
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      id
      title
      description
      number
      priority
      estimate
      dueDate
      sortOrder
      subIssueSortOrder
      trashed
      updatedAt
      startedAt
      snoozedUntilAt
      previousIdentifiers
      createdAt
      completedAt
      canceledAt
      archivedAt
      autoClosedAt
      autoArchivedAt
      identifier
      url
      customerTicketCount
      parent { id }
      project { id }
      projectMilestone { id }
      team { id }
      state { id }
      snoozedBy { id }
      assignee { id }
      creator { id }
      cycle { id }
      labels(first: 100) { nodes { id } }
      subscribers(first: 100) { nodes { id } }
    }
  }
}
"""


# There is no root `issueHistories` field: fetch N issues with their first M history entries per request, then
# paginate any issue whose history overflows M.

HISTORY_FIELDS = """
actor { id }
actorId
actors { id }
addedLabelIds
addedLabels { id }
addedToReleaseIds
addedToReleases { id }
archived
archivedAt
attachment { id }
attachmentId
autoArchived
autoClosed
botActor { id }
changes
createdAt
customerNeedId
descriptionUpdatedBy { id }
fromAssignee { id }
fromAssigneeId
fromCycle { id }
fromCycleId
fromDelegate { id }
fromDueDate
fromEstimate
fromParent { id }
fromParentId
fromPriority
fromProject { id }
fromProjectId
fromProjectMilestone { id }
fromSlaBreached
fromSlaBreachesAt
fromSlaStartedAt
fromSlaType
fromState { id }
fromStateId
fromTeam { id }
fromTeamId
fromTitle
id
updatedAt
issue { id }
issueImport { id }
trashed
updatedDescription
relationChanges { identifier type }
removedFromReleaseIds
removedFromReleases { id }
removedLabelIds
removedLabels { id }
toAssignee { id }
toAssigneeId
toConvertedProject { id }
toConvertedProjectId
toCycle { id }
toCycleId
toDelegate { id }
toDueDate
toEstimate
toParent { id }
toParentId
toPriority
toProject { id }
toProjectId
toProjectMilestone { id }
toSlaBreached
toSlaBreachesAt
toSlaStartedAt
toSlaType
toState { id }
toStateId
toTeam { id }
toTeamId
toTitle
triageResponsibilityAutoAssigned
triageResponsibilityNotifiedUsers { id }
triageResponsibilityTeam { id }
triageRuleMetadata {
  triageRuleError {
    conflictForSameChildLabel
    conflictingLabels { id }
    fromTeam { id }
    property
    toTeam { id }
    type
  }
  updatedByTriageRule { id }
}
workflowMetadata {
  aiConversation { id }
  workflowDefinition { id }
}
"""

ISSUES_WITH_HISTORY_QUERY = f"""
query IssuesWithHistory($first: Int!, $after: String, $filter: IssueFilter, $historyFirst: Int!) {{
  issues(first: $first, after: $after, filter: $filter, includeArchived: true) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id
      history(first: $historyFirst, includeArchived: true) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {HISTORY_FIELDS} }}
      }}
    }}
  }}
}}
"""

ISSUE_HISTORY_PAGE_QUERY = f"""
query IssueHistoryPage($issueId: String!, $first: Int!, $after: String) {{
  issue(id: $issueId) {{
    history(first: $first, after: $after, includeArchived: true) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ {HISTORY_FIELDS} }}
    }}
  }}
}}
"""

# No `history` subfield, so it can't hit an unresolvable history node; `_fetch_history_batch`'s last-resort
# floor uses it to advance past a parent whose history fails even at first=1/historyFirst=1.
ISSUES_IDONLY_QUERY = """
query IssuesIdOnly($first: Int!, $after: String, $filter: IssueFilter) {
  issues(first: $first, after: $after, filter: $filter, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes { id }
  }
}
"""


# No root `projectHistories` field either; batched like issue history.

PROJECT_HISTORY_FIELDS = """
id
createdAt
updatedAt
archivedAt
entries
project { id }
"""

PROJECTS_WITH_HISTORY_QUERY = f"""
query ProjectsWithHistory($first: Int!, $after: String, $filter: ProjectFilter, $historyFirst: Int!) {{
  projects(first: $first, after: $after, filter: $filter, includeArchived: true) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id
      history(first: $historyFirst, includeArchived: true) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {PROJECT_HISTORY_FIELDS} }}
      }}
    }}
  }}
}}
"""

PROJECT_HISTORY_PAGE_QUERY = f"""
query ProjectHistoryPage($projectId: String!, $first: Int!, $after: String) {{
  project(id: $projectId) {{
    history(first: $first, after: $after, includeArchived: true) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ {PROJECT_HISTORY_FIELDS} }}
    }}
  }}
}}
"""

# See ISSUES_IDONLY_QUERY.
PROJECTS_IDONLY_QUERY = """
query ProjectsIdOnly($first: Int!, $after: String, $filter: ProjectFilter) {
  projects(first: $first, after: $after, filter: $filter, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes { id }
  }
}
"""


# No root `initiativeHistories` field either.

INITIATIVE_HISTORY_FIELDS = """
id
createdAt
updatedAt
archivedAt
entries
initiative { id }
"""

INITIATIVES_WITH_HISTORY_QUERY = f"""
query InitiativesWithHistory($first: Int!, $after: String, $filter: InitiativeFilter, $historyFirst: Int!) {{
  initiatives(first: $first, after: $after, filter: $filter, includeArchived: true) {{
    pageInfo {{ hasNextPage endCursor }}
    nodes {{
      id
      history(first: $historyFirst, includeArchived: true) {{
        pageInfo {{ hasNextPage endCursor }}
        nodes {{ {INITIATIVE_HISTORY_FIELDS} }}
      }}
    }}
  }}
}}
"""

INITIATIVE_HISTORY_PAGE_QUERY = f"""
query InitiativeHistoryPage($initiativeId: String!, $first: Int!, $after: String) {{
  initiative(id: $initiativeId) {{
    history(first: $first, after: $after, includeArchived: true) {{
      pageInfo {{ hasNextPage endCursor }}
      nodes {{ {INITIATIVE_HISTORY_FIELDS} }}
    }}
  }}
}}
"""

# See ISSUES_IDONLY_QUERY.
INITIATIVES_IDONLY_QUERY = """
query InitiativesIdOnly($first: Int!, $after: String, $filter: InitiativeFilter) {
  initiatives(first: $first, after: $after, filter: $filter, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes { id }
  }
}
"""


# Fixed column sets: columns Linear has since removed (e.g. user.role) are emitted as null to keep the schema stable.

ORGANIZATION_QUERY = """
query OrganizationSingleton {
  organization {
    id name urlKey createdAt updatedAt archivedAt
    logoUrl gitBranchFormat gitLinkbackMessagesEnabled gitPublicLinkbackMessagesEnabled
    periodUploadVolume roadmapEnabled samlEnabled
    deletionRequestedAt
  }
}
"""

TEAMS_QUERY = """
query Teams($first: Int!, $after: String) {
  teams(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name key color icon description private timezone
      archivedAt createdAt updatedAt
      autoArchivePeriod autoClosePeriod
      cycleCooldownTime cycleDuration cycleIssueAutoAssignCompleted cycleIssueAutoAssignStarted
      cycleLockToActive cycleStartDay cyclesEnabled upcomingCycleCount
      defaultIssueEstimate
      groupIssueHistory
      issueEstimationAllowZero issueEstimationExtended issueEstimationType
      triageEnabled
      autoCloseStateId
      organization { id }
      parent { id }
      defaultIssueState { id }
      defaultTemplateForMembers { id }
      defaultTemplateForNonMembers { id }
      markedAsDuplicateWorkflowState { id }
      triageIssueState { id }
    }
  }
}
"""

# team_keys has no root query; it is derived from the teams list.
TEAM_KEYS_QUERY = """
query TeamKeys($first: Int!, $after: String) {
  teams(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes { id key createdAt updatedAt archivedAt }
  }
}
"""

TEAM_MEMBERSHIPS_QUERY = """
query TeamMemberships($first: Int!, $after: String) {
  teamMemberships(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id owner archivedAt createdAt updatedAt
      team { id } user { id }
    }
  }
}
"""

INITIATIVES_QUERY = """
query Initiatives($first: Int!, $after: String) {
  initiatives(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name description color icon slugId status
      targetDate targetDateResolution
      startedAt completedAt
      archivedAt createdAt updatedAt
      organization { id } creator { id } owner { id } parentInitiative { id }
    }
  }
}
"""

INITIATIVE_TO_PROJECTS_QUERY = """
query InitiativeToProjects($first: Int!, $after: String) {
  initiativeToProjects(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id archivedAt createdAt updatedAt
      initiative { id } project { id }
    }
  }
}
"""

USERS_QUERY = """
query Users($first: Int!, $after: String) {
  users(first: $first, after: $after, includeArchived: true, includeDisabled: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name displayName email description avatarUrl
      active timezone disableReason gitHubUserId
      statusEmoji statusLabel statusUntilAt
      archivedAt createdAt updatedAt
      organization { id }
    }
  }
}
"""

PROJECT_STATUSES_QUERY = """
query ProjectStatuses($first: Int!, $after: String) {
  projectStatuses(first: $first, after: $after) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name description color type position indefinite
      archivedAt createdAt updatedAt
    }
  }
}
"""

PROJECT_UPDATES_QUERY = """
query ProjectUpdates($first: Int!, $after: String) {
  projectUpdates(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id bodyData health reactionData editedAt
      archivedAt createdAt updatedAt
      project { id } user { id }
    }
  }
}
"""

PROJECT_MILESTONES_QUERY = """
query ProjectMilestonesAll($first: Int!, $after: String) {
  projectMilestones(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name targetDate
      archivedAt createdAt updatedAt
      project { id }
    }
  }
}
"""

COMMENTS_QUERY = """
query Comments($first: Int!, $after: String) {
  comments(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id bodyData reactionData editedAt
      issueId parentId resolvingCommentId resolvedAt
      archivedAt createdAt updatedAt
      user { id } resolvingUser { id }
    }
  }
}
"""

CYCLES_QUERY = """
query Cycles($first: Int!, $after: String) {
  cycles(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name number startsAt endsAt completedAt autoArchivedAt
      completedIssueCountHistory completedScopeHistory
      issueCountHistory scopeHistory
      archivedAt createdAt updatedAt
      team { id }
      uncompletedIssuesUponClose(first: 250) { nodes { id } }
    }
  }
}
"""

WORKFLOW_STATES_QUERY = """
query WorkflowStates($first: Int!, $after: String) {
  workflowStates(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name description color type position
      archivedAt createdAt updatedAt
      team { id }
    }
  }
}
"""

ISSUE_LABELS_QUERY = """
query IssueLabels($first: Int!, $after: String) {
  issueLabels(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id name color description
      archivedAt createdAt updatedAt
      creator { id } organization { id } team { id }
    }
  }
}
"""

ISSUE_RELATIONS_QUERY = """
query IssueRelations($first: Int!, $after: String) {
  issueRelations(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id type archivedAt createdAt updatedAt
      issue { id } relatedIssue { id }
    }
  }
}
"""

ATTACHMENTS_QUERY = """
query Attachments($first: Int!, $after: String) {
  attachments(first: $first, after: $after, includeArchived: true) {
    pageInfo { hasNextPage endCursor }
    nodes {
      id title subtitle url source metadata
      groupBySource
      archivedAt createdAt updatedAt
      creator { id } issue { id }
    }
  }
}
"""


class LinearStreams(str, Enum):
    PROJECTS = "projects"
    PROJECT_LABELS = "project_labels"
    ISSUES = "issues"
    ISSUE_HISTORY = "issue_history"
    PROJECT_HISTORY = "project_history"
    INITIATIVE_HISTORY = "initiative_history"
    ORGANIZATIONS = "organizations"
    TEAMS = "teams"
    TEAM_KEYS = "team_keys"
    TEAM_MEMBERSHIPS = "team_memberships"
    INITIATIVES = "initiatives"
    INITIATIVE_TO_PROJECTS = "initiative_to_projects"
    USERS = "users"
    PROJECT_STATUSES = "project_statuses"
    PROJECT_UPDATES = "project_updates"
    PROJECT_MILESTONES = "project_milestones"
    COMMENTS = "comments"
    CYCLES = "cycles"
    WORKFLOW_STATES = "workflow_states"
    ISSUE_LABEL = "issue_label"
    ISSUE_RELATION = "issue_relation"
    ATTACHMENT = "attachment"


class LinearSourceConfig(SourceConfig):
    stream: LinearStreams
    api_key: str
    start_date: Optional[str] = None  # ISO 8601 — initial cutoff for incremental streams
    # Rate limits arrive as HTTP 400 with a RATELIMITED body, which the GraphQL classifier must read.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


class LinearAuth(AuthBase):
    """Personal API key in the Authorization header, without a Bearer prefix."""

    def __init__(self, api_key: str):
        self.api_key = api_key

    def __call__(self, request):
        request.headers["Authorization"] = self.api_key
        request.headers["Content-Type"] = "application/json"
        return request


class LinearSource(AbstractSource):
    def __init__(self, config: LinearSourceConfig):
        super().__init__(config)
        self.config: LinearSourceConfig = config
        self._start_date: Optional[datetime] = None
        # Logged once per run, to show which cap (request count or complexity) is binding.
        self._ratelimit_headers_logged = False
        if config.start_date:
            self._start_date = datetime.fromisoformat(config.start_date.replace("Z", "+00:00"))
            if self._start_date.tzinfo is None:
                self._start_date = self._start_date.replace(tzinfo=timezone.utc)

    def get_authenticator(self) -> AuthBase:
        return LinearAuth(self.config.api_key)

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in LinearStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return LinearSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            result = self._graphql_post("{ viewer { id } }")
            if result.get("data", {}).get("viewer", {}).get("id"):
                return True, None
            return False, "No viewer ID returned"
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _graphql_post(self, query: str, variables: dict = None) -> dict:
        """`retry_exceptions` is empty: the session adapter already retries 5xx."""
        return graphql_post(
            self.session,
            GRAPHQL_URL,
            query,
            variables,
            classify=_classify_linear_graphql,
            rate_limit_wait_hint=self._rate_limit_wait_hint,
            transient_attempts=4,
            transient_wait=(15, 60),
            rate_limit_attempts=7,
            rate_limit_wait=(60, 900),
            log_prefix="Linear",
        )

    def _rate_limit_wait_hint(self, response) -> Optional[float]:
        if not self._ratelimit_headers_logged:
            self._ratelimit_headers_logged = True
            logger.warning(
                "Linear rate-limit response (first occurrence this run) -- "
                f"Retry-After={response.headers.get('Retry-After')!r}, "
                f"X-RateLimit-Requests-Remaining={response.headers.get('X-RateLimit-Requests-Remaining')!r}, "
                f"X-RateLimit-Requests-Reset={response.headers.get('X-RateLimit-Requests-Reset')!r}, "
                f"X-RateLimit-Complexity-Remaining={response.headers.get('X-RateLimit-Complexity-Remaining')!r}, "
                f"X-RateLimit-Complexity-Reset={response.headers.get('X-RateLimit-Complexity-Reset')!r}, "
                f"body={response.text[:1000]!r}"
            )
        return self._rate_limit_reset_wait_seconds(response)

    @staticmethod
    def _rate_limit_reset_wait_seconds(response) -> Optional[float]:
        """Seconds until reset from Retry-After, else X-RateLimit-Requests-Reset, else
        X-RateLimit-Complexity-Reset (both epoch milliseconds); None if none parse. Never raises."""
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return max(0.0, float(retry_after))
            except (TypeError, ValueError):
                pass

        now = time.time()
        for header in ("X-RateLimit-Requests-Reset", "X-RateLimit-Complexity-Reset"):
            value = response.headers.get(header)
            if not value:
                continue
            try:
                reset_epoch_seconds = float(value) / 1000.0
            except (TypeError, ValueError):
                continue
            return max(0.0, reset_epoch_seconds - now)
        return None

    def _graphql_post_adaptive(
        self,
        query: str,
        variables: dict,
        *,
        start_size: int,
        min_size: int = 1,
        label: str = "",
    ) -> Tuple[dict, int]:
        """No phantom probe: Linear hasn't shown the pageInfo-only asymmetry, so the floor stays fail-closed."""
        return graphql_post_adaptive(
            self.session,
            GRAPHQL_URL,
            query,
            variables,
            page_size_var="first",
            start_size=start_size,
            floor=min_size,
            shrink_steps=HISTORY_SHRINK_STEPS,
            probe=None,
            label=label,
            post=self._graphql_post,
        )

    def _fetch_history_batch(
        self,
        *,
        batch_query: str,
        idonly_query: str,
        connection: str,
        variables: dict,
        history_first: int,
        label: str,
    ) -> Tuple[dict, int]:
        """An unresolvable history node would otherwise wedge the stream, since `after` only advances on success.

        Retrying the same `after`, it shrinks `historyFirst`, then `first` to 1, then falls back to an id-only
        query that synthesizes an empty history for the head parent (`skipped_head` = 1) so the cursor advances.
        If even the id-only query fails it is a different outage, and the error propagates.
        """
        try:
            return self._graphql_post(batch_query, variables), 0
        except TransientGraphQLError:
            pass

        prev_size = history_first
        for size in build_page_shrink_sequence(history_first, 1, steps=HISTORY_SHRINK_STEPS):
            if size >= history_first:
                continue
            logger.warning(
                f"history batch still failing for {label} (after={variables.get('after')}) "
                f"-- shrinking historyFirst {prev_size}->{size}, retrying same cursor"
            )
            variables["historyFirst"] = size
            try:
                return self._graphql_post(batch_query, variables), 0
            except TransientGraphQLError:
                prev_size = size
                continue

        orig_first = variables["first"]
        logger.warning(
            f"history batch still failing for {label} (after={variables.get('after')}) "
            f"-- shrinking first {orig_first}->1, retrying same cursor"
        )
        variables["first"] = 1
        variables["historyFirst"] = 1
        try:
            return self._graphql_post(batch_query, variables), 0
        except TransientGraphQLError:
            pass

        idonly_variables: dict = {"first": 1, "after": variables.get("after")}
        if "filter" in variables:
            idonly_variables["filter"] = variables["filter"]
        idonly_result = self._graphql_post(idonly_query, idonly_variables)
        idonly_page = idonly_result["data"][connection]
        idonly_page_info = idonly_page["pageInfo"]
        idonly_nodes = idonly_page["nodes"]

        if not idonly_nodes:
            return idonly_result, 0

        node_id = idonly_nodes[0]["id"]
        logger.error(
            f"SKIPPING unfetchable head history for {label}:{node_id} (after={variables.get('after')}); "
            f"history node #1 failed even at first=1/historyFirst=1. Advanced {connection} cursor via "
            f"id-only floor and dropped this parent's history. Fix upstream (Linear) and re-fetch via a "
            f"stream reset."
        )
        synthetic = {
            "data": {
                connection: {
                    "pageInfo": idonly_page_info,
                    "nodes": [
                        {
                            "id": node_id,
                            "history": {
                                "nodes": [],
                                "pageInfo": {"hasNextPage": False, "endCursor": None},
                            },
                        }
                    ],
                }
            }
        }
        return synthetic, 1

    def get_projects(self, pagination: dict = None) -> SourceIteration:
        variables: dict = {"first": 20}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]
        if self._start_date is not None:
            variables["filter"] = {"updatedAt": {"gt": self._start_date.isoformat()}}

        result = self._graphql_post(PROJECTS_QUERY, variables)

        nodes = result["data"]["projects"]["nodes"]
        page_info = result["data"]["projects"]["pageInfo"]

        records = [SourceRecord(id=node["id"], data=node) for node in nodes]

        next_pagination = page_info if page_info.get("hasNextPage") else {}

        logger.info(f"Fetched {len(records)} projects (hasNextPage={page_info.get('hasNextPage')})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_issue_history(node: dict) -> dict:
        out = dict(node)
        issue = out.get("issue")
        out["issueId"] = issue["id"] if issue else None
        issue_import = out.get("issueImport")
        out["issueImportId"] = issue_import["id"] if issue_import else None
        return out

    @staticmethod
    def _transform_issue(node: dict) -> dict:
        RELATION_FIELDS = [
            "parent",
            "project",
            "projectMilestone",
            "team",
            "state",
            "snoozedBy",
            "assignee",
            "creator",
            "cycle",
        ]
        ARRAY_FIELDS = {
            "labels": "labelIds",
            "subscribers": "subscriberIds",
        }

        issue = {}
        for key, value in node.items():
            if key in RELATION_FIELDS:
                issue[f"{key}Id"] = value["id"] if value else None
            elif key in ARRAY_FIELDS:
                issue[ARRAY_FIELDS[key]] = [n["id"] for n in (value or {}).get("nodes", [])]
            else:
                issue[key] = value
        return issue

    def get_issues(self, pagination: dict = None) -> SourceIteration:
        variables: dict = {"first": 250}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]
        if self._start_date is not None:
            variables["filter"] = {"updatedAt": {"gt": self._start_date.isoformat()}}

        result = self._graphql_post(ISSUES_QUERY, variables)

        nodes = result["data"]["issues"]["nodes"]
        page_info = result["data"]["issues"]["pageInfo"]

        records = [SourceRecord(id=node["id"], data=self._transform_issue(node)) for node in nodes]

        next_pagination = page_info if page_info.get("hasNextPage") else {}

        logger.info(f"Fetched {len(records)} issues (hasNextPage={page_info.get('hasNextPage')})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_project_labels(self, pagination: dict = None) -> SourceIteration:
        variables = {"first": 50}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]

        result = self._graphql_post(PROJECT_LABELS_QUERY, variables)

        nodes = result["data"]["projectLabels"]["nodes"]
        page_info = result["data"]["projectLabels"]["pageInfo"]

        records = [SourceRecord(id=node["id"], data=node) for node in nodes]

        next_pagination = page_info if page_info.get("hasNextPage") else {}

        logger.info(f"Fetched {len(records)} project labels (hasNextPage={page_info.get('hasNextPage')})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    # The binding cap is usually requests per hour, not complexity, so batches are large; an IssueHistory node
    # selects ~80 fields, so 25 x 10 keeps a request's cost modest.
    ISSUES_BATCH_SIZE = 25
    HISTORY_FIRST = 10
    HISTORY_PAGE_SIZE = 100

    def _paginate_issue_history(self, issue_id: str, after: str) -> Tuple[List[dict], int]:
        """Overflow history pages for one issue; `skipped` is 1 if the tail was abandoned."""
        nodes: List[dict] = []
        cursor = after
        size = self.HISTORY_PAGE_SIZE
        while True:
            variables = {"issueId": issue_id, "after": cursor}
            try:
                result, size = self._graphql_post_adaptive(
                    ISSUE_HISTORY_PAGE_QUERY, variables, start_size=size, label=f"issue_history:{issue_id}"
                )
            except TransientGraphQLError:
                # Dropping the tail beats wedging the stream, but the run then reports success: this ERROR and
                # skipped_overflow= are the only signal. The batch query itself stays fail-closed.
                logger.error(
                    f"SKIPPING unfetchable overflow history page for issue_history:{issue_id} "
                    f"(after={cursor}) after exhausting retries+shrink; dropping the remaining "
                    f"tail of this parent's history. Fix upstream (Linear) and re-fetch via a "
                    f"stream reset."
                )
                return nodes, 1
            issue = result["data"].get("issue")
            if issue is None:
                break
            page = issue["history"]
            nodes.extend(page["nodes"])
            page_info = page["pageInfo"]
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return nodes, 0

    def get_issue_history(self, pagination: dict = None) -> SourceIteration:
        variables: dict = {"first": self.ISSUES_BATCH_SIZE, "historyFirst": self.HISTORY_FIRST}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]
        if self._start_date is not None:
            variables["filter"] = {"updatedAt": {"gt": self._start_date.isoformat()}}

        result, skipped_head = self._fetch_history_batch(
            batch_query=ISSUES_WITH_HISTORY_QUERY,
            idonly_query=ISSUES_IDONLY_QUERY,
            connection="issues",
            variables=variables,
            history_first=self.HISTORY_FIRST,
            label="issue_history",
        )
        page = result["data"]["issues"]
        issue_nodes = page["nodes"]
        page_info = page["pageInfo"]

        records: List[SourceRecord] = []
        overflow_issues: List[Tuple[str, str]] = []
        for issue in issue_nodes:
            history = issue.get("history") or {}
            for node in history.get("nodes", []):
                records.append(SourceRecord(id=node["id"], data=self._transform_issue_history(node)))
            history_page = history.get("pageInfo") or {}
            if history_page.get("hasNextPage"):
                overflow_issues.append((issue["id"], history_page["endCursor"]))

        skipped_overflow = 0
        for issue_id, after in overflow_issues:
            extra, skipped = self._paginate_issue_history(issue_id, after)
            skipped_overflow += skipped
            logger.info(f"Paginated {len(extra)} extra history entries for long-tail issue {issue_id}")
            for node in extra:
                records.append(SourceRecord(id=node["id"], data=self._transform_issue_history(node)))

        logger.info(
            f"Batch fetched {len(records)} history records from {len(issue_nodes)} issues "
            f"(overflow={len(overflow_issues)}, hasNextPage={page_info.get('hasNextPage')}, "
            f"skipped_overflow={skipped_overflow}, skipped_head={skipped_head})"
        )

        next_pagination = page_info if page_info.get("hasNextPage") else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    # Project/initiative history nodes are far lighter than IssueHistory, so these batches are larger.
    PROJECTS_BATCH_SIZE = 50
    PROJECT_HISTORY_FIRST = 50
    PROJECT_HISTORY_PAGE_SIZE = 100

    INITIATIVES_BATCH_SIZE = 50
    INITIATIVE_HISTORY_FIRST = 50
    INITIATIVE_HISTORY_PAGE_SIZE = 100

    @staticmethod
    def _transform_project_history(node: dict) -> dict:
        out = dict(node)
        project = out.pop("project", None)
        out["projectId"] = project["id"] if project else None
        return out

    def _paginate_project_history(self, project_id: str, after: str) -> Tuple[List[dict], int]:
        """Overflow history pages for one project; `skipped` is 1 if the tail was abandoned."""
        nodes: List[dict] = []
        cursor = after
        size = self.PROJECT_HISTORY_PAGE_SIZE
        while True:
            variables = {"projectId": project_id, "after": cursor}
            try:
                result, size = self._graphql_post_adaptive(
                    PROJECT_HISTORY_PAGE_QUERY, variables, start_size=size, label=f"project_history:{project_id}"
                )
            except TransientGraphQLError:
                # Dropping the tail beats wedging the stream, but the run then reports success: this ERROR and
                # skipped_overflow= are the only signal. The batch query itself stays fail-closed.
                logger.error(
                    f"SKIPPING unfetchable overflow history page for project_history:{project_id} "
                    f"(after={cursor}) after exhausting retries+shrink; dropping the remaining "
                    f"tail of this parent's history. Fix upstream (Linear) and re-fetch via a "
                    f"stream reset."
                )
                return nodes, 1
            project = result["data"].get("project")
            if project is None:
                break
            page = project["history"]
            nodes.extend(page["nodes"])
            page_info = page["pageInfo"]
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return nodes, 0

    def get_project_history(self, pagination: dict = None) -> SourceIteration:
        variables: dict = {"first": self.PROJECTS_BATCH_SIZE, "historyFirst": self.PROJECT_HISTORY_FIRST}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]
        if self._start_date is not None:
            variables["filter"] = {"updatedAt": {"gt": self._start_date.isoformat()}}

        result, skipped_head = self._fetch_history_batch(
            batch_query=PROJECTS_WITH_HISTORY_QUERY,
            idonly_query=PROJECTS_IDONLY_QUERY,
            connection="projects",
            variables=variables,
            history_first=self.PROJECT_HISTORY_FIRST,
            label="project_history",
        )
        page = result["data"]["projects"]
        project_nodes = page["nodes"]
        page_info = page["pageInfo"]

        records: List[SourceRecord] = []
        overflow_projects: List[Tuple[str, str]] = []
        for project in project_nodes:
            history = project.get("history") or {}
            for node in history.get("nodes", []):
                records.append(SourceRecord(id=str(node["id"]), data=self._transform_project_history(node)))
            history_page = history.get("pageInfo") or {}
            if history_page.get("hasNextPage"):
                overflow_projects.append((project["id"], history_page["endCursor"]))

        skipped_overflow = 0
        for project_id, after in overflow_projects:
            extra, skipped = self._paginate_project_history(project_id, after)
            skipped_overflow += skipped
            logger.info(f"Paginated {len(extra)} extra history entries for long-tail project {project_id}")
            for node in extra:
                records.append(SourceRecord(id=str(node["id"]), data=self._transform_project_history(node)))

        logger.info(
            f"Batch fetched {len(records)} history records from {len(project_nodes)} projects "
            f"(overflow={len(overflow_projects)}, hasNextPage={page_info.get('hasNextPage')}, "
            f"skipped_overflow={skipped_overflow}, skipped_head={skipped_head})"
        )

        next_pagination = page_info if page_info.get("hasNextPage") else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    @staticmethod
    def _transform_initiative_history(node: dict) -> dict:
        out = dict(node)
        initiative = out.pop("initiative", None)
        out["initiativeId"] = initiative["id"] if initiative else None
        return out

    def _paginate_initiative_history(self, initiative_id: str, after: str) -> Tuple[List[dict], int]:
        """Overflow history pages for one initiative; `skipped` is 1 if the tail was abandoned."""
        nodes: List[dict] = []
        cursor = after
        size = self.INITIATIVE_HISTORY_PAGE_SIZE
        while True:
            variables = {"initiativeId": initiative_id, "after": cursor}
            try:
                result, size = self._graphql_post_adaptive(
                    INITIATIVE_HISTORY_PAGE_QUERY,
                    variables,
                    start_size=size,
                    label=f"initiative_history:{initiative_id}",
                )
            except TransientGraphQLError:
                # Dropping the tail beats wedging the stream, but the run then reports success: this ERROR and
                # skipped_overflow= are the only signal. The batch query itself stays fail-closed.
                logger.error(
                    f"SKIPPING unfetchable overflow history page for "
                    f"initiative_history:{initiative_id} (after={cursor}) after exhausting "
                    f"retries+shrink; dropping the remaining tail of this parent's history. "
                    f"Fix upstream (Linear) and re-fetch via a stream reset."
                )
                return nodes, 1
            initiative = result["data"].get("initiative")
            if initiative is None:
                break
            page = initiative["history"]
            nodes.extend(page["nodes"])
            page_info = page["pageInfo"]
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info["endCursor"]
        return nodes, 0

    def get_initiative_history(self, pagination: dict = None) -> SourceIteration:
        variables: dict = {"first": self.INITIATIVES_BATCH_SIZE, "historyFirst": self.INITIATIVE_HISTORY_FIRST}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]
        if self._start_date is not None:
            variables["filter"] = {"updatedAt": {"gt": self._start_date.isoformat()}}

        result, skipped_head = self._fetch_history_batch(
            batch_query=INITIATIVES_WITH_HISTORY_QUERY,
            idonly_query=INITIATIVES_IDONLY_QUERY,
            connection="initiatives",
            variables=variables,
            history_first=self.INITIATIVE_HISTORY_FIRST,
            label="initiative_history",
        )
        page = result["data"]["initiatives"]
        initiative_nodes = page["nodes"]
        page_info = page["pageInfo"]

        records: List[SourceRecord] = []
        overflow_initiatives: List[Tuple[str, str]] = []
        for initiative in initiative_nodes:
            history = initiative.get("history") or {}
            for node in history.get("nodes", []):
                records.append(SourceRecord(id=str(node["id"]), data=self._transform_initiative_history(node)))
            history_page = history.get("pageInfo") or {}
            if history_page.get("hasNextPage"):
                overflow_initiatives.append((initiative["id"], history_page["endCursor"]))

        skipped_overflow = 0
        for initiative_id, after in overflow_initiatives:
            extra, skipped = self._paginate_initiative_history(initiative_id, after)
            skipped_overflow += skipped
            logger.info(f"Paginated {len(extra)} extra history entries for long-tail initiative {initiative_id}")
            for node in extra:
                records.append(SourceRecord(id=str(node["id"]), data=self._transform_initiative_history(node)))

        logger.info(
            f"Batch fetched {len(records)} history records from {len(initiative_nodes)} initiatives "
            f"(overflow={len(overflow_initiatives)}, hasNextPage={page_info.get('hasNextPage')}, "
            f"skipped_overflow={skipped_overflow}, skipped_head={skipped_head})"
        )

        next_pagination = page_info if page_info.get("hasNextPage") else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == LinearStreams.PROJECTS:
            return self.get_projects(pagination)
        if self.config.stream == LinearStreams.PROJECT_LABELS:
            return self.get_project_labels(pagination)
        if self.config.stream == LinearStreams.ISSUES:
            return self.get_issues(pagination)
        if self.config.stream == LinearStreams.ISSUE_HISTORY:
            return self.get_issue_history(pagination)
        if self.config.stream == LinearStreams.PROJECT_HISTORY:
            return self.get_project_history(pagination)
        if self.config.stream == LinearStreams.INITIATIVE_HISTORY:
            return self.get_initiative_history(pagination)
        if self.config.stream == LinearStreams.ORGANIZATIONS:
            return self.get_organizations(pagination)
        if self.config.stream == LinearStreams.TEAMS:
            return self.get_teams(pagination)
        if self.config.stream == LinearStreams.TEAM_KEYS:
            return self.get_team_keys(pagination)
        if self.config.stream == LinearStreams.TEAM_MEMBERSHIPS:
            return self.get_team_memberships(pagination)
        if self.config.stream == LinearStreams.INITIATIVES:
            return self.get_initiatives(pagination)
        if self.config.stream == LinearStreams.INITIATIVE_TO_PROJECTS:
            return self.get_initiative_to_projects(pagination)
        if self.config.stream == LinearStreams.USERS:
            return self.get_users(pagination)
        if self.config.stream == LinearStreams.PROJECT_STATUSES:
            return self.get_project_statuses(pagination)
        if self.config.stream == LinearStreams.PROJECT_UPDATES:
            return self.get_project_updates(pagination)
        if self.config.stream == LinearStreams.PROJECT_MILESTONES:
            return self.get_project_milestones(pagination)
        if self.config.stream == LinearStreams.COMMENTS:
            return self.get_comments(pagination)
        if self.config.stream == LinearStreams.CYCLES:
            return self.get_cycles(pagination)
        if self.config.stream == LinearStreams.WORKFLOW_STATES:
            return self.get_workflow_states(pagination)
        if self.config.stream == LinearStreams.ISSUE_LABEL:
            return self.get_issue_labels(pagination)
        if self.config.stream == LinearStreams.ISSUE_RELATION:
            return self.get_issue_relations(pagination)
        if self.config.stream == LinearStreams.ATTACHMENT:
            return self.get_attachments(pagination)

        raise NotImplementedError(f"Stream '{self.config.stream}' is not implemented for Linear source")

    def get_records_after(
        self,
        source_state: SourceIncrementalState,
        pagination: dict = None,
    ) -> SourceIteration:
        since = source_state.last_run
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)
        logger.info(f"Linear incremental sync {self.config.stream} since {since.isoformat()}")

        original = self._start_date
        self._start_date = since
        try:
            return self.get(pagination)
        finally:
            self._start_date = original

    @staticmethod
    def _flatten_node(
        node: dict,
        expected_fields: List[str],
        relation_fields: Optional[List[str]] = None,
        array_fields: Optional[dict] = None,
    ) -> dict:
        """Keys match `expected_fields` exactly; `relation_fields` map `foo { id }` to `fooId`, `array_fields` map
        `foos { nodes { id } }` to an output name, and absent fields are None."""
        relations = set(relation_fields or [])
        arrays = array_fields or {}
        out: dict = {}
        for k, v in node.items():
            if k in relations:
                out[f"{k}Id"] = v["id"] if v else None
            elif k in arrays:
                out[arrays[k]] = [n["id"] for n in (v or {}).get("nodes", [])]
            else:
                out[k] = v
        for field in expected_fields:
            if field not in out:
                out[field] = None
        return out

    def _paginate(
        self,
        query: str,
        connection_name: str,
        pagination: Optional[dict],
        transform,
        first: int = 50,
    ) -> SourceIteration:
        variables: dict = {"first": first}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]

        result = self._graphql_post(query, variables)
        page = result["data"][connection_name]
        records = [SourceRecord(id=node["id"], data=transform(node)) for node in page["nodes"]]
        page_info = page["pageInfo"]
        next_pagination = page_info if page_info.get("hasNextPage") else {}
        logger.info(f"Fetched {len(records)} {connection_name} (hasNextPage={page_info.get('hasNextPage')})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    _ORGANIZATION_FIELDS = [
        "allowedAuthServices",
        "archivedAt",
        "createdAt",
        "deletionRequestedAt",
        "deletionRequestedById",
        "enabled",
        "gitBranchFormat",
        "gitLinkbackMessagesEnabled",
        "gitPublicLinkbackMessagesEnabled",
        "id",
        "linearPreviewFlags",
        "logoUrl",
        "name",
        "periodUploadVolume",
        "reducedPersonalInformation",
        "roadmapEnabled",
        "samlEnabled",
        "serviceId",
        "updatedAt",
        "urlKey",
    ]
    _TEAM_FIELDS = [
        "archivedAt",
        "autoArchivePeriod",
        "autoClosePeriod",
        "autoCloseStateId",
        "color",
        "createdAt",
        "cycleCooldownTime",
        "cycleDuration",
        "cycleIssueAutoAssignCompleted",
        "cycleIssueAutoAssignStarted",
        "cycleLockToActive",
        "cycleStartDay",
        "cyclesEnabled",
        "defaultIssueEstimate",
        "defaultIssueStateId",
        "defaultTemplateForMembersId",
        "defaultTemplateForNonMembersId",
        "description",
        "draftWorkflowStateId",
        "groupIssueHistory",
        "icon",
        "id",
        "inviteHash",
        "issueEstimationAllowZero",
        "issueEstimationExtended",
        "issueEstimationType",
        "issueOrderingNoPriorityFirst",
        "issueSortOrderDefaultToBottom",
        "key",
        "markedAsDuplicateWorkflowStateId",
        "mergeWorkflowStateId",
        "name",
        "organizationId",
        "parentId",
        "private",
        "reviewWorkflowStateId",
        "slackIssueComments",
        "slackIssueStatuses",
        "slackNewIssue",
        "startWorkflowStateId",
        "timezone",
        "triageEnabled",
        "triageIssueStateId",
        "upcomingCycleCount",
        "updatedAt",
    ]
    _TEAM_KEY_FIELDS = ["archivedAt", "createdAt", "id", "name", "teamId", "updatedAt"]
    _TEAM_MEMBERSHIP_FIELDS = [
        "archivedAt",
        "createdAt",
        "id",
        "owner",
        "teamId",
        "updatedAt",
        "userId",
    ]
    _INITIATIVE_FIELDS = [
        "archivedAt",
        "color",
        "completedAt",
        "createdAt",
        "creatorId",
        "description",
        "icon",
        "id",
        "name",
        "organizationId",
        "ownerId",
        "parentInitiativeId",
        "slugId",
        "startedAt",
        "status",
        "targetDate",
        "targetDateResolution",
        "updatedAt",
    ]
    _INITIATIVE_TO_PROJECT_FIELDS = [
        "archivedAt",
        "createdAt",
        "id",
        "initiativeId",
        "projectId",
        "updatedAt",
    ]
    _USER_FIELDS = [
        "active",
        "alternativeEmails",
        "archivedAt",
        "avatarUrl",
        "createdAt",
        "description",
        "disableReason",
        "displayName",
        "email",
        "external",
        "externalUserMapping",
        "gitHubUserId",
        "id",
        "inviteHash",
        "name",
        "organizationId",
        "role",
        "statusEmoji",
        "statusLabel",
        "statusUntilAt",
        "timezone",
        "updatedAt",
        "userAccountId",
    ]
    _PROJECT_STATUS_FIELDS = [
        "archivedAt",
        "color",
        "createdAt",
        "description",
        "id",
        "indefinite",
        "name",
        "organizationId",
        "position",
        "type",
        "updatedAt",
    ]
    _PROJECT_UPDATE_FIELDS = [
        "archivedAt",
        "bodyData",
        "createdAt",
        "editedAt",
        "health",
        "id",
        "projectId",
        "reactionData",
        "updatedAt",
        "userId",
    ]
    _PROJECT_MILESTONE_FIELDS = [
        "archivedAt",
        "createdAt",
        "id",
        "name",
        "projectId",
        "targetDate",
        "updatedAt",
    ]
    _COMMENT_FIELDS = [
        "archivedAt",
        "attachmentId",
        "bodyData",
        "createdAt",
        "editedAt",
        "id",
        "issueId",
        "parentId",
        "reactionData",
        "resolvedAt",
        "resolvingCommentId",
        "resolvingUserId",
        "sourceMetadata",
        "updatedAt",
        "userId",
    ]
    _CYCLE_FIELDS = [
        "archivedAt",
        "autoArchivedAt",
        "completedAt",
        "completedIssueCountHistory",
        "completedScopeHistory",
        "createdAt",
        "endsAt",
        "id",
        "issueCountHistory",
        "name",
        "number",
        "scopeHistory",
        "startsAt",
        "teamId",
        "uncompletedIssuesUponCloseIds",
        "updatedAt",
    ]
    _WORKFLOW_STATE_FIELDS = [
        "archivedAt",
        "color",
        "createdAt",
        "description",
        "id",
        "name",
        "position",
        "teamId",
        "type",
        "updatedAt",
    ]
    _ISSUE_LABEL_FIELDS = [
        "archivedAt",
        "color",
        "createdAt",
        "creatorId",
        "description",
        "id",
        "name",
        "organizationId",
        "teamId",
        "updatedAt",
    ]
    _ISSUE_RELATION_FIELDS = [
        "archivedAt",
        "createdAt",
        "id",
        "issueId",
        "relatedIssueId",
        "type",
        "updatedAt",
    ]
    _ATTACHMENT_FIELDS = [
        "archivedAt",
        "createdAt",
        "creatorId",
        "groupBySource",
        "iconUploadId",
        "id",
        "issueId",
        "metadata",
        "source",
        "sourceMetadata",
        "subtitle",
        "title",
        "updatedAt",
        "url",
    ]

    def get_organizations(self, pagination: dict = None) -> SourceIteration:
        result = self._graphql_post(ORGANIZATION_QUERY)
        node = result["data"]["organization"]
        if node is None:
            logger.warning("Linear `organization` query returned null")
            return SourceIteration(records=[], next_pagination={})
        record = SourceRecord(
            id=node["id"],
            data=self._flatten_node(node, self._ORGANIZATION_FIELDS),
        )
        logger.info("Fetched 1 organization (singleton)")
        return SourceIteration(records=[record], next_pagination={})

    def get_teams(self, pagination: dict = None) -> SourceIteration:
        relations = [
            "organization",
            "parent",
            "defaultIssueState",
            "defaultTemplateForMembers",
            "defaultTemplateForNonMembers",
            "markedAsDuplicateWorkflowState",
            "triageIssueState",
        ]
        return self._paginate(
            TEAMS_QUERY,
            "teams",
            pagination,
            transform=lambda n: self._flatten_node(n, self._TEAM_FIELDS, relation_fields=relations),
            first=50,
        )

    def get_team_keys(self, pagination: dict = None) -> SourceIteration:
        """`id = name = team.key`; the team's timestamps are kept so joins against `teams` stay consistent."""
        variables: dict = {"first": 50}
        if pagination and pagination.get("endCursor"):
            variables["after"] = pagination["endCursor"]

        result = self._graphql_post(TEAM_KEYS_QUERY, variables)
        page = result["data"]["teams"]
        records: List[SourceRecord] = []
        for team in page["nodes"]:
            key = team.get("key")
            if not key:
                continue
            data = {
                "id": key,
                "name": key,
                "teamId": team["id"],
                "archivedAt": team.get("archivedAt"),
                "createdAt": team.get("createdAt"),
                "updatedAt": team.get("updatedAt"),
            }
            records.append(SourceRecord(id=key, data=data))
        page_info = page["pageInfo"]
        next_pagination = page_info if page_info.get("hasNextPage") else {}
        logger.info(f"Fetched {len(records)} team_keys (hasNextPage={page_info.get('hasNextPage')})")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_team_memberships(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            TEAM_MEMBERSHIPS_QUERY,
            "teamMemberships",
            pagination,
            transform=lambda n: self._flatten_node(n, self._TEAM_MEMBERSHIP_FIELDS, relation_fields=["team", "user"]),
            first=100,
        )

    def get_initiatives(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            INITIATIVES_QUERY,
            "initiatives",
            pagination,
            transform=lambda n: self._flatten_node(
                n, self._INITIATIVE_FIELDS, relation_fields=["creator", "organization", "owner", "parentInitiative"]
            ),
            first=50,
        )

    def get_initiative_to_projects(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            INITIATIVE_TO_PROJECTS_QUERY,
            "initiativeToProjects",
            pagination,
            transform=lambda n: self._flatten_node(
                n, self._INITIATIVE_TO_PROJECT_FIELDS, relation_fields=["initiative", "project"]
            ),
            first=100,
        )

    def get_users(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            USERS_QUERY,
            "users",
            pagination,
            transform=lambda n: self._flatten_node(n, self._USER_FIELDS, relation_fields=["organization"]),
            first=100,
        )

    def get_project_statuses(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            PROJECT_STATUSES_QUERY,
            "projectStatuses",
            pagination,
            transform=lambda n: self._flatten_node(n, self._PROJECT_STATUS_FIELDS),
            first=50,
        )

    def get_project_updates(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            PROJECT_UPDATES_QUERY,
            "projectUpdates",
            pagination,
            transform=lambda n: self._flatten_node(n, self._PROJECT_UPDATE_FIELDS, relation_fields=["project", "user"]),
            first=100,
        )

    def get_project_milestones(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            PROJECT_MILESTONES_QUERY,
            "projectMilestones",
            pagination,
            transform=lambda n: self._flatten_node(n, self._PROJECT_MILESTONE_FIELDS, relation_fields=["project"]),
            first=100,
        )

    def get_comments(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            COMMENTS_QUERY,
            "comments",
            pagination,
            transform=lambda n: self._flatten_node(n, self._COMMENT_FIELDS, relation_fields=["user", "resolvingUser"]),
            first=100,
        )

    def get_cycles(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            CYCLES_QUERY,
            "cycles",
            pagination,
            transform=lambda n: self._flatten_node(
                n,
                self._CYCLE_FIELDS,
                relation_fields=["team"],
                array_fields={"uncompletedIssuesUponClose": "uncompletedIssuesUponCloseIds"},
            ),
            first=50,
        )

    def get_workflow_states(self, pagination: dict = None) -> SourceIteration:
        return self._paginate(
            WORKFLOW_STATES_QUERY,
            "workflowStates",
            pagination,
            transform=lambda n: self._flatten_node(n, self._WORKFLOW_STATE_FIELDS, relation_fields=["team"]),
            first=100,
        )

    def get_issue_labels(self, pagination: dict = None) -> SourceIteration:
        relations = ["creator", "organization", "team"]
        return self._paginate(
            ISSUE_LABELS_QUERY,
            "issueLabels",
            pagination,
            transform=lambda n: self._flatten_node(n, self._ISSUE_LABEL_FIELDS, relation_fields=relations),
            first=50,
        )

    def get_issue_relations(self, pagination: dict = None) -> SourceIteration:
        relations = ["issue", "relatedIssue"]
        return self._paginate(
            ISSUE_RELATIONS_QUERY,
            "issueRelations",
            pagination,
            transform=lambda n: self._flatten_node(n, self._ISSUE_RELATION_FIELDS, relation_fields=relations),
            first=50,
        )

    def get_attachments(self, pagination: dict = None) -> SourceIteration:
        relations = ["creator", "issue"]
        return self._paginate(
            ATTACHMENTS_QUERY,
            "attachments",
            pagination,
            transform=lambda n: self._flatten_node(n, self._ATTACHMENT_FIELDS, relation_fields=relations),
            first=50,
        )
