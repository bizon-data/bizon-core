"""GitHub source: repositories, pull requests, reviews, comments, commits, deployments, workflow runs and
repository content, from the GitHub REST and GraphQL APIs using GitHub App authentication.

Auto-discovers all installations the app has access to, so no installation ID is required.
"""

import base64
import glob
import json
import os
import posixpath
import shutil
import tarfile
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from typing import Any, Callable, List, Optional, Set, Tuple

import jwt
from loguru import logger
from pydantic import Field
from requests import Session
from requests.adapters import HTTPAdapter
from requests.auth import AuthBase
from requests.exceptions import (
    ChunkedEncodingError,
    HTTPError,
    Timeout,
)
from requests.exceptions import (
    ConnectionError as RequestsConnectionError,
)
from requests.exceptions import (
    RetryError as RequestsRetryError,
)
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from urllib3.util.retry import Retry

from bizon.source.config import HttpConfig, HttpRetryConfig, SourceConfig
from bizon.source.graphql import (
    ErrorClass,
    GraphQLRateLimited,
    MutableCounter,
    PhantomProbe,
    TransientGraphQLError,  # noqa: F401 -- re-exported for RepoFetchError callers and tests
    errors_match,
    graphql_post,
    graphql_post_adaptive,
)
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

BASE_URL = "https://api.github.com"
GRAPHQL_URL = "https://api.github.com/graphql"
API_VERSION = "2022-11-28"

PR_GRAPHQL_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number title state body url
        createdAt updatedAt closedAt mergedAt
        merged isDraft additions deletions changedFiles mergeable
        mergeCommit { oid }
        author { login avatarUrl ... on User { databaseId } }
        mergedBy { login avatarUrl ... on User { databaseId } }
        assignees(first: 10) {
          totalCount
          nodes { login avatarUrl ... on User { databaseId } }
        }
        headRefName headRefOid baseRefName baseRefOid
        headRepository { nameWithOwner }
        baseRepository { nameWithOwner }
        commits { totalCount }
        comments { totalCount }
      }
    }
  }
}
"""

# Node-free sibling of the PR_*_GRAPHQL_QUERY queries (same connection, same variables). Some
# repos carry pull-request index entries whose nodes GitHub can never resolve: if a page fails
# but this pageInfo-only probe succeeds, only node resolution is broken and the cursor can step
# past those entries. If the probe fails too it is a real outage and callers fail closed.
PR_PAGE_CURSOR_PROBE_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
    }
  }
}
"""

PR_REVIEWS_GRAPHQL_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        reviews(first: 10) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId state body
            createdAt updatedAt submittedAt
            authorAssociation
            author { login avatarUrl ... on User { databaseId } }
            commit { oid }
          }
        }
      }
    }
  }
}
"""

PR_REVIEWS_EXTRA_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          databaseId state body
          createdAt updatedAt submittedAt
          authorAssociation
          author { login avatarUrl ... on User { databaseId } }
          commit { oid }
        }
      }
    }
  }
}
"""

# Shared inline fragment for review comment fields
_REVIEW_COMMENT_FIELDS = """
  databaseId
  body
  path
  line
  originalLine
  startLine
  originalStartLine
  diffHunk
  outdated
  createdAt
  updatedAt
  url
  author { login avatarUrl ... on User { databaseId } }
  replyTo { databaseId }
  commit { oid }
  originalCommit { oid }
  pullRequestReview { databaseId }
"""

PR_REVIEW_COMMENTS_GRAPHQL_QUERY = (
    """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        reviews(first: 10) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            databaseId
            comments(first: 100) {
              totalCount
              pageInfo { hasNextPage endCursor }
              nodes {
                """
    + _REVIEW_COMMENT_FIELDS
    + """
              }
            }
          }
        }
      }
    }
  }
}
"""
)

PR_REVIEW_COMMENTS_EXTRA_REVIEWS_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          comments(first: 100) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              """
    + _REVIEW_COMMENT_FIELDS
    + """
            }
          }
        }
      }
    }
  }
}
"""
)

PR_REVIEW_COMMENTS_EXTRA_COMMENTS_QUERY = (
    """
query($reviewNodeId: ID!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  node(id: $reviewNodeId) {
    ... on PullRequestReview {
      comments(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          """
    + _REVIEW_COMMENT_FIELDS
    + """
        }
      }
    }
  }
}
"""
)

PR_COMMENTS_GRAPHQL_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        comments(first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            databaseId
            body
            createdAt updatedAt
            url
            author { login avatarUrl ... on User { databaseId } }
          }
        }
      }
    }
  }
}
"""

PR_COMMENTS_EXTRA_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      comments(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
        pageInfo { hasNextPage endCursor }
        nodes {
          databaseId
          body
          createdAt updatedAt
          url
          author { login avatarUrl ... on User { databaseId } }
        }
      }
    }
  }
}
"""

DEPLOYMENTS_GRAPHQL_QUERY = """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    deployments(first: $first, after: $after, orderBy: {field: CREATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId environment description task
        originalEnvironment commitOid
        createdAt updatedAt state
        ref { name }
        commit { oid }
        creator { login avatarUrl ... on User { databaseId } }
        payload
        latestStatus {
          state description createdAt updatedAt
          logUrl environmentUrl
          creator { login avatarUrl ... on User { databaseId } }
        }
      }
    }
  }
}
"""

# Shared inline fragment for timeline item nodes (most event types only expose `id`, not `databaseId`)
_TIMELINE_ITEMS_NODES = """
  __typename
  ... on AddedToProjectEvent { id createdAt actor { login } }
  ... on AssignedEvent { id createdAt actor { login } assignee { ... on User { login databaseId } } }
  ... on AutoMergeDisabledEvent { id createdAt actor { login } }
  ... on AutoMergeEnabledEvent { id createdAt actor { login } }
  ... on BaseRefChangedEvent { id createdAt actor { login } }
  ... on ClosedEvent { id createdAt actor { login } }
  ... on CommentDeletedEvent { id createdAt actor { login } }
  ... on ConnectedEvent { id createdAt actor { login } }
  ... on ConvertToDraftEvent { id createdAt actor { login } }
  ... on CrossReferencedEvent { id createdAt actor { login } }
  ... on DemilestonedEvent { id createdAt actor { login } }
  ... on DeployedEvent { id createdAt actor { login } }
  ... on HeadRefDeletedEvent { id createdAt actor { login } }
  ... on HeadRefForcePushedEvent { id createdAt actor { login } }
  ... on HeadRefRestoredEvent { id createdAt actor { login } }
  ... on IssueComment { databaseId createdAt updatedAt body author { login ... on User { databaseId } } }
  ... on LabeledEvent { id createdAt actor { login } label { name } }
  ... on LockedEvent { id createdAt actor { login } }
  ... on MergedEvent { id createdAt actor { login } commit { oid } mergeRefName }
  ... on MilestonedEvent { id createdAt actor { login } milestoneTitle }
  ... on PullRequestCommit { commit { oid message authoredDate committedDate author { name email user { login databaseId } } } }
  ... on PullRequestReview { databaseId createdAt updatedAt state body author { login ... on User { databaseId } } }
  ... on ReadyForReviewEvent { id createdAt actor { login } }
  ... on ReferencedEvent { id createdAt actor { login } }
  ... on RemovedFromProjectEvent { id createdAt actor { login } }
  ... on RenamedTitleEvent { id createdAt actor { login } previousTitle currentTitle }
  ... on ReopenedEvent { id createdAt actor { login } }
  ... on ReviewDismissedEvent { id createdAt actor { login } }
  ... on ReviewRequestRemovedEvent { id createdAt actor { login } requestedReviewer { ... on User { login databaseId } ... on Team { slug } } }
  ... on ReviewRequestedEvent { id createdAt actor { login } requestedReviewer { ... on User { login databaseId } ... on Team { slug } } }
  ... on UnassignedEvent { id createdAt actor { login } assignee { ... on User { login databaseId } } }
  ... on UnlabeledEvent { id createdAt actor { login } label { name } }
  ... on UnlockedEvent { id createdAt actor { login } }
"""

PR_TIMELINE_GRAPHQL_QUERY = (
    """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        timelineItems(first: 50) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            """
    + _TIMELINE_ITEMS_NODES
    + """
          }
        }
      }
    }
  }
}
"""
)

PR_TIMELINE_EXTRA_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      timelineItems(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          """
    + _TIMELINE_ITEMS_NODES
    + """
        }
      }
    }
  }
}
"""
)

DEFAULT_BINARY_EXTENSIONS = frozenset(
    {
        # Images
        "png",
        "jpg",
        "jpeg",
        "gif",
        "bmp",
        "ico",
        "svg",
        "webp",
        "tiff",
        "tif",
        "psd",
        "ai",
        "eps",
        # Compiled
        "pyc",
        "pyo",
        "class",
        "o",
        "obj",
        "so",
        "dll",
        "dylib",
        "exe",
        "bin",
        "elf",
        # Archives
        "zip",
        "tar",
        "gz",
        "bz2",
        "xz",
        "7z",
        "rar",
        "jar",
        "war",
        "ear",
        "whl",
        # Media
        "mp3",
        "mp4",
        "avi",
        "mov",
        "mkv",
        "flv",
        "wav",
        "ogg",
        "flac",
        "aac",
        "wma",
        "wmv",
        "webm",
        # Fonts
        "ttf",
        "otf",
        "woff",
        "woff2",
        "eot",
        # Binary docs
        "pdf",
        "doc",
        "docx",
        "xls",
        "xlsx",
        "ppt",
        "pptx",
        # Data
        "db",
        "sqlite",
        "sqlite3",
        "pickle",
        "pkl",
        "npy",
        "npz",
        "h5",
        "hdf5",
        "parquet",
        "avro",
        # Other
        "lock",
        "map",
        "wasm",
        "deb",
        "rpm",
        "msi",
        "dmg",
        "iso",
    }
)

SKIP_PATH_SUFFIXES = (".min.js", ".min.css")


# Shared inline fragment for reaction nodes (used by both reaction streams)
_REACTION_FIELDS = """
  databaseId
  content
  createdAt
  user { login ... on User { databaseId } }
"""

PR_COMMENT_REACTIONS_GRAPHQL_QUERY = (
    """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        comments(first: 100, orderBy: {field: UPDATED_AT, direction: DESC}) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            databaseId
            updatedAt
            reactions(first: 20) {
              totalCount
              pageInfo { hasNextPage endCursor }
              nodes {
                """
    + _REACTION_FIELDS
    + """
              }
            }
          }
        }
      }
    }
  }
}
"""
)

PR_COMMENT_REACTIONS_EXTRA_COMMENTS_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      comments(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          updatedAt
          reactions(first: 20) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              """
    + _REACTION_FIELDS
    + """
            }
          }
        }
      }
    }
  }
}
"""
)

PR_COMMENT_REACTIONS_EXTRA_REACTIONS_QUERY = (
    """
query($commentNodeId: ID!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  node(id: $commentNodeId) {
    ... on IssueComment {
      reactions(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          """
    + _REACTION_FIELDS
    + """
        }
      }
    }
  }
}
"""
)

# Review-comment reaction queries.
# Cost budget: PRs (3) -> reviews (10) -> comments (50) -> reactions (20).
# Approx cost: 1 + 1 + 3 + 30 + 3*10*50 + 3*10*50*1 = ~1535 points/query.
PR_REVIEW_COMMENT_REACTIONS_GRAPHQL_QUERY = (
    """
query($owner: String!, $name: String!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequests(first: $first, after: $after, orderBy: {field: UPDATED_AT, direction: DESC}) {
      pageInfo { hasNextPage endCursor }
      nodes {
        databaseId number updatedAt
        reviews(first: 10) {
          totalCount
          pageInfo { hasNextPage endCursor }
          nodes {
            id
            databaseId
            comments(first: 50) {
              totalCount
              pageInfo { hasNextPage endCursor }
              nodes {
                id
                databaseId
                updatedAt
                reactions(first: 20) {
                  totalCount
                  pageInfo { hasNextPage endCursor }
                  nodes {
                    """
    + _REACTION_FIELDS
    + """
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""
)

PR_REVIEW_COMMENT_REACTIONS_EXTRA_REVIEWS_QUERY = (
    """
query($owner: String!, $name: String!, $number: Int!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  repository(owner: $owner, name: $name) {
    pullRequest(number: $number) {
      reviews(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          comments(first: 50) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              id
              databaseId
              updatedAt
              reactions(first: 20) {
                totalCount
                pageInfo { hasNextPage endCursor }
                nodes {
                  """
    + _REACTION_FIELDS
    + """
                }
              }
            }
          }
        }
      }
    }
  }
}
"""
)

PR_REVIEW_COMMENT_REACTIONS_EXTRA_COMMENTS_QUERY = (
    """
query($reviewNodeId: ID!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  node(id: $reviewNodeId) {
    ... on PullRequestReview {
      comments(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          databaseId
          updatedAt
          reactions(first: 20) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              """
    + _REACTION_FIELDS
    + """
            }
          }
        }
      }
    }
  }
}
"""
)

PR_REVIEW_COMMENT_REACTIONS_EXTRA_REACTIONS_QUERY = (
    """
query($commentNodeId: ID!, $first: Int!, $after: String) {
  rateLimit { cost remaining resetAt }
  node(id: $commentNodeId) {
    ... on PullRequestReviewComment {
      reactions(first: $first, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          """
    + _REACTION_FIELDS
    + """
        }
      }
    }
  }
}
"""
)


class GitHubStreams(str, Enum):
    REPOSITORIES = "repositories"
    MEMBERS = "members"
    PULL_REQUESTS = "pull_requests"
    PULL_REQUEST_REVIEWS = "pull_request_reviews"
    PULL_REQUEST_COMMENTS = "pull_request_comments"
    PULL_REQUEST_REVIEW_COMMENTS = "pull_request_review_comments"
    COMMITS = "commits"
    DEPLOYMENTS = "deployments"
    WORKFLOW_RUNS = "workflow_runs"
    WORKFLOW_JOBS = "workflow_jobs"
    REPO_CONTENT = "repo_content"
    ORGANIZATION_TEAMS = "organization_teams"
    ORGANIZATION_TEAM_MEMBERS = "organization_team_members"
    PULL_REQUEST_TIMELINE = "pull_request_timeline"
    PULL_REQUEST_COMMENT_REACTIONS = "pull_request_comment_reactions"
    PULL_REQUEST_REVIEW_COMMENT_REACTIONS = "pull_request_review_comment_reactions"


class GitHubSourceConfig(SourceConfig):
    stream: GitHubStreams

    app_id: str = Field(..., description="GitHub App ID")
    private_key: str = Field(..., description="PEM private key (newlines can be escaped as \\n)")
    org: Optional[str] = Field(default=None, description="Optional org filter for org-level endpoints")
    repos: List[str] = Field(
        default_factory=list,
        description="Optional explicit repo list (owner/repo). If empty, auto-discovers from installations.",
    )
    per_page: int = Field(default=100, ge=1, le=100, description="Results per page (max 100)")
    max_file_size: int = Field(
        default=1_000_000, ge=0, description="Skip files larger than this (bytes). 0 = no limit."
    )
    blobs_per_batch: int = Field(default=50, ge=1, le=200, description="Number of blob fetches per get() call.")
    blob_workers: int = Field(
        default=10, ge=1, le=20, description="Concurrent threads for blob fetches within a batch."
    )
    excluded_extensions: Optional[List[str]] = Field(
        default=None, description="Override default binary extension blocklist."
    )
    start_date: Optional[str] = Field(
        default=None,
        description="Only fetch data after this date (ISO format, e.g. '2024-01-01'). Applies to PRs, commits, deployments, workflow runs, and PR reviews.",
    )
    concurrent_repos: int = Field(default=5, ge=1, le=15, description="Number of repos to fetch from concurrently.")
    lookback_days: int = Field(
        default=2,
        ge=0,
        description=(
            "On each incremental run, re-scan this many days before the last successful job's "
            "watermark, in addition to fetching forward. Duplicate rows are expected and should be "
            "deduplicated downstream; this guards against a silently-missed window (e.g. a "
            "transient per-repo error on a prior run)."
        ),
    )
    backfill_since: Optional[str] = Field(
        default=None,
        description=(
            "One-time backfill floor (ISO date, e.g. '2024-01-01'). When set, incremental runs "
            "fetch from min(watermark - lookback_days, backfill_since) instead of the watermark "
            "alone, so a previously skipped window is re-fetched and appended (deduplicate "
            "downstream on _source_record_id / latest _source_timestamp). Remove it after one "
            "successful run: while set, every run re-scans from this date, so the window grows "
            "unbounded. It is a floor, never a ceiling -- ignored once the watermark is older."
        ),
    )
    max_phantom_skips_per_page: int = Field(
        default=50,
        ge=0,
        description=(
            "Max consecutive unresolvable GraphQL index entries to step over at one cursor "
            "before giving up and failing closed. 0 disables the skip entirely (pure "
            "fail-closed behaviour)."
        ),
    )
    max_phantom_skips_per_run: int = Field(
        default=200,
        ge=1,
        description=(
            "Systemic-breakage ceiling: total entries skipped across the whole run. Exceeding "
            "it raises rather than reporting a large silent gap as success."
        ),
    )
    # The source inspects 401/403/404/409/429/5xx responses itself, so the session must not raise on them.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(
            raise_for_status=False,
            retries=HttpRetryConfig(status_forcelist=[429, 500, 502, 503, 504], retry_after_max=None),
        ),
        description="HTTP timeout/retry policy",
    )


class GitHubAppAuth(AuthBase):
    """GitHub App authentication using JWT and installation access tokens.

    Generates a JWT signed with the app's private key, then exchanges it for
    short-lived installation tokens. Automatically discovers all installations
    and caches tokens per installation (refreshed at the 50-minute mark).
    """

    def __init__(self, app_id: str, private_key: str):
        self.app_id = app_id
        self.private_key = private_key
        self._current_token: Optional[str] = None
        self._current_installation_id: Optional[int] = None
        self._token_expires_at: float = 0
        self._installation_tokens: dict[int, tuple[str, float]] = {}
        self._token_lock = threading.Lock()

    def _generate_jwt(self) -> str:
        now = int(time.time())
        payload = {
            "iat": now - 60,  # issued at time, 60s in the past to allow clock drift
            "exp": now + (10 * 60),  # max 10 minutes
            "iss": self.app_id,
        }
        return jwt.encode(payload, self.private_key, algorithm="RS256")

    def get_jwt_headers(self) -> dict:
        """Headers for app-level API calls (e.g., listing installations)."""
        return {
            "Authorization": f"Bearer {self._generate_jwt()}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": API_VERSION,
        }

    def set_installation(self, installation_id: int, token: str, expires_at: float):
        """Cache a token for an installation and set it as current."""
        self._installation_tokens[installation_id] = (token, expires_at)
        self._current_installation_id = installation_id
        self._current_token = token
        self._token_expires_at = expires_at

    def use_jwt_mode(self):
        """Switch to JWT mode for app-level endpoints."""
        self._current_token = None
        self._current_installation_id = None

    def __call__(self, request):
        if self._current_token:
            request.headers["Authorization"] = f"Bearer {self._current_token}"
        else:
            request.headers["Authorization"] = f"Bearer {self._generate_jwt()}"
        request.headers["Accept"] = "application/vnd.github+json"
        request.headers["X-GitHub-Api-Version"] = API_VERSION
        return request

    def get_installation_token_threadsafe(self, installation_id: int, session: Session) -> str:
        """Thread-safe token retrieval that does NOT mutate _current_token.

        Checks the cache under lock, returns if still valid. Otherwise generates
        a new JWT and POSTs for a fresh installation token, updating only the
        cache (not session-level auth state).
        """
        with self._token_lock:
            cached = self._installation_tokens.get(installation_id)
            if cached:
                token, expires_at = cached
                if time.time() < expires_at - 600:  # 50-min mark
                    return token

        # Token expired or missing — fetch outside the lock to avoid blocking
        jwt_token = self._generate_jwt()
        response = session.post(
            f"{BASE_URL}/app/installations/{installation_id}/access_tokens",
            auth=_StaticTokenAuth(jwt_token),
        )
        response.raise_for_status()
        data = response.json()
        token = data["token"]
        expires_at = datetime.fromisoformat(data["expires_at"].replace("Z", "+00:00")).timestamp()

        with self._token_lock:
            self._installation_tokens[installation_id] = (token, expires_at)

        logger.info(f"Obtained access token for installation {installation_id} (threadsafe)")
        return token

    def invalidate_installation_token(self, installation_id: int):
        """Remove a cached token so the next call fetches a fresh one."""
        with self._token_lock:
            self._installation_tokens.pop(installation_id, None)


class _StaticTokenAuth(AuthBase):
    """Sets a fixed token on each request, bypassing session-level auth."""

    def __init__(self, token: str):
        self._token = token

    def __call__(self, request):
        request.headers["Authorization"] = f"Bearer {self._token}"
        request.headers["Accept"] = "application/vnd.github+json"
        request.headers["X-GitHub-Api-Version"] = API_VERSION
        return request


# Kept as module-level names: the REST helpers below raise RateLimitError, and
# RepoFetchError wrapping plus the tests import both from here.
RateLimitError = GraphQLRateLimited


# HTTPAdapter raises RetryError after exhausting its status_forcelist. Treat that
# exhaustion as retryable at the outer tenacity layer too; otherwise a run gets only
# the adapter's same-request retries and the exhausted 500 escapes immediately.
TRANSIENT_NETWORK_ERRORS = (
    RequestsConnectionError,
    ChunkedEncodingError,
    RequestsRetryError,
    Timeout,
)


class RepoFetchError(RuntimeError):
    """Raised when a per-repo GraphQL/REST fetch fails transiently or ambiguously
    (GraphQL `errors[]`, network error) and cannot be safely treated as "no data".

    Swallowing it would let the run succeed and the incremental watermark advance past the
    un-fetched window, leaving a permanent silent gap. Raising fails the run so the window is
    retried next time. Never raise it for a genuinely gone or inaccessible repo
    (`data.repository is None` with no `errors[]`, or REST 404/409): those are legitimate skips.
    """

    def __init__(self, repo: str, context: str, cause: Exception):
        self.repo = repo
        self.fetch_context = context
        self.cause = cause
        super().__init__(f"Failed to fetch {context} for {repo}: {cause}")


# /graphql 5xx responses are handed to the classifier rather than retried at length by the
# HTTP adapter: the GraphQL session does a few quick status retries, then returns the last
# response instead of raising RetryError. A RetryError would land on the 60-900s rate-limit
# backoff with no page-size shrink, and an oversized page can then retry the same request for
# over an hour before failing the stream.
GRAPHQL_TRANSIENT_STATUSES = (500, 502, 503, 504)
GRAPHQL_ADAPTER_RETRIES = 3

# REST `/actions/runs/{id}/jobs` page sizes, largest first. GitHub can fail to render a
# 100-job page for a run with a few hundred jobs and return a deterministic 502 while 50 and
# 25 work, so `_fetch_jobs_for_runs` steps down this ladder instead of retrying the same size.
JOBS_PAGE_SIZE_LADDER = (100, 50, 25, 10)

_TRANSIENT_GRAPHQL_ERROR_TYPES = {"SERVICE_UNAVAILABLE", "INTERNAL", "RATE_LIMITED"}
_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES = (
    "something went wrong while executing your query",
    "timeout",
    "timed out",
    # GitHub's node/query-cost ceiling for an oversized page -- a
    # deterministic cost signal, not a server blip, so the real remedy is the
    # adaptive page-size shrink in `_graphql_post_adaptive`, same as a timeout.
    "resource limits for this query exceeded",
)


def _classify_github_graphql(response, body: dict) -> Optional[ErrorClass]:
    """Map a GitHub GraphQL response onto the shared transport's error classes.

    Conservative on TRANSIENT: only the signatures above match, so a genuine
    query bug (bad field, NOT_FOUND) is FATAL and fails fast instead of burning
    every retry attempt. Only the top-level `type` is inspected -- that's where
    GitHub puts the discriminator, and widening it would silently reclassify
    errors. A 401 is deliberately unclaimed so the caller can refresh the
    installation token and retry.
    """
    if response.status_code in (403, 429):
        return ErrorClass.RATE_LIMITED
    if response.status_code in GRAPHQL_TRANSIENT_STATUSES:
        # A /graphql 502 is usually GitHub killing a query that ran past its ~10s
        # server limit, so retrying the same page size keeps failing the same way.
        # TRANSIENT sends it to the adaptive page-size shrink instead.
        return ErrorClass.TRANSIENT
    if not response.ok:
        return None

    errors = body.get("errors")
    if not errors:
        return None
    if errors_match(
        errors,
        types=_TRANSIENT_GRAPHQL_ERROR_TYPES,
        message_signatures=_TRANSIENT_GRAPHQL_MESSAGE_SIGNATURES,
    ):
        return ErrorClass.TRANSIENT
    return ErrorClass.FATAL


def _github_rate_limit_hint(response) -> Optional[float]:
    """Seconds to wait from GitHub's own throttle headers, or None to fall back
    to the transport's blind backoff ladder."""
    retry_after = response.headers.get("retry-after")
    if retry_after:
        try:
            return max(0.0, float(retry_after))
        except (TypeError, ValueError):
            pass
    reset_ts = response.headers.get("x-ratelimit-reset")
    if reset_ts:
        try:
            return max(float(reset_ts) - time.time(), 1.0)
        except (TypeError, ValueError):
            pass
    return None


def _pr_page_info(body: dict) -> dict:
    """pageInfo block out of a PR_PAGE_CURSOR_PROBE_QUERY response."""
    return body.get("data", {}).get("repository", {}).get("pullRequests", {}).get("pageInfo", {}) or {}


_CURSOR_V2_PREFIX = b"cursor:v2:"


def _try_decode_relay_cursor_timestamp(cursor: str) -> Optional[str]:
    """Best-effort decode of a GitHub Relay `cursor:v2:` opaque cursor's leading ISO
    timestamp, purely so a phantom-entry skip log line names a human-readable position
    instead of only an opaque blob.

    The cursor is base64(b"cursor:v2:" + msgpack([<ISO timestamp str>, <int id>, ...])).
    Decoding it fully would mean adding a `msgpack` dependency for a debug log line, so
    this hand-rolls just enough of the msgpack wire format to pull out the first array
    element when it's a short string (fixstr, the case for every timestamp-ordered
    cursor observed) -- str8/16/32 and non-array payloads are left alone rather than
    guessed at. Deliberately defensive: ANY unexpected shape returns None rather than
    raising, because this is a nice-to-have for log readability, not load-bearing --
    a decode failure must never break the actual GraphQL fetch it's logging about.
    """
    try:
        raw = base64.b64decode(cursor)
    except Exception:
        return None
    if not raw.startswith(_CURSOR_V2_PREFIX):
        return None
    payload = raw[len(_CURSOR_V2_PREFIX) :]
    if len(payload) < 2:
        return None
    # First byte: msgpack fixarray (0x90-0x9f) header for the [timestamp, id, ...] tuple.
    if not (0x90 <= payload[0] <= 0x9F):
        return None
    # Second byte: msgpack fixstr (0xa0-0xbf) header; low 5 bits are the length.
    if not (0xA0 <= payload[1] <= 0xBF):
        return None
    str_len = payload[1] & 0x1F
    start = 2
    end = start + str_len
    if end > len(payload):
        return None
    try:
        return payload[start:end].decode("utf-8")
    except Exception:
        return None


def _describe_relay_cursor(cursor: str) -> Optional[str]:
    timestamp = _try_decode_relay_cursor_timestamp(cursor)
    return f"timestamp={timestamp}" if timestamp else None


class GitHubSource(AbstractSource):
    def __init__(self, config: GitHubSourceConfig):
        # Handle escaped newlines in PEM keys from env vars
        config.private_key = config.private_key.replace("\\n", "\n")
        # Initialize auth before super().__init__ which calls get_authenticator()
        self.auth = GitHubAppAuth(app_id=config.app_id, private_key=config.private_key)
        super().__init__(config)
        self.config: GitHubSourceConfig = config
        self._rate_limit_lock = threading.Lock()  # for sequential _api_get
        # Per-installation rate limit tracking: maps installation_id -> reset timestamp
        self._rate_limit_resets: dict[int, float] = {}
        self._rate_limit_resets_lock = threading.Lock()
        self._start_date: Optional[datetime] = None
        if config.start_date:
            self._start_date = datetime.fromisoformat(config.start_date.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
        # Phantom GraphQL index entries stepped over across this whole run, checked
        # against config.max_phantom_skips_per_run.
        self._phantom_skips = MutableCounter()
        # POST /graphql goes through its own session (see _build_graphql_session).
        # Installation-token minting and REST keep using self.session.
        self._graphql_session = self._build_graphql_session()
        # GET .../actions/runs/{id}/jobs goes through its own fail-fast session
        # (see _build_jobs_session) so a 5xx reaches the page-size shrink quickly.
        self._jobs_session = self._build_jobs_session()
        # repo_content tarball extraction state (instance-local, not pagination --
        # full refresh never resumes mid-job, so losing it on a process restart is fine).
        self._tarball_dir: Optional[str] = None
        self._tarball_repo: Optional[str] = None
        self._tarball_missing: Set[str] = set()

    def __del__(self):
        self._cleanup_tarball_dir()
        super().__del__()

    def _wait_for_rate_limit(self, installation_id: int):
        """Block until the rate limit resets for this installation.

        If another thread already recorded a reset time, we just sleep until
        that time instead of sending a doomed request.
        """
        with self._rate_limit_resets_lock:
            reset_at = self._rate_limit_resets.get(installation_id, 0)
        wait = reset_at - time.time()
        if wait > 0:
            logger.info(f"Installation {installation_id} rate-limited, waiting {wait:.0f}s for reset")
            time.sleep(wait)

    def _set_rate_limit_reset(self, installation_id: int, reset_at: float):
        """Record when the rate limit resets for an installation."""
        with self._rate_limit_resets_lock:
            # Only update if this reset is later than what we already know
            existing = self._rate_limit_resets.get(installation_id, 0)
            if reset_at > existing:
                self._rate_limit_resets[installation_id] = reset_at

    def get_retry_policy(self) -> Optional[Retry]:
        retry = super().get_retry_policy()
        # An exhausted forcelist must raise RetryError: TRANSIENT_NETWORK_ERRORS hands it to the tenacity backoff.
        return retry.new(raise_on_status=True) if retry is not None else None

    @staticmethod
    def _build_graphql_session() -> Session:
        """Session used only for POST /graphql.

        Separate from the default session because the right 5xx policy is different.
        The REST session retries a 502 ten times with backoff, which is correct
        for a GET. On /graphql a persistent 502 usually means the query is too
        heavy, and ten retries of the same page cost about 14 minutes per attempt.
        This session does a few quick status retries to absorb a real blip, then
        returns the last 5xx response so `_classify_github_graphql` can mark it
        TRANSIENT and `graphql_post_adaptive` can shrink the page. 429 is not in
        the forcelist because the classifier handles it with the reset hint.
        Connect errors and read timeouts still raise and go on the transport's
        rate-limit backoff as before.
        """
        session = Session()
        retries = Retry(
            total=GRAPHQL_ADAPTER_RETRIES,
            backoff_factor=2,
            status_forcelist=list(GRAPHQL_TRANSIENT_STATUSES),
            allowed_methods=["POST"],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    @staticmethod
    def _build_jobs_session() -> Session:
        """Session used only for GET /actions/runs/{id}/jobs.

        Same reasoning as `_build_graphql_session`: the shared REST session retries a
        502 ten times with backoff (about 12 min), and a persistent 502 here means the
        page is too big to render, so retrying it never helps. This session does a few
        quick status retries, then returns the last 5xx response (`raise_on_status=False`)
        so `_fetch_jobs_for_runs` can restart the run at a smaller page size.
        """
        session = Session()
        retries = Retry(
            total=3,
            backoff_factor=2,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
            raise_on_status=False,
        )
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    @retry(
        retry=retry_if_exception_type((RateLimitError, *TRANSIENT_NETWORK_ERRORS)),
        wait=wait_exponential(multiplier=60, min=60, max=900),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _api_get(self, url: str, **kwargs):
        """Rate-limit-aware GET that respects GitHub rate limit headers.

        On 403/429: reads retry-after / x-ratelimit-reset headers, sleeps,
        then retries once. If still rate-limited, raises RateLimitError for
        tenacity exponential backoff.
        """
        response = self.session.get(url, **kwargs)
        if response.status_code in (403, 429):
            retry_after = response.headers.get("retry-after")
            remaining = response.headers.get("x-ratelimit-remaining")
            reset_ts = response.headers.get("x-ratelimit-reset")

            if retry_after:
                wait_secs = int(retry_after)
            elif remaining == "0" and reset_ts:
                wait_secs = max(int(reset_ts) - int(time.time()) + 1, 1)
            else:
                wait_secs = 60  # GitHub docs: "wait at least one minute"

            logger.warning(f"Rate limited (HTTP {response.status_code}) — sleeping {wait_secs}s")
            with self._rate_limit_lock:
                time.sleep(wait_secs)

            # Token may have expired during the sleep — refresh before retry
            if self.auth._current_installation_id is not None:
                self._get_installation_token(self.auth._current_installation_id)

            response = self.session.get(url, **kwargs)
            if response.status_code in (403, 429):
                raise RateLimitError(wait_secs)
        return response

    @retry(
        retry=retry_if_exception_type((RateLimitError, *TRANSIENT_NETWORK_ERRORS)),
        wait=wait_exponential(multiplier=60, min=60, max=900),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    def _api_get_with_token(self, url: str, installation_id: int, session: Optional[Session] = None, **kwargs):
        """Thread-safe rate-limit-aware GET using per-request auth.

        Like _api_get but bypasses session-level auth entirely. Each request
        carries its own token via _StaticTokenAuth, so concurrent threads
        never interfere with each other's authentication state.

        Before sending, checks if this installation is known to be rate-limited
        and waits for the reset time. This prevents all threads from firing
        doomed requests and then each sleeping independently.

        `session` overrides self.session for the GET itself (token minting always
        uses self.session).
        """
        session = session or self.session
        # Wait if another thread already discovered this installation is rate-limited
        self._wait_for_rate_limit(installation_id)

        token = self.auth.get_installation_token_threadsafe(installation_id, self.session)
        response = session.get(url, auth=_StaticTokenAuth(token), **kwargs)

        # Handle 401: token may have been invalidated server-side
        if response.status_code == 401:
            logger.warning(f"Got 401 for installation {installation_id}, refreshing token")
            self.auth.invalidate_installation_token(installation_id)
            token = self.auth.get_installation_token_threadsafe(installation_id, self.session)
            response = session.get(url, auth=_StaticTokenAuth(token), **kwargs)

        if response.status_code in (403, 429):
            retry_after = response.headers.get("retry-after")
            remaining = response.headers.get("x-ratelimit-remaining")
            reset_ts = response.headers.get("x-ratelimit-reset")

            if retry_after:
                wait_secs = int(retry_after)
                reset_at = time.time() + wait_secs
            elif remaining == "0" and reset_ts:
                reset_at = int(reset_ts) + 1
                wait_secs = max(reset_at - time.time(), 1)
            else:
                wait_secs = 60
                reset_at = time.time() + wait_secs

            # Record the reset time so other threads wait instead of sending doomed requests
            self._set_rate_limit_reset(installation_id, reset_at)

            logger.warning(
                f"Rate limited (HTTP {response.status_code}) — sleeping {wait_secs:.0f}s (installation {installation_id})"
            )
            time.sleep(wait_secs)

            # Token may have expired during the sleep — refresh before retry
            token = self.auth.get_installation_token_threadsafe(installation_id, self.session)
            response = session.get(url, auth=_StaticTokenAuth(token), **kwargs)
            if response.status_code in (403, 429):
                raise RateLimitError(wait_secs)
        return response

    def _graphql_post(self, query: str, variables: dict, installation_id: int) -> dict:
        """Thread-safe GraphQL POST through the shared transport.

        Adds the two GitHub-specific concerns the transport can't own: a
        per-request installation token, refreshed once on a 401 (which
        `_classify_github_graphql` deliberately leaves unclaimed so it lands
        here), and the GraphQL point budget GitHub reports inline.
        """
        try:
            result = self._graphql_request(query, variables, installation_id)
        except HTTPError as exc:
            if exc.response is None or exc.response.status_code != 401:
                raise
            logger.warning(f"GraphQL got 401 for installation {installation_id}, refreshing token")
            self.auth.invalidate_installation_token(installation_id)
            result = self._graphql_request(query, variables, installation_id)

        self._sleep_if_graphql_budget_low(result)
        return result

    def _graphql_request(self, query: str, variables: dict, installation_id: int) -> dict:
        # Attempt counts and waits: a short bounded retry for TransientGraphQLError
        # (retrying the same oversized page is near-futile -- the real remedy is the
        # adaptive page-size shrink), and a
        # long one shared by rate limits and network errors, including the
        # adapter's exhausted RetryError.
        #
        # auth_factory, not auth: minting the installation token is itself an HTTP
        # call, so it has to happen inside the retry ladder or a network blip on
        # /access_tokens fails the whole stream. It also means the 401 path above
        # only has to invalidate the cache -- the next attempt re-mints.
        #
        # The POST goes through self._graphql_session, which returns a persistent
        # 5xx response instead of raising RetryError. The classifier treats it as
        # TRANSIENT, so after 3 attempts the adaptive caller shrinks the page
        # (about 3 minutes per page size). The token is
        # still minted on self.session.
        return graphql_post(
            self._graphql_session,
            GRAPHQL_URL,
            query,
            variables,
            classify=_classify_github_graphql,
            rate_limit_wait_hint=_github_rate_limit_hint,
            retry_exceptions=TRANSIENT_NETWORK_ERRORS,
            transient_attempts=3,
            transient_wait=(15, 60),
            rate_limit_attempts=6,
            rate_limit_wait=(60, 900),
            auth_factory=lambda: _StaticTokenAuth(
                self.auth.get_installation_token_threadsafe(installation_id, self.session)
            ),
        )

    def _sleep_if_graphql_budget_low(self, result: dict) -> None:
        """GraphQL has its own 5000 points/hr pool, reported inline rather than in
        headers; sleep to the reset instead of spending the last of it."""
        rate_limit = result.get("data", {}).get("rateLimit", {})
        if not rate_limit:
            return
        remaining = rate_limit.get("remaining", 5000)
        logger.debug(
            f"GraphQL rate limit: cost={rate_limit.get('cost', 0)}, remaining={remaining}, "
            f"resetAt={rate_limit.get('resetAt', '')}"
        )
        if remaining < 50:
            reset_dt = self._parse_gh_timestamp(rate_limit.get("resetAt", ""))
            wait_secs = max(reset_dt.timestamp() - time.time() + 1, 1)
            logger.warning(f"GraphQL rate limit nearly exhausted ({remaining} remaining), sleeping {wait_secs:.0f}s")
            time.sleep(wait_secs)

    def _graphql_post_adaptive(
        self,
        query: str,
        variables: dict,
        installation_id: int,
        *,
        start_size: int,
        min_size: int = 1,
        repo: str = "",
        cursor_probe_query: Optional[str] = None,
        max_size: Optional[int] = None,
    ) -> Tuple[dict, int]:
        """Fetch one page, shrinking `first` on a persistent TransientGraphQLError
        and retrying the same cursor.

        `cursor_probe_query` must be the node-free sibling of `query`
        (PR_PAGE_CURSOR_PROBE_QUERY). Omitting it -- or
        `max_phantom_skips_per_page == 0` -- keeps the pure fail-closed behaviour:
        the caller wraps the raised error in RepoFetchError and the watermark does
        not advance past the un-fetched window. `max_size` is the stream's
        configured page size, restored after a phantom recovery.
        """
        probe = None
        if cursor_probe_query is not None:
            probe = PhantomProbe(
                query=cursor_probe_query,
                cursor_var="after",
                max_skips_per_page=self.config.max_phantom_skips_per_page,
                max_skips_per_run=self.config.max_phantom_skips_per_run,
                run_counter=self._phantom_skips,
                page_info=_pr_page_info,
                describe_cursor=_describe_relay_cursor,
            )
        return graphql_post_adaptive(
            self.session,
            GRAPHQL_URL,
            query,
            variables,
            page_size_var="first",
            start_size=start_size,
            floor=min_size,
            max_size=max_size,
            probe=probe,
            label=repo or "repo",
            post=partial(self._graphql_post, installation_id=installation_id),
        )

    def get_authenticator(self) -> AuthBase:
        return self.auth

    @staticmethod
    def streams() -> List[str]:
        return [stream.value for stream in GitHubStreams]

    @staticmethod
    def get_config_class() -> type:
        return GitHubSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self.auth.use_jwt_mode()
            response = self.session.get(f"{BASE_URL}/app")
            response.raise_for_status()
            app_data = response.json()
            logger.info(f"Connected to GitHub App: {app_data.get('name')}")
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def _get_installations(self) -> List[dict]:
        """Get all installations for this GitHub App."""
        self.auth.use_jwt_mode()
        installations = []
        page = 1
        while True:
            response = self.session.get(
                f"{BASE_URL}/app/installations",
                params={"per_page": self.config.per_page, "page": page},
            )
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            installations.extend(data)
            if len(data) < self.config.per_page:
                break
            page += 1
        logger.info(f"Found {len(installations)} installations")
        return installations

    def _get_installation_token(self, installation_id: int) -> str:
        """Exchange JWT for an installation access token. Caches and auto-refreshes.

        Used by sequential (main-thread) code paths. Lock-protected for safety
        since the cache is shared with the threadsafe path.
        """
        with self.auth._token_lock:
            cached = self.auth._installation_tokens.get(installation_id)
            if cached:
                token, expires_at = cached
                # Refresh at 50-minute mark (tokens expire at 60 min)
                if time.time() < expires_at - 600:
                    self.auth.set_installation(installation_id, token, expires_at)
                    return token

        self.auth.use_jwt_mode()
        response = self.session.post(f"{BASE_URL}/app/installations/{installation_id}/access_tokens")
        response.raise_for_status()
        data = response.json()
        token = data["token"]
        expires_at_str = data["expires_at"]
        expires_at = datetime.fromisoformat(expires_at_str.replace("Z", "+00:00")).timestamp()
        with self.auth._token_lock:
            self.auth.set_installation(installation_id, token, expires_at)
        logger.info(f"Obtained access token for installation {installation_id}")
        return token

    def _switch_to_installation(self, installation_id: int):
        """Ensure we're using the token for the given installation."""
        self._get_installation_token(installation_id)

    def _get_repos_for_installation(self, installation_id: int) -> List[dict]:
        """Get all repos accessible to an installation.

        Returns list of {"name": "owner/repo", "default_branch": "main"}.
        """
        self._switch_to_installation(installation_id)
        repos = []
        page = 1
        while True:
            response = self._api_get(
                f"{BASE_URL}/installation/repositories",
                params={"per_page": self.config.per_page, "page": page},
            )
            response.raise_for_status()
            data = response.json()
            for repo in data.get("repositories", []):
                repos.append({"name": repo["full_name"], "default_branch": repo.get("default_branch", "main")})
            if len(data.get("repositories", [])) < self.config.per_page:
                break
            page += 1
        return repos

    def _discover_all_repos(self) -> List[dict]:
        """Discover all repos across all installations.

        Returns list of {"repo": "owner/repo", "installation_id": 123, "default_branch": "main"}.
        """
        if self.config.repos:
            installations = self._get_installations()
            repo_map = []
            matched = set()
            for inst in installations:
                inst_id = inst["id"]
                inst_repos = self._get_repos_for_installation(inst_id)
                for repo_info in inst_repos:
                    if repo_info["name"] in self.config.repos and repo_info["name"] not in matched:
                        repo_map.append(
                            {
                                "repo": repo_info["name"],
                                "installation_id": inst_id,
                                "default_branch": repo_info["default_branch"],
                            }
                        )
                        matched.add(repo_info["name"])
            return repo_map

        installations = self._get_installations()
        repo_map = []
        for inst in installations:
            inst_id = inst["id"]
            inst_repos = self._get_repos_for_installation(inst_id)
            for repo_info in inst_repos:
                repo_map.append(
                    {
                        "repo": repo_info["name"],
                        "installation_id": inst_id,
                        "default_branch": repo_info["default_branch"],
                    }
                )
        return repo_map

    def get_repositories(self, pagination: dict) -> SourceIteration:
        """Fetch all repositories across all installations."""
        if not pagination:
            # First call: discover installations
            installations = self._get_installations()
            if not installations:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_installations": [i["id"] for i in installations[1:]],
                "current_installation_id": installations[0]["id"],
                "page": 1,
            }

        installation_id = pagination["current_installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_installations", [])

        self._switch_to_installation(installation_id)

        response = self._api_get(
            f"{BASE_URL}/installation/repositories",
            params={"per_page": self.config.per_page, "page": page},
        )
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=str(repo["id"]), data={**repo, "_installation_id": installation_id})
            for repo in data.get("repositories", [])
        ]
        logger.info(f"Fetched {len(records)} repositories from installation {installation_id} (page {page})")

        # Check if more pages for this installation
        if len(data.get("repositories", [])) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_installations": remaining,
                    "current_installation_id": installation_id,
                    "page": page + 1,
                },
            )

        # Move to next installation
        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_installations": remaining[1:],
                    "current_installation_id": remaining[0],
                    "page": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def get_members(self, pagination: dict) -> SourceIteration:
        """Fetch org members for each installation's org account."""
        if not pagination:
            installations = self._get_installations()
            if not installations:
                return SourceIteration(records=[], next_pagination={})
            # Each installation is on an org (or user account)
            orgs = []
            for inst in installations:
                account = inst.get("account", {})
                if account.get("type") == "Organization":
                    orgs.append({"org": account["login"], "installation_id": inst["id"]})
            if not orgs:
                logger.warning("No organization installations found for members stream")
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_orgs": orgs[1:],
                "current_org": orgs[0],
                "page": 1,
            }

        current = pagination["current_org"]
        org_name = current["org"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_orgs", [])

        self._switch_to_installation(installation_id)

        response = self._api_get(
            f"{BASE_URL}/orgs/{org_name}/members",
            params={"per_page": self.config.per_page, "page": page},
        )
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=str(member["id"]), data={**member, "_org": org_name, "_installation_id": installation_id})
            for member in data
        ]
        logger.info(f"Fetched {len(records)} members from {org_name} (page {page})")

        if len(data) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining,
                    "current_org": current,
                    "page": page + 1,
                },
            )

        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining[1:],
                    "current_org": remaining[0],
                    "page": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def get_organization_teams(self, pagination: dict) -> SourceIteration:
        """Fetch teams for each installation's org account."""
        if not pagination:
            installations = self._get_installations()
            if not installations:
                return SourceIteration(records=[], next_pagination={})
            orgs = []
            for inst in installations:
                account = inst.get("account", {})
                if account.get("type") == "Organization":
                    orgs.append({"org": account["login"], "installation_id": inst["id"]})
            if not orgs:
                logger.warning("No organization installations found for organization_teams stream")
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_orgs": orgs[1:],
                "current_org": orgs[0],
                "page": 1,
            }

        current = pagination["current_org"]
        org_name = current["org"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_orgs", [])

        self._switch_to_installation(installation_id)

        response = self._api_get(
            f"{BASE_URL}/orgs/{org_name}/teams",
            params={"per_page": self.config.per_page, "page": page},
        )
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(id=str(team["id"]), data={**team, "_org": org_name, "_installation_id": installation_id})
            for team in data
        ]
        logger.info(f"Fetched {len(records)} teams from {org_name} (page {page})")

        if len(data) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining,
                    "current_org": current,
                    "page": page + 1,
                },
            )

        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining[1:],
                    "current_org": remaining[0],
                    "page": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _fetch_all_teams_for_org(self, org_name: str, installation_id: int) -> List[dict]:
        """Fetch all teams for an org (handles pagination internally)."""
        self._switch_to_installation(installation_id)
        teams = []
        page = 1
        while True:
            response = self._api_get(
                f"{BASE_URL}/orgs/{org_name}/teams",
                params={"per_page": self.config.per_page, "page": page},
            )
            response.raise_for_status()
            data = response.json()
            teams.extend(data)
            if len(data) < self.config.per_page:
                break
            page += 1
        return teams

    def get_organization_team_members(self, pagination: dict) -> SourceIteration:
        """Fetch members for each team in each org installation.

        Pagination state tracks three levels: orgs → teams → members per team.
        """
        if not pagination:
            installations = self._get_installations()
            if not installations:
                return SourceIteration(records=[], next_pagination={})
            orgs = []
            for inst in installations:
                account = inst.get("account", {})
                if account.get("type") == "Organization":
                    orgs.append({"org": account["login"], "installation_id": inst["id"]})
            if not orgs:
                logger.warning("No organization installations found for organization_team_members stream")
                return SourceIteration(records=[], next_pagination={})

            # Fetch all teams for the first org to bootstrap
            first_org = orgs[0]
            teams = self._fetch_all_teams_for_org(first_org["org"], first_org["installation_id"])
            if not teams and len(orgs) > 1:
                # Skip orgs with no teams
                for i, org in enumerate(orgs[1:], 1):
                    teams = self._fetch_all_teams_for_org(org["org"], org["installation_id"])
                    if teams:
                        first_org = org
                        orgs = orgs[i:]
                        break
            if not teams:
                return SourceIteration(records=[], next_pagination={})

            team_dicts = [{"slug": t["slug"], "id": t["id"]} for t in teams]
            pagination = {
                "remaining_orgs": orgs[1:],
                "current_org": first_org,
                "remaining_teams": team_dicts[1:],
                "current_team": team_dicts[0],
                "page": 1,
            }

        current_org = pagination["current_org"]
        org_name = current_org["org"]
        installation_id = current_org["installation_id"]
        current_team = pagination["current_team"]
        team_slug = current_team["slug"]
        team_id = current_team["id"]
        page = pagination.get("page", 1)
        remaining_teams = pagination.get("remaining_teams", [])
        remaining_orgs = pagination.get("remaining_orgs", [])

        self._switch_to_installation(installation_id)

        response = self._api_get(
            f"{BASE_URL}/orgs/{org_name}/teams/{team_slug}/members",
            params={"per_page": self.config.per_page, "page": page},
        )
        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(
                id=f"{team_id}_{member['id']}",
                data={
                    **member,
                    "_org": org_name,
                    "_installation_id": installation_id,
                    "_team_slug": team_slug,
                    "_team_id": team_id,
                },
            )
            for member in data
        ]
        logger.info(f"Fetched {len(records)} members from team {org_name}/{team_slug} (page {page})")

        # More pages for current team
        if len(data) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    **pagination,
                    "page": page + 1,
                },
            )

        # Move to next team in current org
        if remaining_teams:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining_orgs,
                    "current_org": current_org,
                    "remaining_teams": remaining_teams[1:],
                    "current_team": remaining_teams[0],
                    "page": 1,
                },
            )

        # Move to next org
        if remaining_orgs:
            next_org = remaining_orgs[0]
            teams = self._fetch_all_teams_for_org(next_org["org"], next_org["installation_id"])
            if not teams:
                return SourceIteration(records=records, next_pagination={})
            team_dicts = [{"slug": t["slug"], "id": t["id"]} for t in teams]
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_orgs": remaining_orgs[1:],
                    "current_org": next_org,
                    "remaining_teams": team_dicts[1:],
                    "current_team": team_dicts[0],
                    "page": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _fetch_paginated_for_repos(
        self,
        pagination: dict,
        endpoint_template: str,
        id_field: str = "id",
        params: Optional[dict] = None,
    ) -> SourceIteration:
        """Generic helper for streams that paginate across multiple repos.

        Args:
            pagination: Pagination state dict.
            endpoint_template: URL template with {repo} placeholder (e.g. "/repos/{repo}/pulls").
            id_field: Field to use as record ID (default "id").
            params: Extra query params to include.
        """
        extra_params = params or {}

        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_repos": repo_map[1:],
                "current_repo": repo_map[0],
                "page": 1,
                "repos_loaded": 1,
            }

        current = pagination["current_repo"]
        repo_name = current["repo"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_repos", [])

        self._switch_to_installation(installation_id)

        url = f"{BASE_URL}{endpoint_template.format(repo=repo_name)}"
        response = self._api_get(
            url,
            params={"per_page": self.config.per_page, "page": page, **extra_params},
        )

        # 409 = empty repo (no commits), 404 = repo not found — skip to next
        if response.status_code in (404, 409):
            logger.warning(f"Skipping {repo_name} (HTTP {response.status_code}), advancing to next repo")
            if remaining:
                return SourceIteration(
                    records=[],
                    next_pagination={
                        "remaining_repos": remaining[1:],
                        "current_repo": remaining[0],
                        "page": 1,
                        "repos_loaded": 1,
                    },
                )
            return SourceIteration(records=[], next_pagination={})

        response.raise_for_status()
        data = response.json()

        records = [
            SourceRecord(
                id=str(item[id_field]),
                data={**item, "_repository": repo_name, "_installation_id": installation_id},
            )
            for item in data
        ]
        logger.info(f"Fetched {len(records)} records from {repo_name} (page {page})")

        # More pages for this repo?
        if len(data) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining,
                    "current_repo": current,
                    "page": page + 1,
                    "repos_loaded": 1,
                },
            )

        # Next repo
        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining[1:],
                    "current_repo": remaining[0],
                    "page": 1,
                    "repos_loaded": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _get_blocked_extensions(self) -> frozenset:
        if self.config.excluded_extensions is not None:
            return frozenset(self.config.excluded_extensions)
        return DEFAULT_BINARY_EXTENSIONS

    @staticmethod
    def _should_include_file(path: str, size: int, blocked_exts: frozenset, max_file_size: int) -> bool:
        if max_file_size and size > max_file_size:
            return False
        if any(path.endswith(suffix) for suffix in SKIP_PATH_SUFFIXES):
            return False
        ext = path.rsplit(".", 1)[-1].lower() if "." in path else ""
        if ext in blocked_exts:
            return False
        return True

    def _fetch_tree(self, repo_name: str, branch: str) -> Tuple[List[dict], bool]:
        """Fetch the recursive git tree, returning (filtered_entries, truncated)."""
        response = self._api_get(
            f"{BASE_URL}/repos/{repo_name}/git/trees/{branch}",
            params={"recursive": "1"},
        )
        if response.status_code == 404 or response.status_code == 409:
            logger.warning(f"Tree fetch failed for {repo_name}:{branch} (HTTP {response.status_code}), skipping repo")
            return [], 0
        response.raise_for_status()
        data = response.json()
        truncated = int(data.get("truncated", False))
        if truncated:
            logger.warning(f"Tree for {repo_name}:{branch} was truncated (>100k entries), processing partial results")

        blocked_exts = self._get_blocked_extensions()
        max_size = self.config.max_file_size
        entries = [
            {"path": item["path"], "sha": item["sha"], "size": item.get("size", 0)}
            for item in data.get("tree", [])
            if item["type"] == "blob"
            and self._should_include_file(item["path"], item.get("size", 0), blocked_exts, max_size)
        ]
        logger.info(f"Tree for {repo_name}: {len(entries)} text files out of {len(data.get('tree', []))} total entries")
        return entries, truncated

    def _fetch_blob_content(self, repo_name: str, sha: str, installation_id: Optional[int] = None) -> Optional[str]:
        """Fetch a blob and return decoded text content, or None on failure.

        Args:
            installation_id: If provided, uses thread-safe per-request auth.
                           If None, uses session-level auth (sequential path).
        """
        url = f"{BASE_URL}/repos/{repo_name}/git/blobs/{sha}"
        if installation_id is not None:
            response = self._api_get_with_token(url, installation_id)
        else:
            response = self._api_get(url)
        if response.status_code in (404, 422):
            logger.warning(f"Blob fetch failed for {repo_name} sha={sha[:8]} (HTTP {response.status_code}), skipping")
            return None
        response.raise_for_status()
        data = response.json()
        try:
            raw = base64.b64decode(data.get("content", ""))
            return raw.decode("utf-8")
        except (UnicodeDecodeError, ValueError):
            logger.warning(f"Binary content detected for {repo_name} sha={sha[:8]}, skipping")
            return None

    def _cleanup_tarball_dir(self):
        tarball_dir = getattr(self, "_tarball_dir", None)
        if tarball_dir:
            shutil.rmtree(tarball_dir, ignore_errors=True)
        self._tarball_dir = None
        self._tarball_repo = None
        self._tarball_missing = set()

    @staticmethod
    def _sweep_stale_tarball_temp_files():
        """A retried run on the same host finds a killed attempt's temp dir/file still on disk.
        Sweep at the start of a fresh run (empty pagination) rather than relying on per-repo
        cleanup, which never runs for an attempt that never got that far."""
        tmp_dir = tempfile.gettempdir()
        for pattern in ("gh_repo_content_*", "gh_tarball_*"):
            for path in glob.glob(os.path.join(tmp_dir, pattern)):
                if os.path.isdir(path):
                    shutil.rmtree(path, ignore_errors=True)
                else:
                    try:
                        os.remove(path)
                    except OSError:
                        pass

    def _fetch_repo_tarball(
        self, repo_name: str, branch: str, installation_id: int, wanted_paths: Set[str]
    ) -> Optional[Tuple[str, Set[str]]]:
        """Download the repo's default-branch tarball (1 call) and extract only the
        tree-filtered paths into a fresh per-repo temp dir.

        Returns (extract_dir, missing_paths) where missing_paths is the subset of
        wanted_paths not found in the tarball (LFS pointer / submodule / etc — callers
        fall back to a per-blob fetch for those), or None if the download/extraction
        failed entirely, in which case the caller falls back to per-blob for the repo.
        """
        url = f"{BASE_URL}/repos/{repo_name}/tarball/{branch}"
        tar_fd, tar_path = tempfile.mkstemp(prefix="gh_tarball_", suffix=".tar.gz")
        os.close(tar_fd)
        extract_dir = tempfile.mkdtemp(prefix="gh_repo_content_")
        response = None
        try:
            # This redirects to a pre-signed codeload.github.com URL (token in the
            # query string); requests correctly strips our Authorization header on
            # the cross-host hop, and the pre-signed URL carries access on its own --
            # the auth-less follow-up request is expected, not a leak.
            response = self._api_get_with_token(url, installation_id, stream=True, timeout=(10, 60))
            if response.status_code in (404, 409, 422):
                logger.warning(
                    f"Tarball fetch failed for {repo_name}:{branch} (HTTP {response.status_code}), falling back to per-blob"
                )
                return None
            response.raise_for_status()
            with open(tar_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    fh.write(chunk)

            missing = set(wanted_paths)
            with tarfile.open(tar_path, mode="r:gz") as tar:
                for member in tar:
                    # Regular files only -- isfile() already excludes symlinks/hardlinks/dirs.
                    if not member.isfile():
                        continue
                    # Tarball root is a single "<owner>-<repo>-<sha>/" component; strip it.
                    parts = member.name.split("/", 1)
                    if len(parts) != 2:
                        continue
                    relpath = posixpath.normpath(parts[1])
                    # Guard against path traversal in a maliciously/oddly crafted tar entry.
                    if relpath.startswith("..") or relpath.startswith("/") or relpath not in wanted_paths:
                        continue
                    src = tar.extractfile(member)
                    if src is None:
                        continue
                    dest = os.path.join(extract_dir, relpath)
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    with open(dest, "wb") as out:
                        shutil.copyfileobj(src, out)
                    missing.discard(relpath)
            return extract_dir, missing
        except Exception as exc:
            logger.warning(f"Tarball fetch/extract failed for {repo_name}:{branch}: {exc}, falling back to per-blob")
            shutil.rmtree(extract_dir, ignore_errors=True)
            return None
        finally:
            if response is not None:
                response.close()
            try:
                os.remove(tar_path)
            except OSError:
                pass

    def _refresh_tarball_state(self, repo_name: str, branch: str, installation_id: int, wanted_paths: Set[str]):
        """Ensure `self._tarball_dir` holds a live extraction for `repo_name` covering
        `wanted_paths`, (re-)downloading if it's missing -- including a dir that vanished
        mid-repo (e.g. node disk pressure), which would otherwise crash the next disk read.
        """
        if self._tarball_repo == repo_name:
            if self._tarball_dir is None:
                return  # already attempted and failed for this repo this run -- don't hammer it every batch
            if os.path.isdir(self._tarball_dir):
                return
            logger.warning(f"Extracted tarball dir for {repo_name} vanished mid-repo, re-downloading")
        result = self._fetch_repo_tarball(repo_name, branch, installation_id, wanted_paths)
        self._tarball_repo = repo_name
        if result is None:
            self._tarball_dir = None
            self._tarball_missing = set()
        else:
            self._tarball_dir, self._tarball_missing = result

    def _read_entry_content(self, repo_name: str, entry: dict, installation_id: int) -> Optional[str]:
        """Read one tree entry's content from the extracted tarball, falling back to the
        per-blob API for anything the tarball didn't have."""
        if self._tarball_dir and self._tarball_repo == repo_name and entry["path"] not in self._tarball_missing:
            try:
                with open(os.path.join(self._tarball_dir, entry["path"]), "rb") as fh:
                    return fh.read().decode("utf-8")
            except FileNotFoundError:
                pass  # not actually extracted -- fall through to per-blob
            except UnicodeDecodeError:
                logger.warning(f"Binary content detected for {repo_name} path={entry['path']}, skipping")
                return None
        return self._fetch_blob_content(repo_name, entry["sha"], installation_id=installation_id)

    def get_repo_content(self, pagination: dict) -> SourceIteration:
        """Fetch text file contents from all repos using Git Trees + Blobs APIs."""
        if not pagination:
            self._sweep_stale_tarball_temp_files()
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_repos": repo_map[1:],
                "current_repo": repo_map[0],
                "tree_entries": [],
                "tree_truncated": 0,
                "repos_loaded": 1,
            }

        current = pagination["current_repo"]
        repo_name = current["repo"]
        installation_id = current["installation_id"]
        default_branch = current.get("default_branch", "main")
        remaining = pagination.get("remaining_repos", [])
        tree_entries = pagination.get("tree_entries", [])
        tree_truncated = pagination.get("tree_truncated", 0)

        self._switch_to_installation(installation_id)

        # If no tree entries buffered, fetch the tree for the current repo
        if not tree_entries:
            tree_entries, tree_truncated = self._fetch_tree(repo_name, default_branch)
            if not tree_entries:
                # Empty or failed tree — advance to next repo
                self._cleanup_tarball_dir()
                if remaining:
                    return SourceIteration(
                        records=[],
                        next_pagination={
                            "remaining_repos": remaining[1:],
                            "current_repo": remaining[0],
                            "tree_entries": [],
                            "tree_truncated": 0,
                            "repos_loaded": 1,
                        },
                    )
                return SourceIteration(records=[], next_pagination={})

        # One tarball call covers the whole repo (vs. one REST call per file, which can
        # exhaust the installation's hourly budget on a large repo). Re-checked every
        # call so a dir that vanished mid-repo gets re-downloaded, not crashed on.
        self._refresh_tarball_state(repo_name, default_branch, installation_id, {e["path"] for e in tree_entries})

        # Pop a batch of entries
        batch_size = self.config.blobs_per_batch
        batch = tree_entries[:batch_size]
        remaining_entries = tree_entries[batch_size:]

        # Scale workers proportionally to repo size: more files → more parallelism
        # e.g. 50 files → 3 workers, 5000 → 10, 11000+ → 20 (capped)
        effective_workers = min(20, max(3, len(tree_entries) // 500))

        # Fetch blobs concurrently (thread-safe auth via installation_id)
        def _fetch_entry(entry):
            content = self._read_entry_content(repo_name, entry, installation_id)
            return entry, content

        records = []
        try:
            with ThreadPoolExecutor(max_workers=effective_workers) as pool:
                futures = {pool.submit(_fetch_entry, e): e for e in batch}
                for future in as_completed(futures):
                    entry, content = future.result()
                    if content is None:
                        continue
                    records.append(
                        SourceRecord(
                            id=f"{repo_name}:{entry['path']}",
                            data={
                                "repo": repo_name,
                                "installation_id": installation_id,
                                "path": entry["path"],
                                "content": content,
                                "sha": entry["sha"],
                                "size": entry["size"],
                                "default_branch": default_branch,
                                "tree_truncated": tree_truncated,
                            },
                        )
                    )
        except Exception:
            # A genuine failure here (not the expected per-entry None/fallback path)
            # aborts the batch -- don't leave this repo's extracted dir behind for a
            # retried run to trip over; the startup sweep is the other net.
            self._cleanup_tarball_dir()
            raise
        logger.info(
            f"Fetched {len(records)} file contents from {repo_name} ({len(remaining_entries)} entries remaining, {effective_workers} workers)"
        )

        # More entries for this repo?
        if remaining_entries:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining,
                    "current_repo": current,
                    "tree_entries": remaining_entries,
                    "tree_truncated": tree_truncated,
                    "repos_loaded": 1,
                },
            )

        # Next repo
        self._cleanup_tarball_dir()
        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining[1:],
                    "current_repo": remaining[0],
                    "tree_entries": [],
                    "tree_truncated": 0,
                    "repos_loaded": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _transform_graphql_pr_to_rest(node: dict) -> dict:
        """Convert a GraphQL PR node to REST-compatible dict for BigQuery compatibility."""

        def _reshape_user(graphql_user: Optional[dict]) -> Optional[dict]:
            if not graphql_user:
                return None
            return {
                "login": graphql_user.get("login"),
                "id": graphql_user.get("databaseId"),
                "avatar_url": graphql_user.get("avatarUrl"),
            }

        def _map_mergeable(value: Optional[str]) -> Optional[bool]:
            if value == "MERGEABLE":
                return True
            if value == "CONFLICTING":
                return False
            return None  # UNKNOWN or null

        state = node.get("state", "").upper()
        # GraphQL uses OPEN/CLOSED/MERGED; REST uses open/closed (merged is a separate field)
        rest_state = "closed" if state in ("CLOSED", "MERGED") else "open"

        return {
            "id": node.get("databaseId"),
            "number": node.get("number"),
            "title": node.get("title"),
            "state": rest_state,
            "body": node.get("body"),
            "url": node.get("url"),
            "html_url": node.get("url"),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
            "closed_at": node.get("closedAt"),
            "merged_at": node.get("mergedAt"),
            "merge_commit_sha": (node.get("mergeCommit") or {}).get("oid"),
            "merged": node.get("merged", False),
            "draft": node.get("isDraft", False),
            "additions": node.get("additions"),
            "deletions": node.get("deletions"),
            "changed_files": node.get("changedFiles"),
            "mergeable": _map_mergeable(node.get("mergeable")),
            "user": _reshape_user(node.get("author")),
            "merged_by": _reshape_user(node.get("mergedBy")),
            # REST shape ([{login, id, avatar_url}]).
            "assignees": [_reshape_user(a) for a in (node.get("assignees") or {}).get("nodes") or []],
            "head": {
                "ref": node.get("headRefName"),
                "sha": node.get("headRefOid"),
                "repo": {"full_name": (node.get("headRepository") or {}).get("nameWithOwner")},
            },
            "base": {
                "ref": node.get("baseRefName"),
                "sha": node.get("baseRefOid"),
                "repo": {"full_name": (node.get("baseRepository") or {}).get("nameWithOwner")},
            },
            # No "labels" key: they are not fetched here. PR labels can be rebuilt from the
            # pull_request_timeline stream's Labeled/Unlabeled events.
            "commits": (node.get("commits") or {}).get("totalCount"),
            "comments": (node.get("comments") or {}).get("totalCount"),
            "review_comments": None,
            "maintainer_can_modify": None,
            "rebaseable": None,
        }

    def _fetch_pull_requests_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PRs for a single repo using GraphQL (sequential pagination)."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        # Smaller page size for GraphQL to keep response payloads manageable
        # (PR bodies can be very large; 100 PRs can produce multi-MB responses)
        initial_page_size = 25
        page_size = initial_page_size

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            # Throttle to avoid GitHub secondary rate limits (abuse detection)
            if page > 0:
                time.sleep(1)

            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"pull requests (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found or inaccessible via GraphQL, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for node in nodes:
                # Check start_date cutoff (results ordered by UPDATED_AT DESC)
                if self._start_date and node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_data = self._transform_graphql_pr_to_rest(node)
                pr_data["_repository"] = repo_name
                pr_data["_installation_id"] = installation_id
                records.append(SourceRecord(id=str(pr_data["id"]), data=pr_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name}, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} pull requests from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_requests(self, pagination: dict) -> SourceIteration:
        """Fetch pull requests using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        # Process one repo at a time (GraphQL budget is ~49 queries/hr)
        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_pull_requests_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    def get_commits(self, pagination: dict) -> SourceIteration:
        params = {}
        if self._start_date:
            params["since"] = self._start_date.isoformat()
        return self._fetch_repos_concurrent(
            pagination=pagination,
            endpoint_template="/repos/{repo}/commits",
            id_field="sha",
            params=params,
        )

    @staticmethod
    def _transform_graphql_deployment_to_rest(node: dict) -> dict:
        """Convert a GraphQL deployment node to REST-compatible dict for BigQuery compatibility."""

        def _reshape_user(graphql_user: Optional[dict]) -> Optional[dict]:
            if not graphql_user:
                return None
            return {
                "login": graphql_user.get("login"),
                "id": graphql_user.get("databaseId"),
                "avatar_url": graphql_user.get("avatarUrl"),
            }

        def _reshape_latest_status(status_node: Optional[dict]) -> Optional[dict]:
            if not status_node:
                return None
            return {
                "state": (status_node.get("state") or "").lower(),
                "description": status_node.get("description"),
                "created_at": status_node.get("createdAt"),
                "updated_at": status_node.get("updatedAt"),
                "log_url": status_node.get("logUrl"),
                "environment_url": status_node.get("environmentUrl"),
                "creator": _reshape_user(status_node.get("creator")),
            }

        # payload: GraphQL returns a string, REST can return an object
        raw_payload = node.get("payload")
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except (ValueError, TypeError):
                pass  # keep as string

        return {
            "id": node.get("databaseId"),
            "environment": node.get("environment"),
            "original_environment": node.get("originalEnvironment"),
            "description": node.get("description"),
            "task": node.get("task"),
            "state": (node.get("state") or "").lower(),
            "created_at": node.get("createdAt"),
            "updated_at": node.get("updatedAt"),
            "sha": node.get("commitOid") or (node.get("commit") or {}).get("oid"),
            "ref": (node.get("ref") or {}).get("name"),
            "creator": _reshape_user(node.get("creator")),
            "payload": raw_payload,
            "latest_status": _reshape_latest_status(node.get("latestStatus")),
            # REST-only fields not available via GraphQL
            "url": None,
            "node_id": None,
            "statuses_url": None,
            "repository_url": None,
        }

    def _fetch_deployments_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all deployments for a single repo using GraphQL (sequential pagination)."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        page_size = 100  # deployments are lightweight, unlike PR bodies

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            try:
                # No cursor_probe_query here (deliberately): the phantom-index defect has only
                # been seen on the pullRequests(orderBy: UPDATED_AT) connection. The
                # deployments connection is a different connection with a different
                # orderBy and no evidence it's affected -- extending the skip fix here would
                # need its own probe query, not this one, so it's out of scope for now.
                result, page_size = self._graphql_post_adaptive(
                    DEPLOYMENTS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"deployments (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for deployments, skipping")
                break

            deploy_connection = repo_data["deployments"]
            nodes = deploy_connection.get("nodes", [])
            page_info = deploy_connection.get("pageInfo", {})

            hit_cutoff = False
            for node in nodes:
                # Check start_date cutoff (results ordered by CREATED_AT DESC)
                if self._start_date and node.get("createdAt"):
                    created = self._parse_gh_timestamp(node["createdAt"])
                    if created < self._start_date:
                        hit_cutoff = True
                        continue

                deploy_data = self._transform_graphql_deployment_to_rest(node)
                deploy_data["_repository"] = repo_name
                deploy_data["_installation_id"] = installation_id
                records.append(SourceRecord(id=str(deploy_data["id"]), data=deploy_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} deployments, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} deployments from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_deployments(self, pagination: dict) -> SourceIteration:
        """Fetch deployments using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_deployments_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    def get_workflow_runs(self, pagination: dict) -> SourceIteration:
        """Fetch workflow runs across repos concurrently."""
        params = {}
        if self._start_date:
            params["created"] = f">={self._start_date.strftime('%Y-%m-%dT%H:%M:%SZ')}"
        return self._fetch_repos_concurrent(
            pagination=pagination,
            endpoint_template="/repos/{repo}/actions/runs",
            params=params,
            response_key="workflow_runs",
        )

    def get_workflow_jobs(self, pagination: dict) -> SourceIteration:
        """Fetch workflow jobs (full refresh).

        Jobs are a NESTED resource under runs -- there is no repo-wide jobs list.
        Lists workflow runs per repo/page (same `created>=start_date` filter as
        workflow_runs, when a start_date is configured), then resolves each run's
        jobs via `_fetch_jobs_for_runs`. Uses the sequential single-page-per-repo
        cursor (see `_get_workflow_jobs_paginated`) rather than workflow_runs' full
        concurrent-repo-pagination model, so memory stays bounded to one page of
        runs (and their jobs) at a time.
        """
        return self._get_workflow_jobs_paginated(pagination, since=self._start_date)

    @staticmethod
    def _transform_graphql_review_to_rest(review_node: dict) -> dict:
        """Convert a GraphQL review node to REST-compatible dict."""
        author = review_node.get("author")
        user = None
        if author:
            user = {
                "login": author.get("login"),
                "id": author.get("databaseId"),
                "avatar_url": author.get("avatarUrl"),
            }
        return {
            "id": review_node.get("databaseId"),
            "state": review_node.get("state"),
            "body": review_node.get("body"),
            "submitted_at": review_node.get("submittedAt"),
            "created_at": review_node.get("createdAt"),
            "updated_at": review_node.get("updatedAt"),
            "author_association": review_node.get("authorAssociation"),
            "user": user,
            "commit_id": (review_node.get("commit") or {}).get("oid"),
        }

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_reviews_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining reviews for a PR that has more than the initial batch."""
        reviews = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"owner": owner, "name": name, "number": pr_number, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(PR_REVIEWS_EXTRA_QUERY, variables, installation_id)
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"{owner}/{name}", f"extra reviews for PR #{pr_number}", e) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            review_conn = pr_data["reviews"]
            for node in review_conn.get("nodes", []):
                reviews.append(self._transform_graphql_review_to_rest(node))
            page_info = review_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return reviews

    def _fetch_reviews_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PR reviews for a repo using GraphQL with nested reviews."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        # Use smaller page size to keep cost reasonable: 1 + 25*(1+10) = 276 pts/query
        initial_page_size = 25
        page_size = initial_page_size

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_REVIEWS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"PR reviews (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for reviews, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                review_conn = pr_node.get("reviews", {})

                # Collect inline reviews
                review_records = []
                for review_node in review_conn.get("nodes", []):
                    review_records.append(self._transform_graphql_review_to_rest(review_node))

                # Fetch extra reviews if the PR has more than the initial batch
                review_page_info = review_conn.get("pageInfo", {})
                if review_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_reviews_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        review_page_info.get("endCursor"),
                    )
                    review_records.extend(extra)
                    logger.info(f"PR {repo_name}#{pr_number} had {len(review_records)} reviews (fetched extra pages)")

                for review_data in review_records:
                    review_data["_repository"] = repo_name
                    review_data["_installation_id"] = installation_id
                    review_data["_pull_request_number"] = pr_number
                    review_data["_pull_request_id"] = pr_db_id
                    records.append(SourceRecord(id=str(review_data["id"]), data=review_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} reviews, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} PR reviews from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_reviews(self, pagination: dict) -> SourceIteration:
        """Fetch PR reviews using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_reviews_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _transform_graphql_comment_to_rest(comment_node: dict) -> dict:
        """Convert a GraphQL issue comment node to REST-compatible dict."""
        author = comment_node.get("author")
        user = None
        if author:
            user = {
                "login": author.get("login"),
                "id": author.get("databaseId"),
                "avatar_url": author.get("avatarUrl"),
            }
        return {
            "id": comment_node.get("databaseId"),
            "body": comment_node.get("body"),
            "created_at": comment_node.get("createdAt"),
            "updated_at": comment_node.get("updatedAt"),
            "html_url": comment_node.get("url"),
            "user": user,
        }

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_comments_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining comments for a PR that has more than the initial batch."""
        comments = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"owner": owner, "name": name, "number": pr_number, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(PR_COMMENTS_EXTRA_QUERY, variables, installation_id)
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"{owner}/{name}", f"extra comments for PR #{pr_number}", e) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            comment_conn = pr_data["comments"]
            for node in comment_conn.get("nodes", []):
                comments.append(self._transform_graphql_comment_to_rest(node))
            page_info = comment_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return comments

    def _fetch_comments_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PR comments for a repo using GraphQL with nested comments."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        # Use smaller page size to keep cost reasonable: 1 + 25*(1+1) = ~51 pts/query
        initial_page_size = 25
        page_size = initial_page_size

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_COMMENTS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"PR comments (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for comments, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                comment_conn = pr_node.get("comments", {})

                # Collect inline comments
                comment_records = []
                for comment_node in comment_conn.get("nodes", []):
                    comment_records.append(self._transform_graphql_comment_to_rest(comment_node))

                # Fetch extra comments if the PR has more than the initial batch (100)
                comment_page_info = comment_conn.get("pageInfo", {})
                if comment_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_comments_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        comment_page_info.get("endCursor"),
                    )
                    comment_records.extend(extra)
                    logger.info(f"PR {repo_name}#{pr_number} had {len(comment_records)} comments (fetched extra pages)")

                for comment_data in comment_records:
                    comment_data["_repository"] = repo_name
                    comment_data["_installation_id"] = installation_id
                    comment_data["_pull_request_number"] = pr_number
                    comment_data["_pull_request_id"] = pr_db_id
                    records.append(SourceRecord(id=str(comment_data["id"]), data=comment_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} comments, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} PR comments from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_comments(self, pagination: dict) -> SourceIteration:
        """Fetch PR comments using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_comments_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _transform_graphql_review_comment_to_rest(comment_node: dict) -> dict:
        """Convert a GraphQL PullRequestReviewComment node to REST-compatible dict."""
        author = comment_node.get("author")
        user = None
        if author:
            user = {
                "login": author.get("login"),
                "id": author.get("databaseId"),
                "avatar_url": author.get("avatarUrl"),
            }
        return {
            "id": comment_node.get("databaseId"),
            "body": comment_node.get("body"),
            "path": comment_node.get("path"),
            "line": comment_node.get("line"),
            "original_line": comment_node.get("originalLine"),
            "start_line": comment_node.get("startLine"),
            "original_start_line": comment_node.get("originalStartLine"),
            "diff_hunk": comment_node.get("diffHunk"),
            "outdated": comment_node.get("outdated"),
            "created_at": comment_node.get("createdAt"),
            "updated_at": comment_node.get("updatedAt"),
            "html_url": comment_node.get("url"),
            "user": user,
            "in_reply_to_id": (comment_node.get("replyTo") or {}).get("databaseId"),
            "commit_id": (comment_node.get("commit") or {}).get("oid"),
            "original_commit_id": (comment_node.get("originalCommit") or {}).get("oid"),
            "pull_request_review_id": (comment_node.get("pullRequestReview") or {}).get("databaseId"),
        }

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_review_comments_graphql(
        self,
        review_node_id: str,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining comments for a review that has more than the initial batch."""
        comments = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"reviewNodeId": review_node_id, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(PR_REVIEW_COMMENTS_EXTRA_COMMENTS_QUERY, variables, installation_id)
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"node:{review_node_id}", "extra review comments", e) from e
            node_data = (result.get("data", {}) or {}).get("node")
            if not node_data:
                break
            comment_conn = node_data.get("comments", {})
            for node in comment_conn.get("nodes", []):
                comments.append(self._transform_graphql_review_comment_to_rest(node))
            page_info = comment_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return comments

    def _collect_comments_from_reviews(
        self,
        review_nodes: list,
        installation_id: int,
        repo_name: str,
        pr_number: int,
    ) -> List[dict]:
        """Extract all review comments from a list of review nodes, fetching extra pages as needed."""
        comment_records = []
        for review_node in review_nodes:
            comment_conn = review_node.get("comments", {})
            for comment_node in comment_conn.get("nodes", []):
                comment_records.append(self._transform_graphql_review_comment_to_rest(comment_node))

            comment_page_info = comment_conn.get("pageInfo", {})
            if comment_page_info.get("hasNextPage"):
                review_node_id = review_node.get("id")
                if review_node_id:
                    extra = self._fetch_extra_review_comments_graphql(
                        review_node_id,
                        installation_id,
                        comment_page_info.get("endCursor"),
                    )
                    comment_records.extend(extra)
                    logger.info(
                        f"Review {review_node.get('databaseId')} on {repo_name}#{pr_number} "
                        f"had extra comment pages ({len(extra)} additional comments)"
                    )
        return comment_records

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_review_comments_reviews_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining reviews (with comments) for a PR that has more than the initial batch."""
        all_comments = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"owner": owner, "name": name, "number": pr_number, "first": 10, "after": cursor}
            try:
                result = self._graphql_post(PR_REVIEW_COMMENTS_EXTRA_REVIEWS_QUERY, variables, installation_id)
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(
                    f"{owner}/{name}", f"extra reviews for review comments on PR #{pr_number}", e
                ) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            review_conn = pr_data["reviews"]
            all_comments.extend(
                self._collect_comments_from_reviews(
                    review_conn.get("nodes", []),
                    installation_id,
                    f"{owner}/{name}",
                    pr_number,
                )
            )
            page_info = review_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return all_comments

    def _fetch_review_comments_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PR review comments for a repo using GraphQL with nested reviews->comments."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        # Small page size due to triple nesting: cost ~= 1 + N*(1 + 10*(1+100))
        # With N=3: ~3,034 points per query (under 5,000 limit)
        initial_page_size = 3
        page_size = initial_page_size

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_REVIEW_COMMENTS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"PR review comments (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for review comments, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                review_conn = pr_node.get("reviews", {})

                comment_records = self._collect_comments_from_reviews(
                    review_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                )

                review_page_info = review_conn.get("pageInfo", {})
                if review_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_review_comments_reviews_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        review_page_info.get("endCursor"),
                    )
                    comment_records.extend(extra)
                    logger.info(
                        f"PR {repo_name}#{pr_number} had extra review pages for review comments "
                        f"({len(comment_records)} total comments)"
                    )

                for comment_data in comment_records:
                    comment_data["_repository"] = repo_name
                    comment_data["_installation_id"] = installation_id
                    comment_data["_pull_request_number"] = pr_number
                    comment_data["_pull_request_id"] = pr_db_id
                    records.append(SourceRecord(id=str(comment_data["id"]), data=comment_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} review comments, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} PR review comments from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_review_comments(self, pagination: dict) -> SourceIteration:
        """Fetch PR review comments using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_review_comments_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _transform_graphql_reaction_to_rest(reaction_node: dict) -> dict:
        """Convert a GraphQL Reaction node to a flat dict (no parent context)."""
        user_data = reaction_node.get("user")
        user = None
        if user_data:
            user = {
                "login": user_data.get("login"),
                "id": user_data.get("databaseId"),
            }
        return {
            "id": reaction_node.get("databaseId"),
            "content": reaction_node.get("content"),
            "created_at": reaction_node.get("createdAt"),
            "user": user,
        }

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_pr_comment_reactions_graphql(
        self,
        comment_node_id: str,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining reactions for a top-level PR comment that has > 20 reactions."""
        reactions = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"commentNodeId": comment_node_id, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(
                    PR_COMMENT_REACTIONS_EXTRA_REACTIONS_QUERY,
                    variables,
                    installation_id,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"node:{comment_node_id}", "extra PR comment reactions", e) from e
            node_data = (result.get("data", {}) or {}).get("node")
            if not node_data:
                break
            reaction_conn = node_data.get("reactions", {})
            for node in reaction_conn.get("nodes", []):
                reactions.append(self._transform_graphql_reaction_to_rest(node))
            page_info = reaction_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return reactions

    def _collect_reactions_from_pr_comments(
        self,
        comment_nodes: list,
        installation_id: int,
        repo_name: str,
        pr_number: int,
        pr_db_id: int,
    ) -> List[SourceRecord]:
        """Walk top-level PR comment nodes and emit one SourceRecord per reaction."""
        records = []
        for comment_node in comment_nodes:
            comment_db_id = comment_node.get("databaseId")
            comment_node_id = comment_node.get("id")
            comment_updated_at = comment_node.get("updatedAt")
            reaction_conn = comment_node.get("reactions", {}) or {}

            reactions = [self._transform_graphql_reaction_to_rest(node) for node in reaction_conn.get("nodes", [])]
            page_info = reaction_conn.get("pageInfo", {})
            if page_info.get("hasNextPage") and comment_node_id:
                extra = self._fetch_extra_pr_comment_reactions_graphql(
                    comment_node_id,
                    installation_id,
                    page_info.get("endCursor"),
                )
                reactions.extend(extra)

            for reaction_data in reactions:
                reaction_data["_repository"] = repo_name
                reaction_data["_installation_id"] = installation_id
                reaction_data["_pull_request_number"] = pr_number
                reaction_data["_pull_request_id"] = pr_db_id
                reaction_data["_comment_id"] = comment_db_id
                reaction_data["_comment_updated_at"] = comment_updated_at
                records.append(SourceRecord(id=str(reaction_data["id"]), data=reaction_data))
        return records

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_pr_comments_with_reactions_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
        repo_name: str,
        pr_db_id: int,
    ) -> List[SourceRecord]:
        """Fetch remaining comments (and their reactions) for a PR with > 100 comments."""
        records = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {
                "owner": owner,
                "name": name,
                "number": pr_number,
                "first": 100,
                "after": cursor,
            }
            try:
                result = self._graphql_post(
                    PR_COMMENT_REACTIONS_EXTRA_COMMENTS_QUERY,
                    variables,
                    installation_id,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"extra reaction-comments for PR #{pr_number}", e) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            comment_conn = pr_data["comments"]
            records.extend(
                self._collect_reactions_from_pr_comments(
                    comment_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                    pr_db_id,
                )
            )
            page_info = comment_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return records

    def _fetch_pr_comment_reactions_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all top-level PR comment reactions for a repo using GraphQL."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records: List[SourceRecord] = []
        cursor = None
        page = 0
        # Cost: 1 + 1 + 10 + 10*100 = ~1012 pts/query.
        initial_page_size = 10
        page_size = initial_page_size

        logger.info(f"Starting PR comment reactions fetch for {repo_name}")

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            logger.info(f"[{repo_name}] PR comment reactions page {page}, records so far: {len(records)}")
            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_COMMENT_REACTIONS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"PR comment reactions (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for reactions, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                comment_conn = pr_node.get("comments", {})

                comment_records = self._collect_reactions_from_pr_comments(
                    comment_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                    pr_db_id,
                )
                comment_page_info = comment_conn.get("pageInfo", {})
                if comment_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_pr_comments_with_reactions_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        comment_page_info.get("endCursor"),
                        repo_name,
                        pr_db_id,
                    )
                    comment_records.extend(extra)

                records.extend(comment_records)

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} PR comment reactions, stopping")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} PR comment reactions from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_comment_reactions(self, pagination: dict) -> SourceIteration:
        """Fetch top-level PR comment reactions (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_pr_comment_reactions_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_review_comment_reactions_graphql(
        self,
        comment_node_id: str,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining reactions for a review comment that has > 20 reactions."""
        reactions = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"commentNodeId": comment_node_id, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(
                    PR_REVIEW_COMMENT_REACTIONS_EXTRA_REACTIONS_QUERY,
                    variables,
                    installation_id,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"node:{comment_node_id}", "extra review comment reactions", e) from e
            node_data = (result.get("data", {}) or {}).get("node")
            if not node_data:
                break
            reaction_conn = node_data.get("reactions", {})
            for node in reaction_conn.get("nodes", []):
                reactions.append(self._transform_graphql_reaction_to_rest(node))
            page_info = reaction_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return reactions

    def _collect_reactions_from_review_comments(
        self,
        comment_nodes: list,
        installation_id: int,
        repo_name: str,
        pr_number: int,
        pr_db_id: int,
        review_db_id: int,
    ) -> List[SourceRecord]:
        """Walk review-comment nodes and emit one SourceRecord per reaction."""
        records = []
        for comment_node in comment_nodes:
            comment_db_id = comment_node.get("databaseId")
            comment_node_id = comment_node.get("id")
            comment_updated_at = comment_node.get("updatedAt")
            reaction_conn = comment_node.get("reactions", {}) or {}

            reactions = [self._transform_graphql_reaction_to_rest(node) for node in reaction_conn.get("nodes", [])]
            page_info = reaction_conn.get("pageInfo", {})
            if page_info.get("hasNextPage") and comment_node_id:
                extra = self._fetch_extra_review_comment_reactions_graphql(
                    comment_node_id,
                    installation_id,
                    page_info.get("endCursor"),
                )
                reactions.extend(extra)

            for reaction_data in reactions:
                reaction_data["_repository"] = repo_name
                reaction_data["_installation_id"] = installation_id
                reaction_data["_pull_request_number"] = pr_number
                reaction_data["_pull_request_id"] = pr_db_id
                reaction_data["_pull_request_review_id"] = review_db_id
                reaction_data["_comment_id"] = comment_db_id
                reaction_data["_comment_updated_at"] = comment_updated_at
                records.append(SourceRecord(id=str(reaction_data["id"]), data=reaction_data))
        return records

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_review_comments_with_reactions_graphql(
        self,
        review_node_id: str,
        installation_id: int,
        after: str,
        repo_name: str,
        pr_number: int,
        pr_db_id: int,
        review_db_id: int,
    ) -> List[SourceRecord]:
        """Fetch remaining comments (with reactions) for a review with > 50 comments."""
        records = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"reviewNodeId": review_node_id, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(
                    PR_REVIEW_COMMENT_REACTIONS_EXTRA_COMMENTS_QUERY,
                    variables,
                    installation_id,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(
                    repo_name, f"extra reaction-comments for review {review_node_id} on PR #{pr_number}", e
                ) from e
            node_data = (result.get("data", {}) or {}).get("node")
            if not node_data:
                break
            comment_conn = node_data.get("comments", {})
            records.extend(
                self._collect_reactions_from_review_comments(
                    comment_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                    pr_db_id,
                    review_db_id,
                )
            )
            page_info = comment_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return records

    def _collect_reactions_from_reviews(
        self,
        review_nodes: list,
        installation_id: int,
        repo_name: str,
        pr_number: int,
        pr_db_id: int,
    ) -> List[SourceRecord]:
        """Walk review nodes — for each, walk comments and emit reactions."""
        records = []
        for review_node in review_nodes:
            review_node_id = review_node.get("id")
            review_db_id = review_node.get("databaseId")
            comment_conn = review_node.get("comments", {}) or {}

            review_records = self._collect_reactions_from_review_comments(
                comment_conn.get("nodes", []),
                installation_id,
                repo_name,
                pr_number,
                pr_db_id,
                review_db_id,
            )
            comment_page_info = comment_conn.get("pageInfo", {})
            if comment_page_info.get("hasNextPage") and review_node_id:
                extra = self._fetch_extra_review_comments_with_reactions_graphql(
                    review_node_id,
                    installation_id,
                    comment_page_info.get("endCursor"),
                    repo_name,
                    pr_number,
                    pr_db_id,
                    review_db_id,
                )
                review_records.extend(extra)

            records.extend(review_records)
        return records

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_reviews_with_reactions_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
        repo_name: str,
        pr_db_id: int,
    ) -> List[SourceRecord]:
        """Fetch remaining reviews (with comments and reactions) for a PR with > 10 reviews."""
        records = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {
                "owner": owner,
                "name": name,
                "number": pr_number,
                "first": 10,
                "after": cursor,
            }
            try:
                result = self._graphql_post(
                    PR_REVIEW_COMMENT_REACTIONS_EXTRA_REVIEWS_QUERY,
                    variables,
                    installation_id,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"extra reaction-reviews for PR #{pr_number}", e) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            reviews_conn = pr_data["reviews"]
            records.extend(
                self._collect_reactions_from_reviews(
                    reviews_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                    pr_db_id,
                )
            )
            page_info = reviews_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return records

    def _fetch_review_comment_reactions_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PR review comment reactions for a repo using GraphQL."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records: List[SourceRecord] = []
        cursor = None
        page = 0
        # Cost: 1 + 1 + 3 + 30 + 3*10*50 + 3*10*50*1 = ~1535 pts/query.
        initial_page_size = 3
        page_size = initial_page_size

        logger.info(f"Starting review comment reactions fetch for {repo_name}")

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            logger.info(f"[{repo_name}] review comment reactions page {page}, records so far: {len(records)}")
            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_REVIEW_COMMENT_REACTIONS_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"review comment reactions (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for review reactions, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                reviews_conn = pr_node.get("reviews", {}) or {}

                pr_records = self._collect_reactions_from_reviews(
                    reviews_conn.get("nodes", []),
                    installation_id,
                    repo_name,
                    pr_number,
                    pr_db_id,
                )
                reviews_page_info = reviews_conn.get("pageInfo", {})
                if reviews_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_reviews_with_reactions_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        reviews_page_info.get("endCursor"),
                        repo_name,
                        pr_db_id,
                    )
                    pr_records.extend(extra)

                records.extend(pr_records)

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} review comment reactions, stopping")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} review comment reactions from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_review_comment_reactions(self, pagination: dict) -> SourceIteration:
        """Fetch PR review comment reactions (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_review_comment_reactions_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _transform_graphql_timeline_item(node: dict) -> dict:
        """Convert a GraphQL timeline item node to a flat dict."""
        typename = node.get("__typename", "Unknown")
        actor = node.get("actor")
        author = node.get("author")

        item = {"event": typename, "created_at": node.get("createdAt")}

        # IssueComment and PullRequestReview have databaseId; others use node id
        if node.get("databaseId"):
            item["id"] = node["databaseId"]
        elif node.get("id"):
            item["id"] = node["id"]

        # Actor-based events (most timeline events)
        if actor:
            item["actor"] = {"login": actor.get("login")}

        # Author-based events (IssueComment, PullRequestReview)
        if author:
            item["author"] = {
                "login": author.get("login"),
                "id": author.get("databaseId"),
            }

        # Event-specific fields
        if typename == "IssueComment":
            item["body"] = node.get("body")
            item["updated_at"] = node.get("updatedAt")
        elif typename == "PullRequestReview":
            item["state"] = node.get("state")
            item["body"] = node.get("body")
            item["updated_at"] = node.get("updatedAt")
        elif typename == "PullRequestCommit":
            commit = node.get("commit", {})
            commit_author = commit.get("author", {})
            item["id"] = commit.get("oid")
            item["created_at"] = commit.get("committedDate")
            item["message"] = commit.get("message")
            item["authored_date"] = commit.get("authoredDate")
            item["author"] = {
                "name": commit_author.get("name"),
                "email": commit_author.get("email"),
                "login": (commit_author.get("user") or {}).get("login"),
                "id": (commit_author.get("user") or {}).get("databaseId"),
            }
        elif typename == "MergedEvent":
            item["commit_id"] = (node.get("commit") or {}).get("oid")
            item["merge_ref_name"] = node.get("mergeRefName")
        elif typename == "LabeledEvent":
            item["label"] = (node.get("label") or {}).get("name")
        elif typename == "UnlabeledEvent":
            item["label"] = (node.get("label") or {}).get("name")
        elif typename == "RenamedTitleEvent":
            item["previous_title"] = node.get("previousTitle")
            item["current_title"] = node.get("currentTitle")
        elif typename == "MilestonedEvent":
            item["milestone_title"] = node.get("milestoneTitle")
        elif typename in ("ReviewRequestedEvent", "ReviewRequestRemovedEvent"):
            reviewer = node.get("requestedReviewer") or {}
            item["requested_reviewer"] = {
                "login": reviewer.get("login"),
                "id": reviewer.get("databaseId"),
                "team_slug": reviewer.get("slug"),
            }
        elif typename in ("AssignedEvent", "UnassignedEvent"):
            assignee = node.get("assignee") or {}
            item["assignee"] = {
                "login": assignee.get("login"),
                "id": assignee.get("databaseId"),
            }

        return item

    # Per-PR overflow fetcher: already paginates at a small fixed page size (10-100),
    # so a query-cost timeout is unlikely here. Intentionally left on plain _graphql_post
    # rather than _graphql_post_adaptive.
    def _fetch_extra_timeline_graphql(
        self,
        owner: str,
        name: str,
        pr_number: int,
        installation_id: int,
        after: str,
    ) -> List[dict]:
        """Fetch remaining timeline items for a PR that has more than the initial batch."""
        items = []
        cursor = after
        while True:
            time.sleep(1)
            variables = {"owner": owner, "name": name, "number": pr_number, "first": 100, "after": cursor}
            try:
                result = self._graphql_post(PR_TIMELINE_EXTRA_QUERY, variables, installation_id)
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(f"{owner}/{name}", f"extra timeline items for PR #{pr_number}", e) from e
            pr_data = (result.get("data", {}).get("repository") or {}).get("pullRequest")
            if not pr_data:
                break
            timeline_conn = pr_data["timelineItems"]
            for node in timeline_conn.get("nodes", []):
                items.append(self._transform_graphql_timeline_item(node))
            page_info = timeline_conn.get("pageInfo", {})
            if not page_info.get("hasNextPage"):
                break
            cursor = page_info.get("endCursor")
        return items

    def _fetch_timeline_graphql(self, repo_info: dict) -> List[SourceRecord]:
        """Fetch all PR timeline items for a repo using GraphQL with nested timelineItems."""
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        owner, name = repo_name.split("/", 1)

        records = []
        cursor = None
        page = 0
        initial_page_size = 10  # Conservative: each PR includes up to 50 timeline items
        page_size = initial_page_size

        while True:
            variables = {"owner": owner, "name": name, "first": page_size}
            if cursor:
                variables["after"] = cursor

            if page > 0:
                time.sleep(1)

            try:
                result, page_size = self._graphql_post_adaptive(
                    PR_TIMELINE_GRAPHQL_QUERY,
                    variables,
                    installation_id,
                    start_size=page_size,
                    repo=repo_name,
                    cursor_probe_query=PR_PAGE_CURSOR_PROBE_QUERY,
                    max_size=initial_page_size,
                )
            except (RuntimeError, RequestsConnectionError) as e:
                raise RepoFetchError(repo_name, f"PR timeline (page {page})", e) from e

            repo_data = result.get("data", {}).get("repository")
            if not repo_data:
                logger.warning(f"Repository {repo_name} not found via GraphQL for timeline, skipping")
                break

            pr_connection = repo_data["pullRequests"]
            nodes = pr_connection.get("nodes", [])
            page_info = pr_connection.get("pageInfo", {})

            hit_cutoff = False
            for pr_node in nodes:
                if self._start_date and pr_node.get("updatedAt"):
                    updated = self._parse_gh_timestamp(pr_node["updatedAt"])
                    if updated < self._start_date:
                        hit_cutoff = True
                        continue

                pr_db_id = pr_node.get("databaseId")
                pr_number = pr_node.get("number")
                timeline_conn = pr_node.get("timelineItems", {})

                # Collect inline timeline items
                timeline_items = []
                for node in timeline_conn.get("nodes", []):
                    timeline_items.append(self._transform_graphql_timeline_item(node))

                # Fetch extra items if the PR has more than the initial batch
                timeline_page_info = timeline_conn.get("pageInfo", {})
                if timeline_page_info.get("hasNextPage"):
                    extra = self._fetch_extra_timeline_graphql(
                        owner,
                        name,
                        pr_number,
                        installation_id,
                        timeline_page_info.get("endCursor"),
                    )
                    timeline_items.extend(extra)
                    logger.info(
                        f"PR {repo_name}#{pr_number} had {len(timeline_items)} timeline items (fetched extra pages)"
                    )

                for idx, item_data in enumerate(timeline_items):
                    item_data["_repository"] = repo_name
                    item_data["_installation_id"] = installation_id
                    item_data["_pull_request_number"] = pr_number
                    item_data["_pull_request_id"] = pr_db_id
                    record_id = item_data.get("id") or f"{pr_number}_{idx}"
                    records.append(SourceRecord(id=str(record_id), data=item_data))

            page += 1

            if hit_cutoff:
                logger.info(f"Hit start_date cutoff for {repo_name} timeline, stopping pagination")
                break

            if not page_info.get("hasNextPage"):
                break

            cursor = page_info.get("endCursor")

        logger.info(f"Fetched {len(records)} PR timeline items from {repo_name} via GraphQL ({page} pages)")
        return records

    def get_pull_request_timeline(self, pagination: dict) -> SourceIteration:
        """Fetch PR timeline events using GraphQL API (sequential repo processing)."""
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        repo_info = remaining[0]
        remaining = remaining[1:]

        records = self._fetch_timeline_graphql(repo_info)

        if remaining:
            return SourceIteration(records=records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _parse_gh_timestamp(ts: str) -> datetime:
        """Parse a GitHub API timestamp (ISO 8601) into an aware datetime."""
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))

    def _filter_records_after(
        self,
        items: list,
        timestamp_field: str,
        since: datetime,
        id_field: str = "id",
        extra_data: Optional[dict] = None,
    ) -> tuple[list[SourceRecord], bool]:
        """Filter items by timestamp, returning records and whether we hit the cutoff.

        Returns:
            (records, hit_cutoff): records that pass the filter, and True if any item
            was older than `since` (meaning we can stop paginating).
        """
        records = []
        hit_cutoff = False
        extra = extra_data or {}
        for item in items:
            item_ts = self._parse_gh_timestamp(item[timestamp_field])
            if item_ts < since:
                hit_cutoff = True
                continue
            records.append(SourceRecord(id=str(item[id_field]), data={**item, **extra}))
        return records, hit_cutoff

    def _fetch_incremental_for_repos(
        self,
        pagination: dict,
        endpoint_template: str,
        timestamp_field: str,
        since: datetime,
        id_field: str = "id",
        params: Optional[dict] = None,
        enrich_fn: Optional[Callable[[List[SourceRecord]], List[SourceRecord]]] = None,
    ) -> SourceIteration:
        """Like _fetch_paginated_for_repos but stops when records are older than `since`."""
        extra_params = params or {}

        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_repos": repo_map[1:],
                "current_repo": repo_map[0],
                "page": 1,
                "repos_loaded": 1,
            }

        current = pagination["current_repo"]
        repo_name = current["repo"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_repos", [])

        self._switch_to_installation(installation_id)

        url = f"{BASE_URL}{endpoint_template.format(repo=repo_name)}"
        response = self._api_get(
            url,
            params={"per_page": self.config.per_page, "page": page, **extra_params},
        )

        # 409 = empty repo, 404 = not found — skip to next
        if response.status_code in (404, 409):
            logger.warning(f"Skipping {repo_name} (HTTP {response.status_code}), advancing to next repo")
            if remaining:
                return SourceIteration(
                    records=[],
                    next_pagination={
                        "remaining_repos": remaining[1:],
                        "current_repo": remaining[0],
                        "page": 1,
                        "repos_loaded": 1,
                    },
                )
            return SourceIteration(records=[], next_pagination={})

        response.raise_for_status()
        data = response.json()

        extra_data = {"_repository": repo_name, "_installation_id": installation_id}
        records, hit_cutoff = self._filter_records_after(data, timestamp_field, since, id_field, extra_data)
        if enrich_fn and records:
            records = enrich_fn(records)
        logger.info(f"Fetched {len(records)} incremental records from {repo_name} (page {page}, cutoff={hit_cutoff})")

        # If we hit the cutoff, skip remaining pages for this repo
        if hit_cutoff:
            if remaining:
                return SourceIteration(
                    records=records,
                    next_pagination={
                        "remaining_repos": remaining[1:],
                        "current_repo": remaining[0],
                        "page": 1,
                        "repos_loaded": 1,
                    },
                )
            return SourceIteration(records=records, next_pagination={})

        # More pages for this repo?
        if len(data) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining,
                    "current_repo": current,
                    "page": page + 1,
                    "repos_loaded": 1,
                },
            )

        # Next repo
        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining[1:],
                    "current_repo": remaining[0],
                    "page": 1,
                    "repos_loaded": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _fetch_all_pages_for_repo(
        self,
        repo_info: dict,
        endpoint_template: str,
        id_field: str = "id",
        params: Optional[dict] = None,
        timestamp_field: Optional[str] = None,
        since: Optional[datetime] = None,
        response_key: Optional[str] = None,
        enrich_fn: Optional[Callable[[List[SourceRecord]], List[SourceRecord]]] = None,
    ) -> List[SourceRecord]:
        """Fetch all pages for a single repo. Used by concurrent fetcher.

        Args:
            response_key: If set, extract data from response[key] (e.g. "workflow_runs").
            enrich_fn: If set, called on the records after fetching to add extra fields.
        """
        repo_name = repo_info["repo"]
        installation_id = repo_info["installation_id"]
        extra_params = params or {}
        records = []
        page = 1

        while True:
            url = f"{BASE_URL}{endpoint_template.format(repo=repo_name)}"
            response = self._api_get_with_token(
                url,
                installation_id,
                params={"per_page": self.config.per_page, "page": page, **extra_params},
            )
            if response.status_code in (404, 409):
                logger.warning(f"Skipping {repo_name} (HTTP {response.status_code})")
                break
            response.raise_for_status()
            raw = response.json()
            data = raw.get(response_key, []) if response_key else raw

            if timestamp_field and since:
                hit_cutoff = False
                for item in data:
                    ts = self._parse_gh_timestamp(item[timestamp_field])
                    if ts < since:
                        hit_cutoff = True
                        continue
                    records.append(
                        SourceRecord(
                            id=str(item[id_field]),
                            data={**item, "_repository": repo_name, "_installation_id": installation_id},
                        )
                    )
                if hit_cutoff:
                    break
            else:
                records.extend(
                    [
                        SourceRecord(
                            id=str(item[id_field]),
                            data={**item, "_repository": repo_name, "_installation_id": installation_id},
                        )
                        for item in data
                    ]
                )

            if len(data) < self.config.per_page:
                break
            page += 1

        logger.info(f"Fetched {len(records)} records from {repo_name} ({page} pages)")
        if enrich_fn and records:
            records = enrich_fn(records)
            logger.info(f"Enriched {len(records)} records from {repo_name}")
        return records

    def _fetch_repos_concurrent(
        self,
        pagination: dict,
        endpoint_template: str,
        id_field: str = "id",
        params: Optional[dict] = None,
        timestamp_field: Optional[str] = None,
        since: Optional[datetime] = None,
        response_key: Optional[str] = None,
        enrich_fn: Optional[Callable[[List[SourceRecord]], List[SourceRecord]]] = None,
    ) -> SourceIteration:
        """Fetch data from multiple repos concurrently, each repo fully paginated."""
        batch_size = self.config.concurrent_repos

        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {"remaining_repos": repo_map}

        remaining = pagination["remaining_repos"]
        if not remaining:
            return SourceIteration(records=[], next_pagination={})

        batch = remaining[:batch_size]
        remaining = remaining[batch_size:]

        all_records = []
        with ThreadPoolExecutor(max_workers=len(batch)) as pool:
            futures = {
                pool.submit(
                    self._fetch_all_pages_for_repo,
                    repo_info,
                    endpoint_template,
                    id_field,
                    params,
                    timestamp_field,
                    since,
                    response_key,
                    enrich_fn,
                ): repo_info
                for repo_info in batch
            }
            for future in as_completed(futures):
                all_records.extend(future.result())

        logger.info(
            f"Batch done: {len(all_records)} records from {len(batch)} repos ({len(remaining)} repos remaining)"
        )

        if remaining:
            return SourceIteration(records=all_records, next_pagination={"remaining_repos": remaining})
        return SourceIteration(records=all_records, next_pagination={})

    def _get_commits_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """Commits API supports native `since` param."""
        return self._fetch_paginated_for_repos(
            pagination=pagination,
            endpoint_template="/repos/{repo}/commits",
            id_field="sha",
            params={"since": since.isoformat()},
        )

    def _get_pull_requests_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PRs via GraphQL with cutoff at `since` (UPDATED_AT DESC order)."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_requests(pagination)
        finally:
            self._start_date = original_start_date

    def _get_deployments_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """Deployments via GraphQL with cutoff at `since` (CREATED_AT DESC order)."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_deployments(pagination)
        finally:
            self._start_date = original_start_date

    def _get_workflow_runs_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """Workflow runs support `created` filter with >= operator."""
        since_str = since.strftime("%Y-%m-%dT%H:%M:%SZ")

        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_repos": repo_map[1:],
                "current_repo": repo_map[0],
                "page": 1,
                "repos_loaded": 1,
            }

        current = pagination["current_repo"]
        repo_name = current["repo"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_repos", [])

        self._switch_to_installation(installation_id)

        response = self._api_get(
            f"{BASE_URL}/repos/{repo_name}/actions/runs",
            params={
                "per_page": self.config.per_page,
                "page": page,
                "created": f">={since_str}",
            },
        )

        if response.status_code in (404, 409):
            logger.warning(
                f"Skipping {repo_name} incremental workflow runs (HTTP {response.status_code}), advancing to next repo"
            )
            if remaining:
                return SourceIteration(
                    records=[],
                    next_pagination={
                        "remaining_repos": remaining[1:],
                        "current_repo": remaining[0],
                        "page": 1,
                        "repos_loaded": 1,
                    },
                )
            return SourceIteration(records=[], next_pagination={})

        response.raise_for_status()
        data = response.json()

        runs = data.get("workflow_runs", [])
        records = [
            SourceRecord(
                id=str(run["id"]),
                data={**run, "_repository": repo_name, "_installation_id": installation_id},
            )
            for run in runs
        ]
        logger.info(f"Fetched {len(records)} incremental workflow runs from {repo_name} (page {page})")

        if len(runs) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining,
                    "current_repo": current,
                    "page": page + 1,
                    "repos_loaded": 1,
                },
            )

        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining[1:],
                    "current_repo": remaining[0],
                    "page": 1,
                    "repos_loaded": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _fetch_jobs_for_runs(self, runs: List[dict], repo_name: str, installation_id: int) -> List[SourceRecord]:
        """Resolve the jobs for a page of workflow runs, concurrently, one job SourceRecord per job.

        Jobs are nested under `/repos/{repo}/actions/runs/{run_id}/jobs` (filter=all
        so every run attempt's jobs are captured, not just the latest). Uses
        `_api_get_with_token` (thread-safe, per-request auth) so the runs on this
        page can be resolved concurrently, matching the concurrency idiom used by
        `_fetch_all_pages_for_repo`. A 404/409 on an individual run's jobs endpoint
        is skipped with a warning rather than failing the whole page.

        A 5xx on a jobs page restarts that run's fetch from page 1 at the next smaller
        size in `JOBS_PAGE_SIZE_LADDER`, discarding the partial records. A 5xx at the
        smallest size raises `RepoFetchError` (fail-closed, never skipped).
        """
        if not runs:
            return []

        # Sizes <= per_page, largest first; a per_page off the ladder is the first size.
        page_sizes = [self.config.per_page] + [s for s in JOBS_PAGE_SIZE_LADDER if s < self.config.per_page]

        def _fetch_jobs_for_one_run(run: dict) -> List[SourceRecord]:
            run_id = run["id"]
            url = f"{BASE_URL}/repos/{repo_name}/actions/runs/{run_id}/jobs"
            for i, page_size in enumerate(page_sizes):
                job_records: List[SourceRecord] = []
                page = 1
                while True:
                    response = self._api_get_with_token(
                        url,
                        installation_id,
                        session=self._jobs_session,
                        params={"per_page": page_size, "page": page, "filter": "all"},
                        timeout=(10, 60),
                    )
                    if response.status_code in (404, 409):
                        logger.warning(f"Skipping jobs for {repo_name} run {run_id} (HTTP {response.status_code})")
                        return job_records
                    if response.status_code >= 500:
                        if i + 1 == len(page_sizes):
                            raise RepoFetchError(
                                repo_name,
                                f"workflow jobs for run {run_id}",
                                HTTPError(
                                    f"HTTP {response.status_code} at per_page={page_size} page {page}",
                                    response=response,
                                ),
                            )
                        logger.warning(
                            f"HTTP {response.status_code} on jobs for {repo_name} run {run_id} "
                            f"(page {page}, per_page={page_size}), retrying from page 1 at "
                            f"per_page={page_sizes[i + 1]}"
                        )
                        break
                    response.raise_for_status()
                    jobs = response.json().get("jobs", [])
                    job_records.extend(
                        SourceRecord(
                            id=str(job["id"]),
                            data={**job, "_repository": repo_name, "_installation_id": installation_id},
                        )
                        for job in jobs
                    )
                    if len(jobs) < page_size:
                        return job_records
                    page += 1
            raise AssertionError("unreachable: the smallest page size raises or returns")

        all_records: List[SourceRecord] = []
        with ThreadPoolExecutor(max_workers=min(len(runs), 8)) as pool:
            futures = [pool.submit(_fetch_jobs_for_one_run, run) for run in runs]
            for future in as_completed(futures):
                all_records.extend(future.result())
        return all_records

    def _get_workflow_jobs_paginated(self, pagination: dict, since: Optional[datetime]) -> SourceIteration:
        """Shared pagination core for workflow_jobs (used by both full-refresh and incremental).

        Jobs are a NESTED resource under runs -- there is no repo-wide jobs list --
        so this lists workflow runs per repo/page (same `created>=since` filter as
        workflow_runs, applied only when `since` is set) and resolves each page's
        runs' jobs via `_fetch_jobs_for_runs`. Mirrors the sequential per-repo
        cursor shape of `_get_workflow_runs_incremental` (`remaining_repos`,
        `current_repo`, `page`, `repos_loaded`) plus its 404/409 skip-and-advance
        handling, so this stays bounded to one page of runs (and their jobs) in
        memory per iteration.

        LIMITATION (incremental): a run created before `since` whose jobs complete
        afterward will NOT be re-ingested by a later incremental sync -- job
        completion time is not part of the run-listing filter. Re-runs create a
        new run (new `created_at`) and so ARE captured on the next sync; `filter=all`
        on the jobs endpoint also captures prior attempts of a re-listed run.
        """
        if not pagination:
            repo_map = self._discover_all_repos()
            if not repo_map:
                return SourceIteration(records=[], next_pagination={})
            pagination = {
                "remaining_repos": repo_map[1:],
                "current_repo": repo_map[0],
                "page": 1,
                "repos_loaded": 1,
            }

        current = pagination["current_repo"]
        repo_name = current["repo"]
        installation_id = current["installation_id"]
        page = pagination.get("page", 1)
        remaining = pagination.get("remaining_repos", [])

        self._switch_to_installation(installation_id)

        params = {"per_page": self.config.per_page, "page": page}
        if since:
            params["created"] = f">={since.strftime('%Y-%m-%dT%H:%M:%SZ')}"

        response = self._api_get(
            f"{BASE_URL}/repos/{repo_name}/actions/runs",
            params=params,
        )

        if response.status_code in (404, 409):
            logger.warning(f"Skipping {repo_name} workflow_jobs (HTTP {response.status_code}), advancing to next repo")
            if remaining:
                return SourceIteration(
                    records=[],
                    next_pagination={
                        "remaining_repos": remaining[1:],
                        "current_repo": remaining[0],
                        "page": 1,
                        "repos_loaded": 1,
                    },
                )
            return SourceIteration(records=[], next_pagination={})

        response.raise_for_status()
        data = response.json()
        runs = data.get("workflow_runs", [])

        records = self._fetch_jobs_for_runs(runs, repo_name, installation_id)
        logger.info(f"Fetched {len(records)} jobs from {len(runs)} runs in {repo_name} (page {page})")

        if len(runs) == self.config.per_page:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining,
                    "current_repo": current,
                    "page": page + 1,
                    "repos_loaded": 1,
                },
            )

        if remaining:
            return SourceIteration(
                records=records,
                next_pagination={
                    "remaining_repos": remaining[1:],
                    "current_repo": remaining[0],
                    "page": 1,
                    "repos_loaded": 1,
                },
            )

        return SourceIteration(records=records, next_pagination={})

    def _get_workflow_jobs_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """Incremental workflow jobs -- see `_get_workflow_jobs_paginated` docstring
        for the pagination shape and the known incremental-coverage limitation."""
        return self._get_workflow_jobs_paginated(pagination, since=since)

    def _get_pull_request_reviews_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PR reviews via GraphQL with cutoff at `since` (UPDATED_AT DESC order)."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_reviews(pagination)
        finally:
            self._start_date = original_start_date

    def _get_pull_request_comments_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PR comments via GraphQL with cutoff at `since` (UPDATED_AT DESC order)."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_comments(pagination)
        finally:
            self._start_date = original_start_date

    def _get_pull_request_timeline_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PR timeline with cutoff at `since` — limits which PRs are fetched."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_timeline(pagination)
        finally:
            self._start_date = original_start_date

    def _get_pull_request_review_comments_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PR review comments via GraphQL with cutoff at `since` (UPDATED_AT DESC order)."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_review_comments(pagination)
        finally:
            self._start_date = original_start_date

    def _get_pull_request_comment_reactions_incremental(self, pagination: dict, since: datetime) -> SourceIteration:
        """PR comment reactions via GraphQL with cutoff at `since` on parent PR's UPDATED_AT."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_comment_reactions(pagination)
        finally:
            self._start_date = original_start_date

    def _get_pull_request_review_comment_reactions_incremental(
        self, pagination: dict, since: datetime
    ) -> SourceIteration:
        """PR review comment reactions via GraphQL with cutoff at `since` on parent PR's UPDATED_AT."""
        original_start_date = self._start_date
        self._start_date = since
        try:
            return self.get_pull_request_review_comment_reactions(pagination)
        finally:
            self._start_date = original_start_date

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """Incremental sync — only fetch records updated/created after last_run."""
        since = source_state.last_run
        if since.tzinfo is None:
            since = since.replace(tzinfo=timezone.utc)

        # Re-scan a small trailing window every run in case a prior run's watermark advanced
        # past data that was never fetched. Duplicates are expected and deduplicated downstream.
        if self.config.lookback_days:
            since = since - timedelta(days=self.config.lookback_days)

        # `min` makes backfill_since a floor only: once the watermark passes it, this is a no-op.
        if self.config.backfill_since:
            floor = datetime.fromisoformat(self.config.backfill_since.replace("Z", "+00:00")).replace(
                tzinfo=timezone.utc
            )
            since = min(since, floor)

        logger.info(f"Incremental sync for stream {self.config.stream} since {since.isoformat()}")
        stream = self.config.stream

        if stream == GitHubStreams.REPOSITORIES:
            # Repositories rarely change — just do full refresh
            return self.get_repositories(pagination)
        elif stream == GitHubStreams.MEMBERS:
            # Members rarely change — just do full refresh
            return self.get_members(pagination)
        elif stream == GitHubStreams.ORGANIZATION_TEAMS:
            # Teams rarely change — just do full refresh
            return self.get_organization_teams(pagination)
        elif stream == GitHubStreams.ORGANIZATION_TEAM_MEMBERS:
            # Team membership rarely changes — just do full refresh
            return self.get_organization_team_members(pagination)
        elif stream == GitHubStreams.PULL_REQUESTS:
            return self._get_pull_requests_incremental(pagination, since)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEWS:
            return self._get_pull_request_reviews_incremental(pagination, since)
        elif stream == GitHubStreams.PULL_REQUEST_COMMENTS:
            return self._get_pull_request_comments_incremental(pagination, since)
        elif stream == GitHubStreams.COMMITS:
            return self._get_commits_incremental(pagination, since)
        elif stream == GitHubStreams.DEPLOYMENTS:
            return self._get_deployments_incremental(pagination, since)
        elif stream == GitHubStreams.WORKFLOW_RUNS:
            return self._get_workflow_runs_incremental(pagination, since)
        elif stream == GitHubStreams.WORKFLOW_JOBS:
            return self._get_workflow_jobs_incremental(pagination, since)
        elif stream == GitHubStreams.REPO_CONTENT:
            return self.get_repo_content(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_TIMELINE:
            return self._get_pull_request_timeline_incremental(pagination, since)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEW_COMMENTS:
            return self._get_pull_request_review_comments_incremental(pagination, since)
        elif stream == GitHubStreams.PULL_REQUEST_COMMENT_REACTIONS:
            return self._get_pull_request_comment_reactions_incremental(pagination, since)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEW_COMMENT_REACTIONS:
            return self._get_pull_request_review_comment_reactions_incremental(pagination, since)

        raise NotImplementedError(f"Incremental sync not implemented for stream '{stream}'")

    def get(self, pagination: dict = None) -> SourceIteration:
        stream = self.config.stream

        if stream == GitHubStreams.REPOSITORIES:
            return self.get_repositories(pagination)
        elif stream == GitHubStreams.MEMBERS:
            return self.get_members(pagination)
        elif stream == GitHubStreams.ORGANIZATION_TEAMS:
            return self.get_organization_teams(pagination)
        elif stream == GitHubStreams.ORGANIZATION_TEAM_MEMBERS:
            return self.get_organization_team_members(pagination)
        elif stream == GitHubStreams.PULL_REQUESTS:
            return self.get_pull_requests(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEWS:
            return self.get_pull_request_reviews(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_COMMENTS:
            return self.get_pull_request_comments(pagination)
        elif stream == GitHubStreams.COMMITS:
            return self.get_commits(pagination)
        elif stream == GitHubStreams.DEPLOYMENTS:
            return self.get_deployments(pagination)
        elif stream == GitHubStreams.WORKFLOW_RUNS:
            return self.get_workflow_runs(pagination)
        elif stream == GitHubStreams.WORKFLOW_JOBS:
            return self.get_workflow_jobs(pagination)
        elif stream == GitHubStreams.REPO_CONTENT:
            return self.get_repo_content(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_TIMELINE:
            return self.get_pull_request_timeline(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEW_COMMENTS:
            return self.get_pull_request_review_comments(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_COMMENT_REACTIONS:
            return self.get_pull_request_comment_reactions(pagination)
        elif stream == GitHubStreams.PULL_REQUEST_REVIEW_COMMENT_REACTIONS:
            return self.get_pull_request_review_comment_reactions(pagination)

        raise NotImplementedError(f"Stream '{stream}' is not implemented for GitHub source")
