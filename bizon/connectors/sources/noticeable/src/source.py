import uuid
from typing import Any, List, Optional, Tuple

from pydantic import Field
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.graphql import graphql_post_adaptive
from bizon.source.models import SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

EMAIL_OPENED_EVENTS_QUERY = """
query EmailOpenedEvents($first: Int!, $after: String, $projectId: ID!) {
    project(projectId: $projectId) {
        name
        emailEvents(first: $first, after: $after, type: OPEN) {
            pageInfo {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
            }
            edges {
                cursor
                node {
                    id
                    dispatchId
                    createdAt
                    recipient
                    subject
                }
            }
        }
    }
}
"""

PUBLICATIONS_QUERY = """
query Publications($first: Int!, $after: String, $projectId: ID!) {
    project(projectId: $projectId) {
        publications(first: $first, after: $after, isDraft: false) {
            pageInfo {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
            }
            edges {
                cursor
                node {
                    author {
                        avatar
                        displayName
                        email
                        jobTitle
                    }
                    content {
                        html
                        plaintext
                    }
                    createdAt
                    excerpt
                    id
                    isDraft
                    labels {
                        backgroundColor
                        name
                        slug
                        textColor
                    }
                    permalink
                    publishedAt
                    segments
                    slug
                    title
                    updatedAt
                }
            }
        }
    }
}
"""

PUBLICATION_COMMENTS_QUERY = """
query PublicationComments($first: Int!, $after: String, $projectId: ID!) {
    publicationComments(projectId: $projectId, first: $first, after: $after) {
        pageInfo {
            endCursor
            hasNextPage
            hasPreviousPage
            startCursor
        }
        edges {
            cursor
            node {
                author {
                    avatar
                    browser
                    displayName
                    email
                    id
                    os
                    platform
                }
                content {
                    plaintext
                }
                createdAt
                handledAt
                id
                origin
                projectId
                publicationId
                referer
            }
        }
    }
}
"""

EMAIL_SUBSCRIPTIONS_QUERY = """
query EmailSubscriptions($first: Int!, $after: String, $projectId: ID!) {
    project(projectId: $projectId) {
        emailSubscriptions(first: $first, after: $after) {
            pageInfo {
                endCursor
                hasNextPage
                hasPreviousPage
                startCursor
            }
            edges {
                cursor
                node {
                    createdAt
                    displayName
                    email
                    isArchived
                    status
                    updatedAt
                }
            }
        }
    }
}
"""


class NoticeableSourceConfig(SourceConfig):
    project_id: str = Field(..., description="Project ID of the Noticeable account")
    # The GraphQL transport classifies non-2xx bodies itself (429/5xx retried, errors[] surfaced).
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )


# Starting `first` per stream. A shrink is carried forward in self._page_sizes, so a sustained
# degradation on a long walk steps the ladder down once instead of on every page.
DEFAULT_PAGE_SIZES = {
    "email_opened_events": 100,
    "publications": 10,
    "publication_comments": 10,
    "email_subscriptions": 100,
}


class NoticeableSource(AbstractSource):
    def __init__(self, config: NoticeableSourceConfig):
        super().__init__(config)
        self.config: NoticeableSourceConfig = config
        self.url_graphql = "https://api.noticeable.io/graphql"
        self._page_sizes = dict(DEFAULT_PAGE_SIZES)

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value == AuthType.API_KEY:
            return AuthBuilder.token(params=self.config.authentication.params)

    @staticmethod
    def streams() -> List[str]:
        return ["email_opened_events", "publications", "publication_comments", "email_subscriptions"]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return NoticeableSourceConfig

    def check_connection(self) -> Tuple[bool | Any | None]:
        return True, None

    def get_total_records_count(self) -> int | None:
        return None

    def run_graphql_query(self, query: str, variables: dict, *, label: str) -> dict:
        """POST, shrinking `first` on the same cursor on a transient error, and carry the size that
        worked forward to the next page."""
        body, size = graphql_post_adaptive(
            self.session,
            self.url_graphql,
            query,
            variables,
            page_size_var="first",
            start_size=self._page_sizes[label],
            label=label,
            log_prefix="Noticeable",
        )
        self._page_sizes[label] = size
        return body

    def get_email_opened_events(self, pagination: dict) -> SourceIteration:
        variables = {
            "first": self._page_sizes["email_opened_events"],
            "after": (pagination or {}).get("endCursor"),
            "projectId": self.config.project_id,
        }
        data = self.run_graphql_query(EMAIL_OPENED_EVENTS_QUERY, variables, label="email_opened_events")

        edges = data.get("data", {}).get("project", {}).get("emailEvents", {}).get("edges", [])

        records = [SourceRecord(id=edge["node"]["id"], data=edge["node"]) for edge in edges]
        pagination_info = data.get("data", {}).get("project", {}).get("emailEvents", {}).get("pageInfo", {})
        next_pagination = pagination_info if pagination_info.get("hasNextPage") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_publications(self, pagination: dict) -> SourceIteration:
        variables = {
            "first": self._page_sizes["publications"],
            "after": (pagination or {}).get("endCursor"),
            "projectId": self.config.project_id,
        }
        data = self.run_graphql_query(PUBLICATIONS_QUERY, variables, label="publications")

        edges = data.get("data", {}).get("project", {}).get("publications", {}).get("edges", [])

        records = [SourceRecord(id=edge["node"]["id"], data=edge["node"]) for edge in edges]
        pagination_info = data.get("data", {}).get("project", {}).get("publications", {}).get("pageInfo", {})
        next_pagination = pagination_info if pagination_info.get("hasNextPage") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_publication_comments(self, pagination: dict) -> SourceIteration:
        variables = {
            "first": self._page_sizes["publication_comments"],
            "after": (pagination or {}).get("endCursor"),
            "projectId": self.config.project_id,
        }
        data = self.run_graphql_query(PUBLICATION_COMMENTS_QUERY, variables, label="publication_comments")

        edges = data.get("data", {}).get("publicationComments", {}).get("edges", [])

        records = [SourceRecord(id=edge["node"]["id"], data=edge["node"]) for edge in edges]
        pagination_info = data.get("data", {}).get("publicationComments", {}).get("pageInfo", {})
        next_pagination = pagination_info if pagination_info.get("hasNextPage") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_email_subscriptions(self, pagination: dict) -> SourceIteration:
        # The API exposes no stable id for a subscription, so each record gets a random one.
        variables = {
            "first": self._page_sizes["email_subscriptions"],
            "after": (pagination or {}).get("endCursor"),
            "projectId": self.config.project_id,
        }
        data = self.run_graphql_query(EMAIL_SUBSCRIPTIONS_QUERY, variables, label="email_subscriptions")

        edges = data.get("data", {}).get("project", {}).get("emailSubscriptions", {}).get("edges", [])

        records = [SourceRecord(id=str(uuid.uuid4()), data=edge["node"]) for edge in edges]
        pagination_info = data.get("data", {}).get("project", {}).get("emailSubscriptions", {}).get("pageInfo", {})
        next_pagination = pagination_info if pagination_info.get("hasNextPage") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == "email_opened_events":
            return self.get_email_opened_events(pagination)
        if self.config.stream == "publications":
            return self.get_publications(pagination)
        if self.config.stream == "publication_comments":
            return self.get_publication_comments(pagination)
        if self.config.stream == "email_subscriptions":
            return self.get_email_subscriptions(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Noticeable")
