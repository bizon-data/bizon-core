import pytest

from bizon.connectors.sources.notion.src.config import NotionSourceConfig, NotionStreams
from bizon.connectors.sources.notion.src.source import NOTION_API_VERSION, NotionSource
from bizon.source.discover import find_unknown_source_keys

AUTH = {"type": "bearer", "params": {"token": "test-token"}}


def make_source(**overrides) -> NotionSource:
    config = {
        "name": "notion",
        "stream": NotionStreams.PAGES,
        "authentication": AUTH,
        "init_pipeline": False,
        **overrides,
    }
    return NotionSource(NotionSourceConfig.model_validate(config))


def block(block_id, block_type, has_children=False):
    return {"id": block_id, "type": block_type, "has_children": has_children}


# page -> children; a toggle is a container block, not a page boundary
TREE = {
    "root": [
        block("toggle", "toggle", has_children=True),
        block("child-page", "child_page", has_children=True),
        block("para", "paragraph"),
    ],
    "toggle": [block("inline-db", "child_database")],
    "child-page": [block("deep-db", "child_database")],
    "db-page": [block("grandchild-page", "child_page")],
    "grandchild-page": [],
    "deep-db-page": [],
}
DB_PAGES = {"inline-db": ["db-page"], "deep-db": ["deep-db-page"], "configured-db": ["configured-page"]}


@pytest.fixture
def fake_api(monkeypatch):
    calls = {"children": [], "queries": [], "databases": []}

    def get_block_children(self, block_id, start_cursor=None):
        calls["children"].append(block_id)
        return {"results": TREE.get(block_id, []), "has_more": False}

    def get_pages_from_database(self, database_id, apply_filter=False):
        return list(DB_PAGES.get(database_id, []))

    def get_database(self, database_id):
        calls["databases"].append(database_id)
        return {"id": database_id, "title": [], "data_sources": [{"id": f"ds-{database_id}"}]}

    def query_data_source(self, data_source_id, start_cursor=None, filter=None):
        calls["queries"].append((data_source_id, filter))
        db_id = data_source_id.removeprefix("ds-")
        return {"results": [{"id": p} for p in DB_PAGES.get(db_id, [])], "has_more": False}

    def get_page(self, page_id):
        return {"id": page_id}

    monkeypatch.setattr(NotionSource, "get_block_children", get_block_children)
    monkeypatch.setattr(NotionSource, "get_pages_from_database", get_pages_from_database)
    monkeypatch.setattr(NotionSource, "get_database", get_database)
    monkeypatch.setattr(NotionSource, "query_data_source", query_data_source)
    monkeypatch.setattr(NotionSource, "get_page", get_page)
    return calls


def drain(source, method):
    records, pagination = [], None
    while True:
        iteration = method(pagination)
        records.extend(r.id for r in iteration.records)
        if not iteration.next_pagination:
            return records
        pagination = iteration.next_pagination


@pytest.mark.parametrize("prefix", ["", "notion_"])
def test_both_field_spellings_are_accepted(prefix):
    raw = {
        "name": "notion",
        "stream": "pages",
        "authentication": AUTH,
        f"{prefix}database_ids": ["db"],
        f"{prefix}page_ids": ["page"],
        f"{prefix}database_filters": {"db": {"property": "Status"}},
    }

    config = NotionSourceConfig.model_validate(raw)

    assert config.database_ids == ["db"]
    assert config.page_ids == ["page"]
    assert config.database_filters == {"db": {"property": "Status"}}
    assert find_unknown_source_keys(NotionSourceConfig, raw) == []


def test_session_keeps_notion_headers_under_default_http_policy():
    source = make_source()

    assert source.session.headers["Notion-Version"] == NOTION_API_VERSION
    assert source.session.timeout == (10, 60)


@pytest.mark.parametrize(
    ("filters", "start_date", "expected"),
    [
        ({}, None, None),
        ({"db": {"property": "Status"}}, None, {"property": "Status"}),
        ({}, "2024-01-01", {"timestamp": "created_time", "created_time": {"on_or_after": "2024-01-01"}}),
        (
            {"db": {"property": "Status"}},
            "2024-01-01",
            {
                "and": [
                    {"property": "Status"},
                    {"timestamp": "created_time", "created_time": {"on_or_after": "2024-01-01"}},
                ]
            },
        ),
    ],
)
def test_start_date_is_and_combined_with_database_filter(filters, start_date, expected):
    source = make_source(database_filters=filters, start_date=start_date)

    assert source.get_filter_for_database("db") == expected


def test_start_date_reaches_data_source_query(fake_api):
    source = make_source(database_ids=["configured-db"], start_date="2024-01-01")

    drain(source, source.get_pages)

    assert fake_api["queries"] == [
        ("ds-configured-db", {"timestamp": "created_time", "created_time": {"on_or_after": "2024-01-01"}})
    ]


def test_discovery_walks_containers_child_pages_and_database_pages(fake_api):
    source = make_source()

    db_ids, page_ids = source.discover_children_from_pages(["root"])

    assert db_ids == ["inline-db", "deep-db"]
    # pages of a discovered database are walked but only child_page blocks are returned
    assert page_ids == ["grandchild-page", "child-page"]
    assert "db-page" in fake_api["children"]


def test_discovery_stops_at_max_recursion_depth(fake_api):
    source = make_source(max_recursion_depth=1)

    db_ids, page_ids = source.discover_children_from_pages(["root"])

    # depth 0 still sees the toggle's database and the child page, but nothing below them
    assert db_ids == ["inline-db"]
    assert page_ids == ["child-page"]
    assert "child-page" not in fake_api["children"]


def test_databases_stream_includes_discovered_databases(fake_api):
    source = make_source(
        stream="databases", database_ids=["configured-db"], page_ids=["root"], full_recursive_search=True
    )

    assert drain(source, source.get_databases) == ["configured-db", "inline-db", "deep-db"]


def test_data_sources_stream_includes_discovered_databases(fake_api):
    source = make_source(stream="data_sources", page_ids=["root"], full_recursive_search=True)

    assert drain(source, source.get_data_sources) == ["ds-inline-db", "ds-deep-db"]


def test_discovery_is_off_by_default(fake_api):
    source = make_source(stream="databases", page_ids=["root"])

    assert drain(source, source.get_databases) == []
    assert fake_api["children"] == []


def test_pages_stream_includes_discovered_pages_and_database_pages(fake_api):
    source = make_source(database_ids=["configured-db"], page_ids=["root"], full_recursive_search=True)

    records = drain(source, source.get_pages)

    assert records == ["configured-page", "db-page", "deep-db-page", "root", "grandchild-page", "child-page"]


@pytest.mark.parametrize("full_recursive_search", [False, True])
def test_blocks_markdown_follows_child_databases_only_with_recursive_search(monkeypatch, full_recursive_search):
    seen = []

    def fake_fetch(self, block_id, fetch_child_databases=True, **kwargs):
        seen.append(fetch_child_databases)
        return []

    monkeypatch.setattr(NotionSource, "fetch_blocks_recursively", fake_fetch)
    source = make_source(stream="blocks_markdown", page_ids=["root"], full_recursive_search=full_recursive_search)

    source.get_blocks_markdown()

    assert seen == [full_recursive_search]


def test_blocks_stream_still_follows_child_databases_by_default(monkeypatch):
    seen = []

    def fake_fetch(self, block_id, fetch_child_databases=True, **kwargs):
        seen.append(fetch_child_databases)
        return []

    monkeypatch.setattr(NotionSource, "fetch_blocks_recursively", fake_fetch)
    source = make_source(stream="blocks", page_ids=["root"])

    source.get_blocks()

    assert seen == [True]
