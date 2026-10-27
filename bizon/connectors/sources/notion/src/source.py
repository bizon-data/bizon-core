from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, List, Optional, Tuple

from loguru import logger
from requests.auth import AuthBase

from bizon.source.auth.builder import AuthBuilder
from bizon.source.auth.config import AuthType
from bizon.source.config import SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

from .config import NotionSourceConfig, NotionStreams

NOTION_API_VERSION = "2025-09-03"
BASE_URL = "https://api.notion.com/v1"


class NotionSource(AbstractSource):
    def __init__(self, config: NotionSourceConfig):
        super().__init__(config)
        self.config: NotionSourceConfig = config
        self.session.headers.update(
            {
                "Notion-Version": NOTION_API_VERSION,
                "Content-Type": "application/json",
            }
        )

    def get_authenticator(self) -> AuthBase:
        if self.config.authentication.type.value in [AuthType.API_KEY, AuthType.BEARER]:
            return AuthBuilder.token(params=self.config.authentication.params)
        return None

    @staticmethod
    def streams() -> List[str]:
        return [item.value for item in NotionStreams]

    @staticmethod
    def get_config_class() -> SourceConfig:
        return NotionSourceConfig

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        """Test connection by listing users."""
        try:
            response = self.session.get(f"{BASE_URL}/users")
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, str(e)

    def get_total_records_count(self) -> Optional[int]:
        return None

    # ==================== USERS STREAM ====================

    def get_users(self, pagination: dict = None) -> SourceIteration:
        """Fetch all users accessible to the integration."""
        params = {"page_size": self.config.page_size}
        if pagination and pagination.get("start_cursor"):
            params["start_cursor"] = pagination["start_cursor"]

        response = self.session.get(f"{BASE_URL}/users", params=params)
        response.raise_for_status()
        data = response.json()

        records = [SourceRecord(id=user["id"], data=user) for user in data.get("results", [])]

        next_pagination = {"start_cursor": data.get("next_cursor")} if data.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== DATABASES STREAM ====================

    def get_database(self, database_id: str) -> dict:
        """Fetch a single database by ID."""
        response = self.session.get(f"{BASE_URL}/databases/{database_id}")
        response.raise_for_status()
        return response.json()

    def get_databases(self, pagination: dict = None) -> SourceIteration:
        """Fetch databases for the configured database_ids, plus any discovered under page_ids."""
        # Track progress through database_ids list
        if pagination:
            remaining_ids = pagination.get("remaining_ids", [])
        else:
            remaining_ids = self._database_ids_with_discovered()
            if not remaining_ids:
                logger.warning("No database_ids configured, returning empty results")

        if not remaining_ids:
            return SourceIteration(records=[], next_pagination={})

        # Process one database at a time
        database_id = remaining_ids[0]
        remaining_ids = remaining_ids[1:]

        try:
            database_data = self.get_database(database_id)
            records = [SourceRecord(id=database_data["id"], data=database_data)]
        except Exception as e:
            logger.error(f"Failed to fetch database {database_id}: {e}")
            records = []

        next_pagination = {"remaining_ids": remaining_ids} if remaining_ids else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== DATA SOURCES STREAM ====================

    def get_data_sources(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch data sources from databases, including any discovered under page_ids.
        In the 2025-09-03 API, databases contain a data_sources array.
        """
        if pagination:
            remaining_ids = pagination.get("remaining_ids", [])
        else:
            remaining_ids = self._database_ids_with_discovered()
            if not remaining_ids:
                logger.warning("No database_ids configured, returning empty results")

        if not remaining_ids:
            return SourceIteration(records=[], next_pagination={})

        database_id = remaining_ids[0]
        remaining_ids = remaining_ids[1:]

        records = []
        try:
            database_data = self.get_database(database_id)
            data_sources = database_data.get("data_sources", [])

            for ds in data_sources:
                # Enrich data source with parent database info
                ds_record = {
                    **ds,
                    "parent_database_id": database_id,
                    "parent_database_title": self._extract_title(database_data),
                }
                records.append(SourceRecord(id=ds["id"], data=ds_record))

        except Exception as e:
            logger.error(f"Failed to fetch data sources from database {database_id}: {e}")

        next_pagination = {"remaining_ids": remaining_ids} if remaining_ids else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== PAGES STREAM ====================

    def query_data_source(self, data_source_id: str, start_cursor: str = None, filter: dict = None) -> dict:
        """Query a data source for its pages."""
        payload = {"page_size": self.config.page_size}
        if start_cursor:
            payload["start_cursor"] = start_cursor
        if filter:
            payload["filter"] = filter

        response = self.session.post(f"{BASE_URL}/data_sources/{data_source_id}/query", json=payload)
        response.raise_for_status()
        return response.json()

    def get_page(self, page_id: str) -> dict:
        """Fetch a single page by ID."""
        response = self.session.get(f"{BASE_URL}/pages/{page_id}")
        response.raise_for_status()
        return response.json()

    def get_pages(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch pages from data sources (querying databases) and/or specific page IDs.
        """
        records = []

        if pagination:
            # Continue previous pagination state
            # remaining_data_sources is list of {"ds_id": str, "db_id": str}
            remaining_data_sources = pagination.get("remaining_data_sources", [])
            current_data_source = pagination.get("current_data_source")  # {"ds_id": str, "db_id": str}
            data_source_cursor = pagination.get("data_source_cursor")
            remaining_page_ids = pagination.get("remaining_page_ids", [])
            data_sources_loaded = pagination.get("data_sources_loaded", False)
        else:
            remaining_data_sources = []
            current_data_source = None
            data_source_cursor = None
            remaining_page_ids = list(self.config.page_ids)
            data_sources_loaded = False

        # First, load all data sources from databases if not done
        if not data_sources_loaded:
            all_database_ids = list(self.config.database_ids)
            if self.config.full_recursive_search and self.config.page_ids:
                discovered_db_ids, discovered_page_ids = self.discover_children_from_pages(self.config.page_ids)
                all_database_ids.extend(discovered_db_ids)
                remaining_page_ids.extend(discovered_page_ids)

            for db_id in all_database_ids:
                try:
                    db_data = self.get_database(db_id)
                    for ds in db_data.get("data_sources", []):
                        remaining_data_sources.append({"ds_id": ds["id"], "db_id": db_id})
                except Exception as e:
                    logger.error(f"Failed to get data sources from database {db_id}: {e}")
            data_sources_loaded = True

        # Process current data source if we have one with a cursor
        if current_data_source and data_source_cursor:
            try:
                ds_filter = self.get_filter_for_database(current_data_source["db_id"])
                result = self.query_data_source(current_data_source["ds_id"], data_source_cursor, filter=ds_filter)
                for page in result.get("results", []):
                    records.append(SourceRecord(id=page["id"], data=page))

                if result.get("has_more"):
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": current_data_source,
                            "data_source_cursor": result.get("next_cursor"),
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )
            except Exception as e:
                logger.error(f"Failed to query data source {current_data_source['ds_id']}: {e}")

        # Process next data source
        if remaining_data_sources:
            ds_info = remaining_data_sources[0]
            remaining_data_sources = remaining_data_sources[1:]

            try:
                ds_filter = self.get_filter_for_database(ds_info["db_id"])
                result = self.query_data_source(ds_info["ds_id"], filter=ds_filter)
                for page in result.get("results", []):
                    records.append(SourceRecord(id=page["id"], data=page))

                if result.get("has_more"):
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": ds_info,
                            "data_source_cursor": result.get("next_cursor"),
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )

                # If there are more data sources, continue
                if remaining_data_sources:
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": None,
                            "data_source_cursor": None,
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )
            except Exception as e:
                logger.error(f"Failed to query data source {ds_info['ds_id']}: {e}")
                # Continue with remaining data sources
                if remaining_data_sources:
                    return SourceIteration(
                        records=records,
                        next_pagination={
                            "remaining_data_sources": remaining_data_sources,
                            "current_data_source": None,
                            "data_source_cursor": None,
                            "remaining_page_ids": remaining_page_ids,
                            "data_sources_loaded": True,
                        },
                    )

        # Process individual page IDs
        if remaining_page_ids:
            page_id = remaining_page_ids[0]
            remaining_page_ids = remaining_page_ids[1:]

            try:
                page_data = self.get_page(page_id)
                records.append(SourceRecord(id=page_data["id"], data=page_data))
            except Exception as e:
                logger.error(f"Failed to fetch page {page_id}: {e}")

            if remaining_page_ids:
                return SourceIteration(
                    records=records,
                    next_pagination={
                        "remaining_data_sources": [],
                        "current_data_source": None,
                        "data_source_cursor": None,
                        "remaining_page_ids": remaining_page_ids,
                        "data_sources_loaded": True,
                    },
                )

        return SourceIteration(records=records, next_pagination={})

    # ==================== BLOCKS STREAM ====================

    def get_block_children(self, block_id: str, start_cursor: str = None) -> dict:
        """Fetch children blocks of a block."""
        params = {"page_size": self.config.page_size}
        if start_cursor:
            params["start_cursor"] = start_cursor

        response = self.session.get(f"{BASE_URL}/blocks/{block_id}/children", params=params)
        response.raise_for_status()
        return response.json()

    def get_pages_from_database(self, database_id: str, apply_filter: bool = False) -> List[str]:
        """Get all page IDs from a database by querying its data sources.

        Args:
            database_id: The database ID to fetch pages from
            apply_filter: Whether to apply database_filters config (False for inline databases)
        """
        page_ids = []
        db_filter = self.get_filter_for_database(database_id) if apply_filter else None
        try:
            db_data = self.get_database(database_id)
            if not db_data:
                return page_ids
            for ds in db_data.get("data_sources") or []:
                cursor = None
                while True:
                    result = self.query_data_source(ds["id"], cursor, filter=db_filter)
                    if not result:
                        break
                    for page in result.get("results") or []:
                        if page and page.get("id"):
                            page_ids.append(page["id"])
                    if result.get("has_more"):
                        cursor = result.get("next_cursor")
                    else:
                        break
        except Exception as e:
            logger.error(f"Failed to get pages from database {database_id}: {e}")
        return page_ids

    def fetch_blocks_recursively(
        self,
        block_id: str,
        parent_input_database_id: Optional[str] = None,
        parent_input_page_id: Optional[str] = None,
        source_page_id: Optional[str] = None,
        current_depth: int = 0,
        fetch_child_databases: bool = True,
        global_order_counter: Optional[List[int]] = None,
    ) -> List[dict]:
        """
        Fetch all blocks under a block_id recursively.
        Also fetches content from child_database blocks.

        Args:
            block_id: The block/page ID to fetch children from
            parent_input_database_id: The original input database ID from config
            parent_input_page_id: The original input page ID from config
            source_page_id: The immediate page this block belongs to
            current_depth: Current recursion depth (0 = top level)
            fetch_child_databases: Whether to recurse into child_database blocks (disable for all_* streams)
            global_order_counter: Mutable counter [int] for tracking reading order across all blocks in a page

        Returns:
            Flat list of all blocks with lineage tracking fields
        """
        # Initialize counter on first call
        if global_order_counter is None:
            global_order_counter = [0]
        # Check recursion depth limit
        if current_depth >= self.config.max_recursion_depth:
            logger.warning(
                f"Max recursion depth {self.config.max_recursion_depth} reached for block {block_id}, stopping recursion"
            )
            return []

        all_blocks = []
        cursor = None
        block_order = 0  # Track position within parent

        while True:
            result = self.get_block_children(block_id, cursor)
            if not result:
                break

            for block in result.get("results") or []:
                if not block:
                    continue
                # Add lineage tracking
                block["parent_block_id"] = block_id
                block["parent_input_database_id"] = parent_input_database_id
                block["parent_input_page_id"] = parent_input_page_id
                block["source_page_id"] = source_page_id
                # Add depth and ordering
                block["depth"] = current_depth
                block["block_order"] = block_order
                block["page_order"] = global_order_counter[0]
                block_order += 1
                global_order_counter[0] += 1

                all_blocks.append(block)

                # Handle child_database blocks - fetch their content in parallel
                if (
                    block.get("type") == "child_database"
                    and self.config.fetch_blocks_recursively
                    and fetch_child_databases
                ):
                    child_db_id = block.get("id")
                    logger.info(f"Found inline database {child_db_id} at depth {current_depth}, fetching its content")

                    try:
                        # Get all pages from the inline database
                        inline_page_ids = self.get_pages_from_database(child_db_id)

                        # Fetch blocks from inline pages in parallel
                        # Note: parallel execution means global_order_counter won't be sequential for inline DBs
                        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
                            futures = {
                                executor.submit(
                                    self.fetch_blocks_recursively,
                                    block_id=inline_page_id,
                                    parent_input_database_id=parent_input_database_id,
                                    parent_input_page_id=parent_input_page_id,
                                    source_page_id=inline_page_id,
                                    current_depth=current_depth + 1,
                                    fetch_child_databases=fetch_child_databases,
                                    global_order_counter=global_order_counter,
                                ): inline_page_id
                                for inline_page_id in inline_page_ids
                            }
                            for future in as_completed(futures):
                                try:
                                    inline_blocks = future.result()
                                    all_blocks.extend(inline_blocks)
                                except Exception as e:
                                    page_id = futures[future]
                                    logger.error(f"Failed to fetch blocks from inline page {page_id}: {e}")

                    except Exception as e:
                        logger.error(f"Failed to fetch content from inline database {child_db_id}: {e}")

                # Recursively fetch children if block has children
                # Skip child_page and child_database - they are references, not containers with inline content
                elif (
                    block.get("has_children")
                    and self.config.fetch_blocks_recursively
                    and block.get("type") not in ("child_page", "child_database")
                ):
                    try:
                        child_blocks = self.fetch_blocks_recursively(
                            block_id=block["id"],
                            parent_input_database_id=parent_input_database_id,
                            parent_input_page_id=parent_input_page_id,
                            source_page_id=source_page_id,
                            current_depth=current_depth + 1,
                            fetch_child_databases=fetch_child_databases,
                            global_order_counter=global_order_counter,
                        )
                        all_blocks.extend(child_blocks)
                    except Exception as e:
                        # synced_block can return 404 if original block is inaccessible
                        if block.get("type") == "synced_block":
                            logger.warning(f"Skipping synced_block {block.get('id')} - children inaccessible: {e}")
                        else:
                            logger.error(f"Failed to fetch children of block {block.get('id')}: {e}")

            if result.get("has_more"):
                cursor = result.get("next_cursor")
            else:
                break

        return all_blocks

    def get_blocks(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch blocks from databases and pages.
        Blocks are fetched recursively if fetch_blocks_recursively is True.
        Also fetches content from inline databases (child_database blocks).
        """
        if pagination:
            # Each item is: {"block_id": str, "input_db_id": str|None, "input_page_id": str|None, "source_page_id": str|None}
            items_to_process = pagination.get("items_to_process", [])
            items_loaded = pagination.get("items_loaded", False)
        else:
            items_to_process = []
            items_loaded = False

        # First, collect all database IDs and page IDs to fetch blocks from
        if not items_loaded:
            # Add configured page_ids (these are direct input pages)
            for page_id in self.config.page_ids:
                items_to_process.append(
                    {
                        "block_id": page_id,
                        "input_db_id": None,
                        "input_page_id": page_id,
                        "source_page_id": page_id,
                    }
                )

            # Collect pages from databases
            for db_id in self.config.database_ids:
                try:
                    db_filter = self.get_filter_for_database(db_id)
                    db_data = self.get_database(db_id)
                    for ds in db_data.get("data_sources", []):
                        cursor = None
                        while True:
                            result = self.query_data_source(ds["id"], cursor, filter=db_filter)
                            for page in result.get("results", []):
                                items_to_process.append(
                                    {
                                        "block_id": page["id"],
                                        "input_db_id": db_id,
                                        "input_page_id": None,
                                        "source_page_id": page["id"],
                                    }
                                )
                            if result.get("has_more"):
                                cursor = result.get("next_cursor")
                            else:
                                break
                except Exception as e:
                    logger.error(f"Failed to collect pages from database {db_id}: {e}")

            items_loaded = True

        if not items_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process a batch in parallel
        batch_size = self.config.max_workers
        batch = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        records = []

        def fetch_item_blocks(item_info: dict) -> List[dict]:
            """Fetch all blocks for a database or page."""
            return self.fetch_blocks_recursively(
                block_id=item_info["block_id"],
                parent_input_database_id=item_info["input_db_id"],
                parent_input_page_id=item_info["input_page_id"],
                source_page_id=item_info["source_page_id"],
            )

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(fetch_item_blocks, item_info): item_info for item_info in batch}
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    blocks = future.result()
                    for block in blocks:
                        records.append(SourceRecord(id=block["id"], data=block))
                    logger.info(f"Fetched {len(blocks)} blocks from {item_info['block_id']}")
                except Exception as e:
                    logger.error(f"Failed to fetch blocks from {item_info['block_id']}: {e}")

        next_pagination = {"items_to_process": items_to_process, "items_loaded": True} if items_to_process else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== HELPERS ====================

    def get_filter_for_database(self, database_id: str) -> Optional[dict]:
        """Get the filter configured for a database, AND-combined with the start_date filter if set."""
        db_filter = self.config.database_filters.get(database_id)
        created_time_filter = (
            {"timestamp": "created_time", "created_time": {"on_or_after": self.config.start_date}}
            if self.config.start_date
            else None
        )

        if db_filter and created_time_filter:
            return {"and": [db_filter, created_time_filter]}
        return db_filter or created_time_filter

    def _database_ids_with_discovered(self) -> List[str]:
        database_ids = list(self.config.database_ids)
        if self.config.full_recursive_search and self.config.page_ids:
            discovered_db_ids, _ = self.discover_children_from_pages(self.config.page_ids)
            database_ids.extend(discovered_db_ids)
        return database_ids

    def discover_children_from_pages(self, page_ids: List[str]) -> Tuple[List[str], List[str]]:
        """
        Recursively discover child databases and child pages under page_ids, descending into
        container blocks (toggles, columns, callouts...) and into the pages of discovered databases,
        up to max_recursion_depth.

        Returns:
            (discovered_database_ids, discovered_child_page_ids). Pages that belong to a discovered
            database are walked for deeper children but not returned.
        """
        discovered_db_ids: List[str] = []
        discovered_child_page_ids: List[str] = []
        seen_ids = set(page_ids)

        def _discover_in_page(page_id: str, depth: int):
            if depth >= self.config.max_recursion_depth:
                logger.warning(f"Discovery: max recursion depth {self.config.max_recursion_depth} reached at {page_id}")
                return

            cursor = None
            while True:
                try:
                    result = self.get_block_children(page_id, cursor)
                except Exception as e:
                    logger.error(f"Discovery: failed to get children of {page_id}: {e}")
                    break

                for block in result.get("results") or []:
                    if not block:
                        continue
                    block_type = block.get("type")
                    child_id = block.get("id")

                    if block_type == "child_database" and child_id not in seen_ids:
                        seen_ids.add(child_id)
                        discovered_db_ids.append(child_id)
                        logger.info(f"Discovery: found inline database {child_id} at depth {depth}")
                        try:
                            for pid in self.get_pages_from_database(child_id):
                                if pid not in seen_ids:
                                    seen_ids.add(pid)
                                    _discover_in_page(pid, depth + 1)
                        except Exception as e:
                            logger.error(f"Discovery: failed to get pages from database {child_id}: {e}")

                    elif block_type == "child_page" and child_id not in seen_ids:
                        seen_ids.add(child_id)
                        discovered_child_page_ids.append(child_id)
                        logger.info(f"Discovery: found child page {child_id} at depth {depth}")
                        _discover_in_page(child_id, depth + 1)

                    elif block.get("has_children") and block_type not in ("child_page", "child_database"):
                        # Container blocks are not a page/database boundary, so they keep the same depth
                        _discover_in_page(child_id, depth)

                if result.get("has_more"):
                    cursor = result.get("next_cursor")
                else:
                    break

        for page_id in page_ids:
            _discover_in_page(page_id, 0)

        logger.info(
            f"Discovery complete: found {len(discovered_db_ids)} databases and "
            f"{len(discovered_child_page_ids)} child pages from {len(page_ids)} input pages"
        )
        return discovered_db_ids, discovered_child_page_ids

    def _extract_title(self, database_data: dict) -> str:
        """Extract plain text title from database object."""
        title_parts = database_data.get("title", [])
        return "".join(part.get("plain_text", "") for part in title_parts)

    # ==================== MARKDOWN CONVERSION ====================

    def _extract_rich_text(self, rich_text_array: List[dict]) -> str:
        """Convert Notion rich text array to markdown string with formatting."""
        if not rich_text_array:
            return ""

        result = []
        for item in rich_text_array:
            text = item.get("plain_text", "")
            annotations = item.get("annotations", {})
            href = item.get("href")

            # Apply formatting in order: code, bold, italic, strikethrough
            if annotations.get("code"):
                text = f"`{text}`"
            if annotations.get("bold"):
                text = f"**{text}**"
            if annotations.get("italic"):
                text = f"*{text}*"
            if annotations.get("strikethrough"):
                text = f"~~{text}~~"
            if href:
                text = f"[{text}]({href})"

            result.append(text)

        return "".join(result)

    def _block_to_markdown(self, block: dict) -> str:
        """Convert a single Notion block to markdown string."""
        block_type = block.get("type", "")
        content = block.get(block_type) or {}

        # Text blocks
        if block_type == "paragraph":
            return self._extract_rich_text(content.get("rich_text", []))

        elif block_type == "heading_1":
            return f"# {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "heading_2":
            return f"## {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "heading_3":
            return f"### {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "bulleted_list_item":
            return f"- {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "numbered_list_item":
            return f"1. {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "to_do":
            checkbox = "[x]" if content.get("checked") else "[ ]"
            return f"- {checkbox} {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "quote":
            return f"> {self._extract_rich_text(content.get('rich_text', []))}"

        elif block_type == "callout":
            icon = content.get("icon") or {}
            emoji = icon.get("emoji", "💡")
            text = self._extract_rich_text(content.get("rich_text", []))
            return f"> {emoji} {text}"

        elif block_type == "code":
            language = content.get("language", "")
            code_text = self._extract_rich_text(content.get("rich_text", []))
            return f"```{language}\n{code_text}\n```"

        elif block_type == "equation":
            return f"$$ {content.get('expression', '')} $$"

        elif block_type == "divider":
            return "---"

        elif block_type == "toggle":
            return f"<details><summary>{self._extract_rich_text(content.get('rich_text', []))}</summary></details>"

        # Media blocks
        elif block_type == "image":
            url = content.get("external", {}).get("url") or content.get("file", {}).get("url", "")
            caption = self._extract_rich_text(content.get("caption", []))
            return f"![{caption}]({url})"

        elif block_type == "video":
            url = content.get("external", {}).get("url") or content.get("file", {}).get("url", "")
            return f"[Video]({url})"

        elif block_type == "file":
            url = content.get("external", {}).get("url") or content.get("file", {}).get("url", "")
            caption = self._extract_rich_text(content.get("caption", [])) or "File"
            return f"[{caption}]({url})"

        elif block_type == "pdf":
            url = content.get("external", {}).get("url") or content.get("file", {}).get("url", "")
            return f"[PDF]({url})"

        elif block_type == "bookmark":
            url = content.get("url", "")
            caption = self._extract_rich_text(content.get("caption", [])) or url
            return f"[{caption}]({url})"

        elif block_type == "embed":
            return f"[Embed]({content.get('url', '')})"

        elif block_type == "link_preview":
            return f"[Link Preview]({content.get('url', '')})"

        # Table blocks
        elif block_type == "table":
            return "[Table - see child blocks for rows]"

        elif block_type == "table_row":
            cells = content.get("cells", [])
            row = " | ".join(self._extract_rich_text(cell) for cell in cells)
            return f"| {row} |"

        # Database/page references
        elif block_type == "child_page":
            return f"[Page: {content.get('title', 'Untitled')}]"

        elif block_type == "child_database":
            return f"[Database: {content.get('title', 'Untitled')}]"

        elif block_type == "link_to_page":
            page_id = content.get("page_id") or content.get("database_id", "")
            return f"[Link to page: {page_id}]"

        elif block_type == "table_of_contents":
            return "[Table of Contents]"

        elif block_type == "breadcrumb":
            return "[Breadcrumb]"

        elif block_type == "synced_block":
            return "[Synced Block]"

        elif block_type == "template":
            return "[Template]"

        elif block_type == "column_list":
            return ""  # Column list is just a container

        elif block_type == "column":
            return ""  # Column is just a container

        else:
            return f"[Unsupported block type: {block_type}]"

    def get_blocks_markdown(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch blocks and convert them to markdown.
        Returns one record per block with its markdown content.
        """
        if pagination:
            items_to_process = pagination.get("items_to_process", [])
            items_loaded = pagination.get("items_loaded", False)
        else:
            items_to_process = []
            items_loaded = False

        # Collect all database IDs and page IDs to fetch blocks from
        if not items_loaded:
            for page_id in self.config.page_ids:
                items_to_process.append(
                    {
                        "block_id": page_id,
                        "input_db_id": None,
                        "input_page_id": page_id,
                        "source_page_id": page_id,
                    }
                )

            for db_id in self.config.database_ids:
                # Collect pages from database's data_sources
                try:
                    db_filter = self.get_filter_for_database(db_id)
                    db_data = self.get_database(db_id)
                    for ds in db_data.get("data_sources", []):
                        cursor = None
                        while True:
                            result = self.query_data_source(ds["id"], cursor, filter=db_filter)
                            for page in result.get("results", []):
                                items_to_process.append(
                                    {
                                        "block_id": page["id"],
                                        "input_db_id": db_id,
                                        "input_page_id": None,
                                        "source_page_id": page["id"],
                                    }
                                )
                            if result.get("has_more"):
                                cursor = result.get("next_cursor")
                            else:
                                break
                except Exception as e:
                    logger.error(f"Failed to collect pages from database {db_id}: {e}")

            items_loaded = True

        if not items_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process a batch in parallel
        batch_size = self.config.max_workers
        batch = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        records = []

        def fetch_and_convert_item(item_info: dict) -> List[dict]:
            """Fetch blocks for a database or page and convert each to markdown."""
            blocks = self.fetch_blocks_recursively(
                block_id=item_info["block_id"],
                parent_input_database_id=item_info["input_db_id"],
                parent_input_page_id=item_info["input_page_id"],
                source_page_id=item_info["source_page_id"],
                fetch_child_databases=self.config.full_recursive_search,
            )

            # Convert each block to markdown record
            block_records = []
            for block in blocks or []:
                if not block:
                    continue
                md = self._block_to_markdown(block)
                block_records.append(
                    {
                        "block_id": block.get("id"),
                        "block_type": block.get("type"),
                        "markdown": md,
                        "source_page_id": block.get("source_page_id"),
                        "parent_block_id": block.get("parent_block_id"),
                        "parent_input_database_id": block.get("parent_input_database_id"),
                        "parent_input_page_id": block.get("parent_input_page_id"),
                        "depth": block.get("depth"),
                        "block_order": block.get("block_order"),
                        "page_order": block.get("page_order"),
                        "block_raw": block,
                    }
                )
            return block_records

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(fetch_and_convert_item, item_info): item_info for item_info in batch}
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    block_records = future.result()
                    for block_record in block_records:
                        records.append(SourceRecord(id=block_record.get("block_id"), data=block_record))
                    logger.info(f"Converted {len(block_records)} blocks to markdown from {item_info['block_id']}")
                except Exception as e:
                    import traceback

                    logger.error(
                        f"Failed to fetch/convert blocks from {item_info['block_id']}: {e}\n{traceback.format_exc()}"
                    )

        next_pagination = {"items_to_process": items_to_process, "items_loaded": True} if items_to_process else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== SEARCH API (ALL_* STREAMS) ====================

    def search(self, start_cursor: str = None) -> dict:
        """
        Search all pages and databases accessible to the integration.

        Args:
            start_cursor: Pagination cursor

        Returns:
            Raw search results (filter client-side by object type)
        """
        payload = {"page_size": self.config.page_size}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        response = self.session.post(f"{BASE_URL}/search", json=payload)
        response.raise_for_status()
        return response.json()

    def search_by_type(self, object_type: str, start_cursor: str = None) -> dict:
        """
        Search and filter by object type client-side.

        Args:
            object_type: "page" or "database"
            start_cursor: Pagination cursor

        Returns:
            Filtered results matching object_type
        """
        result = self.search(start_cursor=start_cursor)

        # Filter results by object type
        filtered_results = [item for item in result.get("results", []) if item.get("object") == object_type]

        return {
            "results": filtered_results,
            "has_more": result.get("has_more", False),
            "next_cursor": result.get("next_cursor"),
        }

    def get_all_databases(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch all databases accessible to the integration.
        In 2025-09-03 API, we get data_sources from search and fetch their parent databases.
        """
        if pagination:
            db_ids_to_fetch = pagination.get("db_ids_to_fetch", [])
            dbs_loaded = pagination.get("dbs_loaded", False)
        else:
            db_ids_to_fetch = []
            dbs_loaded = False

        # Collect unique database IDs from data_sources
        if not dbs_loaded:
            seen_db_ids = set()
            search_cursor = None

            while True:
                result = self.search_by_type(object_type="data_source", start_cursor=search_cursor)
                for ds in result.get("results", []):
                    # Data sources have a parent.database_id
                    parent = ds.get("parent", {})
                    if parent.get("type") == "database_id":
                        db_id = parent.get("database_id")
                        if db_id and db_id not in seen_db_ids:
                            seen_db_ids.add(db_id)
                            db_ids_to_fetch.append(db_id)

                if result.get("has_more"):
                    search_cursor = result.get("next_cursor")
                else:
                    break

            dbs_loaded = True
            logger.info(f"Found {len(db_ids_to_fetch)} unique databases from data_sources")

        if not db_ids_to_fetch:
            return SourceIteration(records=[], next_pagination={})

        # Fetch one database at a time
        db_id = db_ids_to_fetch[0]
        db_ids_to_fetch = db_ids_to_fetch[1:]

        records = []
        try:
            db_data = self.get_database(db_id)
            records.append(SourceRecord(id=db_data["id"], data=db_data))
        except Exception as e:
            logger.error(f"Failed to fetch database {db_id}: {e}")

        next_pagination = {"db_ids_to_fetch": db_ids_to_fetch, "dbs_loaded": True} if db_ids_to_fetch else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_all_data_sources(self, pagination: dict = None) -> SourceIteration:
        """Fetch all data_sources accessible to the integration."""
        cursor = pagination.get("start_cursor") if pagination else None

        result = self.search_by_type(object_type="data_source", start_cursor=cursor)

        records = [SourceRecord(id=ds["id"], data=ds) for ds in result.get("results", [])]

        next_pagination = {"start_cursor": result.get("next_cursor")} if result.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_all_pages(self, pagination: dict = None) -> SourceIteration:
        """Fetch all pages accessible to the integration."""
        cursor = pagination.get("start_cursor") if pagination else None

        result = self.search_by_type(object_type="page", start_cursor=cursor)

        records = [SourceRecord(id=page["id"], data=page) for page in result.get("results", [])]

        next_pagination = {"start_cursor": result.get("next_cursor")} if result.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_all_blocks_markdown(self, pagination: dict = None) -> SourceIteration:
        """
        Fetch all databases and pages accessible to the integration and convert their blocks to markdown.
        Includes databases and pages from search API AND pages from all databases via data_sources.
        """
        if pagination:
            items_to_process = pagination.get("items_to_process", [])
            items_loaded = pagination.get("items_loaded", False)
        else:
            items_to_process = []
            items_loaded = False

        # Collect databases and pages from search API
        if not items_loaded:
            seen_ids = set()

            # 1. Get pages from search API
            search_cursor = None
            while True:
                result = self.search_by_type(object_type="page", start_cursor=search_cursor)
                for page in result.get("results", []):
                    if page["id"] not in seen_ids:
                        seen_ids.add(page["id"])
                        items_to_process.append(
                            {
                                "block_id": page["id"],
                                "input_db_id": None,
                                "input_page_id": None,
                                "source_page_id": page["id"],
                            }
                        )

                if result.get("has_more"):
                    search_cursor = result.get("next_cursor")
                else:
                    break

            logger.info(f"Found {len(items_to_process)} pages from search API")

            # 2. Get all data_sources and their parent databases + query for pages
            ds_search_cursor = None
            while True:
                result = self.search_by_type(object_type="data_source", start_cursor=ds_search_cursor)
                for ds in result.get("results", []):
                    ds_id = ds["id"]
                    # Get parent database_id from data_source
                    parent = ds.get("parent", {})
                    parent_db_id = parent.get("database_id") if parent.get("type") == "database_id" else None

                    # Add the parent database to fetch its blocks (headers, descriptions, etc.)
                    if parent_db_id and parent_db_id not in seen_ids:
                        seen_ids.add(parent_db_id)
                        items_to_process.append(
                            {
                                "block_id": parent_db_id,
                                "input_db_id": parent_db_id,
                                "input_page_id": None,
                                "source_page_id": None,
                            }
                        )

                    try:
                        # Query data_source for pages (no filter for all_* streams)
                        ds_cursor = None
                        while True:
                            ds_result = self.query_data_source(ds_id, ds_cursor)
                            for page in ds_result.get("results", []):
                                if page["id"] not in seen_ids:
                                    seen_ids.add(page["id"])
                                    items_to_process.append(
                                        {
                                            "block_id": page["id"],
                                            "input_db_id": parent_db_id,
                                            "input_page_id": None,
                                            "source_page_id": page["id"],
                                        }
                                    )
                            if ds_result.get("has_more"):
                                ds_cursor = ds_result.get("next_cursor")
                            else:
                                break
                    except Exception as e:
                        logger.error(f"Failed to get pages from data_source {ds_id}: {e}")

                if result.get("has_more"):
                    ds_search_cursor = result.get("next_cursor")
                else:
                    break

            items_loaded = True
            logger.info(
                f"Total {len(items_to_process)} unique items (databases + pages) to process for all_blocks_markdown"
            )

        if not items_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process a batch in parallel
        batch_size = self.config.max_workers
        batch = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        records = []

        def fetch_and_convert_item(item_info: dict) -> List[dict]:
            """Fetch blocks for a database or page and convert each to markdown."""
            # fetch_child_databases=False because all_blocks_markdown already collects
            # all pages from all data_sources, so we don't need to recurse into child_database blocks
            blocks = self.fetch_blocks_recursively(
                block_id=item_info["block_id"],
                parent_input_database_id=item_info["input_db_id"],
                parent_input_page_id=item_info["input_page_id"],
                source_page_id=item_info["source_page_id"],
                fetch_child_databases=False,
            )

            # Convert each block to markdown record
            block_records = []
            for block in blocks or []:
                if not block:
                    continue
                md = self._block_to_markdown(block)
                block_records.append(
                    {
                        "block_id": block.get("id"),
                        "block_type": block.get("type"),
                        "markdown": md,
                        "source_page_id": block.get("source_page_id"),
                        "parent_block_id": block.get("parent_block_id"),
                        "parent_input_database_id": block.get("parent_input_database_id"),
                        "parent_input_page_id": block.get("parent_input_page_id"),
                        "depth": block.get("depth"),
                        "block_order": block.get("block_order"),
                        "page_order": block.get("page_order"),
                        "block_raw": block,
                    }
                )
            return block_records

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(fetch_and_convert_item, item_info): item_info for item_info in batch}
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    block_records = future.result()
                    for block_record in block_records:
                        records.append(SourceRecord(id=block_record.get("block_id"), data=block_record))
                    logger.info(f"Converted {len(block_records)} blocks to markdown from {item_info['block_id']}")
                except Exception as e:
                    logger.error(f"Failed to fetch/convert blocks from {item_info['block_id']}: {e}")

        next_pagination = {"items_to_process": items_to_process, "items_loaded": True} if items_to_process else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    # ==================== INCREMENTAL SYNC ====================

    def search_with_filter(
        self, start_cursor: str = None, last_edited_after: str = None, object_type: str = None
    ) -> dict:
        """
        Search with optional last_edited_time filter for incremental sync.

        Note: Notion Search API doesn't support timestamp filtering directly.
        We sort by last_edited_time descending and filter client-side.

        Args:
            start_cursor: Pagination cursor
            last_edited_after: ISO 8601 timestamp to filter by last_edited_time
            object_type: Optional filter by object type ("page" or "database")

        Returns:
            Search results filtered by timestamp
        """
        payload = {"page_size": self.config.page_size}
        if start_cursor:
            payload["start_cursor"] = start_cursor

        # Sort by last_edited_time descending to get most recent first
        if last_edited_after:
            payload["sort"] = {"direction": "descending", "timestamp": "last_edited_time"}

        response = self.session.post(f"{BASE_URL}/search", json=payload)
        response.raise_for_status()
        result = response.json()

        # Filter by object_type client-side if specified
        if object_type:
            result["results"] = [item for item in result.get("results", []) if item.get("object") == object_type]

        # Filter by last_edited_time client-side
        # Since results are sorted descending, stop when we hit an old item
        if last_edited_after:
            filtered_results = []
            found_old_item = False
            for item in result.get("results", []):
                item_edited_time = item.get("last_edited_time", "")
                if item_edited_time > last_edited_after:
                    filtered_results.append(item)
                else:
                    found_old_item = True
                    break

            result["results"] = filtered_results
            # If we found an old item, no need to paginate further
            if found_old_item:
                result["has_more"] = False

        return result

    def get_pages_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """
        Fetch pages updated after source_state.last_run using the Search API with timestamp filter.
        """
        cursor = pagination.get("start_cursor") if pagination else None
        last_edited_after = source_state.last_run.isoformat()

        result = self.search_with_filter(start_cursor=cursor, last_edited_after=last_edited_after, object_type="page")

        records = [SourceRecord(id=page["id"], data=page) for page in result.get("results", [])]

        logger.info(f"Incremental sync: fetched {len(records)} pages updated after {last_edited_after}")

        next_pagination = {"start_cursor": result.get("next_cursor")} if result.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_all_pages_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """
        Fetch all pages accessible to the integration updated after source_state.last_run.
        Same as get_pages_after but without database_ids filter.
        """
        return self.get_pages_after(source_state, pagination)

    def get_databases_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """
        Fetch databases updated after source_state.last_run.
        """
        cursor = pagination.get("start_cursor") if pagination else None
        last_edited_after = source_state.last_run.isoformat()

        # Search for data_sources (databases don't appear directly in search in 2025-09-03 API)
        result = self.search_with_filter(
            start_cursor=cursor, last_edited_after=last_edited_after, object_type="data_source"
        )

        # Extract unique database IDs from data_sources
        seen_db_ids = set()
        records = []
        for ds in result.get("results", []):
            parent = ds.get("parent", {})
            if parent.get("type") == "database_id":
                db_id = parent.get("database_id")
                if db_id and db_id not in seen_db_ids:
                    seen_db_ids.add(db_id)
                    try:
                        db_data = self.get_database(db_id)
                        records.append(SourceRecord(id=db_data["id"], data=db_data))
                    except Exception as e:
                        logger.error(f"Failed to fetch database {db_id}: {e}")

        logger.info(f"Incremental sync: fetched {len(records)} databases updated after {last_edited_after}")

        next_pagination = {"start_cursor": result.get("next_cursor")} if result.get("has_more") else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_blocks_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """
        Fetch blocks from pages updated after source_state.last_run.
        First finds updated pages, then fetches their blocks.
        """
        if pagination:
            items_to_process = pagination.get("items_to_process", [])
            items_loaded = pagination.get("items_loaded", False)
            search_cursor = pagination.get("search_cursor")
        else:
            items_to_process = []
            items_loaded = False
            search_cursor = None

        last_edited_after = source_state.last_run.isoformat()

        # Collect pages updated after last_run
        if not items_loaded:
            while True:
                result = self.search_with_filter(
                    start_cursor=search_cursor, last_edited_after=last_edited_after, object_type="page"
                )
                for page in result.get("results", []):
                    items_to_process.append(
                        {
                            "block_id": page["id"],
                            "input_db_id": None,
                            "input_page_id": None,
                            "source_page_id": page["id"],
                        }
                    )

                if result.get("has_more"):
                    search_cursor = result.get("next_cursor")
                else:
                    break

            items_loaded = True
            logger.info(f"Incremental sync: found {len(items_to_process)} pages updated after {last_edited_after}")

        if not items_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process a batch in parallel
        batch_size = self.config.max_workers
        batch = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        records = []

        def fetch_item_blocks(item_info: dict) -> List[dict]:
            return self.fetch_blocks_recursively(
                block_id=item_info["block_id"],
                parent_input_database_id=item_info["input_db_id"],
                parent_input_page_id=item_info["input_page_id"],
                source_page_id=item_info["source_page_id"],
            )

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(fetch_item_blocks, item_info): item_info for item_info in batch}
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    blocks = future.result()
                    for block in blocks:
                        records.append(SourceRecord(id=block["id"], data=block))
                except Exception as e:
                    logger.error(f"Failed to fetch blocks from {item_info['block_id']}: {e}")

        next_pagination = {"items_to_process": items_to_process, "items_loaded": True} if items_to_process else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_blocks_markdown_after(
        self, source_state: SourceIncrementalState, pagination: dict = None
    ) -> SourceIteration:
        """
        Fetch blocks from pages updated after source_state.last_run and convert to markdown.
        Respects database_ids and database_filters configuration.
        """
        if pagination:
            items_to_process = pagination.get("items_to_process", [])
            items_loaded = pagination.get("items_loaded", False)
        else:
            items_to_process = []
            items_loaded = False

        last_edited_after = source_state.last_run.isoformat()

        # Collect pages updated after last_run from configured databases
        if not items_loaded:
            # Query each configured database with timestamp filter
            for db_id in self.config.database_ids:
                try:
                    db_data = self.get_database(db_id)
                    db_filter = self.get_filter_for_database(db_id)

                    for ds in db_data.get("data_sources", []):
                        ds_cursor = None
                        while True:
                            # Build filter with last_edited_time constraint
                            incremental_filter = {
                                "timestamp": "last_edited_time",
                                "last_edited_time": {"after": last_edited_after},
                            }
                            # Combine with existing database filter if present
                            if db_filter:
                                combined_filter = {"and": [incremental_filter, db_filter]}
                            else:
                                combined_filter = incremental_filter

                            result = self.query_data_source(ds["id"], ds_cursor, filter=combined_filter)
                            for page in result.get("results", []):
                                items_to_process.append(
                                    {
                                        "block_id": page["id"],
                                        "input_db_id": db_id,
                                        "input_page_id": None,
                                        "source_page_id": page["id"],
                                    }
                                )

                            if result.get("has_more"):
                                ds_cursor = result.get("next_cursor")
                            else:
                                break
                except Exception as e:
                    logger.error(f"Failed to query database {db_id} for incremental sync: {e}")

            # Also check configured page_ids (filter by last_edited_time)
            for page_id in self.config.page_ids:
                try:
                    page_data = self.get_page(page_id)
                    if page_data.get("last_edited_time", "") > last_edited_after:
                        items_to_process.append(
                            {
                                "block_id": page_id,
                                "input_db_id": None,
                                "input_page_id": page_id,
                                "source_page_id": page_id,
                            }
                        )
                except Exception as e:
                    logger.error(f"Failed to fetch page {page_id} for incremental sync: {e}")

            items_loaded = True
            logger.info(
                f"Incremental sync: found {len(items_to_process)} pages for blocks_markdown after {last_edited_after}"
            )

        if not items_to_process:
            return SourceIteration(records=[], next_pagination={})

        # Process a batch in parallel
        batch_size = self.config.max_workers
        batch = items_to_process[:batch_size]
        items_to_process = items_to_process[batch_size:]

        records = []

        def fetch_and_convert_item(item_info: dict) -> List[dict]:
            blocks = self.fetch_blocks_recursively(
                block_id=item_info["block_id"],
                parent_input_database_id=item_info["input_db_id"],
                parent_input_page_id=item_info["input_page_id"],
                source_page_id=item_info["source_page_id"],
                fetch_child_databases=False,
            )

            block_records = []
            for block in blocks or []:
                if not block:
                    continue
                md = self._block_to_markdown(block)
                block_records.append(
                    {
                        "block_id": block.get("id"),
                        "block_type": block.get("type"),
                        "markdown": md,
                        "source_page_id": block.get("source_page_id"),
                        "parent_block_id": block.get("parent_block_id"),
                        "parent_input_database_id": block.get("parent_input_database_id"),
                        "parent_input_page_id": block.get("parent_input_page_id"),
                        "depth": block.get("depth"),
                        "block_order": block.get("block_order"),
                        "page_order": block.get("page_order"),
                        "block_raw": block,
                    }
                )
            return block_records

        with ThreadPoolExecutor(max_workers=self.config.max_workers) as executor:
            futures = {executor.submit(fetch_and_convert_item, item_info): item_info for item_info in batch}
            for future in as_completed(futures):
                item_info = futures[future]
                try:
                    block_records = future.result()
                    for block_record in block_records:
                        records.append(SourceRecord(id=block_record.get("block_id"), data=block_record))
                except Exception as e:
                    logger.error(f"Failed to fetch/convert blocks from {item_info['block_id']}: {e}")

        next_pagination = {"items_to_process": items_to_process, "items_loaded": True} if items_to_process else {}

        return SourceIteration(records=records, next_pagination=next_pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        """
        Fetch records updated after source_state.last_run for incremental sync.

        Supported streams:
        - pages, all_pages: Uses Search API with last_edited_time filter
        - databases, all_databases: Uses Search API to find updated data_sources
        - blocks, all_blocks_markdown: First finds updated pages, then fetches their blocks
        """
        stream = self.config.stream

        if stream in [NotionStreams.PAGES, NotionStreams.ALL_PAGES]:
            return self.get_pages_after(source_state, pagination)
        elif stream in [NotionStreams.DATABASES, NotionStreams.ALL_DATABASES]:
            return self.get_databases_after(source_state, pagination)
        elif stream == NotionStreams.BLOCKS:
            return self.get_blocks_after(source_state, pagination)
        elif stream in [NotionStreams.BLOCKS_MARKDOWN, NotionStreams.ALL_BLOCKS_MARKDOWN]:
            return self.get_blocks_markdown_after(source_state, pagination)
        else:
            # For streams that don't support incremental, fall back to full refresh
            logger.warning(f"Stream {stream} does not support incremental sync, falling back to full refresh")
            return self.get(pagination)

    # ==================== MAIN DISPATCH ====================

    def get(self, pagination: dict = None) -> SourceIteration:
        if self.config.stream == NotionStreams.USERS:
            return self.get_users(pagination)
        elif self.config.stream == NotionStreams.DATABASES:
            return self.get_databases(pagination)
        elif self.config.stream == NotionStreams.DATA_SOURCES:
            return self.get_data_sources(pagination)
        elif self.config.stream == NotionStreams.PAGES:
            return self.get_pages(pagination)
        elif self.config.stream == NotionStreams.BLOCKS:
            return self.get_blocks(pagination)
        elif self.config.stream == NotionStreams.BLOCKS_MARKDOWN:
            return self.get_blocks_markdown(pagination)
        elif self.config.stream == NotionStreams.ALL_PAGES:
            return self.get_all_pages(pagination)
        elif self.config.stream == NotionStreams.ALL_DATABASES:
            return self.get_all_databases(pagination)
        elif self.config.stream == NotionStreams.ALL_DATA_SOURCES:
            return self.get_all_data_sources(pagination)
        elif self.config.stream == NotionStreams.ALL_BLOCKS_MARKDOWN:
            return self.get_all_blocks_markdown(pagination)

        raise NotImplementedError(f"Stream {self.config.stream} not implemented for Notion")
