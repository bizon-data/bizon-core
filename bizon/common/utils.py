import json
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

from pytz import UTC

from .models import BizonRecord


def convert_to_bizon_record(record: Dict[str, Any]) -> BizonRecord:
    return {
        "_bizon_id": uuid4().hex,
        "_bizon_loaded_at": datetime.now(tz=UTC).isoformat(),
        "_bizon_data": json.dumps(record),
    }
