import json
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

from bizon.common.models.config import BizonRecord


def convert_to_bizon_record(record: Dict[str, Any]) -> BizonRecord:
    return {
        "_bizon_id": uuid4().hex,
        "_bizon_loaded_at": datetime.utcnow().isoformat(),
        "_bizon_data": json.dumps(record),
    }
