import sys
from datetime import datetime
from typing import List

from bizon.destinations.models import DestinationRecord


class DestinationBuffer:

    def __init__(self, buffer_size: int, buffer_flush_timeout: int) -> None:
        self.buffer_size = buffer_size * 1024 * 1024  # Convert to bytes
        self.buffer_flush_timeout = buffer_flush_timeout
        self.records: List[DestinationRecord] = []
        self._iterations: List[int] = []
        self.pagination = {}
        self.modified_at: List[datetime] = [datetime.utcnow()]

    @property
    def current_size(self) -> int:
        """Return buffer size"""
        return sys.getsizeof(self.records)

    @property
    def buffer_free_space_pct(self) -> float:
        """Return free space in buffer in percentage"""
        return round((self.buffer_free_space / self.buffer_size) * 100, 3)

    @property
    def from_iteration(self) -> int:
        """Return the smallest iteration in buffer"""
        if not self._iterations:
            raise ValueError("Buffer is empty")
        return min(self._iterations)

    @property
    def to_iteration(self) -> int:
        """Return the largest iteration in buffer"""
        if not self._iterations:
            raise ValueError("Buffer is empty")
        return max(self._iterations)

    @property
    def buffer_free_space(self) -> int:
        """Return free space for records in buffer"""
        assert self.current_size <= self.buffer_size, "Buffer size exceeded"
        return self.buffer_size - self.current_size

    @property
    def ripeness(self) -> float:
        """Return buffer ripeness"""
        if self.buffer_flush_timeout == 0:
            return 0
        return round((max(self.modified_at) - min(self.modified_at)).seconds, 2)

    @property
    def is_ripe(self) -> bool:
        """Check if buffer is ripe for flushing based on the timeout"""
        if self.buffer_flush_timeout == 0:
            return False
        return (max(self.modified_at) - min(self.modified_at)).seconds >= self.buffer_flush_timeout

    def flush(self):
        """Flush buffer"""
        self.records = []
        self._iterations = []
        self.pagination = {}
        self.modified_at = []

    def add_source_iteration_records_to_buffer(
        self, iteration: int, records: List[DestinationRecord], pagination: dict = None
    ):
        """Add records for the given iteration to buffer"""
        self.records.extend(records)
        self._iterations.append(iteration)
        self.pagination = pagination
        self.modified_at.append(datetime.utcnow())
