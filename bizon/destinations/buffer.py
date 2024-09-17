from typing import List

from bizon.destinations.models import DestinationRecord


class DestinationBuffer:

    def __init__(self, buffer_size: int) -> None:
        self.buffer_size = buffer_size
        self.records: List[DestinationRecord] = []
        self._current_size = 0
        self._iterations: List[int] = []

    @property
    def current_size(self) -> int:
        """Return buffer size"""
        return self._current_size

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

    def flush(self):
        """Flush buffer"""
        self.records = []
        self._current_size = 0
        self._iterations = []

    def add_source_iteration_records_to_buffer(self, iteration: int, records: List[DestinationRecord]):
        """Add records for the given iteration to buffer"""
        self.records.extend(records)
        self._current_size += len(records)
        self._iterations.append(iteration)
