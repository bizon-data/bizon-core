from threading import Thread

from .queue import PipelineQueue


class DestinationThread(Thread):
    def __init__(self, queue: PipelineQueue):
        super(DestinationThread, self).__init__()
        self.name = "destination"
        self.queue = queue

    def run(self) -> None:
        while True:
            records = self.queue.get()
            print("Consuming data from queue: ", len(records))
