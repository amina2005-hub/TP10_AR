import queue
from dataclasses import dataclass

@dataclass
class Broker:
    num_partitions: int
    def __post_init__(self):
        self.partitions = [queue.Queue() for _ in range(self.num_partitions)]
    def publish(self, key: str, value: dict):
        p = partition_of(key, self.num_partitions)
        self.partitions[p].put((p, value))
    def read(self, p: int, timeout=0.5):
        try: return self.partitions[p].get(timeout=timeout)
        except queue.Empty: return None