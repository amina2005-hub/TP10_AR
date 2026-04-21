import hashlib
from collections import Counter

def partition_of(key: str, n: int) -> int:
    h = hashlib.sha256(key.encode()).digest()
    return int.from_bytes(h[:4], "big") % n

keys_uniforme = [f"sensor-{i:03d}" for i in range(200)]
repartition = Counter(partition_of(k, 4) for k in keys_uniforme)
print("Uniforme :", repartition)

keys_skew = ["sensor-HOT"] * 180 + [f"sensor-{i}" for i in range(20)]
repartition_skew = Counter(partition_of(k, 4) for k in keys_skew)
print("Skew     :", repartition_skew)


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

def assign_round_robin(num_partitions: int, consumers: list[str]) -> dict[str, list[int]]:
    assignment = {c: [] for c in consumers}
    for p in range(num_partitions):
        c = consumers[p % len(consumers)]
        assignment[c].append(p)
    return assignment

print(assign_round_robin(6, ["C0", "C1", "C2"]))
# {'C0': [0, 3], 'C1': [1, 4], 'C2': [2, 5]}

store = OffsetStore("state/offsets.json")

# Premier run
for i in range(store.get("agri-stats", 0), 10):
    print(f"traitement evt {i} sur partition 0")
    store.commit("agri-stats", 0, i + 1)

# Arrêt volontaire ici (simule un crash)

# Second run : on reprend là où on s'était arrêté
print("reprise à offset =", store.get("agri-stats", 0))