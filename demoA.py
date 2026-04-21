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