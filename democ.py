def assign_round_robin(num_partitions: int, consumers: list[str]) -> dict[str, list[int]]:
    assignment = {c: [] for c in consumers}
    for p in range(num_partitions):
        c = consumers[p % len(consumers)]
        assignment[c].append(p)
    return assignment

print(assign_round_robin(6, ["C0", "C1", "C2"]))
# {'C0': [0, 3], 'C1': [1, 4], 'C2': [2, 5]}