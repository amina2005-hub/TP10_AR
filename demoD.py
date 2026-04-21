store = OffsetStore("state/offsets.json")

# Premier run
for i in range(store.get("agri-stats", 0), 10):
    print(f"traitement evt {i} sur partition 0")
    store.commit("agri-stats", 0, i + 1)

# Arrêt volontaire ici (simule un crash)

# Second run : on reprend là où on s'était arrêté
print("reprise à offset =", store.get("agri-stats", 0))