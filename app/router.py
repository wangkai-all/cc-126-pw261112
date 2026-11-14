"""Fixed-hash, versioned routing for customer-keyed shards.

A customer and every one of its orders share one routing key: the customer id.
Routing is a pure function of (key, version).  A version pins a shard count, so
the same key maps to a position that can be recomputed and audited forever.
"""

ALGORITHM = "fnv1a64-mod-n-v1"


def fnv1a64(text: str) -> int:
    value = 0xCBF29CE484222325
    for byte in text.encode("utf-8"):
        value ^= byte
        value = (value * 0x100000001B3) & 0xFFFFFFFFFFFFFFFF
    return value


def shard_for(key: str, shard_count: int) -> int:
    if shard_count < 1:
        raise ValueError("shard count must be >= 1")
    return fnv1a64(key) % shard_count


class Router:
    """Version table: version -> (algorithm, shard count)."""

    def __init__(self, versions: list[dict]):
        if not versions:
            raise ValueError("router needs at least one version")
        self.versions = sorted(versions, key=lambda v: v["version"])

    @property
    def live_version(self) -> int:
        return self.versions[-1]["version"]

    def get(self, version: int) -> dict:
        for item in self.versions:
            if item["version"] == version:
                return item
        raise KeyError(f"unknown routing version {version}")

    def shard_count(self, version: int) -> int:
        return self.get(version)["shard_count"]

    def location(self, key: str, version: int) -> int:
        return shard_for(key, self.shard_count(version))

    def plan(self, old: int, new: int, bound_pairs: list[tuple[str, int]]) -> dict:
        """Classify every bound customer under old and new versions."""
        decisions = []
        for key, pinned in sorted(bound_pairs):
            old_shard = self.location(key, old)
            new_shard = self.location(key, new)
            decisions.append({
                "customer_id": key,
                "pinned_shard": pinned,
                "old_shard": old_shard,
                "new_shard": new_shard,
                "moves": new_shard != pinned,
            })
        return {"old_version": old, "new_version": new,
                "old_shard_count": self.shard_count(old),
                "new_shard_count": self.shard_count(new),
                "customers": decisions}
