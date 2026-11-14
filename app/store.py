"""Low-level shard storage: one SQLite file per shard, one connection per use.

The storage layer owns schemas and transactional row access.  Routing decisions
and copy/verify/switch sequencing live in engine.py.
"""

import contextlib
import hashlib
import json
import os
import sqlite3
import threading
from datetime import datetime, timezone

SHARD_SCHEMA = """
CREATE TABLE IF NOT EXISTS customers (
  customer_id  TEXT PRIMARY KEY,
  name         TEXT NOT NULL,
  routed_version INTEGER NOT NULL,
  content_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS orders (
  order_id     TEXT PRIMARY KEY,
  customer_id  TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  created_at   TEXT NOT NULL,
  note         TEXT NOT NULL DEFAULT '',
  routed_version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_by_customer ON orders(customer_id);
CREATE TABLE IF NOT EXISTS write_log (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  customer_id TEXT NOT NULL,
  order_id TEXT NOT NULL,
  origin TEXT NOT NULL,
  at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS write_log_customer ON write_log(customer_id, seq);
CREATE TABLE IF NOT EXISTS migration_copy (
  migration_id INTEGER NOT NULL,
  customer_id  TEXT NOT NULL,
  kind         TEXT NOT NULL,
  row_key      TEXT NOT NULL,
  payload      TEXT NOT NULL,
  source_seq   INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (migration_id, customer_id, kind, row_key)
);
CREATE TABLE IF NOT EXISTS migration_copy_state (
  migration_id INTEGER NOT NULL,
  customer_id  TEXT NOT NULL,
  copy_seq     INTEGER NOT NULL DEFAULT 0,
  copied_rows  INTEGER NOT NULL DEFAULT 0,
  updated_at   TEXT NOT NULL,
  PRIMARY KEY (migration_id, customer_id)
);
"""

CONTROL_SCHEMA = """
CREATE TABLE IF NOT EXISTS router_versions (
  version INTEGER PRIMARY KEY,
  algorithm TEXT NOT NULL,
  shard_count INTEGER NOT NULL,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL,
  retired_at TEXT
);
CREATE TABLE IF NOT EXISTS bindings (
  customer_id TEXT PRIMARY KEY,
  shard INTEGER NOT NULL,
  version INTEGER NOT NULL,
  generation INTEGER NOT NULL,
  previous_shard INTEGER,
  handoff_at TEXT,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS migrations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  old_version INTEGER NOT NULL,
  new_version INTEGER NOT NULL,
  status TEXT NOT NULL,
  plan_json TEXT NOT NULL,
  created_at TEXT NOT NULL,
  finished_at TEXT,
  UNIQUE (old_version, new_version)
);
CREATE TABLE IF NOT EXISTS migration_units (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  migration_id INTEGER NOT NULL,
  customer_id TEXT NOT NULL,
  source_shard INTEGER NOT NULL,
  target_shard INTEGER NOT NULL,
  status TEXT NOT NULL,
  copy_seq INTEGER NOT NULL DEFAULT 0,
  verified_at TEXT,
  switched_at TEXT,
  handoff_at TEXT,
  cleaned_at TEXT,
  detail TEXT,
  updated_at TEXT NOT NULL,
  UNIQUE (migration_id, customer_id)
);
CREATE TABLE IF NOT EXISTS unit_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  migration_id INTEGER NOT NULL,
  customer_id TEXT NOT NULL,
  phase TEXT NOT NULL,
  checked_at TEXT NOT NULL,
  source_customers INTEGER NOT NULL,
  source_orders INTEGER NOT NULL,
  target_customers INTEGER NOT NULL,
  target_orders INTEGER NOT NULL,
  source_hash TEXT,
  target_hash TEXT,
  equal INTEGER NOT NULL,
  detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS unit_conflicts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  migration_id INTEGER NOT NULL,
  customer_id TEXT NOT NULL,
  table_name TEXT NOT NULL,
  row_key TEXT NOT NULL,
  source_json TEXT,
  target_json TEXT,
  detail TEXT NOT NULL,
  phase TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  migration_id INTEGER,
  customer_id TEXT,
  kind TEXT NOT NULL,
  detail TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS query_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  at TEXT NOT NULL,
  customer_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  owner_shard INTEGER NOT NULL,
  owner_version INTEGER NOT NULL,
  hash_shard INTEGER NOT NULL,
  found INTEGER NOT NULL,
  orders_count INTEGER NOT NULL,
  total_cents INTEGER NOT NULL,
  context TEXT NOT NULL
);
"""


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def content_customer(row: dict) -> dict:
    return {"customer_id": row["customer_id"], "name": row["name"]}


def content_order(row: dict) -> dict:
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "amount_cents": row["amount_cents"],
        "created_at": row["created_at"],
        "note": row.get("note", ""),
    }


def hash_content(customer: dict | None, orders: list[dict]) -> str:
    ordered = sorted((content_order(o) for o in orders),
                     key=lambda o: (o["created_at"], o["order_id"]))
    doc = json.dumps(
        {"customer": content_customer(customer) if customer else None,
         "orders": ordered},
        sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(doc.encode("utf-8")).hexdigest()


class ShardStore:
    def __init__(self, data_dir: str, shard_count: int = 2):
        self.data_dir = os.path.abspath(data_dir)
        os.makedirs(self.data_dir, exist_ok=True)
        self._lock = threading.RLock()
        self._prepare(shard_count)

    # ---------------------------------------------------------------- files
    def path(self, shard: int) -> str:
        return os.path.join(self.data_dir, f"shard-{shard}.sqlite3")

    def exists(self, shard: int) -> bool:
        return os.path.exists(self.path(shard))

    @property
    def shard_count(self) -> int:
        count = 0
        while self.exists(count):
            count += 1
        return count

    def _prepare(self, shard_count: int):
        for shard in range(shard_count):
            with self.connect(shard) as conn:
                conn.executescript(SHARD_SCHEMA)
                if shard == 0:
                    conn.executescript(CONTROL_SCHEMA)
                conn.commit()
        with self.connect(0) as conn:
            empty = conn.execute("SELECT COUNT(*) FROM router_versions").fetchone()[0] == 0
        if empty:
            with self.connect(0) as conn:
                conn.execute(
                    "INSERT INTO router_versions(version, algorithm, shard_count, status,"
                    " created_at) VALUES (1, 'fnv1a64-mod-n-v1', 2, 'live', ?)",
                    (utcnow(),))
                conn.commit()

    def add_shard(self) -> int:
        with self._lock:
            shard = self.shard_count
            with self.connect(shard) as conn:
                conn.executescript(SHARD_SCHEMA)
                conn.commit()
            return shard

    def ensure_shards(self, count: int) -> list[int]:
        created = []
        while self.shard_count < count:
            created.append(self.add_shard())
        return created

    def reset(self, shard_count: int = 2):
        with self._lock:
            for shard in range(self.shard_count + 8):
                for suffix in ("", "-wal", "-shm", "-journal"):
                    try:
                        os.remove(self.path(shard) + suffix)
                    except FileNotFoundError:
                        pass
            self._prepare(shard_count)

    @contextlib.contextmanager
    def connect(self, shard: int):
        conn = sqlite3.connect(self.path(shard), timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
        finally:
            conn.close()

    @contextlib.contextmanager
    def transaction(self, shard: int):
        with self._lock, self.connect(shard) as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.execute("ROLLBACK")
                raise
            else:
                conn.execute("COMMIT")

    # -------------------------------------------------------------- reading
    @staticmethod
    def read_customer(conn, customer_id: str) -> dict | None:
        row = conn.execute(
            "SELECT customer_id, name, routed_version, content_hash FROM customers"
            " WHERE customer_id=?", (customer_id,)).fetchone()
        return dict(row) if row else None

    @staticmethod
    def read_orders(conn, customer_id: str) -> list[dict]:
        rows = conn.execute(
            "SELECT order_id, customer_id, amount_cents, created_at, note, routed_version"
            " FROM orders WHERE customer_id=? ORDER BY created_at, order_id",
            (customer_id,)).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def read_orders_with_seq(conn, customer_id: str) -> list[dict]:
        rows = conn.execute(
            "SELECT o.order_id, o.customer_id, o.amount_cents, o.created_at, o.note,"
            " o.routed_version, COALESCE((SELECT MAX(w.seq) FROM write_log w"
            "   WHERE w.customer_id=o.customer_id AND w.order_id=o.order_id), 0)"
            "   AS write_seq"
            " FROM orders o WHERE o.customer_id=? ORDER BY o.created_at, o.order_id",
            (customer_id,)).fetchall()
        return [dict(r) for r in rows]

    def read_snapshot(self, shard: int, customer_id: str) -> dict:
        """One transaction, so customer + orders come from one consistent view."""
        with self.connect(shard) as conn:
            conn.execute("BEGIN")
            try:
                customer = self.read_customer(conn, customer_id)
                orders = self.read_orders_with_seq(conn, customer_id)
            finally:
                conn.execute("COMMIT")
        return {"shard": shard, "customer": customer, "orders": orders,
                "hash": hash_content(customer, orders) if customer else None}

    def physical_presence(self, customer_id: str) -> list[dict]:
        out = []
        for shard in range(self.shard_count):
            snap = self.read_snapshot(shard, customer_id)
            out.append({"shard": shard, "present": snap["customer"] is not None,
                        "orders": len(snap["orders"])})
        return out

    def shard_counts(self) -> list[dict]:
        out = []
        for shard in range(self.shard_count):
            with self.connect(shard) as conn:
                customers = conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
                orders = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
                orphans = conn.execute(
                    "SELECT COUNT(*) FROM orders o LEFT JOIN customers c"
                    " ON c.customer_id=o.customer_id WHERE c.customer_id IS NULL"
                ).fetchone()[0]
                versions = conn.execute(
                    "SELECT routed_version, COUNT(*) FROM customers"
                    " GROUP BY routed_version ORDER BY routed_version").fetchall()
            out.append({"shard": shard, "customers": customers, "orders": orders,
                        "orphan_orders": orphans,
                        "versions": {str(r[0]): r[1] for r in versions}})
        return out

    # -------------------------------------------------------------- writing
    @staticmethod
    def log_write(conn, customer_id: str, order_id: str, origin: str, at: str | None = None):
        conn.execute(
            "INSERT INTO write_log(customer_id, order_id, origin, at) VALUES (?,?,?,?)",
            (customer_id, order_id, origin, at or utcnow()))

    def insert_customer(self, shard: int, customer: dict, orders: list[dict],
                        origin: str = "seed"):
        with self.transaction(shard) as conn:
            conn.execute(
                "INSERT INTO customers(customer_id, name, routed_version, content_hash)"
                " VALUES (?,?,?,?)",
                (customer["customer_id"], customer["name"],
                 customer.get("routed_version", 1), ""))
            for order in orders:
                self.insert_order_row(conn, order)
                self.log_write(conn, order["customer_id"], order["order_id"], origin,
                               order["created_at"])
            self.refresh_hash(conn, customer["customer_id"])

    @staticmethod
    def insert_order_row(conn, order: dict):
        conn.execute(
            "INSERT INTO orders(order_id, customer_id, amount_cents, created_at, note,"
            " routed_version) VALUES (?,?,?,?,?,?)",
            (order["order_id"], order["customer_id"], order["amount_cents"],
             order["created_at"], order.get("note", ""),
             order.get("routed_version", 1)))

    @staticmethod
    def refresh_hash(conn, customer_id: str):
        customer = ShardStore.read_customer(conn, customer_id)
        orders = ShardStore.read_orders(conn, customer_id)
        conn.execute("UPDATE customers SET content_hash=? WHERE customer_id=?",
                     (hash_content(customer, orders), customer_id))

    def next_order_id(self, conn, customer_id: str) -> str:
        prefix = "ORD-" + customer_id.split("-")[-1] + "-"
        rows = conn.execute("SELECT order_id FROM orders WHERE order_id LIKE ?",
                            (prefix + "%",)).fetchall()
        next_n = 1
        for row in rows:
            try:
                next_n = max(next_n, int(row["order_id"][len(prefix):]) + 1)
            except ValueError:
                continue
        return prefix + f"{next_n:02d}"

    def add_order(self, shard: int, customer_id: str, amount_cents: int, note: str,
                  origin: str, version: int = 1) -> dict:
        with self.transaction(shard) as conn:
            if self.read_customer(conn, customer_id) is None:
                raise KeyError(customer_id)
            order = {
                "order_id": self.next_order_id(conn, customer_id),
                "customer_id": customer_id,
                "amount_cents": int(amount_cents),
                "created_at": utcnow(),
                "note": note,
                "routed_version": version,
            }
            self.insert_order_row(conn, order)
            self.log_write(conn, customer_id, order["order_id"], origin,
                           order["created_at"])
            self.refresh_hash(conn, customer_id)
        return order

    def delete_customer_data(self, shard: int, customer_id: str) -> int:
        """Remove a customer and its orders from a non-owning shard."""
        with self.transaction(shard) as conn:
            orders = conn.execute("DELETE FROM orders WHERE customer_id=?",
                                  (customer_id,)).rowcount
            conn.execute("DELETE FROM customers WHERE customer_id=?", (customer_id,))
            conn.execute("DELETE FROM write_log WHERE customer_id=?", (customer_id,))
        return orders

    # -------------------------------------------------- migration staging I/O
    @staticmethod
    def staging_state(conn, migration_id: int, customer_id: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM migration_copy_state WHERE migration_id=? AND customer_id=?",
            (migration_id, customer_id)).fetchone()
        return dict(row) if row else None

    @staticmethod
    def staging_rows(conn, migration_id: int, customer_id: str,
                     kind: str | None = None) -> list[dict]:
        sql = ("SELECT * FROM migration_copy WHERE migration_id=? AND customer_id=?")
        args = [migration_id, customer_id]
        if kind:
            sql += " AND kind=?"
            args.append(kind)
        sql += " ORDER BY kind, row_key"
        return [dict(r) for r in conn.execute(sql, args).fetchall()]

    @staticmethod
    def staging_row(conn, migration_id: int, customer_id: str, kind: str,
                    row_key: str) -> dict | None:
        row = conn.execute(
            "SELECT * FROM migration_copy WHERE migration_id=? AND customer_id=?"
            " AND kind=? AND row_key=?", (migration_id, customer_id, kind, row_key)
        ).fetchone()
        return dict(row) if row else None
