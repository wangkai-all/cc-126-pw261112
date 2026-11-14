"""Versioned routing, online copy/verify/switch, and fault recovery.

Ownership rules, which the rest of this file enforces:

* A customer is *reachable* only through its binding row.  A physical copy on
  another shard is never routable.  Customer and every order share the one
  routing key (customer_id).
* Writes always go to the current owner (binding), reads too.  During a copy
  that is the source shard; the switch re-points the binding atomically, and
  from that instant writes and reads go to the target.  A per-customer lock
  covers (binding lookup + row write) and (verify + materialize + switch), so
  no write can slip between verification and the switch.
* Copies land in a staging table keyed by (migration, customer, kind, row_key).
  Replaying a copy re-checks each row: identical row = skip, different row =
  conflict and stop.  We never delete-and-rewrite to hide a mismatch.
* An interrupted unit keeps its staged rows and resumes from them; new orders
  written meanwhile are picked up by the final delta in verification.
* The old routing version is retired only after every unit of the migration is
  switched.  A conflict leaves it live.
"""

import contextlib
import json
import threading

from .router import ALGORITHM, Router, shard_for
from .store import ShardStore, content_customer, content_order, hash_content, utcnow

UNIT_PENDING = "planned"
UNIT_COPYING = "copying"
UNIT_INTERRUPTED = "interrupted"
UNIT_COPIED = "copied"
UNIT_VERIFIED = "verified"
UNIT_SWITCHED = "switched"
UNIT_CONFLICT = "conflict"
UNIT_ACTIVE = (UNIT_PENDING, UNIT_COPYING, UNIT_INTERRUPTED, UNIT_COPIED, UNIT_VERIFIED)


class EngineError(Exception):
    pass


class Engine:
    def __init__(self, data_dir: str):
        self.store = ShardStore(data_dir, shard_count=2)
        self._locks: dict[str, threading.RLock] = {}
        self._locks_guard = threading.Lock()

    # ------------------------------------------------------------- locking
    def _customer_lock(self, customer_id: str) -> threading.RLock:
        with self._locks_guard:
            return self._locks.setdefault(customer_id, threading.RLock())

    @contextlib.contextmanager
    def _hold(self, customer_id: str):
        with self._customer_lock(customer_id):
            yield

    # -------------------------------------------------------------- helpers
    def _event(self, kind: str, detail: str, migration_id=None, customer_id=None):
        with self.store.transaction(0) as conn:
            conn.execute(
                "INSERT INTO events(at, migration_id, customer_id, kind, detail)"
                " VALUES (?,?,?,?,?)",
                (utcnow(), migration_id, customer_id, kind, detail))

    def versions(self) -> list[dict]:
        with self.store.connect(0) as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM router_versions ORDER BY version")]
        return rows

    def router(self) -> Router:
        return Router(self.versions())

    def live_version(self) -> int:
        with self.store.connect(0) as conn:
            row = conn.execute(
                "SELECT MAX(version) FROM router_versions WHERE status='live'"
            ).fetchone()
        if row[0] is None:
            raise EngineError("no live routing version")
        return row[0]

    def _binding(self, conn, customer_id: str) -> dict | None:
        row = conn.execute("SELECT * FROM bindings WHERE customer_id=?",
                           (customer_id,)).fetchone()
        return dict(row) if row else None

    def owner(self, customer_id: str) -> dict:
        """Authoritative location: the binding.  Every query/write uses this."""
        with self.store.connect(0) as conn:
            binding = self._binding(conn, customer_id)
        if binding is None:
            raise KeyError(customer_id)
        return {"shard": binding["shard"], "version": binding["version"],
                "generation": binding["generation"], "binding": binding}

    def hash_positions(self, customer_id: str) -> dict:
        out = {}
        for v in self.versions():
            out[str(v["version"])] = shard_for(customer_id, v["shard_count"])
        return out

    # --------------------------------------------------------------- seeding
    def seed(self, reset: bool = False) -> dict:
        if reset:
            self.store.reset(2)
        if self.store.shard_counts()[0]["customers"] + self.store.shard_counts()[1]["customers"] > 0:
            return {"seeded": False, "customers": self.customer_count()}
        live = self.live_version()
        shard_count = self.router().shard_count(live)
        names = ["远山", "青禾", "北岸", "拾光", "木棉", "星野", "云栖", "南乔", "长川",
                 "白露", "听风", "屿声", "小满", "行舟", "望舒", "林间", "朝雾", "叙白"]
        total_orders = 0
        for i in range(1, 19):
            cid = f"CUST-{i:03d}"
            shard = shard_for(cid, shard_count)
            orders = []
            for n in range(1, 3 + i % 3):
                orders.append({
                    "order_id": f"ORD-{i:03d}-{n:02d}",
                    "customer_id": cid,
                    "amount_cents": 800 + (i * 173 + n * 97) % 12600,
                    "created_at": f"2026-10-{1 + (i * 3 + n) % 27:02d}T"
                                  f"{8 + (i + n) % 10:02d}:{(n * 11) % 60:02d}:00+00:00",
                    "note": "初始化订单",
                    "routed_version": live,
                })
            with self.store.transaction(shard) as conn:
                conn.execute(
                    "INSERT INTO customers(customer_id, name, routed_version, content_hash)"
                    " VALUES (?,?,?,?)", (cid, f"{names[i - 1]}商贸", live, ""))
                for order in orders:
                    self.store.insert_order_row(conn, order)
                    self.store.log_write(conn, cid, order["order_id"], "seed",
                                         order["created_at"])
                    total_orders += 1
                self.store.refresh_hash(conn, cid)
            with self.store.transaction(0) as conn:
                conn.execute(
                    "INSERT INTO bindings(customer_id, shard, version, generation,"
                    " previous_shard, handoff_at, updated_at) VALUES (?,?,?,?,?,?,?)",
                    (cid, shard, live, 1, None, None, utcnow()))
        self._event("seed", f"初始化 18 位客户 / {total_orders} 笔订单，路由版本 v{live}")
        return {"seeded": True, "customers": 18, "orders": total_orders}

    def customer_count(self) -> int:
        return sum(c["customers"] for c in self.store.shard_counts())

    # --------------------------------------------------------------- queries
    def query(self, customer_id: str, mode: str = "owner", tax: bool = True) -> dict:
        """mode=owner honors bindings; raw_vN is a diagnostic direct read of the
        location that version's hash alone would choose (never used to serve
        regular traffic)."""
        with self._hold(customer_id):
            with self.store.connect(0) as conn:
                binding = self._binding(conn, customer_id)
            if binding is None:
                raise KeyError(customer_id)
            hash_positions = self.hash_positions(customer_id)
            if mode == "owner":
                shard = binding["shard"]
                snap = self.store.read_snapshot(shard, customer_id)
                version = binding["version"]
                basis = "binding"
                note = (f"按绑定读取：分片 {shard}，路由版本 v{version}"
                        f"（第 {binding['generation']} 代）")
            elif mode.startswith("raw_v"):
                version = int(mode.split("_v")[1])
                shard_count = self.router().shard_count(version)
                shard = shard_for(customer_id, shard_count)
                snap = self.store.read_snapshot(shard, customer_id)
                basis = "raw-hash"
                owner_here = binding["shard"] == shard
                note = (f"诊断直读 v{version} 哈希位置分片 {shard}；"
                        + ("与所有者一致" if owner_here else
                           f"不是所有者（所有者为分片 {binding['shard']}），仅供对照"))
            else:
                raise EngineError("mode must be owner or raw_vN")
            if hash_positions and str(version) not in hash_positions:
                raise EngineError(f"没有路由版本 v{version}")
            presence = self.store.physical_presence(customer_id)
            unit = self._unit_for_customer(customer_id)
            context = self._unit_context(customer_id)
            result = {
                "customer_id": customer_id,
                "mode": mode,
                "found": snap["customer"] is not None,
                "customer": snap["customer"],
                "orders": [content_order(o) for o in snap["orders"]],
                "orders_count": len(snap["orders"]),
                "total_cents": sum(o["amount_cents"] for o in snap["orders"]),
                "shard": shard,
                "version": version,
                "basis": basis,
                "note": note,
                "hash_positions": hash_positions,
                "owner_shard": binding["shard"],
                "owner_version": binding["version"],
                "generation": binding["generation"],
                "handoff_at": binding["handoff_at"],
                "presence": presence,
                "unit": unit,
                "context": context,
            }
        if tax:
            with self.store.transaction(0) as conn:
                conn.execute(
                    "INSERT INTO query_log(at, customer_id, mode, owner_shard,"
                    " owner_version, hash_shard, found, orders_count, total_cents,"
                    " context) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (utcnow(), customer_id, mode, result["owner_shard"],
                     result["owner_version"], shard, int(result["found"]),
                     result["orders_count"], result["total_cents"], context))
        return result

    def _unit_for_customer(self, customer_id: str) -> dict | None:
        with self.store.connect(0) as conn:
            row = conn.execute(
                "SELECT * FROM migration_units WHERE customer_id=? ORDER BY id DESC"
                " LIMIT 1", (customer_id,)).fetchone()
        return dict(row) if row else None

    def _unit_context(self, customer_id: str) -> str:
        unit = self._unit_for_customer(customer_id)
        if not unit:
            with self.store.connect(0) as conn:
                migration = conn.execute(
                    "SELECT id FROM migrations ORDER BY id DESC LIMIT 1").fetchone()
            if migration:
                return f"迁移 #{migration['id']}：本客户无需搬迁，原地保留"
            return "无迁移任务"
        labels = {
            UNIT_PENDING: "待迁移", UNIT_COPYING: "复制中", UNIT_INTERRUPTED: "已中断",
            UNIT_COPIED: "复制完成（未核验）", UNIT_VERIFIED: "已核验（未切换）",
            UNIT_SWITCHED: "已切换", UNIT_CONFLICT: "冲突已停止",
        }
        return (f"迁移 #{unit['migration_id']} 单元状态："
                f"{labels.get(unit['status'], unit['status'])}")

    def query_log(self, limit: int = 20) -> list[dict]:
        with self.store.connect(0) as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM query_log ORDER BY id DESC LIMIT ?", (limit,))]

    def events(self, limit: int = 60) -> list[dict]:
        with self.store.connect(0) as conn:
            return [dict(r) for r in conn.execute(
                "SELECT * FROM events ORDER BY id DESC LIMIT ?", (limit,))]

    def customers(self) -> list[dict]:
        out = []
        with self.store.connect(0) as conn:
            bindings = {r["customer_id"]: dict(r) for r in conn.execute(
                "SELECT * FROM bindings")}
        for shard in range(self.store.shard_count):
            with self.store.connect(shard) as conn:
                rows = [dict(r) for r in conn.execute(
                    "SELECT customer_id, name, routed_version FROM customers"
                    " ORDER BY customer_id")]
            for row in rows:
                binding = bindings.get(row["customer_id"])
                if binding is None:
                    continue
                # only list customers owned here; retained copies are shown in detail
                if binding["shard"] != shard:
                    continue
                out.append({"customer_id": row["customer_id"], "name": row["name"],
                            "shard": binding["shard"], "version": binding["version"],
                            "generation": binding["generation"]})
        return sorted(out, key=lambda r: r["customer_id"])

    # ---------------------------------------------------------------- writes
    def add_order(self, customer_id: str, amount_cents: int, note: str = "") -> dict:
        amount_cents = int(amount_cents)
        if amount_cents <= 0:
            raise EngineError("金额必须为正整数")
        with self._hold(customer_id):
            with self.store.connect(0) as conn:
                binding = self._binding(conn, customer_id)
            if binding is None:
                raise KeyError(customer_id)
            owner_shard = binding["shard"]
            order = self.store.add_order(owner_shard, customer_id, amount_cents,
                                         note or "页面新增订单", "ui",
                                         binding["version"])
            self._event("write",
                        f"新增订单 {order['order_id']}（{amount_cents} 分）→ 所有者分片 "
                        f"{owner_shard} · v{binding['version']}", customer_id=customer_id)
            return {"order": order, "shard": owner_shard,
                    "version": binding["version"]}

    # ------------------------------------------------------------- plan
    def plan_migration(self, new_version: int | None = None) -> dict:
        live = self.live_version()
        with self.store.connect(0) as conn:
            active = conn.execute(
                "SELECT * FROM migrations WHERE status IN ('running','needs_attention')"
                " ORDER BY id DESC LIMIT 1").fetchone()
        router = self.router()
        new_version = new_version or live + 1
        if new_version <= live:
            raise EngineError("新路由版本必须高于当前 live 版本")
        with self.store.connect(0) as conn:
            existing = conn.execute(
                "SELECT * FROM migrations WHERE old_version=? AND new_version=?",
                (live, new_version)).fetchone()
        if existing:
            return {"migration_id": existing["id"], "reused": True,
                    "message": "相同迁移请求已存在，复用原计划，未复制任何数据",
                    "migration": dict(existing)}
        if active:
            raise EngineError(f"迁移 #{active['id']} 尚未完成，不能创建新迁移")
        old_count = router.shard_count(live)
        new_count = old_count + 1
        self.store.ensure_shards(new_count)
        with self.store.connect(0) as conn:
            bindings = [dict(r) for r in conn.execute(
                "SELECT customer_id, shard FROM bindings ORDER BY customer_id")]
        decisions = []
        for b in bindings:
            old_shard = shard_for(b["customer_id"], old_count)
            new_shard = shard_for(b["customer_id"], new_count)
            decisions.append({
                "customer_id": b["customer_id"], "pinned_shard": b["shard"],
                "old_shard": old_shard, "new_shard": new_shard,
                "moves": new_shard != b["shard"],
                "had_drift": old_shard != b["shard"],
            })
        movers = [d for d in decisions if d["moves"]]
        if not movers:
            raise EngineError("没有客户需要搬迁")
        plan = {
            "old_version": live, "new_version": new_version,
            "old_shard_count": old_count, "new_shard_count": new_count,
            "algorithm": ALGORITHM,
            "customers": decisions,
            "movers": len(movers),
            "stay": len(decisions) - len(movers),
        }
        now = utcnow()
        with self.store.transaction(0) as conn:
            conn.execute(
                "INSERT INTO router_versions(version, algorithm, shard_count, status,"
                " created_at) VALUES (?,?,?,?,?)",
                (new_version, ALGORITHM, new_count, "staging", now))
            cur = conn.execute(
                "INSERT INTO migrations(old_version, new_version, status, plan_json,"
                " created_at) VALUES (?,?,?,?,?)",
                (live, new_version, "running",
                 json.dumps(plan, ensure_ascii=False), now))
            migration_id = cur.lastrowid
            for d in movers:
                conn.execute(
                    "INSERT INTO migration_units(migration_id, customer_id, source_shard,"
                    " target_shard, status, updated_at) VALUES (?,?,?,?,?,?)",
                    (migration_id, d["customer_id"], d["pinned_shard"], d["new_shard"],
                     UNIT_PENDING, now))
        self._event("plan", f"新增分片：路由 v{live}（{old_count} 分片）→ v{new_version}"
                            f"（{new_count} 分片），{len(movers)}/{len(decisions)} 位客户需搬迁",
                    migration_id=migration_id)
        return {"migration_id": migration_id, "reused": False,
                "message": "已生成迁移计划，旧路由保持 live 直到全部单元切换",
                "plan": plan}

    # -------------------------------------------------------------- copying
    def _unit(self, migration_id: int, customer_id: str) -> dict:
        with self.store.connect(0) as conn:
            row = conn.execute(
                "SELECT * FROM migration_units WHERE migration_id=? AND customer_id=?",
                (migration_id, customer_id)).fetchone()
        if row is None:
            raise KeyError(f"unit {migration_id}/{customer_id}")
        unit = dict(row)
        with self.store.connect(0) as conn:
            migration = dict(conn.execute(
                "SELECT old_version, new_version FROM migrations WHERE id=?",
                (migration_id,)).fetchone())
        unit.update(migration)
        return unit

    def _set_unit(self, unit_id: int, **fields):
        unit_id = int(unit_id)
        fields["updated_at"] = utcnow()
        cols = ", ".join(f"{k}=?" for k in fields)
        with self.store.transaction(0) as conn:
            conn.execute(f"UPDATE migration_units SET {cols} WHERE id=?",
                         (*fields.values(), unit_id))

    def _staged_rows(self, conn, migration_id, customer_id):
        return self.store.staging_rows(conn, migration_id, customer_id)

    def stage_rows(self, migration_id: int, customer_id: str,
                   max_new_rows: int | None = None) -> dict:
        """Copy source rows into the target's staging table, resuming from
        already-staged rows.  max_new_rows simulates a stop/fault after N rows.

        The copy is *purely additive*: a row already in staging is never
        rewritten.  If it differs from the source we stop as a conflict and
        keep both copies, so a later "last write" cannot paper over a split."""
        unit = self._unit(migration_id, customer_id)
        if unit["status"] == UNIT_SWITCHED:
            return {"status": UNIT_SWITCHED, "customer_id": customer_id,
                    "staged_new": 0,
                    "staged_total": self._staged_count(migration_id, customer_id),
                    "note": "已切换，重复复制请求未写入任何新行"}
        if unit["status"] == UNIT_VERIFIED:
            return {"status": UNIT_VERIFIED, "customer_id": customer_id,
                    "staged_new": 0,
                    "staged_total": self._staged_count(migration_id, customer_id),
                    "note": "已核验，重复复制请求未写入任何新行"}
        if unit["status"] not in (UNIT_PENDING, UNIT_COPYING, UNIT_INTERRUPTED,
                                  UNIT_COPIED):
            raise EngineError("该单元当前不能复制")
        target = unit["target_shard"]
        source = unit["source_shard"]
        if unit["status"] in (UNIT_PENDING, UNIT_INTERRUPTED):
            self._set_unit(unit["id"], status=UNIT_COPYING)
            self._event("copy",
                        f"开始{'续传' if unit['status'] == UNIT_INTERRUPTED else '复制'}："
                        f"分片 {source} → {target}", migration_id, customer_id)
        snapshot = self.store.read_snapshot(source, customer_id)
        if snapshot["customer"] is None:
            self._fail(unit, "源分片不存在该客户")
        source_rows = [("customer", customer_id,
                        content_customer(snapshot["customer"]), 0)]
        for order in snapshot["orders"]:
            source_rows.append(("order", order["order_id"], content_order(order),
                                order["write_seq"]))
        budget = max_new_rows
        staged_new = 0
        interrupted = False
        copy_seq = unit["copy_seq"] or 0
        conflict = None
        for kind, key, payload, seq in source_rows:
            with self.store.transaction(target) as conn:
                existing = self.store.staging_row(conn, migration_id, customer_id,
                                                  kind, key)
                if existing is not None:
                    if json.loads(existing["payload"]) != payload:
                        # Cannot record the conflict here: this transaction
                        # already holds the write lock on the target, which may
                        # itself be control shard 0.  Defer until it closes.
                        conflict = {
                            "kind": kind, "key": key, "source": payload,
                            "target": json.loads(existing["payload"]),
                        }
                        break
                    continue
                conn.execute(
                    "INSERT INTO migration_copy(migration_id, customer_id, kind,"
                    " row_key, payload, source_seq) VALUES (?,?,?,?,?,?)",
                    (migration_id, customer_id, kind, key,
                     json.dumps(payload, sort_keys=True, ensure_ascii=False), seq))
                state = self.store.staging_state(conn, migration_id, customer_id)
                if state is None:
                    conn.execute(
                        "INSERT INTO migration_copy_state(migration_id, customer_id,"
                        " copy_seq, copied_rows, updated_at) VALUES (?,?,?,?,?)",
                        (migration_id, customer_id, max(seq, copy_seq), 1, utcnow()))
                else:
                    conn.execute(
                        "UPDATE migration_copy_state SET copy_seq=?, copied_rows=?,"
                        " updated_at=? WHERE migration_id=? AND customer_id=?",
                        (max(state["copy_seq"], seq), state["copied_rows"] + 1,
                         utcnow(), migration_id, customer_id))
                copy_seq = max(copy_seq, seq)
            if conflict is not None:
                break
            staged_new += 1
            if budget is not None and staged_new >= budget:
                interrupted = True
                break
        if conflict is not None:
            self._conflict(
                unit, "copy", conflict["kind"], conflict["key"],
                source_json=conflict["source"],
                target_json=conflict["target"],
                detail="续传时发现已暂存行与源内容不同，停止该单元")
            self._after_unit_change(migration_id)
            return {"status": UNIT_CONFLICT, "customer_id": customer_id}
        if interrupted:
            self._set_unit(unit["id"], status=UNIT_INTERRUPTED, copy_seq=copy_seq)
            self._event("interrupt",
                        f"复制在已确认 {self._staged_count(migration_id, customer_id)} 行后中断；"
                        f"续传将从暂存表继续", migration_id, customer_id)
            return {"status": UNIT_INTERRUPTED, "customer_id": customer_id,
                    "staged_new": staged_new,
                    "staged_total": self._staged_count(migration_id, customer_id)}
        self._set_unit(unit["id"], status=UNIT_COPIED, copy_seq=copy_seq)
        self._event("copied",
                    f"复制完成，暂存 {self._staged_count(migration_id, customer_id)} 行",
                    migration_id, customer_id)
        return {"status": UNIT_COPIED, "customer_id": customer_id,
                "staged_new": staged_new,
                "staged_total": self._staged_count(migration_id, customer_id)}

    def _staged_count(self, migration_id, customer_id) -> int:
        with self.store.connect(self._unit(migration_id, customer_id)["target_shard"]) as conn:
            return conn.execute(
                "SELECT COUNT(*) FROM migration_copy WHERE migration_id=? AND customer_id=?",
                (migration_id, customer_id)).fetchone()[0]

    def _fail(self, unit, detail):
        self._set_unit(unit["id"], status=UNIT_CONFLICT, detail=detail)
        self._event("error", detail, unit["migration_id"], unit["customer_id"])
        raise EngineError(detail)

    def _conflict(self, unit, phase, table_name, row_key, source_json, target_json,
                  detail):
        self._set_unit(unit["id"], status=UNIT_CONFLICT, detail=detail)
        with self.store.transaction(0) as conn:
            conn.execute(
                "INSERT INTO unit_conflicts(migration_id, customer_id, table_name,"
                " row_key, source_json, target_json, detail, created_at, phase)"
                " VALUES (?,?,?,?,?,?,?,?,?)",
                (unit["migration_id"], unit["customer_id"], table_name, row_key,
                 json.dumps(source_json, sort_keys=True, ensure_ascii=False),
                 json.dumps(target_json, sort_keys=True, ensure_ascii=False),
                 detail, utcnow(), phase))
            conn.execute(
                "INSERT INTO events(at, migration_id, customer_id, kind, detail)"
                " VALUES (?,?,?,?,?)",
                (utcnow(), unit["migration_id"], unit["customer_id"], "conflict",
                 f"冲突：{table_name}/{row_key} · {detail}；两边证据均已保留，未覆盖"))
        return UNIT_CONFLICT

    # ------------------------------------------------------------- verify
    def _reconcile_and_compare(self, unit) -> dict:
        """Caller must hold the customer lock.  Stage any rows written to the
        source since the last copy, then compare staging to the source."""
        mid, cid = unit["migration_id"], unit["customer_id"]
        source, target = unit["source_shard"], unit["target_shard"]
        snap = self.store.read_snapshot(source, cid)
        if snap["customer"] is None:
            self._fail(unit, "源分片不存在该客户")
        source_rows = [("customer", cid, content_customer(snap["customer"]), 0)]
        for order in snap["orders"]:
            source_rows.append(("order", order["order_id"], content_order(order),
                                order["write_seq"]))
        source_map = {(kind, key): payload for kind, key, payload, _ in source_rows}
        with self.store.transaction(target) as conn:
            staged = self._staged_rows(conn, mid, cid)
            staged_map = {(r["kind"], r["row_key"]): json.loads(r["payload"])
                          for r in staged}
            added = 0
            for kind, key, payload, seq in source_rows:
                if (kind, key) in staged_map:
                    continue
                conn.execute(
                    "INSERT INTO migration_copy(migration_id, customer_id, kind,"
                    " row_key, payload, source_seq) VALUES (?,?,?,?,?,?)",
                    (mid, cid, kind, key,
                     json.dumps(payload, sort_keys=True, ensure_ascii=False), seq))
                staged_map[(kind, key)] = payload
                added += 1
            state = self.store.staging_state(conn, mid, cid)
            if state:
                conn.execute(
                    "UPDATE migration_copy_state SET copy_seq=?, copied_rows=?,"
                    " updated_at=? WHERE migration_id=? AND customer_id=?",
                    (max([s for _, _, _, s in source_rows] + [state["copy_seq"]]),
                     len(staged_map), utcnow(), mid, cid))
        missing = [k for k in source_map if k not in staged_map]
        extra = [k for k in staged_map if k not in source_map]
        differing = [k for k in source_map if k in staged_map
                     and staged_map[k] != source_map[k]]
        source_orders = [p for (kind, _), p in source_map.items() if kind == "order"]
        staged_orders = [p for (kind, _), p in staged_map.items() if kind == "order"]
        source_hash = hash_content(
            snap["customer"],
            [content_order(o) for o in snap["orders"]])
        staged_hash = hash_content(
            source_map.get(("customer", cid)),
            staged_orders)
        equal = not missing and not extra and not differing and \
            len(source_orders) == len(staged_orders) and source_hash == staged_hash
        result = {
            "equal": equal,
            "source_customers": 1 if snap["customer"] else 0,
            "source_orders": len(snap["orders"]),
            "staged_customers": 1 if ("customer", cid) in staged_map else 0,
            "staged_orders": len(staged_orders),
            "source_hash": source_hash,
            "staged_hash": staged_hash,
            "missing": [f"{k}/{key}" for k, key in missing],
            "extra": [f"{k}/{key}" for k, key in extra],
            "differing": [f"{k}/{key}" for k, key in differing],
            "delta_added": added,
        }
        return {"result": result, "source_map": source_map, "staged_map": staged_map,
                "snapshot": snap}

    def _record_check(self, unit, phase: str, result: dict, detail: str = ""):
        with self.store.transaction(0) as conn:
            conn.execute(
                "INSERT INTO unit_checks(migration_id, customer_id, phase, checked_at,"
                " source_customers, source_orders, target_customers, target_orders,"
                " source_hash, target_hash, equal, detail)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (unit["migration_id"], unit["customer_id"], phase, utcnow(),
                 result["source_customers"], result["source_orders"],
                 result["staged_customers"], result["staged_orders"],
                 result["source_hash"], result["staged_hash"],
                 int(result["equal"]), detail or (
                     f"源 {result['source_orders']} 单 / 暂存 {result['staged_orders']} 单；"
                     f"指纹 {'一致' if result['source_hash'] == result['staged_hash'] else '不一致'}")))

    def verify(self, migration_id: int, customer_id: str,
               do_switch: bool = False) -> dict:
        """Verify (and optionally switch) under the customer lock, so writes can
        neither slip in after the snapshot nor be routed to two owners."""
        with self._hold(customer_id):
            unit = self._unit(migration_id, customer_id)
            if unit["status"] == UNIT_SWITCHED:
                return {"status": UNIT_SWITCHED, "customer_id": customer_id,
                        "note": "已切换，重复请求不复制数据"}
            if unit["status"] in (UNIT_PENDING, UNIT_COPYING):
                raise EngineError("复制尚未完成，不能核验")
            # An interrupted unit may still be verified: reconciliation stages
            # whatever is still missing on the source, and verification passes
            # only when staging then matches the source exactly.
            if unit["status"] == UNIT_CONFLICT:
                raise EngineError("该单元因冲突停止，需先处理冲突")
            reconciliation = self._reconcile_and_compare(unit)
            result = reconciliation["result"]
            if not result["equal"]:
                detail = (f"数量/内容不一致：缺 {len(result['missing'])}、"
                          f"多 {len(result['extra'])}、异 {len(result['differing'])}")
                self._record_check(unit, "verify", result, detail + "；未切换")
                first_bad = (result["missing"] + result["extra"] + result["differing"])
                table_name, row_key = (first_bad[0].split("/", 1)
                                       if first_bad else ("customer", customer_id))
                self._conflict(unit, "verify", table_name, row_key,
                               source_json=reconciliation["source_map"].get(
                                   (table_name, row_key)),
                               target_json=reconciliation["staged_map"].get(
                                   (table_name, row_key)),
                               detail=detail + "；保留两边内容并停止该单元，未切换")
                self._after_unit_change(migration_id)
                return {"status": UNIT_CONFLICT, "customer_id": customer_id,
                        "check": result}
            self._record_check(unit, "verify", result)
            if not do_switch:
                self._set_unit(unit["id"], status=UNIT_VERIFIED, verified_at=utcnow())
                self._event("verify",
                            f"核验通过：客户 {result['staged_customers']}/{result['source_customers']}，"
                            f"订单 {result['staged_orders']}/{result['source_orders']}，"
                            f"内容指纹一致", migration_id, customer_id)
                return {"status": UNIT_VERIFIED, "customer_id": customer_id,
                        "check": result}
            return self._do_switch(unit, result, reconciliation)
    def _do_switch(self, unit, result, reconciliation) -> dict:
        mid, cid = unit["migration_id"], unit["customer_id"]
        source, target = unit["source_shard"], unit["target_shard"]
        handoff = utcnow()
        customer = reconciliation["source_map"].get(("customer", cid))
        orders = [p for (kind, _), p in sorted(reconciliation["staged_map"].items())
                  if kind == "order"]
        # 1) materialize the target from staging (idempotent delete+insert of
        #    this customer's rows only) - before the binding can point here.
        with self.store.transaction(target) as conn:
            conn.execute("DELETE FROM orders WHERE customer_id=?", (cid,))
            conn.execute(
                "INSERT OR REPLACE INTO customers(customer_id, name, routed_version,"
                " content_hash) VALUES (?,?,?,?)",
                (cid, customer["name"], unit["new_version"], ""))
            for order in orders:
                order = dict(order)
                order["routed_version"] = unit["new_version"]
                self.store.insert_order_row(conn, order)
            self.store.refresh_hash(conn, cid)
        # 2) atomic ownership flip: binding + unit status in one transaction.
        with self.store.transaction(0) as conn:
            binding = self._binding(conn, cid)
            if binding is None or binding["shard"] != source \
                    or binding["version"] != unit["old_version"]:
                raise EngineError("切换前绑定已变化，拒绝切换")
            conn.execute(
                "UPDATE bindings SET shard=?, version=?, generation=?,"
                " previous_shard=?, handoff_at=?, updated_at=? WHERE customer_id=?",
                (target, unit["new_version"], binding["generation"] + 1, source,
                 handoff, handoff, cid))
            conn.execute(
                "UPDATE migration_units SET status=?, verified_at=?, switched_at=?,"
                " handoff_at=?, detail=?, updated_at=? WHERE id=?",
                (UNIT_SWITCHED, utcnow(), handoff, handoff,
                 f"已切换：写入归属 分片 {target} · v{unit['new_version']}",
                 utcnow(), unit["id"]))
            conn.execute(
                "INSERT INTO events(at, migration_id, customer_id, kind, detail)"
                " VALUES (?,?,?,?,?)",
                (handoff, mid, cid, "switch",
                 f"绑定原子切换 分片 {source} → {target}，v{unit['old_version']} →"
                 f" v{unit['new_version']}；交接时刻之后写入只走分片 {target}"))
        # 3) post-switch check: the materialized target equals the verified staging.
        target_snap = self.store.read_snapshot(target, cid)
        target_hash = hash_content(target_snap["customer"],
                                   [content_order(o) for o in target_snap["orders"]])
        target_result = {
            "source_customers": result["staged_customers"],
            "source_orders": result["staged_orders"],
            "staged_customers": 1 if target_snap["customer"] else 0,
            "staged_orders": len(target_snap["orders"]),
            "source_hash": result["staged_hash"],
            "staged_hash": target_hash,
            "equal": target_hash == result["staged_hash"],
        }
        self._record_check(unit, "switch", target_result,
                           "切换后核对：目标实表 vs 已核验暂存；"
                           + ("一致" if target_result["equal"] else "不一致"))
        self._after_unit_change(mid)
        return {"status": UNIT_SWITCHED, "customer_id": cid,
                "handoff_at": handoff, "check": target_result, "source": source,
                "target": target, "version": unit["new_version"]}

    # --------------------------------------------------------------- cleanup
    def cleanup_all(self, migration_id: int) -> dict:
        results = []
        for unit in self.units(migration_id):
            if unit["status"] == UNIT_SWITCHED and not unit["cleaned_at"]:
                results.append(self.cleanup(migration_id, unit["customer_id"]))
        return {"cleaned": len([r for r in results if r["cleaned"]]),
                "skipped": sum(0 if r["cleaned"] else 1 for r in results)}

    def cleanup(self, migration_id: int, customer_id: str) -> dict:
        with self._hold(customer_id):
            unit = self._unit(migration_id, customer_id)
            if unit["status"] != UNIT_SWITCHED:
                raise EngineError("只有已切换的单元可以清理旧副本")
            if unit["cleaned_at"]:
                return {"cleaned": False, "note": "旧副本已清理，重复请求无变化"}
            with self.store.connect(0) as conn:
                binding = self._binding(conn, customer_id)
            if binding["shard"] != unit["target_shard"]:
                raise EngineError("绑定不在目标分片，拒绝清理")
            target = self.store.read_snapshot(unit["target_shard"], customer_id)
            before = len(target["orders"])
            removed = self.store.delete_customer_data(unit["source_shard"], customer_id)
            after = self.store.read_snapshot(unit["target_shard"], customer_id)
            result = {
                "source_customers": 0, "source_orders": 0,
                "staged_customers": 1 if after["customer"] else 0,
                "staged_orders": len(after["orders"]),
                "source_hash": None,
                "staged_hash": after["hash"],
                "equal": before == len(after["orders"]) and after["customer"] is not None,
            }
            self._record_check(unit, "cleanup", result,
                               f"清理源分片 {unit['source_shard']} 保留副本 "
                               f"{removed} 单；目标仍为 {after['orders'].__len__()} 单")
            self._set_unit(unit["id"], cleaned_at=utcnow())
            self._event("cleanup",
                        f"清理旧副本：分片 {unit['source_shard']} 删除 {removed} 单；"
                        f"所有者分片 {unit['target_shard']} 数据未动",
                        migration_id, customer_id)
            return {"cleaned": True, "removed_orders": removed,
                    "target_orders": len(after["orders"]), "check": result}

    # --------------------------------------------------------------- driving
    def run(self, migration_id: int, stop_at_interrupt: bool = True) -> dict:
        units = self.units(migration_id)
        steps = []
        for unit in units:
            if unit["status"] == UNIT_SWITCHED:
                continue
            if unit["status"] == UNIT_CONFLICT:
                steps.append({"customer_id": unit["customer_id"], "status": UNIT_CONFLICT})
                continue
            if unit["status"] in (UNIT_PENDING, UNIT_COPYING, UNIT_INTERRUPTED):
                res = self.stage_rows(migration_id, unit["customer_id"])
                steps.append({"customer_id": unit["customer_id"],
                              "status": res["status"], "step": "copy"})
                if res["status"] == UNIT_INTERRUPTED and stop_at_interrupt:
                    break
                if res["status"] == UNIT_CONFLICT:
                    break
            res = self.verify(migration_id, unit["customer_id"], do_switch=True)
            steps.append({"customer_id": unit["customer_id"], "status": res["status"],
                          "step": "verify+switch"})
            if res["status"] == UNIT_CONFLICT:
                break
        self._after_unit_change(migration_id)
        return {"migration": self.migration_detail(migration_id)["migration"],
                "steps": steps}

    def _after_unit_change(self, migration_id: int):
        units = self.units(migration_id)
        with self.store.connect(0) as conn:
            migration = dict(conn.execute("SELECT * FROM migrations WHERE id=?",
                                          (migration_id,)).fetchone())
        if any(u["status"] == UNIT_CONFLICT for u in units):
            if migration["status"] != "needs_attention":
                with self.store.transaction(0) as conn:
                    conn.execute(
                        "UPDATE migrations SET status='needs_attention', finished_at=?"
                        " WHERE id=?", (utcnow(), migration_id))
                self._event("attention", "存在冲突单元，迁移停止；旧路由保持 live，未退役",
                            migration_id)
            return
        if all(u["status"] == UNIT_SWITCHED for u in units):
            self._finalize(migration_id)

    def _finalize(self, migration_id: int):
        with self.store.connect(0) as conn:
            migration = dict(conn.execute("SELECT * FROM migrations WHERE id=?",
                                          (migration_id,)).fetchone())
        if migration["status"] == "completed":
            return
        if migration["status"] == "needs_attention":
            return
        now = utcnow()
        # Phase 1 (control shard): advance version pointers for non-movers only
        # after every unit has switched.  Do not touch data shards inside this
        # transaction - shard 0 is itself a data shard for some customers and a
        # second write connection would deadlock on the BEGIN IMMEDIATE.
        with self.store.transaction(0) as conn:
            pending = conn.execute(
                "SELECT COUNT(*) FROM migration_units WHERE migration_id=? AND status!=?",
                (migration_id, UNIT_SWITCHED)).fetchone()[0]
            if pending:
                return
            # non-movers keep their shard, only the version pointer advances
            moved = {r["customer_id"] for r in conn.execute(
                "SELECT customer_id FROM migration_units WHERE migration_id=?",
                (migration_id,))}
            upgraded = 0
            upgrades = []
            for row in conn.execute("SELECT customer_id, shard, generation FROM bindings"):
                if row["customer_id"] in moved:
                    continue
                conn.execute(
                    "UPDATE bindings SET version=?, generation=?, updated_at=?"
                    " WHERE customer_id=?",
                    (migration["new_version"], row["generation"] + 1, now,
                     row["customer_id"]))
                upgrades.append((row["shard"], row["customer_id"]))
                upgraded += 1
            conn.execute(
                "UPDATE router_versions SET status='live' WHERE version=?",
                (migration["new_version"],))
            conn.execute(
                "UPDATE router_versions SET status='retired', retired_at=?"
                " WHERE version=? AND status!='retired'",
                (now, migration["old_version"]))
            conn.execute(
                "UPDATE migrations SET status='completed', finished_at=? WHERE id=?",
                (now, migration_id))
            conn.execute(
                "INSERT INTO events(at, migration_id, kind, detail) VALUES (?,?,?,?)",
                (now, migration_id, "retire",
                 f"全部单元已切换：v{migration['new_version']} 设为 live，"
                 f"v{migration['old_version']} 退役；{upgraded} 位未搬迁客户原地升级绑定版本"))
        # Phase 2 (data shards): mirror the version stamp onto the retained rows.
        by_shard = {}
        for shard, customer_id in upgrades:
            by_shard.setdefault(shard, []).append(customer_id)
        for shard, customer_ids in by_shard.items():
            with self.store.transaction(shard) as shard_conn:
                for customer_id in customer_ids:
                    shard_conn.execute(
                        "UPDATE customers SET routed_version=? WHERE customer_id=?",
                        (migration["new_version"], customer_id))
                    shard_conn.execute(
                        "UPDATE orders SET routed_version=? WHERE customer_id=?",
                        (migration["new_version"], customer_id))
        self._event("complete", f"迁移 #{migration_id} 完成", migration_id)

    # --------------------------------------------------------------- reports
    def units(self, migration_id: int) -> list[dict]:
        with self.store.connect(0) as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM migration_units WHERE migration_id=? ORDER BY id",
                (migration_id,))]
            checks = [dict(r) for r in conn.execute(
                "SELECT * FROM unit_checks WHERE migration_id=? ORDER BY id",
                (migration_id,))]
        by_unit: dict[str, list] = {}
        for check in checks:
            by_unit.setdefault(check["customer_id"], []).append(check)
        with self.store.connect(0) as conn:
            migration = dict(conn.execute(
                "SELECT old_version, new_version FROM migrations WHERE id=?",
                (migration_id,)).fetchone())
        for row in rows:
            row.update(migration)
            row["checks"] = by_unit.get(row["customer_id"], [])
            row["staged_rows"] = self._staged_count(migration_id, row["customer_id"])
            snap = self.store.read_snapshot(row["source_shard"], row["customer_id"])
            row["source_present"] = snap["customer"] is not None
            row["source_orders"] = len(snap["orders"])
            row["source_customers"] = 1 if snap["customer"] else 0
        return rows

    def migration_detail(self, migration_id: int) -> dict:
        with self.store.connect(0) as conn:
            row = conn.execute("SELECT * FROM migrations WHERE id=?",
                               (migration_id,)).fetchone()
            if row is None:
                raise KeyError(migration_id)
            migration = dict(row)
            conflicts = [dict(r) for r in conn.execute(
                "SELECT * FROM unit_conflicts WHERE migration_id=? ORDER BY id",
                (migration_id,))]
        for c in conflicts:
            c["source"] = json.loads(c["source_json"]) if c["source_json"] else None
            c["target"] = json.loads(c["target_json"]) if c["target_json"] else None
        migration["plan"] = json.loads(migration.pop("plan_json"))
        for unit in self.units(migration_id):
            unit["presence"] = self.store.physical_presence(unit["customer_id"])
            unit["binding"] = self.owner(unit["customer_id"])
        migration["units"] = self.units(migration_id)
        migration["conflicts"] = conflicts
        return {"migration": migration, "units": migration["units"],
                "conflicts": conflicts}

    def migrations(self) -> list[dict]:
        with self.store.connect(0) as conn:
            rows = [dict(r) for r in conn.execute(
                "SELECT * FROM migrations ORDER BY id DESC")]
        for row in rows:
            plan = json.loads(row.pop("plan_json"))
            row["movers"] = plan["movers"]
            row["stay"] = plan["stay"]
            row["old_shard_count"] = plan["old_shard_count"]
            row["new_shard_count"] = plan["new_shard_count"]
        return rows

    def state(self) -> dict:
        versions = self.versions()
        counts = self.store.shard_counts()
        events = self.events(40)
        migrations = self.migrations()
        stats = {
            "live_version": next((v["version"] for v in versions
                                  if v["status"] == "live"), None),
            "staging_version": next((v["version"] for v in versions
                                     if v["status"] == "staging"), None),
            "conflicts": 0,
            "units_total": 0,
            "units_switched": 0,
        }
        with self.store.connect(0) as conn:
            stats["customers"] = conn.execute(
                "SELECT COUNT(*) FROM bindings").fetchone()[0]
            owned_by_shard = {r["shard"]: r["n"] for r in conn.execute(
                "SELECT shard, COUNT(*) AS n FROM bindings GROUP BY shard")}
            bindings = [dict(r) for r in conn.execute(
                "SELECT customer_id, shard FROM bindings")]
            stats["orders_owned"] = sum(
                len(self.store.read_snapshot(b["shard"], b["customer_id"])["orders"])
                for b in bindings)
            stats["conflicts"] = conn.execute(
                "SELECT COUNT(*) FROM unit_conflicts").fetchone()[0]
            if migrations:
                latest = migrations[0]
                stats["units_total"] = conn.execute(
                    "SELECT COUNT(*) FROM migration_units WHERE migration_id=?",
                    (latest["id"],)).fetchone()[0]
                stats["units_switched"] = conn.execute(
                    "SELECT COUNT(*) FROM migration_units WHERE migration_id=?"
                    " AND status=?", (latest["id"], UNIT_SWITCHED)).fetchone()[0]
        return {
            "versions": versions,
            "counts": counts,
            "customers": self.customers(),
            "migrations": migrations,
            "latest_migration": self.migration_detail(migrations[0]["id"])
            if migrations else None,
            "events": events,
            "stats": stats,
            "algorithm": ALGORITHM,
            "shard_files": [self.store.path(i) for i in range(self.store.shard_count)],
            "owned_by_shard": {str(k): v for k, v in owned_by_shard.items()},
            "physical_by_shard": {str(c["shard"]): c for c in counts},
        }

    # -------------------------------------------------------------- conflicts
    def inject_conflict(self, migration_id: int, customer_id: str) -> dict:
        """Demo hook: change an already-staged source order behind the copy's back.

        The staged copy keeps its old value.  The next verification compares the
        two, finds them different, records both verbatim and stops the unit -
        nothing is overwritten."""
        unit = self._unit(migration_id, customer_id)
        if unit["status"] == UNIT_SWITCHED:
            raise EngineError("已切换的单元不能注入冲突")
        if unit["status"] in (UNIT_PENDING, UNIT_COPYING):
            raise EngineError("请先复制该单元的部分行，再注入冲突")
        source = unit["source_shard"]
        with self.store.transaction(source) as conn:
            row = conn.execute(
                "SELECT order_id, amount_cents, note, created_at FROM orders"
                " WHERE customer_id=? ORDER BY created_at, order_id LIMIT 1",
                (customer_id,)).fetchone()
            if row is None:
                raise EngineError("源分片没有订单可改")
            conn.execute("UPDATE orders SET amount_cents=amount_cents+777,"
                         " note=note||' [冲突演练]' WHERE order_id=?",
                         (row["order_id"],))
            self.store.refresh_hash(conn, customer_id)
        with self.store.connect(unit["target_shard"]) as conn:
            staged = self.store.staging_row(conn, migration_id, customer_id,
                                            "order", row["order_id"])
        if staged is None:
            raise EngineError("该订单尚未复制到暂存，请先多复制几行再注入冲突")
        staged_payload = json.loads(staged["payload"])
        source_now = {"order_id": row["order_id"],
                      "customer_id": customer_id,
                      "amount_cents": row["amount_cents"] + 777,
                      "created_at": row["created_at"],
                      "note": (row["note"] or "") + " [冲突演练]"}
        self._set_unit(unit["id"], status=UNIT_COPIED,
                       detail="源数据在复制后被改动（冲突演练），等待核验")
        self._event("dtl", f"冲突演练：源订单 {row['order_id']} 内容被改动；"
                           f"暂存副本保持原值，核验将发现差异、保留两边并停止该单元",
                    migration_id, customer_id)
        return {"injected": True, "order_id": row["order_id"],
                "old_amount": row["amount_cents"],
                "new_amount": row["amount_cents"] + 777,
                "source_now": source_now, "staged_value": staged_payload}

    def audit(self) -> dict:
        """Owner-based integrity: exactly one routable location per customer, no
        orphan orders, every binding resolving to real data, and no order
        duplicated across shards with different contents."""
        problems = []
        notes = []
        customers = self.customers()
        bindings = {}
        with self.store.connect(0) as conn:
            for r in conn.execute("SELECT * FROM bindings"):
                bindings[r["customer_id"]] = dict(r)
        for c in customers:
            b = bindings[c["customer_id"]]
            snap = self.store.read_snapshot(b["shard"], c["customer_id"])
            if snap["customer"] is None:
                problems.append(f"{c['customer_id']} 绑定在分片 {b['shard']} 但该分片无客户")
            elif not snap["orders"]:
                problems.append(f"{c['customer_id']} 在所有者分片没有订单")
            owner_orders = {o["order_id"]: o["amount_cents"] for o in snap["orders"]}
            for presence in self.store.physical_presence(c["customer_id"]):
                if presence["shard"] == b["shard"] or not presence["present"]:
                    continue
                retained = self.store.read_snapshot(presence["shard"],
                                                    c["customer_id"])["orders"]
                if b["version"] > 1:
                    notes.append(f"{c['customer_id']}：分片 {presence['shard']} 保留 "
                                 f"{len(retained)} 单旧副本（已切换，不可路由）")
                for order in retained:
                    if order["amount_cents"] != owner_orders.get(order["order_id"]):
                        problems.append(
                            f"{c['customer_id']}/{order['order_id']}：非所有者分片 "
                            f"{presence['shard']} 与所有者内容不同")
        counts = self.store.shard_counts()
        for shard in counts:
            if shard["orphan_orders"]:
                problems.append(f"分片 {shard['shard']} 有 {shard['orphan_orders']} 条孤儿订单")
        return {"ok": not problems, "problems": problems, "notes": notes,
                "customers": len(customers), "bindings": len(bindings)}
