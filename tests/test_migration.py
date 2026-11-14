"""End-to-end tests for routing, online migration, and fault recovery.

Run: python3 -m unittest discover -s tests -v
"""

import os
import shutil
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.engine import (Engine, EngineError, UNIT_CONFLICT, UNIT_COPIED,
                        UNIT_INTERRUPTED, UNIT_SWITCHED)
from app.router import fnv1a64, shard_for
from app.store import hash_content


class EngineCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="shard-test-")
        self.engine = Engine(self.tmp)
        self.engine.seed(reset=True)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def plan(self):
        res = self.engine.plan_migration()
        return res["migration_id"]

    # ------------------------------------------------------------- routing
    def test_fixed_hash_is_stable_and_versioned(self):
        self.assertEqual(fnv1a64("CUST-001"), fnv1a64("CUST-001"))
        self.assertNotEqual(shard_for("CUST-003", 2), shard_for("CUST-003", 3))
        # same key -> same position; shard count alone changes the modulus
        for cid in ("CUST-001", "CUST-009", "CUST-017"):
            two = shard_for(cid, 2)
            self.assertEqual(two, fnv1a64(cid) % 2)
            self.assertEqual(shard_for(cid, 3), fnv1a64(cid) % 3)

    def test_customer_and_orders_share_one_route(self):
        cid = "CUST-001"
        route = self.engine.query(cid)
        snap = self.engine.store.read_snapshot(route["shard"], cid)
        self.assertEqual({o["customer_id"] for o in snap["orders"]}, {cid})
        for presence in route["presence"]:
            if presence["present"]:
                snap2 = self.engine.store.read_snapshot(presence["shard"], cid)
                self.assertTrue(all(o["customer_id"] == cid
                                    for o in snap2["orders"]))

    def test_plan_only_lists_customers_that_move(self):
        plan = self.engine.plan_migration()["plan"]
        for customer in plan["customers"]:
            self.assertEqual(customer["moves"],
                             customer["new_shard"] != customer["pinned_shard"])
        self.assertEqual(plan["movers"],
                         len([c for c in plan["customers"] if c["moves"]]))

    # ------------------------------------------------------------- happy path
    def test_migration_switches_every_unit_then_retires_old_version(self):
        mid = self.plan()
        self.assertEqual(self.engine.live_version(), 1)
        result = self.engine.run(mid)
        for unit in self.engine.units(mid):
            self.assertEqual(unit["status"], UNIT_SWITCHED)
            self.assertTrue(any(c["phase"] == "verify" and c["equal"] == 1
                                for c in unit["checks"]))
        self.assertEqual(result["migration"]["status"], "completed")
        versions = {v["version"]: v for v in self.engine.versions()}
        self.assertEqual(versions[2]["status"], "live")
        self.assertEqual(versions[1]["status"], "retired")
        self.assertTrue(self.engine.audit()["ok"])

    # -------------------------------------------------- interruption / resume
    def test_interrupted_copy_resumes_without_duplicates(self):
        mid = self.plan()
        unit = self.engine.units(mid)[0]
        cid = unit["customer_id"]
        first = self.engine.stage_rows(mid, cid, max_new_rows=1)
        self.assertEqual(first["status"], UNIT_INTERRUPTED)
        self.assertEqual(first["staged_total"], 1)
        self.assertEqual(self.engine._unit(mid, cid)["status"], UNIT_INTERRUPTED)
        # an order arrives while the copy is paused; it goes to the owner (source)
        write = self.engine.add_order(cid, 4321, "复制中断期间补写")
        self.assertEqual(write["shard"], unit["source_shard"])
        resumed = self.engine.stage_rows(mid, cid)
        self.assertEqual(resumed["status"], UNIT_COPIED)
        # rerunning the copy adds nothing
        again = self.engine.stage_rows(mid, cid)
        self.assertEqual(again["staged_new"], 0)
        self.assertEqual(again["status"], UNIT_COPIED)
        source_snap = self.engine.store.read_snapshot(unit["source_shard"], cid)
        check = self.engine.verify(mid, cid, do_switch=True)
        self.assertEqual(check["status"], UNIT_SWITCHED)
        staged = check["check"]
        # one source-snapshot row count, read outside the verify transaction
        self.assertEqual(staged["source_orders"], len(source_snap["orders"]))
        self.assertEqual(staged["staged_orders"], staged["source_orders"])
        self.assertEqual(staged["source_hash"], staged["staged_hash"])

    def test_writes_before_switch_go_to_source_and_after_switch_to_target(self):
        mid = self.plan()
        unit = next(u for u in self.engine.units(mid)
                    if u["customer_id"] == "CUST-002")
        cid = unit["customer_id"]
        self.engine.stage_rows(mid, cid)
        before = self.engine.add_order(cid, 2000, "切换前")
        self.assertEqual(before["shard"], unit["source_shard"])
        self.engine.verify(mid, cid, do_switch=True)
        after = self.engine.add_order(cid, 3000, "切换后")
        self.assertEqual(after["shard"], unit["target_shard"])
        q = self.engine.query(cid)
        self.assertEqual(q["shard"], unit["target_shard"])
        ids = [o["order_id"] for o in q["orders"]]
        self.assertIn(before["order"]["order_id"], ids)
        self.assertIn(after["order"]["order_id"], ids)
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(q["orders"]), len(self.engine.store.read_snapshot(
            unit["target_shard"], cid)["orders"]))
        reported = self.engine.query(cid)["orders_count"]
        self.assertEqual(reported, len(ids))

    # --------------------------------------------------------------- conflicts
    def test_conflict_stops_unit_keeps_evidence_and_old_route_live(self):
        mid = self.plan()
        unit = self.engine.units(mid)[0]
        cid = unit["customer_id"]
        self.engine.stage_rows(mid, cid, max_new_rows=2)
        injected = self.engine.inject_conflict(mid, cid)
        resumed = self.engine.stage_rows(mid, cid)
        self.assertEqual(resumed["status"], UNIT_CONFLICT)
        detail = self.engine.migration_detail(mid)
        self.assertTrue(detail["conflicts"])
        conflict = detail["conflicts"][0]
        self.assertIsNotNone(conflict["source"])
        self.assertIsNotNone(conflict["target"])
        self.assertNotEqual(conflict["source"], conflict["target"])
        switched = self.engine.query(cid)
        self.assertEqual(switched["shard"], unit["source_shard"])
        self.assertEqual(self.engine.live_version(), 1)
        self.assertEqual(detail["migration"]["status"], "needs_attention")
        self.engine.run(mid)
        self.assertEqual(self.engine.live_version(), 1)

    # ------------------------------------------------------------ idempotence
    def test_repeated_plan_and_run_do_not_copy_twice(self):
        first = self.engine.plan_migration()
        again = self.engine.plan_migration()
        self.assertTrue(again["reused"])
        self.assertEqual(again["migration_id"], first["migration_id"])
        before = self.engine.state()["stats"]["orders_owned"]
        self.engine.run(first["migration_id"])
        after = self.engine.state()["stats"]["orders_owned"]
        # materialising targets adds physical copies, but the owned set is unchanged
        self.assertEqual(after, before)
        second_run = self.engine.run(first["migration_id"])
        self.assertEqual(second_run["steps"], [])
        self.assertEqual(self.engine.state()["stats"]["orders_owned"], after)
        for customer in self.engine.state()["customers"]:
            ids = [o["order_id"] for o in self.engine.query(customer["customer_id"])["orders"]]
            self.assertEqual(len(ids), len(set(ids)))

    def test_stage_rows_on_switched_unit_is_noop(self):
        mid = self.plan()
        unit = self.engine.units(mid)[0]
        cid = unit["customer_id"]
        self.engine.stage_rows(mid, cid)
        self.engine.verify(mid, cid, do_switch=True)
        before = self.engine._staged_count(mid, cid)
        res = self.engine.stage_rows(mid, cid)
        self.assertEqual(res["staged_new"], 0)
        self.assertEqual(self.engine._staged_count(mid, cid), before)

    # --------------------------------------------------------------- cleanup
    def test_cleanup_removes_old_copy_only_after_switch(self):
        mid = self.plan()
        unit = next(u for u in self.engine.units(mid)
                    if u["customer_id"] == "CUST-002")
        cid = unit["customer_id"]
        with self.assertRaises(EngineError):
            self.engine.cleanup(mid, cid)
        self.engine.stage_rows(mid, cid)
        self.engine.verify(mid, cid, do_switch=True)
        target_before = self.engine.store.read_snapshot(unit["target_shard"], cid)
        res = self.engine.cleanup(mid, cid)
        self.assertTrue(res["cleaned"])
        target_after = self.engine.store.read_snapshot(unit["target_shard"], cid)
        self.assertEqual(len(target_before["orders"]), len(target_after["orders"]))
        self.assertEqual(self.engine.store.read_snapshot(
            unit["source_shard"], cid)["customer"], None)
        self.assertFalse(self.engine.cleanup(mid, cid)["cleaned"])
        self.assertEqual(self.engine.query(cid)["orders_count"],
                         len(target_after["orders"]))

    # ------------------------------------------------------- version lifecycle
    def test_old_version_not_retired_while_any_unit_is_unswitched(self):
        mid = self.plan()
        units = self.engine.units(mid)
        for unit in units[:-1]:
            self.engine.stage_rows(mid, unit["customer_id"])
            self.engine.verify(mid, unit["customer_id"], do_switch=True)
        self.assertEqual(self.engine.live_version(), 1)
        versions = {v["version"]: v for v in self.engine.versions()}
        self.assertEqual(versions[1]["status"], "live")
        self.assertEqual(versions[2]["status"], "staging")
        last = units[-1]["customer_id"]
        self.engine.stage_rows(mid, last)
        self.engine.verify(mid, last, do_switch=True)
        self.assertEqual(self.engine.live_version(), 2)
        self.assertEqual({v["version"]: v for v in self.engine.versions()}[1]["status"],
                         "retired")

    def test_unmoved_customers_get_new_version_and_single_owner(self):
        mid = self.plan()
        detail = self.engine.migration_detail(mid)["migration"]["plan"]
        movers = {c["customer_id"] for c in detail["customers"] if c["moves"]}
        self.engine.run(mid)
        for c in self.engine.state()["customers"]:
            self.assertEqual(c["version"], 2)
            if c["customer_id"] not in movers:
                route = self.engine.query(c["customer_id"])
                self.assertEqual(route["shard"], c["shard"])
            presences = [p for p in self.engine.store.physical_presence(
                c["customer_id"]) if p["present"]]
            self.assertTrue(any(p["shard"] == c["shard"] for p in presences))

    def test_hash_content_matches_after_switch(self):
        mid = self.plan()
        unit = self.engine.units(mid)[0]
        cid = unit["customer_id"]
        self.engine.run(mid)
        snap = self.engine.store.read_snapshot(unit["target_shard"], cid)
        self.assertEqual(snap["hash"],
                         hash_content(snap["customer"], snap["orders"]))


if __name__ == "__main__":
    unittest.main(verbosity=2)
