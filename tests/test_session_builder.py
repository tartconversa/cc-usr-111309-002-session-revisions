"""可修订会话事实构建器测试（全部基于合成数据，标准库 unittest）。"""

import json
import random
import tempfile
import threading
import unittest
from pathlib import Path

from src.session_builder import (
    SessionFactStore,
    adjudicate,
    build_components,
    order_active_events,
    reference_rebuild,
)
from src.replay_revisions import replay, write_outputs

ROOT = Path(__file__).resolve().parents[1]


def evt(event_id, device, at, revision=1, withdrawn=False):
    return {
        "event_id": event_id,
        "device": device,
        "event_at_seconds": at,
        "revision": revision,
        "withdrawn": withdrawn,
    }


def feed(store, device, triples):
    """triples: (event_id, at, revision, withdrawn) 简写序列。"""
    entries = []
    for item in triples:
        event_id, at = item[0], item[1]
        revision = item[2] if len(item) > 2 else 1
        withdrawn = item[3] if len(item) > 3 else False
        entries.append(store.apply(evt(event_id, device, at, revision, withdrawn)))
    return entries


def snapshot_keys(store, device, version=None):
    return {
        session["session_key"]: session["member_ids"]
        for session in store.snapshot(device, version)["sessions"]
    }


class GapAndConnectionTests(unittest.TestCase):
    def test_exactly_gap_seconds_connected(self):
        # 恰好 10 秒（<= gap）连接；11 秒断开。
        connected = build_components(
            order_active_events([evt("a", "d", 0), evt("b", "d", 10)]), 10
        )
        self.assertEqual(len(connected), 1)
        disconnected = build_components(
            order_active_events([evt("a", "d", 0), evt("b", "d", 11)]), 10
        )
        self.assertEqual(len(disconnected), 2)

    def test_gap_is_adjacent_interval_not_span(self):
        # 总跨度 20 秒，但每段相邻间隔都 <= 10，仍是一条链一个会话。
        store = SessionFactStore()
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20)])
        snap = store.snapshot("d")
        self.assertEqual(snap["counts"]["sessions"], 1)
        session = snap["sessions"][0]
        self.assertEqual(session["member_ids"], ["a", "b", "c"])
        self.assertEqual(session["duration_seconds"], 20)
        self.assertEqual(snap["counts"]["bridges"], 2)

    def test_same_timestamp_ordered_by_event_id(self):
        store = SessionFactStore()
        feed(store, "d", [("z", 5), ("a", 5), ("m", 5)])
        snap = store.snapshot("d")
        self.assertEqual(len(snap["sessions"]), 1)
        self.assertEqual(snap["sessions"][0]["member_ids"], ["a", "m", "z"])
        # 桥按 (时刻, event_id) 排序产生。
        self.assertEqual(
            [(b["left_event_id"], b["right_event_id"]) for b in snap["bridges"]],
            [("a", "m"), ("m", "z")],
        )

    def test_single_point_duration_zero_is_measurement(self):
        store = SessionFactStore()
        store.apply(evt("solo", "d", 42))
        snap = store.snapshot("d")
        self.assertEqual(snap["counts"]["sessions"], 1)
        self.assertEqual(snap["sessions"][0]["duration_seconds"], 0)
        self.assertEqual(snap["sessions"][0]["member_ids"], ["solo"])
        self.assertEqual(snap["counts"]["bridges"], 0)

    def test_empty_device_creates_no_session(self):
        store = SessionFactStore()
        # 没有任何写入时设备不存在；写后撤回同样不造会话。
        store.apply(evt("a", "d", 0, withdrawn=True))
        self.assertEqual(store.snapshot("d", version=0)["sessions"], [])
        self.assertEqual(store.snapshot("d")["sessions"], [])


class RevisionAdjudicationTests(unittest.TestCase):
    def test_highest_revision_wins(self):
        winner, conflict = adjudicate([
            evt("a", "d", 0, revision=1),
            evt("a", "d", 9, revision=2),
        ])
        self.assertEqual(conflict, ())
        self.assertEqual(winner["event_at_seconds"], 9)

    def test_same_revision_identical_is_idempotent(self):
        store = SessionFactStore()
        first = store.apply(evt("a", "d", 0, revision=1))
        second = store.apply(evt("a", "d", 0, revision=1))
        self.assertTrue(first["bumped"])
        self.assertFalse(second["bumped"])
        self.assertEqual(second["note"], "no_change")
        self.assertEqual(store.latest_version("d"), 1)

    def test_higher_revision_same_content_no_bump(self):
        store = SessionFactStore()
        store.apply(evt("a", "d", 0, revision=1))
        entry = store.apply(evt("a", "d", 0, revision=2))
        self.assertFalse(entry["bumped"], "内容一致的更高 revision 不改变事实")
        self.assertEqual(entry["adjudication"]["winning_revision"], 2)

    def test_tie_conflict_quarantined(self):
        store = SessionFactStore()
        store.apply(evt("a", "d", 0, revision=1))
        entry = store.apply(evt("a", "d", 5, revision=1))
        self.assertEqual(entry["adjudication"]["status"], "quarantined")
        self.assertEqual(len(entry["adjudication"]["quarantined_records"]), 2)
        self.assertTrue(entry["bumped"])
        # 隔离后该设备无有效事件、无会话。
        self.assertEqual(store.snapshot("d")["sessions"], [])
        # 更高 revision 解除隔离。
        entry = store.apply(evt("a", "d", 7, revision=2))
        self.assertEqual(entry["adjudication"]["status"], "active")
        self.assertEqual(store.snapshot("d")["sessions"][0]["started_at"], 7)

    def test_withdrawn_then_revived(self):
        store = SessionFactStore()
        store.apply(evt("a", "d", 0, revision=1))
        store.apply(evt("a", "d", 0, revision=2, withdrawn=True))
        self.assertEqual(store.snapshot("d")["sessions"], [])
        store.apply(evt("a", "d", 3, revision=3))
        snap = store.snapshot("d")
        self.assertEqual(len(snap["sessions"]), 1)
        self.assertEqual(snap["sessions"][0]["started_at"], 3)


class CanonicalKeyTests(unittest.TestCase):
    def test_key_is_lexicographic_min_not_time_first(self):
        store = SessionFactStore()
        # a 在 0、c 在 20，a 键最小；随后加入 z 在 10 桥接，键仍为 a。
        feed(store, "d", [("a", 0), ("c", 20), ("z", 10)])
        self.assertEqual(list(snapshot_keys(store, "d")), ["a"])

    def test_later_time_smaller_id_sets_canonical(self):
        store = SessionFactStore()
        # 先有 z 键会话；后到 a（更晚时刻但字典序更小）合并后 canonical=a。
        store.apply(evt("z", "d", 0))
        store.apply(evt("y", "d", 20))
        self.assertEqual(set(snapshot_keys(store, "d")), {"y", "z"})
        entry = store.apply(evt("a", "d", 10))
        # 稳定键变化显式给 old->new，且不是新增访问。
        changes = {(row["old_key"], row["new_key"], row["change"]) for row in entry["lineage"]}
        self.assertIn(("z", "a", "merged"), changes)
        self.assertIn(("y", "a", "merged"), changes)
        snap = store.snapshot("d")
        self.assertEqual(len(snap["sessions"]), 1)
        self.assertEqual(snap["sessions"][0]["session_key"], "a")
        self.assertEqual(snap["sessions"][0]["member_ids"], ["a", "y", "z"])


class MergeSplitScenarioTests(unittest.TestCase):
    def test_fixture_scenario_two_to_one_to_two(self):
        """题面主场景：a@0、c@20 两会话 -> 补 b@10 合一 -> 撤 b 恢复两会话。"""
        store = SessionFactStore(gap_seconds=10)
        store.apply(evt("a", "d1", 0))
        store.apply(evt("c", "d1", 20))
        v2 = store.snapshot("d1")
        self.assertEqual(v2["counts"]["sessions"], 2)
        self.assertEqual(store.latest_version("d1"), 2)

        merge_entry = store.apply(evt("b", "d1", 10))
        v3 = store.snapshot("d1")
        self.assertEqual(v3["counts"]["sessions"], 1)
        session = v3["sessions"][0]
        self.assertEqual(session["member_ids"], ["a", "b", "c"])
        self.assertEqual(session["started_at"], 0)
        self.assertEqual(session["ended_at"], 20)  # 首尾 20 秒仍合法
        self.assertEqual(v3["counts"]["bridges"], 2)
        self.assertEqual(
            {(row["old_key"], row["change"]) for row in merge_entry["lineage"]},
            {("a", "merged"), ("c", "merged")},
        )

        split_entry = store.apply(evt("b", "d1", 10, revision=2, withdrawn=True))
        v4 = store.snapshot("d1")
        self.assertEqual(v4["counts"]["sessions"], 2)
        self.assertEqual(
            sorted(tuple(s["member_ids"]) for s in v4["sessions"]),
            [("a",), ("c",)],
        )
        self.assertEqual(
            {(row["new_key"], row["change"]) for row in split_entry["lineage"]},
            {("a", "split"), ("c", "split")},
        )
        # 事件数量守恒：有效事件 2，成员总数 2。
        self.assertEqual(v4["counts"]["members_sum"], 2)

    def test_history_versions_remain_queryable(self):
        store = SessionFactStore(gap_seconds=10)
        store.apply(evt("a", "d1", 0))
        store.apply(evt("c", "d1", 20))
        store.apply(evt("b", "d1", 10))
        store.apply(evt("b", "d1", 10, revision=2, withdrawn=True))
        self.assertEqual(store.snapshot("d1", version=0)["sessions"], [])
        self.assertEqual(
            sorted(tuple(s["member_ids"]) for s in store.snapshot("d1", 2)["sessions"]),
            [("a",), ("c",)],
        )
        self.assertEqual(
            [tuple(s["member_ids"]) for s in store.snapshot("d1", 3)["sessions"]],
            [("a", "b", "c")],
        )
        self.assertEqual(
            sorted(tuple(s["member_ids"]) for s in store.snapshot("d1", 4)["sessions"]),
            [("a",), ("c",)],
        )

    def test_late_bridge_merges_long_chain_not_just_one_gap_window(self):
        """迟到点沿相邻间隔扩展到闭合边界：长链整体受影响，不漏掉远端会话。"""
        store = SessionFactStore(gap_seconds=10)
        # 左链 a0-b10-c20，右链 x31-y41，间隔 11 断开。
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20), ("x", 31), ("y", 41)])
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 2)
        stable_fact_id = store.snapshot("d")["sessions"][0]["fact_id"]

        entry = store.apply(evt("m", "d", 25))  # c->m=5, m->x=6，桥接两链
        self.assertEqual(entry["affected_event_ids"],
                         ["a", "b", "c", "m", "x", "y"])
        snap = store.snapshot("d")
        self.assertEqual(snap["counts"]["sessions"], 1)
        self.assertEqual(snap["sessions"][0]["member_ids"],
                         ["a", "b", "c", "m", "x", "y"])
        self.assertNotIn(stable_fact_id, [s["fact_id"] for s in snap["sessions"]])

    def test_unaffected_components_keep_fact_identity(self):
        """闭包外的会话事实保留 fact_id，不被撤销重发。"""
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("b", 10), ("x", 100), ("y", 110)])
        untouched_id = next(
            s["fact_id"] for s in store.snapshot("d")["sessions"]
            if s["session_key"] == "x"
        )
        store.apply(evt("c", "d", 20))  # 只并入左链
        live_ids = {s["fact_id"] for s in store.snapshot("d")["sessions"]}
        self.assertIn(untouched_id, live_ids)

    def test_time_correction_splits_session(self):
        """时间更正把一个会话拆成两个。"""
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20)])
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 1)
        # b 从 10 改到 15：a->b=15 断开，b->c=5 连接。
        entry = store.apply(evt("b", "d", 15, revision=2))
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 2)
        self.assertEqual(
            sorted(tuple(s["member_ids"]) for s in store.snapshot("d")["sessions"]),
            [("a",), ("b", "c")],
        )
        self.assertTrue(
            any(row["change"] == "split" for row in entry["lineage"])
        )

    def test_time_correction_moves_across_chain(self):
        """更正后事件跨过原有位置，旧位置与新位置两侧闭包都要覆盖。"""
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20), ("x", 31), ("y", 41)])
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 2)
        # b 从 10 移到 25：a@0->c@20 间隔 20 断开；c@20->b@25->x@31->y@41 连成一链。
        entry = store.apply(evt("b", "d", 25, revision=2))
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 2)
        self.assertEqual(
            sorted(tuple(s["member_ids"]) for s in store.snapshot("d")["sessions"]),
            [("a",), ("b", "c", "x", "y")],
        )
        # 闭包覆盖了旧位置（a）与新位置两侧（c, x, y），没有遗漏远端链。
        self.assertEqual(entry["affected_event_ids"], ["a", "b", "c", "x", "y"])
        # 桥按 (时刻, event_id) 定向，不允许出现 (c, b) 这种反向键。
        self.assertEqual(
            [(b["left_event_id"], b["right_event_id"])
             for b in store.snapshot("d")["bridges"]],
            [("c", "b"), ("b", "x"), ("x", "y")],
        )

    def test_late_bridge_between_two_chains_joins_all(self):
        """迟到事件落在两条链的间隙中且两侧间隔都 <= gap 时，合并为单条长链。"""
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20), ("x", 31), ("y", 41)])
        self.assertEqual(store.snapshot("d")["counts"]["sessions"], 2)
        entry = store.apply(evt("m", "d", 25))  # c->m=5, m->x=6
        self.assertEqual(entry["affected_event_ids"],
                         ["a", "b", "c", "m", "x", "y"])
        snap = store.snapshot("d")
        self.assertEqual(snap["counts"]["sessions"], 1)
        self.assertEqual(snap["sessions"][0]["member_ids"],
                         ["a", "b", "c", "m", "x", "y"])


    def test_key_changed_one_to_one_is_not_new_visit(self):
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("z", 0), ("y", 10)])  # 会话键 y
        entry = store.apply(evt("a", "d", 5))    # 并入后 canonical 变为 a
        row = next(row for row in entry["lineage"] if row["old_key"] == "y")
        self.assertEqual(row["change"], "key_changed")
        self.assertEqual(row["new_key"], "a")
        self.assertEqual(row["new_member_count"], 3)
        # 没有 created 行：键变化不是新增访问。
        self.assertFalse(any(row["change"] == "created" for row in entry["lineage"]))
        self.assertEqual(store.snapshot("d")["sessions"][0]["session_key"], "a")


class CrossDayTests(unittest.TestCase):
    def test_cross_day_seconds_are_plain_numeric_chain(self):
        # 相对秒数跨日（86395 -> 86405 间隔 10），连接规则与普通秒数一致。
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("p", 86395), ("q", 86405), ("r", 86415)])
        snap = store.snapshot("d")
        self.assertEqual(snap["counts"]["sessions"], 1)
        self.assertEqual(snap["sessions"][0]["member_ids"], ["p", "q", "r"])
        # 渲染层验证跨日绝对时刻：2026-11-12 23:59:55 + 86405s = 11-14 00:00:00。
        from src.replay_revisions import render_time
        self.assertEqual(
            render_time(86405, "2026-11-12T23:59:55+08:00"),
            "2026-11-14T00:00:00+08:00",
        )
        self.assertEqual(
            render_time(86395, "2026-11-12T23:59:55+08:00"),
            "2026-11-13T23:59:50+08:00",
        )


class ArrivalOrderTests(unittest.TestCase):
    def _final_view(self, order):
        store = SessionFactStore(gap_seconds=10)
        for event in order:
            store.apply(dict(event))
        return sorted(
            (s["session_key"], tuple(s["member_ids"]), s["started_at"], s["ended_at"])
            for s in store.snapshot("d")["sessions"]
        )

    def test_different_arrival_orders_converge(self):
        events = [evt("a", "d", 0), evt("b", "d", 10), evt("c", "d", 20), evt("z", "d", 100)]
        orders = [
            events,
            list(reversed(events)),
            [events[2], events[0], events[3], events[1]],
            [events[1], events[3], events[0], events[2]],
        ]
        baseline = self._final_view(orders[0])
        for order in orders[1:]:
            self.assertEqual(self._final_view(order), baseline)

    def test_revision_arrival_order_independent(self):
        sequences = [
            [evt("a", "d", 0, 1), evt("a", "d", 5, 2), evt("a", "d", 5, 1)],
            [evt("a", "d", 5, 1), evt("a", "d", 0, 1), evt("a", "d", 5, 2)],
            [evt("a", "d", 5, 2), evt("a", "d", 0, 1), evt("a", "d", 5, 1)],
        ]
        views = []
        for sequence in sequences:
            store = SessionFactStore(gap_seconds=10)
            for event in sequence:
                store.apply(event)
            views.append(
                sorted((s["session_key"], tuple(s["member_ids"]))
                       for s in store.snapshot("d")["sessions"])
            )
        self.assertEqual(views[0], views[1])
        self.assertEqual(views[1], views[2])


class IncrementalVsReferenceTests(unittest.TestCase):
    def test_randomized_incremental_matches_full_reference(self):
        rng = random.Random(20261113)
        ids = [f"e{i:02d}" for i in range(12)]
        for trial in range(40):
            store = SessionFactStore(gap_seconds=10)
            records_by_id: dict[str, list[dict]] = {}
            arrivals = []
            for event_id in ids:
                revisions = rng.randint(1, 3)
                for revision in range(1, revisions + 1):
                    at = rng.choice([0, 5, 10, 11, 15, 20, 25, 40, 100])
                    withdrawn = rng.random() < 0.25
                    event = evt(event_id, "d", at, revision, withdrawn)
                    arrivals.append(event)
                    records_by_id.setdefault(event_id, []).append(event)
            rng.shuffle(arrivals)
            for event in arrivals:
                entry = store.apply(event)
                # 每次事实变化后，现行事实必须等于对当前全集的全量重算。
                reference = reference_rebuild(
                    store.dump_state()["devices"]["d"]["records"], 10
                )
                snap = store.snapshot("d")
                self.assertEqual(
                    sorted(tuple(s["member_ids"]) for s in snap["sessions"]),
                    sorted(tuple(s["member_ids"]) for s in reference["sessions"]),
                    f"trial {trial} after {event}",
                )
                self.assertEqual(
                    snap["counts"]["members_sum"],
                    reference["counts"]["members_sum"],
                )
            # 最终修订日志累计守恒。
            last = store.revision_log("d")[-1]
            reference = reference_rebuild(records_by_id, 10)
            self.assertEqual(last["counts"], reference["counts"])

    def test_bridge_gaps_recorded_and_revoked(self):
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("b", 10), ("c", 20)])
        bridge_ids = {b["fact_id"] for b in store.snapshot("d")["bridges"]}
        self.assertEqual(len(bridge_ids), 2)
        store.apply(evt("b", "d", 10, revision=2, withdrawn=True))
        # 旧桥必须出现在撤销账本，新会话无桥。
        revoked = {item["fact_id"] for item in store.dump_state()["devices"]["d"]["ledger"]
                   if item["op"] == "revoke" and item["fact_kind"] == "bridge"}
        self.assertTrue(bridge_ids <= revoked)
        self.assertEqual(store.snapshot("d")["counts"]["bridges"], 0)


class MultiDeviceAndConcurrencyTests(unittest.TestCase):
    def test_devices_independent_and_parallel(self):
        store = SessionFactStore(gap_seconds=10)
        events = []
        for device in ("d1", "d2", "d3"):
            events.extend([evt(f"{device}a", device, 0), evt(f"{device}b", device, 10)])
        entries = store.apply_batch(events, parallel=True, max_workers=4)
        self.assertTrue(all(entry["bumped"] for entry in entries))
        for device in ("d1", "d2", "d3"):
            snap = store.snapshot(device)
            self.assertEqual(snap["counts"]["sessions"], 1)
            self.assertEqual(snap["counts"]["members_sum"], 2)

    def test_same_device_atomic_visibility_under_contention(self):
        store = SessionFactStore(gap_seconds=10)
        errors = []

        def writer(prefix):
            try:
                for i in range(50):
                    store.apply(evt(f"{prefix}{i:02d}", "shared", i * 5))
            except Exception as exc:  # 并发下任何异常都失败
                errors.append(exc)

        threads = [threading.Thread(target=writer, args=(p,)) for p in ("a", "b", "c")]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(errors, [])
        # 终态守恒：150 个有效事件全部恰属于一个会话（相邻间隔均为 5）。
        snap = store.snapshot("shared")
        self.assertEqual(snap["counts"]["sessions"], 1)
        self.assertEqual(snap["counts"]["members_sum"], 150)
        self.assertEqual(snap["counts"]["bridges"], 149)


class PersistenceTests(unittest.TestCase):
    def test_dump_load_roundtrip_preserves_history(self):
        store = SessionFactStore(gap_seconds=10)
        feed(store, "d", [("a", 0), ("c", 20), ("b", 10),
                          ("b", 10, 2, True)])
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.json"
            store.save(path)
            restored = SessionFactStore.load(path)
        for version in range(5):
            self.assertEqual(
                store.snapshot("d", version),
                restored.snapshot("d", version),
            )
        self.assertEqual(restored.revision_log("d"), store.revision_log("d"))

    def test_fixture_replay_outputs_and_conservation(self):
        with tempfile.TemporaryDirectory() as directory:
            store, report = replay(
                str(ROOT / "fixtures" / "events.jsonl"),
                str(ROOT / "fixtures" / "sessions.json"),
            )
            paths = write_outputs(store, report, Path(directory))
            # 配置声明的 input_versions 与重放产生的事实版本对齐（no-op 不产生版本）。
            bumped = [e["version"] for e in store.revision_log("d1") if e["bumped"]]
            self.assertEqual(
                report["input_versions_declared"],
                [version for version in bumped if version >= 2],
            )
            data = json.loads(Path(paths["report"]).read_text(encoding="utf-8"))
            self.assertTrue(
                all(check.get("active_eq_members_sum", True)
                    for check in data["conservation_checks"])
            )
            snapshots = json.loads(Path(paths["snapshots"]).read_text(encoding="utf-8"))
            self.assertEqual(snapshots[0]["version"], 0)
            self.assertEqual(snapshots[0]["sessions"], [])
            # 每版本成员集合展示。
            version_members = {
                row["version"]: [
                    sorted(session["member_ids"]) for session in row["sessions"]
                ]
                for row in data["version_members"]
            }
            self.assertEqual(version_members[2], [["a"], ["c"]])
            self.assertEqual(version_members[3], [["a", "b", "c"]])
            self.assertEqual(version_members[4], [["a"], ["c"]])
            # 账本同时含会话与桥事实，且撤销数与日志一致。
            ledger = [
                json.loads(line)
                for line in Path(paths["ledger"]).read_text(encoding="utf-8").splitlines()
            ]
            self.assertIn("bridge", {item["fact_kind"] for item in ledger})
            self.assertIn("session", {item["fact_kind"] for item in ledger})


class InputNormalizationTests(unittest.TestCase):
    def test_event_at_alias_accepted(self):
        store = SessionFactStore()
        store.apply({"event_id": "a", "device": "d", "event_at": 0,
                     "revision": 1, "withdrawn": False})
        self.assertEqual(store.snapshot("d")["sessions"][0]["started_at"], 0)

    def test_bad_record_rejected(self):
        store = SessionFactStore()
        for bad in (
            {"event_id": "", "device": "d", "event_at_seconds": 0,
             "revision": 1, "withdrawn": False},
            {"event_id": "a", "device": "d", "event_at_seconds": 0,
             "revision": 1.0, "withdrawn": False},
            {"event_id": "a", "device": "d", "event_at_seconds": "0",
             "revision": 1, "withdrawn": False},
        ):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    store.apply(bad)


if __name__ == "__main__":
    unittest.main()
