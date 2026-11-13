"""可修订会话事实构建器（合成台架设备事件）。

职责边界
========
本模块只处理合成设备事件的会话归并与版本修订，不做任何用户行为模式挖掘、
画像或轨迹分析。输入全部来自虚构设施的合成数据。

核心规则
========
1. 裁决：同一 ``event_id`` 取最高 ``revision`` 的记录为准；最高 revision 平级且
   内容（device / event_at_seconds / withdrawn）不一致时判定为平级冲突，隔离
   （quarantine），不参与会话构建，直到更高 revision 解除冲突。
2. 排序：每台设备内按 ``(event_at_seconds, event_id)`` 排序。
3. 连接：相邻事件时间差 ``<= gap_seconds`` 即连接；会话是连接链的极大分量。
   注意约束的是相邻间隔，不是会话首尾时差——一条链的总跨度可以远超 gap。
4. 会话键 ``session_key`` 取成员 ``event_id`` 的字典序最小值，与时间先后无关。
5. 单事件会话合法：时长 0 表示单点，不是缺测；空设备不产生任何会话。

增量修订
========
每次写入只重算"受影响闭包"：从变化事件在旧图/新图中的连接分量出发，沿相邻
间隔一直扩展到断开边界（间隔 > gap）。闭包外的事实保留稳定 ``fact_id``，
不撤销、不重发。迟到桥点合并多会话、撤回或时间更正拆分会话时，先撤销闭包内
旧事实（会话事实 + 成员桥事实），再在同一版本原子发布新集合。所有发布/撤销
进入仅追加账本，任意历史版本仍可通过 :meth:`SessionFactStore.snapshot` 查询。
"""

from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

GAP_SECONDS_DEFAULT = 10

# 事实账本中的操作类型
OP_PUBLISH = "publish"
OP_REVOKE = "revoke"
KIND_SESSION = "session"
KIND_BRIDGE = "bridge"


def normalize_event(raw: dict) -> dict:
    """校验单条原始事件并归一化字段名（接受 event_at 或 event_at_seconds）。"""
    try:
        event_id = raw["event_id"]
        device = raw["device"]
        revision = raw["revision"]
        withdrawn = raw["withdrawn"]
    except KeyError as exc:
        raise ValueError(f"原始事件缺少必填字段: {exc.args[0]}") from exc
    event_at = raw.get("event_at_seconds")
    if event_at is None:
        event_at = raw.get("event_at")
    if not isinstance(event_id, str) or not event_id:
        raise ValueError("event_id 必须是非空字符串")
    if not isinstance(device, str) or not device:
        raise ValueError("device 必须是非空字符串")
    if not isinstance(revision, int) or isinstance(revision, bool):
        raise ValueError("revision 必须是整数")
    if not isinstance(withdrawn, bool):
        raise ValueError("withdrawn 必须是布尔值")
    if not isinstance(event_at, (int, float)) or isinstance(event_at, bool):
        raise ValueError("event_at_seconds 必须是数值")
    return {
        "event_id": event_id,
        "device": device,
        "event_at_seconds": event_at,
        "revision": revision,
        "withdrawn": withdrawn,
    }


def _signature(event: dict) -> tuple:
    """同一 event_id 下用于判断内容是否冲突的签名。"""
    return (event["device"], event["event_at_seconds"], event["withdrawn"])


def adjudicate(records: list[dict]) -> tuple[dict | None, tuple]:
    """对同一 event_id 的全部记录裁决。

    返回 ``(winner, quarantined)``：
    - 正常时 winner 为最高 revision 记录（平级重复内容视为同一条），quarantined 为空；
    - 平级冲突时 winner 为 None，quarantined 为冲突的平级记录元组。
    """
    if not records:
        return None, ()
    top_revision = max(record["revision"] for record in records)
    top = [record for record in records if record["revision"] == top_revision]
    signatures = {_signature(record) for record in top}
    if len(signatures) == 1:
        return top[0], ()
    return None, tuple(top)


def order_active_events(active_events: list[dict]) -> list[dict]:
    """每设备内按 (时刻, event_id) 排序。原地排序并返回同一列表，
    保证调用方持有的分量对象本身就是时序顺序（避免成员桥左右反向）。"""
    active_events.sort(
        key=lambda event: (event["event_at_seconds"], event["event_id"])
    )
    return active_events


def build_components(ordered_events: list[dict], gap_seconds: float) -> list[list[dict]]:
    """把事件切成极大连接分量（会话成员链）。

    入口强制按 (时刻, event_id) 排序并返回新列表：调用方传入未排序序列（如
    dict.values()）时，返回的分量也严格按时序排列，成员桥的左右方向不会反转。
    """
    ordered_events = order_active_events(list(ordered_events))
    components: list[list[dict]] = []
    current: list[dict] = []
    previous = None
    for event in ordered_events:
        if (
            previous is not None
            and event["event_at_seconds"] - previous["event_at_seconds"] <= gap_seconds
        ):
            current.append(event)
        else:
            if current:
                components.append(current)
            current = [event]
        previous = event
    if current:
        components.append(current)
    return components


def reference_rebuild(records_by_id: dict, gap_seconds: float) -> dict:
    """全量参考重算。

    从裁决后的事件全集一次性重算会话与成员桥，仅用于与增量结果交叉核对，
    不参与任何事实的发布或撤销（增量路径不得用全表清空重算代替）。
    """
    active: list[dict] = []
    withdrawn = 0
    quarantined = 0
    for records in records_by_id.values():
        winner, conflict = adjudicate(records)
        if winner is None:
            if conflict:
                quarantined += 1
            continue
        if winner["withdrawn"]:
            withdrawn += 1
            continue
        active.append(winner)
    ordered = order_active_events(active)
    sessions: list[dict] = []
    bridges: list[dict] = []
    for component in build_components(ordered, gap_seconds):
        member_ids = sorted(event["event_id"] for event in component)
        sessions.append(
            {
                "session_key": min(member_ids),
                "member_ids": member_ids,
                "member_count": len(member_ids),
                "started_at": component[0]["event_at_seconds"],
                "ended_at": component[-1]["event_at_seconds"],
            }
        )
        for left, right in zip(component, component[1:]):
            bridges.append(
                {
                    "left_event_id": left["event_id"],
                    "right_event_id": right["event_id"],
                    "session_key": min(member_ids),
                    "gap_seconds": right["event_at_seconds"] - left["event_at_seconds"],
                }
            )
    return {
        "sessions": sessions,
        "bridges": bridges,
        "counts": {
            "active_events": len(active),
            "withdrawn_events": withdrawn,
            "quarantined_events": quarantined,
            "sessions": len(sessions),
            "bridges": len(bridges),
            "members_sum": sum(len(item["member_ids"]) for item in sessions),
        },
    }


def _winner_view(winner: dict | None, quarantined: tuple) -> tuple[str, dict | None]:
    if winner is None:
        return ("quarantined" if quarantined else "absent", None)
    if winner["withdrawn"]:
        return "withdrawn", winner
    return "active", winner


class SessionFactStore:
    """按设备隔离的可修订会话事实存储。

    每台设备持有独立版本号、事实账本与锁：多设备并行构建互不阻塞；
    同一设备的一次 :meth:`apply` 在设备锁内完成，读快照要么看到修订前、
    要么看到修订后，不会看到中间态。
    """

    def __init__(self, gap_seconds: float = GAP_SECONDS_DEFAULT, cross_check: bool = True):
        self.gap_seconds = gap_seconds
        self._cross_check = cross_check
        self._states: dict[str, dict] = {}
        self._locks: dict[str, threading.RLock] = {}
        self._bucket_guard = threading.Lock()

    # ------------------------------------------------------------------ 内部状态
    def _bucket(self, device: str) -> tuple[dict, threading.RLock]:
        with self._bucket_guard:
            state = self._states.get(device)
            if state is None:
                state = {
                    "version": 0,
                    "records": {},          # event_id -> 原始记录列表（仅追加）
                    "sessions": {},         # session_key -> 现行会话事实
                    "bridges": {},          # (left, right) -> 现行成员桥事实
                    "fact_seq": 0,
                    "ledger": [],           # 仅追加事实账本
                    "revisions": [],        # 每次写入的修订日志（含 no-op）
                }
                self._states[device] = state
                self._locks[device] = threading.RLock()
            return state, self._locks[device]

    def _active_winners(self, state: dict) -> dict:
        """对设备内全部 event_id 裁决，返回 event_id -> 现行有效事件。"""
        active: dict[str, dict] = {}
        for event_id, records in state["records"].items():
            winner, conflict = adjudicate(records)
            if winner is not None and not winner["withdrawn"]:
                active[event_id] = winner
        return active

    def _new_fact_id(self, state: dict, device: str, kind: str) -> str:
        state["fact_seq"] += 1
        prefix = "sf" if kind == KIND_SESSION else "br"
        return f"{device}:{prefix}:{state['fact_seq']}"

    # ------------------------------------------------------------------ 写入
    def apply(self, raw_event: dict) -> dict:
        """写入一条原始事件，返回本次修订日志条目。"""
        event = normalize_event(raw_event)
        state, lock = self._bucket(event["device"])
        with lock:
            return self._apply_locked(state, event)

    def apply_batch(self, raw_events: list[dict], parallel: bool = False,
                    max_workers: int | None = None) -> list[dict]:
        """批量写入。parallel=True 时按设备分组并行，同设备内仍串行且原子。"""
        if not parallel:
            return [self.apply(raw_event) for raw_event in raw_events]
        groups: dict[str, list[dict]] = {}
        for raw_event in raw_events:
            event = normalize_event(raw_event)
            groups.setdefault(event["device"], []).append(event)
        entries: list[dict] = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._apply_group, device, group)
                for device, group in groups.items()
            ]
            for future in futures:
                entries.extend(future.result())
        return entries

    def _apply_group(self, device: str, events: list[dict]) -> list[dict]:
        state, lock = self._bucket(device)
        with lock:
            return [self._apply_locked(state, event) for event in events]

    def _apply_locked(self, state: dict, event: dict) -> dict:
        device = event["device"]
        event_id = event["event_id"]
        previous_records = list(state["records"].get(event_id, ()))
        old_winner, old_conflict = adjudicate(previous_records)
        old_status, old_winner_event = _winner_view(old_winner, old_conflict)

        state["records"].setdefault(event_id, []).append(event)
        winner, conflict = adjudicate(state["records"][event_id])
        new_status, winner_event = _winner_view(winner, conflict)

        # 事实视图是否变化：只看裁决后的有效形态，不看 revision 数字本身。
        # 重送（同 revision 重复，或更高 revision 但内容一致）都是 no-op。
        old_shape = None
        if old_winner_event is not None:
            old_shape = (old_winner_event["event_at_seconds"], old_winner_event["withdrawn"])
        new_shape = None
        if winner_event is not None:
            new_shape = (winner_event["event_at_seconds"], winner_event["withdrawn"])
        view_changed = (old_status, old_shape) != (new_status, new_shape)

        entry: dict = {
            "device": device,
            "version": state["version"],
            "bumped": False,
            "raw_event": event,
            "adjudication": {
                "event_id": event_id,
                "status": new_status,
                "winning_revision": winner["revision"] if winner is not None else None,
                "quarantined_records": list(conflict),
            },
            "affected_event_ids": [],
            "revoked": {"session_fact_ids": [], "bridge_fact_ids": [], "session_keys": []},
            "published": {"session_fact_ids": [], "bridge_fact_ids": [], "session_keys": []},
            "lineage": [],
            "counts": self._counts(state),
        }

        if not view_changed:
            # 幂等重送 / 仍处隔离：事实集合不变，版本号不前进。
            entry["note"] = "no_change"
            state["revisions"].append(entry)
            return entry

        # ---- 旧图与新图的会话分量（全量构建仅用于定位闭包与交叉核对） ----
        old_active = self._active_winners_before(state, event_id)
        new_active = self._active_winners(state)
        old_ordered = order_active_events(list(old_active.values()))
        new_ordered = order_active_events(list(new_active.values()))
        old_components = build_components(old_ordered, self.gap_seconds)
        new_components = build_components(new_ordered, self.gap_seconds)

        affected = self._affected_closure(
            event_id, old_active, new_active, old_components, new_components, new_ordered
        )
        entry["affected_event_ids"] = sorted(affected)

        old_session_facts = [
            fact for fact in state["sessions"].values()
            if set(fact["member_ids"]) & affected
        ]
        new_session_components = [
            component for component in new_components
            if {item["event_id"] for item in component} & affected
        ]

        # 受影响闭包内的分量一律撤销重发。不能用"成员集合相同就保留事实"做
        # 优化：事件时刻更正可能保持成员集合与起止边界不变，却改变内部成员桥
        # 的 gap 与方向，保留会留下携带旧时刻的桥。闭包外的分量不受波及，
        # 其事实身份天然保留（增量行为由闭包定位保证，而非全表重算）。
        lineage = self._lineage(old_session_facts, new_session_components)
        entry["lineage"] = lineage

        next_version = state["version"] + 1

        # 撤销旧事实（会话 + 其成员桥）。桥按成员归属判定，不能按 session_key：
        # 合并/键漂移后会话的 canonical key 会变，旧桥挂的是旧键。
        old_affected_members = set()
        for fact in old_session_facts:
            old_affected_members.update(fact["member_ids"])
        for fact in old_session_facts:
            self._revoke(state, next_version, KIND_SESSION, fact["fact_id"])
            entry["revoked"]["session_fact_ids"].append(fact["fact_id"])
            entry["revoked"]["session_keys"].append(fact["session_key"])
        for bridge_key in list(state["bridges"]):
            bridge = state["bridges"][bridge_key]
            if bridge["left_event_id"] in old_affected_members \
                    and bridge["right_event_id"] in old_affected_members:
                self._revoke(state, next_version, KIND_BRIDGE, bridge["fact_id"])
                entry["revoked"]["bridge_fact_ids"].append(bridge["fact_id"])
                del state["bridges"][bridge_key]
        for fact in old_session_facts:
            del state["sessions"][fact["session_key"]]

        # 发布新事实。成员与桥一律基于按 (时刻, event_id) 排序后的序列构建，
        # 不依赖分量对象自身的存储顺序，杜绝桥左右反向。
        for raw_component in new_session_components:
            component = order_active_events(
                [dict(member) for member in raw_component]
            )
            member_ids = sorted(item["event_id"] for item in component)
            session_key = min(member_ids)
            fact_id = self._new_fact_id(state, device, KIND_SESSION)
            session_fact = {
                "fact_id": fact_id,
                "device": device,
                "session_key": session_key,
                "member_ids": member_ids,
                "member_count": len(member_ids),
                "started_at": component[0]["event_at_seconds"],
                "ended_at": component[-1]["event_at_seconds"],
                "duration_seconds": component[-1]["event_at_seconds"]
                - component[0]["event_at_seconds"],
                "first_published_version": next_version,
            }
            state["sessions"][session_key] = session_fact
            self._publish(state, next_version, KIND_SESSION, fact_id, session_fact)
            entry["published"]["session_fact_ids"].append(fact_id)
            entry["published"]["session_keys"].append(session_key)
            for left, right in zip(component, component[1:]):
                bridge_key = (left["event_id"], right["event_id"])
                bridge_id = self._new_fact_id(state, device, KIND_BRIDGE)
                bridge_fact = {
                    "fact_id": bridge_id,
                    "device": device,
                    "session_key": session_key,
                    "left_event_id": left["event_id"],
                    "right_event_id": right["event_id"],
                    "left_event_at_seconds": left["event_at_seconds"],
                    "gap_seconds": right["event_at_seconds"] - left["event_at_seconds"],
                    "first_published_version": next_version,
                }
                state["bridges"][bridge_key] = bridge_fact
                self._publish(state, next_version, KIND_BRIDGE, bridge_id, bridge_fact)
                entry["published"]["bridge_fact_ids"].append(bridge_id)

        state["version"] = next_version
        entry["version"] = next_version
        entry["bumped"] = True
        entry["counts"] = self._counts(state)

        # 守恒：现行成员总数必须等于有效事件数；桥数等于成员数减会话数。
        counts = entry["counts"]
        if counts["members_sum"] != counts["active_events"]:
            raise RuntimeError("事件数量守恒校验失败：会话成员总数 != 有效事件数")
        if counts["bridges"] != counts["active_events"] - counts["sessions"]:
            raise RuntimeError("成员桥数量守恒校验失败")
        if self._cross_check:
            self._reference_cross_check(state)

        state["revisions"].append(entry)
        return entry

    def _active_winners_before(self, state: dict, exclude_event_id: str) -> dict:
        """还原写入前的有效事件集合（用去掉最新一条后的记录裁决）。"""
        snapshot_records = state["records"]
        previous: dict[str, dict] = {}
        for event_id, records in snapshot_records.items():
            old_records = records[:-1] if event_id == exclude_event_id else records
            winner, conflict = adjudicate(old_records)
            if winner is not None and not winner["withdrawn"]:
                previous[event_id] = winner
        return previous

    def _affected_closure(self, event_id, old_active, new_active,
                          old_components, new_components, new_ordered) -> set[str]:
        """闭包定位：沿相邻间隔（<= gap 定义的连接关系）扩展到断开边界。

        连接关系已经由分量切分编码：同一分量内任意两点沿链的每一跳都 <= gap，
        不同分量之间的边界间隔 > gap。因此闭包 = 变化事件在旧图/新图中所在的
        整个分量，加上新图插入点两侧事件在旧图中的整个分量。不做跨分量的二次
        扩散——那会越过 > gap 的断开边界，把无关长链错误纳入。
        """
        affected: set[str] = set()

        def component_of(components: list[list[dict]], target_id: str) -> set[str]:
            for component in components:
                ids = {item["event_id"] for item in component}
                if target_id in ids:
                    return set(ids)
            return set()

        if event_id in old_active:
            affected |= component_of(old_components, event_id)
        if event_id in new_active:
            affected |= component_of(new_components, event_id)
            new_by_id = {item["event_id"]: item for item in new_ordered}
            new_ids = [item["event_id"] for item in new_ordered]
            position = new_ids.index(event_id)
            target_event = new_by_id[event_id]
            # 插入点两侧的旧分量只在相邻间隔 <= gap 时才纳入：间隔超阈值说明
            # 两侧本就断开，邻居属于另一条无关链，不能让闭包越过断开边界。
            for neighbor_id in (
                new_ids[position - 1:position] + new_ids[position + 1:position + 2]
            ):
                neighbor = new_by_id[neighbor_id]
                if abs(neighbor["event_at_seconds"] - target_event["event_at_seconds"]) \
                        > self.gap_seconds:
                    continue
                if neighbor_id in old_active:
                    affected |= component_of(old_components, neighbor_id)
        return affected

    @staticmethod
    def _lineage(old_facts: list[dict], new_components: list[list[dict]]) -> list[dict]:
        """构建 old -> new 血缘；键变化显式记录，不计为新增访问。

        以成员集合是否相交建边，按边两端的度数分类：
        1->1 同键 members_updated / 异键 key_changed；1->多 split；多->1 merged；
        无边的旧会话 removed、新会话 created。
        """
        new_views = []
        for component in new_components:
            member_ids = sorted(item["event_id"] for item in component)
            new_views.append({"session_key": min(member_ids), "member_ids": set(member_ids),
                              "member_count": len(member_ids)})
        edges: list[tuple[dict, dict]] = []
        for fact in old_facts:
            old_members = set(fact["member_ids"])
            for view in new_views:
                if old_members & view["member_ids"]:
                    edges.append((fact, view))
        old_degree: dict[str, int] = {}
        new_degree: dict[str, int] = {}
        for fact, view in edges:
            old_degree[fact["session_key"]] = old_degree.get(fact["session_key"], 0) + 1
            new_degree[view["session_key"]] = new_degree.get(view["session_key"], 0) + 1

        rows: list[dict] = []
        for fact, view in edges:
            if old_degree[fact["session_key"]] > 1:
                change = "split"
            elif new_degree[view["session_key"]] > 1:
                change = "merged"
            elif fact["session_key"] != view["session_key"]:
                change = "key_changed"
            else:
                change = "members_updated"
            rows.append({
                "change": change,
                "old_key": fact["session_key"],
                "new_key": view["session_key"],
                "old_member_count": fact["member_count"],
                "new_member_count": view["member_count"],
            })
        matched_old = {fact["session_key"] for fact, _ in edges}
        for fact in old_facts:
            if fact["session_key"] not in matched_old:
                rows.append({
                    "change": "removed",
                    "old_key": fact["session_key"],
                    "new_key": None,
                    "old_member_count": fact["member_count"],
                    "new_member_count": 0,
                })
        matched_new = {view["session_key"] for _, view in edges}
        for view in new_views:
            if view["session_key"] not in matched_new:
                rows.append({
                    "change": "created",
                    "old_key": None,
                    "new_key": view["session_key"],
                    "old_member_count": 0,
                    "new_member_count": view["member_count"],
                })
        return rows

    # ------------------------------------------------------------------ 账本
    def _publish(self, state: dict, version: int, kind: str, fact_id: str, fact: dict) -> None:
        state["ledger"].append({
            "version": version, "op": OP_PUBLISH, "fact_kind": kind,
            "fact_id": fact_id, "fact": fact,
        })

    def _revoke(self, state: dict, version: int, kind: str, fact_id: str) -> None:
        state["ledger"].append({
            "version": version, "op": OP_REVOKE, "fact_kind": kind, "fact_id": fact_id,
        })

    def _materialize(self, state: dict, version: int) -> tuple[dict, dict]:
        """回放账本得到某版本时刻的现行事实。"""
        sessions: dict[str, dict] = {}
        bridges: dict[tuple, dict] = {}
        for item in state["ledger"]:
            if item["version"] > version:
                break
            if item["op"] == OP_PUBLISH:
                if item["fact_kind"] == KIND_SESSION:
                    sessions[item["fact_id"]] = item["fact"]
                else:
                    fact = item["fact"]
                    bridges[(fact["left_event_id"], fact["right_event_id"])] = fact
            else:
                if item["fact_kind"] == KIND_SESSION:
                    sessions = {k: v for k, v in sessions.items() if k != item["fact_id"]}
                else:
                    bridges = {k: v for k, v in bridges.items() if v["fact_id"] != item["fact_id"]}
        return sessions, bridges

    def _counts(self, state: dict) -> dict:
        active = withdrawn = quarantined = 0
        for records in state["records"].values():
            winner, conflict = adjudicate(records)
            if winner is None:
                if conflict:
                    quarantined += 1
            elif winner["withdrawn"]:
                withdrawn += 1
            else:
                active += 1
        return {
            "active_events": active,
            "withdrawn_events": withdrawn,
            "quarantined_events": quarantined,
            "sessions": len(state["sessions"]),
            "bridges": len(state["bridges"]),
            "members_sum": sum(len(fact["member_ids"]) for fact in state["sessions"].values()),
        }

    def _reference_cross_check(self, state: dict) -> None:
        """与全量参考核对现行事实；只比对内容，不动任何 fact_id。"""
        reference = reference_rebuild(state["records"], self.gap_seconds)
        live_sessions = sorted(
            (
                {
                    "session_key": fact["session_key"],
                    "member_ids": sorted(fact["member_ids"]),
                    "member_count": fact["member_count"],
                    "started_at": fact["started_at"],
                    "ended_at": fact["ended_at"],
                }
                for fact in state["sessions"].values()
            ),
            key=lambda item: item["session_key"],
        )
        ref_sessions = sorted(reference["sessions"], key=lambda item: item["session_key"])
        if live_sessions != ref_sessions:
            raise RuntimeError(f"增量会话事实与全量参考不一致: {live_sessions} != {ref_sessions}")
        live_bridges = sorted(
            (left, right, fact["session_key"], fact["gap_seconds"])
            for (left, right), fact in state["bridges"].items()
        )
        ref_bridges = sorted(
            (item["left_event_id"], item["right_event_id"],
             item["session_key"], item["gap_seconds"])
            for item in reference["bridges"]
        )
        if live_bridges != ref_bridges:
            raise RuntimeError("增量成员桥事实与全量参考不一致")
        if self._counts(state) != reference["counts"]:
            raise RuntimeError("计数与全量参考不一致")

    # ------------------------------------------------------------------ 读取
    def devices(self) -> list[str]:
        with self._bucket_guard:
            return sorted(self._states)

    def latest_version(self, device: str) -> int:
        state, lock = self._bucket(device)
        with lock:
            return state["version"]

    def snapshot(self, device: str, version: int | None = None) -> dict:
        """返回某设备在某版本的事实快照；version=None 取最新，0 为空初态。"""
        state, lock = self._bucket(device)
        with lock:
            target = state["version"] if version is None else version
            if target < 0 or target > state["version"]:
                raise KeyError(f"设备 {device} 不存在版本 {target}")
            sessions, bridges = self._materialize(state, target)
            session_list = [dict(fact) for fact in sessions.values()]
            bridge_list = [dict(fact) for fact in bridges.values()]
            counts = {
                "sessions": len(session_list),
                "bridges": len(bridge_list),
                "members_sum": sum(len(fact["member_ids"]) for fact in session_list),
            }
            return {
                "device": device,
                "version": target,
                "sessions": sorted(session_list, key=lambda item: item["session_key"]),
                # 桥按时序定向排序：先左事件时刻，再两侧 event_id。
                "bridges": sorted(
                    bridge_list,
                    key=lambda item: (
                        item["left_event_at_seconds"],
                        item["left_event_id"],
                        item["right_event_id"],
                    ),
                ),
                "counts": counts,
            }

    def revision_log(self, device: str) -> list[dict]:
        state, lock = self._bucket(device)
        with lock:
            return [dict(entry) for entry in state["revisions"]]

    # ------------------------------------------------------------------ 持久化
    def dump_state(self) -> dict:
        with self._bucket_guard:
            devices = list(self._states)
        payload = {"gap_seconds": self.gap_seconds, "devices": {}}
        for device in devices:
            state, lock = self._bucket(device)
            with lock:
                payload["devices"][device] = {
                    "version": state["version"],
                    "records": state["records"],
                    "fact_seq": state["fact_seq"],
                    "sessions": state["sessions"],
                    "bridges": [
                        {"key": list(key), "fact": fact}
                        for key, fact in state["bridges"].items()
                    ],
                    "ledger": state["ledger"],
                    "revisions": state["revisions"],
                }
        return payload

    def save(self, path: str | Path) -> None:
        Path(path).write_text(
            json.dumps(self.dump_state(), ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    @classmethod
    def load(cls, path: str | Path) -> "SessionFactStore":
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return cls.from_state(data)

    @classmethod
    def from_state(cls, payload: dict) -> "SessionFactStore":
        store = cls(gap_seconds=payload["gap_seconds"], cross_check=False)
        for device, blob in payload["devices"].items():
            state, _ = store._bucket(device)
            state["version"] = blob["version"]
            state["records"] = {
                event_id: [dict(record) for record in records]
                for event_id, records in blob["records"].items()
            }
            state["fact_seq"] = blob["fact_seq"]
            state["sessions"] = {
                key: dict(fact) for key, fact in blob["sessions"].items()
            }
            state["bridges"] = {
                tuple(item["key"]): dict(item["fact"]) for item in blob["bridges"]
            }
            state["ledger"] = [dict(item) for item in blob["ledger"]]
            state["revisions"] = [dict(item) for item in blob["revisions"]]
        return store
