"""按到达顺序重放合成设备事件，产出可修订会话事实集合。

用法（解释器隔离运行，路径均以参数传入）：

    python3 -I -m src.replay_revisions fixtures/events.jsonl fixtures/sessions.json \
        --output-dir .workspace-output

产出（计算输出目录，绝不覆盖 fixtures 原始资料）：
    session_facts.jsonl   仅追加事实账本：publish/revoke（会话事实 + 成员桥事实）
    session_snapshots.json 每个设备每个版本的完整快照（历史版本可查）
    revision_log.jsonl    逐次写入的增量修订日志（含 no-op 重送、平级冲突隔离）
    replay_report.json    每版本成员集合、血缘 old->new、事件数量守恒核对
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from src.session_builder import SessionFactStore
from src.session_reader import load


def render_time(seconds: float, origin: str | None) -> str | None:
    """把相对秒数渲染为绝对时刻，用于展示跨日；纯展示字段，不参与任何计算。"""
    if origin is None:
        return None
    base = datetime.fromisoformat(origin)
    return (base + timedelta(seconds=seconds)).isoformat()


def replay(events_path: str, config_path: str) -> tuple[SessionFactStore, dict]:
    config = load(config_path)
    gap_seconds = config.get("gap_seconds", 10)
    origin = config.get("time_origin")
    raw_events = load(events_path)

    store = SessionFactStore(gap_seconds=gap_seconds)
    snapshots: list[dict] = []

    for index, raw_event in enumerate(raw_events):
        entry = store.apply(raw_event)
        if index == 0:
            # 版本 0：空初态——空设备不造会话。
            snapshots.append(store.snapshot(raw_event["device"], version=0))
        if entry["bumped"]:
            snapshot = store.snapshot(raw_event["device"])
            for session in snapshot["sessions"]:
                session["started_at_iso"] = render_time(session["started_at"], origin)
                session["ended_at_iso"] = render_time(session["ended_at"], origin)
            snapshots.append(snapshot)

    report = build_report(store, snapshots, config, raw_events)
    return store, report


def build_report(store: SessionFactStore, snapshots: list[dict],
                 config: dict, raw_events: list[dict]) -> dict:
    devices = store.devices()
    report = {
        "gap_seconds": config.get("gap_seconds", 10),
        "time_origin": config.get("time_origin"),
        "raw_event_count": len(raw_events),
        "devices": devices,
        "input_versions_declared": config.get("input_versions"),
        "version_members": [],
        "conservation_checks": [],
    }
    for snapshot in snapshots:
        member_row = {
            "device": snapshot["device"],
            "version": snapshot["version"],
            "sessions": [
                {
                    "session_key": item["session_key"],
                    "member_ids": item["member_ids"],
                    "member_count": item["member_count"],
                    "started_at": item["started_at"],
                    "ended_at": item["ended_at"],
                    "duration_seconds": item["duration_seconds"],
                }
                for item in snapshot["sessions"]
            ],
            "bridges": [
                {
                    "left_event_id": item["left_event_id"],
                    "right_event_id": item["right_event_id"],
                    "gap_seconds": item["gap_seconds"],
                }
                for item in snapshot["bridges"]
            ],
        }
        report["version_members"].append(member_row)
        counts = snapshot["counts"]
        report["conservation_checks"].append({
            "device": snapshot["device"],
            "version": snapshot["version"],
            "members_sum": counts["members_sum"],
            "sessions": counts["sessions"],
            "bridges": counts["bridges"],
            "identity_members_eq_bridges_plus_sessions":
                counts["members_sum"] == counts["bridges"] + counts["sessions"],
        })
    # 终态逐设备守恒：现行成员总数 == 裁决后有效事件数。
    for device in devices:
        entry = store.revision_log(device)[-1]
        counts = entry["counts"]
        report["conservation_checks"].append({
            "device": device,
            "version": entry["version"],
            "active_events": counts["active_events"],
            "withdrawn_events": counts["withdrawn_events"],
            "quarantined_events": counts["quarantined_events"],
            "members_sum": counts["members_sum"],
            "active_eq_members_sum": counts["active_events"] == counts["members_sum"],
        })
    return report


def write_outputs(store: SessionFactStore, report: dict, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    ledger_path = output_dir / "session_facts.jsonl"
    log_path = output_dir / "revision_log.jsonl"
    snapshots_path = output_dir / "session_snapshots.json"
    report_path = output_dir / "replay_report.json"

    with ledger_path.open("w", encoding="utf-8") as stream:
        for device in store.devices():
            for item in store.dump_state()["devices"][device]["ledger"]:
                stream.write(json.dumps({"device": device, **item}, ensure_ascii=False) + "\n")
    with log_path.open("w", encoding="utf-8") as stream:
        for device in store.devices():
            for entry in store.revision_log(device):
                stream.write(json.dumps(entry, ensure_ascii=False) + "\n")
    snapshots_path.write_text(
        json.dumps(
            [store.snapshot(device, version=v)
             for device in store.devices()
             for v in range(store.latest_version(device) + 1)],
            ensure_ascii=False, indent=2,
        ),
        encoding="utf-8",
    )
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    return {
        "ledger": str(ledger_path),
        "revision_log": str(log_path),
        "snapshots": str(snapshots_path),
        "report": str(report_path),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="重放合成设备事件并构建可修订会话事实")
    parser.add_argument("events_path")
    parser.add_argument("config_path")
    parser.add_argument("--output-dir", default=".workspace-output")
    args = parser.parse_args()

    store, report = replay(args.events_path, args.config_path)
    paths = write_outputs(store, report, Path(args.output_dir))

    for row in report["version_members"]:
        members = [
            f"{item['session_key']}={'+'.join(item['member_ids'])}"
            for item in row["sessions"]
        ]
        print(f"v{row['version']} {row['device']}: {members or '(空)'}")
    print(json.dumps({"outputs": paths, "checks": report["conservation_checks"]},
                     ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
