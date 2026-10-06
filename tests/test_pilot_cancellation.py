from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.pilots.service import PilotOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, init_db

from tests.test_pilot_operations import PROTOCOL, submit_payload


ACTOR = "emergency-supervisor"
REASON = "暴雨红色预警，亲子团接驳行程终止"


def make_service() -> tuple[PilotOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(datetime(2026, 10, 6, 20, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    return service, clock


def submit_and_claim(service: PilotOperationsService, key: str, lease_seconds: int = 60) -> dict:
    submitted = service.submit(submit_payload(key))
    claimed = service.claim("village-shuttle-site", ["chapter-transfer"], lease_seconds)
    assert claimed and claimed["id"] == submitted["id"]
    return claimed


def test_cancel_running_records_commitment_and_blocks_fulfillment(client):
    service, _ = make_service()
    claimed = submit_and_claim(service, "storm-cancel-001")

    requested = service.cancel(claimed["id"], ACTOR, REASON)
    assert requested["status"] == "cancel_requested"
    assert requested["cancel_requested_by"] == ACTOR
    assert requested["cancel_reason"] == REASON
    assert requested["cancel_requested_at"] == "2026-10-06T20:00:00+00:00"
    assert requested["finished_at"] is None
    assert requested["lease_owner"] == "village-shuttle-site"

    with pytest.raises(ConflictError, match="禁止续租"):
        service.heartbeat(claimed["id"], "village-shuttle-site", 60)
    with pytest.raises(ConflictError, match="不再接受履约回执"):
        service.complete(claimed["id"], "village-shuttle-site", {"value": 1}, {})
    with pytest.raises(ConflictError, match="确认停止"):
        service.fail(claimed["id"], "village-shuttle-site", "weather_hold", "暴雨封路", True)

    # 停止中的场次不会被重新领取
    assert service.claim("other-site", ["chapter-transfer"], 60) is None


def test_node_confirm_closes_immediately(client):
    service, _ = make_service()
    claimed = submit_and_claim(service, "storm-cancel-002")
    service.cancel(claimed["id"], ACTOR, REASON)

    confirmed = service.confirm_cancel(claimed["id"], "village-shuttle-site")
    assert confirmed["status"] == "cancelled"
    assert confirmed["stop_outcome"] == "confirmed"
    assert confirmed["lease_owner"] == ""
    assert confirmed["finished_at"] == "2026-10-06T20:00:00+00:00"

    details = service.get_session(claimed["id"])
    assert details["outcome"] == "cancel_confirmed"
    assert details["cancel_requested_by"] == ACTOR
    assert details["cancel_reason"] == REASON
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_confirmed"]
    assert details["interventions"][1]["actor"] == "village-shuttle-site"

    # 确认后的迟到履约回执无法改写结果
    with pytest.raises(ConflictError, match="已经停止"):
        service.complete(claimed["id"], "village-shuttle-site", {"value": 1}, {})
    assert service.get_session(claimed["id"])["status"] == "cancelled"


def test_unconfirmed_cancel_closed_by_timeout_recovery(client):
    service, clock = make_service()
    claimed = submit_and_claim(service, "storm-cancel-003", lease_seconds=30)
    service.cancel(claimed["id"], ACTOR, REASON)

    clock.advance(seconds=31)
    result = service.recover_expired()
    assert result["cancelled"] == [claimed["id"]]
    assert result["recovered"] == [] and result["exhausted"] == []

    details = service.get_session(claimed["id"])
    assert details["status"] == "cancelled"
    assert details["stop_outcome"] == "timeout"
    assert details["outcome"] == "cancel_timeout"
    assert details["finished_at"] == "2026-10-06T20:00:31+00:00"
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_timeout"]

    # 不会回到待分配队列
    assert service.claim("other-site", ["chapter-transfer"], 60) is None


def test_duplicate_cancel_and_recovery_rerun_never_rewrite(client):
    service, clock = make_service()
    claimed = submit_and_claim(service, "storm-cancel-004", lease_seconds=30)
    service.cancel(claimed["id"], ACTOR, REASON)

    # 停止流程中的重复申请：不改写原因与请求人，也不新增干预记录
    again = service.cancel(claimed["id"], "other-actor", "另一套说辞")
    assert again["status"] == "cancel_requested"
    assert again["cancel_requested_by"] == ACTOR
    assert again["cancel_reason"] == REASON
    assert len(service.get_session(claimed["id"])["interventions"]) == 1

    clock.advance(seconds=31)
    service.recover_expired()
    # 终态上的重复申请与恢复重跑均不改写结果
    final = service.cancel(claimed["id"], "other-actor", "另一套说辞")
    assert final["status"] == "cancelled" and final["stop_outcome"] == "timeout"
    rerun = service.recover_expired()
    assert rerun == {"recovered": [], "exhausted": [], "cancelled": []}
    details = service.get_session(claimed["id"])
    assert details["cancel_requested_by"] == ACTOR
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_timeout"]

    # 迟到的节点确认也无法把超时停止改写为确认停止
    late = service.confirm_cancel(claimed["id"], "village-shuttle-site")
    assert late["stop_outcome"] == "timeout"
    assert len(service.get_session(claimed["id"])["interventions"]) == 2


def test_restart_preserves_cancel_commitment(client):
    service, clock = make_service()
    claimed = submit_and_claim(service, "storm-cancel-005", lease_seconds=30)
    service.cancel(claimed["id"], ACTOR, REASON)

    # 模拟进程重启：同一数据库上重建服务实例，承诺继续兑现
    restarted = PilotOperationsService(get_connection(), clock)
    clock.advance(seconds=31)
    result = restarted.recover_expired()
    assert result["cancelled"] == [claimed["id"]]
    details = restarted.get_session(claimed["id"])
    assert details["outcome"] == "cancel_timeout"
    assert details["cancel_reason"] == REASON


def test_queued_cancel_is_immediate_and_retry_requeues_cleanly(client):
    service, _ = make_service()
    submitted = service.submit(submit_payload("storm-cancel-006"))

    cancelled = service.cancel(submitted["id"], ACTOR, REASON)
    assert cancelled["status"] == "cancelled"
    assert cancelled["stop_outcome"] == "confirmed"
    assert cancelled["cancel_requested_by"] == ACTOR
    assert cancelled["finished_at"] == "2026-10-06T20:00:00+00:00"

    # 人工重试（改派）开始新生命周期，停止元数据清空
    retried = service.retry(submitted["id"], ACTOR, "预警解除，重新排期")
    assert retried["status"] == "queued"
    assert retried["cancel_requested_by"] == ""
    assert retried["stop_outcome"] == ""
    reclaimed = service.claim("village-shuttle-site", ["chapter-transfer"], 60)
    assert reclaimed and reclaimed["id"] == submitted["id"]


def test_outcome_distinguishes_completion_and_stop_paths(client):
    service, clock = make_service()
    done = submit_and_claim(service, "storm-cancel-007")
    service.complete(done["id"], "village-shuttle-site", {"value": 1}, {})

    confirmed = submit_and_claim(service, "storm-cancel-008")
    service.cancel(confirmed["id"], ACTOR, REASON)
    service.confirm_cancel(confirmed["id"], "village-shuttle-site")

    timed_out = submit_and_claim(service, "storm-cancel-009", lease_seconds=30)
    service.cancel(timed_out["id"], ACTOR, REASON)
    clock.advance(seconds=31)
    service.recover_expired()

    outcomes = {row["id"]: row["outcome"] for row in service.list_sessions()}
    assert outcomes[done["id"]] == "succeeded"
    assert outcomes[confirmed["id"]] == "cancel_confirmed"
    assert outcomes[timed_out["id"]] == "cancel_timeout"


def test_cancel_terminal_session_rejected(client):
    service, _ = make_service()
    claimed = submit_and_claim(service, "storm-cancel-010")
    service.complete(claimed["id"], "village-shuttle-site", {"value": 1}, {})
    with pytest.raises(ConflictError, match="不允许取消"):
        service.cancel(claimed["id"], ACTOR, REASON)


def test_cancel_confirm_api_flow(client):
    response = client.post("/api/pilots/protocols?actor=administrator", json=PROTOCOL)
    assert response.status_code == 201
    submitted = client.post("/api/pilots/sessions", json=submit_payload("storm-cancel-011")).json()
    claimed = client.post(
        "/api/pilots/sessions/claim",
        json={"site_code": "village-shuttle-site", "capabilities": ["chapter-transfer"], "lease_seconds": 60},
    ).json()["session"]
    assert claimed["id"] == submitted["id"]

    cancelled = client.post(
        f"/api/pilots/sessions/{submitted['id']}/cancel",
        json={"actor": ACTOR, "reason": REASON},
    )
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancel_requested"

    heartbeat = client.post(
        f"/api/pilots/sessions/{submitted['id']}/heartbeat",
        json={"site_code": "village-shuttle-site", "capabilities": [], "lease_seconds": 60},
    )
    assert heartbeat.status_code == 409

    wrong_site = client.post(f"/api/pilots/sessions/{submitted['id']}/cancel/confirm", json={"site_code": "other-site"})
    assert wrong_site.status_code == 409

    confirmed = client.post(f"/api/pilots/sessions/{submitted['id']}/cancel/confirm", json={"site_code": "village-shuttle-site"})
    assert confirmed.status_code == 200
    assert confirmed.json()["stop_outcome"] == "confirmed"

    details = client.get(f"/api/pilots/session-details/{submitted['id']}").json()
    assert details["outcome"] == "cancel_confirmed"
    assert details["cancel_requested_by"] == ACTOR
    listed = client.get("/api/pilots/sessions", params={"status": "cancelled"}).json()["items"]
    assert [item["outcome"] for item in listed] == ["cancel_confirmed"]
