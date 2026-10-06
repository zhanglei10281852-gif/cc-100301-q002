from __future__ import annotations

from datetime import UTC, datetime

import pytest

from app.pilots.service import PilotOperationsService
from app.core.clock import FrozenClock
from app.core.errors import ConflictError
from app.database import get_connection, init_db, transaction


PROTOCOL = {
    "code": "chapter-transfer",
    "name": "多城章节线路换乘节奏行程方案",
    "capability": "chapter-transfer",
    "parameter_schema": {
        "minutes": {"type": "integer", "required": True, "minimum": 1, "maximum": 30},
        "assist_level": {"type": "number", "required": False, "minimum": 0.0, "maximum": 1.0},
        "scene": {"type": "string", "required": True, "choices": ["rail", "coach"]},
    },
    "default_parameters": {"assist_level": 0.4},
    "max_runtime_seconds": 300,
    "max_attempts": 2,
}


def submit_payload(key: str, *, user: str = "pilot-operator-1", priority: int = 50) -> dict:
    return {
        "protocol_code": "chapter-transfer",
        "project_code": "seven-city-story-route",
        "requested_by": user,
        "parameters": {"minutes": 8, "scene": "rail"},
        "priority": priority,
        "idempotency_key": key,
    }


def create_protocol(client) -> None:
    response = client.post("/api/pilots/protocols?actor=administrator", json=PROTOCOL)
    assert response.status_code == 201, response.text


def test_protocol_submission_idempotency_and_parameter_validation(client):
    create_protocol(client)
    first = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    second = client.post("/api/pilots/sessions", json=submit_payload("request-000001"))
    assert first.status_code == second.status_code == 202
    assert first.json()["id"] == second.json()["id"]
    invalid = submit_payload("request-000002")
    invalid["parameters"]["minutes"] = 50
    rejected = client.post("/api/pilots/sessions", json=invalid)
    assert rejected.status_code == 422


def test_priority_capability_claim_and_observation_version(client):
    create_protocol(client)
    low = client.post("/api/pilots/sessions", json=submit_payload("priority-low", priority=10)).json()
    high = client.post("/api/pilots/sessions", json=submit_payload("priority-high", priority=90)).json()
    no_match = client.post("/api/pilots/sessions/claim", json={"site_code": "w0", "capabilities": ["other"], "lease_seconds": 60})
    assert no_match.status_code == 200 and no_match.json()["session"] is None
    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "w1", "capabilities": ["chapter-transfer"], "lease_seconds": 60})
    assert claimed.status_code == 200
    assert claimed.json()["session"]["id"] == high["id"]
    completed = client.post(
        f"/api/pilots/sessions/{high['id']}/complete",
        json={"site_code": "w1", "observation": {"value": 3.14}, "metrics": {"seconds": 2}},
    )
    assert completed.status_code == 200
    details = client.get(f"/api/pilots/session-details/{high['id']}").json()
    assert details["status"] == "succeeded"
    assert details["current_observation_version"] == 1
    assert len(details["observations"]) == 1
    assert low["status"] == "queued"


def test_quota_cancel_retry_priority_and_batch_interventions(client):
    create_protocol(client)
    quota = client.put(
        "/api/pilots/quotas?actor=administrator",
        json={"subject_type": "user", "subject_key": "limited", "max_queued": 1, "max_running": 1, "daily_submissions": 2},
    )
    assert quota.status_code == 200
    one = client.post("/api/pilots/sessions", json=submit_payload("quota-one", user="limited")).json()
    blocked = client.post("/api/pilots/sessions", json=submit_payload("quota-two", user="limited"))
    assert blocked.status_code == 409
    cancelled = client.post(f"/api/pilots/sessions/{one['id']}/cancel", json={"actor": "administrator", "reason": "项目暂停"})
    assert cancelled.status_code == 200 and cancelled.json()["status"] == "cancelled"
    retried = client.post(f"/api/pilots/sessions/{one['id']}/retry", json={"actor": "administrator", "reason": "项目恢复", "priority": 95})
    assert retried.status_code == 200 and retried.json()["priority"] == 95
    other = client.post("/api/pilots/sessions", json=submit_payload("batch-other", user="other-user")).json()
    batch = client.post(
        "/api/pilots/sessions/batch",
        json={"session_ids": [one["id"], other["id"]], "operation": "priority", "actor": "administrator", "reason": "线路合作方临时到场", "priority": 99},
    )
    assert batch.status_code == 200
    assert len(batch.json()["succeeded"]) == 2
    details = client.get(f"/api/pilots/session-details/{one['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel", "retry", "priority"]


def test_failure_backoff_and_expired_lease_recovery(client):
    init_db()
    clock = FrozenClock(datetime(2026, 9, 26, 2, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    first = service.submit(submit_payload("failure-000001"))
    claimed = service.claim("site-a", ["chapter-transfer"], 10)
    assert claimed and claimed["id"] == first["id"]
    failed = service.fail(first["id"], "site-a", "connection_delayed", "前序列车晚点导致接驳窗口不稳定", True)
    assert failed["status"] == "queued"
    assert failed["available_at"] > failed["updated_at"]
    clock.advance(seconds=2)
    claimed_again = service.claim("site-a", ["chapter-transfer"], 10)
    assert claimed_again and claimed_again["attempt_count"] == 2
    clock.advance(seconds=11)
    recovered = service.recover_expired()
    assert recovered["exhausted"] == [first["id"]]
    details = service.get_session(first["id"])
    assert details["status"] == "failed"
    assert details["interventions"][-1]["action"] == "lease_recovery"


def make_clocked_service(hour: int = 8) -> tuple[PilotOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(datetime(2026, 10, 6, hour, 0, tzinfo=UTC))
    service = PilotOperationsService(get_connection(), clock)
    service.create_protocol(PROTOCOL, "administrator")
    return service, clock


def test_cancel_commitment_confirmed_by_site(client):
    service, clock = make_clocked_service()
    submitted = service.submit(submit_payload("cancel-confirm-001"))
    claimed = service.claim("site-storm", ["chapter-transfer"], 120)
    assert claimed and claimed["id"] == submitted["id"]

    requested = service.cancel(submitted["id"], "support-agent-7", "暴雨红色预警，亲子团要求终止行程")
    assert requested["status"] == "cancel_requested"
    assert requested["cancel_requested_by"] == "support-agent-7"
    assert requested["cancel_reason"] == "暴雨红色预警，亲子团要求终止行程"
    assert requested["cancel_requested_at"]
    assert requested["lease_owner"] == "site-storm"
    assert requested["finished_at"] is None

    # 承诺生效后：不得续租、不得提交普通履约回执、不会被重新领取。
    with pytest.raises(ConflictError):
        service.heartbeat(submitted["id"], "site-storm", 120)
    with pytest.raises(ConflictError):
        service.complete(submitted["id"], "site-storm", {"value": 1}, {})
    with pytest.raises(ConflictError):
        service.fail(submitted["id"], "site-storm", "connection_delayed", "前序列车晚点", True)
    assert service.claim("site-other", ["chapter-transfer"], 60) is None

    # 重复申请不改写已经登记的承诺。
    again = service.cancel(submitted["id"], "another-agent", "重复登记的其他原因")
    assert again["status"] == "cancel_requested"
    assert again["cancel_requested_by"] == "support-agent-7"
    assert again["cancel_reason"] == "暴雨红色预警，亲子团要求终止行程"

    # 其他节点不能代为确认。
    with pytest.raises(ConflictError):
        service.confirm_cancel(submitted["id"], "site-other")

    clock.advance(seconds=30)
    confirmed = service.confirm_cancel(submitted["id"], "site-storm", "车辆已安全返回接待点")
    assert confirmed["status"] == "cancelled"
    assert confirmed["cancel_outcome"] == "confirmed"
    assert confirmed["outcome"] == "cancel_confirmed"
    assert confirmed["finished_at"]
    assert confirmed["lease_owner"] == ""

    # 终态确定后：迟到的确认、完成与重复申请都无法改写。
    with pytest.raises(ConflictError):
        service.confirm_cancel(submitted["id"], "site-storm")
    with pytest.raises(ConflictError):
        service.complete(submitted["id"], "site-storm", {"value": 1}, {})
    replay = service.cancel(submitted["id"], "support-agent-7", "暴雨红色预警，亲子团要求终止行程")
    assert replay["cancel_outcome"] == "confirmed"

    details = service.get_session(submitted["id"])
    assert details["outcome"] == "cancel_confirmed"
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_confirm"]
    assert details["interventions"][0]["actor"] == "support-agent-7"
    assert details["interventions"][1]["actor"] == "site-storm"


def test_cancel_commitment_expires_with_lease(client):
    service, clock = make_clocked_service()
    submitted = service.submit(submit_payload("cancel-expire-001"))
    claimed = service.claim("site-offline", ["chapter-transfer"], 30)
    assert claimed and claimed["id"] == submitted["id"]
    service.cancel(submitted["id"], "support-agent-9", "暴雨预警，亲子团终止行程")

    # 节点离线，租约越过期限：恢复作业沿取消方向终结，而不是回到待分配队列。
    clock.advance(seconds=31)
    result = service.recover_expired()
    assert result["cancelled"] == [submitted["id"]]
    assert result["recovered"] == [] and result["exhausted"] == []

    details = service.get_session(submitted["id"])
    assert details["status"] == "cancelled"
    assert details["cancel_outcome"] == "expired"
    assert details["outcome"] == "cancel_expired"
    assert details["cancel_requested_by"] == "support-agent-9"
    assert details["finished_at"]
    assert details["interventions"][-1]["action"] == "cancel_timeout"
    assert service.claim("site-other", ["chapter-transfer"], 60) is None

    # 恢复作业重跑幂等，迟到的节点确认与完成都改不了超时终态。
    assert service.recover_expired() == {"recovered": [], "exhausted": [], "cancelled": []}
    with pytest.raises(ConflictError):
        service.confirm_cancel(submitted["id"], "site-offline")
    with pytest.raises(ConflictError):
        service.complete(submitted["id"], "site-offline", {"value": 1}, {})
    assert service.get_session(submitted["id"])["cancel_outcome"] == "expired"


def test_cancel_commitment_survives_process_restart(client):
    service, clock = make_clocked_service()
    submitted = service.submit(submit_payload("cancel-restart-001"))
    service.claim("site-a", ["chapter-transfer"], 60)
    service.cancel(submitted["id"], "support-agent-3", "道路积水，终止接驳")

    # 模拟进程重启：新的服务实例读取同一数据库，承诺与终态行为保持一致。
    restarted = PilotOperationsService(get_connection(), clock)
    view = restarted.get_session(submitted["id"])
    assert view["status"] == "cancel_requested"
    assert view["cancel_requested_by"] == "support-agent-3"
    with pytest.raises(ConflictError):
        restarted.heartbeat(submitted["id"], "site-a", 60)
    confirmed = restarted.confirm_cancel(submitted["id"], "site-a")
    assert confirmed["cancel_outcome"] == "confirmed"
    assert restarted.get_session(submitted["id"])["outcome"] == "cancel_confirmed"


def test_queued_cancel_and_outcome_visibility_via_api(client):
    create_protocol(client)
    submitted = client.post("/api/pilots/sessions", json=submit_payload("cancel-queued-001")).json()
    cancelled = client.post(f"/api/pilots/sessions/{submitted['id']}/cancel", json={"actor": "support-agent-1", "reason": "项目计划变更"})
    assert cancelled.status_code == 200
    assert cancelled.json()["status"] == "cancelled"
    assert cancelled.json()["cancel_outcome"] == "immediate"
    assert cancelled.json()["outcome"] == "cancel_immediate"

    details = client.get(f"/api/pilots/session-details/{submitted['id']}").json()
    assert details["outcome"] == "cancel_immediate"
    assert details["cancel_requested_by"] == "support-agent-1"
    listed = client.get("/api/pilots/sessions", params={"status": "cancelled"}).json()["items"]
    assert [item["outcome"] for item in listed] == ["cancel_immediate"]


def test_cancel_confirmation_api(client):
    create_protocol(client)
    submitted = client.post("/api/pilots/sessions", json=submit_payload("cancel-api-001")).json()
    claimed = client.post("/api/pilots/sessions/claim", json={"site_code": "w1", "capabilities": ["chapter-transfer"], "lease_seconds": 60})
    assert claimed.json()["session"]["id"] == submitted["id"]
    client.post(f"/api/pilots/sessions/{submitted['id']}/cancel", json={"actor": "support-agent-2", "reason": "暴雨预警终止行程"})

    missing = client.post(f"/api/pilots/sessions/{submitted['id']}/heartbeat", json={"site_code": "w1", "capabilities": [], "lease_seconds": 60})
    assert missing.status_code == 409
    wrong_site = client.post(f"/api/pilots/sessions/{submitted['id']}/cancel-confirmation", json={"site_code": "w2"})
    assert wrong_site.status_code == 409
    confirmed = client.post(f"/api/pilots/sessions/{submitted['id']}/cancel-confirmation", json={"site_code": "w1", "note": "游客已安置到县城酒店"})
    assert confirmed.status_code == 200
    assert confirmed.json()["status"] == "cancelled"
    assert confirmed.json()["outcome"] == "cancel_confirmed"

    details = client.get(f"/api/pilots/session-details/{submitted['id']}").json()
    assert details["interventions"][-1]["action"] == "cancel_confirm"
    assert details["interventions"][-1]["reason"] == "游客已安置到县城酒店"

