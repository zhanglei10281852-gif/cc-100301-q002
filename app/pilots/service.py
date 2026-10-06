from __future__ import annotations

import hashlib
import json
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.pilots.repository import PilotRepository
from app.core.clock import Clock, SystemClock, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class PilotOperationsService:
    """管理运营方案、配额、游览场次租约、观察记录版本和人工干预。"""

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = PilotRepository(self.connection)

    def list_protocols(self) -> list[dict[str, Any]]:
        return self.repository.active_protocols()

    def create_protocol(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            if repository.protocol_by_code(payload["code"]):
                raise ConflictError("参数方案编码已存在")
            return repository.create_protocol(
                code=payload["code"], name=payload["name"], capability=payload["capability"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return PilotRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            protocol = repository.protocol_by_code(payload["protocol_code"])
            if protocol is None or not protocol["active"]:
                raise NotFoundError("参数方案不存在或已经停用")
            parameters = self._validate_parameters(protocol, payload["parameters"])
            existing = repository.session_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的运营参数")
                return dict(repository.session_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_session(
                protocol_id=protocol["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=protocol["max_attempts"], now=now,
            )

    def list_sessions(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.repository.list_sessions(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))
        return [self._with_outcome(row) for row in rows]

    def get_session(self, session_id: int) -> dict[str, Any]:
        row = self.repository.session_by_id(session_id)
        if row is None:
            raise NotFoundError("运营游览场次不存在")
        observation = self._with_outcome(dict(row))
        observation["observations"] = self.repository.observation_versions(session_id)
        observation["interventions"] = self.repository.interventions(session_id)
        return observation

    @staticmethod
    def _with_outcome(session: dict[str, Any]) -> dict[str, Any]:
        """附加面向客服的终态判读：正常完成、节点确认停止或超时停止。"""
        outcome = session["status"]
        if outcome == "cancelled":
            outcome = "cancel_timeout" if session.get("stop_outcome") == "timeout" else "cancel_confirmed"
        return {**session, "outcome": outcome}

    def claim(self, site_code: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE pilot_sessions SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (site_code, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.session_by_id(candidate["id"]))

    def heartbeat(self, session_id: int, site_code: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            session = PilotRepository(connection).session_by_id(session_id)
            if session is not None and session["status"] == "cancel_requested":
                raise ConflictError("游览场次已进入停止流程，禁止续租")
            cursor = connection.execute(
                "UPDATE pilot_sessions SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, session_id, site_code),
            )
            if cursor.rowcount != 1:
                raise ConflictError("游览场次未由当前执行站点持有")
            return dict(PilotRepository(connection).session_by_id(session_id))

    def complete(self, session_id: int, site_code: str, observation: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] == "cancel_requested":
                raise ConflictError("游览场次已进入停止流程，不再接受履约回执")
            if session["status"] == "cancelled":
                raise ConflictError("游览场次已经停止，迟到回执不会改变结果")
            if session["status"] != "running" or session["lease_owner"] != site_code:
                raise ConflictError("游览场次未由当前执行站点持有")
            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM pilot_observations WHERE session_id=?", (session_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO pilot_observations(session_id,version,observation_json,metrics_json,observation_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (session_id, version, json.dumps(observation, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest({"observation": observation, "metrics": metrics}), site_code, now),
            )
            connection.execute(
                "UPDATE pilot_sessions SET status='succeeded',current_observation_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def fail(self, session_id: int, site_code: str, error_code: str, message: str, retryable: bool) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] == "cancel_requested":
                raise ConflictError("游览场次已进入停止流程，请确认停止而不是报告失败")
            if session["status"] != "running" or session["lease_owner"] != site_code:
                raise ConflictError("游览场次未由当前执行站点持有")
            can_retry = retryable and int(session["attempt_count"]) < int(session["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(session["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            connection.execute(
                "UPDATE pilot_sessions SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (status, available, error_code, message[:2000], None if can_retry else now, now, session_id),
            )
            return dict(repository.session_by_id(session_id))

    def cancel(self, session_id: int, actor: str, reason: str, batch_key: str = "") -> dict[str, Any]:
        """登记停止请求：从发起时起即构成必须兑现的业务承诺。

        排队中的场次立即取消；运行中的场次进入 cancel_requested，保留租约
        作为超时兜底，等待节点确认或租约过期后由恢复入口沿取消方向关闭。
        重复申请直接返回当前状态，不改写已确立的请求人、原因与时间。
        """
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] in {"cancel_requested", "cancelled"}:
                return dict(session)
            if session["status"] not in {"queued", "running"}:
                raise ConflictError("当前游览场次状态不允许取消")
            before = dict(session)
            if session["status"] == "queued":
                connection.execute(
                    "UPDATE pilot_sessions SET status='cancelled',stop_outcome='confirmed',cancel_requested_by=?,cancel_reason=?,cancel_requested_at=?,finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                    (actor, reason, now, now, now, session_id),
                )
            else:
                connection.execute(
                    "UPDATE pilot_sessions SET status='cancel_requested',cancel_requested_by=?,cancel_reason=?,cancel_requested_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running'",
                    (actor, reason, now, now, session_id),
                )
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=actor, action="cancel", reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def confirm_cancel(self, session_id: int, site_code: str) -> dict[str, Any]:
        """执行站点确认停止请求：立即沿取消方向关闭场次。

        已经取消的场次（含恢复入口按超时关闭的）直接返回当前状态，
        迟到的确认不会改写已经确定的停止结果。
        """
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            if session["status"] == "cancelled":
                return dict(session)
            if session["status"] != "cancel_requested" or session["lease_owner"] != site_code:
                raise ConflictError("游览场次不在停止流程中或未由当前执行站点持有")
            before = dict(session)
            cursor = connection.execute(
                "UPDATE pilot_sessions SET status='cancelled',stop_outcome='confirmed',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='cancel_requested' AND lease_owner=?",
                (now, now, session_id, site_code),
            )
            if cursor.rowcount != 1:
                return dict(repository.session_by_id(session_id))
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=site_code, action="cancel_confirmed", reason="执行站点确认停止请求", before=before, after=after, batch_key="", now=now)
            return after

    def retry(self, session_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消游览场次可以人工重试")
            chosen = session["priority"] if priority is None else priority
            connection.execute("UPDATE pilot_sessions SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',cancel_requested_by='',cancel_reason='',cancel_requested_at='',stop_outcome='',finished_at=NULL,updated_at=?,version=version+1 WHERE id=?", (chosen, now, now, session["id"]))
        return self._intervene(session_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, session_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, session: sqlite3.Row, now: str) -> None:
            if session["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的游览场次可以调整优先级")
            connection.execute("UPDATE pilot_sessions SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, session["id"]))
        return self._intervene(session_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "session_ids": payload["session_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for session_id in list(dict.fromkeys(payload["session_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(session_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(session_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(session_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"session_id": session_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"session_id": session_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-site") -> dict[str, Any]:
        """恢复租约过期的场次。

        运行中的场次按既有规则重新排队或转失败；处于停止流程
        （cancel_requested）的场次不再回到待分配队列，而是沿取消方向
        以超时方式关闭，兑现停止承诺。重复运行不会改写已确定的结果。
        """
        now = to_storage(self.clock.now())
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            rows = connection.execute(
                "SELECT * FROM pilot_sessions WHERE (status='running' AND lease_expires_at<>'' AND lease_expires_at<?) OR (status='cancel_requested' AND (lease_expires_at='' OR lease_expires_at<?)) ORDER BY id",
                (now, now),
            ).fetchall()
            for session in rows:
                before = dict(session)
                if session["status"] == "cancel_requested":
                    cursor = connection.execute(
                        "UPDATE pilot_sessions SET status='cancelled',stop_outcome='timeout',lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=? AND status='cancel_requested'",
                        (now, now, session["id"]),
                    )
                    if cursor.rowcount != 1:
                        continue
                    cancelled.append(int(session["id"]))
                    after = dict(repository.session_by_id(session["id"]))
                    repository.add_intervention(session_id=session["id"], actor=actor, action="cancel_timeout", reason="租约过期未获节点确认，停止请求按超时兑现", before=before, after=after, batch_key="", now=now)
                    continue
                if int(session["attempt_count"]) < int(session["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(session["id"]))
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(session["id"]))
                connection.execute(
                    "UPDATE pilot_sessions SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='执行站点租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, now, finished_at, now, session["id"]),
                )
                after = dict(repository.session_by_id(session["id"]))
                repository.add_intervention(session_id=session["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM pilot_sessions GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM pilot_sessions WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "protocols": len(self.repository.active_protocols())}

    def _intervene(self, session_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = PilotRepository(connection)
            session = repository.session_by_id(session_id)
            if session is None:
                raise NotFoundError("运营游览场次不存在")
            before = dict(session)
            mutation(connection, session, now)
            after = dict(repository.session_by_id(session_id))
            repository.add_intervention(session_id=session_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            return after

    def _check_quota(self, repository: PilotRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队游览场次配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行游览场次配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数方案至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, protocol: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(protocol["parameter_schema_json"])
        values = {**json.loads(protocol["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含方案未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized


