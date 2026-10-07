#!/usr/bin/env python3
"""Airline disruption recovery engine using standard-library SQLite and HTTP."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, time, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8202
ROLES = {"viewer", "scheduler", "ops_manager", "auditor"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: Any = None):
        super().__init__(message)
        self.status, self.code, self.message, self.details = status, code, message, details


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    return (value or utcnow()).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None) -> datetime:
    if not value:
        raise ApiError(400, "time_required", "必须提供 ISO 8601 时间")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)


def parse_clock(value: str) -> time:
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_clock", f"时刻格式应为 HH:MM: {value}") from exc


def overlaps(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> bool:
    return a_start < b_end and b_start < a_end


class Repository:
    def __init__(self, db_path: str | Path):
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False, isolation_level=None)
        self.row_lock = threading.RLock()
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=10000")
        self._init()

    @contextmanager
    def tx(self):
        # 单连接共享给多线程，进程内串行化事务；SQLite WAL + busy_timeout 负责跨进程并发
        with self.row_lock:
            self.conn.execute("BEGIN IMMEDIATE")
            try:
                yield self.conn
                self.conn.execute("COMMIT")
            except Exception:
                self.conn.execute("ROLLBACK")
                raise

    def _init(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE IF NOT EXISTS permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0, UNIQUE(origin,destination,valid_from,valid_to));
            CREATE TABLE IF NOT EXISTS flights(
                id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                passenger_count INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0,
                revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS recovery_plans(
                id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL REFERENCES disruptions(id), name TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft', revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT,
                created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT
            );
            CREATE TABLE IF NOT EXISTS assignments(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                flight_id INTEGER NOT NULL REFERENCES flights(id), aircraft_id TEXT NOT NULL REFERENCES aircraft(id), crew_id TEXT NOT NULL REFERENCES crew(id),
                new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0,
                missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id)
            );
            CREATE TABLE IF NOT EXISTS connections(
                id INTEGER PRIMARY KEY AUTOINCREMENT, upstream_flight_id INTEGER NOT NULL REFERENCES flights(id),
                downstream_flight_id INTEGER NOT NULL REFERENCES flights(id), passenger_count INTEGER NOT NULL,
                min_connect_minutes INTEGER NOT NULL DEFAULT 60,
                UNIQUE(upstream_flight_id,downstream_flight_id)
            );
            CREATE TABLE IF NOT EXISTS connection_results(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL REFERENCES recovery_plans(id) ON DELETE CASCADE,
                connection_id INTEGER NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
                upstream_flight_id INTEGER NOT NULL, downstream_flight_id INTEGER NOT NULL,
                passenger_count INTEGER NOT NULL, connect_minutes INTEGER, min_connect_minutes INTEGER NOT NULL,
                status TEXT NOT NULL, reason TEXT, signature TEXT NOT NULL, UNIQUE(plan_id,connection_id)
            );
            CREATE TABLE IF NOT EXISTS settlements(
                id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL UNIQUE REFERENCES recovery_plans(id),
                plan_revision INTEGER NOT NULL, status TEXT NOT NULL, total_amount INTEGER NOT NULL,
                settled_by TEXT NOT NULL, settled_at TEXT NOT NULL, idempotency_key TEXT NOT NULL UNIQUE
            );
            CREATE TABLE IF NOT EXISTS settlement_ledger(
                id INTEGER PRIMARY KEY AUTOINCREMENT, settlement_id INTEGER NOT NULL REFERENCES settlements(id) ON DELETE CASCADE,
                plan_id INTEGER NOT NULL, plan_revision INTEGER NOT NULL, kind TEXT NOT NULL,
                flight_id INTEGER, upstream_flight_no TEXT, downstream_flight_no TEXT,
                passenger_count INTEGER NOT NULL DEFAULT 0, amount INTEGER NOT NULL,
                UNIQUE(plan_id,plan_revision,kind,flight_id,upstream_flight_no,downstream_flight_no)
            );
            CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            """
        )
        self._migrate()

    def _migrate(self) -> None:
        """升级旧库：补齐新列。旧航班没有中转记录，按可衔接处理（无记录即无错失）。"""
        cols = {r["name"] for r in self.conn.execute("PRAGMA table_info(assignments)")}
        if "flight_revision" not in cols:
            self.conn.execute("ALTER TABLE assignments ADD COLUMN flight_revision INTEGER NOT NULL DEFAULT 0")

    @staticmethod
    def audit(conn: sqlite3.Connection, plan_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute("INSERT INTO audit_log(plan_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
                     (plan_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()))


class AirlineRecoveryService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

    @staticmethod
    def identity(headers: Any) -> tuple[str, str]:
        actor, role = headers.get("X-User-Id", "").strip(), headers.get("X-Role", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效 X-Role")
        return actor, role

    @staticmethod
    def _dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    @staticmethod
    def _attach_connections(conn: sqlite3.Connection, flight_row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        data = dict(flight_row)
        data["connections"] = [dict(r) for r in conn.execute(
            """SELECT c.*, df.flight_no downstream_flight_no, df.origin downstream_origin, df.destination downstream_destination
               FROM connections c JOIN flights df ON df.id=c.downstream_flight_id
               WHERE c.upstream_flight_id=? ORDER BY df.std""", (data["id"],))]
        return data

    def _flight_with_connections(self, conn: sqlite3.Connection, flight_id: int) -> dict[str, Any]:
        row = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
        return self._attach_connections(conn, row)

    def seed_airport(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager":
            raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机场数据")
        code = str(body.get("code", "")).upper().strip()
        country = str(body.get("country", "")).upper().strip()
        if not code or not country:
            raise ApiError(400, "missing_fields", "code 和 country 必填")
        start = str(body.get("curfew_start", "23:00")); end = str(body.get("curfew_end", "06:00"))
        parse_clock(start); parse_clock(end)
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO airports(code,country,curfew_start,curfew_end) VALUES(?,?,?,?)", (code, country, start, end))
            return dict(conn.execute("SELECT * FROM airports WHERE code=?", (code,)).fetchone())

    def seed_aircraft(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护飞机")
        ident, model = str(body.get("id", "")).strip(), str(body.get("model", "")).strip()
        if not ident or not model: raise ApiError(400, "missing_fields", "id 和 model 必填")
        due = iso(parse_time(body.get("maintenance_due")))
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO aircraft(id,model,maintenance_due,status) VALUES(?,?,?,?)", (ident, model, due, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM aircraft WHERE id=?", (ident,)).fetchone())

    def seed_crew(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "seed_forbidden", "只有运行经理可以维护机组")
        ident, name, base = str(body.get("id", "")).strip(), str(body.get("name", "")).strip(), str(body.get("base", "")).upper().strip()
        duty = parse_time(body.get("duty_start")); maximum = body.get("max_duty_minutes")
        if not ident or not name or not base or not isinstance(maximum, int) or maximum <= 0:
            raise ApiError(400, "invalid_crew", "id、name、base 和正整数 max_duty_minutes 必填")
        with self.repo.tx() as conn:
            conn.execute("INSERT OR REPLACE INTO crew(id,name,base,duty_start,max_duty_minutes,status) VALUES(?,?,?,?,?,?)", (ident, name, base, iso(duty), maximum, body.get("status", "active")))
            return dict(conn.execute("SELECT * FROM crew WHERE id=?", (ident,)).fetchone())

    def create_permit(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "permit_forbidden", "只有运行经理可以维护航线许可")
        origin, destination = str(body.get("origin", "")).upper(), str(body.get("destination", "")).upper()
        if not origin or not destination: raise ApiError(400, "missing_fields", "origin 和 destination 必填")
        valid_from, valid_to = parse_time(body.get("valid_from")), parse_time(body.get("valid_to"))
        if valid_to <= valid_from: raise ApiError(400, "invalid_permit", "许可结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            try:
                cur = conn.execute("INSERT INTO permits(origin,destination,valid_from,valid_to,curfew_exempt) VALUES(?,?,?,?,?)",
                                   (origin, destination, iso(valid_from), iso(valid_to), int(bool(body.get("curfew_exempt")))))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "permit_exists", "相同航线与有效期的许可已存在") from exc
            return dict(conn.execute("SELECT * FROM permits WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_flight(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "flight_forbidden", "当前角色不能创建航班")
        required = ("flight_no", "origin", "destination", "std", "sta", "aircraft_id", "crew_id")
        if any(not body.get(k) for k in required): raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(k for k in required if not body.get(k))}")
        std, sta = parse_time(body["std"]), parse_time(body["sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
        passengers = body.get("passenger_count", 0)
        if not isinstance(passengers, int) or passengers < 0: raise ApiError(400, "invalid_passengers", "passenger_count 必须是非负整数")
        connections = body.get("connections", [])
        if not isinstance(connections, list): raise ApiError(400, "invalid_connections", "connections 必须是列表")
        with self.repo.tx() as conn:
            for table, ident in (("aircraft", body["aircraft_id"]), ("crew", body["crew_id"])):
                row = conn.execute(f"SELECT status FROM {table} WHERE id=?", (ident,)).fetchone()
                if not row or row["status"] != "active": raise ApiError(409, "resource_unavailable", f"{table} {ident} 不可用")
            try:
                cur = conn.execute("""INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,updated_at)
                                      VALUES(?,?,?,?,?,?,?,?,?)""",
                                   (body["flight_no"].upper(), body["origin"].upper(), body["destination"].upper(), iso(std), iso(sta),
                                    body["aircraft_id"], body["crew_id"], passengers, iso()))
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "flight_exists", "航班号已存在") from exc
            flight_id = cur.lastrowid
            for item in connections:
                self._insert_connection(conn, flight_id, item)
            return self._flight_with_connections(conn, flight_id)

    def _parse_connection_body(self, body: dict[str, Any]) -> tuple[int, int, int, int]:
        downstream_id, count, mct = body.get("downstream_flight_id"), body.get("passenger_count"), body.get("min_connect_minutes", 60)
        if not isinstance(downstream_id, int) or not isinstance(count, int) or count < 0:
            raise ApiError(400, "invalid_connection", "downstream_flight_id 必须为整数，passenger_count 必须是非负整数")
        if not isinstance(mct, int) or mct < 0:
            raise ApiError(400, "invalid_connection", "min_connect_minutes 必须是非负整数")
        return downstream_id, count, mct

    def _insert_connection(self, conn: sqlite3.Connection, upstream_flight_id: int, body: dict[str, Any]) -> dict[str, Any]:
        downstream_flight_id, count, mct = self._parse_connection_body(body)
        if downstream_flight_id == upstream_flight_id:
            raise ApiError(400, "invalid_connection", "下游航班不能是航班自身")
        upstream = conn.execute("SELECT * FROM flights WHERE id=?", (upstream_flight_id,)).fetchone()
        downstream = conn.execute("SELECT * FROM flights WHERE id=?", (downstream_flight_id,)).fetchone()
        if not upstream or not downstream: raise ApiError(404, "flight_not_found", "中转航段不存在")
        if upstream["destination"] != downstream["origin"]:
            raise ApiError(400, "airport_mismatch", "中转衔接要求上游到达机场等于下游起飞机场")
        if parse_time(downstream["std"]) <= parse_time(upstream["sta"]):
            raise ApiError(400, "invalid_connection", "下游计划起飞时间必须晚于上游计划到达时间")
        if count > upstream["passenger_count"]:
            raise ApiError(400, "invalid_connection", "中转人数不能超过上游航班载客数")
        conn.execute("""INSERT INTO connections(upstream_flight_id,downstream_flight_id,passenger_count,min_connect_minutes)
                        VALUES(?,?,?,?) ON CONFLICT(upstream_flight_id,downstream_flight_id)
                        DO UPDATE SET passenger_count=excluded.passenger_count,min_connect_minutes=excluded.min_connect_minutes""",
                     (upstream_flight_id, downstream_flight_id, count, mct))
        row = conn.execute("SELECT * FROM connections WHERE upstream_flight_id=? AND downstream_flight_id=?",
                           (upstream_flight_id, downstream_flight_id)).fetchone()
        return dict(row)

    def create_connection(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "connection_forbidden", "当前角色不能登记中转衔接")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM flights WHERE id=?", (flight_id,)).fetchone():
                raise ApiError(404, "flight_not_found", "航班不存在")
            result = self._insert_connection(conn, flight_id, body)
            Repository.audit(conn, None, actor, role, "connection_registered",
                             {"upstream_flight_id": flight_id, "downstream_flight_id": result["downstream_flight_id"], "passenger_count": result["passenger_count"]})
            return result

    def create_disruption(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "disruption_forbidden", "当前角色不能登记中断")
        kind, resource = str(body.get("kind", "")).strip(), str(body.get("resource_id", "")).strip()
        if kind not in {"airport_closure", "aircraft_fault", "crew_timeout"} or not resource:
            raise ApiError(400, "invalid_disruption", "kind 或 resource_id 无效")
        start, end = parse_time(body.get("starts_at")), parse_time(body.get("ends_at"))
        if end <= start: raise ApiError(400, "invalid_times", "中断结束时间必须晚于开始时间")
        with self.repo.tx() as conn:
            cur = conn.execute("INSERT INTO disruptions(kind,resource_id,starts_at,ends_at,created_at) VALUES(?,?,?,?,?)",
                               (kind, resource.upper() if kind == "airport_closure" else resource, iso(start), iso(end), iso()))
            return dict(conn.execute("SELECT * FROM disruptions WHERE id=?", (cur.lastrowid,)).fetchone())

    def create_plan(self, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "plan_forbidden", "当前角色不能创建恢复方案")
        disruption_id, name = body.get("disruption_id"), str(body.get("name", "")).strip()
        assignments = body.get("assignments", [])
        if not isinstance(disruption_id, int) or not name or not isinstance(assignments, list):
            raise ApiError(400, "invalid_plan", "disruption_id、name 和 assignments 必填")
        with self.repo.tx() as conn:
            if not conn.execute("SELECT 1 FROM disruptions WHERE id=?", (disruption_id,)).fetchone():
                raise ApiError(404, "disruption_not_found", "中断事件不存在")
            cur = conn.execute("INSERT INTO recovery_plans(disruption_id,name,created_by,created_at) VALUES(?,?,?,?)", (disruption_id, name, actor, iso()))
            plan_id = cur.lastrowid
            for item in assignments:
                self._insert_assignment(conn, plan_id, item, replace=False)
            stale = self._recompute_connections(conn, plan_id)
            self._refresh_missed_counts(conn, plan_id)
            Repository.audit(conn, plan_id, actor, role, "plan_created", {"disruption_id": disruption_id, "assignment_count": len(assignments), "connections_invalidated": stale})
            return self._plan_payload(conn, plan_id)

    def _insert_assignment(self, conn: sqlite3.Connection, plan_id: int, item: dict[str, Any], replace: bool) -> None:
        required = ("flight_id", "aircraft_id", "crew_id", "new_std", "new_sta")
        if any(item.get(k) in (None, "") for k in required): raise ApiError(400, "invalid_assignment", f"飞行调整缺少字段: {', '.join(k for k in required if item.get(k) in (None, ''))}")
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
        std, sta = parse_time(item["new_std"]), parse_time(item["new_sta"])
        if sta <= std: raise ApiError(400, "invalid_times", "新到达时间必须晚于新起飞时间")
        flight = conn.execute("SELECT * FROM flights WHERE id=?", (item["flight_id"],)).fetchone()
        if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
        # 航班级乐观锁：调度员基于某一航班版本提交调整，航班已被他人更新则拒绝并返回最新结果
        expected_flight_revision = item.get("expected_flight_revision")
        if expected_flight_revision is not None:
            if not isinstance(expected_flight_revision, int):
                raise ApiError(400, "revision_required", "expected_flight_revision 必须是整数")
            if flight["revision"] != expected_flight_revision:
                raise ApiError(409, "flight_revision_conflict", "该航班时刻已被其他人更新，请基于最新结果重算",
                               {"expected": expected_flight_revision, "current": flight["revision"],
                                "latest_flight": self._attach_connections(conn, flight)})
        if flight["status"] == "canceled" and item.get("status", "planned") != "canceled":
            raise ApiError(409, "canceled_flight", "已取消航班不能安排执行")
        delay = int((std - parse_time(flight["std"])).total_seconds() // 60)
        # 错失中转由系统按当前时刻重算，忽略手填数字
        try:
            if replace:
                conn.execute("""UPDATE assignments SET aircraft_id=?,crew_id=?,new_std=?,new_sta=?,status=?,delay_minutes=?,
                                missed_connections=0,flight_revision=? WHERE plan_id=? AND flight_id=?""",
                             (item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay,
                              flight["revision"], plan_id, item["flight_id"]))
                if conn.execute("SELECT changes()").fetchone()[0] == 0:
                    raise KeyError
            else:
                conn.execute("""INSERT INTO assignments(plan_id,flight_id,aircraft_id,crew_id,new_std,new_sta,status,delay_minutes,missed_connections,flight_revision)
                                VALUES(?,?,?,?,?,?,?,?,0,?)""",
                             (plan_id, item["flight_id"], item["aircraft_id"], item["crew_id"], iso(std), iso(sta), item.get("status", "planned"), delay, flight["revision"]))
        except sqlite3.IntegrityError as exc:
            raise ApiError(409, "assignment_conflict", "方案中该航班已存在或资源无效") from exc
        except KeyError as exc:
            raise ApiError(404, "assignment_not_found", "待替换的航班调整不存在") from exc

    def add_assignment(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "assignment_forbidden", "当前角色不能修改方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "draft": raise ApiError(409, "plan_locked", "已锁定方案不能修改")
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案已被其他人更新")
            self._insert_assignment(conn, plan_id, body, replace=True)
            stale = self._recompute_connections(conn, plan_id)
            conn.execute("UPDATE recovery_plans SET revision=revision+1 WHERE id=?", (plan_id,))
            Repository.audit(conn, plan_id, actor, role, "assignment_reassigned",
                             {"flight_id": body.get("flight_id"), "connections_invalidated": stale})
            return self._plan_payload(conn, plan_id)

    def _plan_times(self, conn: sqlite3.Connection, plan_id: int) -> dict[int, dict[str, Any]]:
        """方案内航班的生效时刻：有调整用新时刻，否则用计划时刻。"""
        result: dict[int, dict[str, Any]] = {}
        for a in conn.execute("SELECT * FROM assignments WHERE plan_id=?", (plan_id,)):
            result[a["flight_id"]] = {"std": a["new_std"], "sta": a["new_sta"], "status": a["status"]}
        return result

    def _recompute_connections(self, conn: sqlite3.Connection, plan_id: int) -> int:
        """按方案当前生效时刻重算所有受影响中转。时刻一变，旧结论签名失效并整体重写。

        返回被判定失效的旧结论条数（供调用方写入审计）。
        """
        plan_times = self._plan_times(conn, plan_id)
        if not plan_times:
            conn.execute("DELETE FROM connection_results WHERE plan_id=?", (plan_id,))
            return 0
        placeholders = ",".join("?" for _ in plan_times)
        links = conn.execute(
            f"""SELECT c.*, uf.flight_no upstream_flight_no, uf.origin upstream_origin, uf.destination upstream_destination,
                       uf.std upstream_std, uf.sta upstream_sta, uf.status upstream_status, uf.passenger_count upstream_capacity,
                       df.flight_no downstream_flight_no, df.origin downstream_origin, df.destination downstream_destination,
                       df.std downstream_std, df.sta downstream_sta, df.status downstream_status
                FROM connections c
                JOIN flights uf ON uf.id=c.upstream_flight_id JOIN flights df ON df.id=c.downstream_flight_id
                WHERE c.upstream_flight_id IN ({placeholders}) OR c.downstream_flight_id IN ({placeholders})""",
            (*plan_times.keys(), *plan_times.keys())).fetchall()
        evaluated: list[dict[str, Any]] = []
        for c in links:
            up = plan_times.get(c["upstream_flight_id"], {"std": c["upstream_std"], "sta": c["upstream_sta"], "status": c["upstream_status"]})
            down = plan_times.get(c["downstream_flight_id"], {"std": c["downstream_std"], "sta": c["downstream_sta"], "status": c["downstream_status"]})
            up_arr, down_dep = parse_time(up["sta"]), parse_time(down["std"])
            connect_minutes = int((down_dep - up_arr).total_seconds() // 60)
            status, reason = "connectable", None
            if up["status"] == "canceled" or down["status"] == "canceled":
                status, reason = "missed", "flight_canceled"
            elif connect_minutes < c["min_connect_minutes"]:
                status, reason = "missed", "below_min_connect"
            # 签名含所有生效时刻：上游或下游任一趟时刻变化都会使旧结论失效
            signature = f"{c['id']}|{up['std']}|{up['sta']}|{up['status']}|{down['std']}|{down['sta']}|{down['status']}|{c['passenger_count']}|{c['min_connect_minutes']}"
            evaluated.append({"connection_id": c["id"], "upstream_flight_id": c["upstream_flight_id"],
                              "downstream_flight_id": c["downstream_flight_id"], "passenger_count": c["passenger_count"],
                              "connect_minutes": connect_minutes, "min_connect_minutes": c["min_connect_minutes"],
                              "status": status, "reason": reason, "signature": signature})
        existing = {r["connection_id"]: r for r in conn.execute("SELECT * FROM connection_results WHERE plan_id=?", (plan_id,))}
        stale = sum(1 for e in evaluated if e["connection_id"] in existing and existing[e["connection_id"]]["signature"] != e["signature"])
        stale += sum(1 for cid in existing if cid not in {e["connection_id"] for e in evaluated})
        conn.execute("DELETE FROM connection_results WHERE plan_id=?", (plan_id,))
        for e in evaluated:
            conn.execute("""INSERT INTO connection_results(plan_id,connection_id,upstream_flight_id,downstream_flight_id,
                passenger_count,connect_minutes,min_connect_minutes,status,reason,signature)
                VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (plan_id, e["connection_id"], e["upstream_flight_id"], e["downstream_flight_id"], e["passenger_count"],
                 e["connect_minutes"], e["min_connect_minutes"], e["status"], e["reason"], e["signature"]))
        return stale

    def _missed_links(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        rows = conn.execute(
            """SELECT cr.*, uf.flight_no upstream_flight_no, df.flight_no downstream_flight_no,
                      uf.destination transfer_airport
               FROM connection_results cr
               JOIN flights uf ON uf.id=cr.upstream_flight_id JOIN flights df ON df.id=cr.downstream_flight_id
               WHERE cr.plan_id=? AND cr.status='missed' ORDER BY df.flight_no""", (plan_id,)).fetchall()
        return [dict(r) for r in rows]

    def _refresh_missed_counts(self, conn: sqlite3.Connection, plan_id: int) -> None:
        conn.execute("UPDATE assignments SET missed_connections=0 WHERE plan_id=?", (plan_id,))
        for row in conn.execute(
                """SELECT cr.upstream_flight_id, SUM(cr.passenger_count) missed
                   FROM connection_results cr WHERE cr.plan_id=? AND cr.status='missed'
                   GROUP BY cr.upstream_flight_id""", (plan_id,)):
            conn.execute("UPDATE assignments SET missed_connections=? WHERE plan_id=? AND flight_id=?",
                         (row["missed"], plan_id, row["upstream_flight_id"]))

    def _validate_plan(self, conn: sqlite3.Connection, plan_id: int) -> list[dict[str, Any]]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        rows = [dict(r) for r in conn.execute("""SELECT a.*, f.flight_no, f.origin, f.destination, f.passenger_count, f.status flight_status
                                                  FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        if not rows: raise ApiError(409, "empty_plan", "方案没有飞行调整")
        problems: list[dict[str, Any]] = []
        by_aircraft: dict[str, list[dict[str, Any]]] = {}
        by_crew: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if row["status"] == "canceled": continue
            std, sta = parse_time(row["new_std"]), parse_time(row["new_sta"])
            aircraft = conn.execute("SELECT * FROM aircraft WHERE id=?", (row["aircraft_id"],)).fetchone()
            crew = conn.execute("SELECT * FROM crew WHERE id=?", (row["crew_id"],)).fetchone()
            origin = conn.execute("SELECT * FROM airports WHERE code=?", (row["origin"],)).fetchone()
            destination = conn.execute("SELECT * FROM airports WHERE code=?", (row["destination"],)).fetchone()
            if not aircraft or aircraft["status"] != "active": problems.append({"assignment_id": row["id"], "code": "aircraft_unavailable"})
            elif parse_time(aircraft["maintenance_due"]) < sta: problems.append({"assignment_id": row["id"], "code": "maintenance_due", "resource": aircraft["id"]})
            if not crew or crew["status"] != "active": problems.append({"assignment_id": row["id"], "code": "crew_unavailable"})
            if not origin or not destination: problems.append({"assignment_id": row["id"], "code": "airport_unknown"})
            if crew:
                duty_start, max_duty = parse_time(crew["duty_start"]), crew["max_duty_minutes"]
                if (sta - duty_start).total_seconds() / 60 > max_duty: problems.append({"assignment_id": row["id"], "code": "duty_limit", "resource": crew["id"]})
            if destination:
                curfew_start, curfew_end = parse_clock(destination["curfew_start"]), parse_clock(destination["curfew_end"])
                permit = conn.execute("""SELECT * FROM permits WHERE origin=? AND destination=? AND valid_from<=? AND valid_to>=?""",
                                      (row["origin"], row["destination"], row["new_sta"], row["new_sta"])).fetchone()
                arrival_clock = sta.timetz().replace(tzinfo=None)
                inside = arrival_clock >= curfew_start or arrival_clock < curfew_end if curfew_start > curfew_end else curfew_start <= arrival_clock < curfew_end
                if inside and not (permit and permit["curfew_exempt"]): problems.append({"assignment_id": row["id"], "code": "airport_curfew"})
                if row["origin"] != row["destination"] and not permit: problems.append({"assignment_id": row["id"], "code": "route_permit_missing"})
                elif row["origin"] != row["destination"] and not (parse_time(permit["valid_from"]) <= std <= parse_time(permit["valid_to"])):
                    problems.append({"assignment_id": row["id"], "code": "route_permit_window"})
            by_aircraft.setdefault(row["aircraft_id"], []).append(row)
            by_crew.setdefault(row["crew_id"], []).append(row)
        for bucket_name, buckets in (("aircraft", by_aircraft), ("crew", by_crew)):
            for resource, items in buckets.items():
                for i, left in enumerate(items):
                    for right in items[i + 1:]:
                        if overlaps(parse_time(left["new_std"]), parse_time(left["new_sta"]), parse_time(right["new_std"]), parse_time(right["new_sta"])):
                            problems.append({"code": f"{bucket_name}_overlap", "resource": resource, "assignments": [left["id"], right["id"]]})
        return problems

    def validate_plan(self, plan_id: int, actor: str, role: str) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager", "auditor"}: raise ApiError(403, "validate_forbidden", "当前角色不能校验方案")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            stale = self._recompute_connections(conn, plan_id)
            self._refresh_missed_counts(conn, plan_id)
            problems = self._validate_plan(conn, plan_id)
            if not problems and plan["status"] == "draft":
                metrics = self._metrics(conn, plan_id)
                conn.execute("UPDATE recovery_plans SET metrics_json=?,score_json=? WHERE id=?", (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), plan_id))
            missed = self._missed_links(conn, plan_id)
            Repository.audit(conn, plan_id, actor, role, "plan_validated", {"valid": not problems, "connections_invalidated": stale, "missed_connections": sum(m["passenger_count"] for m in missed)})
            return {"valid": not problems, "problems": problems, "connections_invalidated": stale,
                    "missed_connections_detail": missed, "plan": self._plan_payload(conn, plan_id)}

    def _metrics(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        rows = conn.execute("""SELECT a.*,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)).fetchall()
        canceled = sum(1 for row in rows if row["status"] == "canceled")
        missed = conn.execute("SELECT COALESCE(SUM(passenger_count),0) total FROM connection_results WHERE plan_id=? AND status='missed'", (plan_id,)).fetchone()["total"]
        return {"flight_count": len(rows), "canceled": canceled, "total_delay_minutes": sum(max(0, row["delay_minutes"]) for row in rows),
                "affected_passengers": sum(row["passenger_count"] for row in rows), "missed_connections": missed}

    @staticmethod
    def _score(metrics: dict[str, Any]) -> dict[str, int]:
        score = metrics["canceled"] * 100000 + metrics["missed_connections"] * 5000 + metrics["total_delay_minutes"] * 100 + metrics["affected_passengers"]
        return {"cost_score": score, "lower_is_better": 1}

    def lock_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "lock_forbidden", "只有运行经理可以锁定恢复方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "locked": return self._plan_payload(conn, plan_id)
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案版本已变化")
            self._recompute_connections(conn, plan_id)
            self._refresh_missed_counts(conn, plan_id)
            stale_flights = [dict(r) for r in conn.execute(
                """SELECT a.flight_id, a.flight_revision AS submitted_revision, f.revision AS current_revision, f.flight_no
                   FROM assignments a JOIN flights f ON f.id=a.flight_id
                   WHERE a.plan_id=? AND a.flight_revision>0 AND a.flight_revision!=f.revision""", (plan_id,))]
            if stale_flights:
                raise ApiError(409, "flight_revision_conflict", "方案中存在基于过时航班时刻的调整，请按最新结果重算", stale_flights)
            problems = self._validate_plan(conn, plan_id)
            if problems: raise ApiError(409, "plan_invalid", "方案未通过约束校验", problems)
            conflicts = []
            for row in conn.execute("SELECT * FROM assignments WHERE plan_id=? AND status!='canceled'", (plan_id,)).fetchall():
                conflicting = conn.execute("""SELECT a.*,p.name plan_name FROM assignments a JOIN recovery_plans p ON p.id=a.plan_id
                    WHERE p.id!=? AND p.status='locked' AND a.status!='canceled' AND (a.aircraft_id=? OR a.crew_id=?)
                    AND a.new_std<? AND a.new_sta>?""",
                    (plan_id, row["aircraft_id"], row["crew_id"], row["new_sta"], row["new_std"])).fetchall()
                conflicts.extend({"assignment_id": row["id"], "conflict_plan_id": item["plan_id"], "conflict_plan": item["plan_name"], "resource": item["aircraft_id"] if item["aircraft_id"] == row["aircraft_id"] else item["crew_id"]} for item in conflicting)
            if conflicts: raise ApiError(409, "locked_resource_conflict", "与已锁定方案存在飞机或机组冲突", conflicts)
            metrics = self._metrics(conn, plan_id)
            conn.execute("""UPDATE recovery_plans SET status='locked',metrics_json=?,score_json=?,locked_at=?,locked_by=? WHERE id=?""",
                         (json.dumps(metrics, ensure_ascii=False), json.dumps(self._score(metrics)), iso(), actor, plan_id))
            for row in conn.execute("""SELECT a.*,f.flight_no FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? AND a.status!='canceled'""", (plan_id,)):
                conn.execute("UPDATE flights SET std=?,sta=?,aircraft_id=?,crew_id=?,delay_minutes=?,revision=revision+1,updated_at=? WHERE id=?",
                             (row["new_std"], row["new_sta"], row["aircraft_id"], row["crew_id"], max(0, row["delay_minutes"]), iso(), row["flight_id"]))
                conn.execute("UPDATE assignments SET status='active' WHERE id=?", (row["id"],))
            Repository.audit(conn, plan_id, actor, role, "plan_locked", {"metrics": metrics})
            return self._plan_payload(conn, plan_id)

    def _settlement_payload(self, conn: sqlite3.Connection, settlement_id: int, idempotent: bool = False) -> dict[str, Any]:
        settlement = dict(conn.execute("SELECT * FROM settlements WHERE id=?", (settlement_id,)).fetchone())
        ledger = [dict(r) for r in conn.execute("SELECT * FROM settlement_ledger WHERE settlement_id=? ORDER BY id", (settlement_id,))]
        settlement["ledger"] = ledger
        settlement["idempotent_replay"] = idempotent
        return settlement

    def settle_plan(self, plan_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "ops_manager": raise ApiError(403, "settle_forbidden", "只有运行经理可以结算方案")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] != "locked": raise ApiError(409, "plan_not_locked", "只有已锁定方案可以结算")
            if plan["revision"] != expected: raise ApiError(409, "revision_conflict", "方案版本已变化，请按最新版本重试结算")
            previous = conn.execute("SELECT * FROM settlements WHERE plan_id=?", (plan_id,)).fetchone()
            if previous:
                # 同一方案版本的成功结算直接回放，重试不产生第二笔账
                if previous["plan_revision"] == plan["revision"] and previous["status"] == "settled":
                    Repository.audit(conn, plan_id, actor, role, "plan_settlement_replayed", {"settlement_id": previous["id"]})
                    return self._settlement_payload(conn, previous["id"], idempotent=True)
                raise ApiError(409, "settlement_version_mismatch", "已有结算对应不同方案版本", {"settled_revision": previous["plan_revision"], "current_revision": plan["revision"]})
            self._recompute_connections(conn, plan_id)
            self._refresh_missed_counts(conn, plan_id)
            entries: list[tuple[str, int | None, str | None, str | None, int, int]] = []
            for a in conn.execute("""SELECT a.*,f.flight_no,f.passenger_count FROM assignments a JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=?""", (plan_id,)):
                if a["status"] == "canceled":
                    entries.append(("canceled_flight", a["flight_id"], a["flight_no"], None, a["passenger_count"], 100000))
                elif a["delay_minutes"] > 0:
                    entries.append(("delay", a["flight_id"], a["flight_no"], None, a["passenger_count"], max(0, a["delay_minutes"]) * 100))
            for link in self._missed_links(conn, plan_id):
                entries.append(("missed_connection", None, link["upstream_flight_no"], link["downstream_flight_no"], link["passenger_count"], link["passenger_count"] * 5000))
            # 模拟外部结算网关失败：整笔回滚，不落任何台账；按同一方案版本重试即可
            if body.get("fail_gateway"):
                raise ApiError(502, "settlement_gateway", "结算网关失败，方案未记账，可按同一方案版本重试",
                               {"idempotency_key": f"plan-{plan_id}-rev{plan['revision']}", "entries_preview": len(entries)})
            idempotency_key = f"plan-{plan_id}-rev{plan['revision']}"
            cur = conn.execute("""INSERT INTO settlements(plan_id,plan_revision,status,total_amount,settled_by,settled_at,idempotency_key)
                                  VALUES(?,?,?,?,?,?,?)""",
                               (plan_id, plan["revision"], "settled", 0, actor, iso(), idempotency_key))
            settlement_id = cur.lastrowid
            total = 0
            for kind, flight_id, up_no, down_no, count, amount in entries:
                # 唯一键 (plan_id,plan_revision,kind,航班,航段对) 保证同批旅客不会记两遍
                conn.execute("""INSERT INTO settlement_ledger(settlement_id,plan_id,plan_revision,kind,flight_id,upstream_flight_no,downstream_flight_no,passenger_count,amount)
                                VALUES(?,?,?,?,?,?,?,?,?)""",
                             (settlement_id, plan_id, plan["revision"], kind, flight_id, up_no, down_no, count, amount))
                total += amount
            conn.execute("UPDATE settlements SET total_amount=? WHERE id=?", (total, settlement_id))
            Repository.audit(conn, plan_id, actor, role, "plan_settled", {"settlement_id": settlement_id, "total_amount": total, "entries": len(entries)})
            return self._settlement_payload(conn, settlement_id)

    def cancel_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "cancel_forbidden", "当前角色不能取消航班")
        reason = str(body.get("reason", "")).strip()
        if not reason: raise ApiError(400, "reason_required", "取消原因必填")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["status"] == "canceled": return {"flight": self._attach_connections(conn, flight), "idempotent": True}
            conn.execute("UPDATE flights SET status='canceled',cancel_reason=?,revision=revision+1,updated_at=? WHERE id=?", (reason, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_canceled", {"flight_id": flight_id, "reason": reason})
            return {"flight": self._flight_with_connections(conn, flight_id), "idempotent": False}

    def recover_flight(self, flight_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"scheduler", "ops_manager"}: raise ApiError(403, "recover_forbidden", "当前角色不能恢复航班")
        expected = body.get("expected_revision")
        if not isinstance(expected, int): raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        with self.repo.tx() as conn:
            flight = conn.execute("SELECT * FROM flights WHERE id=?", (flight_id,)).fetchone()
            if not flight: raise ApiError(404, "flight_not_found", "航班不存在")
            if flight["revision"] != expected: raise ApiError(409, "revision_conflict", "航班版本已变化")
            if flight["status"] != "canceled": raise ApiError(409, "not_canceled", "只有取消航班可以恢复")
            std, sta = parse_time(body.get("new_std")), parse_time(body.get("new_sta"))
            if sta <= std: raise ApiError(400, "invalid_times", "到达时间必须晚于起飞时间")
            aircraft_id, crew_id = body.get("aircraft_id", flight["aircraft_id"]), body.get("crew_id", flight["crew_id"])
            conn.execute("""UPDATE flights SET status='scheduled',std=?,sta=?,aircraft_id=?,crew_id=?,cancel_reason=NULL,
                            delay_minutes=0,revision=revision+1,updated_at=? WHERE id=?""",
                         (iso(std), iso(sta), aircraft_id, crew_id, iso(), flight_id))
            Repository.audit(conn, None, actor, role, "flight_recovered", {"flight_id": flight_id})
            return {"flight": self._flight_with_connections(conn, flight_id)}

    def _plan_payload(self, conn: sqlite3.Connection, plan_id: int) -> dict[str, Any]:
        plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
        if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
        assignments = [dict(r) for r in conn.execute(
            """SELECT a.*,f.flight_no,f.origin,f.destination,f.passenger_count FROM assignments a
               JOIN flights f ON f.id=a.flight_id WHERE a.plan_id=? ORDER BY a.new_std""", (plan_id,))]
        missed = self._missed_links(conn, plan_id)
        missed_by_upstream: dict[int, list[dict[str, Any]]] = {}
        for link in missed:
            missed_by_upstream.setdefault(link["upstream_flight_id"], []).append(
                {"downstream_flight_no": link["downstream_flight_no"], "passenger_count": link["passenger_count"],
                 "connect_minutes": link["connect_minutes"], "min_connect_minutes": link["min_connect_minutes"], "reason": link["reason"]})
        for item in assignments:
            item["missed_connection_details"] = missed_by_upstream.get(item["flight_id"], [])
        result = dict(plan)
        result = dict(plan)
        # draft 方案时刻随时可能变化，指标按当前结论实时计算；锁定后才使用冻结快照
        result["metrics"] = self._metrics(conn, plan_id) if plan["status"] == "draft" else (json.loads(plan["metrics_json"]) if plan["metrics_json"] else self._metrics(conn, plan_id))
        result["score"] = json.loads(plan["score_json"]) if plan["score_json"] else None
        result["assignments"] = assignments
        result["missed_connections_detail"] = missed
        result["connection_results"] = [dict(r) for r in conn.execute(
            """SELECT cr.connection_id,cr.upstream_flight_id,cr.downstream_flight_id,uf.flight_no upstream_flight_no,
                      df.flight_no downstream_flight_no,cr.passenger_count,cr.connect_minutes,cr.min_connect_minutes,
                      cr.status,cr.reason,cr.signature
               FROM connection_results cr JOIN flights uf ON uf.id=cr.upstream_flight_id JOIN flights df ON df.id=cr.downstream_flight_id
               WHERE cr.plan_id=? ORDER BY df.flight_no""", (plan_id,))]
        return result

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        conn = self.repo.conn
        with self.repo.row_lock:
            plan = conn.execute("SELECT * FROM recovery_plans WHERE id=?", (plan_id,)).fetchone()
            if not plan: raise ApiError(404, "plan_not_found", "方案不存在")
            if plan["status"] == "draft":
                # 草稿方案：其他调度员改了航班时刻后，旧结论立即失效并重算
                with self.repo.tx() as tx_conn:
                    self._recompute_connections(tx_conn, plan_id)
                    self._refresh_missed_counts(tx_conn, plan_id)
                    return self._plan_payload(tx_conn, plan_id)
            # 锁定方案时刻已冻结，直接返回冻结结果
            return self._plan_payload(conn, plan_id)

    def compare_plans(self, disruption_id: int) -> dict[str, Any]:
        plans = []
        with self.repo.row_lock:
            for row in self.repo.conn.execute("SELECT id FROM recovery_plans WHERE disruption_id=? ORDER BY id", (disruption_id,)):
                plan = self.get_plan(row["id"])
                if not plan["score"]:
                    problems = self._validate_plan(self.repo.conn, row["id"])
                    plan["valid"] = not problems
                else:
                    plan["valid"] = True
                plans.append(plan)
        plans.sort(key=lambda item: item["score"]["cost_score"] if item["score"] else 10**18)
        return {"disruption_id": disruption_id, "recommended_plan_id": plans[0]["id"] if plans else None, "plans": plans}

    def state(self) -> dict[str, Any]:
        with self.repo.row_lock:
            conn = self.repo.conn
            flights = [self._attach_connections(conn, r) for r in conn.execute("SELECT * FROM flights ORDER BY std")]
            plans = [self.get_plan(r["id"]) for r in conn.execute("SELECT id FROM recovery_plans ORDER BY id DESC LIMIT 20")]
            return {"flights": flights, "disruptions": [dict(r) for r in conn.execute("SELECT * FROM disruptions ORDER BY id DESC")], "plans": plans, "server_time": iso()}


def respond(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode()
    handler.send_response(status); handler.send_header("Content-Type", "application/json; charset=utf-8"); handler.send_header("Content-Length", str(len(raw))); handler.end_headers(); handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: AirlineRecoveryService
    web_root: Path
    def log_message(self, fmt: str, *args: Any) -> None: print(f"{self.address_string()} - {fmt % args}")
    def body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length: return {}
        try: value = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc: raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(value, dict): raise ApiError(400, "invalid_json", "请求体必须是对象")
        return value
    def get_api(self, path: str) -> tuple[int, Any]:
        if path == "/health": return 200, {"status": "ok", "service": "airline-recovery"}
        actor, role = self.service.identity(self.headers)
        if path == "/api/state": return 200, self.service.state()
        parts = [p for p in path.split("/") if p]
        if len(parts) == 3 and parts[:2] == ["api", "plans"] and parts[2].isdigit(): return 200, self.service.get_plan(int(parts[2]))
        if len(parts) == 4 and parts[:2] == ["api", "disruptions"] and parts[2].isdigit() and parts[3] == "compare": return 200, self.service.compare_plans(int(parts[2]))
        raise ApiError(404, "not_found", "接口不存在")
    def post_api(self, path: str) -> tuple[int, Any]:
        actor, role = self.service.identity(self.headers); body = self.body(); parts = [p for p in path.split("/") if p]
        table = {
            "/api/airports": lambda: (201, self.service.seed_airport(actor, role, body)),
            "/api/aircraft": lambda: (201, self.service.seed_aircraft(actor, role, body)),
            "/api/crew": lambda: (201, self.service.seed_crew(actor, role, body)),
            "/api/permits": lambda: (201, self.service.create_permit(actor, role, body)),
            "/api/flights": lambda: (201, self.service.create_flight(actor, role, body)),
            "/api/disruptions": lambda: (201, self.service.create_disruption(actor, role, body)),
            "/api/recovery-plans": lambda: (201, self.service.create_plan(actor, role, body)),
        }
        if path in table: return table[path]()
        if len(parts) == 4 and parts[:2] == ["api", "plans"] and parts[2].isdigit():
            plan_id, action = int(parts[2]), parts[3]
            if action == "assignments": return 200, self.service.add_assignment(plan_id, actor, role, body)
            if action == "validate": return 200, self.service.validate_plan(plan_id, actor, role)
            if action == "lock": return 200, self.service.lock_plan(plan_id, actor, role, body)
            if action == "settle": return 200, self.service.settle_plan(plan_id, actor, role, body)
        if len(parts) == 4 and parts[:2] == ["api", "flights"] and parts[2].isdigit():
            flight_id, action = int(parts[2]), parts[3]
            if action == "cancel": return 200, self.service.cancel_flight(flight_id, actor, role, body)
            if action == "recover": return 200, self.service.recover_flight(flight_id, actor, role, body)
            if action == "connections": return 201, self.service.create_connection(flight_id, actor, role, body)
        raise ApiError(404, "not_found", "接口不存在")
    def handle_request(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                raw = (self.web_root / "index.html").read_bytes(); self.send_response(200); self.send_header("Content-Type", "text/html; charset=utf-8"); self.send_header("Content-Length", str(len(raw))); self.end_headers(); self.wfile.write(raw); return
            status, payload = self.get_api(parsed.path) if method == "GET" else self.post_api(parsed.path)
            respond(self, status, payload)
        except ApiError as exc:
            payload = {"error": exc.code, "message": exc.message}
            if exc.details is not None: payload["details"] = exc.details
            respond(self, exc.status, payload)
        except Exception as exc:
            print(f"unhandled error: {exc!r}"); respond(self, 500, {"error": "internal_error", "message": str(exc)})
    def do_GET(self) -> None: self.handle_request("GET")
    def do_POST(self) -> None: self.handle_request("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = AirlineRecoveryService(db_path)
    handler = type("AirlineHandler", (Handler,), {"service": service, "web_root": Path(__file__).resolve().parent / "static"})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser(); parser.add_argument("--host", default="127.0.0.1"); parser.add_argument("--port", type=int, default=PORT); parser.add_argument("--db", default=os.environ.get("AIRLINE_DB", "airline_recovery.db")); args = parser.parse_args()
    server = create_server(args.db, args.host, args.port); print(f"airline-recovery listening on http://{args.host}:{args.port}")
    try: server.serve_forever()
    except KeyboardInterrupt: pass
    finally: server.server_close()

if __name__ == "__main__": main()
