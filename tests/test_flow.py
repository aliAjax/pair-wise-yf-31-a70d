import sys, sqlite3, tempfile, threading, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class AirlineFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.svc.seed_airport("ops", "ops_manager", {"code": "CCC", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.create_permit("ops", "ops_manager", {"origin": "BBB", "destination": "CCC", "valid_from": iso(base - timedelta(days=1)), "valid_to": iso(base + timedelta(days=2))})
        self.base = base

    def tearDown(self): self.tmp.cleanup()

    def make_flight(self, number, aircraft, crew):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": "AAA", "destination": "BBB", "std": iso(self.base), "sta": iso(self.base + timedelta(hours=2)), "aircraft_id": aircraft, "crew_id": crew, "passenger_count": 150})

    def test_complete_recovery_and_manual_recovery(self):
        flight = self.make_flight("AB100", "AC1", "CR1")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "换飞机并延误", "assignments": [{"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=3)), "new_sta": iso(self.base + timedelta(hours=5)), "missed_connections": 4}]})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(locked["status"], "locked")
        self.assertEqual(locked["metrics"]["affected_passengers"], 150)
        canceled = self.svc.cancel_flight(flight["id"], "sched", "scheduler", {"reason": "后续机务检查"})["flight"]
        recovered = self.svc.recover_flight(flight["id"], "sched", "scheduler", {"expected_revision": canceled["revision"], "new_std": iso(self.base + timedelta(hours=8)), "new_sta": iso(self.base + timedelta(hours=10))})["flight"]
        self.assertEqual(recovered["status"], "scheduled")

    def test_locked_resource_conflict_and_permissions(self):
        flight1 = self.make_flight("AB101", "AC1", "CR1")
        disruption1 = self.svc.create_disruption("sched", "scheduler", {"kind": "crew_timeout", "resource_id": "CR1", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=2))})
        plan1 = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption1["id"], "name": "方案一", "assignments": [{"flight_id": flight1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2)), "new_sta": iso(self.base + timedelta(hours=4))}]})
        self.svc.lock_plan(plan1["id"], "ops", "ops_manager", {"expected_revision": 1})
        flight2 = self.make_flight("AB102", "AC2", "CR2")
        disruption2 = self.svc.create_disruption("sched", "scheduler", {"kind": "airport_closure", "resource_id": "AAA", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=1))})
        plan2 = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption2["id"], "name": "冲突方案", "assignments": [{"flight_id": flight2["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2, minutes=30)), "new_sta": iso(self.base + timedelta(hours=4, minutes=30))}]})
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan2["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "locked_resource_conflict")
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan2["id"], "sched", "scheduler", {"expected_revision": 1})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.add_assignment(plan1["id"], "sched", "scheduler", {"expected_revision": 1, "flight_id": flight1["id"], "aircraft_id": "AC1", "crew_id": "CR1", "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2))})
        self.assertEqual(ctx.exception.code, "plan_locked")


    def _make_flight(self, number, aircraft, crew, origin="AAA", destination="BBB", hours=0, passengers=150):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": origin, "destination": destination,
            "std": iso(self.base + timedelta(hours=hours)), "sta": iso(self.base + timedelta(hours=hours + 2)),
            "aircraft_id": aircraft, "crew_id": crew, "passenger_count": passengers})

    def test_connections_recomputed_when_times_change(self):
        f1 = self._make_flight("AB200", "AC1", "CR1")
        f2 = self._make_flight("BC300", "AC2", "CR2", origin="BBB", destination="CCC", hours=3)
        conn = self.svc.create_connection(f1["id"], "sched", "scheduler", {"downstream_flight_id": f2["id"], "passenger_count": 40, "min_connect_minutes": 60})
        self.assertEqual(conn["passenger_count"], 40)
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base - timedelta(hours=1)), "ends_at": iso(self.base + timedelta(hours=3))})
        # 初始按原时刻可衔接
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "延误", "assignments": [
            {"flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2))}]})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])
        self.assertEqual([l["status"] for l in check["plan"]["connection_results"]], ["connectable"])
        self.assertEqual(check["missed_connections_detail"], [])
        # 上游延误 1 小时：接续时间 0 分钟 < MCT 60，旧结论失效重算
        updated = self.svc.add_assignment(plan["id"], "sched", "scheduler", {"expected_revision": plan["revision"],
            "flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2",
            "new_std": iso(self.base + timedelta(hours=1)), "new_sta": iso(self.base + timedelta(hours=3))})
        self.assertEqual(updated["revision"], plan["revision"] + 1)
        missed = updated["missed_connections_detail"]
        self.assertEqual(len(missed), 1)
        self.assertEqual(missed[0]["upstream_flight_no"], "AB200")
        self.assertEqual(missed[0]["downstream_flight_no"], "BC300")
        self.assertEqual(missed[0]["passenger_count"], 40)
        self.assertEqual(missed[0]["connect_minutes"], 0)
        self.assertEqual(updated["metrics"]["missed_connections"], 40)
        # 错失清单本身给出上下游航班号和人数（下游 BC300 不在本方案中也照算）
        # 恢复原时刻：重算回到可衔接；手填的数字同样被忽略，以系统重算为准
        hand = self.svc.add_assignment(plan["id"], "sched", "scheduler", {"expected_revision": updated["revision"],
            "flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2",
            "new_std": iso(self.base), "new_sta": iso(self.base + timedelta(hours=2)), "missed_connections": 99})
        self.assertEqual(hand["metrics"]["missed_connections"], 0)
        self.assertEqual([l["status"] for l in hand["connection_results"]], ["connectable"])

    def test_downstream_change_and_cancelation_invalidate_connection(self):
        f1 = self._make_flight("AB210", "AC1", "CR1")
        f2 = self._make_flight("BC310", "AC2", "CR2", origin="BBB", destination="CCC", hours=3)
        self.svc.create_connection(f1["id"], "sched", "scheduler", {"downstream_flight_id": f2["id"], "passenger_count": 20, "min_connect_minutes": 60})
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "airport_closure", "resource_id": "BBB", "starts_at": iso(self.base + timedelta(hours=2)), "ends_at": iso(self.base + timedelta(hours=5))})
        # 只调整下游：起飞提前到 11:00 -> 10:30，与上游 10:00 到达只留 30 分钟
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "下游提前", "assignments": [
            {"flight_id": f2["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2, minutes=30)), "new_sta": iso(self.base + timedelta(hours=4, minutes=30))}]})
        missed = plan["missed_connections_detail"]
        self.assertEqual([(m["upstream_flight_no"], m["downstream_flight_no"], m["passenger_count"], m["reason"]) for m in missed],
                         [("AB210", "BC310", 20, "below_min_connect")])
        # 下游取消：整批中转错失，原因标为航段取消
        updated = self.svc.add_assignment(plan["id"], "sched", "scheduler", {"expected_revision": plan["revision"],
            "flight_id": f2["id"], "aircraft_id": "AC2", "crew_id": "CR2",
            "new_std": iso(self.base + timedelta(hours=6)), "new_sta": iso(self.base + timedelta(hours=8)), "status": "canceled"})
        self.assertEqual([(m["passenger_count"], m["reason"]) for m in updated["missed_connections_detail"]], [(20, "flight_canceled")])

    def test_concurrent_same_flight_submission_second_sees_latest(self):
        f1 = self._make_flight("AB400", "AC1", "CR1")
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=2))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "拼方案", "assignments": [
            {"flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=1)), "new_sta": iso(self.base + timedelta(hours=3))}]})
        barrier = threading.Barrier(2)
        errors: list[ApiError] = []

        def submit(rev: int) -> None:
            barrier.wait()
            try:
                self.svc.add_assignment(plan["id"], f"sched{rev}", "scheduler", {"expected_revision": plan["revision"],
                    "flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2",
                    "new_std": iso(self.base + timedelta(hours=1 + rev)), "new_sta": iso(self.base + timedelta(hours=3 + rev)),
                    "expected_flight_revision": f1["revision"]})
            except ApiError as exc:
                errors.append(exc)

        t1 = threading.Thread(target=submit, args=(1,)); t2 = threading.Thread(target=submit, args=(2,))
        t1.start(); t2.start(); t1.join(); t2.join()
        # 方案级版本冲突：后到者看到 409，可拉取最新结果
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "revision_conflict")

        # 航班已被其他方案锁定更新后，基于旧航班版本的锁定必须失败
        plan_b = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "另一个方案", "assignments": [
            {"flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2",
             "new_std": iso(self.base + timedelta(hours=4)), "new_sta": iso(self.base + timedelta(hours=6)),
             "expected_flight_revision": f1["revision"]}]})
        locked_a = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 2})
        self.assertEqual(locked_a["status"], "locked")
        with self.assertRaises(ApiError) as ctx:
            self.svc.lock_plan(plan_b["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertEqual(ctx.exception.code, "flight_revision_conflict")
        self.assertEqual(ctx.exception.details[0]["flight_no"], "AB400")

    def test_settlement_retry_by_revision_is_idempotent(self):
        f1 = self._make_flight("AB500", "AC1", "CR1")
        f2 = self._make_flight("BC501", "AC2", "CR2", origin="BBB", destination="CCC", hours=3)
        self.svc.create_connection(f1["id"], "sched", "scheduler", {"downstream_flight_id": f2["id"], "passenger_count": 10, "min_connect_minutes": 60})
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=2))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "延误结算", "assignments": [
            {"flight_id": f1["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=2)), "new_sta": iso(self.base + timedelta(hours=4))}]})
        self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        # 网关失败：整笔回滚，无台账
        with self.assertRaises(ApiError) as ctx:
            self.svc.settle_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1, "fail_gateway": True})
        self.assertEqual(ctx.exception.code, "settlement_gateway")
        with self.svc.repo.conn:
            self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) c FROM settlements").fetchone()["c"], 0)
            self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) c FROM settlement_ledger").fetchone()["c"], 0)
        # 按同一方案版本重试成功：延误 120 分钟 + 10 名错失中转
        settlement = self.svc.settle_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertFalse(settlement["idempotent_replay"])
        self.assertEqual(len(settlement["ledger"]), 2)
        self.assertEqual(settlement["total_amount"], 120 * 100 + 10 * 5000)
        missed_entry = [e for e in settlement["ledger"] if e["kind"] == "missed_connection"][0]
        self.assertEqual((missed_entry["upstream_flight_no"], missed_entry["downstream_flight_no"], missed_entry["passenger_count"]), ("AB500", "BC501", 10))
        # 再重试：回放同一笔账，不把同一批旅客记两遍
        replay = self.svc.settle_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 1})
        self.assertTrue(replay["idempotent_replay"])
        self.assertEqual(replay["id"], settlement["id"])
        with self.svc.repo.conn:
            self.assertEqual(self.svc.repo.conn.execute("SELECT COUNT(*) c FROM settlement_ledger").fetchone()["c"], 2)
        # 版本不对不能结算
        with self.assertRaises(ApiError) as ctx:
            self.svc.settle_plan(plan["id"], "ops", "ops_manager", {"expected_revision": 99})
        self.assertEqual(ctx.exception.code, "revision_conflict")

    def test_legacy_db_without_connections_treated_as_connectable(self):
        legacy_dir = tempfile.TemporaryDirectory()
        self.addCleanup(legacy_dir.cleanup)
        db_path = Path(legacy_dir.name) / "legacy.db"
        conn = sqlite3.connect(db_path)
        maintenance = iso(self.base + timedelta(days=5)); duty = iso(self.base - timedelta(hours=2))
        permit_from, permit_to = iso(self.base - timedelta(days=1)), iso(self.base + timedelta(days=2))
        std, sta, now = iso(self.base), iso(self.base + timedelta(hours=2)), iso()
        conn.executescript(
            f"""
            CREATE TABLE airports(code TEXT PRIMARY KEY, country TEXT NOT NULL, curfew_start TEXT NOT NULL, curfew_end TEXT NOT NULL);
            CREATE TABLE aircraft(id TEXT PRIMARY KEY, model TEXT NOT NULL, maintenance_due TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE crew(id TEXT PRIMARY KEY, name TEXT NOT NULL, base TEXT NOT NULL, duty_start TEXT NOT NULL, max_duty_minutes INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'active');
            CREATE TABLE permits(id INTEGER PRIMARY KEY AUTOINCREMENT, origin TEXT NOT NULL, destination TEXT NOT NULL, valid_from TEXT NOT NULL, valid_to TEXT NOT NULL, curfew_exempt INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE flights(id INTEGER PRIMARY KEY AUTOINCREMENT, flight_no TEXT NOT NULL UNIQUE, origin TEXT NOT NULL, destination TEXT NOT NULL,
                std TEXT NOT NULL, sta TEXT NOT NULL, aircraft_id TEXT NOT NULL, crew_id TEXT NOT NULL, passenger_count INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'scheduled', delay_minutes INTEGER NOT NULL DEFAULT 0, revision INTEGER NOT NULL DEFAULT 1, cancel_reason TEXT, updated_at TEXT NOT NULL);
            CREATE TABLE disruptions(id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, resource_id TEXT NOT NULL, starts_at TEXT NOT NULL, ends_at TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'active', created_at TEXT NOT NULL);
            CREATE TABLE recovery_plans(id INTEGER PRIMARY KEY AUTOINCREMENT, disruption_id INTEGER NOT NULL, name TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'draft',
                revision INTEGER NOT NULL DEFAULT 1, score_json TEXT, metrics_json TEXT, created_by TEXT NOT NULL, created_at TEXT NOT NULL, locked_at TEXT, locked_by TEXT);
            CREATE TABLE assignments(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER NOT NULL, flight_id INTEGER NOT NULL, aircraft_id TEXT NOT NULL, crew_id TEXT NOT NULL,
                new_std TEXT NOT NULL, new_sta TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'planned', delay_minutes INTEGER NOT NULL DEFAULT 0, missed_connections INTEGER NOT NULL DEFAULT 0, UNIQUE(plan_id,flight_id));
            CREATE TABLE audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT, plan_id INTEGER, actor TEXT NOT NULL, role TEXT NOT NULL, action TEXT NOT NULL, detail_json TEXT NOT NULL, created_at TEXT NOT NULL);
            INSERT INTO airports VALUES('AAA','CN','23:00','05:00'),('BBB','CN','23:00','05:00');
            INSERT INTO aircraft VALUES('AC1','A320','{maintenance}','active'),('AC2','A320','{maintenance}','active');
            INSERT INTO crew VALUES('CR1','甲','AAA','{duty}',720,'active'),('CR2','乙','AAA','{duty}',720,'active');
            INSERT INTO permits(origin,destination,valid_from,valid_to) VALUES('AAA','BBB','{permit_from}','{permit_to}');
            INSERT INTO flights(flight_no,origin,destination,std,sta,aircraft_id,crew_id,passenger_count,updated_at)
                VALUES('AB900','AAA','BBB','{std}','{sta}','AC1','CR1',80,'{now}');
            """)
        conn.commit(); conn.close()
        svc = AirlineRecoveryService(db_path)
        # 迁移补齐了 flight_revision 列与新表
        with svc.repo.conn:
            cols = {r["name"] for r in svc.repo.conn.execute("PRAGMA table_info(assignments)")}
        self.assertIn("flight_revision", cols)
        flight = svc.repo.conn.execute("SELECT * FROM flights WHERE flight_no='AB900'").fetchone()
        disruption = svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1", "starts_at": iso(self.base), "ends_at": iso(self.base + timedelta(hours=2))})
        plan = svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "旧库方案", "assignments": [
            {"flight_id": flight["id"], "aircraft_id": "AC2", "crew_id": "CR2", "new_std": iso(self.base + timedelta(hours=1)), "new_sta": iso(self.base + timedelta(hours=3))}]})
        # 没有中转记录：按可衔接处理，错失人数为 0
        check = svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])
        self.assertEqual(check["plan"]["metrics"]["missed_connections"], 0)
        self.assertEqual(check["plan"]["connection_results"], [])


if __name__ == "__main__": unittest.main()
