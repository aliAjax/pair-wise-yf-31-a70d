import sys, tempfile, unittest
from datetime import timedelta
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import AirlineRecoveryService, ApiError, iso, utcnow


class ConnectionFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = AirlineRecoveryService(Path(self.tmp.name) / "test.db")
        self.base = utcnow() + timedelta(days=1)
        self.svc.seed_airport("ops", "ops_manager", {"code": "AAA", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "BBB", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_airport("ops", "ops_manager", {"code": "CCC", "country": "CN", "curfew_start": "23:00", "curfew_end": "05:00"})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC1", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=5))})
        self.svc.seed_aircraft("ops", "ops_manager", {"id": "AC2", "model": "A320", "maintenance_due": iso(self.base + timedelta(days=5))})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR1", "name": "甲组", "base": "AAA", "duty_start": iso(self.base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.seed_crew("ops", "ops_manager", {"id": "CR2", "name": "乙组", "base": "AAA", "duty_start": iso(self.base - timedelta(hours=2)), "max_duty_minutes": 720})
        self.svc.create_permit("ops", "ops_manager", {"origin": "AAA", "destination": "BBB", "valid_from": iso(self.base - timedelta(days=1)), "valid_to": iso(self.base + timedelta(days=2))})
        self.svc.create_permit("ops", "ops_manager", {"origin": "BBB", "destination": "CCC", "valid_from": iso(self.base - timedelta(days=1)), "valid_to": iso(self.base + timedelta(days=2))})

    def tearDown(self):
        self.tmp.cleanup()

    def _flight(self, number, origin, destination, std, sta, aircraft, crew, pax=100):
        return self.svc.create_flight("sched", "scheduler", {"flight_no": number, "origin": origin, "destination": destination,
                                                             "std": iso(std), "sta": iso(sta), "aircraft_id": aircraft,
                                                             "crew_id": crew, "passenger_count": pax})

    def _connect(self, upstream, downstream, count, min_connect=45, expected=None):
        expected = upstream["revision"] if expected is None else expected
        return self.svc.set_flight_connections(upstream["id"], "sched", "scheduler",
                                               {"expected_revision": expected,
                                                "connections": [{"downstream_flight_id": downstream["id"],
                                                                 "passenger_count": count,
                                                                 "min_connect_minutes": min_connect}]})

    def test_flight_carries_downstream_connections_and_counts(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        updated = self._connect(up, down, 30)
        conn = updated["connections"][0]
        self.assertEqual(conn["upstream_flight_id"], up["id"])
        self.assertEqual(conn["downstream_flight_id"], down["id"])
        self.assertEqual(conn["passenger_count"], 30)
        self.assertEqual(conn["u_no"], "AB100")
        self.assertEqual(conn["d_no"], "AB200")
        self.assertFalse(conn["missed"])

    def test_connection_connected_when_buffer_sufficient(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        self._connect(up, down, 30, min_connect=45)
        missed = self.svc.list_missed_connections()
        self.assertEqual(missed["missed_passengers"], 0)
        self.assertEqual(missed["missed_connections"], [])

    def test_time_change_invalidates_and_recomputes_missed(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        self._connect(up, down, 30, min_connect=45)
        # 延误上游到达 30 分钟：2:30 + 45m > 3:00，接不上
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1",
                                                                       "starts_at": iso(self.base - timedelta(hours=1)),
                                                                       "ends_at": iso(self.base + timedelta(hours=4))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "延误",
                                                          "assignments": [{"flight_id": up["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                                                                          "new_std": iso(self.base + timedelta(minutes=30)),
                                                                          "new_sta": iso(self.base + timedelta(hours=2, minutes=30))}]})
        check = self.svc.validate_plan(plan["id"], "auditor", "auditor")
        self.assertTrue(check["valid"], check["problems"])
        self.assertEqual(check["missed_passengers"], 30)
        missed = check["missed_connections"][0]
        self.assertEqual((missed["u_no"], missed["d_no"], missed["passenger_count"]), ("AB100", "AB200", 30))
        # 结算后落库结论重算为 missed
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": plan["revision"]})
        self.assertEqual(locked["metrics"]["missed_connections"], 30)
        stored = self.svc.get_flight(up["id"])["connections"][0]
        self.assertEqual(stored["status"], "missed")
        self.assertEqual(stored["live_status"], "missed")
        # 全局 missed 清单列出人数与航班号
        listed = self.svc.list_missed_connections()
        self.assertEqual(listed["missed_passengers"], 30)
        self.assertEqual([(m["u_no"], m["d_no"], m["passenger_count"]) for m in listed["missed_connections"]], [("AB100", "AB200", 30)])

    def test_recover_recomputes_connection(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        self._connect(up, down, 20, min_connect=45)
        canceled = self.svc.cancel_flight(up["id"], "sched", "scheduler", {"reason": "机务"})["flight"]
        # 恢复到延误 30 分钟的时刻 -> 接不上
        recovered = self.svc.recover_flight(up["id"], "sched", "scheduler",
                                            {"expected_revision": canceled["revision"],
                                             "new_std": iso(self.base + timedelta(minutes=30)),
                                             "new_sta": iso(self.base + timedelta(hours=2, minutes=30))})["flight"]
        self.assertEqual(recovered["connections"][0]["live_status"], "missed")
        self.assertEqual(self.svc.list_missed_connections()["missed_passengers"], 20)

    def test_concurrent_submit_later_sees_latest_result(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        # 调度员甲先提交
        first = self._connect(up, down, 30)
        # 调度员乙用旧版本提交同一航班 -> 冲突且能看到最新结果
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "sched", "scheduler",
                                            {"expected_revision": 1, "connections": [{"downstream_flight_id": down["id"], "passenger_count": 99}]})
        self.assertEqual(ctx.exception.code, "revision_conflict")
        latest = ctx.exception.details["flight"]
        self.assertEqual(latest["revision"], first["revision"])
        self.assertEqual(latest["connections"][0]["passenger_count"], 30)

    def test_settlement_retry_by_version_no_double_count(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        self._connect(up, down, 30, min_connect=45)
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1",
                                                                       "starts_at": iso(self.base - timedelta(hours=1)),
                                                                       "ends_at": iso(self.base + timedelta(hours=4))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "延误",
                                                          "assignments": [{"flight_id": up["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                                                                          "new_std": iso(self.base + timedelta(minutes=30)),
                                                                          "new_sta": iso(self.base + timedelta(hours=2, minutes=30))}]})
        first_lock = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": plan["revision"]})
        self.assertEqual(first_lock["settled_revision"], plan["revision"])
        # 按同一方案版本重试结算 -> 幂等，旅客不重复记
        retry = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": plan["revision"]})
        self.assertEqual(retry["status"], "locked")
        self.assertEqual(retry["settled_revision"], plan["revision"])
        self.assertEqual(retry["metrics"]["missed_connections"], 30)
        self.assertEqual(self.svc.list_missed_connections()["missed_passengers"], 30)

    def test_legacy_flight_without_connections_treated_connectable(self):
        # 旧数据没有中转记录：升级后视为可衔接，不产生错失
        legacy = self._flight("AB900", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        self.assertEqual(legacy["connections"], [])
        self.assertEqual(self.svc.list_missed_connections()["missed_passengers"], 0)
        disruption = self.svc.create_disruption("sched", "scheduler", {"kind": "aircraft_fault", "resource_id": "AC1",
                                                                       "starts_at": iso(self.base - timedelta(hours=1)),
                                                                       "ends_at": iso(self.base + timedelta(hours=4))})
        plan = self.svc.create_plan("sched", "scheduler", {"disruption_id": disruption["id"], "name": "无中转",
                                                          "assignments": [{"flight_id": legacy["id"], "aircraft_id": "AC1", "crew_id": "CR1",
                                                                          "new_std": iso(self.base + timedelta(hours=1)),
                                                                          "new_sta": iso(self.base + timedelta(hours=3))}]})
        locked = self.svc.lock_plan(plan["id"], "ops", "ops_manager", {"expected_revision": plan["revision"]})
        self.assertEqual(locked["metrics"]["missed_connections"], 0)

    def test_invalid_connection_inputs_rejected(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        down = self._flight("AB200", "BBB", "CCC", self.base + timedelta(hours=3), self.base + timedelta(hours=5), "AC2", "CR2")
        # 负数人数
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "sched", "scheduler",
                                            {"expected_revision": up["revision"],
                                             "connections": [{"downstream_flight_id": down["id"], "passenger_count": -1}]})
        self.assertEqual(ctx.exception.code, "invalid_connection")
        # 衔接自己
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "sched", "scheduler",
                                            {"expected_revision": up["revision"],
                                             "connections": [{"downstream_flight_id": up["id"], "passenger_count": 1}]})
        self.assertEqual(ctx.exception.code, "invalid_connection")
        # 下游航班不存在
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "sched", "scheduler",
                                            {"expected_revision": up["revision"],
                                             "connections": [{"downstream_flight_id": 9999, "passenger_count": 1}]})
        self.assertEqual(ctx.exception.code, "flight_not_found")
        # 缺少版本号
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "sched", "scheduler", {"connections": []})
        self.assertEqual(ctx.exception.code, "revision_required")

    def test_connection_permission_guard(self):
        up = self._flight("AB100", "AAA", "BBB", self.base, self.base + timedelta(hours=2), "AC1", "CR1")
        with self.assertRaises(ApiError) as ctx:
            self.svc.set_flight_connections(up["id"], "viewer", "viewer", {"expected_revision": up["revision"], "connections": []})
        self.assertEqual(ctx.exception.status, 403)


if __name__ == "__main__":
    unittest.main()
