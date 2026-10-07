# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中转衔接、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。中转旅客的接续结论由系统按航班生效时刻自动重算，无需手填。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。建航班时可用 `connections: [{downstream_flight_id, passenger_count, min_connect_minutes}]` 一并登记中转。
- `POST /api/flights/{id}/connections`：为航班登记/更新要衔接的下游航段及中转人数（校验机场衔接与时刻先后）。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision`（方案版本）和 `expected_flight_revision`（航班级乐观锁）临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案；返回 `missed_connections_detail`（上/下游航班号、人数、接续分钟、原因）。
- `POST /api/plans/{id}/settle`：按方案版本结算。网关失败整笔回滚，带同一 `expected_revision` 重试只回放同一笔账，台账唯一键保证同批旅客不重复记账。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响。

## 中转接续规则

- 每条方案调整的航班携带其中转下游航段与人数；只要上游或下游任一趟航班的生效时刻/状态变化，已保存的接续结论按签名失效并重算（方案创建、改派、校验、锁定及草稿查询时）。
- 接续判定：实际中转时间（下游起飞 − 上游到达）小于最短中转时间（MCT，默认 60 分钟），或任一段被取消，即记为错失。
- 错失清单给出上游、下游航班号、人数、实际/最短中转分钟和原因；手填的 `missed_connections` 会被忽略。
- 并发提交：方案级 `expected_revision` 串行化同一方案；航班级 `expected_flight_revision` 过期返回 409 并附带最新航班数据，后到的调度员基于最新结果重算。锁定时也会拒绝基于过时航班时刻的调整。
- 旧库升级走 `ALTER TABLE`/`CREATE TABLE IF NOT EXISTS` 迁移；没有中转记录的旧数据一律按可衔接处理（错失为 0）。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示（事务已在进程内串行化，WAL + busy_timeout 负责跨进程并发），正式运行需要外部身份系统、共享数据库和更强的跨实例锁。结算网关为模拟实现（`fail_gateway` 可触发失败回滚），正式运行需对接真实支付/结算通道。
