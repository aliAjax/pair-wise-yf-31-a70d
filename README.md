# 航班中断恢复系统

独立的 Python 标准库项目，用 SQLite 保存机场、飞机、机组、航线许可、航班、中断事件和恢复方案。系统会校验维护间隔、执勤时限、机场宵禁、航线许可、资源重叠，并计算取消、延误、受影响旅客和错失衔接成本。

## 运行

```bash
python3 app.py --db airline_recovery.db
```

默认监听 `127.0.0.1:8202`，首页为 `/`，健康检查为 `/health`。

身份头：`X-User-Id`、`X-Role`。角色包括 `viewer`、`scheduler`、`ops_manager`、`auditor`。

## 主要接口

- `POST /api/airports`、`/api/aircraft`、`/api/crew`、`/api/permits`：基础资源与约束。
- `POST /api/flights`、`POST /api/disruptions`：创建航班和中断。
- `GET /api/flights/{id}`、`GET /api/flights/{id}/connections`：查询航班及其中转衔接。
- `POST /api/flights/{id}/connections`：按 `expected_revision` 整组设置该航班要衔接的下游航段与人数。
- `GET /api/connections/missed`：列出当前接不上的中转（含上游/下游航班号与人数）。
- `POST /api/recovery-plans`：一次提交方案及航班调整。
- `POST /api/plans/{id}/assignments`：用 `expected_revision` 临时改派。
- `POST /api/plans/{id}/validate`、`/lock`：校验并原子锁定方案。
- `GET /api/disruptions/{id}/compare`：比较恢复方案成本。
- `POST /api/flights/{id}/cancel`、`/recover`：取消和人工恢复。
- `GET /api/state`、`GET /api/plans/{id}`：查询状态和影响。

## 旅客中转衔接

每个航班可携带要衔接的下游航段及人数（`connections`：下游航班、旅客人数、最短衔接时间）。衔接结论按航班时刻实时重算：上游到达 + 最短衔接时间仍晚于下游起飞即判为接不上，并在 `GET /api/connections/missed` 与方案的 `missed_connections` 中列清航班号和人数。

- **时刻变化即失效重算**：锁定方案（结算）或人工恢复导致航班时刻变化后，相关中转结论立即按新时刻重算落库。
- **并发提交**：修改航班中转或恢复航班时携带 `expected_revision`；版本冲突返回 `409 revision_conflict`，并在 `details.flight` 中带上最新结果，后到的调度员看到最新数据。
- **结算幂等**：方案按 `settled_revision` 记录已结算版本，同一版本重试结算直接返回，不把同一批旅客记两遍。
- **旧数据兼容**：升级前没有中转记录的航班视为可衔接，不产生错失衔接。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 主要局限

时区和机场本地时刻没有引入完整时区数据库；模型使用简化航线许可与宵禁规则。身份头、SQLite 和单进程 HTTP 服务适合原型演示，正式运行需要外部身份系统、共享数据库和更强的跨实例锁。
