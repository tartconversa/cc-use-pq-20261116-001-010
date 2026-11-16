# 展品体验事件湖

本项目使用 FastAPI 和 SQLite 管理场馆、展品、来源契约与事件批次。原始事件保留接收证据，正式报表只读取已发布的日期快照。

## 运行

在预装依赖的环境中，从项目根目录执行：

    uvicorn exhibit_lake.api:app --host 0.0.0.0 --port 8000

默认数据库为本地 SQLite 文件；用 EXHIBIT_LAKE_DB 可指定其他路径。

## 测试与构建

    python -m unittest discover -s tests -v
    python -m compileall -q exhibit_lake
    python -m exhibit_lake.demo

以上三条命令依次运行业务测试、编译检查和本地合成数据验收。依赖版本记录在 requirements.txt。

## 指标定义与重算

正式报表仍只读取已发布快照，但指标不再由查询即时拼装：登记的口径（分子/分母选用的事件类型与度量）是不可变版本，重算任务在创建时同时钉住口径版本与快照输入摘要，结果记录 `definition_version`、`snapshot_digest`、`members_digest` 与 `input_digest`。

- `POST /metrics` 登记口径。相同内容重复登记保持幂等，内容变化会产生新版本，旧版本永远可读。
- `POST /metrics/recomputes` 对历史分区重算：`snapshot_id` 省略时取当前已发布头，`definition_version` 选择历史口径，`definition` 可直接携带新口径（登记为新版本后参与本次计算）。`request_key` 相同但口径、快照或展品范围不同会返回冲突，不会静默复用旧任务。
- `POST /metrics/recomputes/{task_id}/retry` 重试失败任务，沿用任务创建时钉住的口径与输入；输入摘要变化会以完整性错误失败并保留证据，`attempts` 递增。
- `POST /metrics/results/{result_id}/publish` 把结果显式绑定到快照。同一快照同一指标已有生效绑定时，必须提供 `expected_publication_id`，旧绑定会被标记为已退役并保留退役原因，不会被静默覆盖。
- `GET /venues/{venue_id}/days/{local_day}/metrics/{metric_key}` 默认返回当前生效绑定；带 `definition_version` 时返回该口径的结果与当时的绑定记录，用于解释历史对外报表。未绑定口径返回 404，而不是把即时汇总值冒充正式结果。
- `GET /metrics/results/{result_id}/trace` 串起口径、重算任务、快照成员与原始批次。

缺失信号不会被折算成 0：分子或分母没有观测、或分母为零时，结果的 `value` 为 `null`，并通过 `status=no_data` 与 `reason` 说明原因；展品级缺口列在 `unobserved_exhibits`。局部展品重算只能作为诊断结果，不能成为分区对外绑定。
