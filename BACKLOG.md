# APEX-DB Backlog

## kdb+ Gap Closure — Core Features Complete (95% Replacement Rate)
xbar, ema, LEFT/Window JOIN, deltas/ratios, FIRST/LAST all done.
HFT 95% / Quant 90% / Risk 95% replacement rate achieved.

---

## High Priority — Technical

- [ ] **Graviton (ARM) build test** — r8g instance, Highway SVE
- [ ] **Bare-metal tuning detailed guide** — CPU pinning, NUMA, io_uring
- [ ] **g#/p# indexes** — hash/parted partition indexes (s# done)

### Done
- [x] SQL parser Phase 1–3 — IN, IS NULL, NOT, HAVING, arithmetic, CASE WHEN, GROUP BY, date/time, LIKE, UNION/INTERSECT/EXCEPT, subquery/CTE, EXPLAIN, NULL standardization
- [x] All JOIN types — INNER, LEFT, RIGHT, FULL OUTER, ASOF, Window JOIN, uj, pj, aj0
- [x] Time range index — O(log n) timestamp binary search + partition pruning
- [x] Attribute hints s# — sorted column O(log n) range scan
- [x] Timer / Scheduler — interval, daily, once-shot; kdb+ `.z.ts` equivalent
- [x] Connection hooks — `.z.po/.z.pc` equivalent, session tracking, idle eviction
- [x] `\t <sql>` one-shot timer — apex-cli
- [x] Parallel query engine — PARTITION/CHUNKED/SERIAL auto-selection, 3.48x speedup
- [x] exec_group_agg single-column optimization — xbar sorted-scan, 45ms → 10ms (-77%)

---

## High Priority — Business / Operations

- [ ] **Limited DSL AOT compilation** — Nuitka → single binary, Cython → C extensions
- [ ] **Kubernetes operations guide** — Helm, monitoring, troubleshooting
- [ ] **Website & documentation site** — apex-db.io, docs.apex-db.io

### Done
- [x] TLS/SSL + API Key + RBAC + SSO — enterprise security stack
- [x] Rate limiting, admin REST API, query timeout/cancellation, secrets management, audit log
- [x] Production deployment guide, Dockerfile, k8s/deployment.yaml
- [x] Production monitoring — /health /ready /metrics, Prometheus, Grafana
- [x] Backup & recovery automation — backup.sh, restore.sh, EOD scripts
- [x] Python ecosystem — from_polars/pandas/arrow, zero-copy, pip package (twine upload pending)
- [x] Migration toolkit — kdb+, ClickHouse, DuckDB, TimescaleDB (70 tests)
- [x] Feed handler toolkit — FIX, NASDAQ ITCH 5.0, multicast UDP
- [x] AI bare-metal tuner — Claude Opus 4.6 + hugepages/C-states/PGO

---

## Medium Priority

- [ ] **DuckDB embedding** — delegate complex JOINs via Arrow zero-copy
- [ ] **JIT SIMD emit** — generate AVX2/512 vector IR from LLVM JIT
- [ ] **Arrow Flight server** — stream query results as Arrow batches; direct Pandas/Polars client
- [ ] **JDBC/ODBC drivers** — Tableau, Excel, BI tools
- [ ] **ClickHouse wire protocol** — binary protocol compatibility

### Done
- [x] Distributed query scheduler — scatter/gather via TCP RPC
- [x] Data/compute node separation
- [x] Multi-threaded drain, ring buffer dynamic adjustment
- [x] Chunked parallel scan, exec_simple_select parallelization
- [x] Resource isolation — CPU pinning, core affinity

---

## Storage & Format

- [ ] **Arrow Flight server** — see Medium Priority above
- [ ] **HDB Compaction** — merge small partition files

### Done
- [x] Parquet HDB write + S3 flush
- [x] Parquet reader — file → pipeline

---

## Streaming Data Integration

- [ ] **Kafka Connect Sink** — register as Kafka Connect sink plugin
- [ ] **Apache Pulsar consumer**
- [ ] **AWS Kinesis consumer**

### Done
- [x] Apache Kafka consumer — JSON/BINARY/JSON_HUMAN, multi-node routing, Prometheus metrics

---

## Physical AI / Industry

- [ ] **ROS2 plugin** — ROS2 topics → APEX-DB ingestion
- [ ] **OPC-UA connector** — Siemens S7, industrial PLC
- [ ] **MQTT ingestion** — IoT device connection

---

## HA & Replication

- [x] WAL-based async replication
- [x] Replication factor 2, auto failover
- [x] Coordinator HA (active-standby)
- [x] Snapshot coordinator
- [x] Split-brain defense — FencingToken in RPC, K8s Lease, 4 simulation tests

---

## Product Readiness (Beta → GA 필수)

### 데이터 내구성 (가장 높은 위험)
- [x] **장중 자동 스냅샷** ✅ — `FlushConfig::enable_auto_snapshot` + `snapshot_interval_ms`; `FlushManager::snapshot_now()` writes all partitions (ACTIVE included) to binary HDB
- [x] **노드 재시작 자동 복구** ✅ — `PipelineConfig::enable_recovery` + `recovery_snapshot_path`; `ApexPipeline::start()` replays snapshot via `HDBReader` before drain threads launch

### 분산 정밀도 버그 (숫자 오류 → 신뢰 손실)
- [ ] **AVG int64 truncation** — SUM/COUNT 모두 int64; 정수 나눗셈으로 소수점 소실 (예: 66668.67 → 66668)
- [ ] **VWAP int64 overflow** — 대규모 데이터에서 SUM(price×volume) 오버플로 가능

### 분산 장애 안전성
- [ ] **Partial failure policy** — scatter 중 일부 노드 다운 시 동작 정의 (partial result 반환 vs 명확한 에러)
- [ ] **Cancel propagation** — coordinator timeout 시 in-flight RPC를 모든 노드에 취소 전파
- [ ] **In-flight query safety** — scatter 실행 중 노드 추가/제거 시 race condition 방지
- [ ] **Dual-write during migration** — partition 이동 중 신규 tick 유실 방지

### 운영 편의
- [ ] **Helm chart** — k8s/deployment.yaml만 있음; Helm으로 패키징해야 엔터프라이즈 도입 가능
- [ ] **무중단 업그레이드 절차** — rolling upgrade 가이드/자동화 없음
- [ ] **메모리 상한 / eviction 정책** — 메모리 풀 상한 초과 시 동작 미정의
- [ ] **실시간 클러스터 상태 CLI** — `apex-cli \nodes`, `\cluster` 등 운영자용 명령어

---

## DDL & Data Management

- [x] **CREATE TABLE / DROP TABLE** ✅ — `SchemaRegistry` per pipeline; `CREATE TABLE t (col TYPE, ...)`, `DROP TABLE [IF EXISTS] t`; 8 tests pass
- [x] **Retention Policy** ✅ — `ALTER TABLE t SET TTL 30 DAYS`; immediate eviction + `FlushManager::set_ttl()` for continuous eviction in flush_loop()
- [x] **Schema Evolution** ✅ — `ALTER TABLE t ADD COLUMN col TYPE` / `DROP COLUMN col`
- [ ] **HDB Compaction** — merge small partition files

---

## Distributed Query & Cluster

### Cluster Integrity (2026-03-23 review)
- [x] **Unified PartitionRouter** ✅ — ClusterNode & QueryCoordinator share single router via `set_shared_router()` / `connect_coordinator()`
- [x] **FencingToken in RPC protocol** ✅ — RpcHeader epoch field (24 bytes); TcpRpcServer rejects stale-epoch TICK_INGEST/WAL_REPLICATE; epoch=0 bypasses (backward compat)
- [x] **CoordinatorHA auto re-registration** ✅ — on promote, registered_nodes_ replayed into coordinator as remote nodes automatically
- [x] **ComputeNode merge logic** ✅ — delegates to QueryCoordinator (partial agg, GROUP BY merge, AVG rewrite); fixed SELECT * misclassified as SCALAR_AGG

### Distributed Query Correctness (2026-03-23)
- [x] **VWAP distributed decomposition** ✅ — VWAP(p,v) → SUM(p*v), SUM(v) rewrite; reconstruct SUM_PV/SUM_V
- [x] **ORDER BY + LIMIT post-merge** ✅ — coordinator sorts merged results + truncates to LIMIT
- [x] **SELECT * SCALAR_AGG bug** ✅ — fixed in cluster integrity phase
- [x] **HAVING distributed** ✅ — strip HAVING from scatter SQL; apply post-merge filter at coordinator
- [x] **DISTINCT distributed** ✅ — dedup at coordinator after concat via `std::set`
- [x] **Window functions distributed** ✅ — detect OVER clause → scatter base data → ingest into temp pipeline → execute original SQL locally
- [x] **FIRST/LAST distributed** ✅ — fetch-and-compute with timestamp-sorted ingest via `store_tick_direct()`
- [x] **COUNT(DISTINCT col) distributed** ✅ — parser + executor support for `COUNT(DISTINCT col)`; fetch-and-compute at coordinator
- [x] **Subquery/CTE distributed** ✅ — detect CTE/FROM subquery → fetch-and-compute on full dataset
- [x] **Multi-column ORDER BY** ✅ — post-merge sort uses all ORDER BY items with composite key comparison

### Distributed Infrastructure
- [ ] **Cancel propagation** — coordinator timeout → send cancel RPC to all in-flight nodes
- [ ] **Partial failure policy** — define behavior when some nodes fail: partial result vs full error
- [ ] **In-flight query safety during node changes** — add/remove node while scatter is running → race condition
- [ ] **Dual-write during partition migration** — new data to old node during migration → data loss gap
- [ ] **Distributed query timeout** — coordinator sets timeout but remote node execution continues; need remote-side timeout

### Distributed Precision
- [ ] **AVG int64 truncation** — SUM/COUNT both int64; float data AVG loses precision via integer division
- [ ] **VWAP int64 truncation** — same issue; SUM(price*volume) can overflow for large datasets

### Existing
- [ ] **Live rebalancing** — zero-downtime partition movement
- [ ] **Tier C cold query offload** — recent → APEX in-memory, old → DuckDB on S3
- [ ] **PTP clock sync detection** — enforce for ASOF JOIN strict mode

### Done
- [x] Phase C-3 MVP — QueryCoordinator + TCP RPC + partial agg merge
- [x] TCP RPC connection pooling
- [x] AVG distributed merge
- [x] Cross-node ASOF JOIN
- [x] Hot symbol detection & rebalancing
- [x] Partition migration execution
- [x] NodeRegistry — Gossip / K8s pluggable membership

---

## Multi-Usecase Extensions

- [ ] **Pluggable partition strategy** — symbol_affinity / hash_mod / site_id
- [ ] **Edge mode** (`--mode edge`) — single-node with async cloud sync
- [ ] **HyperLogLog** — distributed approximate COUNT DISTINCT

---

## Low Priority

- [ ] Snowflake/Delta Lake hybrid support
- [ ] AWS Fleet API integration (Warm Pool + Placement Group)
- [ ] DynamoDB metadata (partition map)
- [ ] Graph index (CSR) — fund flow tracking
- [ ] InfluxDB migration (InfluxQL → SQL)
