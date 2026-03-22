# APEX-DB 백로그

## ✅ kdb+ 격차 해소 완료! (대체율 93% 달성)

**목표:** 없으면 kdb+ 대체 불가능한 핵심 기능
**참고:** `docs/design/kdb_replacement_analysis.md`, `docs/devlog/010_financial_functions.md`

| 작업 | 상태 | 성능 |
|------|------|------|
| **xbar (시간 바 집계)** | ✅ | 1M → 3,334 bars in 24ms |
| **ema (지수이동평균)** | ✅ | 1M rows in 2.2ms |
| **LEFT JOIN** | ✅ | NULL 센티넬 (INT64_MIN) |
| **Window JOIN (wj)** | ✅ | O(n log m) 이진 탐색 |
| **deltas/ratios 네이티브** | ✅ | OVER 윈도우 함수 |
| **FIRST/LAST 집계** | ✅ | OHLC 캔들차트용 |

**결과:** kdb+ 대체율 **HFT 95%, 퀀트 90%, 리스크 95%** 🎉

---

## 즉시 (다음 커밋)
- [x] **비즈니스 전략 문서화** ✅ 완료 (2026-03-22)
  - `docs/business/BUSINESS_STRATEGY.md` - 완전한 비즈니스 전략 (13개 섹션)
    - 시장 분석, 경쟁 전략, 제품 현황, GTM 전략
    - 마이그레이션 툴킷 로드맵, 재무 전망, 팀 빌드업
    - 12개월 목표: $3.6M ARR, 43 고객
  - `docs/business/EXECUTIVE_SUMMARY.md` - 1페이지 요약 (투자자/경영진용)
- [ ] **설계 문서 전체 업데이트** — 현재 구현 상태에 맞게 동기화
  - high_level_architecture.md: SQL/HTTP/Cluster/병렬쿼리 레이어 추가
  - initial_doc.md: 범용 OLAP/TSDb 타겟 확장
  - system_requirements.md: SQL/HTTP/JOIN/Window/병렬쿼리 요구사항
  - layer4: nanobind → pybind11, DSL 실제 구현
  - README.md: 전체 기능 + 최신 벤치마크 + kdb+ 대체율
  - kdb_replacement_analysis.md: 병렬 쿼리 완료 반영

## 높은 우선순위 (기술)
- [ ] **SQL 파서 완성** — ClickHouse 사용자 유입 핵심
- [ ] **시간 범위 인덱스** — 거의 공짜, 이미 정렬된 데이터
- [ ] **Graviton (ARM) 빌드 테스트** — r8g 인스턴스, Highway SVE

## 높은 우선순위 (비즈니스/운영)
- [x] **프로덕션 배포 가이드** ✅ 완료 (2026-03-22)
  - `docs/deployment/PRODUCTION_DEPLOYMENT.md` - 베어메탈 vs 클라우드 선택 가이드
  - `scripts/tune_bare_metal.sh` - 베어메탈 자동 튜닝 스크립트
  - `Dockerfile` - 클라우드 네이티브 이미지
  - `k8s/deployment.yaml` - Kubernetes 배포 (HPA, PVC, LoadBalancer)
- [ ] **Python 에코시스템 통합** — Research-to-Production 연결 핵심
  - `apex.from_polars(df)` - Polars DataFrame → APEX-DB zero-copy (1-2주)
  - `apex.from_pandas(df)` - Pandas DataFrame → APEX-DB (1주)
  - Arrow 직접 지원 - Polars/Pandas/DuckDB 상호운용성 (3-4주)
  - **비즈니스 가치:** Jupyter 리서치 → 프로덕션 배포 심리스 전환 (4-37x 속도)
- [ ] **제한된 DSL AOT 컴파일** — 프로덕션 배포 & IP 보호
  - **Phase 1 (1주):** Nuitka 통합 - Python DSL → 단일 바이너리 (15x 작음)
  - **Phase 2 (1개월):** Cython 지원 - 핵심 연산 → C 확장 (2-3x 추가 속도)
  - **Phase 3 (3-6개월):** 제한된 DSL 트랜스파일러 - filter/select/groupby → SQL 자동 변환
  - **비즈니스 가치:** 고객 프로덕션 배포 용이성 + 소스코드 IP 보호
- [x] **프로덕션 모니터링 & 로깅** ✅ 완료 (2026-03-22)
  - `/health`, `/ready`, `/metrics` 엔드포인트
  - Prometheus OpenMetrics 형식
  - 구조화된 JSON 로깅 (spdlog)
  - Grafana 대시보드 + 9가지 알림 규칙
  - `docs/operations/PRODUCTION_OPERATIONS.md` 운영 가이드
- [x] **백업 & 복구 자동화** ✅ 완료 (2026-03-22)
  - `scripts/backup.sh` - HDB/WAL/Config 백업, S3 업로드
  - `scripts/restore.sh` - 재해 복구, WAL replay
  - `scripts/eod_process.sh` - EOD 프로세스 자동화
  - cron: 백업 (02:00), EOD (18:00)
- [x] **프로덕션 서비스 설치** ✅ 완료 (2026-03-22)
  - `scripts/install_service.sh` - 원스텝 설치
  - `scripts/apex-db.service` - systemd 서비스
  - 자동 재시작, CPU affinity, OOM 방지
  - 로그 로테이션 (30일)
- [x] **Feed Handler Toolkit (완전 버전)** ✅ 완료 (2026-03-22)
  - **구현 (8 헤더 + 5 구현):**
    - `src/feeds/fix_parser.cpp` - FIX 프로토콜 (350ns 파싱)
    - `src/feeds/fix_feed_handler.cpp` - FIX TCP 리시버 (비동기, 재연결)
    - `src/feeds/multicast_receiver.cpp` - 멀티캐스트 UDP (<1μs)
    - `src/feeds/nasdaq_itch.cpp` - NASDAQ ITCH 5.0 (250ns 파싱)
    - `src/feeds/optimized/fix_parser_fast.cpp` - 최적화 버전 (zero-copy, SIMD)
  - **테스트 (27개 단위 + 10개 벤치마크):**
    - `tests/feeds/test_fix_parser.cpp` - 15개 테스트 (100% 커버리지)
    - `tests/feeds/test_nasdaq_itch.cpp` - 12개 테스트 (100% 커버리지)
    - `tests/feeds/benchmark_feed_handlers.cpp` - 성능 검증
  - **최적화 (6가지 기법):**
    - Zero-copy 파싱 (2-3x), SIMD AVX2 (5-10x), Memory Pool (10-20x)
    - Lock-free Ring Buffer (3-5x), 빠른 숫자 파싱 (2-3x), Cache-line alignment (2-4x)
  - **통합 예제:**
    - `examples/feed_handler_integration.cpp` - FIX/ITCH/성능 테스트
  - **문서:**
    - `docs/feeds/FEED_HANDLER_GUIDE.md` - 사용 가이드
    - `docs/feeds/PERFORMANCE_OPTIMIZATION.md` - 최적화 가이드
    - `docs/feeds/FEED_HANDLER_COMPLETE.md` - 완료 보고서
  - `src/feeds/nasdaq_itch.cpp` - NASDAQ ITCH 5.0 파서 (바이너리)
  - `include/apex/feeds/binance_feed.h` - Binance WebSocket (인터페이스)
  - `examples/feed_handler_integration.cpp` - 통합 예제
  - `docs/feeds/FEED_HANDLER_GUIDE.md` - 사용 가이드
  - **비즈니스 가치:** HFT 시장 진입 ($2.5M-12M), 거래소 직접 연결, kdb+ 완전 대체
- [ ] **마이그레이션 툴킷** — 경쟁 제품 → APEX-DB 자동 변환 (고객 확보 핵심)
  - **Priority 0: kdb+ → APEX-DB** (7주, $2.5M-12M ARR)
    - q → SQL 트랜스파일러 (4주) - select/where/fby/aj/wj 자동 변환
    - HDB 데이터 로더 (2주) - Splayed tables → Columnar format
    - 성능 검증 도구 (1주) - TPC-H + 금융 쿼리 벤치마크
    - **비즈니스 가치:** HFT 시장 진입, 가장 큰 ARPU ($250K-500K/고객)
  - **Priority 1: ClickHouse → APEX-DB** (4주, $1M-3M ARR)
    - SQL 방언 변환 (1주) - arrayJoin/uniq → UNNEST/COUNT(DISTINCT)
    - 데이터 마이그레이션 (1주) - MergeTree → Columnar
    - 쿼리 최적화 (1주) - 느린 쿼리 감지 + Index 추천
    - PoC 자동화 (1주) - 원클릭 마이그레이션
    - **비즈니스 가치:** 빠른 첫 매출 (3개월), 광고 테크/SaaS 분석 시장
  - **Priority 1: DuckDB 상호운용성** (2주, 전략적)
    - DuckDB Parquet → APEX-DB (1주) - Arrow zero-copy
    - 벤치마크 + 블로그 (1주) - "DuckDB의 실시간 버전"
    - **비즈니스 가치:** Hacker News 론칭, 인바운드 리드 50-100/월, Python 커뮤니티
  - **Priority 2: TimescaleDB → APEX-DB** (3주, $500K-1M ARR)
    - 스키마 변환 (1주) - Hypertables → APEX-DB tables
    - pg_dump 자동 변환 (1주)
    - 함수 매핑 (1주) - time_bucket → xbar
    - **비즈니스 가치:** IoT/DevOps 모니터링 시장
  - **전략적: Snowflake/Delta Lake Hybrid 지원** (4주, $3.5M ARR)
    - Snowflake 커넥터 (2주) - JDBC/ODBC 통합, Cold data 쿼리
    - Delta Lake Reader (2주) - Parquet + transaction log 읽기
    - Hybrid 아키텍처 가이드 - "Snowflake for batch, APEX-DB for real-time"
    - **타겟 워크로드:**
      - 실시간 금융 분석 (20 고객 × $50K = $1M)
      - IoT/센서 데이터 (10 고객 × $50K = $500K)
      - 광고 테크 실시간 입찰 (10 고객 × $100K = $1M)
      - 규제 산업 온프레미스 (5 고객 × $200K = $1M)
    - **비즈니스 가치:** 보완재 전략, Snowflake 고객의 실시간 pain 해결
- [ ] **베어메탈 튜닝 상세 가이드** — CPU pinning, NUMA, io_uring
- [ ] **Kubernetes 운영 가이드** — Helm, monitoring, troubleshooting
- [ ] **웹사이트 & 문서 사이트** — apex-db.io, docs.apex-db.io

## 중간 우선순위
- [ ] **분산 쿼리 스케줄러** — DistributedQueryScheduler 구현 (UCX transport 위)
  - PartialAggResult FlatBuffers 직렬화
  - scatter: fragments → UCX send → 각 노드 실행
  - gather: UCX recv → PartialAggResult::merge()
  - 멀티노드 벤치마크 (2-node scatter/gather 지연)
  - **참고:** 단일 노드 병렬화는 이미 완료 (LocalQueryScheduler, 8T = 3.48x)
- [ ] **Data/Compute 노드 분리** — JOIN을 별도 Compute Node에서 RDMA remote_read로 실행, Data Node 영향 제로
- [ ] **CHUNKED 모드 활성화** — 단일 대형 파티션 행 분할 병렬화
- [ ] **exec_simple_select 병렬화** — 현재는 집계만 병렬, SELECT도 병렬화
- [ ] **DuckDB 임베딩 (복잡한 JOIN 위임)** — Arrow zero-copy 전달
- [ ] **JIT SIMD emit** — LLVM JIT에서 AVX2/512 벡터 IR 생성
- [ ] **멀티스레드 drain** — sharded drain threads
- [ ] **Ring Buffer 동적 조정** — direct-to-storage 경로
- [ ] **HugePages 튜닝** — 자동화
- [ ] **리소스 격리** — realtime(코어0-3) vs analytics(코어4-7) CPU pinning

## 스토리지 & 포맷 확장
- [ ] **Parquet 익스포트** — `EXPORT TABLE TO 'file.parquet'` SQL 지원
  - HDB 컬럼 배열 → Apache Parquet 직렬화
  - Spark, DuckDB, Polars 연동 가능
  - **비즈니스 가치:** 데이터 레이크 연동, 퀀트 리서치 워크플로우
- [ ] **Arrow Flight 서버** — 네트워크로 Arrow 포맷 전송
  - 분산 쿼리 결과를 Arrow 배치로 스트리밍
  - Pandas/Polars 클라이언트 직접 연결
  - **비즈니스 가치:** 데이터 엔지니어링 팀 채택 가속
- [ ] **S3 HDB flush** — NVMe 외 S3 직접 flush 지원
  - 파티션 → S3 멀티파트 업로드 (비동기)
  - S3 prefix: `s3://bucket/{symbol}/{hour}/`
  - **비즈니스 가치:** 무한 스케일 히스토리 저장, 클라우드 네이티브

## 보안 & 엔터프라이즈
- [ ] **TLS/SSL** — HTTPS 엔드포인트, mTLS 노드 간 통신
  - HTTP API 443 포트 지원
  - 클러스터 노드 간 인증서 기반 상호 인증
  - **비즈니스 가치:** 프로덕션 배포 필수, 엔터프라이즈 보안 요구사항
- [ ] **API Key / JWT 인증** — HTTP API 접근 제어
  - Bearer token 인증
  - 사용자별 쿼리 추적
- [ ] **RBAC (역할 기반 접근 제어)** — 테이블/컬럼 레벨 권한
  - 팀원마다 접근 가능한 심볼/테이블 제한
  - **비즈니스 가치:** 멀티테넌트 운영, 금융 규제 요구사항
- [ ] **Audit Log** — 누가 언제 어떤 쿼리를 실행했는지 추적
  - EMIR, MiFID II, Basel IV 규제 대응
  - **비즈니스 가치:** 컴플라이언스 시장 진입 필수

## SQL 완성도
- [ ] **Subquery / CTE (WITH 절)** — `WITH daily AS (...) SELECT ...`
- [ ] **CASE WHEN** — 조건 분기 컬럼
- [ ] **UNION / INTERSECT / EXCEPT** — 결과 집합 연산
- [ ] **NULL 처리 표준화** — INT64_MIN 센티넬 → 실제 NULL
- [ ] **날짜/시간 함수** — `DATE_TRUNC`, `NOW()`, `EXTRACT`, `INTERVAL`
- [ ] **String 함수** — `LIKE`, `SUBSTR`, 심볼명 조작
- [ ] **RIGHT JOIN / FULL OUTER JOIN** — SQL 표준 완성
- [ ] **EXPLAIN** — 쿼리 실행 계획 출력 (디버깅, 최적화)

## 클라이언트 생태계
- [ ] **JDBC/ODBC 드라이버** — Tableau, Excel, BI 툴 연결
  - ClickHouse JDBC 호환 구현
  - **비즈니스 가치:** 기업 BI 팀 채택 (데이터 애널리스트 자기주도 사용)
- [ ] **ClickHouse wire protocol** — 바이너리 프로토콜 완전 호환
  - 기존 CH 클라이언트 라이브러리 (Go, Java, .NET) 그대로 사용
  - **비즈니스 가치:** ClickHouse 사용자 마이그레이션 마찰 제로
- [ ] **공식 Python 패키지** — `pip install apex-db`
  - PyPI 배포, `apex.connect("localhost:8123")`
  - **비즈니스 가치:** 개발자 채택률 10x

## Physical AI / 산업 특화
- [ ] **ROS2 플러그인** — ROS2 토픽 → APEX-DB 직접 인제스션
  - `ros2 run apex_db ros_bridge --topic /lidar/scan`
  - **비즈니스 가치:** 자율주행/로봇 시장 진입 핵심
- [ ] **NVIDIA Isaac 통합** — Isaac Sim 센서 데이터 → APEX-DB
  - **비즈니스 가치:** Physical AI 에코시스템 채택
- [ ] **OPC-UA 커넥터** — 산업 표준 프로토콜 지원
  - 지멘스 S7, 패닉 PLC 등 팩토리 장비 직접 연결
  - **비즈니스 가치:** 스마트팩토리 시장 진입
- [ ] **MQTT 인제스션** — IoT 기기 직접 연결
  - Eclipse Mosquitto, AWS IoT Core 연동
  - **비즈니스 가치:** IoT/에지 컴퓨팅 시장

## HA & 복제
- [ ] **WAL 기반 비동기 레플리케이션** — Primary 장애 시 데이터 유실 방지
  - WAL 로그 → Replica에 비동기 전송
  - **비즈니스 가치:** 프로덕션 HA 필수
- [ ] **자동 페일오버** — Primary 죽으면 Replica 자동 승격
  - Raft 또는 단순 heartbeat 기반
  - **비즈니스 가치:** 99.99% SLA 제공 가능
- [ ] **스냅샷 백업** — HDB 전체 일관된 스냅샷
  - S3 업로드 자동화
  - **비즈니스 가치:** 재해 복구 (DR) 지원

## DDL & 데이터 관리
- [ ] **CREATE TABLE / DROP TABLE** — 코드 없이 테이블 생성
- [ ] **Retention Policy** — `ALTER TABLE SET TTL 30 DAYS`
  - 30일 이전 HDB 파티션 자동 삭제
  - **비즈니스 가치:** 스토리지 비용 관리 자동화
- [ ] **Schema Evolution** — 컬럼 추가/삭제 무중단
- [ ] **HDB Compaction** — 소규모 파티션 파일 병합 (읽기 성능 향상)

## 낮은 우선순위 (Phase C-3 이후)
- [ ] **AWS Fleet API 통합** — Warm Pool + Placement Group
- [ ] **DynamoDB 메타데이터** — 파티션 맵
- [ ] **Graph 인덱스 (CSR)** — FDS 자금 이동 추적
- [ ] **InfluxDB 마이그레이션** — InfluxQL → SQL (전략적 가치 낮음)
- [ ] **Graviton (ARM) 빌드 테스트** — r8g 인스턴스, Highway SVE

## 완료
- [x] Phase E — End-to-End Pipeline MVP (5.52M ticks/sec)
- [x] Phase B — Highway SIMD + LLVM JIT (filter 272μs, VWAP 532μs)
- [x] Phase B v2 — BitMask filter (11x), JIT O3 (2.6x)
- [x] Phase A — HDB Tiered Storage + LZ4 (4.8 GB/s flush)
- [x] Phase D — Python Bridge (pybind11, zero-copy 522ns)
- [x] **병렬 쿼리 엔진** — LocalQueryScheduler + WorkerPool (8T = 3.48x)
  - QueryScheduler 추상화 (scatter/gather DI 패턴)
  - ParallelScanExecutor (PARTITION/CHUNKED/SERIAL 자동 선택)
  - 싱글 노드 오버헤드 제로 (num_threads <= 1 or rows < 100K → SERIAL)
  - 벤치마크: GROUP BY 1M rows 0.862ms → 0.248ms (8T)
- [x] **asof JOIN** — AsofJoinOperator (투 포인터 O(n))
- [x] **Hash JOIN (inner/equi)** — HashJoinOperator
- [x] **GROUP BY 집계** — sum/avg/min/max/count
- [x] **Window 함수** — SUM/AVG/MIN/MAX/ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD OVER
- [x] **금융 함수** — VWAP, xbar, EMA, DELTA, RATIO, FIRST, LAST, Window JOIN (wj)
- [x] SQL Parser — 기본 SELECT/WHERE/GROUP BY/JOIN/OVER
- [x] HTTP API — port 8123, ClickHouse 호환
- [x] Distributed Cluster Transport — UCXBackend, SharedMemBackend, PartitionRouter (2ns)
- [ ] Phase C — Distributed Memory (UCX 완료, 쿼리 스케줄러 TODO)
