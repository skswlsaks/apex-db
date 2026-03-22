# Layer 4: Client & Transpilation Layer
# ⚠️ 최종 업데이트: 2026-03-22 (pybind11, DSL, SQL, 금융 함수, 병렬 쿼리 반영)

본 문서는 APEX-DB의 클라이언트 인터페이스 레이어 설계 및 구현 현황입니다.

---

## 1. 구현된 인터페이스 (3가지)

### 1-A. HTTP API (port 8123, ClickHouse 호환)

```
POST /          SQL 쿼리 실행 → JSON 응답
GET  /ping      헬스체크
GET  /stats     파이프라인 통계
```

```bash
curl -X POST http://localhost:8123/ \
  -d 'SELECT vwap(price, volume) FROM trades WHERE symbol = 1'
# → {"columns":["vwap"],"data":[[15037.2]],"rows":1,"execution_time_us":52.3}
```

**구현:** `cpp-httplib` 헤더 전용, 경량, Grafana ClickHouse 플러그인 호환

### 1-B. Python DSL (pybind11 + Lazy Evaluation)

**원래 설계:** nanobind → **실제 구현:** pybind11 (안정성 우선)

```python
import apex

db = apex.Pipeline()
db.start()
db.ingest(symbol=1, price=15000, volume=100)
db.drain()

# 직접 호출
result = db.vwap(symbol=1)          # C++ 직접, 50μs
result = db.count(symbol=1)         # 0.12μs

# zero-copy numpy (핵심 기능)
prices = db.get_column(symbol=1, name="price")   # numpy, 522ns, 복사 없음
volumes = db.get_column(symbol=1, name="volume")

# Lazy DSL (Polars 스타일)
from apex_py.dsl import DataFrame
df = DataFrame(db, symbol=1)
result = df[df['price'] > 15000]['volume'].sum().collect()
```

**Polars vs APEX-DB 비교 (100K rows):**
| | APEX | Polars | 배율 |
|---|---|---|---|
| VWAP | 56.9μs | 228.7μs | **4x** |
| COUNT | 716ns | 26.3μs | **37x** |
| get_column | 522ns | 760ns | **1.5x** |

### 1-C. C++ Direct API

```cpp
ApexPipeline pipeline;
pipeline.start();

// 직접 C++ — 최저 레이턴시
auto result = pipeline.query_vwap(symbol=1);  // 51μs
auto col = pipeline.partition_manager()
    .get_or_create(1, ts)
    .get_column("price");  // 포인터 직접, 0 오버헤드
```

---

## 2. SQL 지원 범위 (현재 구현)

```sql
-- 기본 집계
SELECT count(*), sum(volume), avg(price), vwap(price, volume)
FROM trades WHERE symbol = 1

-- 필터 + 범위
SELECT * FROM trades
WHERE symbol = 1 AND price > 15000
AND timestamp BETWEEN 1000000 AND 2000000

-- GROUP BY
SELECT symbol, sum(volume) FROM trades GROUP BY symbol

-- ASOF JOIN (시계열 핵심)
SELECT t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q ON t.symbol = q.symbol AND t.timestamp >= q.timestamp

-- 일반 JOIN
SELECT t.price, r.risk_score
FROM trades t JOIN risk_factors r ON t.symbol = r.symbol

-- Window 함수
SELECT symbol, price,
       AVG(price) OVER (PARTITION BY symbol ROWS 20 PRECEDING) AS ma20,
       LAG(price, 1) OVER (PARTITION BY symbol) AS prev_price,
       RANK() OVER (ORDER BY price DESC) AS rank
FROM trades

-- 금융 함수 (kdb+ 호환)
SELECT xbar(timestamp, 300000000000) AS bar,
       first(price) AS open,
       max(price) AS high,
       min(price) AS low,
       last(price) AS close,
       sum(volume) AS volume
FROM trades WHERE symbol = 1
GROUP BY xbar(timestamp, 300000000000)

-- Window 함수 (EMA, DELTA, RATIO)
SELECT symbol, price,
       EMA(price, 0.1) OVER (PARTITION BY symbol ORDER BY timestamp) AS ema_slow,
       EMA(price, 20) OVER (PARTITION BY symbol ORDER BY timestamp) AS ema20,
       DELTA(price) OVER (ORDER BY timestamp) AS price_change,
       RATIO(price) OVER (ORDER BY timestamp) AS price_ratio
FROM trades

-- LEFT JOIN (NULL 센티넬)
SELECT t.price, t.volume, r.risk_score
FROM trades t
LEFT JOIN risk_factors r ON t.symbol = r.symbol

-- Window JOIN (wj)
SELECT t.price, wj_avg(q.bid) AS avg_bid, wj_count(q.bid) AS quote_count
FROM trades t
WINDOW JOIN quotes q
ON t.symbol = q.symbol
AND q.timestamp BETWEEN t.timestamp - 5000000000 AND t.timestamp + 5000000000
```

---

## 3. 원래 설계 vs 실제 구현 차이

| 항목 | 원래 설계 | 실제 구현 | 이유 |
|---|---|---|---|
| Python 바인딩 | nanobind | **pybind11** | 빌드 안정성 |
| AST 직렬화 | FlatBuffers | **직접 C++ 호출** | 복잡도 절감 |
| DSL → JIT | Python AST → LLVM | **Lazy Eval → C++ API** | 단계적 구현 |
| 클라이언트 프로토콜 | 자체 개발 | **HTTP (ClickHouse 호환)** | 에코시스템 |

---

## 4. 병렬 쿼리 (QueryScheduler DI)

**현재 구현:** LocalQueryScheduler — scatter/gather 패턴

```python
# Python DSL에서 병렬 쿼리 자동 활성화
db = apex.Pipeline(num_threads=8)
result = db.execute_sql(
    "SELECT symbol, sum(volume) FROM trades GROUP BY symbol"
)
# → 8스레드 병렬, 3.48x 속도 향상
```

**C++ API:**
```cpp
auto scheduler = std::make_unique<LocalQueryScheduler>(8);  // 8 threads
QueryExecutor executor(pipeline, std::move(scheduler));
auto result = executor.execute(ast);
// → 자동 scatter/gather, 0.248ms vs 0.862ms (직렬)
```

**향후:** DistributedQueryScheduler (UCX 기반) — 코드 변경 없이 멀티노드 확장

---

## 5. 향후 로드맵

- [x] **마이그레이션 툴킷** (apex-migrate CLI, kdb+/ClickHouse/DuckDB/TimescaleDB) ✅
- [ ] SQL Window 함수 RANGE 모드 (현재 ROWS만)
- [ ] Python DSL → LLVM JIT 직접 컴파일 (원래 설계 완성)
- [ ] ClickHouse wire protocol 호환 (TCP 바이너리)
- [ ] Grafana 공식 플러그인
---

## 6. 스트리밍 연동 (백로그)

계획된 데이터 소스 커넥터 (BACKLOG.md 참조):
- Kafka/Redpanda/Pulsar (librdkafka, C++ 클라이언트)
- AWS Kinesis, Azure Event Hubs, Google Pub/Sub
- PostgreSQL WAL (CDC), MySQL binlog, MongoDB Change Streams
- 거래소 직접 연결: CME FAST, OPRA, CBOE PITCH, Coinbase, Bybit

- [ ] Python 에코시스템 통합
  - [ ] `apex.from_polars(df)` — zero-copy (1-2주)
  - [ ] `apex.from_pandas(df)` — 변환 (1주)
  - [ ] Arrow 직접 지원 (3-4주)
