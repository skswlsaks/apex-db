<div align="center">

# 🏛️ APEX-DB

### Ultra-Low Latency In-Memory Database

*실시간 + 분석을 통합하는 초저지연 인메모리 데이터베이스*

![C++20](https://img.shields.io/badge/C%2B%2B-20-blue)
![LLVM 19](https://img.shields.io/badge/LLVM-19-orange)
![Highway SIMD](https://img.shields.io/badge/SIMD-Highway-green)
![Tests](https://img.shields.io/badge/tests-53%2B%20passing-brightgreen)

</div>

---

## Overview

APEX-DB는 HFT 특화로 설계된 초저지연 인메모리 데이터베이스로,
**kdb+의 성능**과 **ClickHouse의 범용성**, **Polars의 Python 생태계**를 통합합니다.

### 핵심 성능 (실측)

| 메트릭 | 수치 |
|---|---|
| 인제스션 | **5.52M ticks/sec** |
| filter 1M rows | **272μs** (kdb+ 범위 진입) |
| VWAP 1M rows | **532μs** |
| Window SUM 1M | **1.36ms** (O(n) prefix sum) |
| SQL 파싱 | **1.5~4.5μs** |
| Python zero-copy | **522ns** (numpy view) |
| HDB flush | **4.8 GB/s** |
| Partition routing | **2ns** |

### vs kdb+ / ClickHouse

| | kdb+ | ClickHouse | **APEX-DB** |
|---|---|---|---|
| 인제스션 | ~5M/sec | 배치 최적화 | **5.52M/sec** |
| filter 1M | ~100-300μs | ms 단위 | **272μs** ✅ |
| VWAP 1M | ~200-500μs | ms 단위 | **532μs** ✅ |
| SQL | q 언어 | 표준 SQL | **표준 SQL** |
| Python | PyKX (IPC) | clickhouse-connect | **zero-copy** |
| 오픈소스 | ❌ 유료 | ✅ | ✅ |

---

## Architecture

```
┌───────────────────────────────────────────────────┐
│  Layer 5: Client Interface                         │
│  HTTP API (port 8123) · Python DSL · C++ API      │
├───────────────────────────────────────────────────┤
│  Layer 4: SQL + Query Planning                     │
│  Recursive descent parser · AST executor          │
├───────────────────────────────────────────────────┤
│  Layer 3: Execution Engine                         │
│  Highway SIMD · LLVM JIT · ASOF/Hash JOIN         │
│  Window Functions (ROW_NUMBER/SUM/AVG/LAG/LEAD)   │
├───────────────────────────────────────────────────┤
│  Layer 2: Ingestion (Tick Plant)                   │
│  MPMC Ring Buffer · UCX/RDMA · WAL               │
├───────────────────────────────────────────────────┤
│  Layer 1: Storage Engine (DMMT)                    │
│  Arena Allocator · Column Store · HDB (LZ4)       │
├───────────────────────────────────────────────────┤
│  Layer 0: Distributed Cluster                      │
│  Transport (UCX→CXL) · Consistent Hash · Health  │
└───────────────────────────────────────────────────┘
```

---

## Quick Start

### SQL via HTTP (ClickHouse compatible)

```bash
# 서버 시작
./apex_server --port 8123

# 쿼리 (curl)
curl -X POST http://localhost:8123/ \
  -d 'SELECT vwap(price, volume), count(*) FROM trades WHERE symbol = 1'

# Grafana: ClickHouse 데이터소스로 바로 연결
```

### Python DSL (zero-copy)

```python
import apex
from apex_py.dsl import DataFrame

db = apex.Pipeline()
db.start()

# 틱 인제스트
db.ingest(symbol=1, price=15000, volume=100)
db.drain()

# zero-copy numpy (복사 없음)
prices = db.get_column(symbol=1, name="price")  # 522ns

# Lazy DSL (Polars 스타일)
df = DataFrame(db, symbol=1)
ma20 = df['price'].rolling(20).mean().collect()  # C++ 실행
```

### SQL 예시

```sql
-- 기본 집계
SELECT symbol, vwap(price, volume), sum(volume)
FROM trades
WHERE timestamp BETWEEN 1000000 AND 2000000
GROUP BY symbol

-- ASOF JOIN (시계열 핵심)
SELECT t.price, q.bid, q.ask
FROM trades t
ASOF JOIN quotes q
ON t.symbol = q.symbol AND t.timestamp >= q.timestamp

-- Window 함수
SELECT symbol, price,
       AVG(price) OVER (PARTITION BY symbol ROWS 20 PRECEDING) AS ma20,
       LAG(price, 1) OVER (PARTITION BY symbol) AS prev_price,
       RANK() OVER (ORDER BY price DESC) AS rank
FROM trades
```

---

## Build

```bash
# Dependencies (Amazon Linux 2023 / Fedora)
sudo dnf install -y clang19 clang19-devel llvm19-devel \
  highway-devel numactl-devel ucx-devel ninja-build lz4-devel

mkdir -p build && cd build
cmake .. -G Ninja -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-19 -DCMAKE_CXX_COMPILER=clang++-19
ninja -j$(nproc)

# Tests
./tests/apex_tests
python3 -m pytest ../tests/test_python.py -v
```

---

## Project Structure

```
apex-db/
├── include/apex/
│   ├── storage/        # Arena, ColumnStore, PartitionManager, HDB
│   ├── ingestion/      # RingBuffer, TickPlant, WAL
│   ├── execution/      # VectorizedEngine, JIT, JOIN, WindowFunctions
│   ├── sql/            # Tokenizer, Parser, AST, Executor
│   ├── server/         # HttpServer (ClickHouse compatible)
│   ├── core/           # ApexPipeline (E2E integration)
│   ├── cluster/        # Transport, PartitionRouter, HealthMonitor
│   └── transpiler/     # Python binding (pybind11)
├── src/                # Implementation
├── tests/
│   ├── unit/           # Google Test (53+ tests)
│   └── bench/          # Benchmarks
└── docs/
    ├── design/         # Architecture + Layer design docs
    ├── requirements/   # PRD/SRS
    ├── bench/          # Benchmark results
    └── devlog/         # Development log (000~008)
```

---

## Target Markets

| 분야 | 사용 사례 |
|---|---|
| HFT/금융 | 틱 처리, ASOF JOIN, 리스크 계산 |
| 퀀트 리서치 | 백테스트, Window 함수, Python DSL |
| OLAP | ClickHouse 대체, Grafana 연동 |
| IoT/모니터링 | 시계열 분석, LZ4 압축 |
| ML | Feature Store, zero-copy numpy |

---

## Development Phases

- [x] **Phase E** — E2E Pipeline MVP (5.52M ticks/sec)
- [x] **Phase B** — SIMD + JIT (BitMask 11x, filter kdb+ 범위)
- [x] **Phase A** — HDB Tiered Storage (LZ4, 4.8GB/s flush)
- [x] **Phase D** — Python Bridge (zero-copy, 4x vs Polars)
- [x] **Phase C** — Distributed Cluster (CXL 시뮬, 2ns routing)
- [x] **SQL + HTTP** — Parser + ClickHouse API
- [x] **JOIN + Window** — ASOF, Hash, ROW_NUMBER~LEAD
- [ ] GROUP BY 최적화
- [ ] ARM Graviton 검증
- [ ] AWS Fleet API 통합

## License

Proprietary — All rights reserved.
