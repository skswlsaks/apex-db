# High-Level DB Software Architecture Design
# ⚠️ 최종 업데이트: 2026-03-22 (devlog #011: 병렬 쿼리, #010: 금융 함수 완료)

## Overview

APEX-DB는 **실시간 + 분석을 통합**하는 초저지연 인메모리 데이터베이스.
HFT 특화로 시작했지만 범용 OLAP, TSDb, ML Feature Store로 확장됨.

**kdb+ 대체율: HFT 95%, 퀀트 90%, 리스크 95%** — 핵심 금융 함수 완료 (xbar, EMA, Window JOIN, asof JOIN)

## 시스템 레이어 구조 (현재 실제 구현)

```
┌──────────────────────────────────────────────────────────┐
│  Layer 5: Client Interface (현재 구현)                    │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────┐ │
│  │ HTTP API     │  │ Python DSL   │  │ C++ Direct API  │ │
│  │ (port 8123)  │  │ (pybind11)   │  │ (zero-copy)     │ │
│  │ ClickHouse   │  │ Lazy Eval    │  │ ApexPipeline    │ │
│  │ compatible  │  │ collect()    │  │                 │ │
│  └─────────────┘  └──────────────┘  └─────────────────┘ │
├──────────────────────────────────────────────────────────┤
│  Layer 4: SQL + Query Planning                            │
│  ┌──────────────────┐  ┌────────────────────────────┐    │
│  │ SQL Parser        │  │ Query Executor              │    │
│  │ Recursive descent │  │ AST → SIMD Engine           │    │
│  │ 1.5~4.5μs parse  │  │ JOIN dispatch               │    │
│  └──────────────────┘  └────────────────────────────┘    │
├──────────────────────────────────────────────────────────┤
│  Layer 3: Execution Engine                                │
│  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Vectorized    │  │ LLVM JIT     │  │ JOIN Ops      │  │
│  │ Engine        │  │ O3 compiler  │  │ AsofJoin O(n) │  │
│  │ Highway SIMD  │  │ 1.3ms/1M     │  │ HashJoin/LEFT │  │
│  │ BitMask filter│  │              │  │ Window JOIN   │  │
│  │ 272μs/1M      │  │              │  │ (O(n log m))  │  │
│  └───────────────┘  └──────────────┘  └──────────────┘  │
│  ┌──────────────────────────────────────────────────┐    │
│  │ 금융 함수 (kdb+ 호환)                              │    │
│  │ Window: EMA/DELTA/RATIO/SUM/AVG/LAG/LEAD         │    │
│  │ 집계: FIRST/LAST/xbar (시간 바)                   │    │
│  │ Window JOIN: wj_avg/sum/count/min/max            │    │
│  └──────────────────────────────────────────────────┘    │
│  ┌──────────────────────────────────────────────────┐    │
│  │ QueryScheduler (DI 패턴, devlog #011)             │    │
│  │  LocalQueryScheduler  │  DistributedScheduler(stub)   │
│  │  WorkerPool (jthread) │  → UCX transport (향후)   │    │
│  │  scatter/gather API   │  PartialAggResult merge   │    │
│  │  GROUP BY 3.48x (8T)  │  코드 변경 없이 노드 추가 │    │
│  └──────────────────────────────────────────────────┘    │
├──────────────────────────────────────────────────────────┤
│  Layer 2: Ingestion (Tick Plant)                          │
│  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ MPMC RingBuf  │  │ UCX/RDMA     │  │ WAL Writer   │  │
│  │ 65K slots     │  │ Kernel bypass│  │ crash safe   │  │
│  │ Lock-free     │  │ zero-copy    │  │              │  │
│  └───────────────┘  └──────────────┘  └──────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Layer 1: Storage Engine (DMMT)                           │
│  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Arena         │  │ Column Store │  │ HDB Writer   │  │
│  │ Allocator     │  │ Arrow compat │  │ LZ4 압축     │  │
│  │ Bump pointer  │  │ Append-only  │  │ 4.8GB/s      │  │
│  │ Lock-free     │  │ BitMask      │  │ mmap read    │  │
│  └───────────────┘  └──────────────┘  └──────────────┘  │
├──────────────────────────────────────────────────────────┤
│  Layer 0: Distributed Cluster (Phase C)                   │
│  ┌───────────────┐  ┌──────────────┐  ┌──────────────┐  │
│  │ Transport     │  │ Partition    │  │ Health       │  │
│  │ UCX (now)     │  │ Router       │  │ Monitor      │  │
│  │ CXL (future)  │  │ ConsistHash  │  │ Heartbeat    │  │
│  │ SharedMem(test│  │ 2ns lookup   │  │ Failover     │  │
│  └───────────────┘  └──────────────┘  └──────────────┘  │
└──────────────────────────────────────────────────────────┘
```

## 핵심 성능 수치 (현재 실측)

| 메트릭 | 수치 | 비고 |
|---|---|---|
| 인제스션 | 5.52M ticks/sec | MPMC Ring Buffer |
| filter 1M (BitMask) | 272μs | kdb+ 범위 진입 |
| VWAP 1M | 532μs | Highway SIMD |
| Window SUM 1M | 1.36ms | prefix sum O(n) |
| Hash Join 1M | 42ms | unordered_map |
| ASOF Join 1M | 53ms | binary search |
| **xbar (시간 바)** | 24ms | 1M → 3.3K 5분봉 |
| **EMA** | 2.2ms/1M | O(n) 단일 패스 |
| **DELTA/RATIO** | <2ms/1M | 행간 차이/비율 |
| HDB flush | 4.8 GB/s | NVMe sequential |
| SQL 파싱 | 1.5~4.5μs | recursive descent |
| GROUP BY 병렬 (8T) | 0.248ms/1M | 직렬 대비 3.48x |
| Transport (SHM) | 13.5ns | CXL sim baseline |
| Partition routing | 2.0ns | consistent hash |
| Python zero-copy | 522ns | pybind11 numpy |

## 타겟 시장 (확장)

| 분야 | 사용 사례 | 핵심 기능 | kdb+ 대체율 |
|---|---|---|---|
| HFT | 틱 처리, 실시간 쿼리 | ASOF JOIN, xbar, Window JOIN | **95%** |
| 퀀트 리서치 | 백테스트, 팩터 분석 | EMA/DELTA/RATIO, Python DSL | **90%** |
| 리스크 관리 | 포지션×시나리오, VaR | LEFT JOIN, GROUP BY 병렬 | **95%** |
| FDS | 이상거래 탐지 | 실시간 Window JOIN | **85%** |
| OLAP | ClickHouse 대체 | SQL, HTTP (port 8123), 병렬 쿼리 | — |
| IoT/모니터링 | 시계열 집계 | xbar 시간 바, HDB 압축 | — |
| ML Feature Store | 실시간 feature 서빙 | zero-copy Python, Arrow 호환 | — |

## 구현 기술 스택 (확정)

| 레이어 | 기술 | 비고 |
|---|---|---|
| 언어 | C++20 | Clang 19 |
| SIMD | Google Highway 1.2 | AVX-512/ARM SVE 자동 |
| JIT | LLVM 19 OrcJIT | 동적 쿼리 컴파일 |
| 통신 | UCX (→ CXL) | Transport 교체 가능 |
| 압축 | LZ4 | 0.31 압축비 |
| 포맷 | Apache Arrow 호환 | zero-copy |
| Python | pybind11 (nanobind 대신) | 안정성 우선 |
| HTTP | cpp-httplib | ClickHouse port 8123 |
| 빌드 | CMake + Ninja | |
| 테스트 | Google Test | |
