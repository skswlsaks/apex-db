# High-Level DB Software Architecture Design
# ⚠️ 최종 업데이트: 2026-03-22 (devlog #011: 병렬 쿼리 엔진 + QueryScheduler DI 반영)

## Overview

APEX-DB는 **실시간 + 분석을 통합**하는 초저지연 인메모리 데이터베이스.
HFT 특화로 시작했지만 범용 OLAP, TSDb, ML Feature Store로 확장됨.

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
│  │ Highway SIMD  │  │ 1.3ms/1M     │  │ HashJoin      │  │
│  │ BitMask filter│  │              │  │ Window Funcs  │  │
│  │ 272μs/1M      │  │              │  │ (prefix sum)  │  │
│  └───────────────┘  └──────────────┘  └──────────────┘  │
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
| HDB flush | 4.8 GB/s | NVMe sequential |
| SQL 파싱 | 1.5~4.5μs | recursive descent |
| GROUP BY 병렬 (8T) | 0.248ms/1M | 직렬 대비 3.48x |
| Transport (SHM) | 13.5ns | CXL sim baseline |
| Partition routing | 2.0ns | consistent hash |
| Python zero-copy | 522ns | pybind11 numpy |

## 타겟 시장 (확장)

| 분야 | 사용 사례 | 핵심 기능 |
|---|---|---|
| HFT | 틱 처리, 실시간 쿼리 | μs 레이턴시, ASOF JOIN |
| 퀀트 리서치 | 백테스트, 팩터 분석 | Window 함수, Python DSL |
| 리스크 관리 | 포지션×시나리오 | Hash JOIN, GROUP BY |
| FDS | 이상거래 탐지 | 실시간 스트리밍 + 패턴 |
| OLAP | ClickHouse 대체 | SQL, HTTP API, 범용 쿼리 |
| IoT/모니터링 | 시계열 분석 | TSDb 모드, 압축 |
| ML | Feature 서빙 | zero-copy Python |

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
