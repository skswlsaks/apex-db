<div align="center">

# 🏛️ APEX-DB

### Ultra-Low Latency In-Memory Database for HFT

*금융 고빈도 매매(HFT) 특화 차세대 인메모리 데이터베이스*

![C++20](https://img.shields.io/badge/C%2B%2B-20-blue)
![LLVM](https://img.shields.io/badge/LLVM-19-orange)
![License](https://img.shields.io/badge/license-proprietary-red)
![Status](https://img.shields.io/badge/status-MVP-green)

</div>

---

## Overview

APEX-DB는 기존 kdb+의 한계를 돌파하기 위해 설계된 **클라우드 네이티브 초저지연 인메모리 데이터베이스**입니다.

**핵심 차별점:**
- 🚀 **5.52M ticks/sec** 인제스션 처리량 (kdb+ tickerplant 동등~우위)
- ⚡ **914M rows/sec** VWAP 쿼리 처리 (scalar 기준, SIMD 적용 시 10x+ 예상)
- 🧠 **LLVM JIT 컴파일러** — 동적 쿼리를 런타임에 기계어로 변환
- 🔗 **Research → Production 무번역** — Python 코드가 C++ 성능으로 직접 실행
- 📊 **Zero-Copy** — NIC → 스토리지 → Python까지 데이터 복사 없음

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Layer 4: Transpiler                   │
│           Python DSL → AST → C++ DAG (nanobind)         │
├─────────────────────────────────────────────────────────┤
│                 Layer 3: Execution Engine                │
│        Vectorized Pipeline + LLVM JIT + Highway SIMD    │
├─────────────────────────────────────────────────────────┤
│                 Layer 2: Ingestion (Tick Plant)          │
│       MPMC Ring Buffer + RDMA/UCX + WAL + Zero-Copy     │
├─────────────────────────────────────────────────────────┤
│              Layer 1: Storage Engine (DMMT)              │
│    Arena Allocator + Columnar Store + Partition Manager  │
│         CXL/RDMA Global Shared Memory Pool              │
└─────────────────────────────────────────────────────────┘
```

## Tech Stack

| 구분 | 기술 |
|---|---|
| 언어 | C++20, Rust (패킷 파싱) |
| 컴파일러 | Clang 19 |
| SIMD | Google Highway (AVX-512 / ARM SVE 자동 디스패치) |
| JIT | LLVM 19 OrcJIT |
| 통신 | UCX (RDMA/InfiniBand/AWS EFA 통합) |
| 메모리 | HugePages, NUMA-aware, CXL 3.0 |
| 데이터 포맷 | Apache Arrow C Data Interface 호환 |
| 직렬화 | FlatBuffers |
| Python 바인딩 | nanobind (Zero-copy) |
| 빌드 | CMake + Ninja |
| 테스트 | Google Test |

## Project Structure

```
apex-db/
├── CMakeLists.txt
├── README.md
├── include/apex/           # Public headers
│   ├── common/             #   Types, Logger
│   ├── core/               #   Pipeline (E2E)
│   ├── storage/            #   Arena, ColumnStore, PartitionManager
│   ├── ingestion/          #   RingBuffer, TickPlant, WAL
│   ├── execution/          #   VectorizedEngine, QueryPlanner, JIT
│   └── transpiler/         #   Python DSL bridge (planned)
├── src/                    # Implementation
│   ├── common/
│   ├── core/
│   ├── storage/
│   ├── ingestion/
│   └── execution/
├── tests/
│   ├── unit/               # Google Test unit tests
│   └── bench/              # Benchmarks
├── docs/
│   ├── design/             # 아키텍처 & 레이어별 설계 문서
│   ├── requirements/       # 시스템 요구사항 (PRD/SRS)
│   ├── bench/              # 벤치마크 결과
│   └── devlog/             # 개발 일지
└── scripts/                # Utility scripts
```

## Quick Start

### Prerequisites

```bash
# Amazon Linux 2023 / Fedora
sudo dnf install -y \
  clang19 clang19-devel \
  llvm19-devel llvm19-libs \
  highway-devel \
  numactl-devel \
  ucx-devel \
  boost-devel \
  ninja-build
```

### Build

```bash
cd apex-db
mkdir -p build && cd build

cmake .. \
  -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang-19 \
  -DCMAKE_CXX_COMPILER=clang++-19

ninja -j$(nproc)
```

### Test

```bash
./tests/apex_tests          # 19 unit tests
./bench_pipeline            # E2E benchmark
```

## Benchmark Results (Phase E — MVP)

*환경: Intel Xeon 6975P-C, 8 vCPU, 30GB RAM, Amazon Linux 2023*

### Ingestion
| 배치 | 처리량 |
|---|---|
| Single | 4.97M ticks/sec |
| Batch 512 | **5.52M ticks/sec** |

### Query Latency
| 쿼리 | 100K rows | 1M rows | 5M rows |
|---|---|---|---|
| VWAP (p50) | 52μs | 637μs | 3.5ms |
| Filter+Sum (p50) | 75μs | 789μs | 3.9ms |
| Count (p50) | 0.1μs | 0.1μs | 0.1μs |

### vs kdb+ (참고치)
| 메트릭 | kdb+ | APEX-DB | 상태 |
|---|---|---|---|
| 인제스션 | ~2-5M/sec | 5.52M/sec | ✅ |
| VWAP 1M | ~500-800μs | 637μs | ✅ |

> SIMD + JIT 적용 후 쿼리 10-30x 개선 예상

## Development Phases

- [x] **Phase E** — End-to-End Pipeline MVP
- [ ] **Phase B** — Highway SIMD + LLVM JIT (in progress)
- [ ] **Phase A** — HDB Flush & Tiered Storage
- [ ] **Phase D** — Python Transpiler Bridge
- [ ] **Phase C** — Distributed Memory (Multi-node)

## Docs

| 문서 | 설명 |
|---|---|
| [초기 전략서](docs/design/initial_doc.md) | 타겟 시장, 설계 원칙, 클라우드 전략 |
| [고수준 아키텍처](docs/design/high_level_architecture.md) | 4레이어 시스템 구조 |
| [코어 아키텍처](docs/design/architecture_design.md) | 엔진 상세 설계, R2P 브릿지 |
| [시스템 요구사항](docs/requirements/system_requirements.md) | PRD & SRS |
| [Layer 1: Storage](docs/design/layer1_storage_memory.md) | DMMT, Arena, 파티셔닝 |
| [Layer 2: Ingestion](docs/design/layer2_ingestion_network.md) | Tick Plant, MPMC, RDMA |
| [Layer 3: Execution](docs/design/layer3_execution_engine.md) | 벡터화, JIT, SIMD |
| [Layer 4: Transpiler](docs/design/layer4_transpiler_client.md) | Python DSL, Zero-copy |

## Target Market

- 🏦 고빈도 매매(HFT) 헤지펀드
- 🏛️ 투자은행(IB) 리스크 시스템
- 🪙 가상자산 거래소 인프라
- 🔍 실시간 이상거래 탐지(FDS)

## License

Proprietary — All rights reserved.
