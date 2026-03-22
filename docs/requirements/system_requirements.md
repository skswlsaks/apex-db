# 차세대 HFT 인메모리 DB 시스템 요구사항 정의서
# ⚠️ 최종 업데이트: 2026-03-22 (SQL/HTTP/JOIN/Window/병렬쿼리/금융함수 반영)

---

## 1. 비즈니스 및 목표 시장

### 1차 타겟 (HFT/금융)
- 고빈도 매매(HFT), 퀀트 리서치, 리스크 관리, FDS

### 2차 타겟 (확장, 2026 추가)
- **ClickHouse 대체**: 실시간 OLAP, 대시보드
- **TSDb**: IoT, 인프라 모니터링
- **ML Feature Store**: 실시간 피처 서빙

### 핵심 가치
1. μs 레이턴시 (kdb+ 동등 이상)
2. Python ↔ C++ 무번역 (Research to Production)
3. SQL + HTTP API (범용 에코시스템 연결)
4. Transport 교체 가능 (RDMA → CXL)

---

## 2. 비기능 요구사항

- **N-1 초저지연**: filter 1M < 300μs, VWAP 1M < 600μs ✅ 달성
- **N-2 Zero-Copy**: NIC→스토리지→Python 무복사 ✅ 달성
- **N-3 Memory Disaggregation**: CXL/RDMA 모듈형 ✅ 설계 완료
- **N-4 스토리지 모드**: In-Memory / Tiered / On-Disk ✅ 구현
- **N-5 SIMD 가속**: AVX-512 / ARM SVE ✅ Highway 자동 디스패치
- **N-6 SQL 호환**: ClickHouse 호환 SQL + HTTP ✅ 구현
- **N-7 JOIN 지원**: ASOF, Hash, LEFT, Window JOIN ✅ 구현
- **N-8 병렬 쿼리**: 멀티코어 활용, scatter/gather ✅ 구현 (3.48x@8T)
- **N-9 kdb+ 호환**: xbar, EMA, Window JOIN, asof ✅ 93% 대체율

---

## 3. 기능 요구사항

### 3.1 스토리지 (Layer 1) ✅
- Arrow 호환 컬럼형, Arena Allocator, Partition Manager
- HDB flush (LZ4, 4.8GB/s), mmap zero-copy read

### 3.2 인제스션 (Layer 2) ✅
- MPMC Ring Buffer 65K slots, Lock-free
- UCX/RDMA 커널 바이패스, WAL 복구 로그

### 3.3 실행 엔진 (Layer 3) ✅
- BitMask filter (11x speedup vs SelectionVector)
- Highway SIMD (filter 3x, VWAP 2-3x)
- LLVM JIT O3 (동적 쿼리 컴파일)
- **JOIN 연산자**: ASOF O(n log m), Hash JOIN, LEFT JOIN (NULL 센티넬), Window JOIN O(n log m)
- **병렬 쿼리**: LocalQueryScheduler (scatter/gather), WorkerPool (jthread), 3.48x@8T
- **금융 함수**: xbar (시간 바), EMA, DELTA, RATIO, FIRST, LAST, wj_avg/sum/count/min/max

### 3.4 SQL + API (Layer 4/5) ✅
- Recursive descent SQL parser (1.5~4.5μs)
- HTTP API port 8123 (ClickHouse 호환)
- pybind11 Python 바인딩 + Lazy DSL
- zero-copy numpy (522ns)

### 3.5 분산 클러스터 (Layer 0) ✅ (단일노드 검증)
- Transport 추상화 (UCX→CXL 1줄 교체)
- Consistent Hashing (2ns routing)
- Health Monitor (heartbeat + failover)
- CXL 시뮬레이션 (200/300ns 레이턴시 검증)

---

## 4. 성능 목표 vs 달성

| 지표 | 목표 | 달성 | 상태 |
|---|---|---|---|
| 인제스션 | 5M/sec | **5.52M/sec** | ✅ |
| filter 1M | < 300μs | **272μs** | ✅ |
| VWAP 1M | < 600μs | **532μs** | ✅ |
| **xbar (시간 바)** | < 30ms | **24ms (1M→3.3K)** | ✅ |
| **EMA** | < 3ms | **2.2ms/1M** | ✅ |
| **DELTA/RATIO** | < 3ms | **<2ms/1M** | ✅ |
| **ASOF JOIN** | < 100ms | **53ms/1M** | ✅ |
| **Hash JOIN** | < 100ms | **42ms/1M** | ✅ |
| **GROUP BY 병렬 (8T)** | > 2x | **3.48x** | ✅ |
| HDB flush | > 1GB/s | **4.8GB/s** | ✅ |
| SQL parse | < 50μs | **1.5~4.5μs** | ✅ |
| Python zero-copy | < 1μs | **522ns** | ✅ |
| Transport routing | < 10ns | **2ns** | ✅ |
