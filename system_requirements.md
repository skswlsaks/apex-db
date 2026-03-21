# 차세대 HFT 인메모리 DB 시스템 요구사항 정의서 (PRD & SRS)

본 문서는 금융권 및 고빈도 매매(HFT) 시장을 타겟으로 하는 **초저지연 인메모리 데이터베이스**의 전체 기획 및 시스템 요구사항을 체계적으로 정리한 문서입니다. `initial_doc.md`와 이후 논의된 고수준 아키텍처 설계를 기반으로 작성되었습니다.

---

## 1. 비즈니스 및 목표 시장 (Business & Target Market)

- **핵심 타겟:** 고빈도 매매(HFT) 헤지펀드, 투자은행(IB), 가상자산 거래소 인프라, 실시간 이상거래탐지(FDS) 시스템 파트.
- **제품의 핵심 가치 (Core Value Proposition):**
  1. 기존 DB(kdb+) 시스템의 물리적/소프트웨어적 지연 한계(Tail Latency) 돌파.
  2. 퀀트 리서치(Python)와 실거래 운용(C++) 사이의 번역(Translation) 빙하기를 없애 **Time-To-Market 극단적 단축**.
  3. 클라우드 네이티브(CXL/RDMA) 하드웨어에 최적화된 무한한 메모리 확장성.

---

## 2. 비기능적 요구사항 (Non-Functional Requirements)

시스템 아키텍처가 반드시 지켜야 하는 성능 및 기술적 제약사항입니다.

- **N-1. 초저지연 성능 (Ultra-Low Latency):** 커널 스케줄러 간섭이나 페이지 폴트(Page Fault)를 원천 차단하여 마이크로초(μs) 단위의 틱 데이터 수집 및 질의 속도 보장.
- **N-2. 제로 카피 (Zero-Copy):** 네트워크 패킷 수신부터 데이터 저장, 그리고 클라이언트(Python 등) 반환에 이르기까지 메모리 단일 바이트 복사도 발생하지 않아야 함.
- **N-3. 동적 메모리 분리 (Memory Disaggregation):** 로컬 노드의 물리적 램(RAM) 크기에 종속되지 않고, CXL 3.0 및 RDMA(AWS EFA)를 통해 클러스터 전체의 메모리를 하나의 글로벌 공유 풀(Global Shared Pool)로 운용.
- **N-4. 스토리지 모드 유연성:** 
  - `Pure In-Memory`: HFT를 위한 극단적 틱 처리 전용 모드.
  - `Tiered Storage`: 당일 분은 CXL 메모리(RDB), 과거 분은 NVMe SSD(HDB)로 비동기 병합.
  - `Pure On-Disk`: HDB 전용 모드로 백테스트 및 딥러닝 피처(Feature) 생성 전용.
- **N-5. 하드웨어 가속 강제화 (Hardware Acceleration):** CPU 연산 시 SIMD(AVX-512, ARM SVE) 명령어 100% 활용, 무거운 옵션 계산/FDS 모델은 FPGA나 GPU 머신으로 CXL 오프로딩(Offloading).

---

## 3. 기능적/아키텍처 요구사항 (Functional Architecture Requirements)

### 3.1. 스토리지 엔진 (Storage Engine)
- Apache Arrow와 호환되는 캐시 지향적 순수 컬럼형(Columnar) 데이터 레이아웃 유지.
- ClickHouse의 MergeTree 메커니즘을 차용하여, CXL 메모리의 Hot Data를 비동기로 압축해 객체 스토리지(S3)나 NVMe(SSD)로 내리는 백그라운드 머지(Merge) 스레드 기능.

### 3.2. 수집 및 네트워크 레이어 (Ingestion Layer)
- SRD/RDMA(RoCE v2) 기반 커널 우회 다이렉트 메모리 쓰기(RDMA Write) 기능 탑재.
- C++20 기반의 Lock-Free 다중 생산자-다중 소비자(MPMC) Ring Buffer 기반 메시지 라우팅 시스템 (Tick Plant).

### 3.3. 실행 및 쿼리 엔진 (Execution Engine)
- JIT(Just-In-Time) 컴파일러(LLVM 등)를 내장하여 즉석에서 필터/집계 조건을 C++ 기계어로 최적화 및 핫스팟 컴파일 수행.
- Vectorized 파이프라인 형태의 데이터 블록 스트리밍 런타임 환경.
- 시계열(Time-Series, Append-Only) 연산과 그래프(Graph, CSR 방식) 인덱스 교차 탐색을 동시에 수행할 수 있는 파서 및 플래너.

### 3.4. Research-to-Production 통합 인터페이스 (Transpiler)
- **Python DSL:** 퀀트는 Pandas/Polars 와 완전히 유사한 API 구문을 사용하여 로직 작성.
- **AST 컴파일 변환:** Python 코드는 실행(Interpreting)되지 않고 추상 구문 트리(AST) 형태의 쿼리로 C++ 코어에 전송되며, 엔진에서 네이티브 벡터 연산 실행계획으로 번역.
- **C++/Python Direct Binding:** Pybind11 (또는 nanobind) 기반으로, Python 프로세스가 C++ 할당 메모리를 직렬화 비용(디코딩) 없이 그대로 들여다보는 Zero-copy 뷰(View) 인터페이스.
