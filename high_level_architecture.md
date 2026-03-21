# [High-Level DB Software Architecture Design]

This document outlines the macro-level software architecture of our Ultra-Low Latency In-Memory DB, heavily inspired by **KDB+ (Tick Plant & Array Locality)** and **ClickHouse (Vectorized Execution & MergeTree)**, optimized for modern CXL/RDMA hardware.

## User Review Required

Please review this overarching structure. Once approved, we will perform a deep dive into each component step-by-step as outlined in our task list.

## 🏗 System Architecture Components

The architecture is divided into 4 primary layers.

---

### [1. Ingestion Layer: RDMA Tick Plant]
Inspired by KDB+'s Tickerplant, but redesigned for Kernel Bypass and Zero-Copy.

#### Responsibilities
- Receives high-speed market data (FIX, Binary Pcap).
- Acts as a Publisher/Subscriber router.
- Writes to the write-ahead log (WAL) and memory simultaneously.

#### Key Mechanics
- **Lock-Free MPMC Ring Buffer:** Built with C++20 atomics.
- **Zero-Copy RDMA:** NIC writes directly into the CXL Memory Pool. No OS network stack overhead.

### [2. Storage Layer: Disaggregated Memory MergeTree (DMMT)]
Combines KDB+'s RDB/HDB concept with ClickHouse's background merge capabilities.

#### RDB (Real-time DB) - Hot Data
- Purely in-memory, residing in the **Global Shared Memory Pool** (CXL/EFA).
- Data is stored in Apache Arrow-compatible **Column Vectors**.
- Optimized for instant append and extreme cache locality.

#### HDB (Historical DB) - Warm/Cold Data
- Similar to ClickHouse `MergeTree`.
- Background threads asynchronously flush compacted data chunks from the RDB to NVMe SSDs or Cloud Object Storage (S3).
- Ensures the RDB never runs out of capacity without pausing ingestion.

### [3. Query & Execution Engine]
Replaces KDB+'s `q` interpreter with ClickHouse's Vectorized Engine approach, augmented by JIT.

#### Vectorized Execution Pipeline
- Processes data in `DataBlocks` (e.g., arrays of 8192 rows across multiple columns).
- Fully leverages SIMD (AVX-512, ARM SVE) to execute filters and aggregations.
- Keeps L1/L2 caches hot, eliminating pointer chasing.

#### LLVM JIT Compiler
- For complex predicates (`WHERE price > 100 AND volume * vwap > 1000`), the engine dynamically compiles an optimized machine-code function at runtime instead of interpreting the query tree.

### [4. Client / Transpilation Layer]
The "Research to Production" bridge.

#### AST Transpiler
- Translates Python (Pandas/Polars) logic into a C++ Execution DAG.
- Passes the DAG directly into the Query Engine.
- Uses `pybind11` for **Zero-Copy Data Retrieval** from the RDB memory pool directly to the Python runtime.

## Verification Plan
We will step sequentially through the 4 deep dives listed in `task.md`.
For each deep dive, we will design the C++ class structures, memory layouts, and API boundaries. 
User feedback will dictate the direction of each deep dive.
