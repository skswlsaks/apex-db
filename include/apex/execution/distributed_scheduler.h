#pragma once
// ============================================================================
// APEX-DB: DistributedQueryScheduler — 분산 노드 스케줄러 (스텁)
// ============================================================================
// 나중에 UCX/RDMA 로 원격 노드에 QueryFragment 를 전송하고
// PartialAggResult 를 수집하는 구현체.
//
// 현재 상태: TODO 스텁만 존재.
// 구현 계획:
//   1. UCX ep_create() 로 원격 노드 연결 풀 생성
//   2. scatter(): fragments → PartialAggResult::serialize() → UCX send
//   3. 원격 노드 수신 후 execute_fragment() → serialize result
//   4. gather(): UCX recv → PartialAggResult::deserialize() → merge
// ============================================================================

#include "apex/execution/query_scheduler.h"
#include <stdexcept>
#include <string>
#include <vector>

namespace apex::execution {

class DistributedQueryScheduler : public QueryScheduler {
public:
    // TODO: 원격 노드 주소 목록 (host:port 형식)
    explicit DistributedQueryScheduler(
        std::vector<std::string> /*node_endpoints*/ = {})
    {}

    std::vector<PartialAggResult> scatter(
        const std::vector<QueryFragment>& /*fragments*/) override
    {
        // TODO: serialize fragments → UCX send to remote nodes
        // TODO: collect PartialAggResult from all nodes
        throw std::runtime_error(
            "DistributedQueryScheduler::scatter() not yet implemented. "
            "Planned: UCX/RDMA fragment dispatch to remote nodes.");
    }

    PartialAggResult gather(
        std::vector<PartialAggResult>&& /*partials*/) override
    {
        // TODO: UCX recv → deserialize → merge
        throw std::runtime_error(
            "DistributedQueryScheduler::gather() not yet implemented.");
    }

    size_t worker_count() const override {
        // TODO: return total remote worker count
        return 0;
    }

    std::string scheduler_type() const override { return "distributed"; }
};

} // namespace apex::execution
