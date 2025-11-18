#include <algorithm>
#include <thread>
#include <mutex>
#include "record.h"

std::vector<JoinedRow> block_nested_join(const std::vector<Customer>& C,
                                         const std::vector<OrderRec>& O,
                                         size_t blockSize) {
    std::vector<JoinedRow> result;
    if (blockSize == 0 || C.empty() || O.empty())
        return result;

    // 하드웨어 스레드 개수 확인
    unsigned int hw = std::thread::hardware_concurrency();
    unsigned int numThreads = (hw == 0 ? 4u : hw);

    // C 사이즈와 blockSize를 고려해 유효한 스레드 수 제한
    if (C.size() < blockSize)
        numThreads = 1;
    else {
        size_t maxUseful = C.size() / blockSize;
        if (maxUseful == 0) maxUseful = 1;
        if (numThreads > maxUseful) numThreads = static_cast<unsigned int>(maxUseful);
    }

    // 스레드 1개면 그냥 단일 스레드 버전으로 동작
    if (numThreads == 1) {
        for (size_t i = 0; i < C.size(); i += blockSize) {
            size_t end = std::min(i + blockSize, C.size());
            for (size_t j = i; j < end; ++j) {
                const auto& c = C[j];
                for (const auto& o : O) {
                    if (c.c_custkey == o.o_custkey) {
                        result.push_back({c.c_custkey, c.c_name,
                                          o.o_orderkey, o.o_comment});
                    }
                }
            }
        }
        return result;
    }

    std::mutex mtx;
    std::vector<std::thread> threads;
    size_t chunkSize = (C.size() + numThreads - 1) / numThreads;

    auto worker = [&](size_t start, size_t end) {
        std::vector<JoinedRow> local;
        for (size_t i = start; i < end; i += blockSize) {
            size_t blockEnd = std::min(i + blockSize, end);
            for (size_t j = i; j < blockEnd; ++j) {
                const auto& c = C[j];
                for (const auto& o : O) {
                    if (c.c_custkey == o.o_custkey) {
                        local.push_back({c.c_custkey, c.c_name,
                                         o.o_orderkey, o.o_comment});
                    }
                }
            }
        }

        std::lock_guard<std::mutex> lock(mtx);
        result.insert(result.end(), local.begin(), local.end());
    };

    for (unsigned int t = 0; t < numThreads; ++t) {
        size_t start = t * chunkSize;
        if (start >= C.size()) break;
        size_t end = std::min(C.size(), start + chunkSize);
        threads.emplace_back(worker, start, end);
    }

    for (auto& th : threads) {
        th.join();
    }

    return result;
}
