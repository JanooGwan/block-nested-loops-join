#include <algorithm>
#include <thread>
#include <mutex>
#include <fstream>
#include <cstring>
#include <iostream>
#include "record.h"

// --------------------------------------
// 블록(메모리에 올라온 customer N개)에 대해
// orders 전체와 병렬로 조인하는 함수
//   - 완전히 메모리 내부에서만 동작
// --------------------------------------
static void join_block_parallel(
    const std::vector<Customer> &blockCustomers,
    const std::vector<OrderRec> &orders,
    std::vector<JoinedRow> &globalResult
) {
    if (blockCustomers.empty() || orders.empty())
        return;

    unsigned int hw = std::thread::hardware_concurrency();
    unsigned int numThreads = (hw == 0 ? 4u : hw);

    if (blockCustomers.size() < numThreads)
        numThreads = 1;

    // 단일 스레드면 그냥 직렬로
    if (numThreads == 1) {
        for (const auto &c : blockCustomers) {
            for (const auto &o : orders) {
                if (c.c_custkey == o.o_custkey) {
                    globalResult.push_back(
                        {c.c_custkey, c.c_name, o.o_orderkey, o.o_comment});
                }
            }
        }
        return;
    }

    std::mutex mtx;
    std::vector<std::thread> threads;
    size_t chunkSize = (blockCustomers.size() + numThreads - 1) / numThreads;

    auto worker = [&](size_t start, size_t end) {
        std::vector<JoinedRow> local;
        local.reserve((end - start) * 2); // 예상 크기 예약

        // [수정됨] BNLJ의 핵심: Inner Table(orders)을 기준으로 Outer Block(blockCustomers)을 스캔
        // 즉, Orders를 한 번 훑으면서 -> 현재 메모리에 올라온 Block의 구간(start~end)과 비교
        
        for (const auto &o : orders) {
            // 현재 스레드가 담당하는 블록의 범위(start ~ end)만 순회
            for (size_t i = start; i < end; ++i) {
                const auto &c = blockCustomers[i];
                if (c.c_custkey == o.o_custkey) {
                    local.push_back(
                        {c.c_custkey, c.c_name, o.o_orderkey, o.o_comment});
                }
            }
        }

        std::lock_guard<std::mutex> lock(mtx);
        globalResult.insert(globalResult.end(), local.begin(), local.end());
    };

    for (unsigned int t = 0; t < numThreads; ++t) {
        size_t start = t * chunkSize;
        if (start >= blockCustomers.size()) break;
        size_t end = std::min(blockCustomers.size(), start + chunkSize);
        threads.emplace_back(worker, start, end);
    }

    for (auto &th : threads)
        th.join();
}

// --------------------------------------
// .dat 블록을 읽으면서 BNL 수행
//   - customer.dat : BLOCK_SIZE 바이트 단위 읽기
//   - 그 안의 레코드들을 고객 수 blockSize 개씩 모아
//     join_block_parallel() 에 넘김
// --------------------------------------
std::vector<JoinedRow> block_nested_join_disk(
    const std::string &customer_dat_path,
    const std::vector<OrderRec> &orders,
    size_t blockSize   // 논리 블록당 customer 개수
) {
    std::vector<JoinedRow> result;

    if (orders.empty())
        return result;

    if (blockSize == 0)
        blockSize = 1000; // 기본값

    std::ifstream in(customer_dat_path, std::ios::binary);
    if (!in.is_open()) {
        std::cerr << "[block_nested_join_disk] cannot open: "
                  << customer_dat_path << "\n";
        return result;
    }

    char blockBuf[BLOCK_SIZE];
    std::vector<Customer> currentBlock;
    currentBlock.reserve(blockSize);

    while (in.read(blockBuf, BLOCK_SIZE)) {
        const BlockHeader *hdr =
            reinterpret_cast<const BlockHeader*>(blockBuf);

        int offset = static_cast<int>(sizeof(BlockHeader));

        for (int r = 0; r < hdr->num_records; ++r) {
            int recLen = 0;
            std::memcpy(&recLen, blockBuf + offset, sizeof(int));
            offset += static_cast<int>(sizeof(int));

            // 원본 한 줄 복원
            std::string line(blockBuf + offset, recLen);
            offset += recLen;

            if (line.empty()) continue;

            auto parts = split_pipe(line);
            if (parts.size() < 2) continue;

            Customer c;
            c.c_custkey = std::stoi(parts[0]);
            c.c_name = parts[1];
            currentBlock.push_back(std::move(c));

            // 논리 블록이 가득 찼으면 이 블록에 대해 병렬 조인 수행
            if (currentBlock.size() == blockSize) {
                join_block_parallel(currentBlock, orders, result);
                currentBlock.clear();
            }
        }
    }

    // 마지막에 남은 고객들 처리
    if (!currentBlock.empty()) {
        join_block_parallel(currentBlock, orders, result);
    }

    return result;
}
