#include <algorithm>
#include <fstream>
#include <cstring>
#include <iostream>
#include "record.h"

// --------------------------------------
// 순수 Block Nested Loops Join (단일 스레드)
//  - blockCustomers: 메모리에 올라온 customer N개
//  - orders: 메모리에 올라온 orders 전체
//  - result: join 결과를 계속 뒤에 push_back
// --------------------------------------
static void join_block_serial(
    const std::vector<Customer> &blockCustomers,
    const std::vector<OrderRec> &orders,
    std::vector<JoinedRow> &result
) {
    if (blockCustomers.empty() || orders.empty())
        return;

    // 전통적인 Nested Loop Join
    // outer: blockCustomers
    // inner: orders
    for (const auto &c : blockCustomers) {
        for (const auto &o : orders) {
            if (c.c_custkey == o.o_custkey) {
                result.push_back(
                    {c.c_custkey, c.c_name, o.o_orderkey, o.o_comment});
            }
        }
    }
}

// --------------------------------------
// .dat 블록을 읽으면서 Block Nested Loops Join 수행 (단일 스레드)
//   - customer.dat : BLOCK_SIZE 바이트 단위 읽기
//   - 그 안의 레코드들을 고객 수 blockSize 개씩 모아
//     join_block_serial() 에 넘김
// --------------------------------------
std::vector<JoinedRow> block_nested_join_disk_old(
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

            // 논리 블록이 가득 찼으면 이 블록에 대해 조인 수행
            if (currentBlock.size() == blockSize) {
                join_block_serial(currentBlock, orders, result);
                currentBlock.clear();
            }
        }
    }

    // 마지막에 남은 고객들 처리
    if (!currentBlock.empty()) {
        join_block_serial(currentBlock, orders, result);
    }

    return result;
}
