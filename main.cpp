#include <iostream>
#include <chrono>
#include <vector>
#include <string>
#include "record.h"

int main(int argc, char** argv) {
    // -------------------------------------------------------
    // 기본값 설정
    // -------------------------------------------------------
    std::string dataDir = "data";
    size_t capCustomers = (size_t)-1;
    size_t capOrders = (size_t)-1;
    size_t blockSize = 1000;

    // 모드 관리 변수
    bool useBlock = true;   // true면 block 기반 join
    bool useOld   = false;  // true면 old(single-thread) 버전
    // simple / block / old  ← mode

    // -------------------------------------------------------
    // 인자 파싱
    // -------------------------------------------------------
    if (argc >= 2) dataDir = argv[1];
    if (argc >= 3) blockSize = std::stoul(argv[2]);
    if (argc >= 4) capCustomers = std::stoul(argv[3]);
    if (argc >= 5) capOrders = std::stoul(argv[4]);

    // mode 인자 (simple / block / old)
    std::string mode = "block"; // 기본 block-parallel
    if (argc >= 6) mode = argv[5];

    if (mode == "simple") {
        useBlock = false;
        useOld   = false;
    } else if (mode == "old") {
        useBlock = true;
        useOld   = true;
    } else {
        // 기본 block: parallel
        useBlock = true;
        useOld   = false;
    }

    // -------------------------------------------------------
    // 파일 경로 설정
    // -------------------------------------------------------
    std::string ctbl  = dataDir + "/customer.tbl";
    std::string otbl  = dataDir + "/orders.tbl";
    std::string cdat  = dataDir + "/customer.dat";

    // -------------------------------------------------------
    // 1) orders 로드 (inner table) — 항상 메모리로
    // -------------------------------------------------------
    auto t0 = std::chrono::high_resolution_clock::now();
    auto orders = load_orders(otbl, capOrders);

    // 2) simple 모드면 customer 전체 로딩
    std::vector<Customer> customers;
    if (!useBlock) {
        customers = load_customers(ctbl, capCustomers);
    }

    auto t1 = std::chrono::high_resolution_clock::now();

    std::cout << "Loaded (for join) O=" << orders.size();
    if (!useBlock)
        std::cout << ", C=" << customers.size();
    std::cout << "\n";

    // -------------------------------------------------------
    // Join 수행
    // -------------------------------------------------------
    std::vector<JoinedRow> joined;
    auto t2start = std::chrono::high_resolution_clock::now();

    if (useBlock) {
        if (useOld) {
            // old 버전 = 단일 스레드 block nested join
            joined = block_nested_join_disk_old(cdat, orders, blockSize);
        } else {
            // parallel 버전
            joined = block_nested_join_disk(cdat, orders, blockSize);
        }
    } else {
        // simple nested loop join
        joined = simple_nested_join(customers, orders);
    }

    auto t2end = std::chrono::high_resolution_clock::now();

    // -------------------------------------------------------
    // 결과 저장
    // -------------------------------------------------------
    save_joined("output/result.tbl", joined);

    // -------------------------------------------------------
    // 타이밍 출력
    // -------------------------------------------------------
    long load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    long join_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2end - t2start).count();

    std::cout << "Join result rows: " << joined.size() << "\n";
    std::cout << "Load time: " << load_ms << " ms\n";
    std::cout << "Join time: " << join_ms << " ms\n";

    if (!useBlock)
        std::cout << "Mode: SIMPLE\n";
    else if (useOld)
        std::cout << "Mode: BLOCK-DISK-OLD (single-thread)\n";
    else
        std::cout << "Mode: BLOCK-DISK (parallel)\n";

    std::cout << "blockSize=" << blockSize << "\n";

    return 0;
}
