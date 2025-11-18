#include <iostream>
#include <chrono>
#include <vector>
#include "record.h"

int main(int argc, char** argv) {
    std::string dataDir = "data";
    size_t capCustomers = (size_t)-1;
    size_t capOrders = (size_t)-1;
    size_t blockSize = 1000;
    bool useBlock = true;

    if (argc >= 2) dataDir = argv[1];         // 데이터 폴더
    if (argc >= 3) blockSize = std::stoul(argv[2]);
    if (argc >= 4) capCustomers = std::stoul(argv[3]);
    if (argc >= 5) capOrders = std::stoul(argv[4]);
    if (argc >= 6) useBlock = (std::string(argv[5]) != "simple");

    std::string cpath = dataDir + "/customer.tbl";
    std::string opath = dataDir + "/orders.tbl";

    auto t0 = std::chrono::high_resolution_clock::now();
    auto customers = load_customers(cpath, capCustomers);
    auto orders = load_orders(opath, capOrders);
    auto t1 = std::chrono::high_resolution_clock::now();

    std::cout << "Loaded C=" << customers.size() << ", O=" << orders.size() << "\n";

    std::vector<JoinedRow> joined;
    auto t2start = std::chrono::high_resolution_clock::now();
    if (useBlock) joined = block_nested_join(customers, orders, blockSize);
    else joined = simple_nested_join(customers, orders);
    auto t2end = std::chrono::high_resolution_clock::now();

    save_joined("output/result.tbl", joined);

    auto load_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t1 - t0).count();
    auto join_ms = std::chrono::duration_cast<std::chrono::milliseconds>(t2end - t2start).count();

    std::cout << "Join result rows: " << joined.size() << "\n";
    std::cout << "Load time: " << load_ms << " ms\n";
    std::cout << "Join time: " << join_ms << " ms\n";
    std::cout << (useBlock ? "Mode: BLOCK" : "Mode: SIMPLE") << ", blockSize=" << blockSize << "\n";

    return 0;
}
