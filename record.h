#pragma once
#include <string>
#include <vector>

// -----------------------------
// 공통 레코드 구조체
// -----------------------------
struct Customer
{
    int c_custkey{};
    std::string c_name;
};

struct OrderRec
{
    int o_orderkey{};
    int o_custkey{};
    std::string o_comment;
};

struct JoinedRow
{
    int c_custkey{};
    std::string c_name;
    int o_orderkey{};
    std::string o_comment;
};

// -----------------------------
// .tbl 파싱용 함수
// -----------------------------
std::vector<std::string> split_pipe(const std::string &s);

std::vector<Customer> load_customers(const std::string &filename,
                                     size_t max_rows = (size_t)-1);

std::vector<OrderRec> load_orders(const std::string &filename,
                                  size_t max_rows = (size_t)-1);

void save_joined(const std::string &filename,
                 const std::vector<JoinedRow> &rows);

// -----------------------------
// 메모리 기반 단순 Join (그대로 유지)
// -----------------------------
std::vector<JoinedRow> simple_nested_join(const std::vector<Customer> &C,
                                          const std::vector<OrderRec> &O);

// -----------------------------
// 디스크 블록 포맷 (tbl_to_dat와 공유)
// -----------------------------
constexpr size_t BLOCK_SIZE = 8192;

struct BlockHeader {
    int num_records;
    int free_offset;
};

// -----------------------------
// 디스크 기반 Block Nested Loop Join
//   - outer: customer.dat (BLOCK_SIZE 바이트 고정 블록)
//   - inner: orders (메모리에 전체 로딩)
//   - blockSize: 논리 블록(고객 수) 크기
// -----------------------------
std::vector<JoinedRow> block_nested_join_disk(
    const std::string &customer_dat_path,
    const std::vector<OrderRec> &orders,
    size_t blockSize   // 논리 블록당 customer 개수
);

std::vector<JoinedRow> block_nested_join_disk_old(
    const std::string &customer_dat_path,
    const std::vector<OrderRec> &orders,
    size_t blockSize);
