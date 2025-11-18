#pragma once
#include <string>
#include <vector>

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

std::vector<std::string> split_pipe(const std::string &s);

std::vector<Customer> load_customers(const std::string &filename, size_t max_rows = (size_t)-1);
std::vector<OrderRec> load_orders(const std::string &filename, size_t max_rows = (size_t)-1);

void save_joined(const std::string &filename, const std::vector<JoinedRow> &rows);

std::vector<JoinedRow> simple_nested_join(const std::vector<Customer> &C,
                                          const std::vector<OrderRec> &O);

std::vector<JoinedRow> block_nested_join(const std::vector<Customer> &C,
                                         const std::vector<OrderRec> &O,
                                         size_t blockSize);
