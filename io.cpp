#include <fstream>
#include <sstream>
#include <iostream>
#include "record.h"

std::vector<std::string> split_pipe(const std::string& s) {
    std::vector<std::string> out;
    std::string token;
    std::stringstream ss(s);
    while (std::getline(ss, token, '|'))
        out.push_back(token);
    return out;
}

std::vector<Customer> load_customers(const std::string& filename, size_t max_rows) {
    std::vector<Customer> rows;
    std::ifstream in(filename);
    if (!in.is_open()) {
        std::cerr << "[load_customers] cannot open: " << filename << "\n";
        return rows;
    }
    std::string line;
    size_t cnt = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto parts = split_pipe(line);
        if (parts.size() >= 2) {
            Customer c;
            c.c_custkey = std::stoi(parts[0]);
            c.c_name = parts[1];
            rows.push_back(c);

            if (++cnt == max_rows) break;
        }
    }
    return rows;
}

std::vector<OrderRec> load_orders(const std::string& filename, size_t max_rows) {
    std::vector<OrderRec> rows;
    std::ifstream in(filename);
    if (!in.is_open()) {
        std::cerr << "[load_orders] cannot open: " << filename << "\n";
        return rows;
    }
    std::string line;
    size_t cnt = 0;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        auto parts = split_pipe(line);
        if (parts.size() >= 2) {
            OrderRec o;
            o.o_orderkey = std::stoi(parts[0]);
            o.o_custkey = std::stoi(parts[1]);
            if (parts.size() >= 9)
                o.o_comment = parts[8];
            rows.push_back(o);

            if (++cnt == max_rows) break;
        }
    }
    return rows;
}

void save_joined(const std::string& filename, const std::vector<JoinedRow>& rows) {
    std::ofstream out(filename);
    if (!out.is_open()) {
        std::cerr << "[save_joined] cannot open: " << filename << "\n";
        return;
    }

    for (const auto& r : rows) {
        out << r.c_custkey << '|'
            << r.c_name << '|'
            << r.o_orderkey << '|'
            << r.o_comment << "\n";
    }
}
