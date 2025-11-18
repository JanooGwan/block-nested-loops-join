#include "record.h"

std::vector<JoinedRow> simple_nested_join(const std::vector<Customer>& C,
                                          const std::vector<OrderRec>& O) {
    std::vector<JoinedRow> R;
    R.reserve(C.size());

    for (const auto& c : C) {
        for (const auto& o : O) {
            if (c.c_custkey == o.o_custkey) {
                R.push_back({c.c_custkey, c.c_name, o.o_orderkey, o.o_comment});
            }
        }
    }

    return R;
}
