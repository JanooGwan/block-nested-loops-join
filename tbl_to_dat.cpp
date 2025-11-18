#include <iostream>
#include <fstream>
#include <string>
#include <cstring>

constexpr size_t BLOCK_SIZE = 4096;

struct BlockHeader {
    int num_records;
    int free_offset;
};

void init_block(char* block) {
    BlockHeader* hdr = reinterpret_cast<BlockHeader*>(block);
    hdr->num_records = 0;
    hdr->free_offset = sizeof(BlockHeader);
}

void write_record_to_block(char* block,
                           const std::string& record,
                           std::ofstream& out) {
    BlockHeader* hdr = reinterpret_cast<BlockHeader*>(block);
    int record_len = static_cast<int>(record.size());
    int need = static_cast<int>(sizeof(int) + record_len);

    if (hdr->free_offset + need > static_cast<int>(BLOCK_SIZE)) {
        out.write(block, BLOCK_SIZE);
        init_block(block);
        hdr = reinterpret_cast<BlockHeader*>(block);
    }

    char* p = block + hdr->free_offset;
    std::memcpy(p, &record_len, sizeof(int));
    std::memcpy(p + sizeof(int), record.data(), record_len);

    hdr->free_offset += need;
    hdr->num_records += 1;
}

bool convert_tbl_to_dat(const std::string& tbl_path,
                        const std::string& dat_path) {
    std::ifstream in(tbl_path);
    if (!in.is_open()) {
        std::cerr << "Error: cannot open " << tbl_path << "\n";
        return false;
    }

    std::ofstream out(dat_path, std::ios::binary);
    if (!out.is_open()) {
        std::cerr << "Error: cannot create " << dat_path << "\n";
        return false;
    }

    char block[BLOCK_SIZE];
    init_block(block);

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty()) continue;
        write_record_to_block(block, line, out);
    }

    out.write(block, BLOCK_SIZE);

    return true;
}

int main(int argc, char** argv) {
    if (argc < 3) {
        std::cerr << "Usage: tbl_to_dat <input.tbl> <output.dat>\n";
        return 1;
    }

    std::string input = argv[1];
    std::string output = argv[2];

    if (convert_tbl_to_dat(input, output)) {
        std::cout << "Converted: " << input << " -> " << output << "\n";
        return 0;
    } else {
        return 1;
    }
}
