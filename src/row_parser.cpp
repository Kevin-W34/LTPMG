#include "../include/row_parser.h"

const std::vector<Row> &Row_Parser::parse(const std::string &filename) {
    std::ifstream file(filename);
    json j;
    file >> j; // 将文件内容解析为 JSON 对象
    Row row;
    uint32_t cur = 0;

    for (const auto &table: j["tables"]) {
        row.clear();
        std::string tablename = table["name"];
        int size = table["size"];
        row.set_table_name(tablename);
        row.set_table_size(size);
        for (const auto &attributes: table["attributes"]) {
            std::string name = attributes["name"];
            std::string type = attributes["type"];
            std::string data_distribution = attributes["data_distribution"];
            row.addAttribute(Attribute(name, type, data_distribution));
            LOG(INFO) << "NO." << cur << "," << row.get_table_name() << "," << name << "," << type;
        }
        ++cur;
        tables.push_back(row);
    }

    return tables;
}
