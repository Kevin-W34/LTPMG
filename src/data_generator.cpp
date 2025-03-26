#include "../include/data_generator.h"
#include "../include/param.h"

const std::vector<std::any> &Data_Generator::generateData(const Row &row) {
    rowData.clear();
    for (const auto &attribute: row.getAttributes()) {
        if (attribute.getType() == "int") {
            rowData.push_back(generateRandomInt());
        } else if (attribute.getType() == "double") {
            rowData.push_back(generateRandomDouble());
        } else if (attribute.getType() == "string") {
            rowData.push_back(generateRandomString());
        }
    }

    return rowData;
}
