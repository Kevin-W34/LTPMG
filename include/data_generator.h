#pragma once
#ifndef LTPMG_DATAGENERATOR
#define LTPMG_DATAGENERATOR

#include "define.h"
#include "row.h"
#include "random.h"

class Data_Generator {
public:
    const std::vector<std::any> &generateData(const Row &rowObj);

    int generateRandomInt() { return rand() % 100; } // 生成0到99之间的随机整数
    double generateRandomDouble() { return static_cast<double>(rand()) / RAND_MAX; } // 生成0到1之间的随机浮点数
    std::string generateRandomString() { return "random_string"; } // 可以生成更复杂的随机字符串
    int generateRandomIntRange(int a, int b) { return rand() % (b - a + 1) + a; }
private:
    std::vector<std::any> rowData;
};
#endif
