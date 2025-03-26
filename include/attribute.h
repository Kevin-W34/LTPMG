#pragma once
#ifndef LTPMG_ATTRIBUTE
#define LTPMG_ATTRIBUTE

#include "define.h"
class Attribute
{
public:
    Attribute(const std::string &name, const std::string &type, const std::string &data_distribution)
        : name(name), type(type), data_distribution(data_distribution) {}

    const std::string &getName() const;
    const std::string &getType() const;

private:
    std::string name;              // 属性名称
    std::string type;              // 属性类型
    std::string data_distribution; // 属性的数据分布
};
#endif