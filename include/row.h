#pragma once
#ifndef LTPMG_ROW
#define LTPMG_ROW

#include "attribute.h"


class Row
{
public:
    void addAttribute(const Attribute &attribute);
    void set_table_name(const std::string &table_name);
    void set_table_size(const int &table_size);

    const std::vector<Attribute> &getAttributes() const;
    const std::string &get_table_name() const;
    const int &get_table_size() const;
    void clear();

private:
    std::vector<Attribute> attributes; // 属性列表
    std::string table_name;
    int table_size;
};

#endif