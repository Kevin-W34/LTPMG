#include "row.h"

void Row::addAttribute(const Attribute &attribute)
{
    attributes.push_back(attribute);
}

void Row::set_table_name(const std::string &table_name)
{
    this->table_name = table_name;
}

void Row::set_table_size(const int &table_size)
{
    this->table_size = table_size;
}

const std::vector<Attribute> &Row::getAttributes() const
{
    return attributes;
}

const std::string &Row::get_table_name() const
{
    return table_name;
}

const int &Row::get_table_size() const
{
    return table_size;
}

void Row::clear()
{
    attributes.clear();
}