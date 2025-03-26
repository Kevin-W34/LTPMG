#pragma once
#ifndef LTPMG_ROW_PARSER
#define LTPMG_ROW_PARSER

#include "row.h"
#include "define.h"

class Row_Parser
{
public:
    const std::vector<Row> &parse(const std::string &filename);

private:
    std::vector<Row> tables;
};

#endif