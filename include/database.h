#pragma once

#ifndef LTPMG_DATABASE
#define LTPMG_DATABASE

#include "data_generator.h"
#include "row_parser.h"
#include "row.h"
#include "param.h"
#include "db_structure.h"
#include "define.h"

#ifndef LTPMG_DATABASE_CHECK
// #define LTPMG_DATABASE_CHECK
#endif


#ifndef LTPMG_DATABASE_MULTITHREAD
#define LTPMG_DATABASE_MULTITHREAD
#endif

class Database {
private:
    std::string filepath;

    Row_Parser parser;

    std::vector<Row> rows;

    Data_Generator generator;

    std::vector<std::vector<std::vector<std::any> > > tables;

    Global_Table_Info *tables_for_GPU_info;

    Global_Table *tables_for_GPU;

    Global_Table_Index *index_for_GPU;

    std::vector<UINT32> string_to_uint(const std::string &input);

public:
    Database(const std::string &filepath);

    ~Database();

    void generate(std::shared_ptr<Param> param);

    void print() const;

    void print_global_row() const;

    void test();

    void malloc_global_row(std::shared_ptr<Param> param);

    void copy_to_global_row(std::shared_ptr<Param> param);

    void free_global_row(std::shared_ptr<Param> param);

    void transfer_to_GPU(std::shared_ptr<Param> param);

    void transfer_to_CPU(std::shared_ptr<Param> param);
};

extern void transfer_database_to_GPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                     Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

extern void transfer_database_to_CPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                     Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

#endif
