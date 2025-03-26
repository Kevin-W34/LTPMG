#pragma once

#ifndef LTPMG_QUERY
#define LTPMG_QUERY

#include "define.h"
#include "param.h"
#include "txn_structure.h"
#include "txn_generator.h"

#ifndef LTPMG_GPUQUERY_PRINT
// #define LTPMG_GPUQUERY_PRINT
#endif
class Query
{
private:
    std::shared_ptr<Txn_Generator<Test_Query>> txn_generator_test_query;

    std::shared_ptr<Txn_Generator<Test_Query_2>> txn_generator_test_query_2;

    std::shared_ptr<Txn_Generator<Neworder_Query>> txn_generator_neworder_query;

    std::shared_ptr<Txn_Generator<Payment_Query>> txn_generator_payment_query;

    std::shared_ptr<Txn_Generator<Orderstatus_Query>> txn_generator_orderstatus_query;

    std::shared_ptr<Txn_Generator<Delivery_Query>> txn_generator_delivery_query;

    std::shared_ptr<Txn_Generator<Stocklevel_Query>> txn_generator_stocklevel_query;

    std::shared_ptr<Txn_Generator<YCSB_A_Query>> txn_generator_ycsb_a_query;

    std::shared_ptr<Txn_Generator<YCSB_B_Query>> txn_generator_ycsb_b_query;

    std::shared_ptr<Txn_Generator<YCSB_C_Query>> txn_generator_ycsb_c_query;

    std::shared_ptr<Txn_Generator<YCSB_D_Query>> txn_generator_ycsb_d_query;

    std::shared_ptr<Txn_Generator<YCSB_E_Query>> txn_generator_ycsb_e_query;

    std::vector<std::any> transactions_batch;

    Global_Txn_Info *global_txn_info;

    Global_Txn *global_txn;

    uint32_t global_txn_info_size;

public:
    Query();
    ~Query();
    void generate_txn(std::shared_ptr<Param> param);
    void malloc_global_txn(std::shared_ptr<Param> param);
    void free_global_txn();
    void initial_on_GPU(std::shared_ptr<Param> param);
    void transfer_to_GPU(std::shared_ptr<Param> param);
    void transfer_to_CPU(std::shared_ptr<Param> param);
    static bool sortByType(const std::any &a, const std::any &b)
    {
        return a.type().name() < b.type().name();
    }
};
extern void initial_query_on_GPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any>> transactions_batch_ptr,
                                  Global_Txn_Info *global_txn_info);

extern void transfer_query_to_GPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any>> transactions_batch_ptr,
                                  Global_Txn_Info *global_txn_info);

extern void transfer_query_to_CPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any>> transactions_batch_ptr,
                                  Global_Txn_Info *global_txn_info);

#endif