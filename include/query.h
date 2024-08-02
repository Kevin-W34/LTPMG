#pragma once

#include "dependency.h"
#include "random.h"
#include "common.h"

struct NeworderQuery
{
    uint32_t txn_ID;
    uint32_t W_ID;
    uint32_t D_ID;
    uint32_t C_ID;
    uint32_t O_ID;
    uint32_t N_O_ID;
    uint32_t O_OL_CNT;
    uint32_t O_OL_ID;
    struct NewOrderQueryInfo
    {
        uint32_t OL_I_ID;
        uint32_t OL_SUPPLY_W_ID;
        uint32_t OL_QUANTITY;
    };
    NewOrderQueryInfo INFO[15];
    uint32_t current_op = 0;
};

struct PaymentQuery
{
    uint32_t txn_ID;
    uint32_t W_ID;
    uint32_t D_ID;
    uint32_t C_ID;
    uint32_t C_LAST;
    uint32_t isName; // 0,id; 1,name
    uint32_t C_D_ID;
    uint32_t C_W_ID;
    uint32_t H_AMOUNT;
    uint32_t H_ID;
    uint32_t current_op = 0;
};

class Query
{
private:
    NeworderQuery *neworderquery;
    PaymentQuery *paymentquery;
    uint32_t warehouse_tbl_size;
    uint32_t district_tbl_size;
    uint32_t customer_tbl_size;
    uint32_t history_tbl_size;
    uint32_t neworder_tbl_size;
    uint32_t order_tbl_size;
    uint32_t orderline_tbl_size;
    uint32_t stock_tbl_size;
    uint32_t item_tbl_size;

    uint32_t epoch_tp;
    uint32_t epoch_sync;
    uint32_t batch_size;
    uint32_t neworder_percent;
    uint32_t neworderquery_size;
    uint32_t paymentquery_size;
    uint32_t neworderquery_slice_size;
    uint32_t paymentquery_slice_size;
    uint32_t gen_neworderquery_size;
    uint32_t gen_paymentquery_size;

    uint32_t device_cnt;
    uint32_t *device_IDs;
    std::string device_IDs_str;

    Random *random;

    std::atomic_uint32_t transaction_ID;  //(0);
    std::atomic_uint32_t neworder_tbl_ID;  //(0);
    std::atomic_uint32_t order_tbl_ID;     //(0);
    std::atomic_uint32_t orderline_tbl_ID; //(0)
    std::atomic_uint32_t history_tbl_ID;

public:
    Query(std::shared_ptr<Param> param);
    ~Query();
    void make_neworder(uint32_t thID, uint32_t core_cnt, uint32_t epoch_ID);
    void make_payment(uint32_t thID, uint32_t core_cnt, uint32_t epoch_ID);
    void make_Query();
    void print_neworder();
    void print_payment();
    NeworderQuery *get_neworder_query();
    PaymentQuery *get_payment_query();
    uint32_t get_batch_size();
    uint32_t get_neworder_percent();
    uint32_t get_epoch_tp();
    uint32_t get_epoch_sync();
    uint32_t get_warehouse_size();
    uint32_t *get_device_IDs();
    uint32_t get_device_cnt();
    void analyse_device_IDs();
};

extern void copy_query_to_gpu(NeworderQuery *neworderquery, PaymentQuery *paymentquery);

extern void copy_query_to_cpu();

extern void launchQueryKernel(Result &result);

extern void initialize_gpuquery(unsigned int warehouse_tbl_size, unsigned int device_cnt,
                                unsigned int *device_IDs, uint32_t batch_size,
                                uint32_t neworder_percent, uint32_t epoch_tp,
                                uint32_t epoch_sync);

extern void release_gpuquery();
