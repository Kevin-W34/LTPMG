#pragma once

#include "common.cuh"

struct ParamQuery
{
    uint32_t device_cnt;
    uint32_t device_ID;
    uint32_t epoch_sync;
    uint32_t neworderquery_slice_size; // how many neworder transactions on the current device
    uint32_t paymentquery_slice_size;
    uint32_t query_slice_size;

    uint32_t warehouse_tbl_size; // how many rows in warehouse table
    uint32_t district_tbl_size;
    uint32_t customer_tbl_size;
    uint32_t history_tbl_size;
    uint32_t neworder_tbl_size;
    uint32_t order_tbl_size;
    uint32_t orderline_tbl_size;
    uint32_t stock_tbl_size;
    uint32_t item_tbl_size;

    uint32_t buffer_size;
    // uint32_t used_heap_size = 0;
};

struct NeworderQueryResult
{
    uint32_t W_ID;
    uint32_t W_TAX;
    uint32_t D_ID;
    uint32_t D_TAX;
    uint32_t C_ID;
    uint32_t C_CREDIT;
    uint32_t FIANL_PRICE;
};

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

struct PaymentQueryResult
{
    uint32_t W_ID;
    uint32_t D_ID;
    uint32_t C_ID;
    uint32_t FIANL_PAYMENT;
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

struct Willdo // for database rows
{
    uint32_t device_ID;
    uint32_t epoch_ID;
    uint32_t row_ID;
    uint32_t txn_ID;
    uint32_t data = 0U;
    uint32_t same_row_pre_txn_ID = 0xffffffff;
    uint32_t same_row_next_txn_ID = 0xffffffff;
    unsigned char txnType;
    unsigned char tblType;
    bool isused = false;
    bool isexecuted = false;
    // unsigned char data_uchar[128];
};

struct serial_willdo
{
    uint32_t row_ID;
    uint32_t txn_ID[6];
    uint32_t next_serial_willdo_offset;
};

struct WilldoTable
{
    uint32_t txn_ID = 0;
    Willdo *warehouse_log = nullptr;
    Willdo *district_log = nullptr;
    Willdo *customer_log = nullptr;
    Willdo *history_log = nullptr;
    Willdo *neworder_log = nullptr;
    Willdo *order_log = nullptr;
    Willdo *orderline_log = nullptr;
    Willdo *stock_log = nullptr;
    Willdo *item_log = nullptr;

    uint32_t warehouse_log_size = 0;
    uint32_t district_log_size = 0;
    uint32_t customer_log_size = 0;
    uint32_t history_log_size = 0;
    uint32_t neworder_log_size = 0;
    uint32_t order_log_size = 0;
    uint32_t orderline_log_size = 0;
    uint32_t stock_log_size = 0;
    uint32_t item_log_size = 0;

    uint32_t warehouse_cur = 0;
    uint32_t district_cur = 0;
    uint32_t customer_cur = 0;
    uint32_t history_cur = 0;
    uint32_t neworder_cur = 0;
    uint32_t order_cur = 0;
    uint32_t orderline_cur = 0;
    uint32_t stock_cur = 0;
    uint32_t item_cur = 0;
    uint32_t cur_sendbuffer_offset = 0;

    uint32_t *warehouse_bitmap;
    uint32_t *district_bitmap;
    uint32_t *customer_bitmap;
    uint32_t *history_bitmap;
    uint32_t *neworder_bitmap;
    uint32_t *order_bitmap;
    uint32_t *orderline_bitmap;
    uint32_t *stock_bitmap;
    uint32_t *item_bitmap;

    uint32_t warehouse_bitmap_size;
    uint32_t district_bitmap_size;
    uint32_t customer_bitmap_size;
    uint32_t history_bitmap_size;
    uint32_t neworder_bitmap_size;
    uint32_t order_bitmap_size;
    uint32_t orderline_bitmap_size;
    uint32_t stock_bitmap_size;
    uint32_t item_bitmap_size;

    uint32_t warehouse_access_control_offset;
    uint32_t district_access_control_offset;
    uint32_t customer_access_control_offset;
    uint32_t history_access_control_offset;
    uint32_t neworder_access_control_offset;
    uint32_t order_access_control_offset;
    uint32_t orderline_access_control_offset;
    uint32_t stock_access_control_offset;
    uint32_t item_access_control_offset;

    uint32_t warehouse_access_control_txn_and_row_size;
    uint32_t district_access_control_txn_and_row_size;
    uint32_t customer_access_control_txn_and_row_size;
    uint32_t history_access_control_txn_and_row_size;
    uint32_t neworder_access_control_txn_and_row_size;
    uint32_t order_access_control_txn_and_row_size;
    uint32_t orderline_access_control_txn_and_row_size;
    uint32_t stock_access_control_txn_and_row_size;
    uint32_t item_access_control_txn_and_row_size;

    uint32_t *warehouse_access_control_txn_ID;
    uint32_t *district_access_control_txn_ID;
    uint32_t *customer_access_control_txn_ID;
    uint32_t *history_access_control_txn_ID;
    uint32_t *neworder_access_control_txn_ID;
    uint32_t *order_access_control_txn_ID;
    uint32_t *orderline_access_control_txn_ID;
    uint32_t *stock_access_control_txn_ID;
    uint32_t *item_access_control_txn_ID;

    uint32_t *warehouse_access_control_row_ID;
    uint32_t *district_access_control_row_ID;
    uint32_t *customer_access_control_row_ID;
    uint32_t *history_access_control_row_ID;
    uint32_t *neworder_access_control_row_ID;
    uint32_t *order_access_control_row_ID;
    uint32_t *orderline_access_control_row_ID;
    uint32_t *stock_access_control_row_ID;
    uint32_t *item_access_control_row_ID;

    uint32_t warehouse_access_control_size;
    uint32_t district_access_control_size;
    uint32_t customer_access_control_size;
    uint32_t history_access_control_size;
    uint32_t neworder_access_control_size;
    uint32_t order_access_control_size;
    uint32_t orderline_access_control_size;
    uint32_t stock_access_control_size;
    uint32_t item_access_control_size;

    uint32_t *warehouse_access_control;
    uint32_t *district_access_control;
    uint32_t *customer_access_control;
    uint32_t *history_access_control;
    uint32_t *neworder_access_control;
    uint32_t *order_access_control;
    uint32_t *orderline_access_control;
    uint32_t *stock_access_control;
    uint32_t *item_access_control;

    uint32_t *warehouse_txn_ID_to_log_offset;
    uint32_t *district_txn_ID_to_log_offset;
    uint32_t *customer_txn_ID_to_log_offset;
    uint32_t *history_txn_ID_to_log_offset;
    uint32_t *neworder_txn_ID_to_log_offset;
    uint32_t *order_txn_ID_to_log_offset;
    uint32_t *orderline_txn_ID_to_log_offset;
    uint32_t *stock_txn_ID_to_log_offset;
    uint32_t *item_txn_ID_to_log_offset;
};

class GPUquery
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

    uint32_t batch_size;
    uint32_t neworder_percent;
    uint32_t epoch_tp;
    uint32_t epoch_sync;
    uint32_t neworderquery_size;
    uint32_t paymentquery_size;
    uint32_t neworderquery_slice_size;
    uint32_t paymentquery_slice_size;
    uint32_t query_slice_size;
    uint32_t gen_neworderquery_size;
    uint32_t gen_paymentquery_size;
    uint32_t buffer_size;

    uint32_t device_cnt;
    uint32_t *device_IDs;

    NeworderQuery *neworderquery_d_0;
    NeworderQuery *neworderquery_d_1;
    NeworderQuery *neworderquery_d_2;
    NeworderQuery *neworderquery_d_3;
    NeworderQuery *neworderquery_d_4;
    NeworderQuery *neworderquery_d_5;
    NeworderQuery *neworderquery_d_6;
    NeworderQuery *neworderquery_d_7;

    PaymentQuery *paymentquery_d_0;
    PaymentQuery *paymentquery_d_1;
    PaymentQuery *paymentquery_d_2;
    PaymentQuery *paymentquery_d_3;
    PaymentQuery *paymentquery_d_4;
    PaymentQuery *paymentquery_d_5;
    PaymentQuery *paymentquery_d_6;
    PaymentQuery *paymentquery_d_7;

    NeworderQueryResult *neworderqueryresult_d_0;
    NeworderQueryResult *neworderqueryresult_d_1;
    NeworderQueryResult *neworderqueryresult_d_2;
    NeworderQueryResult *neworderqueryresult_d_3;
    NeworderQueryResult *neworderqueryresult_d_4;
    NeworderQueryResult *neworderqueryresult_d_5;
    NeworderQueryResult *neworderqueryresult_d_6;
    NeworderQueryResult *neworderqueryresult_d_7;

    NeworderQueryResult *neworderqueryresult_0;
    NeworderQueryResult *neworderqueryresult_1;
    NeworderQueryResult *neworderqueryresult_2;
    NeworderQueryResult *neworderqueryresult_3;
    NeworderQueryResult *neworderqueryresult_4;
    NeworderQueryResult *neworderqueryresult_5;
    NeworderQueryResult *neworderqueryresult_6;
    NeworderQueryResult *neworderqueryresult_7;

    PaymentQueryResult *paymentqueryresult_d_0;
    PaymentQueryResult *paymentqueryresult_d_1;
    PaymentQueryResult *paymentqueryresult_d_2;
    PaymentQueryResult *paymentqueryresult_d_3;
    PaymentQueryResult *paymentqueryresult_d_4;
    PaymentQueryResult *paymentqueryresult_d_5;
    PaymentQueryResult *paymentqueryresult_d_6;
    PaymentQueryResult *paymentqueryresult_d_7;

    PaymentQueryResult *paymentqueryresult_0;
    PaymentQueryResult *paymentqueryresult_1;
    PaymentQueryResult *paymentqueryresult_2;
    PaymentQueryResult *paymentqueryresult_3;
    PaymentQueryResult *paymentqueryresult_4;
    PaymentQueryResult *paymentqueryresult_5;
    PaymentQueryResult *paymentqueryresult_6;
    PaymentQueryResult *paymentqueryresult_7;

    ParamQuery paramquery_0;
    ParamQuery paramquery_1;
    ParamQuery paramquery_2;
    ParamQuery paramquery_3;
    ParamQuery paramquery_4;
    ParamQuery paramquery_5;
    ParamQuery paramquery_6;
    ParamQuery paramquery_7;

    ParamQuery *paramquery_d_0;
    ParamQuery *paramquery_d_1;
    ParamQuery *paramquery_d_2;
    ParamQuery *paramquery_d_3;
    ParamQuery *paramquery_d_4;
    ParamQuery *paramquery_d_5;
    ParamQuery *paramquery_d_6;
    ParamQuery *paramquery_d_7;

    WilldoTable willdotable_0;
    WilldoTable willdotable_1;
    WilldoTable willdotable_2;
    WilldoTable willdotable_3;
    WilldoTable willdotable_4;
    WilldoTable willdotable_5;
    WilldoTable willdotable_6;
    WilldoTable willdotable_7;

    WilldoTable *willdotable_d_0;
    WilldoTable *willdotable_d_1;
    WilldoTable *willdotable_d_2;
    WilldoTable *willdotable_d_3;
    WilldoTable *willdotable_d_4;
    WilldoTable *willdotable_d_5;
    WilldoTable *willdotable_d_6;
    WilldoTable *willdotable_d_7;

public:
    GPUquery(uint32_t warehouse_tbl_size, uint32_t device_cnt,
             uint32_t *device_IDs, uint32_t batch_size,
             uint32_t neworder_percent, uint32_t epoch_tp,
             uint32_t epoch_sync);

    ~GPUquery();

    template <typename Query>
    void copy_query_to_gpu(Query *query_c, Query *query_g,
                           uint32_t size_of_q);

    NeworderQuery *get_neworderquery();

    uint32_t get_neworderquery_size();

    PaymentQuery *get_paymentquery();

    uint32_t get_paymentquery_size();

    uint32_t get_epoch_tp();

    uint32_t get_epoch_sync();

    uint32_t get_buffer_size();

    uint32_t get_batch_size();

    uint32_t get_query_slice_size() { return this->query_slice_size; }

    void print_neworder();

    void print_payment();

    void malloc_result(NeworderQueryResult *&neworderqueryresult_c, NeworderQueryResult *&neworderqueryresult_g,
                       PaymentQueryResult *&paymentqueryresult_c, PaymentQueryResult *&paymentqueryresult_g,
                       uint32_t device_ID);

    void initialize_result();

    void free_result(NeworderQueryResult *&neworderqueryresult_c, NeworderQueryResult *&neworderqueryresult_g,
                     PaymentQueryResult *&paymentqueryresult_c, PaymentQueryResult *&paymentqueryresult_g,
                     uint32_t device_ID);

    void release_result();

    void copy_result(uint32_t device_ID, cudaStream_t *stream, uint32_t stream_ID);

    void reset_result(uint32_t device_ID, cudaStream_t *stream, uint32_t stream_ID);

    void malloc_query(NeworderQuery *&neworderquery, PaymentQuery *&paymentquery,
                      uint32_t device_ID);

    void initialize_query();

    void free_query(NeworderQuery *&neworderquery, PaymentQuery *&paymentquery,
                    uint32_t device_ID);

    void release_query();

    void copy_query(NeworderQuery *&neworderquery, PaymentQuery *&paymentquery,
                    uint32_t epoch_ID, uint32_t device_ID, cudaStream_t *stream);

    void copy_query(uint32_t epoch_ID, cudaStream_t *stream);

    NeworderQuery *get_neworderquery_d(uint32_t device_ID);

    PaymentQuery *get_paymentquery_d(uint32_t device_ID);

    uint32_t *get_device_IDs();

    uint32_t get_device_cnt();

    void initialize_param();

    void malloc_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID);

    void release_param();

    void free_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID);

    void copy_param(ParamQuery *&param_g, ParamQuery &param_c, uint32_t device_ID, cudaStream_t *stream);

    void copy_param(uint32_t epoch_ID, cudaStream_t *stream);

    ParamQuery *get_paramquery_d(uint32_t device_ID);

    ParamQuery *get_paramquery(uint32_t device_ID);

    void initialize_willdotable();

    void malloc_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID);

    void release_willdotable();

    void free_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID);

    void copy_willdotable(WilldoTable *&willdotable_g, WilldoTable &willdotable_c, uint32_t device_ID, cudaStream_t *stream);

    void copy_willdotable(uint32_t epoch_ID, cudaStream_t *stream);

    WilldoTable *get_willdotable_d(uint32_t device_ID);

    WilldoTable *get_willdotable(uint32_t device_ID);

    NeworderQueryResult *get_neworderqueryresult(uint32_t device_ID);

    PaymentQueryResult *get_paymentqueryresult(uint32_t device_ID);

    NeworderQueryResult *get_neworderqueryresult_d(uint32_t device_ID);

    PaymentQueryResult *get_paymentqueryresult_d(uint32_t device_ID);

    void print_malloc_size();
};

GPUquery *gpuquery;

extern void initialize_gpuquery(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                                uint32_t *device_IDs, uint32_t batch_size,
                                uint32_t neworder_percent, uint32_t epoch_tp,
                                uint32_t epoch_sync);

extern void release_gpuquery();

extern void launchQueryKernel(Result &result);

extern void copy_query_to_gpu(NeworderQuery *neworderquery, PaymentQuery *paymentquery);

extern void copy_query_to_cpu();

// WilldoTable **willdotables;
Willdo **willdo_send;
Willdo **willdo_recv;

__global__ void Kernel(NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                       ParamQuery *paramquery, uint32_t device_ID);

__global__ void reset_willdo(ParamQuery *paramquery, WilldoTable *willdotables);

__global__ void make_willdo(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                            NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                            ParamQuery *paramquery, WilldoTable *willdotables,
                            uint32_t epoch_ID, Willdo *willdo_send);

__device__ void make_neworder_willdo_warehouse(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_district(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_customer(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_neworder(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_order(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                           ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_stock(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                           ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_item(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                          ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_neworder_willdo_orderline(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, NeworderQueryResult *neworderqueryresult,
                                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);

__device__ void make_payment_willdo_warehouse(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                              ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_payment_willdo_district(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                             ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_payment_willdo_customer(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                             ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);
__device__ void make_payment_willdo_history(uint32_t query_offset, Snapshot *snapshot, PaymentQuery *paymentquery, PaymentQueryResult *paymentqueryresult,
                                            ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_send);

void initialize_buffer(uint32_t *device_IDs, uint32_t device_cnt);

void release_buffer(uint32_t *device_IDs, uint32_t device_cnt);

void copy_willdotables_p2p(cudaStream_t *stream, uint32_t *device_IDs,
                           uint32_t device_cnt, uint32_t size);

__global__ void merge(Snapshot *snapshot, ParamQuery *paramquery, WilldoTable *willdotables,
                      uint32_t epoch_ID, Willdo *willdo_recv);

__global__ void execute(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                        ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_recv);

__device__ void execute_warehouse(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                  NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                  ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_district(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_customer(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_history(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_neworder(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                 NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                 ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_order(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                              NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                              ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_orderline(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                  NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                  ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_stock(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                              NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                              ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);
__device__ void execute_item(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                             NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                             ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__global__ void execute_serial(Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                               NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                               ParamQuery *paramquery, WilldoTable *willdotables, uint32_t epoch_ID, Willdo *willdo_recv);

__device__ void execute_warehouse_serial(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                         NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                         ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_district_serial(uint32_t txn_ID, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_customer_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_history_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                       NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                       ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_neworder_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                        NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                        ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_order_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                     NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                     ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_orderline_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                         NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                         ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

__device__ void execute_stock_serial(uint32_t query_offset, Snapshot *snapshot, NeworderQuery *neworderquery, PaymentQuery *paymentquery,
                                     NeworderQueryResult *neworderqueryresult, PaymentQueryResult *paymentqueryresult,
                                     ParamQuery *paramquery, WilldoTable *willdotables, Willdo *willdo_recv);

void launch_make_willdo(uint32_t device_ID, cudaStream_t stream, uint32_t thID);