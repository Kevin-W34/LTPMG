#pragma once
#include "common.cuh"

class GPUdatabase
{
private:
    WAREHOUSE_ROW *warehouse_tbl;
    DISTRICT_ROW *district_tbl;
    CUSTOMER_ROW *customer_tbl;
    HISTORY_ROW *history_tbl;
    NEWORDER_ROW *neworder_tbl;
    ORDER_ROW *order_tbl;
    ORDERLINE_ROW *orderline_tbl;
    STOCK_ROW *stock_tbl;
    ITEM_ROW *item_tbl;
    uint32_t *customer_name_index;

    uint32_t warehouse_tbl_size;
    uint32_t district_tbl_size;
    uint32_t customer_tbl_size;
    uint32_t history_tbl_size;
    uint32_t neworder_tbl_size;
    uint32_t order_tbl_size;
    uint32_t orderline_tbl_size;
    uint32_t stock_tbl_size;
    uint32_t item_tbl_size;

    uint32_t warehouse_slice_size;
    uint32_t district_slice_size;
    uint32_t customer_slice_size;
    uint32_t history_slice_size;
    uint32_t neworder_slice_size;
    uint32_t order_slice_size;
    uint32_t orderline_slice_size;
    uint32_t stock_slice_size;
    uint32_t item_slice_size;

    uint32_t device_cnt;
    uint32_t *device_IDs;

    Snapshot *snapshot_0;
    Snapshot *snapshot_1;
    Snapshot *snapshot_2;
    Snapshot *snapshot_3;
    Snapshot *snapshot_4;
    Snapshot *snapshot_5;
    Snapshot *snapshot_6;
    Snapshot *snapshot_7;

public:
    Snapshot tmp[8];

    GPUdatabase(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                uint32_t *device_IDs);

    ~GPUdatabase();

    template <typename Table>
    void copy_database_to_gpu(Table *table_c, Table *table_g,
                              uint32_t size_of_t);

    WAREHOUSE_ROW *get_warehouse();

    uint32_t get_warehouse_size();

    DISTRICT_ROW *get_district();

    uint32_t get_district_size();

    CUSTOMER_ROW *get_customer();

    uint32_t get_customer_size();

    HISTORY_ROW *get_history();

    uint32_t get_history_size();

    NEWORDER_ROW *get_neworder();

    uint32_t get_neworder_size();

    ORDER_ROW *get_order();

    uint32_t get_order_size();

    ORDERLINE_ROW *get_orderline();

    uint32_t get_orderline_size();

    STOCK_ROW *get_stock();

    uint32_t get_stock_size();

    ITEM_ROW *get_item();

    uint32_t get_item_size();

    uint32_t *get_customer_name_index();

    uint32_t get_customer_name_index_size();

    void print_warehouse();

    void initialize_snapshot();

    void release_snapshot();

    void malloc_and_copy_snapshot(Snapshot *&snapshot, Snapshot *tmp,
                                  uint32_t device_ID);

    void free_snapshot(Snapshot *&snapshot, Snapshot *tmp, uint32_t device_ID);

    Snapshot *get_snapshot(uint32_t device_ID);

    uint32_t *get_device_IDs();

    uint32_t get_device_cnt();

    void print();
};

GPUdatabase *gpudatabase;

extern void initialize_gpudatabase(uint32_t warehouse_tbl_size, uint32_t device_cnt,
                                   uint32_t *device_IDs);

extern void release_gpudatabase();

extern void copy_database_to_gpu(WAREHOUSE_ROW *warehouse, DISTRICT_ROW *district,
                                      CUSTOMER_ROW *customer, HISTORY_ROW *history,
                                      NEWORDER_ROW *neworder, ORDER_ROW *order,
                                      ORDERLINE_ROW *orderline, STOCK_ROW *stock,
                                      ITEM_ROW *item, uint32_t *customer_name_index);

extern void copy_database_to_cpu();

extern void launchDatabaseKernel();

__global__ void testDatabase(Snapshot *snapshot);