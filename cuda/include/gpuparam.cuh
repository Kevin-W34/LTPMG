#pragma once
#include "define.cuh"
#include "txn_structure.cuh"
#include "db_structure.cuh"

struct Result {
    double cost;
    uint32_t epoch;
    uint32_t batch_size;
};

class Param {
public:
    std::string benchmark;
    uint32_t table_size;
    uint32_t batch_size;
    uint32_t epoch_tp;
    uint32_t epoch_sync;
    std::string deviceIDs;
    uint32_t neworder_percent;
    std::string filepath;
    uint32_t table_cnt;
    uint32_t device_cnt;
    uint32_t *device_IDs;
    std::string data_distribution;
    double zipf_config;

    uint32_t test_1_size = 0;
    uint32_t test_2_size = 0;

    uint32_t warehouse_size = 0;
    uint32_t district_size = 0;
    uint32_t customer_size = 0;
    uint32_t neworder_size = 0;
    uint32_t history_size = 0;
    uint32_t order_size = 0;
    uint32_t orderline_size = 0;
    uint32_t stock_size = 0;
    uint32_t item_size = 100000;

    uint32_t ycsb_size = 0;
    uint32_t bitmap_row_cnt = 0;
    uint32_t bitmap_table_cnt = 0;

    uint32_t test_query_batch_size = 0;
    uint32_t test_query_2_batch_size = 0;
    uint32_t neworder_query_batch_size = 0;
    uint32_t payment_query_batch_size = 0;
    uint32_t orderstatus_query_batch_size = 0;
    uint32_t delivery_query_batch_size = 0;
    uint32_t stocklevel_query_batch_size = 0;
    uint32_t ycsb_a_query_batch_size = 0;
    uint32_t ycsb_b_query_batch_size = 0;
    uint32_t ycsb_c_query_batch_size = 0;
    uint32_t ycsb_d_query_batch_size = 0;
    uint32_t ycsb_e_query_batch_size = 0;

    uint32_t test_query_subtxn_cnt = 0;
    uint32_t test_query_2_subtxn_cnt = 0;
    uint32_t neworder_query_subtxn_cnt = 0;
    uint32_t payment_query_subtxn_cnt = 0;
    uint32_t orderstatus_query_subtxn_cnt = 0;
    uint32_t delivery_query_subtxn_cnt = 0;
    uint32_t stocklevel_query_subtxn_cnt = 0;
    uint32_t ycsb_a_query_subtxn_cnt = 0;
    uint32_t ycsb_b_query_subtxn_cnt = 0;
    uint32_t ycsb_c_query_subtxn_cnt = 0;
    uint32_t ycsb_d_query_subtxn_cnt = 0;
    uint32_t ycsb_e_query_subtxn_cnt = 0;

    uint32_t test_sub_txn_size = 0;
    uint32_t tpcc_all_sub_txn_size = 0;
    uint32_t tpcc_part_sub_txn_size = 0;
    uint32_t ycsb_a_sub_txn_size = 0;
    uint32_t ycsb_b_sub_txn_size = 0;
    uint32_t ycsb_c_sub_txn_size = 0;
    uint32_t ycsb_d_sub_txn_size = 0;
    uint32_t ycsb_e_sub_txn_size = 0;

    uint32_t test_query_subtxn_kinds = 0;
    uint32_t tpcc_all_query_subtxn_kinds = 0;
    uint32_t tpcc_part_query_subtxn_kinds = 0;
    uint32_t ycsb_a_query_subtxn_kinds = 0;
    uint32_t ycsb_b_query_subtxn_kinds = 0;
    uint32_t ycsb_c_query_subtxn_kinds = 0;
    uint32_t ycsb_d_query_subtxn_kinds = 0;
    uint32_t ycsb_e_query_subtxn_kinds = 0;

    uint32_t select_batch_size = 0;
    uint32_t insert_batch_size = 0;
    uint32_t update_batch_size = 0;
    uint32_t scan_batch_size = 0;
    uint32_t delete_batch_size = 0;

    Result result;

    uint32_t get_benchmark_ID();

    uint32_t get_subtxn_kinds();

    uint32_t get_sub_txn_size();

    uint32_t get_subtxn_cnt(const std::type_info &query_kind);

    uint32_t get_txn_batch_size(const std::type_info &query_kind);

    uint32_t get_select_batch_size();

    uint32_t get_insert_batch_size();

    uint32_t get_update_batch_size();

    uint32_t get_scan_batch_size();

    uint32_t get_delete_batch_size();

    uint32_t get_bitmap_size();

    uint32_t get_datapacket_size();
};
