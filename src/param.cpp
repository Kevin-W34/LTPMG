#include "../include/param.h"

Param::Param() : benchmark(FLAGS_benchmark), table_size(FLAGS_table_size),
                 batch_size(FLAGS_batch_size), epoch_tp(FLAGS_epoch_tp),
                 epoch_sync(FLAGS_epoch_sync), deviceIDs(FLAGS_deviceIDs),
                 neworder_percent(FLAGS_neworder_percent), zipf_config(FLAGS_zipf_config),
                 data_distribution(FLAGS_data_distribution) {
    result.cost = 0.0;
    result.epoch = epoch_tp;
    result.batch_size = batch_size;
    if (FLAGS_benchmark == "TEST") {
        filepath = "../data/TEST.json";
        test_query_batch_size = batch_size / 2;
        test_query_2_batch_size = batch_size / 2;
        test_query_subtxn_cnt = 5;
        test_query_2_subtxn_cnt = 5;
        test_sub_txn_size += test_query_subtxn_cnt * test_query_batch_size;
        test_sub_txn_size += test_query_2_subtxn_cnt * test_query_2_batch_size;
        test_query_subtxn_kinds += test_query_subtxn_cnt;
        test_query_subtxn_kinds += test_query_2_subtxn_cnt;
        test_1_size = 8;
        test_2_size = 24;
        table_cnt = 2;
        bitmap_table_cnt = 2;
    } else if (FLAGS_benchmark == "TPCC_ALL" || FLAGS_benchmark == "TPCC_PART") {
        filepath = "../data/TPCC.json";

        neworder_query_subtxn_cnt = 5 + 15 * 3;
        /*  0 select warehouse,
            1 select district,
            2 select customer,
            3 insert neworder,
            4 insert order
            5 select item,
            6 insert orderline
            7 update stock
         */
        payment_query_subtxn_cnt = 7;
        /*  8 select warehouse,
            9 select district,
            10 select customer
            11 update customer,
            12 update warehouse,
            13 update district,
            14 insert history
        */
        orderstatus_query_subtxn_cnt = 3;
        /*  15 select customer,
            16 select order,
            17 select orderline
        */
        delivery_query_subtxn_cnt = 5 * 10;
        /*  18 delete neworder,
            19 update order,
            20 update orderline,
            21 select orderline,
            22 update customer
        */
        stocklevel_query_subtxn_cnt = 1 + 2 * 10;
        /*  23 select district,
            24 select orderline,
            25 select stock
        */
        if (FLAGS_benchmark == "TPCC_PART") {
            neworder_query_batch_size = batch_size * neworder_percent / 100;
            payment_query_batch_size = batch_size - neworder_query_batch_size;
            tpcc_part_sub_txn_size += neworder_query_subtxn_cnt * neworder_query_batch_size;
            tpcc_part_sub_txn_size += payment_query_subtxn_cnt * payment_query_batch_size;
            tpcc_part_query_subtxn_kinds += 8;
            tpcc_part_query_subtxn_kinds += 7;
        } else if (FLAGS_benchmark == "TPCC_ALL") {
            neworder_query_batch_size = batch_size * 44 / 100;
            payment_query_batch_size = batch_size * 44 / 100;
            orderstatus_query_batch_size = batch_size * 4 / 100;
            delivery_query_batch_size = batch_size * 4 / 100;
            stocklevel_query_batch_size = batch_size * 4 / 100;

            tpcc_all_sub_txn_size += neworder_query_subtxn_cnt * neworder_query_batch_size;
            tpcc_all_sub_txn_size += payment_query_subtxn_cnt * payment_query_batch_size;
            tpcc_all_sub_txn_size += orderstatus_query_subtxn_cnt * orderstatus_query_batch_size;
            tpcc_all_sub_txn_size += delivery_query_subtxn_cnt * delivery_query_batch_size;
            tpcc_all_sub_txn_size += stocklevel_query_subtxn_cnt * stocklevel_query_batch_size;

            tpcc_all_query_subtxn_kinds += 8;
            tpcc_all_query_subtxn_kinds += 7;
            tpcc_all_query_subtxn_kinds += 3;
            tpcc_all_query_subtxn_kinds += 5;
            tpcc_all_query_subtxn_kinds += 3;
        }
        warehouse_size = table_size;
        district_size = warehouse_size * 10;
        customer_size = district_size * 3000;
        neworder_size = district_size * 3000;
        history_size = district_size * 3000;
        order_size = district_size * 3000;
        orderline_size = district_size * 45000;
        stock_size = table_size * 100000;
        item_size = 100000;
        table_cnt = 9;
        bitmap_table_cnt = 2;
    } else {
        filepath = "../data/YCSB.json";
        ycsb_size = table_size * 1000000;
        table_cnt = 1;
        bitmap_row_cnt = 100;
        bitmap_table_cnt = 1;
        if (FLAGS_benchmark == "YCSB_A") {
            ycsb_a_query_batch_size = batch_size;
            ycsb_a_query_subtxn_cnt = 10;
            ycsb_a_sub_txn_size = ycsb_a_query_subtxn_cnt * ycsb_a_query_batch_size;
            ycsb_a_query_subtxn_kinds = ycsb_a_query_subtxn_cnt;
        } else if (FLAGS_benchmark == "YCSB_B") {
            ycsb_b_query_batch_size = batch_size;
            ycsb_b_query_subtxn_cnt = 10;
            ycsb_b_sub_txn_size = ycsb_b_query_subtxn_cnt * ycsb_b_query_batch_size;
            ycsb_b_query_subtxn_kinds = ycsb_b_query_subtxn_cnt;
        } else if (FLAGS_benchmark == "YCSB_C") {
            ycsb_c_query_batch_size = batch_size;
            ycsb_c_query_subtxn_cnt = 10;
            ycsb_c_sub_txn_size = ycsb_c_query_subtxn_cnt * ycsb_c_query_batch_size;
            ycsb_c_query_subtxn_kinds = ycsb_c_query_subtxn_cnt;
        } else if (FLAGS_benchmark == "YCSB_D") {
            ycsb_d_query_batch_size = batch_size;
            ycsb_d_query_subtxn_cnt = 10;
            ycsb_d_sub_txn_size = ycsb_d_query_subtxn_cnt * ycsb_d_query_batch_size;
            ycsb_d_query_subtxn_kinds = ycsb_d_query_subtxn_cnt;
        } else if (FLAGS_benchmark == "YCSB_E") {
            ycsb_e_query_batch_size = batch_size;
            ycsb_e_query_subtxn_cnt = 10;
            ycsb_e_sub_txn_size = ycsb_e_query_subtxn_cnt * ycsb_e_query_batch_size;
            ycsb_e_query_subtxn_kinds = ycsb_e_query_subtxn_cnt;
        }
    }
}

Param::~Param() {
}

void Param::print() {
    LOG(INFO) << "benchmark: " << benchmark;
    if (FLAGS_benchmark == "TPCC_ALL" || FLAGS_benchmark == "TPCC_PART") {
        LOG(INFO) << "warehouse_size: " << table_size;
    } else if (FLAGS_benchmark == "TEST") {
        LOG(INFO) << "table_size: " << 32;
    } else {
        LOG(INFO) << "table_size: " << ycsb_size;
    }
    LOG(INFO) << "batch_size: " << batch_size;
    LOG(INFO) << "epoch_tp: " << epoch_tp;
    LOG(INFO) << "epoch_sync: " << epoch_sync;
    LOG(INFO) << "deviceIDs: " << deviceIDs;
    LOG(INFO) << "neworder_percent: " << neworder_percent;
    LOG(INFO) << "zipf_config: " << zipf_config;
    LOG(INFO) << "data_distribution: " << data_distribution;
    LOG(INFO) << "result.cost: " << result.cost;
    LOG(INFO) << "result.epoch: " << result.epoch;
    LOG(INFO) << "result.batch_size: " << result.batch_size;
}

void Param::analyse_deviceIDs() {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream tokenStream(this->deviceIDs);
    while (std::getline(tokenStream, token, ',')) {
        tokens.push_back(token);
    }
    this->device_cnt = tokens.size();
    LOG(INFO) << "this->device_cnt=" << this->device_cnt;
    this->device_IDs = new uint32_t[this->device_cnt];
    for (size_t i = 0; i < this->device_cnt; i++) {
        std::string token = tokens[i];
        this->device_IDs[i] = atoi(token.c_str());
    }
    for (size_t i = 0; i < this->device_cnt; i++) {
        LOG(INFO) << "this->device_IDs[" << i << "]=" << this->device_IDs[i];
    }
}

void Param::free_deviceIDs() {
    delete[] this->device_IDs;
}

uint32_t Param::get_subtxn_kinds() {
    if (benchmark == "TEST") {
        return test_query_subtxn_kinds;
    } else if (benchmark == "TPCC_PART") {
        return tpcc_part_query_subtxn_kinds;
    } else if (benchmark == "TPCC_ALL") {
        return tpcc_all_query_subtxn_kinds;
    } else if (benchmark == "YCSB_A") {
        return ycsb_a_query_subtxn_kinds;
    } else if (benchmark == "YCSB_B") {
        return ycsb_b_query_subtxn_kinds;
    } else if (benchmark == "YCSB_C") {
        return ycsb_c_query_subtxn_kinds;
    } else if (benchmark == "YCSB_D") {
        return ycsb_d_query_subtxn_kinds;
    } else if (benchmark == "YCSB_E") {
        return ycsb_e_query_subtxn_kinds;
    } else {
        return 0;
    }
}

uint32_t Param::get_sub_txn_size() {
    if (benchmark == "TEST") {
        return test_sub_txn_size;
    } else if (benchmark == "TPCC_PART") {
        return tpcc_part_sub_txn_size;
    } else if (benchmark == "TPCC_ALL") {
        return tpcc_all_sub_txn_size;
    } else if (benchmark == "YCSB_A") {
        return ycsb_a_sub_txn_size;
    } else if (benchmark == "YCSB_B") {
        return ycsb_b_sub_txn_size;
    } else if (benchmark == "YCSB_C") {
        return ycsb_c_sub_txn_size;
    } else if (benchmark == "YCSB_D") {
        return ycsb_d_sub_txn_size;
    } else if (benchmark == "YCSB_E") {
        return ycsb_e_sub_txn_size;
    } else {
        return 0;
    }
}

uint32_t Param::get_subtxn_cnt(const std::type_info &query_kind) {
    if (query_kind == typeid(Test_Query)) {
        return test_query_subtxn_cnt;
    } else if (query_kind == typeid(Test_Query_2)) {
        return test_query_2_subtxn_cnt;
    } else if (query_kind == typeid(Neworder_Query)) {
        return neworder_query_subtxn_cnt;
    } else if (query_kind == typeid(Payment_Query)) {
        return payment_query_subtxn_cnt;
    } else if (query_kind == typeid(Orderstatus_Query)) {
        return orderstatus_query_subtxn_cnt;
    } else if (query_kind == typeid(Delivery_Query)) {
        return delivery_query_subtxn_cnt;
    } else if (query_kind == typeid(Stocklevel_Query)) {
        return stocklevel_query_subtxn_cnt;
    } else if (query_kind == typeid(YCSB_A_Query)) {
        return ycsb_a_query_subtxn_cnt;
    } else if (query_kind == typeid(YCSB_B_Query)) {
        return ycsb_b_query_subtxn_cnt;
    } else if (query_kind == typeid(YCSB_C_Query)) {
        return ycsb_c_query_subtxn_cnt;
    } else if (query_kind == typeid(YCSB_D_Query)) {
        return ycsb_d_query_subtxn_cnt;
    } else if (query_kind == typeid(YCSB_E_Query)) {
        return ycsb_e_query_subtxn_cnt;
    } else {
        return 0;
    }
}

uint32_t Param::get_txn_batch_size(const std::type_info &query_kind) {
    if (query_kind == typeid(Test_Query)) {
        return test_query_batch_size;
    } else if (query_kind == typeid(Test_Query_2)) {
        return test_query_2_batch_size;
    } else if (query_kind == typeid(Neworder_Query)) {
        return neworder_query_batch_size;
    } else if (query_kind == typeid(Payment_Query)) {
        return payment_query_batch_size;
    } else if (query_kind == typeid(Orderstatus_Query)) {
        return orderstatus_query_batch_size;
    } else if (query_kind == typeid(Delivery_Query)) {
        return delivery_query_batch_size;
    } else if (query_kind == typeid(Stocklevel_Query)) {
        return stocklevel_query_batch_size;
    } else if (query_kind == typeid(YCSB_A_Query)) {
        return ycsb_a_query_batch_size;
    } else if (query_kind == typeid(YCSB_B_Query)) {
        return ycsb_b_query_batch_size;
    } else if (query_kind == typeid(YCSB_C_Query)) {
        return ycsb_c_query_batch_size;
    } else if (query_kind == typeid(YCSB_D_Query)) {
        return ycsb_d_query_batch_size;
    } else if (query_kind == typeid(YCSB_E_Query)) {
        return ycsb_e_query_batch_size;
    } else {
        return 0;
    }
}
