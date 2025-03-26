#include "../include/gpuparam.cuh"

uint32_t Param::get_benchmark_ID() {
    if (benchmark == "TEST") {
        return 1;
    } else if (benchmark == "TPCC_PART") {
        return 2;
    } else if (benchmark == "TPCC_ALL") {
        return 3;
    } else if (benchmark == "YCSB_A") {
        return 4;
    } else if (benchmark == "YCSB_B") {
        return 4;
    } else if (benchmark == "YCSB_C") {
        return 4;
    } else if (benchmark == "YCSB_D") {
        return 4;
    } else if (benchmark == "YCSB_E") {
        return 4;
    }
    return 0;
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
    }

    return 0;
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
    }

    return 0;
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
    }

    return 0;
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
    }

    return 0;
}

uint32_t Param::get_select_batch_size() {
    uint32_t result = 0;
    if (benchmark == "TEST") {
        result += 5 * test_query_2_batch_size;
        result += test_query_batch_size;
    } else if (benchmark == "TPCC_PART") {
        result += 18 * neworder_query_batch_size;
        result += 3 * payment_query_batch_size;
    } else if (benchmark == "TPCC_ALL") {
        result += 18 * neworder_query_batch_size;
        result += 3 * payment_query_batch_size;
        result += 3 * orderstatus_query_batch_size;
        result += 10 * delivery_query_batch_size;
        result += 21 * stocklevel_query_batch_size;
    } else if (benchmark == "YCSB_A") {
        result = 5 * ycsb_a_query_batch_size;
    } else if (benchmark == "YCSB_B") {
        result = 9 * ycsb_b_query_batch_size;
    } else if (benchmark == "YCSB_C") {
        result = 10 * ycsb_c_query_batch_size;
    } else if (benchmark == "YCSB_D") {
        result = 9 * ycsb_d_query_batch_size;
    } else if (benchmark == "YCSB_E") {
        result = 0;
    }

    return result;
}

uint32_t Param::get_insert_batch_size() {
    uint32_t result = 0;
    if (benchmark == "TEST") {
        result = test_query_batch_size;
    } else if (benchmark == "TPCC_PART") {
        result += 17 * neworder_query_batch_size;
        result += payment_query_batch_size;
    } else if (benchmark == "TPCC_ALL") {
        result += 17 * neworder_query_batch_size;
        result += payment_query_batch_size;
    } else if (benchmark == "YCSB_A") {
        result = 0;
    } else if (benchmark == "YCSB_B") {
        result = 0;
    } else if (benchmark == "YCSB_C") {
        result = 0;
    } else if (benchmark == "YCSB_D") {
        result = 0;
    } else if (benchmark == "YCSB_E") {
        result = ycsb_e_query_batch_size;
    }

    return result;
}

uint32_t Param::get_update_batch_size() {
    uint32_t result = 0;
    if (benchmark == "TEST") {
        result += test_query_batch_size;
    } else if (benchmark == "TPCC_PART") {
        result += 15 * neworder_query_batch_size;
        result += 3 * payment_query_batch_size;
    } else if (benchmark == "TPCC_ALL") {
        result += 15 * neworder_query_batch_size;
        result += 3 * payment_query_batch_size;
        result += 30 * delivery_query_batch_size;
    } else if (benchmark == "YCSB_A") {
        result = 5 * ycsb_a_query_batch_size;
    } else if (benchmark == "YCSB_B") {
        result = ycsb_b_query_batch_size;
    } else if (benchmark == "YCSB_C") {
        result = 0;
    } else if (benchmark == "YCSB_D") {
        result = ycsb_d_query_batch_size;
    } else if (benchmark == "YCSB_E") {
        result = 0;
    }

    return result;
}

uint32_t Param::get_scan_batch_size() {
    uint32_t result = 0;
    if (benchmark == "TEST") {
        result += test_query_batch_size;
    } else if (benchmark == "TPCC_PART") {
        result = 0;
    } else if (benchmark == "TPCC_ALL") {
        result = 0;
    } else if (benchmark == "YCSB_A") {
        result = 0;
    } else if (benchmark == "YCSB_B") {
        result = 0;
    } else if (benchmark == "YCSB_C") {
        result = 0;
    } else if (benchmark == "YCSB_D") {
        result = 0;
    } else if (benchmark == "YCSB_E") {
        result = 9 * ycsb_e_query_batch_size;
    }

    return result;
}

uint32_t Param::get_delete_batch_size() {
    uint32_t result = 0;
    if (benchmark == "TEST") {
        result += test_query_batch_size;
    } else if (benchmark == "TPCC_PART") {
        result = 0;
    } else if (benchmark == "TPCC_ALL") {
        result += 1 * 10 * delivery_query_batch_size;
    } else if (benchmark == "YCSB_A") {
        result = 0;
    } else if (benchmark == "YCSB_B") {
        result = 0;
    } else if (benchmark == "YCSB_C") {
        result = 0;
    } else if (benchmark == "YCSB_D") {
        result = 0;
    } else if (benchmark == "YCSB_E") {
        result = 0;
    }

    return result;
}

uint32_t Param::get_bitmap_size() {
    uint32_t result = 0;
    result = batch_size;

    // if (benchmark == "TEST")
    // {
    //     result = batch_size;
    // }
    // else if (benchmark == "TPCC_PART")
    // {
    // }
    // else if (benchmark == "TPCC_ALL")
    // {
    // }
    // else if (benchmark == "YCSB_A")
    // {
    // }
    // else if (benchmark == "YCSB_B")
    // {
    // }
    // else if (benchmark == "YCSB_C")
    // {
    // }
    // else if (benchmark == "YCSB_D")
    // {
    // }
    // else if (benchmark == "YCSB_E")
    // {
    // }

    uint32_t tmp1 = result / 32;
    uint32_t tmp2 = result % 32;
    result = tmp2 > 0 ? tmp1 + 1 : tmp1;
    return result;
}

uint32_t Param::get_datapacket_size() {
    return this->get_sub_txn_size()*0.5 ;
}
