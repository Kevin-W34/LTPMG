#include "../include/gpulauncher.cuh"

// __device__ void print(uint32_t device_ID) {
//     // uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
//     // printf("__device__ void GPUexecutor::print() thID:%d\n", thID);
// }

template<typename D>
__device__ void select_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo) {
    D data;
    rowID = rowID % metainfo[tableID].table_slice_size;
    if (data_type == 1) {
        data = tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID];
    } else if (data_type == 0) {
        data = tables[tableID].string_data[
            rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID];
    } else if (data_type == 3) {
        data = tables[tableID].double_data[
            rowID * table_info[tableID].double_size + data_ID];
    }
}

template<typename D>
__device__ void scan_operator(uint32_t tableID,
                              uint32_t rowID,
                              uint32_t data_ID,
                              u_char data_type,
                              Global_Table *tables,
                              Global_Table_Info *table_info,
                              Global_Table_Meta *metainfo) {
    D data;
    rowID = rowID % metainfo[tableID].table_slice_size;

    if (data_type == 1) {
        data = tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID];
    } else if (data_type == 0) {
        data = tables[tableID].string_data[
            rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID];
    } else if (data_type == 3) {
        data = tables[tableID].double_data[
            rowID * table_info[tableID].double_size + data_ID];
    }
}

template<typename D>
__device__ void insert_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                D data,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo) {
    rowID = rowID % metainfo[tableID].table_slice_size;

    if (data_type == 1) {
        INT32 int_data = tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID];
        // tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID] = int_data;
    } else if (data_type == 0) {
        UINT32 string_data = tables[tableID].string_data[
            rowID * table_info[tableID].string_size * table_info[tableID].string_length];
        // tables[tableID].string_data[
        //     rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID] = string_data;
    } else if (data_type == 3) {
        DOUBLE double_data = tables[tableID].double_data[
            rowID * table_info[tableID].double_size + data_ID];
        // tables[tableID].double_data[
        //     rowID * table_info[tableID].double_size + data_ID] = double_data;
    }
}

template<typename D>
__device__ void update_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                D data,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo) {
    rowID = rowID % metainfo[tableID].table_slice_size;

    if (data_type == 1) {
        INT32 int_data = tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID];
        // tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID] = int_data;
    } else if (data_type == 0) {
        UINT32 string_data = tables[tableID].string_data[
            rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID];
        // tables[tableID].string_data[
        //     rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID] = string_data;
    } else if (data_type == 3) {
        DOUBLE double_data = tables[tableID].double_data[
            rowID * table_info[tableID].double_size + data_ID];
        // tables[tableID].double_data[
        //     rowID * table_info[tableID].double_size + data_ID] = double_data;
    }
}

template<typename D>
__device__ void delete_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo) {
    rowID = rowID % metainfo[tableID].table_slice_size;

    D data;
    if (data_type == 1) {
        data = tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID];
        // tables[tableID].int_data[rowID * table_info[tableID].int_size + data_ID] = data;
    } else if (data_type == 0) {
        data = tables[tableID].string_data[
            rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID];
        // tables[tableID].string_data[
        //     rowID * table_info[tableID].string_size * table_info[tableID].string_length + data_ID] = data;
    } else if (data_type == 3) {
        data = tables[tableID].double_data[
            rowID * table_info[tableID].double_size + data_ID];
        // tables[tableID].double_data[
        //     rowID * table_info[tableID].double_size + data_ID] = data;
    }
}

template<typename D>
__device__ void select_operator_shared(D &d) {
    D data;
    data = d;
    d = data;
}

template<typename D>
__device__ void scan_operator_shared(D &d) {
    D data;
    data = d;
    d = data;
}

template<typename D>
__device__ void insert_operator_shared(D data,
                                       D &d) {
    data = d;
    d = data;
}

template<typename D>
__device__ void update_operator_shared(D data,
                                       D &d) {
    data = d;
    d = data;
}

template<typename D>
__device__ void delete_operator_shared(D &d) {
    D data;
    data = d;
    d = data;
}

__device__ void select_executor(uint32_t device_ID,
                                uint32_t cur_txn,
                                uint32_t device_cnt,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Index *indexes,
                                Global_Table_Meta *metainfo,
                                Global_Table_Strategy *strategy,
                                Global_Txn_Info *txn_info,
                                Global_Txn *txn,
                                Global_Txn_Exec *txn_exec,
                                Global_Txn_Result *txn_result,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    uint32_t tableID = txn[cur_txn].subtxn.table_ID;
    uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
    uint32_t tid = txn[cur_txn].subtxn.TID;
    uint32_t type = txn[cur_txn].subtxn.type;
    uint32_t ispopular = txn[cur_txn].subtxn.ispopular;
    uint32_t dest_device = txn[cur_txn].subtxn.dest_device;

    if (dest_device == device_ID) {
        if (ispopular == 0 || ispopular == 1) {
            register_txn_exec(device_ID, type, cur_txn, txn_exec);

            register_cc(cur_txn, ispopular, tableID,
                        row_1, tid, table_info, tables, indexes, metainfo, strategy, txn_info,
                        txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        } else {
        }
    } else if (dest_device < 0xffffffff) {
        make_data_packet(device_ID, cur_txn, dest_device, device_cnt, table_info, tables, indexes,
                         metainfo, strategy, txn_info, txn, txn_exec, txn_result, exec_param, aux_struct,
                         data_packet);
    }
}

__device__ void insert_executor(uint32_t device_ID,
                                uint32_t cur_txn,
                                uint32_t device_cnt,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Index *indexes,
                                Global_Table_Meta *metainfo,
                                Global_Table_Strategy *strategy,
                                Global_Txn_Info *txn_info,
                                Global_Txn *txn,
                                Global_Txn_Exec *txn_exec,
                                Global_Txn_Result *txn_result,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    uint32_t tableID = txn[cur_txn].subtxn.table_ID;
    uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
    uint32_t tid = txn[cur_txn].subtxn.TID;
    uint32_t type = txn[cur_txn].subtxn.type;
    uint32_t ispopular = txn[cur_txn].subtxn.ispopular;
    uint32_t dest_device = txn[cur_txn].subtxn.dest_device;

    if (dest_device == device_ID) {
        register_txn_exec(device_ID, type, cur_txn, txn_exec);

        register_cc(cur_txn, ispopular, tableID,
                    row_1, tid, table_info, tables, indexes, metainfo, strategy, txn_info,
                    txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
    } else if (dest_device < 0xffffffff) {
        make_data_packet(device_ID, cur_txn, dest_device, device_cnt, table_info, tables, indexes,
                         metainfo, strategy, txn_info, txn, txn_exec, txn_result, exec_param, aux_struct,
                         data_packet);
    }
}

__device__ void update_executor(uint32_t device_ID,
                                uint32_t cur_txn,
                                uint32_t device_cnt,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Index *indexes,
                                Global_Table_Meta *metainfo,
                                Global_Table_Strategy *strategy,
                                Global_Txn_Info *txn_info,
                                Global_Txn *txn,
                                Global_Txn_Exec *txn_exec,
                                Global_Txn_Result *txn_result,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    uint32_t tableID = txn[cur_txn].subtxn.table_ID;
    uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
    uint32_t tid = txn[cur_txn].subtxn.TID;
    uint32_t type = txn[cur_txn].subtxn.type;
    uint32_t ispopular = txn[cur_txn].subtxn.ispopular;
    uint32_t dest_device = txn[cur_txn].subtxn.dest_device;
    if (dest_device == device_ID) {
        register_txn_exec(device_ID, type, cur_txn, txn_exec);

        register_cc(cur_txn, ispopular, tableID,
                    row_1, tid, table_info, tables, indexes, metainfo, strategy, txn_info,
                    txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
    } else if (dest_device < 0xffffffff) {
        make_data_packet(device_ID, cur_txn, dest_device, device_cnt, table_info, tables, indexes,
                         metainfo, strategy, txn_info, txn, txn_exec, txn_result, exec_param, aux_struct,
                         data_packet);
    }
}

__device__ void scan_executor(uint32_t device_ID,
                              uint32_t cur_txn,
                              uint32_t device_cnt,
                              Global_Table_Info *table_info,
                              Global_Table *tables,
                              Global_Table_Index *indexes,
                              Global_Table_Meta *metainfo,
                              Global_Table_Strategy *strategy,
                              Global_Txn_Info *txn_info,
                              Global_Txn *txn,
                              Global_Txn_Exec *txn_exec,
                              Global_Txn_Result *txn_result,
                              Global_Txn_Exec_Param *exec_param,
                              Global_Txn_Aux_Struct *aux_struct,
                              Global_Data_Packet *data_packet) {
    uint32_t tableID = txn[cur_txn].subtxn.table_ID;
    uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
    uint32_t row_2 = txn[cur_txn].subtxn.dest_Row_2;
    uint32_t tid = txn[cur_txn].subtxn.TID;
    uint32_t row_start = metainfo[tableID].row_start;
    uint32_t row_end = metainfo[tableID].row_end;
    uint32_t type = txn[cur_txn].subtxn.type;
    uint32_t ispopular = txn[cur_txn].subtxn.ispopular;

    bool contain_local = false;
    bool contain_remote = false;
    for (uint32_t row = row_1; row < row_2; ++row) {
        if (row >= row_start && row < row_end) {
            contain_local = true;
            register_cc(cur_txn, ispopular, tableID, row,
                        tid, table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                        txn_result, exec_param, aux_struct, data_packet);
        } else {
            contain_remote = true;
        }
    }
    if (contain_local) {
        register_txn_exec(device_ID, type, cur_txn, txn_exec);
    }
    if (contain_remote) {
        uint32_t dest_device_1 = row_1 / metainfo[tableID].table_slice_size;
        uint32_t dest_device_2 = row_2 / metainfo[tableID].table_slice_size;
        for (uint32_t dest_device = dest_device_1; dest_device < dest_device_2; ++dest_device) {
            make_data_packet(device_ID, cur_txn, dest_device, device_cnt, table_info, tables, indexes,
                             metainfo, strategy, txn_info, txn, txn_exec, txn_result, exec_param, aux_struct,
                             data_packet);
        }
    }
}

__device__ void delete_executor(uint32_t device_ID,
                                uint32_t cur_txn,
                                uint32_t device_cnt,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Index *indexes,
                                Global_Table_Meta *metainfo,
                                Global_Table_Strategy *strategy,
                                Global_Txn_Info *txn_info,
                                Global_Txn *txn,
                                Global_Txn_Exec *txn_exec,
                                Global_Txn_Result *txn_result,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    uint32_t tableID = txn[cur_txn].subtxn.table_ID;
    uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
    uint32_t tid = txn[cur_txn].subtxn.TID;
    uint32_t type = txn[cur_txn].subtxn.type;
    uint32_t ispopular = txn[cur_txn].subtxn.ispopular;
    uint32_t dest_device = txn[cur_txn].subtxn.dest_device;

    if (dest_device == device_ID) {
        register_txn_exec(device_ID, type, cur_txn, txn_exec);

        register_cc(cur_txn, ispopular, tableID, row_1,
                    tid, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                    txn_exec, txn_result, exec_param, aux_struct, data_packet);
    } else if (dest_device < 0xffffffff) {
        make_data_packet(device_ID, cur_txn, dest_device, device_cnt, table_info, tables, indexes,
                         metainfo, strategy, txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
    }
}

__device__ void register_txn_exec(uint32_t device_ID,
                                  uint32_t type,
                                  uint32_t cur_txn,
                                  Global_Txn_Exec *txn_exec) {
    uint32_t txn_exec_loc = 0;
    uint32_t txn_mark = cur_txn;
    if (type == 0) {
        txn_exec_loc = atomicAdd(&txn_exec[0].select_cur, 1);
        txn_exec[0].select_txn_mark[txn_exec_loc] = txn_mark;
    } else if (type == 1) {
        txn_exec_loc = atomicAdd(&txn_exec[0].insert_cur, 1);
        txn_exec[0].insert_txn_mark[txn_exec_loc] = txn_mark;
    } else if (type == 2) {
        txn_exec_loc = atomicAdd(&txn_exec[0].update_cur, 1);
        txn_exec[0].update_txn_mark[txn_exec_loc] = txn_mark;
    } else if (type == 4) {
        txn_exec_loc = atomicAdd(&txn_exec[0].scan_cur, 1);
        txn_exec[0].scan_txn_mark[txn_exec_loc] = txn_mark;
    } else if (type == 3) {
        txn_exec_loc = atomicAdd(&txn_exec[0].delete_cur, 1);
        txn_exec[0].delete_txn_mark[txn_exec_loc] = txn_mark;
    }
}

__device__ void register_cc(uint32_t cur_txn,
                            uint32_t ispopular,
                            uint32_t tableID,
                            uint32_t row,
                            uint32_t tid,
                            Global_Table_Info *table_info,
                            Global_Table *tables,
                            Global_Table_Index *indexes,
                            Global_Table_Meta *metainfo,
                            Global_Table_Strategy *strategy,
                            Global_Txn_Info *txn_info,
                            Global_Txn *txn,
                            Global_Txn_Exec *txn_exec,
                            Global_Txn_Result *txn_result,
                            Global_Txn_Exec_Param *exec_param,
                            Global_Txn_Aux_Struct *aux_struct,
                            Global_Data_Packet *data_packet) {
    uint32_t txn_mark = 0;
    uint32_t cur = row % metainfo[tableID].table_slice_size;
    if (ispopular == 1) {
        // aux_struct
        uint32_t bitmap_size = exec_param[0].bitmap_size;
        uint32_t loc_in = 1 << (tid & 31);
        uint32_t loc_out = tid >> 5;
        loc_out += bitmap_size * cur;
        atomicOr(&aux_struct[tableID].bitmap[loc_out], loc_in);
        txn_mark = cur_txn;
        uint32_t bitmark_offset = bitmap_size * 32 * cur;
        atomicExch(&aux_struct[tableID].bitmap_mark[bitmark_offset + tid], txn_mark);
        atomicAdd(&aux_struct[tableID].bitmap_used_size[cur], 1);
        // aux_struct[tableID].bitmap_all_row[cur_txn] = cur;
    } else if (ispopular == 0) {
        // atomicMin(&aux_struct[tableID].min_TID[cur], tid);
        uint32_t tmp = 0;
        tmp = atomicAdd(&aux_struct[tableID].cnt_TID[cur], 1);
    }
}

__device__ void make_data_packet(uint32_t device_ID,
                                 uint32_t cur_txn,
                                 uint32_t dest_device,
                                 uint32_t device_cnt,
                                 Global_Table_Info *table_info,
                                 Global_Table *tables,
                                 Global_Table_Index *indexes,
                                 Global_Table_Meta *metainfo,
                                 Global_Table_Strategy *strategy,
                                 Global_Txn_Info *txn_info,
                                 Global_Txn *txn,
                                 Global_Txn_Exec *txn_exec,
                                 Global_Txn_Result *txn_result,
                                 Global_Txn_Exec_Param *exec_param,
                                 Global_Txn_Aux_Struct *aux_struct,
                                 Global_Data_Packet *data_packet) {
#ifdef LTPMG_GPUQUERY_TRANSFER_GROUP
    uint32_t cur_group = 0;
    uint32_t dest_group = 0;
    if (device_cnt > 2) {
        cur_group = device_ID / 2;
        dest_group = dest_device / 2;
    } else if (device_cnt > 1) {
        cur_group = device_ID & 1;
        dest_group = dest_device & 1;
    }
    // if (dest_device > device_cnt / 2)
    if (cur_group != dest_group) {
        uint32_t result = atomicAdd(&aux_struct[0].data_packet_cur, 1);
        data_packet[result].mark = cur_txn;
    }
#endif

#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
    uint32_t result = atomicAdd(&aux_struct[0].data_packet_cur, 1);
    data_packet[result].mark = cur_txn;
#endif
}

__global__ void txn_executor(uint32_t device_ID,
                             uint32_t device_cnt,
                             uint32_t sub_txn_size,
                             Global_Table_Info *table_info,
                             Global_Table *tables,
                             Global_Table_Index *indexes,
                             Global_Table_Meta *metainfo,
                             Global_Table_Strategy *strategy,
                             Global_Txn_Info *txn_info,
                             Global_Txn *txn,
                             Global_Txn_Exec *txn_exec,
                             Global_Txn_Result *txn_result,
                             Global_Txn_Exec_Param *exec_param,
                             Global_Txn_Aux_Struct *aux_struct,
                             Global_Data_Packet *data_packet) {
    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t thSize = blockDim.x * gridDim.x;
#ifdef LTPMG_GPUQUERY_TRANSFER_GROUP
    uint32_t cur_group = 0;
    if (device_cnt > 2) {
        cur_group = device_ID / 2;
    } else if (device_cnt > 1) {
        cur_group = device_ID & 1;
    }
    const uint32_t start = sub_txn_size * cur_group / device_cnt;
    const uint32_t end = sub_txn_size * (cur_group + 1) / device_cnt;
#endif
#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
    const uint32_t start = sub_txn_size * device_ID / device_cnt;
    const uint32_t end = sub_txn_size * (device_ID + 1) / device_cnt;
#endif
    uint32_t cur = thID + start;
    while (cur >= start && cur < end) {
        uint32_t type = txn[cur].subtxn.type;
        if (type == 0) {
            select_executor(device_ID, cur, device_cnt, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                            txn_exec, txn_result, exec_param, aux_struct, data_packet);
        } else if (type == 1) {
            insert_executor(device_ID, cur, device_cnt, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                            txn_exec, txn_result, exec_param, aux_struct, data_packet);
        } else if (type == 2) {
            update_executor(device_ID, cur, device_cnt, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                            txn_exec, txn_result, exec_param, aux_struct, data_packet);
        } else if (type == 3) {
            delete_executor(device_ID, cur, device_cnt, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                            txn_exec, txn_result, exec_param, aux_struct, data_packet);
        } else if (type == 4) {
            scan_executor(device_ID, cur, device_cnt, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
                          txn_exec, txn_result, exec_param, aux_struct, data_packet);
        }

        cur += thSize;
    }
}

void execute_on_thread(std::shared_ptr<Param> param,
                       uint32_t cur,
                       Global_Txn *global_txn,
                       Global_Txn_Info *global_txn_info,
                       uint32_t txn_offset,
                       Global_Table *table,
                       Global_Table_Info *table_info) {
    printf("thID:%d execute,txn_offset:%d\n", cur, txn_offset);
    uint32_t global_txn_info_ID = global_txn[txn_offset].global_txn_info_ID;

    uint32_t cur_sub_txn_size = global_txn_info[global_txn_info_ID].cur_subtxn_cnt;
    for (uint32_t j = 0; j < cur_sub_txn_size; j++) {
        uint32_t type = global_txn[txn_offset].subtxn.type;
        uint32_t table_ID = global_txn[txn_offset].subtxn.table_ID;
        // uint32_t dest_device = global_txn[txn_offset].subtxn.dest_device;
        uint32_t row_1 = global_txn[txn_offset].subtxn.dest_Row_1;
        uint32_t row_2 = global_txn[txn_offset].subtxn.dest_Row_2;
#ifdef LTPMG_GPULAUNCHER_PRINT
        // std::cout << "global_txn[" << txn_offset << "].subtxn[" << j << "].type:" << global_txn[txn_offset].subtxn.
        //         type << ",dest_device:" << global_txn[txn_offset].subtxn.dest_device << std::endl;
        for (uint32_t i = 0; i < param->table_cnt; ++i) {
            std::cout << table_info[i].int_size << "," << table_info[i].string_size << "," << table_info[i].
                    double_size << std::endl;
        }
#endif
        switch (type) {
            case 0: {
                // std::cout << "select dest_device:" << dest_device << ",tableID:" << table_ID <<
                //         "," << std::endl;
                INT32 *int_data = new INT32[table_info[table_ID].int_size];
                UINT32 *string_data = new UINT32[table_info[table_ID].string_size * table_info[table_ID].string_length];
                DOUBLE *double_data = new DOUBLE[table_info[table_ID].double_size];
                for (uint32_t i = 0; i < table_info[table_ID].int_size; ++i) {
                    int_data[i] = table[table_ID].int_data[row_1 * table_info[table_ID].int_size + i];
                }
                for (uint32_t i = 0; i < table_info[table_ID].string_size; ++i) {
                    for (uint32_t k = 0; k < table_info[table_ID].string_length; ++k) {
                        string_data[i * table_info[table_ID].string_length + k] = table[table_ID].string_data[
                            row_1 * table_info[table_ID].string_size + i * table_info[table_ID].string_length + k];
                    }
                }
                for (uint32_t i = 0; i < table_info[table_ID].double_size; ++i) {
                    double_data[i] = table[table_ID].double_data[row_1 * table_info[table_ID].double_size + i];
                }
#ifdef LTPMG_GPULAUNCHER_PRINT
                std::cout << row_1 << ":";
                for (uint32_t i = 0; i < table_info[table_ID].string_size * table_info[table_ID].string_length; ++i) {
                    std::cout << std::hex << string_data[i] << " ";
                }
                std::cout << std::endl;
                std::cout << std::dec;
#endif
                delete[] int_data;
                delete[] string_data;
                delete[] double_data;
                break;
            }
            case 4: {
                // std::cout << "scan dest_device:" << global_txn[txn_offset].subtxn.dest_device << std::endl;
                uint32_t row_cnt = row_2 - row_1;
                INT32 *int_data = new INT32[table_info[table_ID].int_size * row_cnt];
                UINT32 *string_data = new UINT32[
                    table_info[table_ID].string_size * table_info[table_ID].string_length * row_cnt];
                DOUBLE *double_data = new DOUBLE[table_info[table_ID].double_size * row_cnt];
                for (uint32_t l = row_1; l < row_2; ++l) {
                    for (uint32_t i = 0; i < table_info[table_ID].int_size; ++i) {
                        int_data[i] = table[table_ID].int_data[l * table_info[table_ID].int_size + i];
                    }
                    for (uint32_t i = 0; i < table_info[table_ID].string_size; ++i) {
                        for (uint32_t k = 0; k < table_info[table_ID].string_length; ++k) {
                            string_data[i * table_info[table_ID].string_length + k] = table[table_ID].string_data[
                                l * table_info[table_ID].string_size + i * table_info[table_ID].string_length + k];
                        }
                    }
                    for (uint32_t i = 0; i < table_info[table_ID].double_size; ++i) {
                        double_data[i] = table[table_ID].double_data[l * table_info[table_ID].double_size + i];
                    }
                }
#ifdef LTPMG_GPULAUNCHER_PRINT
                std::cout << row_1 << ":";
                for (uint32_t i = 0; i < table_info[table_ID].string_size * table_info[table_ID].string_length; ++i) {
                    std::cout << std::hex << string_data[i] << " ";
                }
                std::cout << std::endl;
                std::cout << std::dec;
#endif
                delete[] int_data;
                delete[] string_data;
                delete[] double_data;
                break;
            }
            case 1: {
                // std::cout << "insert dest_device:" << global_txn[txn_offset].subtxn.dest_device << std::endl;
                INT32 *int_data = new INT32[table_info[table_ID].int_size];
                UINT32 *string_data = new UINT32[
                    table_info[table_ID].string_size * table_info[table_ID].string_length];
                DOUBLE *double_data = new DOUBLE[table_info[table_ID].double_size];
                for (uint32_t i = 0; i < table_info[table_ID].int_size; ++i) {
                    int_data[i] = table[table_ID].int_data[row_1 * table_info[table_ID].int_size + i];
                }
                for (uint32_t i = 0; i < table_info[table_ID].string_size; ++i) {
                    for (uint32_t k = 0; k < table_info[table_ID].string_length; ++k) {
                        string_data[i * table_info[table_ID].string_length + k] = table[table_ID].string_data[
                            row_1 * table_info[table_ID].string_size + i * table_info[table_ID].string_length + k];
                    }
                }
                for (uint32_t i = 0; i < table_info[table_ID].double_size; ++i) {
                    double_data[i] = table[table_ID].double_data[row_1 * table_info[table_ID].double_size + i];
                }

#ifdef LTPMG_GPULAUNCHER_PRINT
                std::cout << row_1 << ":";
                for (uint32_t i = 0; i < table_info[table_ID].string_size * table_info[table_ID].string_length; ++i) {
                    std::cout << std::hex << string_data[i] << " ";
                }
                std::cout << std::endl;
                std::cout << std::dec;
#endif
                delete[] int_data;
                delete[] string_data;
                delete[] double_data;
                break;
            }
            case 2: {
                // std::cout << "update dest_device:" << global_txn[txn_offset].subtxn.dest_device << std::endl;
                INT32 *int_data = new INT32[table_info[table_ID].int_size];
                UINT32 *string_data = new UINT32[
                    table_info[table_ID].string_size * table_info[table_ID].string_length];
                DOUBLE *double_data = new DOUBLE[table_info[table_ID].double_size];
                for (uint32_t i = 0; i < table_info[table_ID].int_size; ++i) {
                    int_data[i] = table[table_ID].int_data[row_1 * table_info[table_ID].int_size + i];
                }
                for (uint32_t i = 0; i < table_info[table_ID].string_size; ++i) {
                    for (uint32_t k = 0; k < table_info[table_ID].string_length; ++k) {
                        string_data[i * table_info[table_ID].string_length + k] = table[table_ID].string_data[
                            row_1 * table_info[table_ID].string_size + i * table_info[table_ID].string_length + k];
                    }
                }
                for (uint32_t i = 0; i < table_info[table_ID].double_size; ++i) {
                    double_data[i] = table[table_ID].double_data[row_1 * table_info[table_ID].double_size + i];
                }

#ifdef LTPMG_GPULAUNCHER_PRINT
                std::cout << row_1 << ":";
                for (uint32_t i = 0; i < table_info[table_ID].string_size * table_info[table_ID].string_length; ++i) {
                    std::cout << std::hex << string_data[i] << " ";
                }
                std::cout << std::endl;
                std::cout << std::dec;
#endif
                delete[] int_data;
                delete[] string_data;
                delete[] double_data;
                break;
            }
            case 3: {
                // std::cout << "delete dest_device:" << global_txn[txn_offset].subtxn.dest_device << std::endl;
                INT32 *int_data = new INT32[table_info[table_ID].int_size];
                UINT32 *string_data = new UINT32[
                    table_info[table_ID].string_size * table_info[table_ID].string_length];
                DOUBLE *double_data = new DOUBLE[table_info[table_ID].double_size];
                for (uint32_t i = 0; i < table_info[table_ID].int_size; ++i) {
                    int_data[i] = table[table_ID].int_data[row_1 * table_info[table_ID].int_size + i];
                }
                for (uint32_t i = 0; i < table_info[table_ID].string_size; ++i) {
                    for (uint32_t k = 0; k < table_info[table_ID].string_length; ++k) {
                        string_data[i * table_info[table_ID].string_length + k] = table[table_ID].string_data[
                            row_1 * table_info[table_ID].string_size + i * table_info[table_ID].string_length + k];
                    }
                }
                for (uint32_t i = 0; i < table_info[table_ID].double_size; ++i) {
                    double_data[i] = table[table_ID].double_data[row_1 * table_info[table_ID].double_size + i];
                }

#ifdef LTPMG_GPULAUNCHER_PRINT
                std::cout << row_1 << ":";
                for (uint32_t i = 0; i < table_info[table_ID].string_size * table_info[table_ID].string_length; ++i) {
                    std::cout << std::hex << string_data[i] << " ";
                }
                std::cout << std::endl;
                std::cout << std::dec;
#endif
                delete[] int_data;
                delete[] string_data;
                delete[] double_data;
                break;
            }
            default: {
                break;
            }
        }
    }
}

void execute_on_CPU(std::shared_ptr<Param> param,
                    GPUdatabase *gpudatabase,
                    GPUquery *gpuquery) {
    std::cout << "start execute_on_CPU" << std::endl;

    Global_Txn *global_txn = gpuquery->get_txn_for_cpu();
    Global_Txn_Info *global_txn_info_h = gpuquery->get_txn_info_for_cpu();
    // Global_Table_Strategy *strategy = gpudatabase->get_strategy_for_cpu();
    Global_Table *table = gpudatabase->get_table_for_cpu();
    Global_Table_Info *table_info = gpudatabase->get_table_info_for_cpu(); {
        TaskQueue taskQueue;

        uint32_t cur_sub_txn = 0;
        for (uint32_t i = 0; i < param->get_sub_txn_size(); i++) {
            uint32_t global_txn_info_ID = global_txn[i].global_txn_info_ID;
            uint32_t cur_sub_txn_size = global_txn_info_h[global_txn_info_ID].cur_subtxn_cnt;
            bool is_op_cpu = false;
            for (uint32_t j = 0; j < cur_sub_txn_size; j++) {
                uint32_t dest_device = global_txn[i].subtxn.dest_device;
                if (dest_device == 0xffffffff) {
                    is_op_cpu = true;
                    break;
                }
            }
            if (is_op_cpu) {
                ++cur_sub_txn;
                taskQueue.addTask([param,cur_sub_txn,global_txn,global_txn_info_h,i,table,table_info]() {
                        execute_on_thread(param, cur_sub_txn, global_txn, global_txn_info_h, i, table, table_info);
                    }
                );
            }
        }
        // uint32_t numThreads = std::thread::hardware_concurrency();
        // std::cout << "cur_sub_txn_cnt: " << cur_sub_txn << std::endl;
        if (cur_sub_txn > 0) {
            uint32_t numThreads = 16;
            ThreadPool threadPool(numThreads);
            Semaphore semephore(cur_sub_txn);

            std::thread taskProcessor(processTasks, std::ref(taskQueue), std::ref(threadPool), std::ref(semephore));

            taskProcessor.join();
        }
    }
    std::cout << "end execute_on_CPU" << std::endl;
}

void GPUlauncher::txn_kernel_launcher(std::shared_ptr<Param> param, GPUdatabase *gpudatabase,
                                      GPUquery *gpuquery) {
    std::cout << "start gpulauncher.cu GPUlauncher::txn_kernel_launcher()" << std::endl;

    cudaStream_t *streams;
    streams = new cudaStream_t[param->device_cnt];

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaStreamCreate(&streams[i]));
    }

    std::cout << "start execute" << std::endl;

    long long start_all = gpu_current_time();

    // cpu_exec = std::thread(&execute_on_CPU, param, gpudatabase, gpuquery);

    // std::cout << "param->get_sub_txn_size():" << param->get_sub_txn_size() << std::endl;

    long long start_executor = gpu_current_time();


    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        // execute
        CHECK(cudaSetDevice(param->device_IDs[i]));
        txn_executor<<<512, 512, 0, streams[i]>>>(i,
                                                  param->device_cnt,
                                                  param->get_sub_txn_size(),
                                                  gpudatabase->get_table_info(i),
                                                  gpudatabase->get_table(i),
                                                  gpudatabase->get_index(i),
                                                  gpudatabase->get_meta(i),
                                                  gpudatabase->get_strategy(i),
                                                  gpuquery->get_txn_info(i),
                                                  gpuquery->get_txn(i),
                                                  gpuquery->get_txn_exec(i),
                                                  gpuquery->get_txn_result(i),
                                                  gpuquery->get_exec_param(i),
                                                  gpuquery->get_aux_struct(i),
                                                  gpuquery->get_data_packet(i));
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }

    long long end_executor = gpu_current_time();

    // std::cout << "end execute" << std::endl;

    long long start_transfer = gpu_current_time();

    if (param->device_cnt > 1) {
        gpuquery->transfer_data_packet(param, streams);
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }

    long long end_transfer = gpu_current_time();

    // std::cout << "start merge" << std::endl;

    long long start_merge = gpu_current_time();


    if (param->device_cnt > 1) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // merge
            CHECK(cudaSetDevice(param->device_IDs[i]));
            txn_merge<<<512, 512, 0, streams[i]>>>(i,
                                                   param->device_cnt,
                                                   param->get_datapacket_size(),
                                                   gpudatabase->get_table_info(i),
                                                   gpudatabase->get_table(i),
                                                   gpudatabase->get_index(i),
                                                   gpudatabase->get_meta(i),
                                                   gpudatabase->get_strategy(i),
                                                   gpuquery->get_txn_info(i),
                                                   gpuquery->get_txn(i),
                                                   gpuquery->get_txn_exec(i),
                                                   gpuquery->get_txn_result(i),
                                                   gpuquery->get_exec_param(i),
                                                   gpuquery->get_aux_struct(i),
                                                   gpuquery->get_data_packet(i));
        }

        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }
    }
    long long end_merge = gpu_current_time();
    // std::cout << "end merge" << std::endl;

    long long start_analyse = gpu_current_time();

    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // txn_prefix_offset
            CHECK(cudaSetDevice(param->device_IDs[i]));
            txn_prefix_offset<<<512, 512, 0, streams[i]>>>(i,
                                                           param->device_cnt,
                                                           j,
                                                           gpudatabase->get_table_info(i),
                                                           gpudatabase->get_table(i),
                                                           gpudatabase->get_index(i),
                                                           gpudatabase->get_meta(i),
                                                           gpudatabase->get_strategy(i),
                                                           gpuquery->get_txn_info(i),
                                                           gpuquery->get_txn(i),
                                                           gpuquery->get_txn_exec(i),
                                                           gpuquery->get_txn_result(i),
                                                           gpuquery->get_exec_param(i),
                                                           gpuquery->get_aux_struct(i),
                                                           gpuquery->get_data_packet(i));
        }
    }
    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }

    // std::cout << "start analyse" << std::endl;
#ifdef LTPMG_GPULAUNCHER_SCAN_OPT_BITMAP_POPULAR
    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // analyse
            CHECK(cudaSetDevice(param->device_IDs[i]));
            prefix_bitmap<<<512, 512, 0, streams[i]>>>(i,
                                                       j,
                                                       param->get_benchmark_ID(),
                                                       gpudatabase->get_table_info(i),
                                                       gpudatabase->get_table(i),
                                                       gpudatabase->get_meta(i),
                                                       gpuquery->get_txn(i),
                                                       gpuquery->get_exec_param(i),
                                                       gpuquery->get_aux_struct(i),
                                                       gpuquery->get_data_packet(i));
        }
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }

    long long analyse_node0 = gpu_current_time();

#ifdef LTPMG_GPULAUNCHER_BLOCK_SCAN_OPT_SHM_BITMAP_POPULAR
    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // analyse
            CHECK(cudaSetDevice(param->device_IDs[i]));
            partSum_bitmap<<<512, 512, 0, streams[i]>>>(i,
                                                        j,
                                                        param->get_benchmark_ID(),
                                                        gpudatabase->get_table_info(i),
                                                        gpudatabase->get_table(i),
                                                        gpudatabase->get_meta(i),
                                                        gpuquery->get_txn(i),
                                                        gpuquery->get_exec_param(i),
                                                        gpuquery->get_aux_struct(i),
                                                        gpuquery->get_data_packet(i));
        }
    }
    // for (uint32_t i = 0; i < param->device_cnt; ++i) {
    //     CHECK(cudaStreamSynchronize(streams[i]));
    // }
#endif

    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // analyse
            CHECK(cudaSetDevice(param->device_IDs[i]));
            compact_bitmark<<<512, 512, 0, streams[i]>>>(i,
                                                         j,
                                                         param->get_benchmark_ID(),
                                                         gpudatabase->get_table_info(i),
                                                         gpudatabase->get_table(i),
                                                         gpudatabase->get_meta(i),
                                                         gpuquery->get_txn(i),
                                                         gpuquery->get_exec_param(i),
                                                         gpuquery->get_aux_struct(i),
                                                         gpuquery->get_data_packet(i));
        }
    }
#endif


    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long analyse_node1 = gpu_current_time();

    for (uint32_t j = 0; j < param->bitmap_table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // analyse
            CHECK(cudaSetDevice(param->device_IDs[i]));
            txn_analyze_popular<<<512, 512, 0, streams[i]>>>(i,
                                                             j,
                                                             param->get_benchmark_ID(),
                                                             gpudatabase->get_table_info(i),
                                                             gpudatabase->get_table(i),
                                                             gpudatabase->get_index(i),
                                                             gpudatabase->get_meta(i),
                                                             gpudatabase->get_strategy(i),
                                                             gpuquery->get_txn_info(i),
                                                             gpuquery->get_txn(i),
                                                             gpuquery->get_txn_exec(i),
                                                             gpuquery->get_txn_result(i),
                                                             gpuquery->get_exec_param(i),
                                                             gpuquery->get_aux_struct(i),
                                                             gpuquery->get_data_packet(i));
        }
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long analyse_node2 = gpu_current_time();

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        // analyse
        CHECK(cudaSetDevice(param->device_IDs[i]));
        txn_analyze_regular<<<512, 512, 0, streams[i]>>>(i,
                                                         param->device_cnt,
                                                         param->get_benchmark_ID(),
                                                         gpudatabase->get_table_info(i),
                                                         gpudatabase->get_table(i),
                                                         gpudatabase->get_index(i),
                                                         gpudatabase->get_meta(i),
                                                         gpudatabase->get_strategy(i),
                                                         gpuquery->get_txn_info(i),
                                                         gpuquery->get_txn(i),
                                                         gpuquery->get_txn_exec(i),
                                                         gpuquery->get_txn_result(i),
                                                         gpuquery->get_exec_param(i),
                                                         gpuquery->get_aux_struct(i),
                                                         gpuquery->get_data_packet(i));
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long end_analyse = gpu_current_time();
    // std::cout << "end analyse" << std::endl;

    // std::cout << "start commit" << std::endl;
    long long start_commit = gpu_current_time();
    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // commit
            CHECK(cudaSetDevice(param->device_IDs[i]));
            filter_commit<<<512, 512, 0, streams[i]>>>(i,
                                                       param->device_cnt,
                                                       j,
                                                       gpudatabase->get_table_info(i),
                                                       gpudatabase->get_table(i),
                                                       gpudatabase->get_index(i),
                                                       gpudatabase->get_meta(i),
                                                       gpudatabase->get_strategy(i),
                                                       gpuquery->get_txn_info(i),
                                                       gpuquery->get_txn(i),
                                                       gpuquery->get_txn_exec(i),
                                                       gpuquery->get_txn_result(i),
                                                       gpuquery->get_exec_param(i),
                                                       gpuquery->get_aux_struct(i),
                                                       gpuquery->get_data_packet(i));
        }
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long commit_node0 = gpu_current_time();

    for (uint32_t j = 0; j < param->table_cnt; ++j) {
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            // commit
            CHECK(cudaSetDevice(param->device_IDs[i]));
            txn_commit<<<512, 512, 0, streams[i]>>>(i,
                                                    param->device_cnt,
                                                    j,
                                                    gpudatabase->get_table_info(i),
                                                    gpudatabase->get_table(i),
                                                    gpudatabase->get_index(i),
                                                    gpudatabase->get_meta(i),
                                                    gpudatabase->get_strategy(i),
                                                    gpuquery->get_txn_info(i),
                                                    gpuquery->get_txn(i),
                                                    gpuquery->get_txn_exec(i),
                                                    gpuquery->get_txn_result(i),
                                                    gpuquery->get_exec_param(i),
                                                    gpuquery->get_aux_struct(i),
                                                    gpuquery->get_data_packet(i));
        }
    }

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long end_commit = gpu_current_time();

    for (uint32_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamDestroy(streams[i]));
    }

    // cpu_exec.join();

    long long end_all = gpu_current_time();

    std::cout << "end commit" << std::endl;

    delete[] streams;

    float cost_all = gpu_duration(start_all, end_all);
    float cost_gpu = gpu_duration(start_executor, end_commit);
    float cost_executor = gpu_duration(start_executor, end_executor);
    float cost_transfer = gpu_duration(start_transfer, end_transfer);
    float cost_merge = gpu_duration(start_merge, end_merge);
    float cost_analyse = gpu_duration(start_analyse, end_analyse);
    float cost_commit = gpu_duration(start_commit, end_commit);
    param->result.cost = cost_gpu;
    // std::cout<<"param->result.cost: "<<param->result.cost<<std::endl;
    std::cout << "cost_all: " << cost_all << " s." << std::endl;
    std::cout << "cost_gpu: " << cost_gpu << " s." << std::endl;
    std::cout << "cost_executor: " << cost_executor << " s." << std::endl;
    std::cout << "cost_transfer: " << cost_transfer << " s." << std::endl;
    std::cout << "cost_merge: " << cost_merge << " s." << std::endl;
    std::cout << "cost_analyse: " << cost_analyse << " s." << std::endl;
    std::cout << "cost_commit: " << cost_commit << " s." << std::endl;
    // float node0 = gpu_duration(start_analyse, analyse_node0);
    // float node1 = gpu_duration(analyse_node0, analyse_node1);
    float node2 = gpu_duration(analyse_node1, analyse_node2);
    float node3 = gpu_duration(analyse_node2, end_analyse);
    // std::cout << "analyse_node0: " << node0 << std::endl;
    // std::cout << "analyse_node1: " << node1 << std::endl;
    std::cout << "analyse_node2: " << node2 << std::endl;
    std::cout << "analyse_node3: " << node3 << std::endl;
    float node4 = gpu_duration(start_commit, commit_node0);
    float node5 = gpu_duration(commit_node0, end_commit);
    std::cout << "commit_node0: " << node4 << std::endl;
    std::cout << "commit_node1: " << node5 << std::endl;
    std::cout << "TPS: " << param->batch_size / cost_all << " ." << std::endl;
    std::cout << "TPS: " << param->batch_size / cost_gpu << " ." << std::endl;
#ifdef LTPMG_GPUQUERY_TRANSFER_GROUP
    std::cout << "Transfer data size: " << sizeof(Global_Data_Packet) * param->get_datapacket_size()/2 *
            (param->device_cnt) / (1 << 20) << " MB." << std::endl;
    std::cout << "Bandwidth: " <<
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2 * (param->device_cnt) /
            (cost_transfer * (1 << 30)) << " GB/s." << std::endl;
#endif

#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
    std::cout << "Transfer data size: " << sizeof(Global_Data_Packet) * param->get_datapacket_size() *
            (param->device_cnt-1) / (1 << 20) << " MB." << std::endl;
    std::cout << "Bandwidth: " <<
            sizeof(Global_Data_Packet) * param->get_datapacket_size() * (param->device_cnt-1) /
            (cost_transfer * (1 << 30)) << " GB/s." << std::endl;
#endif
    std::cout << "end gpulauncher.cu GPUlauncher::txn_kernel_launcher()" << std::endl;
}

__global__ void mergeSort(uint32_t *unsorted, uint32_t *sorted, uint32_t arrSize) {
    const uint32_t laneID = threadIdx.x % 32;
    __syncwarp();
    for (uint32_t i = 2; i <= (1 << 32 - __clz(arrSize)); i *= 2) {
        for (uint32_t start = i * laneID; start < arrSize; start += i * 32) {
            uint32_t size = i;
            if (start + size >= arrSize) {
                size = arrSize - start;
            }
            if (start < arrSize) {
                uint32_t sub_size = i >> 1;
                uint32_t offset = start;
                uint32_t left = start;
                uint32_t right = start + sub_size;
                while (left < start + sub_size && right < start + size) {
                    if (unsorted[left] < unsorted[right]) {
                        sorted[offset] = unsorted[left];
                        ++left;
                    } else {
                        sorted[offset] = unsorted[right];
                        ++right;
                    }
                    ++offset;
                }
                while (left < start + sub_size) {
                    sorted[offset] = unsorted[left];
                    ++left;
                    ++offset;
                }
                while (right < start + size) {
                    sorted[offset] = unsorted[right];
                    ++right;
                    ++offset;
                }
            }
        }
        for (uint32_t j = laneID; j < arrSize; j += 32) {
            unsorted[j] = sorted[j];
        }
        __syncwarp();
    }
    __syncwarp();
}

__global__ void partSum(uint32_t *array_d,
                        uint32_t *sorted_array_d,
                        uint32_t seg_Size,
                        uint32_t slice_size) {
    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t thSize = blockDim.x * gridDim.x;
    const uint32_t wpID = threadIdx.x >> 5;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t SHM_start = threadIdx.x & 0xffffffe0;

    __shared__ uint32_t bitmap_tmp[512];
    __shared__ uint32_t partSum[32];
    __shared__ uint32_t block_sum[1];
    for (uint32_t i = blockIdx.x; i < slice_size; i += gridDim.x) {
        if (threadIdx.x == 0) {
            block_sum[0] = 0;
        }
        __syncthreads();
        const uint32_t bitmap_start = i * seg_Size;
        for (uint32_t j = threadIdx.x; j < seg_Size; j += blockDim.x) {
            const uint32_t cur_block_sum = block_sum[0];
            if (wpID == 0) {
                partSum[laneID] = 0;
            }
            if (j < seg_Size) {
                bitmap_tmp[threadIdx.x] = array_d[bitmap_start + j];
            } else {
                bitmap_tmp[threadIdx.x] = 0;
            }
            __syncwarp();
            uint32_t tmp_res = bitmap_tmp[threadIdx.x];
            for (uint32_t k = 16; k > 0; k >>= 1) {
                uint32_t remote = __shfl_up_sync(0xffffffff, tmp_res, k);
                if (laneID >= k) {
                    tmp_res += remote;
                }
            }
            if (laneID == 31) {
                partSum[wpID] = tmp_res;
                printf("partSum[%d]:%d\n", wpID, partSum[wpID]);
            }
            __syncthreads();
            if (wpID == 0) {
                uint32_t tmp_part_res = partSum[laneID];
                // printf("laneID:%d,tmp_part_res:%d\n", laneID, tmp_part_res);
                for (uint32_t k = 16; k > 0; k >>= 1) {
                    uint32_t remote = __shfl_up_sync(0xffffffff, tmp_part_res, k);
                    if (laneID >= k) {
                        tmp_part_res += remote;
                    }
                }
                // printf("laneID:%d,tmp_part_res:%d\n", laneID, tmp_part_res);
                if (laneID == 31) {
                    block_sum[0] = tmp_part_res;
                    // printf("tmp_part_res:%d\n", tmp_part_res);
                }
                partSum[laneID] = tmp_part_res - partSum[laneID];
            }
            __syncthreads();
            tmp_res = tmp_res + partSum[wpID] + cur_block_sum - bitmap_tmp[threadIdx.x];
            if (j < seg_Size) {
                sorted_array_d[bitmap_start + j] = tmp_res;
            } else {
                sorted_array_d[bitmap_start + j] = 0;
            }
        }
        __syncthreads();
    }
}

void GPUlauncher::txn_executor_launcher(std::shared_ptr<Param> param, GPUdatabase *gpudatabase,
                                        GPUquery *gpuquery) {
    std::cout << "start gpulauncher.cu GPUlauncher::txn_executor_launcher()" << std::endl;
    txn_kernel_launcher(param, gpudatabase, gpuquery);
    std::cout << "end gpulauncher.cu GPUlauncher::txn_executor_launcher()" << std::endl;
#ifdef LTPMG_GPULAUNCHER_TEST_MERGESORT
    CHECK(cudaSetDevice(0));
    uint32_t array_size = 1000000;
    uint32_t h_unsorted[array_size];
    for (uint32_t i = 0; i < array_size; ++i) {
        h_unsorted[i] = array_size - i;
    }
    uint32_t h_sorted[array_size];
    uint32_t *d_unsorted;
    uint32_t *d_sorted;
    CHECK(cudaMalloc((void**)&d_unsorted, sizeof(uint32_t) * array_size));
    CHECK(cudaMalloc((void**)&d_sorted, sizeof(uint32_t) * array_size));
    CHECK(cudaMemset(d_unsorted, 0, sizeof(uint32_t) * array_size));
    CHECK(cudaMemset(d_sorted, 0, sizeof(uint32_t) * array_size));
    CHECK(cudaMemcpy(d_unsorted,h_unsorted,sizeof(uint32_t)*array_size,cudaMemcpyHostToDevice));
    mergeSort<<<1,32>>>(d_unsorted, d_sorted, array_size);
    CHECK(cudaDeviceSynchronize());
    CHECK(cudaMemcpy(h_sorted,d_sorted ,sizeof(uint32_t) * array_size,cudaMemcpyDeviceToHost));
    for (uint32_t i = 0; i < array_size; ++i) {
        if (h_sorted[i] != i + 1) {
            std::cout << i << "," << h_sorted[i] << std::endl;
        }
        // std::cout << i << "," << h_sorted[i] << std::endl;
    }
    std::cout << std::endl;
    CHECK(cudaFree(d_unsorted));
    CHECK(cudaFree(d_sorted));
#endif


#ifdef LTPMG_GPULAUNCHER_TEST_PREFIXSUM
    CHECK(cudaSetDevice(0));
    cudaStream_t stream;
    CHECK(cudaStreamCreate(&stream));
    uint32_t array_size = 128;
    uint32_t segment_size = 64;
    uint32_t *h_unsorted;
    uint32_t *cpu_result;
    CHECK(cudaHostAlloc((void**)&h_unsorted,sizeof(uint32_t)*array_size,cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void**)&cpu_result,sizeof(uint32_t)*array_size,cudaHostAllocDefault));
    uint32_t tmp = 0;
    for (uint32_t i = 0; i < array_size; ++i) {
        h_unsorted[i] = i % segment_size;
    }
    for (uint32_t i = 0; i < array_size; ++i) {
        if (i % segment_size == 0) {
            tmp = 0;
        }
        tmp += h_unsorted[i];
        cpu_result[i] = tmp;
    }
    for (uint32_t i = 0; i < array_size; ++i) {
        std::cout << h_unsorted[i] << " ";
    }
    std::cout << std::endl;
    for (uint32_t i = 0; i < array_size; ++i) {
        cpu_result[i] -= h_unsorted[i];
        std::cout << cpu_result[i] << " ";
    }
    std::cout << std::endl;
    uint32_t *h_sorted;
    uint32_t *d_unsorted;
    uint32_t *d_sorted;
    CHECK(cudaHostAlloc((void**)&h_sorted,sizeof(uint32_t)*array_size,cudaHostAllocDefault));
    CHECK(cudaMalloc((void**)&d_unsorted, sizeof(uint32_t) * array_size));
    CHECK(cudaMalloc((void**)&d_sorted, sizeof(uint32_t) * array_size));
    CHECK(cudaMemsetAsync(d_sorted, 0, sizeof(uint32_t) * array_size,stream));
    CHECK(cudaMemcpyAsync(d_unsorted,h_unsorted,sizeof(uint32_t)*array_size,cudaMemcpyHostToDevice,stream));
    partSum<<<1,32,0,stream>>>(d_unsorted, d_sorted, segment_size, array_size / segment_size);
    // CHECK(cudaStreamSynchronize(stream));
    CHECK(cudaMemcpyAsync(h_sorted,d_sorted ,sizeof(uint32_t) * array_size,cudaMemcpyDeviceToHost,stream));
    for (uint32_t i = 0; i < array_size; ++i) {
        // if (h_sorted[i] != i + 1) {
        //     std::cout << i << "," << h_sorted[i] << std::endl;
        // }
        std::cout << h_sorted[i] << " ";
    }
    std::cout << std::endl;
    CHECK(cudaFree(d_unsorted));
    CHECK(cudaFree(d_sorted));
    CHECK(cudaFreeHost(h_sorted));
    CHECK(cudaFreeHost(cpu_result));
    CHECK(cudaFreeHost(h_unsorted));
    CHECK(cudaStreamDestroy(stream));
#endif


#ifdef LTPMG_GPULAUNCHER_TEST_MEMCPYP2P

    for (uint32_t bitsize = 20; bitsize < 28; ++bitsize) {
        cudaStream_t *streams = new cudaStream_t[param->device_cnt];
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(i));
            CHECK(cudaStreamCreate(&streams[i]));
        }
        uint32_t array_size = 1 << bitsize;
        uint32_t flag = 2;
        uint32_t *array_0 = new uint32_t[array_size];
        uint32_t *array_1 = new uint32_t[array_size];
        uint32_t *array_2 = new uint32_t[array_size];
        uint32_t *array_3 = new uint32_t[array_size];

        memset(array_0, 0x00, sizeof(uint32_t) * array_size);
        memset(array_1, 0xff, sizeof(uint32_t) * array_size);
        memset(array_2, 0x0f, sizeof(uint32_t) * array_size);
        memset(array_3, 0xf0, sizeof(uint32_t) * array_size);
        uint32_t *array_0_d;
        uint32_t *array_1_d;
        uint32_t *array_2_d;
        uint32_t *array_3_d;

        CHECK(cudaSetDevice(param->device_IDs[0]));
        CHECK(cudaMalloc((void**)&array_0_d, sizeof(uint32_t) * array_size));
        CHECK(cudaSetDevice(param->device_IDs[1]));
        CHECK(cudaMalloc((void**)&array_1_d, sizeof(uint32_t) * array_size));
        CHECK(cudaSetDevice(param->device_IDs[2]));
        CHECK(cudaMalloc((void**)&array_2_d, sizeof(uint32_t) * array_size));
        CHECK(cudaSetDevice(param->device_IDs[3]));
        CHECK(cudaMalloc((void**)&array_3_d, sizeof(uint32_t) * array_size));

        CHECK(cudaSetDevice(param->device_IDs[0]));
        CHECK(cudaMemcpyAsync(array_0_d,array_0,sizeof(uint32_t) * array_size,cudaMemcpyHostToDevice,streams[0]));
        CHECK(cudaSetDevice(param->device_IDs[1]));
        CHECK(cudaMemcpyAsync(array_1_d,array_1,sizeof(uint32_t) * array_size,cudaMemcpyHostToDevice,streams[1]));
        CHECK(cudaSetDevice(param->device_IDs[2]));
        CHECK(cudaMemcpyAsync(array_2_d,array_2,sizeof(uint32_t) * array_size,cudaMemcpyHostToDevice,streams[2]));
        CHECK(cudaSetDevice(param->device_IDs[3]));
        CHECK(cudaMemcpyAsync(array_3_d,array_3,sizeof(uint32_t) * array_size,cudaMemcpyHostToDevice,streams[3]));
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }

        long long start = gpu_current_time();
        if (flag == 2) {
            CHECK(cudaMemcpyPeerAsync(array_0_d+array_size/2,param->device_IDs[0],
                array_1_d,param->device_IDs[1],
                array_size/2,streams[1]));

            CHECK(cudaMemcpyPeerAsync(array_1_d+array_size/2,param->device_IDs[1],
                array_0_d,param->device_IDs[0],
                array_size/2,streams[0]));
        } else if (flag == 4) {
            // CHECK(cudaMemcpyPeerAsync(array_0_d+array_size/4,param->device_IDs[0],
            //     array_1_d,param->device_IDs[1],
            //     array_size/4,streams[1]));
            //
            // CHECK(cudaMemcpyPeerAsync(array_1_d+array_size/4,param->device_IDs[1],
            //     array_0_d,param->device_IDs[0],
            //     array_size/4,streams[0]));
            //
            // CHECK(cudaMemcpyPeerAsync(array_2_d+array_size/4,param->device_IDs[2],
            //     array_3_d,param->device_IDs[3],
            //     array_size/4,streams[3]));
            //
            // CHECK(cudaMemcpyPeerAsync(array_3_d+array_size/4,param->device_IDs[3],
            //     array_2_d,param->device_IDs[2],
            //     array_size/4,streams[2]));
            //
            // for (uint32_t i = 0; i < param->device_cnt; ++i) {
            //     CHECK(cudaStreamSynchronize(streams[i]));
            // }

            CHECK(cudaMemcpyPeerAsync(array_2_d+array_size/2,param->device_IDs[2],
                array_0_d,param->device_IDs[0],
                array_size/2,streams[0]));

            CHECK(cudaMemcpyPeerAsync(array_3_d+array_size/2,param->device_IDs[3],
                array_1_d,param->device_IDs[1],
                array_size/2,streams[1]));

            CHECK(cudaMemcpyPeerAsync(array_0_d+array_size/2,param->device_IDs[0],
                array_2_d,param->device_IDs[2],
                array_size/2,streams[2]));

            CHECK(cudaMemcpyPeerAsync(array_1_d+array_size/2,param->device_IDs[1],
                array_3_d,param->device_IDs[3],
                array_size/2,streams[3]));
        }

        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }

        long long end = gpu_current_time();
        float cost = gpu_duration(start, end);
        float size = 0.0;
        if (flag == 2) {
            size = (float) 1 * array_size * sizeof(uint32_t) / (1 << 30);
        } else if (flag == 4) {
            size = (float) 2 * array_size * sizeof(uint32_t) / (1 << 30);
        }
        std::cout << "array_size:" << (float) array_size / (1 << 20) << " MB." << std::endl;
        std::cout << "cost:" << cost << " s." << std::endl;
        std::cout << "size:" << size << " GB." << std::endl;
        std::cout << "bandwidth:" << size / cost << " GB/s." << std::endl;;
        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamDestroy(streams[i]));
        }
        delete[] streams;

        CHECK(cudaFree(array_0_d));
        CHECK(cudaFree(array_1_d));
        CHECK(cudaFree(array_2_d));
        CHECK(cudaFree(array_3_d));
        delete[] array_0;
        delete[] array_1;
        delete[] array_2;
        delete[] array_3;
    }
#endif
}

__device__ void merge(uint32_t device_ID,
                      uint32_t cur_datapacket,
                      Global_Table_Info *table_info,
                      Global_Table *tables,
                      Global_Table_Index *indexes,
                      Global_Table_Meta *metainfo,
                      Global_Table_Strategy *strategy,
                      Global_Txn_Info *txn_info,
                      Global_Txn *txn,
                      Global_Txn_Exec *txn_exec,
                      Global_Txn_Result *txn_result,
                      Global_Txn_Exec_Param *exec_param,
                      Global_Txn_Aux_Struct *aux_struct,
                      Global_Data_Packet *data_packet) {
    uint32_t cur_mark = data_packet[cur_datapacket].mark;
    if (cur_mark != 0xffffffff) {
        uint32_t cur_txn = cur_mark;
        uint32_t type = txn[cur_txn].subtxn.type;
        uint32_t tid = txn[cur_txn].subtxn.TID;
        uint32_t tableID = txn[cur_txn].subtxn.table_ID;
        uint32_t row_1 = txn[cur_txn].subtxn.dest_Row_1;
        uint32_t row_2 = txn[cur_txn].subtxn.dest_Row_2;
        uint32_t ispopular = txn[cur_txn].subtxn.ispopular;
        uint32_t row_start = metainfo[tableID].row_start;
        uint32_t row_end = metainfo[tableID].row_end;
        uint32_t dest_device = txn[cur_txn].subtxn.dest_device;
        if (type == 0) {
            if (dest_device == device_ID) {
                if (ispopular == 0 || ispopular == 1) {
                    register_txn_exec(device_ID, type, cur_txn, txn_exec);

                    register_cc(cur_txn, ispopular, tableID, row_1, tid,
                                table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                                txn_result, exec_param, aux_struct, data_packet);
                }
            }
        } else if (type == 1) {
            if (dest_device == device_ID) {
                register_txn_exec(device_ID, type, cur_txn, txn_exec);

                register_cc(cur_txn, ispopular, tableID, row_1, tid,
                            table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                            txn_result, exec_param, aux_struct, data_packet);
            }
        } else if (type == 2) {
            if (dest_device == device_ID) {
                register_txn_exec(device_ID, type, cur_txn, txn_exec);

                register_cc(cur_txn, ispopular, tableID, row_1, tid,
                            table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                            txn_result, exec_param, aux_struct, data_packet);
            }
        } else if (type == 4) {
            bool contain_local = false;
            for (uint32_t row = row_1; row < row_2; ++row) {
                if (row >= row_start && row < row_end) {
                    contain_local = true;
                    register_cc(cur_txn, ispopular, tableID, row, tid,
                                table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                                txn_result, exec_param, aux_struct, data_packet);
                }
            }
            if (contain_local) {
                register_txn_exec(device_ID, type, cur_txn, txn_exec);
            }
        } else if (type == 3) {
            if (dest_device == device_ID) {
                register_txn_exec(device_ID, type, cur_txn, txn_exec);

                register_cc(cur_txn, ispopular, tableID, row_1, tid,
                            table_info, tables, indexes, metainfo, strategy, txn_info, txn, txn_exec,
                            txn_result, exec_param, aux_struct, data_packet);
            }
        }
    }
}

__global__ void txn_merge(uint32_t device_ID,
                          uint32_t device_cnt,
                          uint32_t sub_txn_size,
                          Global_Table_Info *table_info,
                          Global_Table *tables,
                          Global_Table_Index *indexes,
                          Global_Table_Meta *metainfo,
                          Global_Table_Strategy *strategy,
                          Global_Txn_Info *txn_info,
                          Global_Txn *txn,
                          Global_Txn_Exec *txn_exec,
                          Global_Txn_Result *txn_result,
                          Global_Txn_Exec_Param *exec_param,
                          Global_Txn_Aux_Struct *aux_struct,
                          Global_Data_Packet *data_packet) {
    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t thSize = blockDim.x * gridDim.x;
    const uint32_t size = sub_txn_size; // * device_cnt;
#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
    uint32_t cur = thID + sub_txn_size / device_cnt;
#endif

#ifdef LTPMG_GPUQUERY_TRANSFER_GROUP
    uint32_t cur = thID + sub_txn_size / 2;
#endif
    while (cur < size) {
        merge(device_ID, cur, table_info, tables, indexes, metainfo, strategy, txn_info, txn,
              txn_exec, txn_result, exec_param, aux_struct, data_packet);
        cur += thSize;
    }
}

__global__ void txn_prefix_offset(uint32_t device_ID,
                                  uint32_t device_cnt,
                                  uint32_t tableID,
                                  Global_Table_Info *table_info,
                                  Global_Table *tables,
                                  Global_Table_Index *indexes,
                                  Global_Table_Meta *metainfo,
                                  Global_Table_Strategy *strategy,
                                  Global_Txn_Info *txn_info,
                                  Global_Txn *txn,
                                  Global_Txn_Exec *txn_exec,
                                  Global_Txn_Result *txn_result,
                                  Global_Txn_Exec_Param *exec_param,
                                  Global_Txn_Aux_Struct *aux_struct,
                                  Global_Data_Packet *data_packet) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    uint32_t size = __ldg(&metainfo[tableID].table_slice_size);

    while (cur < size) {
        uint32_t cur_cnt = __ldg(&aux_struct[tableID].cnt_TID[cur]);
        if (cur_cnt > 0) {
            uint32_t start_offset = atomicAdd(&aux_struct[tableID].used_rows_cnt, 1);
            // printf("start_offset:%d\n", start_offset);
            aux_struct[tableID].used_rows[start_offset] = cur;
            start_offset = atomicAdd(&aux_struct[tableID].mark_TID_start_offset, cur_cnt);
            aux_struct[tableID].mark_TID_offset[cur] = start_offset;
        }
        cur += thSize;
    }
}

// TODO: 加入popular handle的压缩bitmap优化

__global__ void prefix_bitmap(uint32_t device_ID,
                              uint32_t table_ID,
                              uint32_t benchmark,
                              Global_Table_Info *table_info,
                              Global_Table *tables,
                              Global_Table_Meta *metainfo,
                              Global_Txn *txn,
                              Global_Txn_Exec_Param *exec_param,
                              Global_Txn_Aux_Struct *aux_struct,
                              Global_Data_Packet *data_packet) {
    if (benchmark == 1 || benchmark == 4) {
    } else if (benchmark == 2 || benchmark == 3) {
        if (table_ID == 0 || table_ID == 1) {
        } else {
            return;
        }
    }
    // __shared__ uint32_t SHM_bitmap[512];
    // __shared__ uint32_t SHM_warp_max[16];
    // __shared__ uint32_t SHM_warp_row[16];
    // __shared__ uint32_t SHM_tmp_res[512];
    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t thSize = blockDim.x * gridDim.x;
    const uint32_t wpID = thID >> 5;
    const uint32_t wpSize = thSize >> 5;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t bitmap_mark_size = exec_param[0].bitmap_size * 32;
    const uint32_t table_slice_size = metainfo[table_ID].bitmap_row_slice_size;

    uint32_t tmp_res = 0;
    uint32_t cur_bit = 0;
    for (uint32_t i = wpID;
         i < (table_slice_size * bitmap_mark_size) >> 5;
         i += wpSize) {
        //scan warp
        // cur_bit = aux_struct[table_ID].bitmap[i];
        // tmp_res = cur_bit && (1 << laneID);
        cur_bit = aux_struct[table_ID].bitmap_mark[i * 32 + laneID];
        tmp_res = cur_bit != 0 ? 1 : 0;
        if (tmp_res > 0) {
            cooperative_groups::coalesced_group active = cooperative_groups::coalesced_threads();
            tmp_res = active.thread_rank();
            if (active.thread_rank() == active.num_threads() - 1) {
                aux_struct[table_ID].bitmap_tmp[i] = tmp_res;
            }
        }
        __syncwarp();
        aux_struct[table_ID].bitmap_mark_offset[i * 32 + laneID] = tmp_res; // - (cur_bit && (1 << laneID));
    }
}

__global__ void partSum_bitmap(uint32_t device_ID,
                               uint32_t table_ID,
                               uint32_t benchmark,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Meta *metainfo,
                               Global_Txn *txn,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet) {
    if (benchmark == 1 || benchmark == 4) {
    } else if (benchmark == 2 || benchmark == 3) {
        if (table_ID == 0 || table_ID == 1) {
        } else {
            return;
        }
    }
    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t thSize = blockDim.x * gridDim.x;
    const uint32_t wpID = threadIdx.x >> 5;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t SHM_start = threadIdx.x & 0xffffffe0;
    const uint32_t bitmap_size = exec_param[0].bitmap_size;
    const uint32_t table_slice_size = metainfo[table_ID].bitmap_row_slice_size;
    __shared__ uint32_t bitmap_tmp[512];
    __shared__ uint32_t partSum[32];
    __shared__ uint32_t block_sum[1];

    for (uint32_t i = blockIdx.x; i < table_slice_size; i += gridDim.x) {
        if (threadIdx.x == 0) {
            block_sum[0] = 0;
        }
        __syncthreads();
        const uint32_t bitmap_start = i * bitmap_size;
        if (aux_struct[table_ID].bitmap_used_size[i] > 0) {
            for (uint32_t j = threadIdx.x; j < bitmap_size; j += blockDim.x) {
                const uint32_t cur_block_sum = block_sum[0];
                if (wpID == 0) {
                    partSum[laneID] = 0;
                }
                if (j < bitmap_size) {
                    bitmap_tmp[threadIdx.x] = aux_struct[table_ID].bitmap_tmp[bitmap_start + j];
                } else {
                    bitmap_tmp[threadIdx.x] = 0;
                }
                __syncwarp();
                uint32_t tmp_res = bitmap_tmp[threadIdx.x];
                for (uint32_t k = 16; k > 0; k >>= 1) {
                    uint32_t remote = __shfl_up_sync(0xffffffff, tmp_res, k);
                    if (laneID >= k) {
                        tmp_res += remote;
                    }
                }
                if (laneID == 31) {
                    partSum[wpID] = tmp_res;
                }
                __syncthreads();
                if (wpID == 0) {
                    uint32_t tmp_part_res = partSum[laneID];
                    for (uint32_t k = 16; k > 0; k >>= 1) {
                        uint32_t remote = __shfl_up_sync(0xffffffff, tmp_part_res, k);
                        if (laneID >= k) {
                            tmp_part_res += remote;
                        }
                    }
                    if (laneID == 31) {
                        block_sum[0] = tmp_part_res;
                    }
                    __syncwarp();
                    partSum[laneID] = tmp_part_res - partSum[laneID];
                }
                __syncthreads();
                tmp_res = tmp_res + partSum[wpID] + cur_block_sum - bitmap_tmp[threadIdx.x];
                if (j < bitmap_size) {
                    aux_struct[table_ID].bitmap_tmp[bitmap_start + j] = tmp_res;
                }
            }
        }
        __syncthreads();
    }
}

__global__ void compact_bitmark(uint32_t device_ID,
                                uint32_t table_ID,
                                uint32_t benchmark,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Meta *metainfo,
                                Global_Txn *txn,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    if (benchmark == 1 || benchmark == 4) {
    } else if (benchmark == 2 || benchmark == 3) {
        if (table_ID == 0 || table_ID == 1) {
        } else {
            return;
        }
    }

    const uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t thSize = blockDim.x * gridDim.x;
    const uint32_t bitmap_size = exec_param[0].bitmap_size;
    const uint32_t bitmap_mark_size = exec_param[0].bitmap_size * 32;
    const uint32_t table_slice_size = metainfo[table_ID].bitmap_row_slice_size;
    __shared__ uint32_t SHM_part_res[512];
    uint32_t cur_row = 0;
    uint32_t cur_start = 0;
    uint32_t cur_ID = 0;
    uint32_t bitmap_mark_offset = 0;
    for (uint32_t i = thID; i < bitmap_mark_size * table_slice_size; i += thSize) {
        cur_row = i / bitmap_mark_size;
        cur_start = cur_row * bitmap_size;
        cur_ID = i >> 5;
        bitmap_mark_offset = __ldg(&aux_struct[table_ID].bitmap_mark_offset[i]);
        bitmap_mark_offset += bitmap_mark_size * cur_row;
#ifndef LTPMG_GPULAUNCHER_BLOCK_SCAN_OPT_SHM_BITMAP_POPULAR
        for (uint32_t k = cur_start; k < cur_ID; k += 32) {
            if (k + laneID < cur_ID) {
                SHM_part_res[threadIdx.x] = aux_struct[table_ID].bitmap_tmp[k + laneID];
            } else {
                SHM_part_res[threadIdx.x] = 0;
            }
            __syncwarp();
            uint32_t part_sum = SHM_part_res[threadIdx.x];
            for (uint32_t j = 16; j > 0; j >>= 1) {
                part_sum += __shfl_up_sync(0xffffffff, part_sum, j);
            }
            part_sum = __shfl_sync(0xffffffff, part_sum, 31);
            bitmap_mark_offset += part_sum;
        }
#endif

#ifdef LTPMG_GPULAUNCHER_BLOCK_SCAN_OPT_SHM_BITMAP_POPULAR
        bitmap_mark_offset += __ldg(&aux_struct[table_ID].bitmap_tmp[cur_ID]);
#endif

#ifndef LTPMG_GPULAUNCHER_SCAN_OPT_SHM_BITMAP_POPULAR
        for (uint32_t j=cur_start;j < cur_ID; ++j) {
            bitmap_mark_offset +=aux_struct[table_ID].bitmap_tmp[j];
        }
#endif

        if (aux_struct[table_ID].bitmap_mark[i] != 0) {
            aux_struct[table_ID].bitmap_mark_compressed[bitmap_mark_offset] =
                    __ldg(&aux_struct[table_ID].bitmap_mark[i]);
        }
    }
}

__device__ void popular_handler(uint32_t device_ID,
                                uint32_t table_ID,
                                uint32_t row,
                                uint32_t cur_bitmap,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Meta *metainfo,
                                Global_Txn *txn,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet) {
    const uint32_t warpID = threadIdx.x >> 5;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t bitmap_size = exec_param[0].bitmap_size;
    const uint32_t cur_row = row % metainfo[table_ID].table_slice_size;
    const uint32_t bitmark_start = bitmap_size * 32 * cur_row;
    __shared__ INT32 SHM_INT32_data[16 * 16];
    __shared__ UINT32 SHM_STRING_data[16 * 8 * 16];
    __shared__ DOUBLE SHM_DOUBLE_data[16 * 16];
    for (uint32_t i = laneID;
         i < __ldg(&table_info[table_ID].int_size);
         i += 32) {
        SHM_INT32_data[(threadIdx.x >> 5) + i] =
                tables[table_ID].int_data[cur_row * __ldg(&table_info[table_ID].int_size) + i];
    }
    for (uint32_t i = laneID;
         i < __ldg(&table_info[table_ID].string_size) * __ldg(&table_info[table_ID].string_length);
         i += 32) {
        SHM_STRING_data[(threadIdx.x >> 5) + i] =
                tables[table_ID].string_data[cur_row *
                                             __ldg(&table_info[table_ID].string_size) *
                                             __ldg(&table_info[table_ID].string_length) +
                                             i];
    }
    for (uint32_t i = laneID;
         i < __ldg(&table_info[table_ID].double_size);
         i += 32) {
        SHM_DOUBLE_data[(threadIdx.x >> 5) + i] =
                tables[table_ID].double_data[cur_row *
                                             __ldg(&table_info[table_ID].double_size) +
                                             i];
    }
    __syncwarp();

#ifdef LTPMG_GPULAUNCHER_SCAN_BITMAP_POPULAR
    __shared__ uint32_t SHM_bitmap[32];

    for (uint32_t b = 0; b < bitmap_size; ++b) {
        if (laneID == 0) {
            SHM_bitmap[warpID] = aux_struct[table_ID].bitmap[b + bitmap_size * cur_row];
        }
        __syncwarp();
        if (SHM_bitmap[warpID] == 0) {
            continue;
        }
        uint32_t result = SHM_bitmap[warpID] & (1 << laneID);

        // SHM_bitmark[threadIdx.x] = aux_struct[table_ID].bitmap_mark[bitmark_start + b * 32 + laneID];

#pragma unroll
        for (uint32_t curlane = 0; curlane < 32; ++curlane) {
            uint32_t cur_res = __shfl_sync(0xffffffff, result, curlane);
            __syncwarp();
            // uint32_t mark = SHM_bitmark[threadIdx.x & 0xffffffe0 + curlane];
            if (cur_res != 0) {
                uint32_t mark = aux_struct[table_ID].bitmap_mark[bitmark_start + b * 32 + curlane];
                uint32_t type = txn[mark].subtxn.type;

                if (type == 0) {
                    // select
                    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                        // select_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                        select_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                    }
                    for (uint32_t i = laneID;
                         i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                        // select_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                        select_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                    }
                    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                        // select_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                        select_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                    }
                } else if (type == 4) {
                    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                        // scan_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                        scan_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                    }
                    for (uint32_t i = laneID;
                         i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                        // scan_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                        scan_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                    }
                    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                        // scan_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                        scan_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                    }
                } else if (type == 2) {
                    // update
                    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                        // update_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                        update_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                    }
                    for (uint32_t i = laneID;
                         i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                        // update_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                        update_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                    }
                    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                        // update_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                        update_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                    }
                } else if (type == 3) {
                    // delete
                    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                        // delete_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                        delete_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                    }
                    for (uint32_t i = laneID;
                         i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                        // delete_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                        delete_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                    }
                    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                        // delete_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                        delete_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                    }
                } else if (type == 1) {
                    // insert
                    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                        // insert_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                        insert_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                    }
                    for (uint32_t i = laneID;
                         i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                        // insert_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                        insert_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                    }
                    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                        // insert_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                        insert_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                    }
                }
            }
            __syncwarp();
        }
    }
#endif

#ifdef LTPMG_GPULAUNCHER_SCAN_OPT_BITMAP_POPULAR
    __shared__ uint32_t SHM_mark[512];
    const uint32_t SHM_mark_offset_start = threadIdx.x & 0xffffffe0;
    const uint32_t bitmap_used_size = aux_struct[table_ID].bitmap_used_size[cur_row];
    for (uint32_t b = 0; b < bitmap_used_size; b += 32) {
        // uint32_t mark = aux_struct[table_ID].bitmap_mark_compressed[bitmark_start + b];
        if (b + laneID < bitmap_used_size) {
            SHM_mark[threadIdx.x] = aux_struct[table_ID].bitmap_mark_compressed[bitmark_start + b + laneID];
        } else {
            SHM_mark[threadIdx.x] = 0;
        }
        __syncwarp();

        for (uint32_t j = 0; j < 32 && SHM_mark[SHM_mark_offset_start + j] != 0; ++j) {
            uint32_t mark = SHM_mark[SHM_mark_offset_start + j];
            // if (laneID == 0 && mark > 0) {
            //     printf("mark:%d\n", mark);
            // }
            // continue;
            uint32_t type = txn[mark].subtxn.type;

            if (type == 0) {
                // select
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // select_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    select_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // select_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    select_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // select_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    select_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 4) {
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // scan_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    scan_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // scan_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    scan_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // scan_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    scan_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 2) {
                // update
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // update_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                    update_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // update_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                    update_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // update_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                    update_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 3) {
                // delete
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // delete_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    delete_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // delete_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    delete_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // delete_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    delete_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 1) {
                // insert
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // insert_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                    insert_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // insert_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                    insert_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // insert_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                    insert_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                }
            }
        }
        __syncwarp();
    }
#endif

    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
        tables[table_ID].int_data[cur_row * table_info[table_ID].int_size + i] = SHM_INT32_data[(threadIdx.x >> 5) + i];
    }
    for (uint32_t i = laneID;
         i < table_info[table_ID].string_size * table_info[table_ID].string_length;
         i += 32) {
        tables[table_ID].string_data[cur_row *
                                     table_info[table_ID].string_size *
                                     table_info[table_ID].string_length + i] = SHM_STRING_data[(threadIdx.x >> 5) + i];
    }
    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
        tables[table_ID].double_data[cur_row * table_info[table_ID].double_size + i] = SHM_DOUBLE_data[
            (threadIdx.x >> 5) + i];
    }
    __syncwarp();
}


__global__ void txn_analyze_popular(uint32_t device_ID,
                                    uint32_t table_ID,
                                    uint32_t benchmark,
                                    Global_Table_Info *table_info,
                                    Global_Table *tables,
                                    Global_Table_Index *indexes,
                                    Global_Table_Meta *metainfo,
                                    Global_Table_Strategy *strategy,
                                    Global_Txn_Info *txn_info,
                                    Global_Txn *txn,
                                    Global_Txn_Exec *txn_exec,
                                    Global_Txn_Result *txn_result,
                                    Global_Txn_Exec_Param *exec_param,
                                    Global_Txn_Aux_Struct *aux_struct,
                                    Global_Data_Packet *data_packet) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    // const uint32_t bitmap_size = exec_param[0].bitmap_size;
    uint32_t cur = 0;
    cur = thID >> 5; // cur_row
    if (benchmark == 1) {
        while (cur >= metainfo[table_ID].row_start && cur < metainfo[table_ID].row_end) {
            popular_handler(device_ID, table_ID, cur, 0, table_info, tables, metainfo,
                            txn, exec_param, aux_struct, data_packet);
            cur += (blockDim.x * gridDim.x) >> 5;
        }
    } else if (benchmark == 4) {
        while (cur < 100) {
            popular_handler(device_ID, table_ID, cur, 0, table_info, tables, metainfo,
                            txn, exec_param, aux_struct, data_packet);
            cur += (blockDim.x * gridDim.x) >> 5;
        }
    } else if (benchmark == 2 || benchmark == 3) {
        // benchmark TPCC_PART, warehouse district has bitmap
        if (table_ID == 0 || table_ID == 1) {
            while (cur >= metainfo[table_ID].row_start && cur < metainfo[table_ID].row_end) {
                popular_handler(device_ID, table_ID, cur, 0, table_info, tables, metainfo,
                                txn, exec_param, aux_struct, data_packet);
                cur += (blockDim.x * gridDim.x) >> 5;
            }
        }
    }
}

__device__ void select_analyze(uint32_t device_ID,
                               uint32_t cur_txn,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Index *indexes,
                               Global_Table_Meta *metainfo,
                               Global_Table_Strategy *strategy,
                               Global_Txn_Info *txn_info,
                               Global_Txn *txn,
                               Global_Txn_Exec *txn_exec,
                               Global_Txn_Result *txn_result,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet) {
    uint32_t mark = txn_exec[0].select_txn_mark[cur_txn];
    uint32_t __cur_txn = mark;
    uint32_t ispopular = txn[__cur_txn].subtxn.ispopular;
    // uint32_t cur_tid = txn[__cur_txn].subtxn.TID;
    uint32_t row_1 = txn[__cur_txn].subtxn.dest_Row_1;
    uint32_t tableID = txn[__cur_txn].subtxn.table_ID;

    uint32_t cur = row_1 % metainfo[tableID].table_slice_size;
    if (ispopular == 1) {
    } else if (ispopular == 0) {
        // uint32_t mintITD = aux_struct[tableID].min_TID[cur];
        // if (mintITD == cur_tid) {
        //     // commit
        //     for (uint32_t i = 0; i < table_info[tableID].int_size; ++i) {
        //         select_operator<INT32>(tableID, cur, i, 1, tables, table_info, metainfo);
        //     }
        //     for (uint32_t i = 0; i < table_info[tableID].string_size * table_info[tableID].string_length; ++i) {
        //         select_operator<UINT32>(tableID, cur, i, 0, tables, table_info, metainfo);
        //     }
        //     for (uint32_t i = 0; i < table_info[tableID].double_size; ++i) {
        //         select_operator<DOUBLE>(tableID, cur, i, 3, tables, table_info, metainfo);
        //     }
        // } else
        {
            // plan redo
            uint32_t start_offset = aux_struct[tableID].mark_TID_offset[cur];
            uint32_t inside_offset = atomicAdd(&aux_struct[tableID].tmp_TID[cur], 1);
            aux_struct[0].mark_TID[start_offset + inside_offset] = mark;
        }
    }
}

__device__ void update_analyze(uint32_t device_ID,
                               uint32_t cur_txn,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Index *indexes,
                               Global_Table_Meta *metainfo,
                               Global_Table_Strategy *strategy,
                               Global_Txn_Info *txn_info,
                               Global_Txn *txn,
                               Global_Txn_Exec *txn_exec,
                               Global_Txn_Result *txn_result,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet) {
    uint32_t mark = txn_exec[0].update_txn_mark[cur_txn];
    uint32_t __cur_txn = mark;
    uint32_t ispopular = txn[__cur_txn].subtxn.ispopular;
    // uint32_t tid = txn[__cur_txn].subtxn.TID;
    uint32_t row_1 = txn[__cur_txn].subtxn.dest_Row_1;
    uint32_t tableID = txn[__cur_txn].subtxn.table_ID;

    uint32_t cur = row_1 % metainfo[tableID].table_slice_size;

    if (ispopular == 1) {
    } else if (ispopular == 0) {
        // uint32_t min_tid = aux_struct[tableID].min_TID[cur];
        // if (min_tid == tid) {
        //     // commit
        //     INT32 int_data = 0;
        //     for (uint32_t i = 0; i < table_info[tableID].int_size; ++i) {
        //         int_data = 0;
        //         update_operator<INT32>(tableID, cur, i, 1, int_data, tables, table_info, metainfo);
        //     }
        //     UINT32 string_data = 0;
        //     for (uint32_t i = 0; i < table_info[tableID].string_size * table_info[tableID].string_length;
        //          i += 32) {
        //         string_data = 0;
        //         update_operator<UINT32>(tableID, cur, i, 0, string_data, tables, table_info, metainfo);
        //     }
        //     DOUBLE double_data = 0.0;
        //     for (uint32_t i = 0; i < table_info[tableID].double_size; ++i) {
        //         double_data = 0.0;
        //         update_operator<DOUBLE>(tableID, cur, i, 3, double_data, tables, table_info, metainfo);
        //     }
        // } else
        {
            // plan redo
            uint32_t start_offset = aux_struct[tableID].mark_TID_offset[cur];
            uint32_t inside_offset = atomicAdd(&aux_struct[tableID].tmp_TID[cur], 1);
            aux_struct[0].mark_TID[start_offset + inside_offset] = mark;
        }
    }
}

__device__ void insert_analyze(uint32_t device_ID,
                               uint32_t cur_txn,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Index *indexes,
                               Global_Table_Meta *metainfo,
                               Global_Table_Strategy *strategy,
                               Global_Txn_Info *txn_info,
                               Global_Txn *txn,
                               Global_Txn_Exec *txn_exec,
                               Global_Txn_Result *txn_result,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet) {
    uint32_t mark = txn_exec[0].insert_txn_mark[cur_txn];
    uint32_t __cur_txn = mark;
    uint32_t ispopular = txn[__cur_txn].subtxn.ispopular;
    // uint32_t tid = txn[__cur_txn].subtxn.TID;
    uint32_t row_1 = txn[__cur_txn].subtxn.dest_Row_1;
    uint32_t tableID = txn[__cur_txn].subtxn.table_ID;

    uint32_t cur = row_1 % metainfo[tableID].table_slice_size;

    if (ispopular == 1) {
    } else if (ispopular == 0) {
        // uint32_t min_tid = aux_struct[tableID].min_TID[cur];
        // if (min_tid == tid) {
        //     // commit
        //     INT32 int_data = 0;
        //     for (uint32_t i = 0; i < table_info[tableID].int_size; ++i) {
        //         int_data = 0;
        //         insert_operator<INT32>(tableID, cur, i, 1, int_data, tables, table_info, metainfo);
        //     }
        //     UINT32 string_data = 0;
        //     for (uint32_t i = 0; i < table_info[tableID].string_size * table_info[tableID].string_length; ++i) {
        //         string_data = 0;
        //         insert_operator<UINT32>(tableID, cur, i, 0, string_data, tables, table_info, metainfo);
        //     }
        //     DOUBLE double_data = 0.0;
        //     for (uint32_t i = 0; i < table_info[tableID].double_size; ++i) {
        //         double_data = 0.0;
        //         insert_operator<DOUBLE>(tableID, cur, i, 3, double_data, tables, table_info, metainfo);
        //     }
        // } else
        {
            // plan redo
            uint32_t start_offset = aux_struct[tableID].mark_TID_offset[cur];
            uint32_t inside_offset = atomicAdd(&aux_struct[tableID].tmp_TID[cur], 1);
            aux_struct[0].mark_TID[start_offset + inside_offset] = mark;
        }
    }
}

__device__ void scan_analyze(uint32_t device_ID,
                             uint32_t cur_txn,
                             Global_Table_Info *table_info,
                             Global_Table *tables,
                             Global_Table_Index *indexes,
                             Global_Table_Meta *metainfo,
                             Global_Table_Strategy *strategy,
                             Global_Txn_Info *txn_info,
                             Global_Txn *txn,
                             Global_Txn_Exec *txn_exec,
                             Global_Txn_Result *txn_result,
                             Global_Txn_Exec_Param *exec_param,
                             Global_Txn_Aux_Struct *aux_struct,
                             Global_Data_Packet *data_packet) {
    uint32_t mark = txn_exec[0].scan_txn_mark[cur_txn];
    uint32_t __cur_txn = mark;
    uint32_t ispopular = txn[__cur_txn].subtxn.ispopular;
    // uint32_t tid = txn[__cur_txn].subtxn.TID;
    uint32_t row_1 = txn[__cur_txn].subtxn.dest_Row_1;
    uint32_t row_2 = txn[__cur_txn].subtxn.dest_Row_2;
    uint32_t tableID = txn[__cur_txn].subtxn.table_ID;


    if (ispopular == 1) {
    } else if (ispopular == 0) {
        uint32_t row_start = metainfo[tableID].row_start;
        uint32_t row_end = metainfo[tableID].row_end;

        // bool canCommit = true;
        // for (uint32_t row = row_1; row < row_2; ++row) {
        //     if (row >= row_start && row < row_end) {
        //         uint32_t cur = row % metainfo[tableID].table_slice_size;
        //         uint32_t min_tid = aux_struct[tableID].min_TID[row];
        //         if (min_tid != tid) {
        //             canCommit = false;
        //             break;
        //         }
        //     }
        // }
        // if (canCommit) {
        //     // commit
        //     for (uint32_t row = row_1; row < row_2; ++row) {
        //         if (row >= row_start && row < row_end) {
        //             for (uint32_t i = 0; i < table_info[tableID].int_size; ++i) {
        //                 scan_operator<INT32>(tableID, row, i, 1, tables, table_info, metainfo);
        //             }
        //             for (uint32_t i = 0;
        //                  i < table_info[tableID].string_size * table_info[tableID].string_length; ++i) {
        //                 scan_operator<UINT32>(tableID, row, i, 0, tables, table_info, metainfo);
        //             }
        //             for (uint32_t i = 0; i < table_info[tableID].double_size; ++i) {
        //                 scan_operator<DOUBLE>(tableID, row, i, 3, tables, table_info, metainfo);
        //             }
        //         }
        //     }
        // } else
        {
            for (uint32_t row = row_1; row < row_2; ++row) {
                if (row >= row_start && row < row_end) {
                    uint32_t cur = row % metainfo[tableID].table_slice_size;
                    uint32_t start_offset = aux_struct[tableID].mark_TID_offset[cur];
                    uint32_t inside_offset = atomicAdd(&aux_struct[tableID].tmp_TID[cur], 1);
                    aux_struct[0].mark_TID[start_offset + inside_offset] = mark;
                }
            }
        }
    }
}

__device__ void delete_analyze(uint32_t device_ID,
                               uint32_t cur_txn,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Index *indexes,
                               Global_Table_Meta *metainfo,
                               Global_Table_Strategy *strategy,
                               Global_Txn_Info *txn_info,
                               Global_Txn *txn,
                               Global_Txn_Exec *txn_exec,
                               Global_Txn_Result *txn_result,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet) {
    uint32_t mark = txn_exec[0].delete_txn_mark[cur_txn];
    uint32_t __cur_txn = mark;
    uint32_t ispopular = txn[__cur_txn].subtxn.ispopular;
    // uint32_t tid = txn[__cur_txn].subtxn.TID;
    uint32_t row_1 = txn[__cur_txn].subtxn.dest_Row_1;
    uint32_t tableID = txn[__cur_txn].subtxn.table_ID;

    uint32_t cur = row_1 % metainfo[tableID].table_slice_size;

    if (ispopular == 1) {
    } else if (ispopular == 0) {
        // uint32_t min_tid = aux_struct[tableID].min_TID[cur];
        // if (min_tid == tid) {
        //     // commit
        //     for (uint32_t i = 0; i < table_info[tableID].int_size; ++i) {
        //         delete_operator<INT32>(tableID, cur, i, 1, tables, table_info, metainfo);
        //     }
        //     for (uint32_t i = 0; i < table_info[tableID].string_size * table_info[tableID].string_length; ++i) {
        //         delete_operator<UINT32>(tableID, cur, i, 0, tables, table_info, metainfo);
        //     }
        //     for (uint32_t i = 0; i < table_info[tableID].double_size; ++i) {
        //         delete_operator<DOUBLE>(tableID, cur, i, 3, tables, table_info, metainfo);
        //     }
        // } else
        {
            // plan redo
            uint32_t start_offset = aux_struct[tableID].mark_TID_offset[cur];
            uint32_t inside_offset = atomicAdd(&aux_struct[tableID].tmp_TID[cur], 1);
            aux_struct[0].mark_TID[start_offset + inside_offset] = mark;
        }
    }
}

__global__ void txn_analyze_regular(uint32_t device_ID,
                                    uint32_t device_cnt,
                                    uint32_t benchmark,
                                    Global_Table_Info *table_info,
                                    Global_Table *tables,
                                    Global_Table_Index *indexes,
                                    Global_Table_Meta *metainfo,
                                    Global_Table_Strategy *strategy,
                                    Global_Txn_Info *txn_info,
                                    Global_Txn *txn,
                                    Global_Txn_Exec *txn_exec,
                                    Global_Txn_Result *txn_result,
                                    Global_Txn_Exec_Param *exec_param,
                                    Global_Txn_Aux_Struct *aux_struct,
                                    Global_Data_Packet *data_packet) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t cur = 0;

    cur = thID;
    // cur = atomicAdd(&txn_exec[0].select_tmp, 1);
    while (cur < txn_exec[0].select_cur) {
        select_analyze(device_ID, cur, table_info, tables, indexes, metainfo, strategy,
                       txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        cur += blockDim.x * gridDim.x;
        // cur = atomicAdd(&txn_exec[0].select_tmp, 1);
    }

    cur = thID;
    // cur = atomicAdd(&txn_exec[0].update_tmp, 1);
    while (cur < txn_exec[0].update_cur) {
        update_analyze(device_ID, cur, table_info, tables, indexes, metainfo, strategy,
                       txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        // cur += blockDim.x * gridDim.x;
        cur = atomicAdd(&txn_exec[0].update_tmp, 1);
    }

    cur = thID;
    // cur = atomicAdd(&txn_exec[0].insert_tmp, 1);
    while (cur < txn_exec[0].insert_cur) {
        insert_analyze(device_ID, cur, table_info, tables, indexes, metainfo, strategy,
                       txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        // cur += blockDim.x * gridDim.x;
        cur = atomicAdd(&txn_exec[0].insert_tmp, 1);
    }

    cur = thID;
    // cur = atomicAdd(&txn_exec[0].scan_tmp, 1);
    while (cur < txn_exec[0].scan_cur) {
        scan_analyze(device_ID, cur, table_info, tables, indexes, metainfo, strategy,
                     txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        cur += blockDim.x * gridDim.x;
        // cur = atomicAdd(&txn_exec[0].scan_tmp, 1);
    }

    cur = thID;
    // cur = atomicAdd(&txn_exec[0].delete_tmp, 1);
    while (cur < txn_exec[0].delete_cur) {
        delete_analyze(device_ID, cur, table_info, tables, indexes, metainfo, strategy,
                       txn_info, txn, txn_exec, txn_result, exec_param, aux_struct, data_packet);
        cur += blockDim.x * gridDim.x;
        // cur = atomicAdd(&txn_exec[0].delete_tmp, 1);
    }
}

__global__ void filter_commit(uint32_t device_ID,
                              uint32_t device_cnt,
                              uint32_t table_ID,
                              Global_Table_Info *table_info,
                              Global_Table *tables,
                              Global_Table_Index *indexes,
                              Global_Table_Meta *metainfo,
                              Global_Table_Strategy *strategy,
                              Global_Txn_Info *txn_info,
                              Global_Txn *txn,
                              Global_Txn_Exec *txn_exec,
                              Global_Txn_Result *txn_result,
                              Global_Txn_Exec_Param *exec_param,
                              Global_Txn_Aux_Struct *aux_struct,
                              Global_Data_Packet *data_packet) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t step = (blockDim.x * gridDim.x) >> 5;
    uint32_t laneID = threadIdx.x & 0x1f;
    uint32_t used_rows_offset = thID >> 5;

    while (used_rows_offset < aux_struct[table_ID].used_rows_cnt) {
        const uint32_t row = aux_struct[table_ID].used_rows[used_rows_offset];
        const uint32_t cur_row = row % metainfo[table_ID].table_slice_size;
        const uint32_t txn_cnt = aux_struct[table_ID].tmp_TID[cur_row];
        const uint32_t start_offset = aux_struct[table_ID].mark_TID_offset[cur_row];
        if (txn_cnt > 1) {
            uint32_t size = 0;
            uint32_t left = 0;
            uint32_t right = 0;
            uint32_t offset = 0;
            uint32_t sub_size = 0;
            uint32_t tid = 0xffffffff;
            uint32_t mark = 0;
            uint32_t tid_r = 0xffffffff;
            uint32_t mark_r = 0;
            for (uint32_t i = 2; i <= (1 << 32 - __clz(txn_cnt)); i *= 2) {
                for (uint32_t start = i * laneID; start < txn_cnt; start += i * 32) {
                    size = i;
                    if (start + size >= txn_cnt) {
                        size = txn_cnt - start;
                    }
                    if (start < txn_cnt) {
                        sub_size = i >> 2;
                        offset = start;
                        left = start;
                        right = start + sub_size;
                        while (left < start + sub_size && right < start + size) {
                            mark = aux_struct[0].mark_TID[left + start_offset];
                            tid = txn[mark].subtxn.TID;
                            mark_r = aux_struct[0].mark_TID[right + start_offset];
                            tid_r = txn[mark_r].subtxn.TID;
                            if (tid < tid_r) {
                                aux_struct[0].merge_tmp[offset + start_offset] =
                                        aux_struct[0].mark_TID[left + start_offset];
                                ++left;
                            } else {
                                aux_struct[0].merge_tmp[offset + start_offset] =
                                        aux_struct[0].mark_TID[right + start_offset];
                                ++right;
                            }
                            ++offset;
                        }
                        while (left < start + sub_size) {
                            aux_struct[0].merge_tmp[offset + start_offset] =
                                    aux_struct[0].mark_TID[left + start_offset];
                            ++left;
                            ++offset;
                        }
                        while (right < start + size) {
                            aux_struct[0].merge_tmp[offset + start_offset] =
                                    aux_struct[0].mark_TID[right + start_offset];
                            ++right;
                            ++offset;
                        }
                    }
                }
                for (uint32_t j = laneID; j < txn_cnt; j += 32) {
                    aux_struct[0].mark_TID[start_offset + j] = aux_struct[0].merge_tmp[start_offset + j];
                }
            }
        }
        __syncwarp();
        used_rows_offset += step;
    }
}

__device__ void commit(uint32_t device_ID,
                       uint32_t table_ID,
                       uint32_t row,
                       Global_Table_Info *table_info,
                       Global_Table *tables,
                       Global_Table_Meta *metainfo,
                       Global_Txn *txn,
                       Global_Txn_Exec_Param *exec_param,
                       Global_Txn_Aux_Struct *aux_struct) {
    const uint32_t warpID = threadIdx.x >> 5;
    const uint32_t laneID = threadIdx.x & 0x1f;
    const uint32_t cur_row = row % metainfo[table_ID].table_slice_size;
    const uint32_t txn_cnt = aux_struct[table_ID].tmp_TID[cur_row];
    const uint32_t start_offset = aux_struct[table_ID].mark_TID_offset[cur_row];

    __shared__ INT32 SHM_INT32_data[16 * 16];
    __shared__ UINT32 SHM_STRING_data[16 * 8 * 16];
    __shared__ DOUBLE SHM_DOUBLE_data[16 * 16];
    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
        SHM_INT32_data[(threadIdx.x >> 5) + i] = tables[table_ID].int_data[cur_row * table_info[table_ID].int_size + i];
    }
    for (uint32_t i = laneID;
         i < table_info[table_ID].string_size * table_info[table_ID].string_length;
         i += 32) {
        SHM_STRING_data[(threadIdx.x >> 5) + i] = tables[table_ID].string_data[cur_row *
                                                                               table_info[table_ID].string_size *
                                                                               table_info[table_ID].string_length + i];
    }
    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
        SHM_DOUBLE_data[(threadIdx.x >> 5) + i] = tables[table_ID].double_data[
            cur_row * table_info[table_ID].double_size + i];
    }
    __syncwarp();
    uint32_t cur = 0;
    __shared__ uint32_t SHM_mark[512];
    const uint32_t SHM_mark_offset_start = threadIdx.x & 0xffffffe0;
    while (cur < txn_cnt) {
        //execute
        // uint32_t mark = aux_struct[0].mark_TID[start_offset + cur];
        // ++cur;
        if (cur + laneID < txn_cnt) {
            SHM_mark[threadIdx.x] = aux_struct[0].mark_TID[start_offset + cur + laneID];
        } else {
            SHM_mark[threadIdx.x] = 0;
        }
        __syncwarp();
        cur += 32;
        for (uint32_t j = 0; j < 32 && SHM_mark[SHM_mark_offset_start + j] > 0; ++j) {
            uint32_t mark = SHM_mark[SHM_mark_offset_start + j];
            uint32_t type = txn[mark].subtxn.type;

            if (type == 0) {
                // select
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // select_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    select_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // select_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    select_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // select_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    select_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 4) {
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // scan_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    scan_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // scan_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    scan_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // scan_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    scan_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 2) {
                // update
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // update_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                    update_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // update_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                    update_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // update_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                    update_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 3) {
                // delete
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // delete_operator<INT32>(table_ID, cur_row, i, 1, tables, table_info, metainfo);
                    delete_operator_shared<INT32>(SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // delete_operator<UINT32>(table_ID, cur_row, i, 0, tables, table_info, metainfo);
                    delete_operator_shared<UINT32>(SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // delete_operator<DOUBLE>(table_ID, cur_row, i, 3, tables, table_info, metainfo);
                    delete_operator_shared<DOUBLE>(SHM_DOUBLE_data[warpID * 16 + i]);
                }
            } else if (type == 1) {
                // insert
                for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
                    // insert_operator<INT32>(table_ID, cur_row, i, 1, 0, tables, table_info, metainfo);
                    insert_operator_shared<INT32>(0, SHM_INT32_data[warpID * 16 + i]);
                }
                for (uint32_t i = laneID;
                     i < table_info[table_ID].string_size * table_info[table_ID].string_length; i += 32) {
                    // insert_operator<UINT32>(table_ID, cur_row, i, 0, 0, tables, table_info, metainfo);
                    insert_operator_shared<UINT32>(0, SHM_STRING_data[warpID * 16 * 8 + i]);
                }
                for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
                    // insert_operator<DOUBLE>(table_ID, cur_row, i, 3, 0.0, tables, table_info, metainfo);
                    insert_operator_shared<DOUBLE>(0.0, SHM_DOUBLE_data[warpID * 16 + i]);
                }
            }
            __syncwarp();
        }
    }

    for (uint32_t i = laneID; i < table_info[table_ID].int_size; i += 32) {
        tables[table_ID].int_data[cur_row * table_info[table_ID].int_size + i] =
                SHM_INT32_data[(threadIdx.x >> 5) + i];
    }
    for (uint32_t i = laneID;
         i < table_info[table_ID].string_size * table_info[table_ID].string_length;
         i += 32) {
        tables[table_ID].string_data[cur_row *
                                     table_info[table_ID].string_size *
                                     table_info[table_ID].string_length + i] =
                SHM_STRING_data[(threadIdx.x >> 5) + i];
    }
    for (uint32_t i = laneID; i < table_info[table_ID].double_size; i += 32) {
        tables[table_ID].double_data[cur_row * table_info[table_ID].double_size + i] =
                SHM_DOUBLE_data[(threadIdx.x >> 5) + i];
    }
}


__global__ void txn_commit(uint32_t device_ID,
                           uint32_t device_cnt,
                           uint32_t table_ID,
                           Global_Table_Info *table_info,
                           Global_Table *tables,
                           Global_Table_Index *indexes,
                           Global_Table_Meta *metainfo,
                           Global_Table_Strategy *strategy,
                           Global_Txn_Info *txn_info,
                           Global_Txn *txn,
                           Global_Txn_Exec *txn_exec,
                           Global_Txn_Result *txn_result,
                           Global_Txn_Exec_Param *exec_param,
                           Global_Txn_Aux_Struct *aux_struct,
                           Global_Data_Packet *data_packet) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t step = (blockDim.x * gridDim.x) >> 5;

    uint32_t row = 0; // cur_row
    uint32_t offset = thID >> 5;
    while (offset < aux_struct[table_ID].used_rows_cnt) {
        row = aux_struct[table_ID].used_rows[offset];
        commit(device_ID, table_ID, row, table_info, tables, metainfo,
               txn, exec_param, aux_struct);
        offset += step;
    }
}
