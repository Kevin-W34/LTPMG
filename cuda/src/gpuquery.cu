#include "../include/gpuquery.cuh"

#include "../include/gpucommon.cuh"

GPUquery::GPUquery(/* args */) {
}

GPUquery::~GPUquery() {
    // this->transactions_batch_ptr.reset();
}

int GPUquery::test(int input) {
    std::cout << "Task " << input << " is executing" << std::endl;
    return input;
}

void GPUquery::malloc_global_txn(std::shared_ptr<Param> param,
                                 std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                 Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuquery.cu GPUquery::malloc_global_txn()" << std::endl;

    // for (uint32_t i = 0; i < param->device_cnt; ++i) {
    //     CHECK(cudaSetDevice(param->device_IDs[i]));
    //     for (uint32_t j = 0; j < param->device_cnt; ++j) {
    //         if (param->device_IDs[i]!= param->device_IDs[j]) {
    //             CHECK(cudaDeviceEnablePeerAccess(param->device_IDs[j],0));
    //         }
    //     }
    // }
    // for (uint32_t i = 0; i < param->device_cnt; ++i) {
    //     CHECK(cudaSetDevice(param->device_IDs[i]));
    //     for (uint32_t j = 0; j < param->device_cnt; ++j) {
    //         if (param->device_IDs[i]!= param->device_IDs[j]) {
    //             CHECK(cudaDeviceDisablePeerAccess(param->device_IDs[j]));
    //         }
    //     }
    // }

    CHECK(cudaHostAlloc((void **)&global_txn_info_d, sizeof(Global_Txn_Info *) * param->device_cnt, cudaHostAllocDefault
    ));
    CHECK(cudaHostAlloc((void **)&global_txn_info_h, sizeof(Global_Txn_Info) * param->get_subtxn_kinds(),
        cudaHostAllocDefault));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&global_txn_info_d[i], sizeof(Global_Txn_Info) * param->get_subtxn_kinds()));
    }

    CHECK(cudaHostAlloc((void **)&global_txn_d, sizeof(Global_Txn *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&global_txn_h, sizeof(Global_Txn *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&global_txn, sizeof(Global_Txn) * param->get_sub_txn_size(), cudaHostAllocDefault));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        // TODO: 子事务集如何存储/子事务集合内部内存开辟
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&global_txn_d[i], sizeof(Global_Txn) * param->get_sub_txn_size()));
        CHECK(cudaHostAlloc((void **)&global_txn_h[i], sizeof(Global_Txn) * param->get_sub_txn_size(),
            cudaHostAllocDefault));
    }

    CHECK(cudaHostAlloc((void **)&global_txn_exec_h, sizeof(Global_Txn_Exec *) * param->device_cnt, cudaHostAllocDefault
    ));
    CHECK(cudaHostAlloc((void **)&global_txn_exec_d, sizeof(Global_Txn_Exec *) * param->device_cnt, cudaHostAllocDefault
    ));
    for (size_t i = 0; i < param->device_cnt; ++i) {
        // TODO: 子事务集如何存储/子事务集合内部内存开辟
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&global_txn_exec_d[i], sizeof(Global_Txn_Exec)));
        CHECK(cudaHostAlloc((void **)&global_txn_exec_h[i], sizeof(Global_Txn_Exec), cudaHostAllocDefault));
    }

    CHECK(cudaHostAlloc((void **)&global_txn_result_d, sizeof(Global_Txn *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&global_txn_result_h, sizeof(Global_Txn *) * param->device_cnt, cudaHostAllocDefault));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&global_txn_result_d[i], sizeof(Global_Txn) * param->get_sub_txn_size()));
        CHECK(cudaHostAlloc((void **)&global_txn_result_h[i], sizeof(Global_Txn) * param->get_sub_txn_size(),
            cudaHostAllocDefault));
    }

    CHECK(cudaHostAlloc((void **)&exec_param_h, sizeof(Global_Txn_Exec_Param) * param->get_subtxn_kinds(),
        cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&exec_param_d, sizeof(Global_Txn_Exec_Param *) * param->device_cnt,
        cudaHostAllocDefault));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&exec_param_d[i], sizeof(Global_Txn_Exec_Param) * param->get_subtxn_kinds()));
    }

    CHECK(cudaHostAlloc((void **)&aux_struct_d, sizeof(Global_Txn_Aux_Struct *) * param->device_cnt,
        cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&aux_struct_d_h, sizeof(Global_Txn_Aux_Struct *) * param->device_cnt,
        cudaHostAllocDefault));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&aux_struct_d[i], sizeof(Global_Txn_Aux_Struct) * param->table_cnt));
        CHECK(cudaHostAlloc((void **)&aux_struct_d_h[i], sizeof(Global_Txn_Aux_Struct) * param->table_cnt,
            cudaHostAllocDefault));
    }

    CHECK(cudaHostAlloc((void **)&data_packet_d, sizeof(Global_Data_Packet *) * param->device_cnt, cudaHostAllocDefault
    ));
    CHECK(cudaHostAlloc((void **)&data_packet_h, sizeof(Global_Data_Packet *) * param->device_cnt, cudaHostAllocDefault
    ));


    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&data_packet_d[i],
            sizeof(Global_Data_Packet) * param->get_datapacket_size() ));
        CHECK(cudaHostAlloc((void **)&data_packet_h[i],
            sizeof(Global_Data_Packet) * param->get_datapacket_size() ,
            cudaHostAllocDefault));
        // #ifdef LTPMG_GPUQUERY_PRINTSIZE
        std::cout << "data_packet_d[" << i << "] is " << param->get_datapacket_size() << std::endl;
        std::cout << "data_packet_h[" << i << "] is " << param->get_datapacket_size() << std::endl;
        // #endif
    }

    // 生成参数
    if (param->benchmark == "TEST") {
        gen_param<Test_Query>(param);

        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_all_row, sizeof(UINT32) * param->get_sub_txn_size()
            ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_all_row, sizeof(UINT32) * param->get_sub_txn_size()
            ));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap, sizeof(UINT32) * param->get_bitmap_size() * param->
                test_1_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap, sizeof(UINT32) * param->get_bitmap_size() * param->
                test_2_size/param->device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_tmp, sizeof(UINT32) * param->get_bitmap_size() *
                param->test_1_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_tmp, sizeof(UINT32) * param->get_bitmap_size() *
                param->test_2_size/param->device_cnt));

            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_1_size/param->device_cnt));
            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][1].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_2_size/param->device_cnt));

            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_1_size/param->device_cnt));
            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][1].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_2_size/param->device_cnt));

            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_1_size/param->device_cnt));
            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][1].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> test_2_size/param->device_cnt));


            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_used_size,
                sizeof(UINT32) * 1 * param->test_1_size/ param-> device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_used_size,
                sizeof(UINT32) * 1 * param->test_2_size/ param-> device_cnt));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].min_TID, sizeof(UINT32) * 1 * param->test_1_size/param->
            //     device_cnt));
            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].min_TID, sizeof(UINT32) * 1 * param->test_2_size/param->
            //     device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].cnt_TID, sizeof(UINT32) * 1 * param->test_1_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].cnt_TID, sizeof(UINT32) * 1 * param->test_2_size/param->
                device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].tmp_TID, sizeof(UINT32) * 1 * param->test_1_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].tmp_TID, sizeof(UINT32) * 1 * param->test_2_size/param->
                device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID_offset, sizeof(UINT32) * 1 * param->test_1_size/
                param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].mark_TID_offset, sizeof(UINT32) * 1 * param->test_2_size/
                param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID,
                sizeof(UINT32) * param->get_sub_txn_size()*2));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].merge_tmp,
                sizeof(UINT32) * param->get_sub_txn_size()*2));


#ifdef LTPMG_GPUQUERY_PRINTSIZE
            std::cout << "aux_struct_d_h[" << i << "][0].bitmap is " << param->get_bitmap_size() * param->test_1_size <<
                std::endl;
            std::cout << "aux_struct_d_h[" << i << "][1].bitmap is " << param->get_bitmap_size() * param->test_2_size <<
                std::endl;
            std::cout << "aux_struct_d_h[" << i << "][0].min_TID is " << 1 * param->test_1_size << std::endl;
            std::cout << "aux_struct_d_h[" << i << "][1].min_TID is " << 1 * param->test_2_size << std::endl;
#endif

            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].select_txn_mark, sizeof(UINT32) * param->
                get_select_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].insert_txn_mark, sizeof(UINT32) * param->
                get_insert_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].update_txn_mark, sizeof(UINT32) * param->
                get_update_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].scan_txn_mark, sizeof(UINT32) * param->
                get_scan_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].delete_txn_mark, sizeof(UINT32) * param->
                get_delete_batch_size()));

#ifdef LTPMG_GPUQUERY_PRINTSIZE
            std::cout << "global_txn_exec_h[" << i << "][0].select_txn is " << param->get_select_batch_size() << std::endl;
            std::cout << "global_txn_exec_h[" << i << "][0].insert_txn is " << param->get_insert_batch_size() << std::endl;
            std::cout << "global_txn_exec_h[" << i << "][0].update_txn is " << param->get_update_batch_size() << std::endl;
            std::cout << "global_txn_exec_h[" << i << "][0].scan_txn is " << param->get_scan_batch_size() << std::endl;
            std::cout << "global_txn_exec_h[" << i << "][0].delete_txn is " << param->get_delete_batch_size() << std::endl;
#endif
        }
    } else if (param->benchmark == "TPCC_PART") {
        gen_param<TPCC_PART>(param);

        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_all_row,
                sizeof(UINT32) * param->get_sub_txn_size() ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_tmp,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_used_size,
                sizeof(UINT32) * 1 * param->warehouse_size /param-> device_cnt));
            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].min_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].cnt_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].tmp_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID_offset,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_all_row,
                sizeof(UINT32) * param->get_sub_txn_size() ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_tmp,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_used_size,
                sizeof(UINT32) * 1 * param->district_size/ param-> device_cnt));
            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].min_TID, sizeof(UINT32) * 1 * param->district_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].cnt_TID, sizeof(UINT32) * 1 * param->district_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].tmp_TID, sizeof(UINT32) * 1 * param->district_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].mark_TID_offset,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].min_TID, sizeof(UINT32) * 1 * param->customer_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].cnt_TID, sizeof(UINT32) * 1 * param->customer_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].tmp_TID, sizeof(UINT32) * 1 * param->customer_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].mark_TID_offset,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].min_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].cnt_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].tmp_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].mark_TID_offset,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].min_TID, sizeof(UINT32) * 1 * param->history_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].cnt_TID, sizeof(UINT32) * 1 * param->history_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].tmp_TID, sizeof(UINT32) * 1 * param->history_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].mark_TID_offset,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].min_TID, sizeof(UINT32) * 1 * param->order_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].cnt_TID, sizeof(UINT32) * 1 * param->order_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].tmp_TID, sizeof(UINT32) * 1 * param->order_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].mark_TID_offset,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].min_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].cnt_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].tmp_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].mark_TID_offset,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].min_TID, sizeof(UINT32) * 1 * param->stock_size/param->
                // device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].cnt_TID, sizeof(UINT32) * 1 * param->stock_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].tmp_TID, sizeof(UINT32) * 1 * param->stock_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].mark_TID_offset,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].min_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].cnt_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].tmp_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].mark_TID_offset,
                sizeof(UINT32) * 1 * param->item_size ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID, sizeof(UINT32) * param->get_sub_txn_size()*2));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].merge_tmp, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].select_txn_mark, sizeof(UINT32) * param->
                get_select_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].insert_txn_mark, sizeof(UINT32) * param->
                get_insert_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].update_txn_mark, sizeof(UINT32) * param->
                get_update_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].scan_txn_mark, sizeof(UINT32) * param->
                get_scan_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].delete_txn_mark, sizeof(UINT32) * param->
                get_delete_batch_size()));
        }
    } else if (param->benchmark == "TPCC_ALL") {
        gen_param<TPCC_ALL>(param);
        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_all_row,
                sizeof(UINT32) * param->get_sub_txn_size() ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_tmp,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> warehouse_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_used_size,
                sizeof(UINT32) * 1 * param->warehouse_size /param-> device_cnt));
            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].min_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].cnt_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].tmp_TID, sizeof(UINT32) * 1 * param->warehouse_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID_offset,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_all_row,
                sizeof(UINT32) * param->get_sub_txn_size() ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_tmp,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> district_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].bitmap_used_size,
                sizeof(UINT32) * 1 * param->district_size/ param-> device_cnt));
            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].min_TID,
                // sizeof(UINT32) * 1 * param->district_size/param-> device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].cnt_TID,
                sizeof(UINT32) * 1 * param->district_size/param-> device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].tmp_TID,
                sizeof(UINT32) * 1 * param->district_size/param-> device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].mark_TID_offset,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][1].used_rows,
                sizeof(UINT32) * param->get_sub_txn_size()*2));


            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].min_TID, sizeof(UINT32) * 1 * param->customer_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].cnt_TID, sizeof(UINT32) * 1 * param->customer_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].tmp_TID, sizeof(UINT32) * 1 * param->customer_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].mark_TID_offset,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][2].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].min_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].cnt_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].tmp_TID, sizeof(UINT32) * 1 * param->neworder_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].mark_TID_offset,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][3].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].min_TID, sizeof(UINT32) * 1 * param->history_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].cnt_TID, sizeof(UINT32) * 1 * param->history_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].tmp_TID, sizeof(UINT32) * 1 * param->history_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].mark_TID_offset,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][4].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].min_TID, sizeof(UINT32) * 1 * param->order_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].cnt_TID, sizeof(UINT32) * 1 * param->order_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].tmp_TID, sizeof(UINT32) * 1 * param->order_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].mark_TID_offset,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][5].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].min_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].cnt_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].tmp_TID, sizeof(UINT32) * 1 * param->orderline_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].mark_TID_offset,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][6].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].min_TID, sizeof(UINT32) * 1 * param->stock_size/param->
            //     device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].cnt_TID, sizeof(UINT32) * 1 * param->stock_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].tmp_TID, sizeof(UINT32) * 1 * param->stock_size/param->
                device_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].mark_TID_offset,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][7].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].min_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].cnt_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].tmp_TID, sizeof(UINT32) * 1 * param->item_size));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].mark_TID_offset,
                sizeof(UINT32) * 1 * param->item_size ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][8].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID, sizeof(UINT32) * param->get_sub_txn_size()*2));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].merge_tmp, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].select_txn_mark, sizeof(UINT32) * param->
                get_select_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].insert_txn_mark, sizeof(UINT32) * param->
                get_insert_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].update_txn_mark, sizeof(UINT32) * param->
                get_update_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].scan_txn_mark, sizeof(UINT32) * param->
                get_scan_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].delete_txn_mark, sizeof(UINT32) * param->
                get_delete_batch_size()));

#ifdef LTPMG_GPUQUERY_PRINTSIZE
                    std::cout << "global_txn_exec_h[" << i << "][0].select_txn is " << param->get_select_batch_size() << std::endl;
                    std::cout << "global_txn_exec_h[" << i << "][0].insert_txn is " << param->get_insert_batch_size() << std::endl;
                    std::cout << "global_txn_exec_h[" << i << "][0].update_txn is " << param->get_update_batch_size() << std::endl;
                    std::cout << "global_txn_exec_h[" << i << "][0].scan_txn is " << param->get_scan_batch_size() << std::endl;
                    std::cout << "global_txn_exec_h[" << i << "][0].delete_txn is " << param->get_delete_batch_size() << std::endl;
#endif
        }
    } else if (param->benchmark == "YCSB_A" ||
               param->benchmark == "YCSB_B" ||
               param->benchmark == "YCSB_C" ||
               param->benchmark == "YCSB_D" ||
               param->benchmark == "YCSB_E") {
        if (param->benchmark == "YCSB_A") {
            gen_param<YCSB_A_Query>(param);
        } else if (param->benchmark == "YCSB_B") {
            gen_param<YCSB_B_Query>(param);
        } else if (param->benchmark == "YCSB_C") {
            gen_param<YCSB_C_Query>(param);
        } else if (param->benchmark == "YCSB_D") {
            gen_param<YCSB_D_Query>(param);
        } else if (param->benchmark == "YCSB_E") {
            gen_param<YCSB_E_Query>(param);
        }

        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap,
                sizeof(UINT32) * param->get_bitmap_size() * param-> bitmap_row_cnt));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_all_row,
                sizeof(UINT32) * param->get_sub_txn_size() ));
            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_tmp,
                sizeof(UINT32) * param->get_bitmap_size() * param-> bitmap_row_cnt));

            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> bitmap_row_cnt));
            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark_offset,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> bitmap_row_cnt));
            CHECK(cudaMalloc((void**)&aux_struct_d_h[i][0].bitmap_mark_compressed,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param-> bitmap_row_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].bitmap_used_size,
                sizeof(UINT32) * 1 * param->bitmap_row_cnt));

            // CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].min_TID, sizeof(UINT32) * 1 * param->ycsb_size/param->
            //     device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].cnt_TID, sizeof(UINT32) * 1 * param->ycsb_size/param->
                device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].tmp_TID, sizeof(UINT32) * 1 * param->ycsb_size/param->
                device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID_offset, sizeof(UINT32) * 1 * param->ycsb_size/param
                ->device_cnt));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].mark_TID, sizeof(UINT32) * param->get_sub_txn_size()*2));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].merge_tmp, sizeof(UINT32) * param->get_sub_txn_size()*2));

            CHECK(cudaMalloc((void **)&aux_struct_d_h[i][0].used_rows, sizeof(UINT32) * param->get_sub_txn_size()*2));


            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].select_txn_mark, sizeof(UINT32) * param->
                get_select_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].insert_txn_mark, sizeof(UINT32) * param->
                get_insert_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].update_txn_mark, sizeof(UINT32) * param->
                get_update_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].scan_txn_mark, sizeof(UINT32) * param->
                get_scan_batch_size()));
            CHECK(cudaMalloc((void **)&global_txn_exec_h[i][0].delete_txn_mark, sizeof(UINT32) * param->
                get_delete_batch_size()));
        }
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMemcpy(global_txn_info_d[i], global_txn_info_h, sizeof(Global_Txn_Info) * param->get_subtxn_kinds(),
            cudaMemcpyHostToDevice));
        CHECK(cudaMemcpy(exec_param_d[i], exec_param_h, sizeof(Global_Txn_Exec_Param) * param->get_subtxn_kinds(),
            cudaMemcpyHostToDevice));
        CHECK(cudaMemcpy(aux_struct_d[i], aux_struct_d_h[i], sizeof(Global_Txn_Aux_Struct) * param->table_cnt,
            cudaMemcpyHostToDevice));
        CHECK(cudaMemcpy(global_txn_exec_d[i], global_txn_exec_h[i], sizeof(Global_Txn_Exec),
            cudaMemcpyHostToDevice));
    }
    std::cout << "end gpuquery.cu GPUquery::malloc_global_txn()" << std::endl;
}

void GPUquery::copy_global_txn(std::shared_ptr<Param> param,
                               std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                               Global_Txn_Info *global_txn_info, Global_Table_Meta **meta, Global_Table_Index **index) {
    std::cout << "start gpuquery.cu GPUquery::copy_global_txn()" << std::endl;
    this->transactions_batch_ptr = transactions_batch_ptr;
    this->global_txn_info = global_txn_info;
    clear_global_txn(param, transactions_batch_ptr, global_txn_info);

    // 解析事务
    if (param->benchmark == "TEST") {
        query_parse<Test_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Test_Query_2>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "TPCC_PART") {
        query_parse<Neworder_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Payment_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "TPCC_ALL") {
        query_parse<Neworder_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Payment_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Orderstatus_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Delivery_Query>(param, transactions_batch_ptr, meta, index);
        query_parse<Stocklevel_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "YCSB_A") {
        query_parse<YCSB_A_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "YCSB_B") {
        query_parse<YCSB_B_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "YCSB_C") {
        query_parse<YCSB_C_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "YCSB_D") {
        query_parse<YCSB_D_Query>(param, transactions_batch_ptr, meta, index);
    } else if (param->benchmark == "YCSB_E") {
        query_parse<YCSB_E_Query>(param, transactions_batch_ptr, meta, index);
    }

    std::cout << "end gpuquery.cu GPUquery::copy_global_txn()" << std::endl;
}

void GPUquery::clear_global_txn(std::shared_ptr<Param> param,
                                std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuquery.cu GPUquery::clear_global_txn()" << std::endl;

    cudaStream_t *streams;
    streams = new cudaStream_t[param->device_cnt];

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaStreamCreate(&streams[i]));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMemsetAsync(global_txn_d[i],0,
            sizeof(Global_Txn)*param->get_sub_txn_size(),streams[i]));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));

        CHECK(cudaMemsetAsync(global_txn_exec_h[i][0].select_txn_mark, 0, sizeof(UINT32) * param->
            get_select_batch_size(), streams[i]));
        CHECK(cudaMemsetAsync(global_txn_exec_h[i][0].insert_txn_mark, 0, sizeof(UINT32) * param->
            get_insert_batch_size(), streams[i]));
        CHECK(cudaMemsetAsync(global_txn_exec_h[i][0].update_txn_mark, 0, sizeof(UINT32) * param->
            get_update_batch_size(), streams[i]));
        CHECK(cudaMemsetAsync(global_txn_exec_h[i][0].scan_txn_mark, 0, sizeof(UINT32) * param->
            get_scan_batch_size(), streams[i]));
        CHECK(cudaMemsetAsync(global_txn_exec_h[i][0].delete_txn_mark, 0, sizeof(UINT32) * param->
            get_delete_batch_size(), streams[i]));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].select_cur, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].insert_cur, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].update_cur, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].scan_cur, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].delete_cur, 0, sizeof(UINT32), streams[i]));

        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].select_tmp, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].insert_tmp, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].update_tmp, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].scan_tmp, 0, sizeof(UINT32), streams[i]));
        CHECK(cudaMemsetAsync(&global_txn_exec_d[i][0].delete_tmp, 0, sizeof(UINT32), streams[i]));

        CHECK(cudaMemsetAsync(data_packet_d[i], 0xfffffff,
            sizeof(Global_Data_Packet) * param->get_datapacket_size() , streams[i]));
    }

    if (param->benchmark == "TEST") {
        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].data_packet_cur, 0,
                sizeof(UINT32), streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].used_rows_cnt, 0,
                sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][1].used_rows_cnt, 0,
                sizeof(UINT32), streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].bitmap_size,param->get_bitmap_size() * param-> test_1_size/param
                ->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][1].bitmap_size,param->get_bitmap_size() * param-> test_2_size/param
                ->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_used_size,0,
                sizeof(UINT32)* param-> test_1_size/param ->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_used_size,0,
                sizeof(UINT32)* param-> test_2_size/param ->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> test_2_size/param->device_cnt, streams[i]));

            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->test_1_size/param->device_cnt, streams[i]));
            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->test_1_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->test_2_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].merge_tmp, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].used_rows, 0,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].mark_TID_start_offset, 0,
                sizeof(UINT32) , streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][1].mark_TID_start_offset, 0,
                sizeof(UINT32) , streams[i]));
        }
    } else if (param->benchmark == "TPCC_PART") {
        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));


            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_used_size,0,
                sizeof(UINT32)* param-> warehouse_size/ param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));

            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));


            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_used_size,0,
                sizeof(UINT32)* param-> district_size/ param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].merge_tmp, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

            for (uint32_t j = 0; j < param->table_cnt; ++j) {
                CHECK(cudaMemsetAsync(aux_struct_d_h[i][j].used_rows, 0,
                    sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

                CHECK(cudaMemsetAsync(&aux_struct_d[i][j].mark_TID_start_offset, 0,
                    sizeof(UINT32) , streams[i]));

                CHECK(cudaMemsetAsync(&aux_struct_d[i][j].used_rows_cnt, 0,
                    sizeof(UINT32) , streams[i]));
            }

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].data_packet_cur, 0,
                sizeof(UINT32) , streams[i]));


            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].bitmap_size,param->get_bitmap_size() * param-> warehouse_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][1].bitmap_size,param->get_bitmap_size() * param-> district_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][2].bitmap_size,param->get_bitmap_size() * param-> customer_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][3].bitmap_size,param->get_bitmap_size() * param-> neworder_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][4].bitmap_size,param->get_bitmap_size() * param-> history_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][5].bitmap_size,param->get_bitmap_size() * param-> order_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][6].bitmap_size,param->get_bitmap_size() * param-> orderline_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][7].bitmap_size,param->get_bitmap_size() * param-> stock_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][8].bitmap_size,param->get_bitmap_size() * param-> item_size,
                sizeof(UINT32), streams[i]));
        }
    } else if (param->benchmark == "TPCC_ALL") {
        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));


            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_used_size,0,
                sizeof(UINT32)* param-> warehouse_size/ param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> warehouse_size/param->device_cnt, streams[i]));

            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->warehouse_size/param->device_cnt, streams[i]));


            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_used_size,0,
                sizeof(UINT32)* param-> district_size/ param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> district_size/param->device_cnt, streams[i]));
            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][1].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->district_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][2].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->customer_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][3].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->neworder_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][4].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->history_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][5].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->order_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][6].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->orderline_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][7].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->stock_size/param->device_cnt, streams[i]));


            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][8].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->item_size, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].merge_tmp, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));


            for (uint32_t j = 0; j < param->table_cnt; ++j) {
                CHECK(cudaMemsetAsync(aux_struct_d_h[i][j].used_rows, 0,
                    sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

                CHECK(cudaMemsetAsync(&aux_struct_d[i][j].mark_TID_start_offset, 0,
                    sizeof(UINT32) , streams[i]));

                CHECK(cudaMemsetAsync(&aux_struct_d[i][j].used_rows_cnt, 0,
                    sizeof(UINT32) , streams[i]));
            }

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].data_packet_cur, 0,
                sizeof(UINT32) , streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].bitmap_size,param->get_bitmap_size() * param-> warehouse_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][1].bitmap_size,param->get_bitmap_size() * param-> district_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][2].bitmap_size,param->get_bitmap_size() * param-> customer_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][3].bitmap_size,param->get_bitmap_size() * param-> neworder_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][4].bitmap_size,param->get_bitmap_size() * param-> history_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][5].bitmap_size,param->get_bitmap_size() * param-> order_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][6].bitmap_size,param->get_bitmap_size() * param-> orderline_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][7].bitmap_size,param->get_bitmap_size() * param-> stock_size/
                param->device_cnt, sizeof(UINT32), streams[i]));
            CHECK(cudaMemsetAsync(&aux_struct_d[i][8].bitmap_size,param->get_bitmap_size() * param-> item_size,
                sizeof(UINT32), streams[i]));
        }
    } else if (param->benchmark == "YCSB_A" ||
               param->benchmark == "YCSB_B" ||
               param->benchmark == "YCSB_C" ||
               param->benchmark == "YCSB_D" ||
               param->benchmark == "YCSB_E") {
        for (size_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaSetDevice(param->device_IDs[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].data_packet_cur, 0,
                sizeof(UINT32), streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].used_rows_cnt, 0,
                sizeof(UINT32), streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].bitmap_size,param->get_bitmap_size() *
                param-> bitmap_row_cnt , sizeof(UINT32), streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_used_size,
                0, sizeof(UINT32)* param-> bitmap_row_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> bitmap_row_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_all_row, 0,
                sizeof(UINT32) * param->get_sub_txn_size(), streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_tmp, 0,
                sizeof(UINT32) * param->get_bitmap_size() * param-> bitmap_row_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> bitmap_row_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_offset, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> bitmap_row_cnt, streams[i]));
            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].bitmap_mark_compressed, 0,
                sizeof(UINT32) * param->get_bitmap_size()*32 * param -> bitmap_row_cnt, streams[i]));

            // CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].min_TID, 0xff,
            //     sizeof(UINT32) * 1 * param->ycsb_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].cnt_TID, 0,
                sizeof(UINT32) * 1 * param->ycsb_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].tmp_TID, 0,
                sizeof(UINT32) * 1 * param->ycsb_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID_offset, 0,
                sizeof(UINT32) * 1 * param->ycsb_size/param->device_cnt, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].mark_TID, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].merge_tmp, 0xff,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

            CHECK(cudaMemsetAsync(aux_struct_d_h[i][0].used_rows, 0,
                sizeof(UINT32) * param->get_sub_txn_size()*2, streams[i]));

            CHECK(cudaMemsetAsync(&aux_struct_d[i][0].mark_TID_start_offset, 0,
                sizeof(UINT32) , streams[i]));
        }
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamDestroy(streams[i]));
    }
    delete[] streams;

    std::cout << "end gpuquery.cu GPUquery::clear_global_txn()" << std::endl;
}

void GPUquery::free_global_txn(std::shared_ptr<Param> param,
                               std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                               Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuquery.cu GPUquery::free_global_txn()" << std::endl;

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(global_txn_info_d[i]));
    }
    CHECK(cudaFreeHost(global_txn_info_d));
    CHECK(cudaFreeHost(global_txn_info_h));

    // for (size_t k = 0; k < param->get_subtxn_kinds(); ++k) {
    //     CHECK(cudaFreeHost(global_txn[k].subtxn));
    // }
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(global_txn_d[i]));
        CHECK(cudaFreeHost(global_txn_h[i]));
    }
    CHECK(cudaFreeHost(global_txn_d));
    CHECK(cudaFreeHost(global_txn_h));
    CHECK(cudaFreeHost(global_txn));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(global_txn_exec_h[i][0].select_txn_mark));
        CHECK(cudaFree(global_txn_exec_h[i][0].insert_txn_mark));
        CHECK(cudaFree(global_txn_exec_h[i][0].update_txn_mark));
        CHECK(cudaFree(global_txn_exec_h[i][0].scan_txn_mark));
        CHECK(cudaFree(global_txn_exec_h[i][0].delete_txn_mark));

        CHECK(cudaFree(global_txn_exec_d[i]));
        CHECK(cudaFreeHost(global_txn_exec_h[i]));
    }
    CHECK(cudaFreeHost(global_txn_exec_d));
    CHECK(cudaFreeHost(global_txn_exec_h));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(global_txn_result_d[i]));
        CHECK(cudaFreeHost(global_txn_result_h[i]));
    }
    CHECK(cudaFreeHost(global_txn_result_d));
    CHECK(cudaFreeHost(global_txn_result_h));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(exec_param_d[i]));
    }
    CHECK(cudaFreeHost(exec_param_h));
    CHECK(cudaFreeHost(exec_param_d));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        if (param->benchmark == "TEST") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));

            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][1].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        } else if (param->benchmark == "TPCC_PART") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));

            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][1].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].used_rows));

            for (uint32_t j = 2; j < param->table_cnt; ++j) {
                // CHECK(cudaFree(aux_struct_d_h[i][j].min_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].cnt_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].tmp_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].mark_TID_offset));
                CHECK(cudaFree(aux_struct_d_h[i][j].mark_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].used_rows));
            }
        } else if (param->benchmark == "TPCC_ALL") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));

            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][1].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][1].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][1].mark_TID));

            for (uint32_t j = 2; j < param->table_cnt; ++j) {
                // CHECK(cudaFree(aux_struct_d_h[i][j].min_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].cnt_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].tmp_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].mark_TID_offset));
                CHECK(cudaFree(aux_struct_d_h[i][j].mark_TID));
                CHECK(cudaFree(aux_struct_d_h[i][j].used_rows));
            }
        } else if (param->benchmark == "YCSB_A") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        } else if (param->benchmark == "YCSB_B") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        } else if (param->benchmark == "YCSB_C") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        } else if (param->benchmark == "YCSB_D") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        } else if (param->benchmark == "YCSB_E") {
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_all_row));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_tmp));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_mark_compressed));
            CHECK(cudaFree(aux_struct_d_h[i][0].bitmap_used_size));
            // CHECK(cudaFree(aux_struct_d_h[i][0].min_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].cnt_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].tmp_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID_offset));
            CHECK(cudaFree(aux_struct_d_h[i][0].mark_TID));
            CHECK(cudaFree(aux_struct_d_h[i][0].used_rows));
        }
        CHECK(cudaFree(aux_struct_d[i]));
        CHECK(cudaFreeHost(aux_struct_d_h[i]));
    }
    CHECK(cudaFreeHost(aux_struct_d));
    CHECK(cudaFreeHost(aux_struct_d_h));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(data_packet_d[i]));
        CHECK(cudaFreeHost(data_packet_h[i]));
    }
    CHECK(cudaFreeHost(data_packet_d));
    CHECK(cudaFreeHost(data_packet_h));

    std::cout << "start gpuquery.cu GPUQuery::free_global_txn()" << std::endl;
}

void GPUquery::transfer_data_packet(std::shared_ptr<Param> param, cudaStream_t *streams) {
    // std::cout << "start gpuquery.cu GPUQuery::transfer_data_packet()" << std::endl;

    if (param->device_cnt == 2) {
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_datapacket_size()/2, param->device_IDs[0],
            data_packet_d[1] , param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[1]));
        // 1->0
        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_datapacket_size()/2, param->device_IDs[1],
            data_packet_d[0] , param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[0]));
        // 0->1
    }
    if (param->device_cnt == 4) {
#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_sub_txn_size()/4, param->device_IDs[1],
            data_packet_d[0], param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[0]));
        // 0->1
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_sub_txn_size()/4, param->device_IDs[0],
            data_packet_d[1], param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[1]));
        // 1->0
        CHECK(cudaMemcpyPeerAsync(data_packet_d[3] + param->get_sub_txn_size()/4, param->device_IDs[3],
            data_packet_d[2], param->device_IDs[2],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[2]));
        // 2->3
        CHECK(cudaMemcpyPeerAsync(data_packet_d[2] + param->get_sub_txn_size()/4, param->device_IDs[2],
            data_packet_d[3], param->device_IDs[3],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[3]));
        // 3->2

        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }
#endif

        CHECK(cudaMemcpyPeerAsync(data_packet_d[2] + param->get_datapacket_size()/2, param->device_IDs[2],
            data_packet_d[0], param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[0]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[3] + param->get_datapacket_size()/2, param->device_IDs[3],
            data_packet_d[1], param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[1]));
        // 01->23
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_datapacket_size()/2, param->device_IDs[0],
            data_packet_d[2], param->device_IDs[2],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[2]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_datapacket_size()/2, param->device_IDs[1],
            data_packet_d[3], param->device_IDs[3],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[3]));
        // 23->01
    }
    if (param->device_cnt == 8) {
#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP

        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_sub_txn_size()/8, param->device_IDs[1],
            data_packet_d[0], param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[0] ));
        // 0->1
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_sub_txn_size()/8, param->device_IDs[0],
            data_packet_d[1], param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[1] ));
        // 1->0
        CHECK(cudaMemcpyPeerAsync(data_packet_d[3] + param->get_sub_txn_size()/8, param->device_IDs[3],
            data_packet_d[2], param->device_IDs[2],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[2] ));
        // 2->3
        CHECK(cudaMemcpyPeerAsync(data_packet_d[2] + param->get_sub_txn_size()/8, param->device_IDs[2],
            data_packet_d[3], param->device_IDs[3],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[3] ));
        // 3->2
        CHECK(cudaMemcpyPeerAsync(data_packet_d[5] + param->get_sub_txn_size()/8, param->device_IDs[5],
            data_packet_d[4], param->device_IDs[4],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[4] ));
        // 4->5
        CHECK(cudaMemcpyPeerAsync(data_packet_d[4] + param->get_sub_txn_size()/8, param->device_IDs[4],
            data_packet_d[5], param->device_IDs[5],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[5] ));
        // 5->4
        CHECK(cudaMemcpyPeerAsync(data_packet_d[7] + param->get_sub_txn_size()/8, param->device_IDs[7],
            data_packet_d[6], param->device_IDs[6],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[6] ));
        // 6->7
        CHECK(cudaMemcpyPeerAsync(data_packet_d[6] + param->get_sub_txn_size()/8, param->device_IDs[6],
            data_packet_d[7], param->device_IDs[7],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/8, streams[7] ));
        // 7->6

        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }

        CHECK(cudaMemcpyPeerAsync(data_packet_d[2] + param->get_sub_txn_size()/4, param->device_IDs[2],
            data_packet_d[0], param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[0]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[3] + param->get_sub_txn_size()/4, param->device_IDs[3],
            data_packet_d[1], param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[1]));
        // 01->23
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_sub_txn_size()/4, param->device_IDs[0],
            data_packet_d[2], param->device_IDs[2],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[2]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_sub_txn_size()/4, param->device_IDs[1],
            data_packet_d[3], param->device_IDs[3],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[3]));
        // 23->01
        CHECK(cudaMemcpyPeerAsync(data_packet_d[6] + param->get_sub_txn_size()/4, param->device_IDs[6],
            data_packet_d[4], param->device_IDs[4],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[4]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[7] + param->get_sub_txn_size()/4, param->device_IDs[7],
            data_packet_d[5], param->device_IDs[5],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[5]));
        // 45->67
        CHECK(cudaMemcpyPeerAsync(data_packet_d[4] + param->get_sub_txn_size()/4, param->device_IDs[4],
            data_packet_d[6], param->device_IDs[6],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[6]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[5] + param->get_sub_txn_size()/4, param->device_IDs[5],
            data_packet_d[7], param->device_IDs[7],
            sizeof(Global_Data_Packet) * param->get_sub_txn_size()/4, streams[7]));
        // 67->45

        for (uint32_t i = 0; i < param->device_cnt; ++i) {
            CHECK(cudaStreamSynchronize(streams[i]));
        }
#endif

        CHECK(cudaMemcpyPeerAsync(data_packet_d[4] + param->get_datapacket_size()/2, param->device_IDs[4],
            data_packet_d[0], param->device_IDs[0],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[0]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[5] + param->get_datapacket_size()/2, param->device_IDs[5],
            data_packet_d[1], param->device_IDs[1],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[1]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[6] + param->get_datapacket_size()/2, param->device_IDs[6],
            data_packet_d[2], param->device_IDs[2],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[2]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[7] + param->get_datapacket_size()/2, param->device_IDs[7],
            data_packet_d[3], param->device_IDs[3],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[3]));
        // 0123->4567
        CHECK(cudaMemcpyPeerAsync(data_packet_d[0] + param->get_datapacket_size()/2, param->device_IDs[0],
            data_packet_d[4], param->device_IDs[4],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[4]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[1] + param->get_datapacket_size()/2, param->device_IDs[1],
            data_packet_d[5], param->device_IDs[5],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[5]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[2] + param->get_datapacket_size()/2, param->device_IDs[2],
            data_packet_d[6], param->device_IDs[6],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[6]));
        CHECK(cudaMemcpyPeerAsync(data_packet_d[3] + param->get_datapacket_size()/2, param->device_IDs[3],
            data_packet_d[7], param->device_IDs[7],
            sizeof(Global_Data_Packet) * param->get_datapacket_size()/2, streams[7]));
        // 4567->0123
    }

    // std::cout << "end gpuquery.cu GPUQuery::transfer_data_packet()" << std::endl;
}

template<>
void GPUquery::gen_param<Test_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::test_query_gen_param()" << std::endl;

    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_sub_txn_size = param->get_sub_txn_size();
    }

    // test_query
    global_txn_info_h[0].select_cnt = 1 * param->test_query_batch_size;
    global_txn_info_h[0].cur_subtxn_cnt = 1;

    global_txn_info_h[1].insert_cnt = 1 * param->test_query_batch_size;
    global_txn_info_h[1].cur_subtxn_cnt = 1;

    global_txn_info_h[2].update_cnt = 1 * param->test_query_batch_size;
    global_txn_info_h[2].cur_subtxn_cnt = 1;

    global_txn_info_h[3].scan_cnt = 1 * param->test_query_batch_size;
    global_txn_info_h[3].delete_cnt = 1 * param->test_query_batch_size;
    global_txn_info_h[3].cur_subtxn_cnt = 2;

    // test_query_2
    global_txn_info_h[4].select_cnt = 1 * param->test_query_2_batch_size;
    global_txn_info_h[4].cur_subtxn_cnt = 1;

    global_txn_info_h[5].select_cnt = 1 * param->test_query_2_batch_size;
    global_txn_info_h[5].cur_subtxn_cnt = 1;

    global_txn_info_h[6].select_cnt = 1 * param->test_query_2_batch_size;
    global_txn_info_h[6].cur_subtxn_cnt = 1;

    global_txn_info_h[7].select_cnt = 1 * param->test_query_2_batch_size;
    global_txn_info_h[7].cur_subtxn_cnt = 1;

    global_txn_info_h[8].select_cnt = 1 * param->test_query_2_batch_size;
    global_txn_info_h[8].cur_subtxn_cnt = 1;

    exec_param_h[0].target_platform = 1;
    exec_param_h[0].target_GPU = 0xffffffff;
    exec_param_h[0].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[0].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[0].batch_size = param->batch_size;

    exec_param_h[1].target_platform = 1;
    exec_param_h[1].target_GPU = 0xffffffff;
    exec_param_h[1].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[1].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[1].batch_size = param->batch_size;

    exec_param_h[2].target_platform = 1;
    exec_param_h[2].target_GPU = 0xffffffff;
    exec_param_h[2].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[2].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[2].batch_size = param->batch_size;

    exec_param_h[3].target_platform = 1;
    exec_param_h[3].target_GPU = 0xffffffff;
    exec_param_h[3].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[3].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[3].batch_size = param->batch_size;

    exec_param_h[4].target_platform = 1;
    exec_param_h[4].target_GPU = 0xffffffff;
    exec_param_h[4].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[4].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[4].batch_size = param->batch_size;

    exec_param_h[5].target_platform = 1;
    exec_param_h[5].target_GPU = 0xffffffff;
    exec_param_h[5].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[5].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[5].batch_size = param->batch_size;

    exec_param_h[6].target_platform = 1;
    exec_param_h[6].target_GPU = 0xffffffff;
    exec_param_h[6].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[6].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[6].batch_size = param->batch_size;

    exec_param_h[7].target_platform = 1;
    exec_param_h[7].target_GPU = 0xffffffff;
    exec_param_h[7].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[7].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[7].batch_size = param->batch_size;

    exec_param_h[8].target_platform = 1;
    exec_param_h[8].target_GPU = 0xffffffff;
    exec_param_h[8].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[8].global_sub_txn_size = param->test_sub_txn_size;
    exec_param_h[8].batch_size = param->batch_size;

    std::cout << "end gpuquery.cu GPUquery::test_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<TPCC_PART>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::tpcc_part_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
    }

    global_txn_info_h[0].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[0].cur_subtxn_cnt = 1;
    exec_param_h[0].target_platform = 1;
    exec_param_h[0].target_GPU = 0xffffffff;
    exec_param_h[0].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[0].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[0].batch_size = param->batch_size;

    global_txn_info_h[1].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[1].cur_subtxn_cnt = 1;
    exec_param_h[1].target_platform = 1;
    exec_param_h[1].target_GPU = 0xffffffff;
    exec_param_h[1].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[1].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[1].batch_size = param->batch_size;

    global_txn_info_h[2].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[2].cur_subtxn_cnt = 1;
    exec_param_h[2].target_platform = 1;
    exec_param_h[2].target_GPU = 0xffffffff;
    exec_param_h[2].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[2].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[2].batch_size = param->batch_size;

    global_txn_info_h[3].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[3].cur_subtxn_cnt = 1;
    exec_param_h[3].target_platform = 1;
    exec_param_h[3].target_GPU = 0xffffffff;
    exec_param_h[3].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[3].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[3].batch_size = param->batch_size;

    global_txn_info_h[4].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[4].cur_subtxn_cnt = 1;
    exec_param_h[4].target_platform = 1;
    exec_param_h[4].target_GPU = 0xffffffff;
    exec_param_h[4].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[4].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[4].batch_size = param->batch_size;

    global_txn_info_h[5].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[5].cur_subtxn_cnt = 2;
    exec_param_h[5].target_platform = 1;
    exec_param_h[5].target_GPU = 0xffffffff;
    exec_param_h[5].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[5].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[5].batch_size = param->batch_size;

    global_txn_info_h[6].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[6].cur_subtxn_cnt = 2;
    exec_param_h[6].target_platform = 1;
    exec_param_h[6].target_GPU = 0xffffffff;
    exec_param_h[6].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[6].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[6].batch_size = param->batch_size;

    global_txn_info_h[7].update_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[7].cur_subtxn_cnt = 1;
    exec_param_h[7].target_platform = 1;
    exec_param_h[7].target_GPU = 0xffffffff;
    exec_param_h[7].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[7].global_sub_txn_size = param->neworder_query_batch_size;
    exec_param_h[7].batch_size = param->batch_size;

    global_txn_info_h[8].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[8].cur_subtxn_cnt = 1;
    exec_param_h[8].target_platform = 1;
    exec_param_h[8].target_GPU = 0xffffffff;
    exec_param_h[8].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[8].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[8].batch_size = param->batch_size;

    global_txn_info_h[9].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[9].cur_subtxn_cnt = 1;
    exec_param_h[9].target_platform = 1;
    exec_param_h[9].target_GPU = 0xffffffff;
    exec_param_h[9].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[9].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[9].batch_size = param->batch_size;

    global_txn_info_h[10].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[10].cur_subtxn_cnt = 1;
    exec_param_h[10].target_platform = 1;
    exec_param_h[10].target_GPU = 0xffffffff;
    exec_param_h[10].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[10].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[10].batch_size = param->batch_size;

    global_txn_info_h[11].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[11].cur_subtxn_cnt = 1;
    exec_param_h[11].target_platform = 1;
    exec_param_h[11].target_GPU = 0xffffffff;
    exec_param_h[11].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[11].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[11].batch_size = param->batch_size;

    global_txn_info_h[12].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[12].cur_subtxn_cnt = 1;
    exec_param_h[12].target_platform = 1;
    exec_param_h[12].target_GPU = 0xffffffff;
    exec_param_h[12].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[12].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[12].batch_size = param->batch_size;

    global_txn_info_h[13].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[13].cur_subtxn_cnt = 1;
    exec_param_h[13].target_platform = 1;
    exec_param_h[13].target_GPU = 0xffffffff;
    exec_param_h[13].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[13].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[13].batch_size = param->batch_size;

    global_txn_info_h[14].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[14].cur_subtxn_cnt = 1;
    exec_param_h[14].target_platform = 1;
    exec_param_h[14].target_GPU = 0xffffffff;
    exec_param_h[14].global_txn_info_size = param->get_subtxn_kinds();
    exec_param_h[14].global_sub_txn_size = param->payment_query_batch_size;
    exec_param_h[14].batch_size = param->batch_size;
    std::cout << "end gpuquery.cu GPUquery::tpcc_part_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<TPCC_ALL>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::tpcc_all_query_gen_param()" << std::endl;

    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }

    global_txn_info_h[0].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[0].cur_subtxn_cnt = 1;
    exec_param_h[0].target_platform = 1;
    exec_param_h[0].target_GPU = 0xffffffff;
    exec_param_h[0].global_sub_txn_size = param->neworder_query_batch_size;


    global_txn_info_h[1].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[1].cur_subtxn_cnt = 1;
    exec_param_h[1].target_platform = 1;
    exec_param_h[1].target_GPU = 0xffffffff;
    exec_param_h[1].global_sub_txn_size = param->neworder_query_batch_size;


    global_txn_info_h[2].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[2].cur_subtxn_cnt = 1;
    exec_param_h[2].target_platform = 1;
    exec_param_h[2].target_GPU = 0xffffffff;
    exec_param_h[2].global_sub_txn_size = param->neworder_query_batch_size;


    global_txn_info_h[3].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[3].cur_subtxn_cnt = 1;
    exec_param_h[3].target_platform = 1;
    exec_param_h[3].target_GPU = 0xffffffff;
    exec_param_h[3].global_sub_txn_size = param->neworder_query_batch_size;

    global_txn_info_h[4].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[4].cur_subtxn_cnt = 1;
    exec_param_h[4].target_platform = 1;
    exec_param_h[4].target_GPU = 0xffffffff;
    exec_param_h[4].global_sub_txn_size = param->neworder_query_batch_size;

    global_txn_info_h[5].select_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[5].cur_subtxn_cnt = 1;
    exec_param_h[5].target_platform = 1;
    exec_param_h[5].target_GPU = 0xffffffff;
    exec_param_h[5].global_sub_txn_size = param->neworder_query_batch_size;

    global_txn_info_h[6].insert_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[6].cur_subtxn_cnt = 2;
    exec_param_h[6].target_platform = 1;
    exec_param_h[6].target_GPU = 0xffffffff;
    exec_param_h[6].global_sub_txn_size = param->neworder_query_batch_size;

    global_txn_info_h[7].update_cnt = 1 * param->neworder_query_batch_size;
    global_txn_info_h[7].cur_subtxn_cnt = 1;
    exec_param_h[7].target_platform = 1;
    exec_param_h[7].target_GPU = 0xffffffff;
    exec_param_h[7].global_sub_txn_size = param->neworder_query_batch_size;

    //payment
    global_txn_info_h[8].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[8].cur_subtxn_cnt = 1;
    exec_param_h[8].target_platform = 1;
    exec_param_h[8].target_GPU = 0xffffffff;
    exec_param_h[8].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[9].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[9].cur_subtxn_cnt = 1;
    exec_param_h[9].target_platform = 1;
    exec_param_h[9].target_GPU = 0xffffffff;
    exec_param_h[9].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[10].select_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[10].cur_subtxn_cnt = 1;
    exec_param_h[10].target_platform = 1;
    exec_param_h[10].target_GPU = 0xffffffff;
    exec_param_h[10].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[11].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[11].cur_subtxn_cnt = 1;
    exec_param_h[11].target_platform = 1;
    exec_param_h[11].target_GPU = 0xffffffff;
    exec_param_h[11].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[12].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[12].cur_subtxn_cnt = 1;
    exec_param_h[12].target_platform = 1;
    exec_param_h[12].target_GPU = 0xffffffff;
    exec_param_h[12].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[13].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[13].cur_subtxn_cnt = 1;
    exec_param_h[13].target_platform = 1;
    exec_param_h[13].target_GPU = 0xffffffff;
    exec_param_h[13].global_sub_txn_size = param->payment_query_batch_size;

    global_txn_info_h[14].update_cnt = 1 * param->payment_query_batch_size;
    global_txn_info_h[14].cur_subtxn_cnt = 1;
    exec_param_h[14].target_platform = 1;
    exec_param_h[14].target_GPU = 0xffffffff;
    exec_param_h[14].global_sub_txn_size = param->payment_query_batch_size;

    // Orderstatus
    global_txn_info_h[15].select_cnt = 1 * param->orderstatus_query_batch_size;
    global_txn_info_h[15].cur_subtxn_cnt = 1;
    exec_param_h[15].target_platform = 1;
    exec_param_h[15].target_GPU = 0xffffffff;
    exec_param_h[15].global_sub_txn_size = param->orderstatus_query_batch_size;

    global_txn_info_h[16].select_cnt = 1 * param->orderstatus_query_batch_size;
    global_txn_info_h[16].cur_subtxn_cnt = 1;
    exec_param_h[16].target_platform = 1;
    exec_param_h[16].target_GPU = 0xffffffff;
    exec_param_h[16].global_sub_txn_size = param->orderstatus_query_batch_size;

    global_txn_info_h[17].select_cnt = 1 * param->orderstatus_query_batch_size;
    global_txn_info_h[17].cur_subtxn_cnt = 1;
    exec_param_h[17].target_platform = 1;
    exec_param_h[17].target_GPU = 0xffffffff;
    exec_param_h[17].global_sub_txn_size = param->orderstatus_query_batch_size;

    // Delivery
    global_txn_info_h[18].delete_cnt = 1 * param->delivery_query_batch_size;
    global_txn_info_h[18].cur_subtxn_cnt = 1;
    exec_param_h[18].target_platform = 1;
    exec_param_h[18].target_GPU = 0xffffffff;
    exec_param_h[18].global_sub_txn_size = param->delivery_query_batch_size;

    global_txn_info_h[19].update_cnt = 1 * param->delivery_query_batch_size;
    global_txn_info_h[19].cur_subtxn_cnt = 1;
    exec_param_h[19].target_platform = 1;
    exec_param_h[19].target_GPU = 0xffffffff;
    exec_param_h[19].global_sub_txn_size = param->delivery_query_batch_size;

    global_txn_info_h[20].update_cnt = 1 * param->delivery_query_batch_size;
    global_txn_info_h[20].cur_subtxn_cnt = 1;
    exec_param_h[20].target_platform = 1;
    exec_param_h[20].target_GPU = 0xffffffff;
    exec_param_h[20].global_sub_txn_size = param->delivery_query_batch_size;

    global_txn_info_h[21].select_cnt = 1 * param->delivery_query_batch_size;
    global_txn_info_h[21].cur_subtxn_cnt = 1;
    exec_param_h[21].target_platform = 1;
    exec_param_h[21].target_GPU = 0xffffffff;
    exec_param_h[21].global_sub_txn_size = param->delivery_query_batch_size;

    global_txn_info_h[22].update_cnt = 1 * param->delivery_query_batch_size;
    global_txn_info_h[22].cur_subtxn_cnt = 1;
    exec_param_h[22].target_platform = 1;
    exec_param_h[22].target_GPU = 0xffffffff;
    exec_param_h[22].global_sub_txn_size = param->delivery_query_batch_size;

    // Stocklevel
    global_txn_info_h[23].select_cnt = 1 * param->stocklevel_query_batch_size;
    global_txn_info_h[23].cur_subtxn_cnt = 1;
    exec_param_h[23].target_platform = 1;
    exec_param_h[23].target_GPU = 0xffffffff;
    exec_param_h[23].global_sub_txn_size = param->stocklevel_query_batch_size;

    global_txn_info_h[24].select_cnt = 1 * param->stocklevel_query_batch_size;
    global_txn_info_h[24].cur_subtxn_cnt = 1;
    exec_param_h[24].target_platform = 1;
    exec_param_h[24].target_GPU = 0xffffffff;
    exec_param_h[24].global_sub_txn_size = param->stocklevel_query_batch_size;

    global_txn_info_h[25].select_cnt = 1 * param->stocklevel_query_batch_size;
    global_txn_info_h[25].cur_subtxn_cnt = 1;
    exec_param_h[25].target_platform = 1;
    exec_param_h[25].target_GPU = 0xffffffff;
    exec_param_h[25].global_sub_txn_size = param->stocklevel_query_batch_size;
    std::cout << "end gpuquery.cu GPUquery::tpcc_all_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<YCSB_A_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::ycsb_a_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }
    for (uint32_t i = 0; i < 5; ++i) {
        global_txn_info_h[i].select_cnt = 1 * param->ycsb_a_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_a_query_batch_size;
    }
    for (uint32_t i = 5; i < 10; ++i) {
        global_txn_info_h[i].update_cnt = 1 * param->ycsb_a_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_a_query_batch_size;
    }
    std::cout << "end gpuquery.cu GPUquery::ycsb_a_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<YCSB_B_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::ycsb_b_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }
    for (uint32_t i = 0; i < 9; ++i) {
        global_txn_info_h[i].select_cnt = 1 * param->ycsb_b_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_b_query_batch_size;
    }

    global_txn_info_h[9].update_cnt = 1 * param->ycsb_b_query_batch_size;
    global_txn_info_h[9].cur_subtxn_cnt = 1;
    exec_param_h[9].target_platform = 1;
    exec_param_h[9].target_GPU = 0xffffffff;
    exec_param_h[9].global_sub_txn_size = param->ycsb_b_query_batch_size;
    std::cout << "end gpuquery.cu GPUquery::ycsb_b_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<YCSB_C_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::ycsb_c_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }
    for (uint32_t i = 0; i < 10; ++i) {
        global_txn_info_h[i].select_cnt = 1 * param->ycsb_c_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_c_query_batch_size;
    }
    std::cout << "end gpuquery.cu GPUquery::ycsb_c_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<YCSB_D_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::ycsb_d_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }
    for (uint32_t i = 0; i < 9; ++i) {
        global_txn_info_h[i].select_cnt = 1 * param->ycsb_d_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_d_query_batch_size;
    }

    global_txn_info_h[9].update_cnt = 1 * param->ycsb_d_query_batch_size;
    global_txn_info_h[9].cur_subtxn_cnt = 1;
    exec_param_h[9].target_platform = 1;
    exec_param_h[9].target_GPU = 0xffffffff;
    exec_param_h[9].global_sub_txn_size = param->ycsb_d_query_batch_size;
    std::cout << "end gpuquery.cu GPUquery::ycsb_d_query_gen_param()" << std::endl;
}

template<>
void GPUquery::gen_param<YCSB_E_Query>(std::shared_ptr<Param> param) {
    std::cout << "start gpuquery.cu GPUquery::ycsb_e_query_gen_param()" << std::endl;
    for (uint32_t i = 0; i < param->get_subtxn_kinds(); ++i) {
        exec_param_h[i].bitmap_size = param->batch_size % 32 > 0
                                          ? param->batch_size / 32 + 1
                                          : param->batch_size / 32;
        exec_param_h[i].global_txn_info_size = param->get_subtxn_kinds();
        exec_param_h[i].batch_size = param->batch_size;
    }
    for (uint32_t i = 0; i < 9; ++i) {
        global_txn_info_h[i].scan_cnt = 1 * param->ycsb_e_query_batch_size;
        global_txn_info_h[i].cur_subtxn_cnt = 1;
        exec_param_h[i].target_platform = 1;
        exec_param_h[i].target_GPU = 0xffffffff;
        exec_param_h[i].global_sub_txn_size = param->ycsb_e_query_batch_size;
    }

    global_txn_info_h[9].insert_cnt = 1 * param->ycsb_e_query_batch_size;
    global_txn_info_h[9].cur_subtxn_cnt = 1;
    exec_param_h[9].target_platform = 1;
    exec_param_h[9].target_GPU = 0xffffffff;
    exec_param_h[9].global_sub_txn_size = param->ycsb_e_query_batch_size;
    std::cout << "end gpuquery.cu GPUquery::ycsb_e_query_gen_param()" << std::endl;
}

template<typename T>
void GPUquery::query_parse(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                           Global_Table_Meta **meta, Global_Table_Index **index) {
    std::cout << "start gpuquery.cu GPUquery::query_parse()" << std::endl;

    cudaStream_t *streams;
    streams = new cudaStream_t[param->device_cnt];
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaStreamCreate(&streams[i]));
    }


    T *t_query_h;
    T **t_query_d;
    CHECK(cudaHostAlloc((void **)&t_query_h, sizeof(T) * param->get_txn_batch_size(typeid(T)), cudaHostAllocDefault));
    uint32_t cur = 0;
    for (auto transaction = transactions_batch_ptr->begin(); transaction != transactions_batch_ptr->end(); ++
         transaction) {
        if (typeid(T) == transaction->type()) {
            t_query_h[cur] = std::any_cast<T>(*transaction);
            ++cur;
        }
    }

    CHECK(cudaHostAlloc((void **)&t_query_d, sizeof(T) * param->device_cnt, cudaHostAllocDefault));
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&t_query_d[i], sizeof(T) * param->get_txn_batch_size(typeid(T))));
    }

    // std::cout << get_global_txn_start(param, typeid(T)) << std::endl;

    long long start_parse = gpu_current_time();
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMemcpyAsync(t_query_d[i], t_query_h, sizeof(T) * param->get_txn_batch_size(typeid(T)),
            cudaMemcpyHostToDevice, streams[i]));
        parse<T><<<512, 512, 0, streams[i]>>>(param->get_txn_batch_size(typeid(T)),
                                              get_global_txn_start(param, typeid(T)), global_txn_info_d[i],
                                              t_query_d[i], global_txn_d[i], exec_param_d[i], meta[i], index[i]);
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }
    long long end_parse = gpu_current_time();
    float cost_parse = gpu_duration(start_parse, end_parse);
    std::cout << "cost_parse:" << cost_parse << "s." << std::endl;
    CHECK(cudaMemcpy(global_txn, global_txn_d[0],
        sizeof(Global_Txn) * param->get_sub_txn_size(), cudaMemcpyDeviceToHost));


    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(t_query_d[i]));
    }
    CHECK(cudaFreeHost(t_query_d));
    CHECK(cudaFreeHost(t_query_h));

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamDestroy(streams[i]));
    }

    delete[] streams;

    std::cout << "end gpuquery.cu GPUquery::query_parse()" << std::endl;
}

uint32_t GPUquery::get_global_txn_info_ID(std::shared_ptr<Param> param, uint32_t sub_txn_ID,
                                          const std::type_info &txn_type) {
    uint32_t result = sub_txn_ID;
    if (param->benchmark == "TEST") {
        if (txn_type == typeid(Test_Query)) {
            return result;
        } else if (txn_type == typeid(Test_Query_2)) {
            result += 5;
            return result;
        }
    } else if (param->benchmark == "TPCC_PART") {
        if (txn_type == typeid(Neworder_Query)) {
            if (sub_txn_ID < 5) {
                result = sub_txn_ID;
            } else {
                result = 5 + (sub_txn_ID - 5) % 3;
            }
            return result;
        } else if (txn_type == typeid(Payment_Query)) {
            result += 8;
            return result;
        }
    } else if (param->benchmark == "TPCC_ALL") {
        if (txn_type == typeid(Neworder_Query)) {
            if (sub_txn_ID < 5) {
                result = sub_txn_ID;
            } else {
                result = 5 + (sub_txn_ID - 5) % 3;
            }
            return result;
        } else if (txn_type == typeid(Payment_Query)) {
            result += 8;
            return result;
        } else if (txn_type == typeid(Orderstatus_Query)) {
            result += 8;
            result += 7;
            return result;
        } else if (txn_type == typeid(Delivery_Query)) {
            result = sub_txn_ID % 5;
            result += 8;
            result += 7;
            result += 3;
            return result;
        } else if (txn_type == typeid(Stocklevel_Query)) {
            if (sub_txn_ID > 0)
                result = 1 + (sub_txn_ID - 1) % 2;
            result += 8;
            result += 7;
            result += 3;
            result += 5;
            return result;
        }
    } else if (param->benchmark == "YCSB_A") {
        return result;
    } else if (param->benchmark == "YCSB_B") {
        return result;
    } else if (param->benchmark == "YCSB_C") {
        return result;
    } else if (param->benchmark == "YCSB_D") {
        return result;
    } else if (param->benchmark == "YCSB_E") {
        return result;
    }

    return 0;
}

uint32_t GPUquery::get_global_txn_start(std::shared_ptr<Param> param, const std::type_info &txn_type) {
    uint32_t result = 0;
    if (param->benchmark == "TEST") {
        if (txn_type == typeid(Test_Query)) {
            return result;
        } else if (txn_type == typeid(Test_Query_2)) {
            result += param->test_query_subtxn_cnt * param->test_query_batch_size;
            return result;
        }
    } else if (param->benchmark == "TPCC_PART") {
        if (txn_type == typeid(Neworder_Query)) {
            return result;
        } else if (txn_type == typeid(Payment_Query)) {
            result += param->neworder_query_subtxn_cnt * param->neworder_query_batch_size;
            return result;
        }
    } else if (param->benchmark == "TPCC_ALL") {
        if (txn_type == typeid(Neworder_Query)) {
            return result;
        } else if (txn_type == typeid(Payment_Query)) {
            result += param->neworder_query_subtxn_cnt * param->neworder_query_batch_size;
            return result;
        } else if (txn_type == typeid(Orderstatus_Query)) {
            result += param->neworder_query_subtxn_cnt * param->neworder_query_batch_size;
            result += param->payment_query_subtxn_cnt * param->payment_query_batch_size;
            return result;
        } else if (txn_type == typeid(Delivery_Query)) {
            result += param->neworder_query_subtxn_cnt * param->neworder_query_batch_size;
            result += param->payment_query_subtxn_cnt * param->payment_query_batch_size;
            result += param->orderstatus_query_subtxn_cnt * param->orderstatus_query_batch_size;
            return result;
        } else if (txn_type == typeid(Stocklevel_Query)) {
            result += param->neworder_query_subtxn_cnt * param->neworder_query_batch_size;
            result += param->payment_query_subtxn_cnt * param->payment_query_batch_size;
            result += param->orderstatus_query_subtxn_cnt * param->orderstatus_query_batch_size;
            result += param->delivery_query_subtxn_cnt * param->delivery_query_batch_size;
            return result;
        }
    } else if (param->benchmark == "YCSB_A") {
        return result;
    } else if (param->benchmark == "YCSB_B") {
        return result;
    } else if (param->benchmark == "YCSB_C") {
        return result;
    } else if (param->benchmark == "YCSB_D") {
        return result;
    } else if (param->benchmark == "YCSB_E") {
        return result;
    }
    return 0;
}

Global_Txn_Info *GPUquery::get_txn_info(const int deviceID) {
    return global_txn_info_d[deviceID];
}

Global_Txn *GPUquery::get_txn(const int deviceID) {
    return global_txn_d[deviceID];
}

Global_Txn_Exec *GPUquery::get_txn_exec(const int deviceID) {
    return global_txn_exec_d[deviceID];
}

Global_Txn_Result *GPUquery::get_txn_result(const int deviceID) {
    return global_txn_result_d[deviceID];
}

Global_Txn_Exec_Param *GPUquery::get_exec_param(const int deviceID) {
    return exec_param_d[deviceID];
}

Global_Txn_Aux_Struct *GPUquery::get_aux_struct(const int deviceID) {
    return aux_struct_d[deviceID];
}

Global_Data_Packet *GPUquery::get_data_packet(const int deviceID) {
    return data_packet_d[deviceID];
}

template<>
__global__ void parse<Test_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Test_Query *query,
                                  Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                  Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 0;
        txn[start + 0 * size + cur].subtxn.type = 0; // select
        txn[start + 0 * size + cur].subtxn.table_ID = 1;
        txn[start + 0 * size + cur].subtxn.benchmark = 1;
        txn[start + 0 * size + cur].subtxn.dest_Row_1 = query[cur].Row_0;
        txn[start + 0 * size + cur].subtxn.dest_device = 0xffffffff; // query[cur].Row_0 / meta[1].table_slice_size;
        txn[start + 0 * size + cur].dest_device = 0xffffffff; // txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].sub_txn_cnt = 1;
        txn[start + 0 * size + cur].global_txn_info_ID = 0;

        txn[start + 1 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 1 * size + cur].subtxn.ispopular = 0;
        txn[start + 1 * size + cur].subtxn.type = 1; // insert
        txn[start + 1 * size + cur].subtxn.table_ID = 1;
        txn[start + 1 * size + cur].subtxn.benchmark = 1;
        txn[start + 1 * size + cur].subtxn.dest_Row_1 = query[cur].Row_1;
        txn[start + 1 * size + cur].subtxn.dest_device = query[cur].Row_1 / meta[1].table_slice_size;
        txn[start + 1 * size + cur].dest_device = txn[start + 1 * size + cur].subtxn.dest_device;
        txn[start + 1 * size + cur].global_txn_info_ID = 1;

        txn[start + 2 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 2 * size + cur].subtxn.ispopular = 1;
        txn[start + 2 * size + cur].subtxn.type = 2; // update
        txn[start + 2 * size + cur].subtxn.table_ID = 1;
        txn[start + 2 * size + cur].subtxn.benchmark = 1;
        txn[start + 2 * size + cur].subtxn.dest_Row_1 = query[cur].Row_2;
        txn[start + 2 * size + cur].subtxn.dest_device = query[cur].Row_2 / meta[1].table_slice_size;
        txn[start + 2 * size + cur].dest_device = txn[start + 2 * size + cur].subtxn.dest_device;
        txn[start + 2 * size + cur].global_txn_info_ID = 2;

        txn[start + 3 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 3 * size + cur].subtxn.ispopular = 0;
        txn[start + 3 * size + cur].subtxn.type = 3; // scan
        txn[start + 3 * size + cur].subtxn.table_ID = 1;
        txn[start + 3 * size + cur].subtxn.benchmark = 1;
        txn[start + 3 * size + cur].subtxn.dest_Row_1 = query[cur].Row_3;
        txn[start + 3 * size + cur].subtxn.dest_device = query[cur].Row_3 / meta[1].table_slice_size;
        txn[start + 3 * size + cur].dest_device = txn[start + 3 * size + cur].subtxn.dest_device;
        txn[start + 3 * size + cur].global_txn_info_ID = 3;

        txn[start + 4 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 4 * size + cur].subtxn.ispopular = 0;
        txn[start + 4 * size + cur].subtxn.type = 4; // delete
        txn[start + 4 * size + cur].subtxn.table_ID = 1;
        txn[start + 4 * size + cur].subtxn.benchmark = 1;
        txn[start + 4 * size + cur].subtxn.dest_Row_1 = query[cur].Row_4;
        txn[start + 4 * size + cur].subtxn.dest_device = query[cur].Row_4 / meta[1].table_slice_size;
        txn[start + 4 * size + cur].subtxn.dest_Row_2 = query[cur].Row_5;
        txn[start + 4 * size + cur].subtxn.dest_device = query[cur].Row_5 / meta[1].table_slice_size;
        txn[start + 4 * size + cur].dest_device = txn[start + 3 * size + cur].subtxn.dest_device;
        txn[start + 4 * size + cur].global_txn_info_ID = 4;

        // printf(
        //     "cur:%d,ID:%d %d %d %d %d,TID:%d,%d %d %d %d %d,Row:%d %d %d %d %d %d,dest_device:%d %d %d %d %d,ispopular:%d %d %d %d %d\n",
        //     cur, start + 0 * size + cur, start + 1 * size + cur,
        //     start + 2 * size + cur, start + 3 * size + cur, start + 3 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type,
        //     txn[start + 1 * size + cur].subtxn.type, txn[start + 2 * size + cur].subtxn.type,
        //     txn[start + 3 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn[1].type,
        //     txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 3 * size + cur].subtxn[1].dest_Row_1, txn[start + 3 * size + cur].subtxn[1].dest_Row_2,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 3 * size + cur].dest_device,
        //     txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        //     txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        //     txn[start + 3 * size + cur].subtxn[1].ispopular);

        // printf("ID:%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
        //        cur, txn_info[0].cur_subtxn_cnt, txn_info[1].cur_subtxn_cnt,
        //        txn_info[2].cur_subtxn_cnt, txn_info[3].cur_subtxn_cnt,
        //        param[0].target_platform, param[1].target_platform,
        //        param[2].target_platform, param[3].target_platform);

        cur += thSize;
    }
}

template<>
__global__ void parse<Test_Query_2>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Test_Query_2 *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 1;
        txn[start + 0 * size + cur].subtxn.type = 0;
        txn[start + 0 * size + cur].subtxn.table_ID = 0;
        txn[start + 0 * size + cur].subtxn.benchmark = 1;
        txn[start + 0 * size + cur].subtxn.dest_Row_1 = query[cur].Row_0;
        txn[start + 0 * size + cur].subtxn.dest_device = query[cur].Row_0 / meta[0].table_slice_size;
        txn[start + 0 * size + cur].dest_device = txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].global_txn_info_ID = 5;

        txn[start + 1 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 1 * size + cur].subtxn.ispopular = 0;
        txn[start + 1 * size + cur].subtxn.type = 0;
        txn[start + 1 * size + cur].subtxn.table_ID = 0;
        txn[start + 1 * size + cur].subtxn.benchmark = 1;
        txn[start + 1 * size + cur].subtxn.dest_Row_1 = query[cur].Row_1;
        txn[start + 1 * size + cur].subtxn.dest_device = query[cur].Row_1 / meta[0].table_slice_size;
        txn[start + 1 * size + cur].dest_device = txn[start + 1 * size + cur].subtxn.dest_device;
        txn[start + 1 * size + cur].global_txn_info_ID = 6;

        txn[start + 2 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 2 * size + cur].subtxn.ispopular = 0;
        txn[start + 2 * size + cur].subtxn.type = 0;
        txn[start + 2 * size + cur].subtxn.table_ID = 0;
        txn[start + 2 * size + cur].subtxn.benchmark = 1;
        txn[start + 2 * size + cur].subtxn.dest_Row_1 = query[cur].Row_2;
        txn[start + 2 * size + cur].subtxn.dest_device = query[cur].Row_2 / meta[0].table_slice_size;
        txn[start + 2 * size + cur].dest_device = txn[start + 2 * size + cur].subtxn.dest_device;
        txn[start + 2 * size + cur].global_txn_info_ID = 7;

        txn[start + 3 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 3 * size + cur].subtxn.ispopular = 0;
        txn[start + 3 * size + cur].subtxn.type = 0;
        txn[start + 3 * size + cur].subtxn.table_ID = 0;
        txn[start + 3 * size + cur].subtxn.benchmark = 1;
        txn[start + 3 * size + cur].subtxn.dest_Row_1 = query[cur].Row_3;
        txn[start + 3 * size + cur].subtxn.dest_device = query[cur].Row_3 / meta[0].table_slice_size;
        txn[start + 3 * size + cur].dest_device = txn[start + 3 * size + cur].subtxn.dest_device;
        txn[start + 3 * size + cur].global_txn_info_ID = 8;

        txn[start + 4 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 4 * size + cur].subtxn.ispopular = 0;
        txn[start + 4 * size + cur].subtxn.type = 0;
        txn[start + 4 * size + cur].subtxn.table_ID = 0;
        txn[start + 4 * size + cur].subtxn.benchmark = 1;
        txn[start + 4 * size + cur].subtxn.dest_Row_1 = query[cur].Row_4;
        txn[start + 4 * size + cur].subtxn.dest_device = query[cur].Row_4 / meta[0].table_slice_size;
        txn[start + 4 * size + cur].dest_device = txn[start + 4 * size + cur].subtxn.dest_device;
        txn[start + 4 * size + cur].global_txn_info_ID = 9;

        // printf(
        //     "cur:%d,ID:%d %d %d %d %d,TID:%d,%d %d %d %d %d,Row:%d %d %d %d %d,dest_device:%d %d %d %d %d,ispopular:%d %d %d %d %d\n",
        //     cur, start + 0 * size + cur, start + 1 * size + cur,
        //     start + 2 * size + cur, start + 3 * size + cur, start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type,
        //     txn[start + 1 * size + cur].subtxn.type, txn[start + 2 * size + cur].subtxn.type,
        //     txn[start + 3 * size + cur].subtxn.type, txn[start + 4 * size + cur].subtxn.type,
        //     txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 4 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device,
        //     txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        //     txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        //     txn[start + 4 * size + cur].subtxn.ispopular);

        // printf("ID:%d,%d,%d,%d,%d,%d,%d,%d,%d\n",
        //        cur, txn_info[0].cur_subtxn_cnt, txn_info[1].cur_subtxn_cnt,
        //        txn_info[2].cur_subtxn_cnt, txn_info[3].cur_subtxn_cnt,
        //        param[0].target_platform, param[1].target_platform,
        //        param[2].target_platform, param[3].target_platform);

        cur += thSize;
    }
}

template<>
__global__ void parse<Neworder_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Neworder_Query *query,
                                      Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                      Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 1;
        txn[start + 0 * size + cur].subtxn.type = 0;
        txn[start + 0 * size + cur].subtxn.table_ID = 0;
        txn[start + 0 * size + cur].subtxn.benchmark = 2;
        txn[start + 0 * size + cur].subtxn.dest_Row_1 = query[cur].W_ID;
        txn[start + 0 * size + cur].subtxn.dest_device = query[cur].W_ID / meta[0].table_slice_size;
        txn[start + 0 * size + cur].dest_device = txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].global_txn_info_ID = 0;

        txn[start + 1 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 1 * size + cur].subtxn.ispopular = 1;
        txn[start + 1 * size + cur].subtxn.type = 0;
        txn[start + 1 * size + cur].subtxn.table_ID = 1;
        txn[start + 1 * size + cur].subtxn.benchmark = 2;
        txn[start + 1 * size + cur].subtxn.dest_Row_1 =
                query[cur].W_ID * 10 + query[cur].D_ID;
        txn[start + 1 * size + cur].subtxn.dest_device =
                (query[cur].W_ID * 10 + query[cur].D_ID) / meta[1].table_slice_size;
        txn[start + 1 * size + cur].dest_device = txn[start + 1 * size + cur].subtxn.dest_device;
        txn[start + 1 * size + cur].global_txn_info_ID = 1;

        txn[start + 2 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 2 * size + cur].subtxn.ispopular = 0;
        txn[start + 2 * size + cur].subtxn.type = 0;
        txn[start + 2 * size + cur].subtxn.table_ID = 2;
        txn[start + 2 * size + cur].subtxn.benchmark = 2;
        txn[start + 2 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].C_ID;
        txn[start + 2 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].C_ID) / meta[2].table_slice_size;
        txn[start + 2 * size + cur].dest_device = txn[start + 2 * size + cur].subtxn.dest_device;
        txn[start + 2 * size + cur].global_txn_info_ID = 2;

        txn[start + 3 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 3 * size + cur].subtxn.ispopular = 0;
        txn[start + 3 * size + cur].subtxn.type = 1;
        txn[start + 3 * size + cur].subtxn.table_ID = 3;
        txn[start + 3 * size + cur].subtxn.benchmark = 2;
        txn[start + 3 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].N_O_ID;
        txn[start + 3 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].N_O_ID) / meta[3].table_slice_size;
        txn[start + 3 * size + cur].dest_device = txn[start + 3 * size + cur].subtxn.dest_device;
        txn[start + 3 * size + cur].global_txn_info_ID = 3;

        txn[start + 4 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 4 * size + cur].subtxn.ispopular = 0;
        txn[start + 4 * size + cur].subtxn.type = 1;
        txn[start + 4 * size + cur].subtxn.table_ID = 5;
        txn[start + 4 * size + cur].subtxn.benchmark = 2;
        txn[start + 4 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].O_ID;
        txn[start + 4 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].O_ID) / meta[5].table_slice_size;
        txn[start + 4 * size + cur].dest_device = txn[start + 4 * size + cur].subtxn.dest_device;
        txn[start + 4 * size + cur].global_txn_info_ID = 4;

        for (uint32_t i = 0; i < query[cur].O_OL_CNT; ++i) {
            txn[start + (5 + 3 * i) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (5 + 3 * i) * size + cur].subtxn.ispopular = 0;
            txn[start + (5 + 3 * i) * size + cur].subtxn.type = 1;
            txn[start + (5 + 3 * i) * size + cur].subtxn.table_ID = 6;
            txn[start + (5 + 3 * i) * size + cur].subtxn.benchmark = 2;
            txn[start + (5 + 3 * i) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].O_OL_ID;
            txn[start + (5 + 3 * i) * size + cur].subtxn.dest_device =
                    ((query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].O_OL_ID) / meta[6].table_slice_size;
            txn[start + (5 + 3 * i) * size + cur].dest_device = txn[start + (5 + 3 * i) * size + cur].subtxn.
                    dest_device;
            txn[start + (5 + 3 * i) * size + cur].global_txn_info_ID = 5;

            txn[start + (6 + 3 * i) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (6 + 3 * i) * size + cur].subtxn.ispopular = 0;
            txn[start + (6 + 3 * i) * size + cur].subtxn.type = 2;
            txn[start + (6 + 3 * i) * size + cur].subtxn.table_ID = 7;
            txn[start + (6 + 3 * i) * size + cur].subtxn.benchmark = 2;
            txn[start + (6 + 3 * i) * size + cur].subtxn.dest_Row_1 =
                    query[cur].INFO[i].OL_SUPPLY_W_ID * 1000000 + query[cur].INFO[i].OL_I_ID;
            txn[start + (6 + 3 * i) * size + cur].subtxn.dest_device =
                    (query[cur].INFO[i].OL_SUPPLY_W_ID * 1000000 + query[cur].INFO[i].OL_I_ID) / meta[7].
                    table_slice_size;
            txn[start + (6 + 3 * i) * size + cur].dest_device = txn[start + (6 + 3 * i) * size + cur].subtxn.
                    dest_device;
            txn[start + (6 + 3 * i) * size + cur].global_txn_info_ID = 6;

            txn[start + (7 + 3 * i) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (7 + 3 * i) * size + cur].subtxn.ispopular = 0;
            txn[start + (7 + 3 * i) * size + cur].subtxn.type = 2;
            txn[start + (7 + 3 * i) * size + cur].subtxn.table_ID = 7;
            txn[start + (7 + 3 * i) * size + cur].subtxn.benchmark = 2;
            txn[start + (7 + 3 * i) * size + cur].subtxn.dest_Row_1 =
                    query[cur].INFO[i].OL_SUPPLY_W_ID * 1000000 + query[cur].INFO[i].OL_I_ID;
            txn[start + (7 + 3 * i) * size + cur].subtxn.dest_device =
                    (query[cur].INFO[i].OL_SUPPLY_W_ID * 1000000 + query[cur].INFO[i].OL_I_ID) / meta[7].
                    table_slice_size;
            txn[start + (7 + 3 * i) * size + cur].dest_device = txn[start + (7 + 3 * i) * size + cur].subtxn.
                    dest_device;
            txn[start + (7 + 3 * i) * size + cur].global_txn_info_ID = 7;
        }
        // printf(
        //     "cur:%d,ID:%d %d %d %d %d"
        //     ",TID:%d,%d %d %d %d %d"
        //     ",Row:%d %d %d %d %d %d"
        //     ",dest_device:%d %d %d %d %d"
        //     // ",ispopular:%d %d %d %d %d"
        //     "\n",
        //     cur, start + 0 * size + cur, start + 1 * size + cur,
        //     start + 2 * size + cur, start + 3 * size + cur, start + 3 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type,
        //     txn[start + 1 * size + cur].subtxn.type, txn[start + 2 * size + cur].subtxn.type,
        //     txn[start + 3 * size + cur].subtxn.type, txn[start + 4 * size + cur].subtxn.type,
        //     query[cur].W_ID, query[cur].D_ID, query[cur].C_ID, query[cur].O_ID, query[cur].N_O_ID, query[cur].O_OL_CNT,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device
        //     // ,
        //     // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        //     // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        //     // txn[start + 3 * size + cur].subtxn[1].ispopular
        // );
        cur += thSize;
    }
}

template<>
__global__ void parse<Payment_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Payment_Query *query,
                                     Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                     Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 1;
        txn[start + 0 * size + cur].subtxn.type = 0;
        txn[start + 0 * size + cur].subtxn.table_ID = 0;
        txn[start + 0 * size + cur].subtxn.benchmark = 2;
        txn[start + 0 * size + cur].subtxn.dest_Row_1 = query[cur].W_ID;
        txn[start + 0 * size + cur].subtxn.dest_device = query[cur].W_ID / meta[0].table_slice_size;
        txn[start + 0 * size + cur].dest_device = txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].global_txn_info_ID = 8;

        txn[start + 1 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 1 * size + cur].subtxn.ispopular = 1;
        txn[start + 1 * size + cur].subtxn.type = 0;
        txn[start + 1 * size + cur].subtxn.table_ID = 1;
        txn[start + 1 * size + cur].subtxn.benchmark = 2;
        txn[start + 1 * size + cur].subtxn.dest_Row_1 = query[cur].W_ID * 10 + query[cur].D_ID;
        txn[start + 1 * size + cur].subtxn.dest_device =
                (query[cur].W_ID * 10 + query[cur].D_ID) / meta[1].table_slice_size;
        txn[start + 1 * size + cur].dest_device = txn[start + 1 * size + cur].subtxn.dest_device;
        txn[start + 1 * size + cur].global_txn_info_ID = 9;

        txn[start + 2 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 2 * size + cur].subtxn.ispopular = 0;
        txn[start + 2 * size + cur].subtxn.type = 0;
        txn[start + 2 * size + cur].subtxn.table_ID = 2;
        txn[start + 2 * size + cur].subtxn.benchmark = 2;
        if (query[cur].isName == 1) {
            uint32_t C_ID = 0;
            for (uint32_t i = 0; i < 3000; i++) {
                if (index[2].index[(query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + i] == query[cur].C_LAST) {
                    C_ID = i;
                    break;
                }
            }
            query[cur].C_ID = C_ID;
        }
        txn[start + 2 * size + cur].subtxn.dest_Row_1 =
                (query[cur].C_W_ID * 10 + query[cur].C_D_ID) * 3000 + query[cur].C_ID;
        txn[start + 2 * size + cur].subtxn.dest_device =
                ((query[cur].C_W_ID * 10 + query[cur].C_D_ID) * 3000 + query[cur].C_ID) / meta[2].table_slice_size;
        txn[start + 2 * size + cur].dest_device = txn[start + 2 * size + cur].subtxn.dest_device;
        txn[start + 2 * size + cur].global_txn_info_ID = 10;

        txn[start + 3 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 3 * size + cur].subtxn.ispopular = 0;
        txn[start + 3 * size + cur].subtxn.type = 2;
        txn[start + 3 * size + cur].subtxn.table_ID = 2;
        txn[start + 3 * size + cur].subtxn.benchmark = 2;
        txn[start + 3 * size + cur].subtxn.dest_Row_1 =
                (query[cur].C_W_ID * 10 + query[cur].C_D_ID) * 3000 + query[cur].C_ID;
        txn[start + 3 * size + cur].subtxn.dest_device =
                ((query[cur].C_W_ID * 10 + query[cur].C_D_ID) * 3000 + query[cur].C_ID) / meta[2].table_slice_size;
        txn[start + 3 * size + cur].dest_device = txn[start + 3 * size + cur].subtxn.dest_device;
        txn[start + 3 * size + cur].global_txn_info_ID = 11;

        txn[start + 4 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 4 * size + cur].subtxn.ispopular = 1;
        txn[start + 4 * size + cur].subtxn.type = 2;
        txn[start + 4 * size + cur].subtxn.table_ID = 0;
        txn[start + 4 * size + cur].subtxn.benchmark = 2;
        txn[start + 4 * size + cur].subtxn.dest_Row_1 = query[cur].W_ID;
        txn[start + 4 * size + cur].subtxn.dest_device = query[cur].W_ID / meta[0].table_slice_size;
        txn[start + 4 * size + cur].dest_device = txn[start + 4 * size + cur].subtxn.dest_device;
        txn[start + 4 * size + cur].global_txn_info_ID = 12;

        txn[start + 5 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 5 * size + cur].subtxn.ispopular = 1;
        txn[start + 5 * size + cur].subtxn.type = 2;
        txn[start + 5 * size + cur].subtxn.table_ID = 1;
        txn[start + 5 * size + cur].subtxn.benchmark = 2;
        txn[start + 5 * size + cur].subtxn.dest_Row_1 =
                query[cur].W_ID * 10 + query[cur].D_ID;
        txn[start + 5 * size + cur].subtxn.dest_device =
                (query[cur].W_ID * 10 + query[cur].D_ID) / meta[1].table_slice_size;
        txn[start + 5 * size + cur].dest_device = txn[start + 5 * size + cur].subtxn.dest_device;
        txn[start + 5 * size + cur].global_txn_info_ID = 13;

        txn[start + 6 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 6 * size + cur].subtxn.ispopular = 0;
        txn[start + 6 * size + cur].subtxn.type = 1;
        txn[start + 6 * size + cur].subtxn.table_ID = 4;
        txn[start + 6 * size + cur].subtxn.benchmark = 2;
        txn[start + 6 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].H_ID;
        txn[start + 6 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].H_ID) / meta[4].table_slice_size;
        txn[start + 6 * size + cur].dest_device = txn[start + 6 * size + cur].subtxn.dest_device;
        txn[start + 6 * size + cur].global_txn_info_ID = 14;

        // printf(
        //     "cur:%d,ID:%d %d %d %d %d %d %d"
        //     ",TID:%d"
        //     ",type:%d %d %d %d %d"
        //     // ",Row:%d %d %d %d %d %d"
        //     // ",dest_device:%d %d %d %d %d"
        //     ",ispopular:%d %d %d %d %d %d %d"
        //     "\n",
        //     cur, start + 0 * size + cur, start + 1 * size + cur,
        //     start + 2 * size + cur, start + 3 * size + cur,
        //     start + 4 * size + cur, start + 5 * size + cur, start + 6 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type,
        //     txn[start + 1 * size + cur].subtxn.type, txn[start + 2 * size + cur].subtxn.type,
        //     txn[start + 3 * size + cur].subtxn.type, txn[start + 4 * size + cur].subtxn.type,
        //     // query[cur].W_ID, query[cur].D_ID, query[cur].isName, query[cur].C_ID, query[cur].C_LAST,
        //     // query[cur].H_AMOUNT,
        //     // txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     // txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     // txn[start + 4 * size + cur].dest_device
        //     // ,
        //     txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        //     txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        //     txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        //     txn[start + 6 * size + cur].subtxn.ispopular
        // );

        cur += thSize;
    }
}

template<>
__global__ void parse<Orderstatus_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info,
                                         Orderstatus_Query *query, Global_Txn *txn, Global_Txn_Exec_Param *param,
                                         Global_Table_Meta *meta, Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 0;
        txn[start + 0 * size + cur].subtxn.type = 0;
        txn[start + 0 * size + cur].subtxn.table_ID = 0;
        txn[start + 0 * size + cur].subtxn.benchmark = 2;
        if (query[cur].isName == 1) {
            uint32_t C_ID = 0;
            for (uint32_t i = 0; i < 3000; i++) {
                if (index[2].index[(query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + i] == query[cur].C_LAST) {
                    C_ID = i;
                    break;
                }
            }
            query[cur].C_ID = C_ID;
        }
        txn[start + 0 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].C_ID;
        txn[start + 0 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].C_ID) / meta[0].table_slice_size;
        txn[start + 0 * size + cur].dest_device = txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].global_txn_info_ID = 15;

        txn[start + 1 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 1 * size + cur].subtxn.ispopular = 0;
        txn[start + 1 * size + cur].subtxn.type = 0;
        txn[start + 1 * size + cur].subtxn.table_ID = 5;
        txn[start + 1 * size + cur].subtxn.benchmark = 2;
        txn[start + 1 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].O_ID;
        txn[start + 1 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 3000 + query[cur].O_ID) / meta[5].table_slice_size;
        txn[start + 1 * size + cur].dest_device = txn[start + 1 * size + cur].subtxn.dest_device;
        txn[start + 1 * size + cur].global_txn_info_ID = 16;

        txn[start + 2 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 2 * size + cur].subtxn.ispopular = 0;
        txn[start + 2 * size + cur].subtxn.type = 0;
        txn[start + 2 * size + cur].subtxn.table_ID = 6;
        txn[start + 2 * size + cur].subtxn.benchmark = 2;
        txn[start + 2 * size + cur].subtxn.dest_Row_1 =
                (query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].OL_ID;
        txn[start + 2 * size + cur].subtxn.dest_device =
                ((query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].OL_ID) / meta[6].table_slice_size;
        txn[start + 2 * size + cur].dest_device = txn[start + 2 * size + cur].subtxn.dest_device;
        txn[start + 2 * size + cur].global_txn_info_ID = 17;

        cur += thSize;
    }
}

template<>
__global__ void parse<Delivery_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Delivery_Query *query,
                                      Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                      Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        for (uint32_t i = 0; i < 10; ++i) {
            txn[start + (0 + i * 5) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (0 + i * 5) * size + cur].subtxn.ispopular = 0;
            txn[start + (0 + i * 5) * size + cur].subtxn.type = 3;
            txn[start + (0 + i * 5) * size + cur].subtxn.table_ID = 3;
            txn[start + (0 + i * 5) * size + cur].subtxn.benchmark = 2;
            txn[start + (0 + i * 5) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i];
            txn[start + (0 + i * 5) * size + cur].subtxn.dest_device =
                    ((query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i]) / meta[3].
                    table_slice_size;
            txn[start + (0 + i * 5) * size + cur].dest_device = txn[start + (0 + i * 5) * size + cur].subtxn.
                    dest_device;
            txn[start + (0 + i * 5) * size + cur].global_txn_info_ID = 18;

            txn[start + (1 + i * 5) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (1 + i * 5) * size + cur].subtxn.ispopular = 0;
            txn[start + (1 + i * 5) * size + cur].subtxn.type = 2;
            txn[start + (1 + i * 5) * size + cur].subtxn.table_ID = 5;
            txn[start + (1 + i * 5) * size + cur].subtxn.benchmark = 2;
            txn[start + (1 + i * 5) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i];
            txn[start + (1 + i * 5) * size + cur].subtxn.dest_device =
                    ((query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i]) / meta[5].
                    table_slice_size;
            txn[start + (1 + i * 5) * size + cur].dest_device = txn[start + (1 + i * 5) * size + cur].subtxn.
                    dest_device;
            txn[start + (1 + i * 5) * size + cur].global_txn_info_ID = 19;

            txn[start + (2 + i * 5) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (2 + i * 5) * size + cur].subtxn.ispopular = 0;
            txn[start + (2 + i * 5) * size + cur].subtxn.type = 2;
            txn[start + (2 + i * 5) * size + cur].subtxn.table_ID = 6;
            txn[start + (2 + i * 5) * size + cur].subtxn.benchmark = 2;
            txn[start + (2 + i * 5) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i];
            txn[start + (2 + i * 5) * size + cur].subtxn.dest_device =
                    ((query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i]) / meta[6].
                    table_slice_size;
            txn[start + (2 + i * 5) * size + cur].dest_device = txn[start + (2 + i * 5) * size + cur].subtxn.
                    dest_device;
            txn[start + (2 + i * 5) * size + cur].global_txn_info_ID = 20;

            txn[start + (3 + i * 5) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (3 + i * 5) * size + cur].subtxn.ispopular = 0;
            txn[start + (3 + i * 5) * size + cur].subtxn.type = 0;
            txn[start + (3 + i * 5) * size + cur].subtxn.table_ID = 6;
            txn[start + (3 + i * 5) * size + cur].subtxn.benchmark = 2;
            txn[start + (3 + i * 5) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i];
            txn[start + (3 + i * 5) * size + cur].subtxn.dest_device =
                    ((query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_O_ID[i]) / meta[6].
                    table_slice_size;
            txn[start + (3 + i * 5) * size + cur].dest_device = txn[start + (3 + i * 5) * size + cur].subtxn.
                    dest_device;
            txn[start + (3 + i * 5) * size + cur].global_txn_info_ID = 21;

            txn[start + (4 + i * 5) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (4 + i * 5) * size + cur].subtxn.ispopular = 0;
            txn[start + (4 + i * 5) * size + cur].subtxn.type = 2;
            txn[start + (4 + i * 5) * size + cur].subtxn.table_ID = 2;
            txn[start + (4 + i * 5) * size + cur].subtxn.benchmark = 2;
            txn[start + (4 + i * 5) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_C_ID[i];
            txn[start + (4 + i * 5) * size + cur].subtxn.dest_device =
                    ((query[cur].NO_W_ID[i] * 10 + query[cur].NO_D_ID[i]) * 3000 + query[cur].NO_C_ID[i]) / meta[2].
                    table_slice_size;
            txn[start + (4 + i * 5) * size + cur].dest_device = txn[start + (4 + i * 5) * size + cur].subtxn.
                    dest_device;
            txn[start + (4 + i * 5) * size + cur].global_txn_info_ID = 22;
        }

        cur += thSize;
    }
}

template<>
__global__ void parse<Stocklevel_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info,
                                        Stocklevel_Query *query, Global_Txn *txn, Global_Txn_Exec_Param *param,
                                        Global_Table_Meta *meta, Global_Table_Index *index) {
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        txn[start + 0 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 0 * size + cur].subtxn.ispopular = 0;
        txn[start + 0 * size + cur].subtxn.type = 0;
        txn[start + 0 * size + cur].subtxn.table_ID = 1;
        txn[start + 0 * size + cur].subtxn.benchmark = 2;
        txn[start + 0 * size + cur].subtxn.dest_Row_1 =
                query[cur].W_ID * 10 + query[cur].D_ID;
        txn[start + 0 * size + cur].subtxn.dest_device =
                (query[cur].W_ID * 10 + query[cur].D_ID) / meta[1].table_slice_size;
        txn[start + 0 * size + cur].dest_device = txn[start + 0 * size + cur].subtxn.dest_device;
        txn[start + 0 * size + cur].global_txn_info_ID = 23;

        for (uint32_t i = 0; i < 10; ++i) {
            txn[start + (1 + i * 2) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (1 + i * 2) * size + cur].subtxn.ispopular = 0;
            txn[start + (1 + i * 2) * size + cur].subtxn.type = 0;
            txn[start + (1 + i * 2) * size + cur].subtxn.table_ID = 6;
            txn[start + (1 + i * 2) * size + cur].subtxn.benchmark = 2;
            txn[start + (1 + i * 2) * size + cur].subtxn.dest_Row_1 =
                    (query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].O_OL_ID[i];
            txn[start + (1 + i * 2) * size + cur].subtxn.dest_device =
                    ((query[cur].W_ID * 10 + query[cur].D_ID) * 45000 + query[cur].O_OL_ID[i]) / meta[6].
                    table_slice_size;
            txn[start + (1 + i * 2) * size + cur].dest_device = txn[start + (1 + i * 2) * size + cur].subtxn.
                    dest_device;
            txn[start + (1 + i * 5) * size + cur].global_txn_info_ID = 24;

            txn[start + (2 + i * 2) * size + cur].subtxn.TID = query[cur].TID;
            txn[start + (2 + i * 2) * size + cur].subtxn.ispopular = 0;
            txn[start + (2 + i * 2) * size + cur].subtxn.type = 0;
            txn[start + (2 + i * 2) * size + cur].subtxn.table_ID = 7;
            txn[start + (2 + i * 2) * size + cur].subtxn.benchmark = 2;
            txn[start + (2 + i * 2) * size + cur].subtxn.dest_Row_1 =
                    query[cur].W_ID * 100000 + query[cur].I_ID[i];
            txn[start + (2 + i * 2) * size + cur].subtxn.dest_device =
                    (query[cur].W_ID * 100000 + query[cur].I_ID[i]) / meta[7].table_slice_size;
            txn[start + (2 + i * 2) * size + cur].dest_device = txn[start + (2 + i * 2) * size + cur].subtxn.
                    dest_device;
            txn[start + (2 + i * 5) * size + cur].global_txn_info_ID = 25;
        }
        cur += thSize;
    }
}

template<>
__global__ void parse<YCSB_A_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_A_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    // 50% Read, 50% Write
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
#pragma unroll
        for (uint32_t i = 0; i < 5; ++i) {
            uint32_t ispopular = 0;
            if (query[cur].ROW_ID[i] < 100) {
                ispopular = 1;
            } else {
                ispopular = 0;
            }
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.ispopular = ispopular;
            txn[start + i * size + cur].subtxn.type = 0;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].subtxn.ispopular = 0;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }
        for (uint32_t i = 5; i < 10; ++i) {
            uint32_t ispopular = 0;
            if (query[cur].ROW_ID[i] < 100) {
                ispopular = 1;
            } else {
                ispopular = 0;
            }
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.ispopular = ispopular;
            txn[start + i * size + cur].subtxn.type = 2;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].subtxn.ispopular = 0;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }
        // printf(
        //     "cur:%d,"
        //     // "ID:%d %d %d %d %d,"
        //     "TID:%d,%d %d %d %d %d %d %d %d %d %d,"
        //     // "Row:%d %d %d %d %d %d %d %d %d %d,"
        //     "dest_device:%d %d %d %d %d %d %d %d %d %d\n"
        //     // "ispopular:%d %d %d %d %d %d %d %d %d %d\n"
        //     ,
        //     cur,
        //     // start + 0 * size + cur, start + 1 * size + cur,
        //     // start + 2 * size + cur, start + 3 * size + cur,
        //     // start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type, txn[start + 1 * size + cur].subtxn.type,
        //     txn[start + 2 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn.type,
        //     txn[start + 4 * size + cur].subtxn.type, txn[start + 5 * size + cur].subtxn.type,
        //     txn[start + 6 * size + cur].subtxn.type, txn[start + 7 * size + cur].subtxn.type,
        //     txn[start + 8 * size + cur].subtxn.type, txn[start + 9 * size + cur].subtxn.type,
        //     // txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 4 * size + cur].subtxn.dest_Row_1, txn[start + 5 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 6 * size + cur].subtxn.dest_Row_1, txn[start + 7 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 8 * size + cur].subtxn.dest_Row_1, txn[start + 9 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device, txn[start + 5 * size + cur].dest_device,
        //     txn[start + 6 * size + cur].dest_device, txn[start + 7 * size + cur].dest_device,
        //     txn[start + 8 * size + cur].dest_device, txn[start + 9 * size + cur].dest_device);
        // // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        // // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        // // txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        // // txn[start + 6 * size + cur].subtxn.ispopular, txn[start + 7 * size + cur].subtxn.ispopular,
        // // txn[start + 8 * size + cur].subtxn.ispopular, txn[start + 9 * size + cur].subtxn.ispopular);

        cur += thSize;
    }
}


template<>
__global__ void parse<YCSB_B_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_B_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    // 90% Read, 10% Write
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        uint32_t ispopular = 0;
        for (uint32_t i = 0; i < 9; ++i) {
            if (query[cur].ROW_ID[i] < 100) {
                ispopular = 1;
            } else {
                ispopular = 0;
            }
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.ispopular = ispopular;
            txn[start + i * size + cur].subtxn.type = 0;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].subtxn.ispopular = 0;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }
        if (query[cur].ROW_ID[9] < 100) {
            ispopular = 1;
        } else {
            ispopular = 0;
        }
        txn[start + 9 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 9 * size + cur].subtxn.ispopular = ispopular;
        txn[start + 9 * size + cur].subtxn.type = 2;
        txn[start + 9 * size + cur].subtxn.table_ID = 0;
        txn[start + 9 * size + cur].subtxn.benchmark = 4;
        txn[start + 9 * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[9];
        txn[start + 9 * size + cur].subtxn.dest_device = query[cur].ROW_ID[9] / meta[0].table_slice_size;
        txn[start + 9 * size + cur].dest_device = txn[start + 9 * size + cur].subtxn.dest_device;
        txn[start + 9 * size + cur].global_txn_info_ID = 9;

        // printf(
        //     "cur:%d,"
        //     // "ID:%d %d %d %d %d,"
        //     "TID:%d,%d %d %d %d %d %d %d %d %d %d,"
        //     // "Row:%d %d %d %d %d %d %d %d %d %d,"
        //     "dest_device:%d %d %d %d %d %d %d %d %d %d\n"
        //     // "ispopular:%d %d %d %d %d %d %d %d %d %d\n"
        //     ,
        //     cur,
        //     // start + 0 * size + cur, start + 1 * size + cur,
        //     // start + 2 * size + cur, start + 3 * size + cur,
        //     // start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type, txn[start + 1 * size + cur].subtxn.type,
        //     txn[start + 2 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn.type,
        //     txn[start + 4 * size + cur].subtxn.type, txn[start + 5 * size + cur].subtxn.type,
        //     txn[start + 6 * size + cur].subtxn.type, txn[start + 7 * size + cur].subtxn.type,
        //     txn[start + 8 * size + cur].subtxn.type, txn[start + 9 * size + cur].subtxn.type,
        //     // txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 4 * size + cur].subtxn.dest_Row_1, txn[start + 5 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 6 * size + cur].subtxn.dest_Row_1, txn[start + 7 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 8 * size + cur].subtxn.dest_Row_1, txn[start + 9 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device, txn[start + 5 * size + cur].dest_device,
        //     txn[start + 6 * size + cur].dest_device, txn[start + 7 * size + cur].dest_device,
        //     txn[start + 8 * size + cur].dest_device, txn[start + 9 * size + cur].dest_device);
        // // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        // // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        // // txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        // // txn[start + 6 * size + cur].subtxn.ispopular, txn[start + 7 * size + cur].subtxn.ispopular,
        // // txn[start + 8 * size + cur].subtxn.ispopular, txn[start + 9 * size + cur].subtxn.ispopular);

        cur += thSize;
    }
}

template<>
__global__ void parse<YCSB_C_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_C_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    // 100% Read
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        for (uint32_t i = 0; i < 10; ++i) {
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.ispopular = 0;
            txn[start + i * size + cur].subtxn.type = 0;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }

        // printf(
        //     "cur:%d,"
        //     // "ID:%d %d %d %d %d,"
        //     "TID:%d,%d %d %d %d %d %d %d %d %d %d,"
        //     // "Row:%d %d %d %d %d %d %d %d %d %d,"
        //     "dest_device:%d %d %d %d %d %d %d %d %d %d\n"
        //     // "ispopular:%d %d %d %d %d %d %d %d %d %d\n"
        //     ,
        //     cur,
        //     // start + 0 * size + cur, start + 1 * size + cur,
        //     // start + 2 * size + cur, start + 3 * size + cur,
        //     // start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type, txn[start + 1 * size + cur].subtxn.type,
        //     txn[start + 2 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn.type,
        //     txn[start + 4 * size + cur].subtxn.type, txn[start + 5 * size + cur].subtxn.type,
        //     txn[start + 6 * size + cur].subtxn.type, txn[start + 7 * size + cur].subtxn.type,
        //     txn[start + 8 * size + cur].subtxn.type, txn[start + 9 * size + cur].subtxn.type,
        //     // txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 4 * size + cur].subtxn.dest_Row_1, txn[start + 5 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 6 * size + cur].subtxn.dest_Row_1, txn[start + 7 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 8 * size + cur].subtxn.dest_Row_1, txn[start + 9 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device, txn[start + 5 * size + cur].dest_device,
        //     txn[start + 6 * size + cur].dest_device, txn[start + 7 * size + cur].dest_device,
        //     txn[start + 8 * size + cur].dest_device, txn[start + 9 * size + cur].dest_device);
        // // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        // // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        // // txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        // // txn[start + 6 * size + cur].subtxn.ispopular, txn[start + 7 * size + cur].subtxn.ispopular,
        // // txn[start + 8 * size + cur].subtxn.ispopular, txn[start + 9 * size + cur].subtxn.ispopular);

        cur += thSize;
    }
}

template<>
__global__ void parse<YCSB_D_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_D_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    // 95% Read, 5% Write
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        uint32_t ispopular = 0;

        for (uint32_t i = 0; i < 9; ++i) {
            if (query[cur].ROW_ID[i] < 100) {
                ispopular = 1;
            } else {
                ispopular = 0;
            }
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.ispopular = ispopular;
            txn[start + i * size + cur].subtxn.type = 0;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }
        if (query[cur].ROW_ID[9] < 100) {
            ispopular = 1;
        } else {
            ispopular = 0;
        }
        txn[start + 9 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 9 * size + cur].subtxn.ispopular = ispopular;
        txn[start + 9 * size + cur].subtxn.type = 2;
        txn[start + 9 * size + cur].subtxn.table_ID = 0;
        txn[start + 9 * size + cur].subtxn.benchmark = 4;
        txn[start + 9 * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[9];
        txn[start + 9 * size + cur].subtxn.dest_device = query[cur].ROW_ID[9] / meta[0].table_slice_size;
        txn[start + 9 * size + cur].dest_device = txn[start + 9 * size + cur].subtxn.dest_device;
        txn[start + 9 * size + cur].global_txn_info_ID = 9;


        // printf(
        //     "cur:%d,"
        //     // "ID:%d %d %d %d %d,"
        //     "TID:%d,%d %d %d %d %d %d %d %d %d %d,"
        //     // "Row:%d %d %d %d %d %d %d %d %d %d,"
        //     "dest_device:%d %d %d %d %d %d %d %d %d %d\n"
        //     // "ispopular:%d %d %d %d %d %d %d %d %d %d\n"
        //     ,
        //     cur,
        //     // start + 0 * size + cur, start + 1 * size + cur,
        //     // start + 2 * size + cur, start + 3 * size + cur,
        //     // start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type, txn[start + 1 * size + cur].subtxn.type,
        //     txn[start + 2 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn.type,
        //     txn[start + 4 * size + cur].subtxn.type, txn[start + 5 * size + cur].subtxn.type,
        //     txn[start + 6 * size + cur].subtxn.type, txn[start + 7 * size + cur].subtxn.type,
        //     txn[start + 8 * size + cur].subtxn.type, txn[start + 9 * size + cur].subtxn.type,
        //     // txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 4 * size + cur].subtxn.dest_Row_1, txn[start + 5 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 6 * size + cur].subtxn.dest_Row_1, txn[start + 7 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 8 * size + cur].subtxn.dest_Row_1, txn[start + 9 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device, txn[start + 5 * size + cur].dest_device,
        //     txn[start + 6 * size + cur].dest_device, txn[start + 7 * size + cur].dest_device,
        //     txn[start + 8 * size + cur].dest_device, txn[start + 9 * size + cur].dest_device);
        // // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        // // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        // // txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        // // txn[start + 6 * size + cur].subtxn.ispopular, txn[start + 7 * size + cur].subtxn.ispopular,
        // // txn[start + 8 * size + cur].subtxn.ispopular, txn[start + 9 * size + cur].subtxn.ispopular);

        cur += thSize;
    }
}

template<>
__global__ void parse<YCSB_E_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_E_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index) {
    // 95% Scan, 5% Insert
    uint32_t thID = threadIdx.x + blockDim.x * blockIdx.x;
    uint32_t thSize = blockDim.x * gridDim.x;
    uint32_t cur = thID;
    while (cur < size) {
        for (uint32_t i = 0; i < 9; ++i) {
            // scan 20 items
            txn[start + i * size + cur].subtxn.TID = query[cur].TID;
            txn[start + i * size + cur].subtxn.type = 4;
            txn[start + i * size + cur].subtxn.table_ID = 0;
            txn[start + i * size + cur].subtxn.benchmark = 4;
            txn[start + i * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[i];
            txn[start + i * size + cur].subtxn.dest_Row_2 = query[cur].ROW_ID[i] + 2;
            txn[start + i * size + cur].subtxn.dest_device = query[cur].ROW_ID[i] / meta[0].table_slice_size;
            txn[start + i * size + cur].subtxn.ispopular = 0;
            txn[start + i * size + cur].dest_device = txn[start + i * size + cur].subtxn.dest_device;
            txn[start + i * size + cur].global_txn_info_ID = i;
        }
        txn[start + 9 * size + cur].subtxn.TID = query[cur].TID;
        txn[start + 9 * size + cur].subtxn.type = 1;
        txn[start + 9 * size + cur].subtxn.table_ID = 0;
        txn[start + 9 * size + cur].subtxn.benchmark = 4;
        txn[start + 9 * size + cur].subtxn.dest_Row_1 = query[cur].ROW_ID[9];
        txn[start + 9 * size + cur].subtxn.dest_device = query[cur].ROW_ID[9] / meta[0].table_slice_size;
        txn[start + 9 * size + cur].subtxn.ispopular = 0;
        txn[start + 9 * size + cur].dest_device = txn[start + 9 * size + cur].subtxn.dest_device;
        txn[start + 9 * size + cur].global_txn_info_ID = 9;


        // printf(
        //     "cur:%d,"
        //     // "ID:%d %d %d %d %d,"
        //     "TID:%d,%d %d %d %d %d %d %d %d %d %d,"
        //     // "Row:%d %d %d %d %d %d %d %d %d %d,"
        //     "dest_device:%d %d %d %d %d %d %d %d %d %d\n"
        //     // "ispopular:%d %d %d %d %d %d %d %d %d %d\n"
        //     ,
        //     cur,
        //     // start + 0 * size + cur, start + 1 * size + cur,
        //     // start + 2 * size + cur, start + 3 * size + cur,
        //     // start + 4 * size + cur,
        //     txn[start + 0 * size + cur].subtxn.TID,
        //     txn[start + 0 * size + cur].subtxn.type, txn[start + 1 * size + cur].subtxn.type,
        //     txn[start + 2 * size + cur].subtxn.type, txn[start + 3 * size + cur].subtxn.type,
        //     txn[start + 4 * size + cur].subtxn.type, txn[start + 5 * size + cur].subtxn.type,
        //     txn[start + 6 * size + cur].subtxn.type, txn[start + 7 * size + cur].subtxn.type,
        //     txn[start + 8 * size + cur].subtxn.type, txn[start + 9 * size + cur].subtxn.type,
        //     // txn[start + 0 * size + cur].subtxn.dest_Row_1, txn[start + 1 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 2 * size + cur].subtxn.dest_Row_1, txn[start + 3 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 4 * size + cur].subtxn.dest_Row_1, txn[start + 5 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 6 * size + cur].subtxn.dest_Row_1, txn[start + 7 * size + cur].subtxn.dest_Row_1,
        //     // txn[start + 8 * size + cur].subtxn.dest_Row_1, txn[start + 9 * size + cur].subtxn.dest_Row_1,
        //     txn[start + 0 * size + cur].dest_device, txn[start + 1 * size + cur].dest_device,
        //     txn[start + 2 * size + cur].dest_device, txn[start + 3 * size + cur].dest_device,
        //     txn[start + 4 * size + cur].dest_device, txn[start + 5 * size + cur].dest_device,
        //     txn[start + 6 * size + cur].dest_device, txn[start + 7 * size + cur].dest_device,
        //     txn[start + 8 * size + cur].dest_device, txn[start + 9 * size + cur].dest_device);
        // // txn[start + 0 * size + cur].subtxn.ispopular, txn[start + 1 * size + cur].subtxn.ispopular,
        // // txn[start + 2 * size + cur].subtxn.ispopular, txn[start + 3 * size + cur].subtxn.ispopular,
        // // txn[start + 4 * size + cur].subtxn.ispopular, txn[start + 5 * size + cur].subtxn.ispopular,
        // // txn[start + 6 * size + cur].subtxn.ispopular, txn[start + 7 * size + cur].subtxn.ispopular,
        // // txn[start + 8 * size + cur].subtxn.ispopular, txn[start + 9 * size + cur].subtxn.ispopular);

        cur += thSize;
    }
}
