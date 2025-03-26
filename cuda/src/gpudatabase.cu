#include "../include/gpudatabase.cuh"

GPUdatabase::GPUdatabase(/* args */) {
}

GPUdatabase::~GPUdatabase() {
}

void GPUdatabase::malloc_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                    Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "start gpudatabase.cu GPUdatabase::malloc_global_row()" << std::endl;
    this->table_for_gpu_info = table_for_gpu_info;
    this->table_for_gpu = table_for_gpu;
    // TODO: 此处需要数据划分策略, 并构造数据map以展示每张卡上存了什么数据
    // CHECK(cudaHostAlloc((void **)&tables_info_d, sizeof(Global_Table_Info *) * param->device_cnt,
    //     cudaHostAllocDefault));
    tables_info_d = new Global_Table_Info *[param->device_cnt];
    // CHECK(cudaHostAlloc((void **)&tables_info_h, sizeof(Global_Table_Info) * param->table_cnt,
    //     cudaHostAllocDefault));
    tables_info_h = new Global_Table_Info [param->table_cnt];
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMalloc((void **)&tables_info_d[i], sizeof(Global_Table_Info) * param->table_cnt));
    }

    CHECK(cudaHostAlloc((void **)&tables_d, sizeof(Global_Table *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&tables_d_h, sizeof(Global_Table *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&tables_h, sizeof(Global_Table) * param->table_cnt, cudaHostAllocDefault));

    for (size_t i = 0; i < param->table_cnt; ++i) {
        // CHECK(cudaHostAlloc((void **)&tables_h[i].int_data, sizeof(INT32) * table_for_gpu_info[i].int_size *
        //     table_for_gpu_info[i].table_size, cudaHostAllocDefault));
        // CHECK(cudaHostAlloc((void **)&tables_h[i].string_data, sizeof(UINT32) * table_for_gpu_info[i].string_size *
        //     table_for_gpu_info[i].table_size * table_for_gpu_info[i].string_length, cudaHostAllocDefault));
        // CHECK(cudaHostAlloc((void **)&tables_h[i].double_data, sizeof(DOUBLE) * table_for_gpu_info[i].double_size *
        //     table_for_gpu_info[i].table_size, cudaHostAllocDefault));
        tables_h[i].int_data = new INT32[table_for_gpu_info[i].int_size * table_for_gpu_info[i].table_size];
        tables_h[i].string_data = new UINT32[table_for_gpu_info[i].string_size *
                                             table_for_gpu_info[i].table_size * table_for_gpu_info[i].string_length];
        tables_h[i].double_data = new DOUBLE[table_for_gpu_info[i].double_size * table_for_gpu_info[i].table_size];
    }

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));

        CHECK(cudaMalloc((void **)&tables_d[j], sizeof(Global_Table) * param->table_cnt));
        CHECK(cudaHostAlloc((void **)&tables_d_h[j], sizeof(Global_Table) * param->table_cnt, cudaHostAllocDefault));

        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaMalloc((void **)&tables_d_h[j][i].int_data, sizeof(INT32) * table_for_gpu_info[i].int_size *
                table_for_gpu_info[i].table_size/param->device_cnt));
            CHECK(cudaMalloc((void **)&tables_d_h[j][i].string_data, sizeof(UINT32) * table_for_gpu_info[i].string_size
                * table_for_gpu_info[i].table_size * table_for_gpu_info[i].string_length/param->device_cnt));
            CHECK(cudaMalloc((void **)&tables_d_h[j][i].double_data, sizeof(DOUBLE) * table_for_gpu_info[i].double_size
                * table_for_gpu_info[i].table_size/param->device_cnt));
        }
    }

    CHECK(cudaHostAlloc((void **)&strategy_h, sizeof(Global_Table_Strategy) * param->table_cnt, cudaHostAllocDefault));
    for (size_t i = 0; i < param->table_cnt; ++i) {
        CHECK(cudaHostAlloc((void **)&strategy_h[i].int_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].int_size,
            cudaHostAllocDefault));
        CHECK(cudaHostAlloc((void **)&strategy_h[i].int_target_GPU_platform, sizeof(UINT32) * table_for_gpu_info[i].
            int_size, cudaHostAllocDefault));
        CHECK(cudaHostAlloc((void **)&strategy_h[i].string_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].
            string_size, cudaHostAllocDefault));
        CHECK(cudaHostAlloc((void **)&strategy_h[i].string_target_GPU_platform, sizeof(UINT32) * table_for_gpu_info[i].
            string_size, cudaHostAllocDefault));
        CHECK(cudaHostAlloc((void **)&strategy_h[i].double_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].
            double_size, cudaHostAllocDefault));
        CHECK(cudaHostAlloc((void **)&strategy_h[i].double_target_GPU_platform, sizeof(UINT32) * table_for_gpu_info[i].
            double_size, cudaHostAllocDefault));
    }

    CHECK(cudaHostAlloc((void **)&strategy_d, sizeof(Global_Table_Strategy *) * param->device_cnt, cudaHostAllocDefault
    ));
    CHECK(cudaHostAlloc((void **)&strategy_d_h, sizeof(Global_Table_Strategy *) * param->device_cnt,
        cudaHostAllocDefault));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaMalloc((void **)&strategy_d[j], sizeof(Global_Table_Strategy) * param->table_cnt));
        CHECK(cudaHostAlloc((void **)&strategy_d_h[j], sizeof(Global_Table_Strategy) * param->table_cnt,
            cudaHostAllocDefault));
        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].int_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].
                int_size));
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].int_target_GPU_platform, sizeof(UINT32) * table_for_gpu_info[i
            ].int_size));
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].string_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].
                string_size));
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].string_target_GPU_platform, sizeof(UINT32) *
                table_for_gpu_info[i].string_size));
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].double_target_GPU, sizeof(UINT32) * table_for_gpu_info[i].
                double_size));
            CHECK(cudaMalloc((void **)&strategy_d_h[j][i].double_target_GPU_platform, sizeof(UINT32) *
                table_for_gpu_info[i].double_size));
        }
    }

    CHECK(cudaHostAlloc((void **)&metainfo_d, sizeof(Global_Table_Meta *) * param->device_cnt, cudaHostAllocDefault));
    CHECK(cudaHostAlloc((void **)&metainfo_h, sizeof(Global_Table_Meta *) * param->device_cnt, cudaHostAllocDefault));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaMalloc((void **)&metainfo_d[j], sizeof(Global_Table_Meta) * param->table_cnt));
        CHECK(cudaHostAlloc((void **)&metainfo_h[j], sizeof(Global_Table_Meta) * param->table_cnt, cudaHostAllocDefault
        ));
    }

    //TODO: 索引分配空间，释放，生成索引并在预处理阶段使用，可能需要改造sub_txn部分以增加在launcher中使用index的适配
    CHECK(cudaHostAlloc((void**)&index_h,sizeof(Global_Table_Index *) * param->device_cnt, cudaHostAllocDefault));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaHostAlloc((void**)&index_h[j], sizeof(Global_Table_Index) * param->table_cnt, cudaHostAllocDefault));
    }
    CHECK(cudaHostAlloc((void**)&index_d_h,sizeof(Global_Table_Index *) * param->device_cnt, cudaHostAllocDefault));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaHostAlloc((void**)&index_d_h[j], sizeof(Global_Table_Index) * param->table_cnt, cudaHostAllocDefault
        ));
    }
    CHECK(cudaHostAlloc((void**)&index_d,sizeof(Global_Table_Index *) * param->device_cnt, cudaHostAllocDefault));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaMalloc((void**)&index_d[j], sizeof(Global_Table_Index) * param->table_cnt));
    }

    std::cout << "end gpudatabase.cu GPUdatabase::malloc_global_row()" << std::endl;
}

void GPUdatabase::copy_to_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                     Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "start gpudatabase.cu GPUdatabase::copy_to_global_row()" << std::endl;

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaMemcpy(tables_info_d[i], table_for_gpu_info, sizeof(Global_Table_Info) * param->table_cnt,
            cudaMemcpyHostToDevice));
        CHECK(cudaMemcpy(tables_info_h, table_for_gpu_info, sizeof(Global_Table_Info) * param->table_cnt,
            cudaMemcpyHostToHost));
    }

    // 此处需要数据划分策略, 并构造数据地图以展示每张卡上存了什么数据

    data_partition_strategy(param);

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));

        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaMemcpy(strategy_d_h[j][i].int_target_GPU, strategy_h[i].int_target_GPU, sizeof(UINT32) *
                table_for_gpu_info[i].int_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(strategy_d_h[j][i].int_target_GPU_platform, strategy_h[i].int_target_GPU_platform, sizeof(
                UINT32) * table_for_gpu_info[i].int_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(strategy_d_h[j][i].string_target_GPU, strategy_h[i].string_target_GPU, sizeof(UINT32) *
                table_for_gpu_info[i].string_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(strategy_d_h[j][i].string_target_GPU_platform, strategy_h[i].string_target_GPU_platform,
                sizeof(UINT32) * table_for_gpu_info[i].string_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(strategy_d_h[j][i].double_target_GPU, strategy_h[i].double_target_GPU, sizeof(UINT32) *
                table_for_gpu_info[i].double_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(strategy_d_h[j][i].double_target_GPU_platform, strategy_h[i].double_target_GPU_platform,
                sizeof(UINT32) * table_for_gpu_info[i].double_size, cudaMemcpyHostToDevice));
        }
        CHECK(cudaMemcpy(strategy_d[j], strategy_d_h[j], sizeof(Global_Table_Strategy) * param->table_cnt,
            cudaMemcpyHostToDevice));
    }

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        CHECK(cudaMemcpy(metainfo_d[j], metainfo_h[j], sizeof(Global_Table_Meta) * param->table_cnt,
            cudaMemcpyHostToDevice));
    }

    for (size_t i = 0; i < param->table_cnt; ++i) {
        CHECK(cudaMemcpy(tables_h[i].int_data , table_for_gpu[i].int_data, sizeof(INT32) * table_for_gpu_info[
            i].int_size, cudaMemcpyHostToHost));
        CHECK(cudaMemcpy(tables_h[i].string_data , table_for_gpu[i].string_data, sizeof(UINT32) *
            table_for_gpu_info[i].string_size * table_for_gpu_info[i].string_length,
            cudaMemcpyHostToHost));
        CHECK(cudaMemcpy(tables_h[i].double_data , table_for_gpu[i].double_data, sizeof(DOUBLE) *
            table_for_gpu_info[i].double_size, cudaMemcpyHostToHost));
    }

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaSetDevice(param->device_IDs[j]));
        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaMemcpy(tables_d_h[j][i].int_data , table_for_gpu[i].int_data + metainfo_h[j][i].row_start *
                table_for_gpu_info[i].int_size, sizeof(INT32) * table_for_gpu_info [i].int_size *
                metainfo_h[j][i].table_slice_size, cudaMemcpyHostToDevice));
            CHECK(cudaMemcpy(tables_d_h[j][i].string_data , table_for_gpu[i].string_data + metainfo_h[j][i].row_start *
                table_for_gpu_info[i].string_size*table_for_gpu_info[i].string_length,
                sizeof(UINT32) * table_for_gpu_info[i].string_size * table_for_gpu_info[i].string_length*
                metainfo_h[j][i]. table_slice_size, cudaMemcpyHostToDevice ));
            CHECK(cudaMemcpy(tables_d_h[j][i].double_data , table_for_gpu[i].double_data + metainfo_h[j][i].row_start *
                table_for_gpu_info[i].double_size, sizeof(DOUBLE) * table_for_gpu_info[i].double_size*
                metainfo_h[j][i].table_slice_size, cudaMemcpyHostToDevice));
        }
        CHECK(cudaMemcpy(tables_d[j], tables_d_h[j], sizeof(Global_Table) * param->table_cnt, cudaMemcpyHostToDevice));
    }

    //Index
    if (param->benchmark == "TEST") {
    } else if (param->benchmark == "TPCC_PART") {
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaMemcpy(index_h[j][2].index, index_for_GPU[2].index+metainfo_h[j][2].row_start,
                sizeof(UINT32)*metainfo_h[j][2].table_slice_size, cudaMemcpyHostToHost));
            CHECK(cudaMemcpy(index_d_h[j][2].index,index_for_GPU[2].index+metainfo_h[j][2].row_start,
                sizeof(UINT32)*metainfo_h[j][2].table_slice_size, cudaMemcpyHostToHost));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaMemcpy(index_d[j],index_d_h[j],
                sizeof(Global_Table_Index)*param->table_cnt,cudaMemcpyDeviceToHost));
        }
        // for (size_t j = 0; j < param->device_cnt; ++j) {
        //     std::cout << "index_h[" << j << "][2].index:";
        //     for (uint32_t i = 0; i < 10; ++i) {
        //         std::cout << std::hex << index_h[j][2].index[i] << ",";
        //     }
        //     std::cout << std::dec << std::endl;
        // }
    } else if (param->benchmark == "TPCC_ALL") {
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaMemcpy(index_h[j][2].index, index_for_GPU[2].index+metainfo_h[j][2].row_start,
                sizeof(UINT32)*metainfo_h[j][2].table_slice_size, cudaMemcpyHostToHost));
            CHECK(cudaMemcpy(index_d_h[j][2].index,index_for_GPU[2].index+metainfo_h[j][2].row_start,
                sizeof(UINT32)*metainfo_h[j][2].table_slice_size, cudaMemcpyHostToHost));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaMemcpy(index_d[j],index_d_h[j],
                sizeof(Global_Table_Index)*param->table_cnt,cudaMemcpyDeviceToHost));
        }
    } else if (param->benchmark == "YCSB_A") {
    } else if (param->benchmark == "YCSB_B") {
    } else if (param->benchmark == "YCSB_C") {
    } else if (param->benchmark == "YCSB_D") {
    } else if (param->benchmark == "YCSB_E") {
    }
    std::cout << "end gpudatabase.cu GPUdatabase::copy_to_global_row()" << std::endl;
}

void GPUdatabase::free_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                  Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "start gpudatabase.cu GPUdatabase::free_global_row()" << std::endl;
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaFree(tables_info_d[i]));
    }
    // CHECK(cudaFreeHost(tables_info_d));
    delete[] tables_info_d;
    // CHECK(cudaFreeHost(tables_info_h));
    delete[] tables_info_h;
    for (size_t i = 0; i < param->table_cnt; ++i) {
        // CHECK(cudaFreeHost(tables_h[i].int_data));
        // CHECK(cudaFreeHost(tables_h[i].string_data));
        // CHECK(cudaFreeHost(tables_h[i].double_data));
        delete[] tables_h[i].int_data;
        delete[] tables_h[i].string_data;
        delete[] tables_h[i].double_data;
    }
    for (size_t j = 0; j < param->device_cnt; ++j) {
        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaFree(tables_d_h[j][i].int_data));
            CHECK(cudaFree(tables_d_h[j][i].string_data));
            CHECK(cudaFree(tables_d_h[j][i].double_data));
        }
        CHECK(cudaFree(tables_d[j]));
        CHECK(cudaFreeHost(tables_d_h[j]));
    }
    CHECK(cudaFreeHost(tables_d));
    CHECK(cudaFreeHost(tables_d_h));
    CHECK(cudaFreeHost(tables_h));

    for (size_t j = 0; j < param->device_cnt; ++j) {
        for (size_t i = 0; i < param->table_cnt; ++i) {
            CHECK(cudaFree(strategy_d_h[j][i].int_target_GPU));
            CHECK(cudaFree(strategy_d_h[j][i].int_target_GPU_platform));
            CHECK(cudaFree(strategy_d_h[j][i].string_target_GPU));
            CHECK(cudaFree(strategy_d_h[j][i].string_target_GPU_platform));
            CHECK(cudaFree(strategy_d_h[j][i].double_target_GPU));
            CHECK(cudaFree(strategy_d_h[j][i].double_target_GPU_platform));
        }
        CHECK(cudaFree(strategy_d[j]));
        CHECK(cudaFreeHost(strategy_d_h[j]));
    }
    CHECK(cudaFreeHost(strategy_d));
    CHECK(cudaFreeHost(strategy_d_h));

    for (size_t i = 0; i < param->table_cnt; ++i) {
        CHECK(cudaFreeHost(strategy_h[i].int_target_GPU));
        CHECK(cudaFreeHost(strategy_h[i].int_target_GPU_platform));
        CHECK(cudaFreeHost(strategy_h[i].string_target_GPU));
        CHECK(cudaFreeHost(strategy_h[i].string_target_GPU_platform));
        CHECK(cudaFreeHost(strategy_h[i].double_target_GPU));
        CHECK(cudaFreeHost(strategy_h[i].double_target_GPU_platform));
    }
    CHECK(cudaFreeHost(strategy_h));

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaFree(metainfo_d[j]));
        CHECK(cudaFreeHost(metainfo_h[j]));
    }
    CHECK(cudaFreeHost(metainfo_d));
    CHECK(cudaFreeHost(metainfo_h));

    if (param->benchmark == "TEST") {
    } else if (param->benchmark == "TPCC_PART") {
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaFreeHost(index_h[j][2].index));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaFree(index_d_h[j][2].index));
        }
    } else if (param->benchmark == "TPCC_ALL") {
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaFreeHost(index_h[j][2].index));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaFree(index_d_h[j][2].index));
        }
    } else if (param->benchmark == "YCSB_A") {
    } else if (param->benchmark == "YCSB_B") {
    } else if (param->benchmark == "YCSB_C") {
    } else if (param->benchmark == "YCSB_D") {
    } else if (param->benchmark == "YCSB_E") {
    }

    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaFreeHost(index_h[j]));
    }
    CHECK(cudaFreeHost(index_h));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaFreeHost(index_d_h[j]));
    }
    CHECK(cudaFreeHost(index_d_h));
    for (size_t j = 0; j < param->device_cnt; ++j) {
        CHECK(cudaFree(index_d[j]));
    }
    CHECK(cudaFreeHost(index_d));

    std::cout << "end gpudatabase.cu GPUdatabase::free_global_row()" << std::endl;
}

void GPUdatabase::data_partition_strategy(std::shared_ptr<Param> param) {
    std::cout << "start gpudatabase.cu GPUdatabase::data_partition_strategy()" << std::endl;
    // TODO: 构造数据划分策略，启发式|代价模型计算并判断
    if (param->benchmark == "TEST") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            // std::cout << "strategy_h[j].int_target_GPU:" << " ";
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
                // std::cout << strategy_h[j].int_target_GPU[i] << " ";
            }
            // std::cout << std::endl;

            // std::cout << "strategy_h[j].string_target_GPU:" << " ";
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
                // std::cout << strategy_h[j].string_target_GPU[i] << " ";
            }
            // std::cout << std::endl;

            // std::cout << "strategy_h[j].double_target_GPU:" << " ";
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
                // std::cout << strategy_h[j].double_target_GPU[i] << " ";
            }
            // std::cout << std::endl;
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end
                        << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->test_1_size;
            metainfo_h[j][1].table_size = param->test_2_size;
        }
    } else if (param->benchmark == "TPCC_PART") {
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                if (i == 0 || i == 1) {
                    metainfo_h[j][i].bitmap_row_slice_size = table_slice_size;
                }

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaHostAlloc((void**)&index_h[j][2].index,sizeof(UINT32)*tables_info_h[2].table_size,
                cudaHostAllocDefault));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaSetDevice(param->device_IDs[j]));
            CHECK(cudaMalloc((void**)&index_d_h[j][2].index,sizeof(UINT32)*tables_info_h[2].table_size));
        }
    } else if (param->benchmark == "TPCC_ALL") {
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                if (i == 0 || i == 1) {
                    metainfo_h[j][i].bitmap_row_slice_size = table_slice_size;
                }
                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaHostAlloc((void**)&index_h[j][2].index,sizeof(UINT32)*tables_info_h[2].table_size,
                cudaHostAllocDefault));
        }
        for (size_t j = 0; j < param->device_cnt; ++j) {
            CHECK(cudaSetDevice(param->device_IDs[j]));
            CHECK(cudaMalloc((void**)&index_d_h[j][2].index,sizeof(UINT32)*tables_info_h[2].table_size));
        }
    } else if (param->benchmark == "YCSB_A") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                metainfo_h[j][i].bitmap_row_slice_size = param->bitmap_row_cnt;
                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->ycsb_size;
        }
    } else if (param->benchmark == "YCSB_B") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                metainfo_h[j][i].bitmap_row_slice_size = param->bitmap_row_cnt;

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->ycsb_size;
        }
    } else if (param->benchmark == "YCSB_C") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                metainfo_h[j][i].bitmap_row_slice_size = param->bitmap_row_cnt;

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->ycsb_size;
        }
    } else if (param->benchmark == "YCSB_D") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                metainfo_h[j][i].bitmap_row_slice_size = param->bitmap_row_cnt;

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->ycsb_size;
        }
    } else if (param->benchmark == "YCSB_E") {
        // TODO: 策略划分，无需GPU执行事务的数据标记为GPU不可见
        for (size_t j = 0; j < param->table_cnt; j++) {
            for (size_t i = 0; i < tables_info_h[j].int_size; ++i) {
                strategy_h[j].int_target_GPU[i] = 0xff;
                strategy_h[j].int_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].string_size; ++i) {
                strategy_h[j].string_target_GPU[i] = 0xff;
                strategy_h[j].string_target_GPU_platform[i] = 1;
            }
            for (size_t i = 0; i < tables_info_h[j].double_size; ++i) {
                strategy_h[j].double_target_GPU[i] = 0xff;
                strategy_h[j].double_target_GPU_platform[i] = 1;
            }
        }

        for (size_t j = 0; j < param->device_cnt; ++j) {
            for (size_t i = 0; i < param->table_cnt; ++i) {
                uint32_t table_slice_size = tables_info_h[i].table_size / param->device_cnt;
                metainfo_h[j][i].row_start = table_slice_size * j;
                metainfo_h[j][i].row_end = table_slice_size * (j + 1);
                metainfo_h[j][i].table_slice_size = table_slice_size;
                metainfo_h[j][i].bitmap_row_slice_size = param->bitmap_row_cnt;

                std::cout << "table:" << i << ",start:" << metainfo_h[j][i].row_start << ",end:" << metainfo_h[j][i].
                        row_end << ",slice_size:" << metainfo_h[j][i].table_slice_size << std::endl;
            }
            metainfo_h[j][0].table_size = param->ycsb_size;
        }
    }
    std::cout << "end gpudatabase.cu GPUdatabase::data_partition_strategy()" << std::endl;
}

Global_Table_Info *GPUdatabase::get_table_info(const int deviceID) {
    return tables_info_d[deviceID];
}

Global_Table *GPUdatabase::get_table(const int deviceID) {
    return tables_d[deviceID];
}

Global_Table_Index *GPUdatabase::get_index(const int deviceID) {
    return index_d[deviceID];
}

Global_Table_Meta *GPUdatabase::get_meta(const int deviceID) {
    return metainfo_d[deviceID];
}

Global_Table_Strategy *GPUdatabase::get_strategy(const int deviceID) {
    return strategy_d[deviceID];
}

void GPUdatabase::launch_test(std::shared_ptr<Param> param) {
    cudaStream_t *streams;
    streams = new cudaStream_t[param->device_cnt];
    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        CHECK(cudaStreamCreate(&streams[i]));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaSetDevice(param->device_IDs[i]));
        test<<<1, 1, 0, streams[i]>>>(i, get_table_info(i), get_table(i), get_strategy(i), get_meta(i), get_index(i));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamSynchronize(streams[i]));
    }

    for (size_t i = 0; i < param->device_cnt; ++i) {
        CHECK(cudaStreamDestroy(streams[i]));
    }
    delete[] streams;
}

__global__ void test(int ID, Global_Table_Info *table_info, Global_Table *table, Global_Table_Strategy *strategy,
                     Global_Table_Meta *metainfo, Global_Table_Index *index) {
    // UINT32 tableID = 0;
    // UINT32 dataID = 0;

    // printf("ID:%d,tableID:%d,DataID:%d,int_size:%d,int_data:%d,int_target_GPU:%d\n",
    //        ID, tableID, dataID, table_info[tableID].int_size,
    //        table[tableID].int_data[dataID], strategy[tableID].int_target_GPU[dataID]);
    // printf("ID:%d,tableID:%d,DataID:%d,table[0].int_data[0]:%d,table[1].int_data[0]:%d\n",
    //        ID, tableID, dataID, table[0].int_data[0], table[1].int_data[0]);

    // for (size_t i = 0; i < metainfo[0].table_slice_size; ++i) {
    //     printf("ID:%d,tableID:%d,DataID:%d,table[0].int_data[]:%d\n",
    //            ID, tableID, dataID, table[0].int_data[i]);
    // }
    // printf("\n");
    // for (size_t i = 0; i < metainfo[1].table_slice_size; ++i) {
    //     printf("ID:%d,tableID:%d,DataID:%d,table[1].int_data[]:%d\n",
    //            ID, tableID, dataID, table[1].int_data[i]);
    // }
    // printf("\n");
    // for (size_t i = 0; i < 5; ++i) {
    //     printf("ID:%d,tableID:%d,DataID:%d,table[0].int_data[]:%d\n",
    //            ID, tableID, dataID, table[0].int_data[i]);
    // }
    // printf("\n");
    // for (size_t i = 0; i < 5; ++i) {
    //     printf("ID:%d,tableID:%d,DataID:%d,table[1].int_data[]:%d\n",
    //            ID, tableID, dataID, table[1].int_data[i]);
    // }
    // printf("\n");
    for (uint32_t i = 0; i < 10; ++i) {
        printf("ID:%d,index[2].index[%d]:%x\n", ID, i, index[2].index[i]);
    }
}
