#pragma once

#ifndef LTPMG_GPUDATABASE
#define LTPMG_GPUDATABASE
#include "define.cuh"

#include "gpuparam.cuh"
#include "db_structure.cuh"

class GPUdatabase {
private:
    Global_Table_Info *table_for_gpu_info;
    Global_Table *table_for_gpu;

    Global_Table_Info **tables_info_d;
    Global_Table_Info *tables_info_h;
    // Global_Table_Info tables_info[8];

    Global_Table **tables_d;
    Global_Table **tables_d_h;
    Global_Table *tables_h;

    Global_Table_Index **index_d;
    Global_Table_Index **index_d_h;
    Global_Table_Index **index_h; // 索引

    Global_Table_Strategy **strategy_d;
    Global_Table_Strategy **strategy_d_h;
    Global_Table_Strategy *strategy_h; // 对应属性是否在GPU中可见

    Global_Table_Meta **metainfo_d;
    Global_Table_Meta **metainfo_h; // 表示多少行存在GPU

public:
    GPUdatabase(/* args */);

    ~GPUdatabase();

    void malloc_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                           Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

    void copy_to_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                            Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

    void free_global_row(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                         Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

    void data_partition_strategy(std::shared_ptr<Param> param);

    Global_Table_Info *get_table_info(const int deviceID);

    Global_Table *get_table(const int deviceID);

    Global_Table_Index *get_index(const int deviceID);

    Global_Table_Meta *get_meta(const int deviceID);

    Global_Table_Strategy *get_strategy(const int deviceID);

    Global_Table_Meta **get_meta() { return metainfo_d; }

    Global_Table_Index **get_index() { return index_d; }

    Global_Table_Info *get_table_info_for_cpu() { return tables_info_h; }

    Global_Table *get_table_for_cpu() { return tables_h; }

    Global_Table_Strategy *get_strategy_for_cpu() { return strategy_h; }

    void launch_test(std::shared_ptr<Param> param);
};

__global__ void test(int ID, Global_Table_Info *table_info, Global_Table *table, Global_Table_Strategy *strategy,
                     Global_Table_Meta *metainfo, Global_Table_Index *index);

#endif
