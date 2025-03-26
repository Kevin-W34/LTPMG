#pragma once

#ifndef LTPMG_GPULAUNCHER
#define LTPMG_GPULAUNCHER

#include "define.cuh"
#include "gpuparam.cuh"
#include "txn_structure.cuh"
#include "db_structure.cuh"
#include "gpudatabase.cuh"
#include "gpuquery.cuh"
#include "threadpool.cuh"
#include "gpucommon.cuh"

#ifndef LTPMG_GPULAUNCHER_PRINT
// #define LTPMG_GPULAUNCHER_PRINT
#endif

#ifndef LTPMG_GPULAUNCHER_TEST_MERGESORT
// #define LTPMG_GPULAUNCHER_TEST_MERGESORT
#endif

#ifndef LTPMG_GPULAUNCHER_TEST_PREFIXSUM
// #define LTPMG_GPULAUNCHER_TEST_PREFIXSUM
#endif

#ifndef LTPMG_GPULAUNCHER_TEST_MEMCPYP2P
// #define LTPMG_GPULAUNCHER_TEST_MEMCPYP2P
#endif

#ifndef LTPMG_GPULAUNCHER_SCAN_BITMAP_POPULAR
// #define LTPMG_GPULAUNCHER_SCAN_BITMAP_POPULAR
#endif

#ifndef LTPMG_GPULAUNCHER_SCAN_OPT_BITMAP_POPULAR
#define LTPMG_GPULAUNCHER_SCAN_OPT_BITMAP_POPULAR
#endif

#ifndef LTPMG_GPULAUNCHER_SCAN_OPT_SHM_BITMAP_POPULAR
#define LTPMG_GPULAUNCHER_SCAN_OPT_SHM_BITMAP_POPULAR
#endif

#ifndef LTPMG_GPULAUNCHER_BLOCK_SCAN_OPT_SHM_BITMAP_POPULAR
#define LTPMG_GPULAUNCHER_BLOCK_SCAN_OPT_SHM_BITMAP_POPULAR
#endif


__device__ void print(uint32_t device_ID);

template<typename D>
__device__ void select_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo);

template<typename D>
__device__ void scan_operator(uint32_t tableID,
                              uint32_t rowID,
                              uint32_t data_ID,
                              u_char data_type,
                              Global_Table *tables,
                              Global_Table_Info *table_info,
                              Global_Table_Meta *metainfo);

template<typename D>
__device__ void insert_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                D data,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo);

template<typename D>
__device__ void update_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                D data,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo);

template<typename D>
__device__ void delete_operator(uint32_t tableID,
                                uint32_t rowID,
                                uint32_t data_ID,
                                u_char data_type,
                                Global_Table *tables,
                                Global_Table_Info *table_info,
                                Global_Table_Meta *metainfo);

template<typename D>
__device__ void select_operator_shared(D &d);

template<typename D>
__device__ void scan_operator_shared(D &d);

template<typename D>
__device__ void insert_operator_shared(D data, D &d);

template<typename D>
__device__ void update_operator_shared(D data, D &d);

template<typename D>
__device__ void delete_operator_shared(D &d);

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
                                Global_Data_Packet *data_packet);

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
                                Global_Data_Packet *data_packet);

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
                                Global_Data_Packet *data_packet);

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
                              Global_Data_Packet *data_packet);

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
                                Global_Data_Packet *data_packet);

__device__ void register_txn_exec(uint32_t device_ID,
                                  uint32_t type,
                                  uint32_t cur_txn,
                                  Global_Txn_Exec *txn_exec);

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
                            Global_Data_Packet *data_packet);

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
                                 Global_Data_Packet *data_packet);

__device__ void merge(uint32_t device_ID,
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
                      Global_Data_Packet *data_packet);


void execute_on_thread(std::shared_ptr<Param> param,
                       uint32_t cur,
                       Global_Txn *txn,
                       Global_Txn_Info *global_txn_info,
                       uint32_t txn_offset,
                       Global_Table *table,
                       Global_Table_Info *table_info);

void execute_on_CPU(std::shared_ptr<Param> param,
                    GPUdatabase *gpudatabase,
                    GPUquery *gpuquery);


class GPUlauncher {
private:
    // std::semaphore sem(1);

public:
    std::thread cpu_exec;

    GPUlauncher(/* args */) {
    }

    ~GPUlauncher() {
    }

    void txn_kernel_launcher(std::shared_ptr<Param> param,
                             GPUdatabase *gpudatabase,
                             GPUquery *gpuquery);

    void txn_executor_launcher(std::shared_ptr<Param> param,
                               GPUdatabase *gpudatabase,
                               GPUquery *gpuquery);
};

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
                             Global_Data_Packet *data_packet);

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
                          Global_Data_Packet *data_packet);

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
                                  Global_Data_Packet *data_packet);

__global__ void prefix_bitmap(uint32_t device_ID,
                              uint32_t table_ID,
                              uint32_t benchmark,
                              Global_Table_Info *table_info,
                              Global_Table *tables,
                              Global_Table_Meta *metainfo,
                              Global_Txn *txn,
                              Global_Txn_Exec_Param *exec_param,
                              Global_Txn_Aux_Struct *aux_struct,
                              Global_Data_Packet *data_packet);

__global__ void partSum_bitmap(uint32_t device_ID,
                               uint32_t table_ID,
                               uint32_t benchmark,
                               Global_Table_Info *table_info,
                               Global_Table *tables,
                               Global_Table_Meta *metainfo,
                               Global_Txn *txn,
                               Global_Txn_Exec_Param *exec_param,
                               Global_Txn_Aux_Struct *aux_struct,
                               Global_Data_Packet *data_packet);

__global__ void compact_bitmark(uint32_t device_ID,
                                uint32_t table_ID,
                                uint32_t benchmark,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Meta *metainfo,
                                Global_Txn *txn,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet);

__device__ void popular_handler(uint32_t device_ID,
                                uint32_t table_ID,
                                uint32_t cur_row,
                                uint32_t cur_bitmap,
                                Global_Table_Info *table_info,
                                Global_Table *tables,
                                Global_Table_Meta *metainfo,
                                Global_Txn *txn,
                                Global_Txn_Exec_Param *exec_param,
                                Global_Txn_Aux_Struct *aux_struct,
                                Global_Data_Packet *data_packet);

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
                                    Global_Data_Packet *data_packet);

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
                               Global_Data_Packet *data_packet);

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
                               Global_Data_Packet *data_packet);

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
                               Global_Data_Packet *data_packet);

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
                             Global_Data_Packet *data_packet);

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
                               Global_Data_Packet *data_packet);

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
                                    Global_Data_Packet *data_packet);

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
                              Global_Data_Packet *data_packet);

__device__ void commit(uint32_t device_ID,
                       uint32_t table_ID,
                       uint32_t cur_row,
                       Global_Table_Info *table_info,
                       Global_Table *tables,
                       Global_Table_Meta *metainfo,
                       Global_Txn *txn,
                       Global_Txn_Exec_Param *exec_param,
                       Global_Txn_Aux_Struct *aux_struct);

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
                           Global_Data_Packet *data_packet);

#endif
