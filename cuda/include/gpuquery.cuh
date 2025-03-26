#pragma once
#ifndef LTPMG_GPUQUERY
#define LTPMG_GPUQUERY

#include "define.cuh"
#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "gpuparam.cuh"
#include "txn_structure.cuh"
#include "db_structure.cuh"

#ifndef LTPMG_GPUQUERY_PRINTSIZE
// #define LTPMG_GPUQUERY_PRINTSIZE
#endif

#ifndef LTPMG_GPUQUERY_TRANSFER_GROUP
#define LTPMG_GPUQUERY_TRANSFER_GROUP
#endif

class GPUquery {
private:
    std::string message;
    std::shared_ptr<std::vector<std::any> > transactions_batch_ptr;
    Global_Txn_Info *global_txn_info;

    Global_Txn_Info *global_txn_info_h;
    Global_Txn_Info **global_txn_info_d;

    Global_Txn **global_txn_h;
    Global_Txn **global_txn_d;
    Global_Txn *global_txn;

    Global_Txn_Exec **global_txn_exec_h;
    Global_Txn_Exec **global_txn_exec_d;

    Global_Txn_Result **global_txn_result_h;
    Global_Txn_Result **global_txn_result_d;

    Global_Txn_Exec_Param *exec_param_h;
    Global_Txn_Exec_Param **exec_param_d;

    Global_Txn_Aux_Struct **aux_struct_h;
    Global_Txn_Aux_Struct **aux_struct_d;
    Global_Txn_Aux_Struct **aux_struct_d_h;

    Global_Data_Packet **data_packet_h;
    Global_Data_Packet **data_packet_d;

    uint32_t global_txn_info_size;

public:
    GPUquery();

    ~GPUquery();

    int test(int input);

    void malloc_global_txn(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                           Global_Txn_Info *global_txn_info);

    void copy_global_txn(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                         Global_Txn_Info *global_txn_info, Global_Table_Meta **meta, Global_Table_Index **index);

    void free_global_txn(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                         Global_Txn_Info *global_txn_info);

    void clear_global_txn(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                          Global_Txn_Info *global_txn_info);

    void transfer_data_packet(std::shared_ptr<Param> param, cudaStream_t *streams);

    template<typename T>
    void gen_param(std::shared_ptr<Param> param);

    template<typename T>
    void query_parse(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                     Global_Table_Meta **meta, Global_Table_Index **index);

    uint32_t get_global_txn_info_ID(std::shared_ptr<Param> param, uint32_t sub_txn_ID, const std::type_info &txn_type);

    uint32_t get_global_txn_start(std::shared_ptr<Param> param, const std::type_info &txn_type);

    // void test_query_gen_param(std::shared_ptr<Param> param, uint32_t global_txn_info_size);

    // void tpcc_part_query_gen_param(std::shared_ptr<Param> param, uint32_t global_txn_info_size);

    // void tpcc_all_query_gen_param(std::shared_ptr<Param> param, uint32_t global_txn_info_size);

    Global_Txn_Info *get_txn_info(const int deviceID);

    Global_Txn *get_txn(const int deviceID);

    Global_Txn_Exec *get_txn_exec(const int deviceID);

    Global_Txn_Result *get_txn_result(const int deviceID);

    Global_Txn_Exec_Param *get_exec_param(const int deviceID);

    Global_Txn_Aux_Struct *get_aux_struct(const int deviceID);

    Global_Data_Packet *get_data_packet(const int deviceID);

    Global_Txn *get_txn_for_cpu() { return global_txn; }

    Global_Txn_Info *get_txn_info_for_cpu() { return global_txn_info_h; }

    std::shared_ptr<std::vector<std::any> > get_transactions_batch_ptr() { return transactions_batch_ptr; }
};

template<>
void GPUquery::gen_param<Test_Query>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<TPCC_PART>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<TPCC_ALL>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<YCSB_A_Query>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<YCSB_B_Query>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<YCSB_C_Query>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<YCSB_D_Query>(std::shared_ptr<Param> param);

template<>
void GPUquery::gen_param<YCSB_E_Query>(std::shared_ptr<Param> param);

// template <>
// void GPUquery::query_parse<Test_Query>(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any>> transactions_batch_ptr,
//                                        uint32_t global_txn_info_size);

template<typename T>
__global__ void parse(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, T *query, Global_Txn *txn,
                      Global_Txn_Exec_Param *param, Global_Table_Meta *meta, Global_Table_Index *index);

template<>
__global__ void parse<Test_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Test_Query *query,
                                  Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                  Global_Table_Index *index);

template<>
__global__ void parse<Test_Query_2>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Test_Query_2 *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

template<>
__global__ void parse<Neworder_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Neworder_Query *query,
                                      Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                      Global_Table_Index *index);

template<>
__global__ void parse<Payment_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Payment_Query *query,
                                     Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                     Global_Table_Index *index);

template<>
__global__ void parse<Orderstatus_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info,
                                         Orderstatus_Query *query, Global_Txn *txn, Global_Txn_Exec_Param *param,
                                         Global_Table_Meta *meta, Global_Table_Index *index);

template<>
__global__ void parse<Delivery_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, Delivery_Query *query,
                                      Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                      Global_Table_Index *index);

template<>
__global__ void parse<Stocklevel_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info,
                                        Stocklevel_Query *query, Global_Txn *txn, Global_Txn_Exec_Param *param,
                                        Global_Table_Meta *meta, Global_Table_Index *index);

template<>
__global__ void parse<YCSB_A_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_A_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

template<>
__global__ void parse<YCSB_B_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_B_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

template<>
__global__ void parse<YCSB_C_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_C_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

template<>
__global__ void parse<YCSB_D_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_D_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

template<>
__global__ void parse<YCSB_E_Query>(uint32_t size, uint32_t start, Global_Txn_Info *txn_info, YCSB_E_Query *query,
                                    Global_Txn *txn, Global_Txn_Exec_Param *param, Global_Table_Meta *meta,
                                    Global_Table_Index *index);

#endif
