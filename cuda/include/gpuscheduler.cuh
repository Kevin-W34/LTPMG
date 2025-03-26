#pragma once

#ifndef LTPMG_GPUSCHEDULER
#define LTPMG_GPUSCHEDULER

#include "define.cuh"
#include "gpudatabase.cuh"
#include "gpuquery.cuh"
#include "gpulauncher.cuh"
#include "gpuparam.cuh"

class GPUScheduler {
private:
    GPUdatabase *gpudatabase;

    GPUquery *gpuquery;

    GPUlauncher *gpulauncher;

public:
    GPUScheduler(/* args */);

    ~GPUScheduler();

    void initialize_gpudatabase(std::shared_ptr<Param> param,
                                Global_Table_Info *table_for_gpu_info,
                                Global_Table *table_for_gpu,
                                Global_Table_Index *index_for_GPU);

    void free_gpudatabase(std::shared_ptr<Param> param,
                          Global_Table_Info *table_for_gpu_info,
                          Global_Table *table_for_gpu,
                          Global_Table_Index *index_for_GPU);

    void initialize_gpuquery(std::shared_ptr<Param> param,
                             std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                             Global_Txn_Info *global_txn_info);

    void copy_gpuquery(std::shared_ptr<Param> param,
                       std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                       Global_Txn_Info *global_txn_info);

    void free_gpuquery(std::shared_ptr<Param> param,
                       std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                       Global_Txn_Info *global_txn_info);

    void initialize_launcher(std::shared_ptr<Param> param);

    void execute(std::shared_ptr<Param> param);

    void free_launcher(std::shared_ptr<Param> param);
};

GPUScheduler gpuscheduler;

extern void transfer_database_to_GPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                     Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

extern void transfer_database_to_CPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                     Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU);

extern void initial_query_on_GPU(std::shared_ptr<Param> param,
                                 std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                 Global_Txn_Info *global_txn_info);

extern void transfer_query_to_GPU(std::shared_ptr<Param> param,
                                  std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                  Global_Txn_Info *global_txn_info);

extern void transfer_query_to_CPU(std::shared_ptr<Param> param,
                                  std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                  Global_Txn_Info *global_txn_info);

extern void initial_launcher_on_GPU(std::shared_ptr<Param> param);

extern void execute_on_GPU(std::shared_ptr<Param> param);

extern void free_launcher_on_GPU(std::shared_ptr<Param> param);

#endif
