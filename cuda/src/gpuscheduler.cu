#include "../include/gpuscheduler.cuh"

void transfer_database_to_GPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                              Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "message from gpuscheduler.cu transfer_database_to_GPU()" << std::endl;
    gpuscheduler.initialize_gpudatabase(param, table_for_gpu_info, table_for_gpu, index_for_GPU);
}

void transfer_database_to_CPU(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                              Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "message from gpuscheduler.cu transfer_database_to_CPU()" << std::endl;
    gpuscheduler.free_gpudatabase(param, table_for_gpu_info, table_for_gpu, index_for_GPU);
}

void initial_query_on_GPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                          Global_Txn_Info *global_txn_info) {
    std::cout << "message from gpuquery.cu initial_query_on_GPU()" << std::endl;
    gpuscheduler.initialize_gpuquery(param, transactions_batch_ptr, global_txn_info);
}

void transfer_query_to_GPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                           Global_Txn_Info *global_txn_info) {
    std::cout << "message from gpuquery.cu transfer_query_to_GPU()" << std::endl;
    // gpuscheduler.initialize_gpuquery(param, transactions_batch_ptr, global_txn_info);
    gpuscheduler.copy_gpuquery(param, transactions_batch_ptr, global_txn_info);
}

void transfer_query_to_CPU(std::shared_ptr<Param> param, std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                           Global_Txn_Info *global_txn_info) {
    std::cout << "message from gpuquery.cu transfer_query_to_CPU()" << std::endl;
    gpuscheduler.free_gpuquery(param, transactions_batch_ptr, global_txn_info);
}

void execute_on_GPU(std::shared_ptr<Param> param) {
    std::cout << "message from gpuscheduler.cu execute_on_GPU()" << std::endl;
    gpuscheduler.execute(param);
}

void initial_launcher_on_GPU(std::shared_ptr<Param> param) {
    std::cout << "message from gpuscheduler.cu initial_launcher_on_GPU()" << std::endl;
    gpuscheduler.initialize_launcher(param);
}

void free_launcher_on_GPU(std::shared_ptr<Param> param) {
    std::cout << "message from gpuscheduler.free_launcher_on_GPU()" << std::endl;
    gpuscheduler.free_launcher(param);
}

GPUScheduler::GPUScheduler() {
}

GPUScheduler::~GPUScheduler() {
}

void GPUScheduler::initialize_gpudatabase(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                          Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "start gpuscheduler.cu GPUScheduler::initialize_gpudatabase()" << std::endl;
    gpudatabase = new GPUdatabase();

    gpudatabase->malloc_global_row(param, table_for_gpu_info, table_for_gpu, index_for_GPU);
    gpudatabase->copy_to_global_row(param, table_for_gpu_info, table_for_gpu, index_for_GPU);
    std::cout << "end gpuscheduler.cu GPUScheduler::initialize_gpudatabase()" << std::endl;
}

void GPUScheduler::free_gpudatabase(std::shared_ptr<Param> param, Global_Table_Info *table_for_gpu_info,
                                    Global_Table *table_for_gpu, Global_Table_Index *index_for_GPU) {
    std::cout << "start gpuscheduler.cu GPUScheduler::free_gpudatabase()" << std::endl;
    gpudatabase->free_global_row(param, table_for_gpu_info, table_for_gpu, index_for_GPU);
    delete gpudatabase;

    std::cout << "end gpuscheduler.cu GPUScheduler::free_gpudatabase()" << std::endl;
}

void GPUScheduler::initialize_gpuquery(std::shared_ptr<Param> param,
                                       std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                       Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuscheduler.cu GPUScheduler::initialize_gpuquery()" << std::endl;
    gpuquery = new GPUquery();
    gpuquery->malloc_global_txn(param, transactions_batch_ptr, global_txn_info);
    // gpuquery->copy_global_txn(param, transactions_batch_ptr, global_txn_info, gpudatabase->get_meta());
    std::cout << "end gpuscheduler.cu GPUScheduler::initialize_gpuquery()" << std::endl;
}

void GPUScheduler::copy_gpuquery(std::shared_ptr<Param> param,
                                 std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                 Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuscheduler.cu GPUScheduler::copy_gpuquery()" << std::endl;
    gpuquery->copy_global_txn(param, transactions_batch_ptr, global_txn_info, gpudatabase->get_meta(),
                              gpudatabase->get_index());
    std::cout << "end gpuscheduler.cu GPUScheduler::copy_gpuquery()" << std::endl;
}

void GPUScheduler::free_gpuquery(std::shared_ptr<Param> param,
                                 std::shared_ptr<std::vector<std::any> > transactions_batch_ptr,
                                 Global_Txn_Info *global_txn_info) {
    std::cout << "start gpuscheduler.cu GPUScheduler::free_gpudatabase()" << std::endl;

    gpuquery->free_global_txn(param, transactions_batch_ptr, global_txn_info);
    delete gpuquery;

    std::cout << "end gpuscheduler.cu GPUScheduler::free_gpudatabase()" << std::endl;
}

void GPUScheduler::initialize_launcher(std::shared_ptr<Param> param) {
    std::cout << "start gpuscheduler.cu GPUScheduler::initialize_launcher()" << std::endl;

    gpulauncher = new GPUlauncher();

    std::cout << "end gpuscheduler.cu GPUScheduler::initialize_launcher()" << std::endl;
}

void GPUScheduler::execute(std::shared_ptr<Param> param) {
    std::cout << "start gpuscheduler.cu GPUScheduler::execute()" << std::endl;

    // gpudatabase->launch_test(param);

    // gpulauncher = new GPUlauncher();

    gpulauncher->txn_executor_launcher(param, gpudatabase, gpuquery);

    // delete gpulauncher;

    std::cout << "end gpuscheduler.cu GPUScheduler::execute()" << std::endl;
}

void GPUScheduler::free_launcher(std::shared_ptr<Param> param) {
    std::cout << "start gpuscheduler.cu GPUScheduler::free_launcher()" << std::endl;

    delete gpulauncher;

    std::cout << "end gpuscheduler.cu GPUScheduler::free_launcher()" << std::endl;
}
