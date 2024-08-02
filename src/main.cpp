#include "../include/common.h"
#include "../include/dependency.h"
#include "../include/database.h"
#include "../include/query.h"

#ifndef QUERY
#define QUERY 1
#endif

#ifndef DATABASE
#define DATABASE 1
#endif

int main(int argc, char **argv)
{
    initialize_dependency(argc, argv);
    std::shared_ptr<Param> param_ptr(new Param());
    Result result;
    std::shared_ptr<Database> database_ptr(new Database(param_ptr));
    std::shared_ptr<Query> query_ptr(new Query(param_ptr));
    database_ptr->initialize_tbl();
    query_ptr->make_Query();

#ifdef DATABASE
    initialize_gpudatabase(database_ptr->get_warehouse_size(), database_ptr->get_device_cnt(),
                           database_ptr->get_device_IDs());
#endif

#ifdef QUERY
    initialize_gpuquery(query_ptr->get_warehouse_size(), query_ptr->get_device_cnt(),
                        query_ptr->get_device_IDs(), query_ptr->get_batch_size(),
                        query_ptr->get_neworder_percent(), query_ptr->get_epoch_tp(), query_ptr->get_epoch_sync());
#endif

#ifdef DATABASE
    copy_database_to_gpu(database_ptr->get_warehouse(), database_ptr->get_district(),
                         database_ptr->get_customer(), database_ptr->get_history(),
                         database_ptr->get_neworder(), database_ptr->get_order(),
                         database_ptr->get_orderline(), database_ptr->get_stock(),
                         database_ptr->get_item(), database_ptr->get_customer_name_index());
#endif

#ifdef QUERY
    copy_query_to_gpu(query_ptr->get_neworder_query(), query_ptr->get_payment_query());
#endif

#ifdef QUERY
    launchQueryKernel(result);
#endif
    LOG(INFO) << "result.cost:" << result.cost << std::endl;
    LOG(INFO) << "result.epoch:" << result.epoch << std::endl;
    LOG(INFO) << "result.batch_size:" << result.batch_size << std::endl;
    LOG(INFO) << "result.TPS:" << (float)result.batch_size / result.cost << " tps" << std::endl;
#ifdef DATABASE
    launchDatabaseKernel();
#endif

#ifdef QUERY
    copy_query_to_cpu();
#endif

#ifdef DATABASE
    copy_database_to_cpu();
#endif

#ifdef QUERY
    release_gpuquery();
#endif

#ifdef DATABASE
    release_gpudatabase();
#endif

    // sleep(10.0f);
    free_dependency();
    std::cout << std::endl;
    std::cout << std::endl;
    return 0;
}