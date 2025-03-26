#include "../include/scheduler.h"

Scheduler::Scheduler(/* args */) {
}

Scheduler::~Scheduler() {
}

void Scheduler::execute(std::shared_ptr<Param> param) {
    LOG(INFO) << "Scheduler::execute() start execute_on_GPU()";
    execute_on_GPU(param);
    LOG(INFO) << "Scheduler::execute() end execute_on_GPU()";
}

void Scheduler::init_launcher(std::shared_ptr<Param> param) {
    LOG(INFO) << "Scheduler::execute() start init_launcher()";
    initial_launcher_on_GPU(param);
    LOG(INFO) << "Scheduler::execute() end init_launcher()";
}

void Scheduler::free_launcher(std::shared_ptr<Param> param) {
    LOG(INFO) << "Scheduler::execute() start free_launcher()";
    free_launcher_on_GPU(param);
    LOG(INFO) << "Scheduler::execute() end free_launcher()";
}

void Scheduler::start() {
    param->print();
    param->analyse_deviceIDs();

    db->generate(param);
    db->malloc_global_row(param);
    // db->print();
    db->copy_to_global_row(param);
    // db->print_global_row();

    query->malloc_global_txn(param);
    query->generate_txn(param);
    db->transfer_to_GPU(param);
    query->initial_on_GPU(param);

    this->init_launcher(param);

    for (uint32_t i = 0; i < param->epoch_tp; ++i) {
        LOG(INFO) << "Scheduler::start() query->transfer_to_GPU(param) No." << i;
        query->transfer_to_GPU(param);


        LOG(INFO) << "Scheduler::start() this->execute(param) No." << i;
        this->execute(param);

        // LOG(INFO) << "Scheduler::start() query->transfer_to_CPU(param) No." << i;
        // // query->transfer_to_CPU(param);

        if (i > param->epoch_tp * 0.1 && i < param->epoch_tp * 0.9) {
            LOG(INFO) << "Execution time: " << param->result.cost << " s.";
            LOG(INFO) << "TPS: " << (float) param->result.batch_size / param->result.cost << " .";
        }
    }


    this->free_launcher(param);

    query->transfer_to_CPU(param);

    db->transfer_to_CPU(param);

    query->free_global_txn();

    db->free_global_row(param);

    param->free_deviceIDs();
}

void Scheduler::end() {
    param.reset();
    db.reset();
    query.reset();
}
