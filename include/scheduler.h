#pragma once

#ifndef LTPMG_SCHEDULER
#define LTPMG_SCHEDULER

#include "common.h"
#include "database.h"
#include "query.h"
#include "param.h"

class Scheduler
{
private:
    // Param param;
    std::shared_ptr<Param> param = std::make_shared<Param>(Param());

    // Database *db = new Database(filepath);
    std::shared_ptr<Database> db = std::make_shared<Database>(Database(param->get_filepath()));
    // Query query;

    std::shared_ptr<Query> query = std::make_shared<Query>(Query());

public:
    Scheduler(/* args */);
    ~Scheduler();
    void init_launcher(std::shared_ptr<Param> param);
    void execute(std::shared_ptr<Param> param);
    void free_launcher(std::shared_ptr<Param> param);
    void start();
    void end();
};

extern void initial_launcher_on_GPU(std::shared_ptr<Param> param);

extern void execute_on_GPU(std::shared_ptr<Param> param);

extern void free_launcher_on_GPU(std::shared_ptr<Param> param);

#endif