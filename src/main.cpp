#include "../include/common.h"
#include "../include/database.h"
#include "../include/scheduler.h"

int main(int argc, char **argv)
{
    initialize_dependency(argc, argv);
    // std::shared_ptr<Database> database = std::make_shared<Database>(Database());
    // database->print();
    std::shared_ptr<Scheduler> scheduler = std::make_shared<Scheduler>(Scheduler());
    scheduler->start();
    scheduler->end();
    scheduler.reset();
    return 0;
}
