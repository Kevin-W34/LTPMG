#pragma once
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <cstring>
#include <iostream>

DECLARE_uint32(warehouse_size);
DECLARE_uint32(batch_size);
DECLARE_uint32(epoch_tp);
DECLARE_uint32(epoch_sync);
DECLARE_string(deviceIDs);
DECLARE_uint32(neworder_percent);

void initialize_dependency(int argc, char **argv);

void free_dependency();

long long current_time();

float duration(long long start_t, long long end_t);

void sleep(float time_t);

class Param
{
public:
    uint32_t warehouse_size;
    uint32_t batch_size;
    uint32_t epoch_tp;
    uint32_t epoch_sync;
    std::string deviceIDs;
    uint32_t neworder_percent;
    Param();
    ~Param();
};

struct Result
{
    float cost;
    uint32_t epoch;
    uint32_t batch_size;
};