#pragma once
#ifndef LTPMG_COMMON
#define LTPMG_COMMON
#include "define.h"

DECLARE_string(benchmark);
DECLARE_uint32(table_size);
DECLARE_uint32(batch_size);
DECLARE_uint32(epoch_tp);
DECLARE_uint32(epoch_sync);
DECLARE_string(deviceIDs);
DECLARE_uint32(neworder_percent);
DECLARE_string(log_path);
DECLARE_double(zipf_config);
DECLARE_string(data_distribution);

void initialize_dependency(int argc, char **argv);

void free_dependency();

long long current_time();

float duration(long long start_t, long long end_t);

void sleep(float time_t);


#endif
