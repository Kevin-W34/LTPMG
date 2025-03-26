#pragma once

#ifndef GPUCOMMON_CUH
#define GPUCOMMON_CUH

#include "define.cuh"

long long gpu_current_time();

float gpu_duration(long long start_t, long long end_t);

void gpu_sleep(float time_t);

#endif //GPUCOMMON_CUH
