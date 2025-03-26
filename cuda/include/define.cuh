#pragma once

#ifndef LTPMG_DEFINE_GPU
#define LTPMG_DEFINE_GPU

#include <cstdint>
#include <cstring>
#include <iostream>
#include <fstream>
#include <sstream>
#include <tuple>
#include <vector>
#include <thread>
#include <cstdio>
#include <variant>
#include <memory>
#include <typeinfo>
#include <any>
#include <queue>
#include <functional>
#include <future>
#include <condition_variable>
#include <atomic>
#include <algorithm>
#include <mutex>
#include <random>
#include <assert.h>
#include <semaphore>
#include <cmath>
#include <ctime>

#include "cuda_runtime.h"
#include "device_launch_parameters.h"
#include "cooperative_groups.h"
#include "cub/cub.cuh"

typedef int8_t INT8;
typedef int16_t INT16;
typedef int32_t INT32;
typedef int64_t INT64;

typedef uint8_t UINT8;
typedef uint16_t UINT16;
typedef uint32_t UINT32;
typedef uint64_t UINT64;

typedef char CHAR;
typedef u_char UCHAR;

typedef float FLOAT;
typedef double DOUBLE;

typedef bool BOOL;

typedef std::string STRING;

#define CHECK(res)                                                              \
    {                                                                           \
        if (res != cudaSuccess)                                                 \
        {                                                                       \
            printf("Error : %s:%d , ", __FILE__, __LINE__);                     \
            printf("code : %d , reason : %s \n", res, cudaGetErrorString(res)); \
            exit(-1);                                                           \
        }                                                                       \
    }

// long long current_time_gpu()
// { // get current time
//     timespec time;
//     clock_gettime(CLOCK_MONOTONIC, &time);
//     long long time_t = time.tv_sec * 1000000 + time.tv_nsec / 1000;
//     return time_t;
// }
//
// float duration_gpu(long long start_t, long long end_t)
// { // Computational time
//     float time = ((float)(end_t - start_t)) / 1000000.0;
//     return time;
// }
//
// void sleep_gpu(float time_t)
// {
//     std::cout << "Sleep:" << time_t << "s." << std::endl;
//     long long start_t = current_time_gpu();
//     while (true)
//     {
//         long long end_t = current_time_gpu();
//         float cost = duration_gpu(start_t, end_t);
//         if (time_t <= cost)
//         {
//             break;
//         }
//     }
// }

struct TPCC_PART
{
    /* data */
};

struct TPCC_ALL
{
    /* data */
};

#endif
