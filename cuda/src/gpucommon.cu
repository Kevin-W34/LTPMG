//
// Created by weiji on 2024/12/17.
//
#include "../include/gpucommon.cuh"

long long gpu_current_time() {
    // get current time
    timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    long long time_t = time.tv_sec * 1000000 + time.tv_nsec / 1000;
    return time_t;
}

float gpu_duration(long long start_t, long long end_t) {
    // Computational time
    float time = (float) (end_t - start_t) / 1000000.0;
    return time;
}

void gpu_sleep(float time_t) {
    std::cout << "Sleep:" << time_t << "s." << std::endl;
    long long start_t = gpu_current_time();
    while (true) {
        long long end_t = gpu_current_time();
        float cost = gpu_duration(start_t, end_t);
        if (time_t <= cost) {
            break;
        }
    }
}