#include "../include/common.cuh"

long long current_time()
{ // get current time
    timespec time;
    clock_gettime(CLOCK_MONOTONIC, &time);
    long long time_t = time.tv_sec * 1000000 + time.tv_nsec / 1000;
    return time_t;
}

float duration(long long start_t, long long end_t)
{ // Computational time
    float time = ((float)(end_t - start_t)) / 1000000.0;
    return time;
}

void sleep(float time_t)
{
    std::cout << "Sleep:" << time_t << "s." << std::endl;
    long long start_t = current_time();
    while (true)
    {
        long long end_t = current_time();
        float cost = duration(start_t, end_t);
        if (time_t <= cost)
        {
            break;
        }
    }
}