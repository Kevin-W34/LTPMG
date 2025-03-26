#pragma once

#ifndef LTPMG_GPUTHREADPOOL
#define LTPMG_GPUTHREADPOOL

#include "define.cuh"
#include "gpuparam.cuh"
#include "txn_structure.cuh"
#include "db_structure.cuh"
#include "gpudatabase.cuh"
#include "gpuquery.cuh"

#ifndef LTPMG_GPUTHREADPOOL_PRINT
// #define LTPMG_GPUTHREADPOOL_PRINT
#endif

// 任务队列类
class TaskQueue
{
public:
    void addTask(std::function<void()> task);

    std::function<void()> getTask();

    bool empty();

private:
    std::queue<std::function<void()>> tasks;
    std::mutex mutex;
    std::condition_variable condition;
};

// 信号量类
class Semaphore
{
public:
    Semaphore(int count);

    void pop();

    void push();

    int get_count() { return count; }

private:
    std::mutex mtx;
    std::condition_variable cv;
    int count;
};


// 线程池类
class ThreadPool
{
public:
    ThreadPool(size_t numThreads);

    void enqueue(std::function<void()> task);

    ~ThreadPool();

private:
    uint32_t numThreads;
    std::vector<std::thread> workers;
    std::queue<std::function<void()>> tasks;
    std::mutex queueMutex;
    std::condition_variable condition;
    bool stop;
};

// 函数A：遍历任务队列，启动线程执行任务
void processTasks(TaskQueue& taskQueue, ThreadPool& threadPool, Semaphore& semaphore);

#endif
