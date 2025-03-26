#include "../include/threadpool.cuh"

void TaskQueue::addTask(std::function<void()> task)
{
    std::unique_lock<std::mutex> lock(mutex);
    tasks.push(task);
    condition.notify_one();
}

std::function<void()> TaskQueue::getTask()
{
    std::unique_lock<std::mutex> lock(mutex);
    condition.wait(lock, [this]() { return !tasks.empty(); });
    auto task = tasks.front();
    tasks.pop();
    return task;
}

bool TaskQueue::empty()
{
    std::unique_lock<std::mutex> lock(mutex);
    return tasks.empty();
}

Semaphore::Semaphore(int count) : count(count)
{
}

void Semaphore::pop()
{
    {
        std::unique_lock<std::mutex> lock(mtx);
        --count;
    }
}

void Semaphore::push()
{
    {
        std::unique_lock<std::mutex> lock(mtx);
        ++count;
    }
}

ThreadPool::ThreadPool(size_t numThreads) : stop(false), numThreads(numThreads)
{
    for (size_t i = 0; i < numThreads; ++i)
    {
        workers.emplace_back([this]
        {
            while (true)
            {
                std::function<void()> task;
                {
                    std::unique_lock<std::mutex> lock(queueMutex);
                    condition.wait(lock, [this] { return stop || !tasks.empty(); });
                    if (stop && tasks.empty())
                    {
                        return;
                    }
                    task = std::move(tasks.front());
                    tasks.pop();
                }
                task();
            }
        });
    }
}

ThreadPool::~ThreadPool()
{
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        stop = true;
    }
    condition.notify_all();
    for (std::thread& worker : workers)
    {
        worker.join();
    }
}

void ThreadPool::enqueue(std::function<void()> task)
{
    {
        std::unique_lock<std::mutex> lock(queueMutex);
        if (stop)
        {
            throw std::runtime_error("enqueue on stopped ThreadPool");
        }
        tasks.push(task);
    }
    condition.notify_one();
}

void processTasks(TaskQueue& taskQueue, ThreadPool& threadPool, Semaphore& semaphore)
{
    while (true)
    {
        auto task = taskQueue.getTask();
        threadPool.enqueue(task);
        semaphore.pop();
        if (taskQueue.empty() && semaphore.get_count() == 0)
        {
            break;
        }
    }
}
