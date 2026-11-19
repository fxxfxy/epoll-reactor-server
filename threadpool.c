/*
 * threadpool.c —— 线程池实现（阶段2，阶段7增强优雅排空）
 *
 * 要点：任务执行不持锁（避免阻塞队列）；active_count统计在途任务，
 *       配合all_idle条件变量让destroy等到"队列空+在途清零"再join。
 */
#include "threadpool.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// 工作线程入口函数：循环消费任务
static void *worker_routine(void *arg)
{
    ThreadPool *pool = (ThreadPool *)arg;
    while (1)
    {
        // 加锁
        pthread_mutex_lock(&pool->mutex);

        // 队列是空 并且 没有关闭，就阻塞等待任务
        while (pool->queue_size == 0 && pool->shutdown == 0)
        {
            pthread_cond_wait(&pool->not_empty, &pool->mutex);
        }

        // 阶段7：关闭且队列已空才退出；队列里还有任务就继续取出来执行完。
        // 这样关闭时不丢任务（优雅关闭），也保证每个任务的参数都被回调
        // 正常释放，不会泄漏TaskArg/pkg
        if (pool->shutdown == 1 && pool->queue_size == 0)
        {
            pthread_mutex_unlock(&pool->mutex);
            pthread_exit(NULL);
        }

        // 取出队头任务
        Task task = pool->task_queue[pool->queue_front];
        pool->queue_front = (pool->queue_front + 1) % pool->queue_capacity;
        pool->queue_size--;
        pool->active_count++;   // 阶段7：任务进入"执行中"状态

        // 队列已经有空位，通知生产者可以继续放任务
        pthread_cond_signal(&pool->not_full);

        // 解锁，任务执行不需要拿着锁（重点！避免阻塞队列）
        pthread_mutex_unlock(&pool->mutex);

        // 执行任务
        task.func(task.arg);

        // 阶段7：任务执行完毕，计数减1；全部空闲时广播all_idle，
        // 让threadpool_destroy里的等待者知道"在途任务已清零"
        pthread_mutex_lock(&pool->mutex);
        pool->active_count--;
        if (pool->active_count == 0)
            pthread_cond_broadcast(&pool->all_idle);
        pthread_mutex_unlock(&pool->mutex);
    }
    return NULL;
}

ThreadPool *threadpool_create(int thread_num, int queue_cap)
{
    if (thread_num <= 0 || queue_cap <= 0)
    {
        return NULL;
    }
    ThreadPool *pool = malloc(sizeof(ThreadPool));
    if (!pool)
        return NULL;

    pool->thread_num = thread_num;
    pool->queue_capacity = queue_cap;
    pool->queue_size = 0;
    pool->queue_front = 0;
    pool->queue_rear = 0;
    pool->shutdown = 0;
    pool->active_count = 0;   // 阶段7：执行中任务计数清零

    // 分配任务队列
    pool->task_queue = malloc(sizeof(Task) * queue_cap);
    if (!pool->task_queue)
    {
        free(pool);
        return NULL;
    }
    // 分配线程id数组
    pool->workers = malloc(sizeof(pthread_t) * thread_num);
    if (!pool->workers)
    {
        free(pool->task_queue);
        free(pool);
        return NULL;
    }

    // 初始化锁、条件变量
    pthread_mutex_init(&pool->mutex, NULL);
    pthread_cond_init(&pool->not_empty, NULL);
    pthread_cond_init(&pool->not_full, NULL);
    pthread_cond_init(&pool->all_idle, NULL);   // 阶段7

    // 创建所有工作线程
    for (int i = 0; i < thread_num; i++)
    {
        pthread_create(&pool->workers[i], NULL, worker_routine, pool);
    }
    return pool;
}

int threadpool_add_task(ThreadPool *pool, void (*func)(void *), void *arg)
{
    if (!pool || pool->shutdown == 1)
        return -1;

    pthread_mutex_lock(&pool->mutex);

    // 队列满了，等待队列有空位
    while (pool->queue_size == pool->queue_capacity && pool->shutdown == 0)
    {
        pthread_cond_wait(&pool->not_full, &pool->mutex);
    }
    if (pool->shutdown == 1)
    {
        pthread_mutex_unlock(&pool->mutex);
        return -1;
    }

    // 放到队尾
    pool->task_queue[pool->queue_rear].func = func;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->queue_rear = (pool->queue_rear + 1) % pool->queue_capacity;
    pool->queue_size++;

    // 队列不为空，唤醒一个工作线程
    pthread_cond_signal(&pool->not_empty);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

// 非阻塞入队：队列满/已关闭立即返回-1，绝不cond_wait。
// epoll主线程（生产者）必须用本版本——主线程阻塞等队列空位会让
// 所有连接的IO和心跳停摆；失败由调用方对连接做背压（暂停读/断开）
int threadpool_try_add_task(ThreadPool *pool, void (*func)(void *), void *arg)
{
    if (!pool || pool->shutdown == 1)
        return -1;

    pthread_mutex_lock(&pool->mutex);
    if (pool->shutdown == 1 || pool->queue_size == pool->queue_capacity)
    {
        pthread_mutex_unlock(&pool->mutex);
        return -1;
    }

    // 放到队尾
    pool->task_queue[pool->queue_rear].func = func;
    pool->task_queue[pool->queue_rear].arg = arg;
    pool->queue_rear = (pool->queue_rear + 1) % pool->queue_capacity;
    pool->queue_size++;

    // 队列不为空，唤醒一个工作线程
    pthread_cond_signal(&pool->not_empty);
    pthread_mutex_unlock(&pool->mutex);
    return 0;
}

void threadpool_destroy(ThreadPool *pool)
{
    if (!pool)
        return;
    pthread_mutex_lock(&pool->mutex);
    pool->shutdown = 1;   // 拒绝新任务入队（add_task检查该标记）

    // 阶段7：优雅排空，分两步等待，各自等对应的条件变量，避免错配死等：
    // 第一步：等队列里剩余任务全部被取走。工作线程每取一个任务都会
    // signal not_empty，这里被唤醒后重新检查queue_size
    while (pool->queue_size > 0)
    {
        pthread_cond_wait(&pool->not_empty, &pool->mutex);
    }
    // 第二步：等已取出的在途任务全部执行完。任务执行完毕且计数归零时
    // 工作线程会broadcast all_idle。两步都满足才继续，
    // 保证不丢任务、每个任务的参数都被回调正常释放，不泄漏
    while (pool->active_count > 0)
    {
        pthread_cond_wait(&pool->all_idle, &pool->mutex);
    }
    pthread_mutex_unlock(&pool->mutex);

    // 唤醒所有等待的线程，让它们退出
    pthread_cond_broadcast(&pool->not_empty);
    pthread_cond_broadcast(&pool->not_full);

    // 等待所有工作线程结束
    for (int i = 0; i < pool->thread_num; i++)
    {
        pthread_join(pool->workers[i], NULL);
    }

    // 销毁锁、条件变量，释放内存
    pthread_mutex_destroy(&pool->mutex);
    pthread_cond_destroy(&pool->not_empty);
    pthread_cond_destroy(&pool->not_full);
    pthread_cond_destroy(&pool->all_idle);   // 阶段7
    free(pool->workers);
    free(pool->task_queue);
    free(pool);
}
