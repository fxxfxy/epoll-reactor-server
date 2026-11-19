/*
 * threadpool.h —— 线程池接口（阶段2，阶段7增强优雅排空）
 *
 * 模型：生产者-消费者。固定数量工作线程 + 环形任务队列，
 *       互斥锁保护队列，not_empty/not_full条件变量协调生产消费。
 * 关闭语义（阶段7）：shutdown后拒绝新任务，但会把队列剩余任务
 *       全部执行完、等在途任务清零后才退出线程——不丢任务、不泄漏参数。
 */
#ifndef __THREADPOOL_H
#define __THREADPOOL_H

#include <pthread.h>

// 任务结构体：任务回调函数 + 传入参数
typedef struct task
{
    void (*func)(void *arg);
    void *arg;
} Task;

// 线程池结构体
typedef struct threadpool
{
    Task *task_queue;       // 任务环形队列
    int queue_capacity;      // 队列最大容量
    int queue_size;          // 当前队列里任务数量
    int queue_front;         // 队头（取任务的位置）
    int queue_rear;          // 队尾（放任务的位置）

    pthread_t *workers;      // 存放所有工作线程tid
    int thread_num;          // 工作线程数量

    pthread_mutex_t mutex;   // 保护任务队列的互斥锁
    pthread_cond_t not_empty; // 队列不为空：唤醒工作线程取任务
    pthread_cond_t not_full;  // 队列不满：唤醒生产者放任务
    pthread_cond_t all_idle;  // 阶段7：所有任务执行完毕时广播，供优雅关闭等待

    int shutdown;            // 标记：是否关闭线程池 1关闭 0正常
    int active_count;        // 阶段7：已取出、正在执行中的任务数（不含队列里等待的）
} ThreadPool;

/**
 * @brief  创建线程池
 * @param thread_num  工作线程数目
 * @param queue_cap   任务队列最大容量
 * @return 线程池指针，失败NULL
 */
ThreadPool *threadpool_create(int thread_num, int queue_cap);

/**
 * @brief  向线程池添加任务（生产者，阻塞版）
 *         队列满时会阻塞等待空位。epoll主线程禁止使用本接口，
 *         必须用threadpool_try_add_task，否则主线程一卡，全部IO停摆
 * @param pool 线程池
 * @param func 任务回调函数
 * @param arg  传给回调的参数
 * @return 0成功，-1失败
 */
int threadpool_add_task(ThreadPool *pool, void (*func)(void *), void *arg);

/**
 * @brief  向线程池添加任务（非阻塞try版）
 *         队列满或线程池关闭时立即返回-1，绝不阻塞等待。
 *         epoll主线程必须用本版本；失败时由调用方做背压（暂停读/断开连接）
 * @return 0成功，-1失败（队列满/已关闭/参数非法）
 */
int threadpool_try_add_task(ThreadPool *pool, void (*func)(void *), void *arg);

/**
 * @brief  销毁线程池，等待所有线程退出、释放资源
 */
void threadpool_destroy(ThreadPool *pool);

#endif
