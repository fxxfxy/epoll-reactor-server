/*
 * async_log.h —— 异步日志接口（阶段3，阶段7新增停止接收接口）
 *
 * 模型：业务线程调log_write只做格式化+入队（非阻塞），
 *       专用日志线程取队列写盘，磁盘IO不阻塞epoll主线程。
 * 关闭语义：log_destroy等队列写空后才退出线程，关闭日志不丢；
 *       队列满时丢弃新日志（简单策略，保证业务不被日志拖慢）。
 */
#ifndef __ASYNC_LOG_H
#define __ASYNC_LOG_H

#include <stdio.h>
#include <pthread.h>

// 单条日志最大长度
#define LOG_BUF_LEN 256
// 日志环形队列最大条数
#define LOG_QUEUE_CAP 128

// 一条日志
typedef struct log_item
{
    char msg[LOG_BUF_LEN];
} LogItem;

// 异步日志管理器
typedef struct log_mgr
{
    LogItem queue[LOG_QUEUE_CAP];
    int front;
    int rear;
    int size;

    pthread_mutex_t mutex;
    pthread_cond_t not_empty;

    pthread_t log_tid;
    int stop;   // 日志线程退出标记
    FILE *fp;
} LogMgr;

// 初始化日志，打开日志文件
LogMgr* log_init(const char *file_name);

// 写日志（给业务调用，非阻塞，只是丢进队列）
void log_write(LogMgr *mgr, const char *fmt, ...);

// 阶段7：停止接收新日志（置stop标记），但日志线程继续运行，
// 把队列里已有的日志写完。用于优雅关闭的第一步：此后业务代码
// 调log_write会被安全忽略，不会访问后续将销毁的资源。
// 真正释放资源仍用log_destroy（它会等队列清空后再退出线程）
void log_stop_accept(LogMgr *mgr);

// 关闭日志，等待日志线程写完、释放资源
void log_destroy(LogMgr *mgr);

#endif
