/*
 * async_log.c —— 异步日志实现（阶段3，阶段7新增log_stop_accept）
 *
 * 要点：写盘在日志线程执行并逐条fflush；stop标记只控制"退出/拒收"，
 *       存量队列始终写完，保证优雅关闭时日志完整。
 */
#include "async_log.h"
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <unistd.h>

// 日志线程入口：循环从队列取日志，写入文件
static void *log_routine(void *arg)
{
    LogMgr *mgr = (LogMgr *)arg;
    while (1)
    {
        pthread_mutex_lock(&mgr->mutex);
        // 队列为空且没有收到停止信号，等待
        while (mgr->size == 0 && mgr->stop == 0)
        {
            pthread_cond_wait(&mgr->not_empty, &mgr->mutex);
        }
        // 停止标记，并且队列空了，退出
        if (mgr->stop == 1 && mgr->size == 0)
        {
            pthread_mutex_unlock(&mgr->mutex);
            break;
        }

        // 取出一条日志
        LogItem item = mgr->queue[mgr->front];
        mgr->front = (mgr->front + 1) % LOG_QUEUE_CAP;
        mgr->size --;
        pthread_mutex_unlock(&mgr->mutex);

        // 写文件（IO操作，放在日志线程，不卡epoll主线程）
        fprintf(mgr->fp, "%s\n", item.msg);
        fflush(mgr->fp);
    }
    pthread_exit(NULL);
    return NULL;   // pthread_exit不返回，此处仅消除编译器警告
}

LogMgr* log_init(const char *file_name)
{
    LogMgr *mgr = malloc(sizeof(LogMgr));
    if (!mgr) return NULL;
    memset(mgr, 0, sizeof(LogMgr));

    mgr->fp = fopen(file_name, "a");
    if (!mgr->fp)
    {
        free(mgr);
        return NULL;
    }

    pthread_mutex_init(&mgr->mutex, NULL);
    pthread_cond_init(&mgr->not_empty, NULL);
    mgr->stop = 0;

    // 创建日志写线程
    pthread_create(&mgr->log_tid, NULL, log_routine, mgr);
    return mgr;
}

void log_write(LogMgr *mgr, const char *fmt, ...)
{
    if (!mgr || mgr->stop) return;

    char buf[LOG_BUF_LEN] = {0};
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, LOG_BUF_LEN-1, fmt, ap);
    va_end(ap);

    pthread_mutex_lock(&mgr->mutex);
    // 队列满了：这里简单策略直接丢弃日志（生产可以改成阻塞或者告警）
    if (mgr->size >= LOG_QUEUE_CAP)
    {
        pthread_mutex_unlock(&mgr->mutex);
        return;
    }
    strcpy(mgr->queue[mgr->rear].msg, buf);
    mgr->rear = (mgr->rear + 1) % LOG_QUEUE_CAP;
    mgr->size ++;
    pthread_cond_signal(&mgr->not_empty);
    pthread_mutex_unlock(&mgr->mutex);
}

// 阶段7：只置stop标记，不join线程、不关文件。
// log_write开头检查stop，此后新日志被直接丢弃（关闭阶段可接受）；
// 日志线程不受影响，继续把队列里存量日志写完，直到log_destroy收尾
void log_stop_accept(LogMgr *mgr)
{
    if (!mgr)
        return;
    pthread_mutex_lock(&mgr->mutex);
    mgr->stop = 1;
    pthread_mutex_unlock(&mgr->mutex);
}

void log_destroy(LogMgr *mgr)
{
    if (!mgr) return;
    pthread_mutex_lock(&mgr->mutex);
    mgr->stop = 1;
    pthread_mutex_unlock(&mgr->mutex);

    // 唤醒日志线程，让它退出
    pthread_cond_signal(&mgr->not_empty);
    pthread_join(mgr->log_tid, NULL);

    fclose(mgr->fp);
    pthread_mutex_destroy(&mgr->mutex);
    pthread_cond_destroy(&mgr->not_empty);
    free(mgr);
}
