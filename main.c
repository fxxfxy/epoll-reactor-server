/*
 * main.c —— 程序入口：组件装配与优雅关闭编排（阶段7）
 *
 * 启动顺序：日志 -> 线程池 -> 服务器 -> 注册退出信号 -> epoll主循环
 * 关闭顺序：线程池(等任务排空、工作线程全退出) -> 服务器(释放连接/fd)
 *           -> 日志(存量写盘后关闭)。
 * 顺序不可颠倒：工作线程会访问srv和log，必须先于两者退出。
 */
#include "server.h"
#include <stdio.h>

int main(void)
{
    // 1. 创建异步日志
    LogMgr *log = log_init("server.log");
    if (!log)
    {
        printf("log init failed\n");
        return -1;
    }
    // 2. 创建线程池：4个工作线程，队列容量1024。
    // 加大队列降低突发流量下打满的概率；即便打满，主线程也是
    // 非阻塞try入队+断开背压，绝不阻塞等待（见server.c拆包处）
    ThreadPool *pool = threadpool_create(4, 1024);
    if (!pool)
    {
        log_destroy(log);
        return -1;
    }
    // 3. 创建epoll服务器
    Server *srv = server_create(pool, log);
    if (!srv)
    {
        threadpool_destroy(pool);
        log_destroy(log);
        return -1;
    }

    // 4. 阶段7：注册退出信号（Ctrl+C / kill）。
    // 处理函数只置srv->stop标记，不做任何清理，保证信号安全
    server_setup_signals(srv);

    // 5. 进入主循环，直到收到退出信号
    server_run(srv);

    // ===== 阶段7：优雅关闭，顺序是安全的关键 =====
    // 此刻主循环已退出，但工作线程可能还在执行最后几个任务。
    // 工作线程会访问srv（回包队列、eventfd）和log，所以：

    // 6. 先销毁线程池：内部会等队列排空、在途任务全部执行完、
    //    所有工作线程join退出。返回后没有任何线程再碰srv和log，
    //    后续释放才是安全的。（若先释放srv，工作线程会访问已释放内存）
    threadpool_destroy(pool);

    // 7. 再销毁服务器：关闭全部客户端连接、清空回包队列、
    //    关闭监听fd/epoll/eventfd。此时已无并发访问，无需担心竞态
    server_destroy(srv);

    // 8. 最后销毁日志：把队列里剩余日志（含关闭过程的记录）全部
    //    写盘后退出日志线程、关文件。放在最后保证关闭日志不丢
    log_destroy(log);

    printf("server exited gracefully\n");
    return 0;
}
