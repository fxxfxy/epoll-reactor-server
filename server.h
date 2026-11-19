/*
 * server.h —— 服务器核心接口与数据结构定义（阶段4~7）
 *
 * 模块职责：单Reactor(epoll ET)主框架、连接管理、按长度协议拆包、
 *           异步写回、心跳超时/并发上限、优雅关闭。
 * 线程约定：Conn、epoll、连接链表只允许epoll主线程操作；
 *           线程池工作线程仅通过"回包队列(互斥锁) + eventfd"与主线程交互。
 */
#ifndef __SERVER_H
#define __SERVER_H

#include <sys/epoll.h>
#include <sys/socket.h>
#include "ringbuffer.h"
#include "threadpool.h"
#include "async_log.h"
#include <stdint.h>
#include <time.h>      // 阶段6：time_t/time()，心跳超时检测用
#include <signal.h>    // 阶段7：sig_atomic_t，优雅关闭停止标记用

#define LISTEN_PORT 8888
#define MAX_EVENTS 1024
#define PACK_HEAD_LEN 4    // 包头：4字节长度
#define MAX_PKG_BODY_LEN (1024 * 1024)   // 包体最大长度，超过视为协议错误
#define CONN_BUF_SIZE 4096               // 每个连接读写缓冲区大小

// ===== 阶段6新增：并发上限 + 心跳超时配置 =====
#define MAX_CONN_NUM 1024        // 最大并发连接数，超过后新连接直接拒绝，防止连接打满服务器
#define HEARTBEAT_TIMEOUT 60     // 心跳超时阈值（秒）：超过该时长没有任何数据的连接视为僵死，主动清理
#define HEARTBEAT_CHECK_SEC 5    // 心跳扫描周期（秒）：主循环每隔该时长扫一遍连接链表

// 一个客户端连接
typedef struct conn
{
    int fd;
    RingBuf *rbuf;   // 读环形缓冲
    RingBuf *wbuf;   // 写环形缓冲
    uint64_t conn_id;      // 阶段5：连接唯一ID。工作线程回包时凭ID定位连接，
                           // 而不是直接拿Conn*（连接可能已被主线程关闭释放）
    struct conn *next;     // 阶段5：活跃连接链表指针，仅主线程维护
    time_t last_active;    // 阶段6：最近一次收到数据的时间（秒级），心跳超时检测依据
} Conn;

// 阶段5：回包队列节点（工作线程生产、主线程消费），具体定义在server.c
typedef struct resp_node RespNode;

// 服务器全局
typedef struct server
{
    int epfd;
    int listen_fd;
    struct epoll_event events[MAX_EVENTS];
    ThreadPool *pool;
    LogMgr *log;

    // ===== 阶段5新增：线程池工作线程 -> epoll主线程 的回包通道 =====
    int wakeup_fd;              // eventfd：工作线程写它，唤醒阻塞在epoll_wait的主线程
    pthread_mutex_t resp_mutex; // 保护回包队列的互斥锁（队列被多线程访问）
    RespNode *resp_head;        // 回包队列头（主线程消费）
    RespNode *resp_tail;        // 回包队列尾（工作线程追加）
    Conn *conn_list;            // 活跃连接链表，仅主线程增删查
    uint64_t next_conn_id;      // 连接ID分配器，单调递增，fd复用也不会撞ID

    // ===== 阶段6新增：并发上限 + 心跳超时 =====
    int conn_count;             // 当前活跃连接数，仅主线程维护（接入+1，关闭-1）
    time_t next_check_time;     // 下一次心跳扫描的时间点，server_create里初始化

    // ===== 阶段7新增：优雅关闭 =====
    // volatile sig_atomic_t：信号处理函数里写、主循环里读。
    // volatile防编译器把读取优化掉；sig_atomic_t保证对该类型的读写是原子的，
    // 不会读到"写了一半"的值（普通int在信号打断时理论上可能读到中间状态）
    volatile sig_atomic_t stop;
} Server;

Server* server_create(ThreadPool *pool, LogMgr *log);
void server_setup_signals(Server *srv);   // 阶段7：注册SIGINT/SIGTERM处理，忽略SIGPIPE
void server_run(Server *srv);
void server_stop(Server *srv);   // 阶段7：请求退出主循环（信号处理函数里调用，只置标记）
void server_destroy(Server *srv);

#endif
