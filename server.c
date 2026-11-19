/*
 * server.c —— 服务器核心实现（阶段4~7）
 *
 * 数据流：EPOLLIN -> on_read读入rbuf -> parse_packet按4字节长度头拆包
 *         -> 线程池handle_packet(echo业务) -> 回包队列+eventfd唤醒
 *         -> on_wakeup按conn_id定位连接 -> 写wbuf -> conn_flush发送
 *         -> 发不完挂EPOLLOUT续发，发空摘除。
 * 关键设计：
 *   - ET模式：accept/read/write全部循环处理到EAGAIN，避免事件饿死
 *   - 生命周期：连接用单调递增conn_id标识，回包不带裸指针，
 *     连接已关闭时按ID查不到即安全丢弃，杜绝use-after-free
 *   - 优雅关闭：信号处理只置stop标记，清理按序放在主循环退出后
 */
#include "server.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>
#include <sys/eventfd.h>   // 阶段5：eventfd，工作线程唤醒主线程用
#include <sys/uio.h>       // 阶段5：writev，环形缓冲回绕时一次发两段
#include <time.h>          // 阶段6：time()，心跳超时检测用
#include <signal.h>        // 阶段7：sigaction/sigprocmask，优雅关闭信号处理
#include <stdio.h>         // 阶段7：printf

// 设置非阻塞
static int set_nonblock(int fd)
{
    int flag = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flag | O_NONBLOCK);
    return 0;
}

// 添加fd到epoll
// ptr 统一用 Conn* 传递：监听fd传NULL，业务fd传对应Conn，避免data.fd/data.ptr混用误判
static int epoll_add(int epfd, int fd, void *ptr, uint32_t events)
{
    struct epoll_event ev;
    ev.data.ptr = ptr;
    ev.events = events;
    return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
}

// 修改fd监听事件
static int epoll_mod(int epfd, int fd, void *ptr, uint32_t events)
{
    struct epoll_event ev;
    ev.data.ptr = ptr;
    ev.events = events;
    return epoll_ctl(epfd, EPOLL_CTL_MOD, fd, &ev);
}

// 关闭连接，释放资源
static void conn_close(Server *srv, Conn *conn)
{
    if (!conn)
        return;
    int fd = conn->fd;

    // 阶段5：从活跃连接链表摘除（链表仅主线程操作，无需加锁）。
    // 摘除后，工作线程迟到的回包按conn_id就查不到该连接，会被安全丢弃
    Conn **pp = &srv->conn_list;
    while (*pp && *pp != conn)
        pp = &(*pp)->next;
    if (*pp)
        *pp = conn->next;

    srv->conn_count--;   // 阶段6：活跃连接数减1（与on_accept里的+1配对）

    // 先从epoll摘掉，再释放，避免fd被复用时事件打到已释放的Conn上
    epoll_ctl(srv->epfd, EPOLL_CTL_DEL, fd, NULL);
    close(fd);
    ring_buf_destroy(conn->rbuf);
    ring_buf_destroy(conn->wbuf);
    free(conn);
    log_write(srv->log, "client fd=%d closed", fd);
}

// 传给线程池任务的参数：服务器指针 + 连接唯一ID + 完整包
typedef struct task_arg
{
    Server *srv;        // 阶段5：业务回包时通过它找回包通道
    uint64_t conn_id;   // 阶段5：回包目标连接的唯一ID（不能直接传Conn*，见下）
    char *pkg;
} TaskArg;

// ===================== 阶段5：EPOLLOUT异步写回 + 写缓冲 =====================
//
// 核心难点：业务在线程池的工作线程里执行，但连接的wbuf、epoll、fd
// 都只能由epoll主线程操作（单Reactor模型的线程边界）。
// 解决方案：工作线程把回包挂进"回包队列"（互斥锁保护），再写eventfd
// 唤醒阻塞在epoll_wait的主线程；主线程取出回包，写入连接wbuf并发送。
// 连接生命周期：回包只带conn_id不带Conn*，主线程处理时按ID在活跃链表
// 里查找，连接若已关闭则查不到，回包直接丢弃，不会访问已释放内存。

// 回包队列节点：一个完整回包（包头+包体）
struct resp_node
{
    uint64_t conn_id;   // 目标连接的唯一ID
    char *pkg;          // 完整回包（含4字节包头），malloc分配
    int pkg_len;        // 回包总长度
    RespNode *next;
};

// 按唯一ID在活跃连接链表里查找连接（仅主线程调用，无需加锁）
// 返回NULL说明连接已关闭，对应回包应丢弃
static Conn* conn_find_by_id(Server *srv, uint64_t id)
{
    for (Conn *c = srv->conn_list; c; c = c->next)
        if (c->conn_id == id)
            return c;
    return NULL;
}

// 业务任务：在线程池工作线程里执行
// 阶段5实现echo：把收到的包原样回给客户端（真实业务可在此替换）
static void handle_packet(void *arg)
{
    TaskArg *ta = (TaskArg*)arg;
    Server *srv = ta->srv;

    uint32_t net_len;
    memcpy(&net_len, ta->pkg, PACK_HEAD_LEN);
    int body_len = (int)ntohl(net_len);
    log_write(srv->log, "recv pkg: %s", ta->pkg + PACK_HEAD_LEN);

    // 回包直接复用收到的包（echo语义，内容完全一致）。
    // pkg的所有权从TaskArg转移给回包节点，最终由主线程发送后释放
    RespNode *node = malloc(sizeof(RespNode));
    if (!node)
    {
        log_write(srv->log, "conn_id=%llu resp malloc failed",
                  (unsigned long long)ta->conn_id);
        free(ta->pkg);
        free(ta);
        return;
    }
    node->conn_id = ta->conn_id;
    node->pkg = ta->pkg;
    node->pkg_len = PACK_HEAD_LEN + body_len;
    node->next = NULL;

    // 挂入回包队列（尾插，保证同一连接的回包按业务处理顺序发送）
    pthread_mutex_lock(&srv->resp_mutex);
    if (srv->resp_tail)
        srv->resp_tail->next = node;
    else
        srv->resp_head = node;
    srv->resp_tail = node;
    pthread_mutex_unlock(&srv->resp_mutex);

    // 写eventfd唤醒主线程：主线程可能正阻塞在epoll_wait，
    // 计数器+1会让eventfd变为可读，epoll_wait立即返回。
    // 写失败只记日志不回滚：主线程后续任何事件都会顺带排空回包队列
    uint64_t one = 1;
    if (write(srv->wakeup_fd, &one, sizeof(one)) != sizeof(one))
        log_write(srv->log, "wakeup eventfd write failed: %s", strerror(errno));

    free(ta);   // pkg已交给回包节点，这里只释放任务参数
}

// 把回包队列里所有节点取走（加锁时间极短，只做指针摘链）
// 返回摘下的链表头，调用方（主线程）在无锁环境下逐个处理
static RespNode* resp_drain(Server *srv)
{
    pthread_mutex_lock(&srv->resp_mutex);
    RespNode *head = srv->resp_head;
    srv->resp_head = NULL;
    srv->resp_tail = NULL;
    pthread_mutex_unlock(&srv->resp_mutex);
    return head;
}

// 前置声明：发送函数在文件后面定义
static int conn_flush(Server *srv, Conn *conn);

// 主线程处理一个回包：定位连接 -> 写入写缓冲 -> 尝试发送
// 返回0连接存活，-1连接已被关闭释放（调用方不得再用conn）
static int conn_handle_resp(Server *srv, RespNode *node)
{
    Conn *conn = conn_find_by_id(srv, node->conn_id);
    if (!conn)
    {
        // 连接已关闭（客户端断开/协议错误），回包无意义，直接丢弃
        free(node->pkg);
        free(node);
        return 0;
    }

    // 写缓冲剩余空间不足：说明客户端收得太慢、积压超过上限，
    // 视为异常连接断开，防止内存被无限积压撑爆
    if (ring_buf_get_free_len(conn->wbuf) < node->pkg_len)
    {
        log_write(srv->log, "client fd=%d wbuf overflow, close", conn->fd);
        free(node->pkg);
        free(node);
        conn_close(srv, conn);
        return -1;
    }
    ring_buf_write(conn->wbuf, node->pkg, node->pkg_len);
    free(node->pkg);
    free(node);

    // 先尝试直接发（多数情况下内核发送缓冲有空间，一次就发完，
    // 根本不需要等EPOLLOUT，这是最快的路径）
    return conn_flush(srv, conn);
}

// eventfd可读事件：说明工作线程投递了新回包。
// eventfd注册的是LT模式：只要计数器>0就会持续上报，
// 所以这里读一次清零即可，未处理完的数据下轮还会提醒，不会丢事件
static void on_wakeup(Server *srv)
{
    uint64_t cnt;
    while (read(srv->wakeup_fd, &cnt, sizeof(cnt)) < 0)
    {
        if (errno == EINTR)
            continue;
        break;   // EAGAIN等：没有更多通知，正常退出
    }

    // 排空回包队列，逐个写入对应连接的写缓冲并发送
    RespNode *head = resp_drain(srv);
    while (head)
    {
        RespNode *next = head->next;
        conn_handle_resp(srv, head);   // 内部可能关闭连接，节点已释放
        head = next;
    }
}

// 拆包：从读缓冲拿完整包
// 返回 0 表示连接仍可用，-1 表示连接已被关闭释放（调用方不得再使用conn）
static int parse_packet(Server *srv, Conn *conn)
{
    while (ring_buf_get_data_len(conn->rbuf) >= PACK_HEAD_LEN)
    {
        // 先预读4字节长度，不移动读位置
        uint32_t net_len;
        ring_buf_peek(conn->rbuf, (char*)&net_len, PACK_HEAD_LEN);
        uint32_t pkg_len = ntohl(net_len);

        // 包体长度非法：协议错误，断开连接，避免超大malloc/整数溢出
        if (pkg_len > MAX_PKG_BODY_LEN)
        {
            log_write(srv->log, "client fd=%d bad pkg_len=%u", conn->fd, pkg_len);
            conn_close(srv, conn);
            return -1;
        }

        if (ring_buf_get_data_len(conn->rbuf) >= (int)(PACK_HEAD_LEN + pkg_len))
        {
            // 取出完整包
            char *pkg = malloc(PACK_HEAD_LEN + pkg_len + 1);
            TaskArg *ta = malloc(sizeof(TaskArg));
            if (!pkg || !ta)
            {
                free(pkg);
                free(ta);
                log_write(srv->log, "client fd=%d malloc failed", conn->fd);
                conn_close(srv, conn);
                return -1;
            }
            ring_buf_read(conn->rbuf, pkg, PACK_HEAD_LEN + pkg_len);
            pkg[PACK_HEAD_LEN + pkg_len] = '\0';   // 保证日志按字符串打印不越界
            ta->srv = srv;              // 阶段5：业务回包需要服务器上下文
            ta->conn_id = conn->conn_id; // 阶段5：用唯一ID标识回包目标连接
            ta->pkg = pkg;
            // 丢进线程池；入队失败就地释放，避免内存泄漏
            if (threadpool_add_task(srv->pool, handle_packet, ta) != 0)
            {
                log_write(srv->log, "client fd=%d add task failed", conn->fd);
                free(pkg);
                free(ta);
            }
        }
        else
        {
            // 半包，等下次数据到达
            break;
        }
    }
    return 0;
}

// 读事件处理
// 返回 1 表示conn仍然存活，0 表示连接已关闭并释放（调用方不得再使用conn）
static int on_read(Server *srv, Conn *conn)
{
    char tmp[1024];
    while (1)
    {
        int n = read(conn->fd, tmp, sizeof(tmp));
        if (n > 0)
        {
            // 阶段6：收到任何数据都刷新活跃时间（业务包或心跳包都算"活着"），
            // 心跳扫描时以此判断连接是否超时
            conn->last_active = time(NULL);
            // 缓冲区写满：数据已无法完整保存，断开连接避免协议错乱
            if (ring_buf_write(conn->rbuf, tmp, n) < 0)
            {
                log_write(srv->log, "client fd=%d rbuf full, close", conn->fd);
                conn_close(srv, conn);
                return 0;
            }
            if (parse_packet(srv, conn) != 0)
                return 0;   // conn已在拆包中被释放
        }
        else if (n == 0)
        {
            // 对端关闭
            conn_close(srv, conn);
            return 0;
        }
        else
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }
            if (errno == EINTR)
            {
                continue;
            }
            conn_close(srv, conn);
            return 0;
        }
    }
    return 1;
}

// 阶段5：把写缓冲里的数据尽量发给客户端（仅主线程调用）
// 返回0连接存活，-1连接已被关闭释放（调用方不得再用conn）
static int conn_flush(Server *srv, Conn *conn)
{
    while (ring_buf_get_data_len(conn->wbuf) > 0)
    {
        // 环形缓冲的数据可能回绕成两段，用writev一次系统调用发两段，
        // 避免"拷贝到临时缓冲再send"的额外开销。
        // 直接从RingBuf内部字段计算两段的位置和长度（与ringbuffer.c实现一致）
        RingBuf *rb = conn->wbuf;
        int len = ring_buf_get_data_len(rb);
        struct iovec iov[2];
        int iovcnt;
        if (rb->read_pos + len <= rb->size)
        {
            // 未回绕：一段连续数据
            iov[0].iov_base = rb->buf + rb->read_pos;
            iov[0].iov_len = len;
            iovcnt = 1;
        }
        else
        {
            // 回绕：read_pos到缓冲区末尾一段 + 缓冲区开头一段
            iov[0].iov_base = rb->buf + rb->read_pos;
            iov[0].iov_len = rb->size - rb->read_pos;
            iov[1].iov_base = rb->buf;
            iov[1].iov_len = len - iov[0].iov_len;
            iovcnt = 2;
        }

        ssize_t n = writev(conn->fd, iov, iovcnt);
        if (n > 0)
        {
            // 发出去多少就从写缓冲消费多少（移动读位置）
            char drop[1024];
            int left = (int)n;
            while (left > 0)
            {
                int chunk = left > (int)sizeof(drop) ? (int)sizeof(drop) : left;
                ring_buf_read(rb, drop, chunk);
                left -= chunk;
            }
            continue;   // 继续循环，直到发完或内核缓冲满
        }
        if (n < 0 && errno == EINTR)
            continue;   // 被信号打断，重试
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            // 内核发送缓冲已满：注册EPOLLOUT，等可写事件再继续发。
            // 注意ET模式只在"不可写->可写"跳变时通知一次，
            // 所以on_write里必须循环发到EAGAIN，这里注册后直接返回
            epoll_mod(srv->epfd, conn->fd, conn, EPOLLIN | EPOLLOUT | EPOLLET);
            return 0;
        }
        // 其他错误（EPIPE/ECONNRESET等）：对端已异常，关闭连接
        log_write(srv->log, "client fd=%d send failed: %s", conn->fd, strerror(errno));
        conn_close(srv, conn);
        return -1;
    }

    // 写缓冲已发空：摘掉EPOLLOUT，只留EPOLLIN。
    // 不摘的话ET下虽然不会频繁触发，但语义上应保持"有数据才关注可写"
    epoll_mod(srv->epfd, conn->fd, conn, EPOLLIN | EPOLLET);
    return 0;
}

// 写事件处理（EPOLLOUT触发）：继续把写缓冲里剩余数据发出去
static void on_write(Server *srv, Conn *conn)
{
    conn_flush(srv, conn);   // 内部可能已conn_close，conn此后不可再用
}

// ===================== 阶段6：心跳超时检测 =====================
//
// 思路：不给每个连接单独建定时器（连接多时开销大），而是主循环里
// 周期性（HEARTBEAT_CHECK_SEC秒）遍历一次活跃连接链表，把超过
// HEARTBEAT_TIMEOUT秒没有任何数据的连接判定为僵死，主动关闭。
// 连接"活着"的定义：收到过任何数据（业务包、心跳包均可），
// 每次on_read读到数据都会刷新conn->last_active。
// 扫描和关闭都在主线程，链表无需加锁；用二级指针遍历，
// 关闭节点（摘链+释放）后仍能正确推进，不会踩已释放内存。
static void check_heartbeat(Server *srv)
{
    time_t now = time(NULL);
    Conn **pp = &srv->conn_list;
    while (*pp)
    {
        Conn *c = *pp;
        if (now - c->last_active > HEARTBEAT_TIMEOUT)
        {
            log_write(srv->log, "client fd=%d heartbeat timeout (%lds), close",
                      c->fd, (long)(now - c->last_active));
            conn_close(srv, c);   // 内部已把c从链表摘除，*pp自动指向下一个节点
        }
        else
        {
            pp = &c->next;        // 未超时，继续看下一个
        }
    }
}

// 接受新连接（listen_fd是ET模式，一次事件要把所有等待的连接都收完）
static void on_accept(Server *srv)
{
    while (1)
    {
        struct sockaddr_in cli_addr;
        socklen_t len = sizeof(cli_addr);
        int cfd = accept(srv->listen_fd, (struct sockaddr*)&cli_addr, &len);
        if (cfd < 0)
        {
            if (errno == EINTR)
                continue;
            // EAGAIN表示没有更多连接，正常结束本轮
            return;
        }

        // 阶段6：并发上限保护。连接数已达MAX_CONN_NUM时直接关闭新连接，
        // 不分配任何资源，防止恶意/异常客户端把服务器连接打满。
        // 注意必须先把cfd accept出来再close，否则ET模式下该连接会一直
        // 留在就绪队列里反复触发事件
        if (srv->conn_count >= MAX_CONN_NUM)
        {
            log_write(srv->log, "max conn %d reached, reject fd=%d", MAX_CONN_NUM, cfd);
            close(cfd);
            continue;
        }

        Conn *conn = malloc(sizeof(Conn));
        if (!conn)
        {
            close(cfd);
            continue;
        }
        conn->fd = cfd;
        conn->rbuf = ring_buf_create(CONN_BUF_SIZE);
        conn->wbuf = ring_buf_create(CONN_BUF_SIZE);
        if (!conn->rbuf || !conn->wbuf)
        {
            ring_buf_destroy(conn->rbuf);
            ring_buf_destroy(conn->wbuf);
            free(conn);
            close(cfd);
            log_write(srv->log, "accept fd=%d buf alloc failed", cfd);
            continue;
        }

        // 阶段5：分配唯一ID并挂入活跃连接链表（头插，仅主线程操作）。
        // ID单调递增，即使fd被关闭后复用，也不会和旧连接的ID冲突
        conn->conn_id = ++srv->next_conn_id;
        conn->next = srv->conn_list;
        srv->conn_list = conn;
        conn->last_active = time(NULL);   // 阶段6：接入即视为一次活跃
        srv->conn_count++;                // 阶段6：活跃连接数加1

        set_nonblock(cfd);
        // 用data指针存Conn*（经典做法）
        struct epoll_event ev;
        ev.data.ptr = conn;
        ev.events = EPOLLIN | EPOLLET;
        if (epoll_ctl(srv->epfd, EPOLL_CTL_ADD, cfd, &ev) < 0)
        {
            ring_buf_destroy(conn->rbuf);
            ring_buf_destroy(conn->wbuf);
            free(conn);
            close(cfd);
            continue;
        }

        log_write(srv->log, "new client fd=%d ip=%s", cfd, inet_ntoa(cli_addr.sin_addr));
    }
}

// ===================== 阶段7：信号处理 + 优雅关闭 =====================
//
// 关闭流程总览（顺序是安全的关键，详见main.c）：
//   Ctrl+C/SIGTERM -> 处理函数只置stop标记 -> 主循环退出
//   -> 先销毁线程池（等工作线程全部退出，此后没人再碰srv）
//   -> 再销毁服务器（关连接、清回包队列）-> 最后销毁日志（冲刷完队列）
//
// 信号安全要点：信号处理函数运行在"打断主线程任意位置"的上下文里，
// 只能做异步信号安全的操作。这里只写一个volatile sig_atomic_t标记，
// 不加锁、不malloc、不printf、不调log_write，绝对安全。
// 真正的清理全部放到主循环退出后的正常上下文里按序执行。

// 当前运行的服务器实例指针，仅供信号处理函数定位stop标记。
// 只在main里注册信号前赋值一次，此后只读，无并发写问题
static Server *g_server_for_signal = NULL;

// SIGINT(Ctrl+C)/SIGTERM(kill默认信号)的处理函数：只置标记，不做任何清理
static void on_exit_signal(int signo)
{
    (void)signo;
    if (g_server_for_signal)
        g_server_for_signal->stop = 1;
}

// 注册退出信号。用sigaction而不是signal：语义明确、跨平台行为一致。
// 故意不设SA_RESTART：让阻塞中的epoll_wait被信号打断返回EINTR，
// 主循环借此立即检查stop标记退出，而不是傻等到心跳超时才醒
void server_setup_signals(Server *srv)
{
    g_server_for_signal = srv;

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = on_exit_signal;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;               // 不带SA_RESTART，epoll_wait返回EINTR
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);

    // 忽略SIGPIPE：向已被对端关闭的连接write会收到该信号，默认动作是
    // 直接杀死进程。忽略后write返回EPIPE错误，由conn_flush按错误关连接。
    // （虽然conn_flush已处理EPIPE，但信号先于errno到达，必须在这里挡掉）
    signal(SIGPIPE, SIG_IGN);
}

// 请求退出主循环：只置标记。主循环最迟在一个心跳扫描周期内检查到并退出
// （epoll_wait被信号打断时则立即退出）。可从信号处理函数安全调用
void server_stop(Server *srv)
{
    if (srv)
        srv->stop = 1;
}

Server* server_create(ThreadPool *pool, LogMgr *log)
{
    Server *srv = malloc(sizeof(Server));
    if (!srv)
        return NULL;
    memset(srv, 0, sizeof(Server));
    srv->pool = pool;
    srv->log = log;
    srv->epfd = -1;
    srv->listen_fd = -1;
    srv->wakeup_fd = -1;

    // 阶段5：初始化回包队列互斥锁
    if (pthread_mutex_init(&srv->resp_mutex, NULL) != 0)
    {
        free(srv);
        return NULL;
    }

    // 阶段5：创建eventfd作为"工作线程->主线程"的唤醒通知。
    // EFD_NONBLOCK：读端非阻塞；EFD_CLOEXEC：exec时自动关闭
    srv->wakeup_fd = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (srv->wakeup_fd < 0)
    {
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    srv->epfd = epoll_create1(0);
    if (srv->epfd < 0)
    {
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0)
    {
        close(srv->epfd);
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    int opt = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in serv_addr;
    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(LISTEN_PORT);
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(srv->listen_fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0)
    {
        log_write(log, "bind port %d failed: %s", LISTEN_PORT, strerror(errno));
        close(srv->listen_fd);
        close(srv->epfd);
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }
    if (listen(srv->listen_fd, 128) < 0)
    {
        log_write(log, "listen failed: %s", strerror(errno));
        close(srv->listen_fd);
        close(srv->epfd);
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    set_nonblock(srv->listen_fd);
    // 监听fd的data.ptr置NULL，作为与客户端Conn区分的标记
    if (epoll_add(srv->epfd, srv->listen_fd, NULL, EPOLLIN | EPOLLET) < 0)
    {
        log_write(log, "epoll_add listen_fd failed: %s", strerror(errno));
        close(srv->listen_fd);
        close(srv->epfd);
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    // 阶段5：把eventfd挂进epoll。data.ptr存srv自身：
    // 事件分发时 ptr==srv 即唤醒事件（监听fd是NULL，客户端是Conn*）。
    // 用LT模式：计数器没清零就会持续上报，处理逻辑简单且不会丢通知
    if (epoll_add(srv->epfd, srv->wakeup_fd, srv, EPOLLIN) < 0)
    {
        log_write(log, "epoll_add wakeup_fd failed: %s", strerror(errno));
        close(srv->listen_fd);
        close(srv->epfd);
        close(srv->wakeup_fd);
        pthread_mutex_destroy(&srv->resp_mutex);
        free(srv);
        return NULL;
    }

    // 阶段6：初始化心跳扫描时间点（从当前时刻起，过一个扫描周期再首次检查）
    srv->next_check_time = time(NULL) + HEARTBEAT_CHECK_SEC;

    log_write(log, "server listen on port %d", LISTEN_PORT);
    return srv;
}

void server_run(Server *srv)
{
    if (!srv)
        return;
    while (!srv->stop)   // 阶段7：收到退出信号置位stop后，循环条件不成立即退出
    {
        // 阶段6：epoll_wait不再无限阻塞，而是最多阻塞到"下一次心跳扫描"。
        // 这样即使所有客户端都静默（没有任何IO事件），主线程也能按时
        // 醒来扫描僵死连接；有事件时照常立即返回，不影响实时性。
        // 用差值计算超时而非固定周期，避免每轮都time()带来的扫描漂移
        time_t now = time(NULL);
        int timeout_ms = (int)(srv->next_check_time - now) * 1000;
        if (timeout_ms < 0)
            timeout_ms = 0;   // 已过扫描时间点，立即返回去扫描
        int n = epoll_wait(srv->epfd, srv->events, MAX_EVENTS, timeout_ms);
        if (n < 0)
        {
            if (errno == EINTR)
                continue;   // 阶段7：被退出信号打断，回到循环条件检查stop后退出
            log_write(srv->log, "epoll_wait failed: %s", strerror(errno));
            break;
        }
        for (int i = 0; i < n; i++)
        {
            struct epoll_event *ev = &srv->events[i];
            // data.ptr为NULL说明是监听fd
            if (ev->data.ptr == NULL)
            {
                on_accept(srv);
                continue;
            }
            // 阶段5：data.ptr等于srv自身说明是eventfd唤醒事件
            if (ev->data.ptr == srv)
            {
                on_wakeup(srv);
                continue;
            }

            Conn *conn = (Conn*)ev->data.ptr;
            // 出错/挂断：直接关闭，不再读写
            if (ev->events & (EPOLLERR | EPOLLHUP))
            {
                conn_close(srv, conn);
                continue;
            }
            // 阶段5：EPOLLIN和EPOLLOUT可能同时上报，先读后写；
            // on_read/on_write内部都可能conn_close，conn关闭后不得再使用
            int alive = 1;
            if (ev->events & EPOLLIN)
                alive = on_read(srv, conn);
            if (alive && (ev->events & EPOLLOUT))
                on_write(srv, conn);
        }

        // 阶段6：到达扫描时间点就遍历连接链表清理僵死连接。
        // 放在事件处理之后：本轮刚收到数据的连接last_active已刷新，不会被误杀
        now = time(NULL);
        if (now >= srv->next_check_time)
        {
            check_heartbeat(srv);
            srv->next_check_time = now + HEARTBEAT_CHECK_SEC;
        }
    }

    // ===== 阶段7：主循环已退出，做"尽力而为"的收尾发送 =====
    // 此刻工作线程可能还有正在执行的任务，它们产生的回包会陆续进队列。
    // 这里再排空一次，把已入队的回包尽量发出去（发不下的由destroy丢弃）。
    // 注意：不能在这里关连接——工作线程还在跑，必须等main里
    // threadpool_destroy返回（所有工作线程已退出）后才能安全释放连接
    on_wakeup(srv);
    for (Conn *c = srv->conn_list; c; c = c->next)
        conn_flush(srv, c);   // 把各连接写缓冲里剩余数据尽量发完

    log_write(srv->log, "server main loop stopped");
}

void server_destroy(Server *srv)
{
    if (!srv)
        return;

    // 阶段5：关闭所有还活着的客户端连接（正常应在阶段7优雅关闭时先停业务）
    while (srv->conn_list)
        conn_close(srv, srv->conn_list);

    // 阶段5：清空回包队列里还没处理的节点，避免内存泄漏
    RespNode *head = resp_drain(srv);
    while (head)
    {
        RespNode *next = head->next;
        free(head->pkg);
        free(head);
        head = next;
    }

    close(srv->listen_fd);
    close(srv->epfd);
    if (srv->wakeup_fd >= 0)
        close(srv->wakeup_fd);
    pthread_mutex_destroy(&srv->resp_mutex);
    free(srv);
}
