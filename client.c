// =====================================================================
// client.c —— 阶段8：配套TCP测试客户端
//
// 按服务器协议（4字节大端长度头 + 包体）发包，完整测试收发全链路。
// 相比简易版，补齐了TCP字节流语义下的健壮收发（readn/writen），
// 并内置多种测试模式，覆盖正常echo、粘包、半包、大包场景：
//
//   ./client            默认basic：多轮正常echo收发
//   ./client basic      同上
//   ./client sticky     粘包：多个包拼在一次write里发出，验证服务器拆包
//   ./client half       半包：一个包拆成多次write慢速发出，验证服务器缓存拼包
//   ./client big        大包：接近读缓冲上限的包体，验证多轮read累积拆包
//   ./client all        依次跑完以上全部模式
//
// 编译：make client   （或 gcc client.c -o client）
// 运行前先在另一个终端启动服务器 ./main
//
// 两个与服务器实现相关的约束（测试设计依据）：
//   1. 服务器每连接读缓冲CONN_BUF_SIZE=4096：单包总长(4+包体)不能超过它，
//      否则服务器rbuf写满会主动断开。所以big模式用3000字节包体。
//   2. 服务器线程池任务队列容量8：sticky模式一次发5个包，不会把队列打满。
// =====================================================================
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>

#define SERVER_IP   "127.0.0.1"
#define SERVER_PORT 8888
#define RECV_BUF    4096      // 收包缓冲，与服务器CONN_BUF_SIZE对齐

// ---------------- 连接与健壮收发 ----------------

// 连接服务器，成功返回fd，失败返回-1
static int connect_server(void)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0)
    {
        perror("socket");
        return -1;
    }
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SERVER_PORT);
    if (inet_pton(AF_INET, SERVER_IP, &addr.sin_addr) <= 0)
    {
        perror("inet_pton");
        close(fd);
        return -1;
    }
    if (connect(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0)
    {
        perror("connect");
        close(fd);
        return -1;
    }
    return fd;
}

// 循环读满n字节。TCP是字节流，一次read不保证读满n字节（半包），
// 必须循环读到够数为止。返回实际读到的字节数：
//   读满返回n；对端关闭返回<n；出错返回-1
static int readn(int fd, char *buf, int n)
{
    int left = n;
    while (left > 0)
    {
        int got = read(fd, buf, left);
        if (got < 0)
        {
            if (errno == EINTR)
                continue;       // 被信号打断，重试
            return -1;
        }
        if (got == 0)
            break;              // 对端关闭（收到FIN）
        buf += got;
        left -= got;
    }
    return n - left;
}

// 循环写完n字节。一次write也可能只写入部分数据（短写），
// 同样必须循环。返回0成功，-1失败
static int writen(int fd, const char *buf, int n)
{
    int left = n;
    while (left > 0)
    {
        int put = write(fd, buf, left);
        if (put < 0)
        {
            if (errno == EINTR)
                continue;
            return -1;
        }
        buf += put;
        left -= put;
    }
    return 0;
}

// ---------------- 协议封包/解包 ----------------

// 按协议发包：4字节大端长度头 + 包体。
// 用显式len而不是strlen，支持任意二进制包体
static int send_packet(int fd, const char *body, int len)
{
    uint32_t net_len = htonl((uint32_t)len);
    if (writen(fd, (char*)&net_len, 4) != 0)
        return -1;
    if (len > 0 && writen(fd, body, len) != 0)
        return -1;
    return 0;
}

// 按协议收包：先读满4字节头，再按头里的长度读满包体。
// 成功返回包体长度(>=0)，失败/对端关闭返回-1
static int recv_packet(int fd, char *out, int out_size)
{
    uint32_t net_len;
    if (readn(fd, (char*)&net_len, 4) != 4)
        return -1;
    uint32_t body_len = ntohl(net_len);
    if (body_len > (uint32_t)(out_size - 1))
        return -1;              // 超出本地缓冲，协议异常
    if (body_len == 0)
    {
        out[0] = '\0';
        return 0;
    }
    if (readn(fd, out, (int)body_len) != (int)body_len)
        return -1;
    out[body_len] = '\0';
    return (int)body_len;
}

// 一次完整的"发包->收echo->比对"，内容逐字节一致才算通过。
// 返回0通过，-1失败
static int check_echo(int fd, const char *body, int len, const char *tag)
{
    char buf[RECV_BUF];
    if (send_packet(fd, body, len) != 0)
    {
        printf("[%s] FAIL：发包失败（连接可能已被服务器断开）\n", tag);
        return -1;
    }
    int n = recv_packet(fd, buf, RECV_BUF);
    if (n != len || memcmp(buf, body, len) != 0)
    {
        printf("[%s] FAIL：echo不一致（期望%d字节，实收%d字节）\n", tag, len, n);
        return -1;
    }
    printf("[%s] PASS：%d字节echo逐字节一致\n", tag, len);
    return 0;
}

// ---------------- 测试模式 ----------------

// basic：多轮正常收发，验证阶段5建立的echo闭环
static int test_basic(int fd)
{
    const char *msgs[] = {"hello world", "stage8 client", "echo round 3"};
    for (int i = 0; i < 3; i++)
    {
        char tag[32];
        snprintf(tag, sizeof(tag), "basic-%d", i + 1);
        if (check_echo(fd, msgs[i], (int)strlen(msgs[i]), tag) != 0)
            return -1;
    }
    return 0;
}

// sticky（粘包）：把5个完整包拼进一个缓冲，一次write全部发出。
// 对服务器来说这5个包会在一次EPOLLIN里到达（甚至一次read就读进多个），
// 考验parse_packet的while循环能否连续拆出多个完整包、逐个投递线程池。
// 回包也按同样顺序逐个到达，客户端逐个收并比对
static int test_sticky(int fd)
{
    char bulk[512];
    int off = 0;
    char bodies[5][32];
    int lens[5];

    // 构造5个包：长度头+包体 依次拼接
    for (int i = 0; i < 5; i++)
    {
        snprintf(bodies[i], sizeof(bodies[i]), "sticky-packet-%d", i);
        lens[i] = (int)strlen(bodies[i]);
        uint32_t net_len = htonl((uint32_t)lens[i]);
        memcpy(bulk + off, &net_len, 4);
        off += 4;
        memcpy(bulk + off, bodies[i], lens[i]);
        off += lens[i];
    }

    // 关键：一次write发出全部5个包（制造粘包）
    if (writen(fd, bulk, off) != 0)
    {
        printf("[sticky] FAIL：粘包发送失败\n");
        return -1;
    }
    printf("[sticky] 一次write发出5个包共%d字节\n", off);

    // 逐个收echo并比对（服务器线程池并发处理，但回包队列尾插保证顺序）
    for (int i = 0; i < 5; i++)
    {
        char buf[RECV_BUF];
        char tag[32];
        snprintf(tag, sizeof(tag), "sticky-%d", i);
        int n = recv_packet(fd, buf, RECV_BUF);
        if (n != lens[i] || memcmp(buf, bodies[i], lens[i]) != 0)
        {
            printf("[%s] FAIL：第%d个回包不符（期望%d字节实收%d）\n", tag, i, lens[i], n);
            return -1;
        }
        printf("[%s] PASS：%s\n", tag, buf);
    }
    return 0;
}

// half（半包）：把一个包拆成多次write、中间故意停顿，
// 让服务器每次EPOLLIN只收到包的一部分，考验rbuf缓存+半包等待逻辑。
// 用例1：包头本身被拆成2+2字节（服务器连长度头都要分两次凑齐）
// 用例2：包体被拆成3段慢速发送
static int test_half(int fd)
{
    const char *body = "half packet reassembly test";
    int len = (int)strlen(body);
    uint32_t net_len = htonl((uint32_t)len);
    char buf[RECV_BUF];

    // 用例1：包头拆成2+2
    if (writen(fd, (char*)&net_len, 2) != 0)
        return -1;
    usleep(300000);                          // 停300ms，确保服务器先只收到半个头
    if (writen(fd, (char*)&net_len + 2, 2) != 0)
        return -1;
    usleep(300000);
    if (writen(fd, body, len) != 0)
        return -1;
    int n = recv_packet(fd, buf, RECV_BUF);
    if (n != len || memcmp(buf, body, len) != 0)
    {
        printf("[half-1] FAIL：包头半包场景echo不符\n");
        return -1;
    }
    printf("[half-1] PASS：包头拆成2+2字节仍正确拼包\n");

    // 用例2：完整头 + 包体拆3段
    if (writen(fd, (char*)&net_len, 4) != 0)
        return -1;
    usleep(300000);
    int part = len / 3;
    if (writen(fd, body, part) != 0)                 // 第1段
        return -1;
    usleep(300000);
    if (writen(fd, body + part, part) != 0)          // 第2段
        return -1;
    usleep(300000);
    if (writen(fd, body + 2 * part, len - 2 * part) != 0)  // 剩余
        return -1;
    n = recv_packet(fd, buf, RECV_BUF);
    if (n != len || memcmp(buf, body, len) != 0)
    {
        printf("[half-2] FAIL：包体半包场景echo不符\n");
        return -1;
    }
    printf("[half-2] PASS：包体拆成3段慢速发送仍正确拼包\n");
    return 0;
}

// big（大包）：3000字节包体（总长3004，小于服务器读缓冲4096）。
// 服务器on_read每次最多read 1024字节，这个包要4次read才收全，
// 考验rbuf跨多轮事件累积数据、直到凑齐完整包再拆包投递。
// 包体用可打印的规律字符填充，便于逐字节比对
static int test_big(int fd)
{
    char body[3000];
    for (int i = 0; i < (int)sizeof(body); i++)
        body[i] = (char)('a' + i % 26);

    if (check_echo(fd, body, (int)sizeof(body), "big") != 0)
        return -1;
    return 0;
}

// ---------------- 入口 ----------------

// 跑一个模式：独立建连接->执行->关闭。返回0通过-1失败
static int run_mode(const char *mode)
{
    printf("===== 模式 %s =====\n", mode);
    int fd = connect_server();
    if (fd < 0)
        return -1;

    int ret;
    if (strcmp(mode, "basic") == 0)
        ret = test_basic(fd);
    else if (strcmp(mode, "sticky") == 0)
        ret = test_sticky(fd);
    else if (strcmp(mode, "half") == 0)
        ret = test_half(fd);
    else if (strcmp(mode, "big") == 0)
        ret = test_big(fd);
    else
    {
        printf("未知模式: %s（可选 basic/sticky/half/big/all）\n", mode);
        close(fd);
        return -1;
    }
    close(fd);
    printf("===== 模式 %s：%s =====\n\n", mode, ret == 0 ? "PASS" : "FAIL");
    return ret;
}

int main(int argc, char *argv[])
{
    const char *mode = (argc > 1) ? argv[1] : "basic";

    if (strcmp(mode, "all") == 0)
    {
        // 依次跑全部模式，任何一个失败总体即失败
        const char *modes[] = {"basic", "sticky", "half", "big"};
        int fail = 0;
        for (int i = 0; i < 4; i++)
            if (run_mode(modes[i]) != 0)
                fail = 1;
        printf("总体结果：%s\n", fail ? "FAIL" : "PASS");
        return fail ? 1 : 0;
    }
    return run_mode(mode) == 0 ? 0 : 1;
}
