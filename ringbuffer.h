/*
 * ringbuffer.h —— 环形缓冲区接口（阶段1）
 *
 * 用途：TCP连接的收发缓存，解决流式数据的半包/粘包缓存问题。
 * 特性：peek预读不移动读位置（拆包探包头用）；空间不足写入返回-1。
 * 线程安全：本组件不加锁。每连接的rbuf/wbuf仅由epoll主线程访问，
 *           由使用方保证单线程访问约定。
 */
#ifndef __RINGBUFFER_H
#define __RINGBUFFER_H
#include <stdint.h>
#include <string.h>

// 环形缓冲区结构体
typedef struct {
    char *buf;          // 缓冲区内存起始地址
    int size;           // 缓冲区总大小
    int read_pos;       // 读位置：下一次读从这里拿
    int write_pos;      // 写位置：下一次写写到这里
} RingBuf;

/**
 * @brief  创建环形缓冲区
 * @param  size 缓冲区总字节数
 * @return 成功返回RingBuf*，失败返回NULL
 */
RingBuf *ring_buf_create(int size);

/**
 * @brief  销毁环形缓冲区，释放内存
 * @param  rb 缓冲区指针
 */
void ring_buf_destroy(RingBuf *rb);

/**
 * @brief  往环形缓冲区写入数据
 * @param  rb    缓冲区
 * @param  data  待写入数据起始地址
 * @param  len   待写入字节长度
 * @return 成功返回写入字节数，空间不足返回-1
 */
int ring_buf_write(RingBuf *rb, const char *data, int len);

/**
 * @brief  从环形缓冲区读出数据
 * @param  rb    缓冲区
 * @param  out   读出数据存放地址
 * @param  len   期望读出字节长度
 * @return 实际读出字节数
 */
int ring_buf_read(RingBuf *rb, char *out, int len);

/**
 * @brief  预读数据，只拷贝不移动读位置（拆包时先探包头用）
 * @param  rb    缓冲区
 * @param  out   读出数据存放地址
 * @param  len   期望预读字节长度
 * @return 实际预读字节数
 */
int ring_buf_peek(RingBuf *rb, char *out, int len);

/**
 * @brief  获取缓冲区里面有效数据长度
 */
int ring_buf_get_data_len(RingBuf *rb);

/**
 * @brief  获取缓冲区剩余可用空闲空间
 */
int ring_buf_get_free_len(RingBuf *rb);

#endif
