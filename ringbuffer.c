/*
 * ringbuffer.c —— 环形缓冲区实现（阶段1）
 *
 * 要点：读写各自取模回绕，跨界数据分两段memcpy；
 *       预留1字节区分空/满（可用空间=size-1-数据长度）；
 *       peek与read同构但不移动read_pos，供拆包预读包头。
 */
#include "ringbuffer.h"
#include <stdlib.h>

RingBuf *ring_buf_create(int size)
{
    // 分配结构体本身
    RingBuf *rb = (RingBuf *)malloc(sizeof(RingBuf));
    if (rb == NULL)
    {
        return NULL;
    }
    // 分配缓冲区内存
    rb->buf = (char *)malloc(size);
    if (rb->buf == NULL)
    {
        free(rb);
        return NULL;
    }
    rb->size = size;
    rb->read_pos = 0;
    rb->write_pos = 0;
    return rb;
}

void ring_buf_destroy(RingBuf *rb)
{
    if (rb == NULL)
        return;
    // 先释放缓冲区数组，再释放结构体
    free(rb->buf);
    free(rb);
}

int ring_buf_write(RingBuf *rb, const char *data, int len)
{
    // 校验参数
    if (rb == NULL || data == NULL || len <= 0)
        return -1;
    int free_len = ring_buf_get_free_len(rb);
    // 空闲空间不够，直接返回失败
    if (len > free_len)
        return -1;

    // 从write_pos到缓冲区末尾还有多少字节
    int seg1 = rb->size - rb->write_pos;
    if (seg1 >= len)
    {
        // 不需要绕回，直接连续拷贝
        memcpy(rb->buf + rb->write_pos, data, len);
    }
    else
    {
        // 分两段拷贝：先写到缓冲区末尾，剩下从头开始写
        memcpy(rb->buf + rb->write_pos, data, seg1);
        memcpy(rb->buf, data + seg1, len - seg1);
    }
    // 更新写位置，取模环形
    rb->write_pos = (rb->write_pos + len) % rb->size;
    return len;
}

int ring_buf_read(RingBuf *rb, char *out, int len)
{
    if (rb == NULL || out == NULL || len <= 0)
        return 0;
    int data_len = ring_buf_get_data_len(rb);
    // 不能读超过现有有效数据
    if (len > data_len)
        len = data_len;
    if (len <= 0)
        return 0;

    // 从read_pos到缓冲区末尾还有多少有效数据
    int seg1 = rb->size - rb->read_pos;
    if (seg1 >= len)
    {
        // 不需要绕回，连续读出
        memcpy(out, rb->buf + rb->read_pos, len);
    }
    else
    {
        // 两段读出：末尾一段 + 开头一段
        memcpy(out, rb->buf + rb->read_pos, seg1);
        memcpy(out + seg1, rb->buf, len - seg1);
    }
    // 更新读位置
    rb->read_pos = (rb->read_pos + len) % rb->size;
    return len;
}

int ring_buf_peek(RingBuf *rb, char *out, int len)
{
    if (rb == NULL || out == NULL || len <= 0)
        return 0;
    int data_len = ring_buf_get_data_len(rb);
    // 不能预读超过现有有效数据
    if (len > data_len)
        len = data_len;
    if (len <= 0)
        return 0;

    // 从read_pos到缓冲区末尾还有多少有效数据
    int seg1 = rb->size - rb->read_pos;
    if (seg1 >= len)
    {
        memcpy(out, rb->buf + rb->read_pos, len);
    }
    else
    {
        memcpy(out, rb->buf + rb->read_pos, seg1);
        memcpy(out + seg1, rb->buf, len - seg1);
    }
    // peek不更新read_pos
    return len;
}

int ring_buf_get_data_len(RingBuf *rb)
{
    if (rb->write_pos >= rb->read_pos)
    {
        // 写位置在后面，没有绕回
        return rb->write_pos - rb->read_pos;
    }
    else
    {
        // 写位置已经绕回跑到前面
        return rb->size - (rb->read_pos - rb->write_pos);
    }
}

int ring_buf_get_free_len(RingBuf *rb)
{
    // 预留1字节用来区分空/满，所以 -1
    return rb->size - 1 - ring_buf_get_data_len(rb);
}
