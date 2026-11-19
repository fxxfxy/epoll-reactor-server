test_ring:test_ring.c ringbuffer.c
# 编译测试程序
	gcc test_ring.c ringbuffer.c -o test_ring
# 运行
	./test_ring

test_thread: test_thread.c threadpool.c
	gcc test_thread.c threadpool.c -o test_thread -pthread
	./test_thread

test_log: test_log.c async_log.c
	gcc test_log.c async_log.c -o test_log -pthread
	./test_log	

# 阶段8：配套测试客户端（运行前先启动服务器，用法 ./client [basic|sticky|half|big|all]）
client: client.c
	gcc client.c -o client

main: main.c server.c ringbuffer.c threadpool.c async_log.c
	gcc main.c server.c ringbuffer.c threadpool.c async_log.c -o main -pthread
	./main	