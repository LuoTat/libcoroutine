# libcoroutine

一个简单的有栈非对称协程

## 项目结构

提供了基于 Linux 的 ucontext_t 的有栈非对称协程的包装。

同时也对 Linux 的一些和 socket 相关的系统调用进行了 hook，使得协程在进行网络 IO 操作时可以自动切换协程。

比如`muduo`等网络库，都是基于事件驱动的网络库，使用了`epoll`等多路复用技术，但是在处理网络 IO 时，仍然是阻塞的，只不过由于用了 One Loop per Thread 的设计，分担了阻塞的影响。

而此项目则是通过协程的方式来实现网络 IO 的非阻塞，协程在进行网络 IO 操作时会自动切换到其他协程，从而提高了并发性能。再用一个线程池来统一调度协程，提高了线程的利用率。

## 主要类介绍

### Fiber

协程类，封装了 ucontext_t 相关的操作。提供了`yield`和`resume`等方法来切换协程。

### Scheduler

协程调度器，管理多个协程的调度和切换。向里面添加协程后，可以自动使用多个线程来调度协程的执行。

里面的每一个线程都在运行一个`Scheduler::scheduler`方法，这个方法会不断地从协程队列中取出协程来执行。如果当前没有协程可以执行，就会调用`idle`方法。这个方法此处只是 sleep 了一下。

同时使用了`cds::container::VyukovMPMCCycleQueue`这个无锁并发队列来作为协程的队列，保证了多线程环境下的安全性和高效性。

### TimerManager

定时器管理类，提供了定时器的添加、删除和触发等功能。

使用了`cds::container::EllenBinTreeSet`这个无锁并发红黑树来作为定时器的存储结构，保证了多线程环境下的安全性和高效性。

### EventCtx

事件的 read/write 上下文类。其实就是对应事件的回调函数

### FdCtx

文件描述符上下文类，封装了文件描述符的相关信息。主要是保存了读和写事件的 EventCtx

### IOScheduler

IO 调度器，继承自 Scheduler 和 TimerManager，主要是用来管理事件和定时器的调度。

可以把这个类看作是一个协程版的`epoll`，它内部维护了一个`epoll`实例。同时也重写了`Scheduler::idle`方法，在这个方法中每一个线程都会调用`epoll_wait`来等待事件的发生。然后将其们加入到协程队列中，等待调度。

### Hook

几乎把所有和 socket 相关的系统调用都被 hook 了，比如`socket`、`connect`、`accept`、`read`、`write`、`close`等。

同时也 hook 了`sleep`、`usleep`、`nanosleep`等睡眠函数。

原理是，比如`read`函数，当协程调用这个函数时，首先会判断当前的文件描述符是否是 socket，如果不是，则直接调用原始的`read`函数。

然后先直接调用一次`read`函数，如果返回值大于等于 0，说明读取成功，直接返回。

如果返回值小于 0，并且`errno`是`EAGAIN`，说明当前没有数据可读，这时就需要将当前协程挂起，等待数据可读的事件发生。

这时会调用`IOScheduler::add_event_as_fiber_yield`方法，将当前协程挂起，并将读事件加入到`epoll`中等待。

为什么这里要调用`IOScheduler::add_event_as_fiber_yield`方法，而不是直接调用`IOScheduler::add_event`方法再调用`Fiber::yield`。

因为普通`IOScheduler::add_event`方法只是将事件加入到`epoll`中，同时其回调函数是用户传入的，所以我如果在 A 协程中调用`IOScheduler::add_event`方法，这样一般是不会有什么问题的。

但是这里在`read`函数的时，我们 add_event 后，其回调函数是当前在执行的协程，这就有了问题，如果所以我如果在 A 协程中调用`IOScheduler::add_event`方法，然后事件触发了，这样另一个线程就会去执行 A 协程，这样就会导致 A 协程在还没有 `yield` 的时候就被另一个线程执行了，这样就会导致不可预期的错误。

所以我们需要一个`IOScheduler::add_event_as_fiber_yield`方法，这个方法和`IOScheduler::add_event`几乎一致，只是为了让别的线程在我们`epoll_ctl`添加事件后不会立刻执行当前协程，而是等当前协程`yield`后再执行。

同时由于这个函数最后是调用了`Fiber::yield`，所以一般的加锁的操作都不适合在这个函数中进行，因为这给锁的持有时间会变得不可控，可能会导致死锁。

所以我们用了 `static thread_local bool st_should_unlock`和`static thread_local std::unique_lock<std::mutex> st_unique_lock` 这两个全局变量来实现延迟加锁和解锁。

把这个锁的加锁和解锁操作延迟到了` IOScheduler::idle`方法中。

## 性能测试

测试硬件环境：

-   操作系统： Gentoo Linux 2.18
-   KDE Plasma 版本： 6.4.5
-   KDE 程序框架版本： 6.18.0
-   Qt 版本： 6.9.3
-   内核版本： 6.17.1-gentoo (64 位)
-   图形平台： Wayland
-   处理器： 16 × AMD Ryzen 9 7940HS w/ Radeon 780M Graphics
-   内存： 16 GiB 内存 (14.9 GiB 可用)
-   图形处理器： AMD Radeon 780M Graphics
-   制造商： ASUSTeK COMPUTER INC.
-   产品名称： VivoBook_ASUSLaptop M1605XA_M1605XA
-   系统版本： 1.0

使用 `ab` 工具对服务器进行性能测试，测试命令如下：

```bash
ab -c 1000 -n 1000000 http://127.0.0.1:8888/
```

测试结果如下：

```
This is ApacheBench, Version 2.3 <$Revision: 1923142 $>
Copyright 1996 Adam Twiss, Zeus Technology Ltd, http://www.zeustech.net/
Licensed to The Apache Software Foundation, http://www.apache.org/

Benchmarking 127.0.0.1 (be patient)
Completed 100000 requests
Completed 200000 requests
Completed 300000 requests
Completed 400000 requests
Completed 500000 requests
Completed 600000 requests
Completed 700000 requests
Completed 800000 requests
Completed 900000 requests
Completed 1000000 requests
Finished 1000000 requests


Server Software:
Server Hostname:        127.0.0.1
Server Port:            8080

Document Path:          /
Document Length:        13 bytes

Concurrency Level:      1024
Time taken for tests:   14.262 seconds
Complete requests:      1000000
Failed requests:        0
Total transferred:      97000000 bytes
HTML transferred:       13000000 bytes
Requests per second:    70115.72 [#/sec] (mean)
Time per request:       14.604 [ms] (mean)
Time per request:       0.014 [ms] (mean, across all concurrent requests)
Transfer rate:          6641.82 [Kbytes/sec] received

Connection Times (ms)
              min  mean[+/-sd] median   max
Connect:        0    7   2.1      6      19
Processing:     3    8   2.5      8      53
Waiting:        0    5   1.7      4      46
Total:          6   15   2.7     14      58

Percentage of the requests served within a certain time (ms)
  50%     14
  66%     15
  75%     16
  80%     16
  90%     18
  95%     19
  98%     21
  99%     23
 100%     58 (longest request)

```

使用 `wrk` 工具对服务器进行性能测试，测试命令如下：

```bash
wrk -t16 -c1024 -d30s http://127.0.0.1:8080/
```

测试结果如下：

```
Running 30s test @ http://127.0.0.1:8080/
  16 threads and 1024 connections
  Thread Stats   Avg      Stdev     Max   +/- Stdev
    Latency   159.35us    1.13ms  56.26ms   96.22%
    Req/Sec    50.66k    23.82k   75.32k    80.16%
  1905215 requests in 30.09s, 176.24MB read
Requests/sec:  63322.58
Transfer/sec:      5.86MB
```
