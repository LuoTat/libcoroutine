module;
#include <cerrno>
#include <cassert>
#include <cstdarg>
#include <fcntl.h>
#include <dlfcn.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/epoll.h>
#include <sys/socket.h>

module Coroutine.Hook;

import std;
import Coroutine.Fiber;
import Coroutine.IOScheduler;
import Coroutine.TimerManager;

using namespace std::chrono_literals;

namespace ltt
{

/*
 * 这里我们只用对 socket 的 fd 进行 hook
 * 每一个 socket fd 最终都是 O_NONBLOCK 的
*/
static thread_local bool            st_hook_enable {false};
static std::shared_ptr<IOScheduler> s_hook_io_scheduler;

std::shared_ptr<IOScheduler> get_hook_io_scheduler()
{
    return s_hook_io_scheduler;
}

void set_hook_io_scheduler(std::shared_ptr<IOScheduler> hook_io_scheduler)
{
    s_hook_io_scheduler = hook_io_scheduler;
}

bool is_hook_enable()
{
    return st_hook_enable;
}

void set_hook_enable(bool flag)
{
    st_hook_enable = flag;
}

static bool is_socket(int fd)
{
    struct stat st;
    if (fstat(fd, &st) == -1)
        return false;

    return S_ISSOCK(st.st_mode);    // 宏判断是否为 socket
}

template <typename OriginFun, typename... Args>
static ssize_t socket_io(int fd, std::uint32_t event, int ms_type, OriginFun fun_origin, Args&&... args)
{
    // 如果没有启用 hook
    // 或者启用了 hook 但是 fd 不是 socket
    // 则直接调用原函数
    if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(fd))
        return fun_origin(fd, std::forward<Args>(args)...);

    // 获取超时时间
    timeval   v;
    socklen_t len {sizeof(v)};
    getsockopt(fd, SOL_SOCKET, ms_type, &v, &len);
    auto ms {std::chrono::milliseconds(v.tv_sec * 1000 + v.tv_usec / 1000)};

    // 用于条件定时器
    TimerManager::TimePoint time_point {TimerManager::TimePoint::max()};

    auto                cond {std::make_shared<int>(0)};
    std::weak_ptr<void> weak_cond {cond};
    bool                is_timeout {false};

retry:
    // 执行原始函数
    ssize_t ret {fun_origin(fd, std::forward<Args>(args)...)};

    // 如果由于信号中断导致返回-1，则需要重试
    while (ret == -1 && errno == EINTR)
    {
        ret = fun_origin(fd, std::forward<Args>(args)...);
    }

    // 如果由于资源暂时不可用导致返回 -1，则添加一个事件来等待资源可用
    if (ret == -1 && errno == EAGAIN)
    {
        // 如果设置了超时时间，则添加一个条件定时器
        if (ms != std::chrono::milliseconds::zero())
        {
            time_point = s_hook_io_scheduler->add_condition_timer(ms, false, weak_cond,
                                                                  [fd, event, &is_timeout]
                                                                  {
                                                                      is_timeout = true;
                                                                      s_hook_io_scheduler->cancel_event(fd, event);
                                                                  });
        }

        // 将 fd 和事件添加到调度器中
        if (s_hook_io_scheduler->add_event_as_fiber_yield(fd, event, false))
        {
            // 如果添加事件成功，则挂起当前协程，等待事件触发
            // 事件触发后，检查定时器是否被取消
            if (time_point != TimerManager::TimePoint::max())
                s_hook_io_scheduler->del_timer(time_point);

            if (is_timeout)
            {
                errno = ETIMEDOUT;
                return -1;
            }
            goto retry;
        }
        else
        {
            // 如果添加事件失败，则删除定时器并返回错误
            if (time_point != TimerManager::TimePoint::max())
                s_hook_io_scheduler->del_timer(time_point);

            return -1;
        }
    }
    cond.reset();    // 清理条件定时器的条件
    return ret;
}

extern "C"
{
    unsigned int sleep(unsigned int __seconds)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
            return sleep_origin(__seconds);

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(__seconds * 1000ms, false,
                                       [fiber]
                                       {
                                           fiber->resume();
                                       });
        fiber->yield();
        return 0;
    }

    int usleep(__useconds_t __useconds)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
            return usleep_origin(__useconds);

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(std::chrono::milliseconds {__useconds / 1000}, false,
                                       [fiber]
                                       {
                                           fiber->resume();
                                       });
        fiber->yield();
        return 0;
    }

    ssize_t read(int __fd, void* __buf, size_t __nbytes)
    {
        return socket_io(__fd, EPOLLIN, SO_RCVTIMEO, read_origin, __buf, __nbytes);
    }

    ssize_t write(int __fd, const void* __buf, size_t __n)
    {
        return socket_io(__fd, EPOLLOUT, SO_SNDTIMEO, write_origin, __buf, __n);
    }

    int close(int __fd)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(__fd))
            return close_origin(__fd);

        s_hook_io_scheduler->cancel_all(__fd);
        return close_origin(__fd);
    }

    int nanosleep(const struct timespec* __requested_time, struct timespec* __remaining)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
            return nanosleep_origin(__requested_time, __remaining);

        auto ms {std::chrono::milliseconds(__requested_time->tv_sec * 1000 + __requested_time->tv_nsec / 1000 / 1000)};

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(ms, false,
                                       [fiber]
                                       {
                                           fiber->resume();
                                       });
        fiber->yield();
        return 0;
    }

    int socket(int __domain, int __type, int __protocol) __THROW
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
            return socket_origin(__domain, __type, __protocol);

        return socket_origin(__domain, __type | SOCK_NONBLOCK, __protocol);
    }

    int getsockopt(int __fd, int __level, int __optname, void* __restrict __optval, socklen_t* __restrict __optlen) __THROW
    {
        return getsockopt_origin(__fd, __level, __optname, __optval, __optlen);
    }

    int setsockopt(int __fd, int __level, int __optname, const void* __optval, socklen_t __optlen) __THROW
    {
        return setsockopt_origin(__fd, __level, __optname, __optval, __optlen);
    }

    int connect(int __fd, __CONST_SOCKADDR_ARG __addr, socklen_t __len)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(__fd))
            return connect_origin(__fd, __addr, __len);

        int ret {connect_origin(__fd, __addr, __len)};
        if (ret == 0)
            return 0;
        if (ret != -1 && errno != EINPROGRESS)    // 如果不是 EINPROGRESS 错误，则直接返回错误
            return ret;

        // 用于条件定时器
        TimerManager::TimePoint time_point {TimerManager::TimePoint::max()};
        auto                    cond {std::make_shared<int>(0)};
        std::weak_ptr<void>     weak_cond {cond};
        bool                    is_timeout {false};

        // 默认连接超时时间为 1min
        time_point = s_hook_io_scheduler->add_condition_timer(1min, false, weak_cond,
                                                              [__fd, &is_timeout]
                                                              {
                                                                  is_timeout = true;
                                                                  s_hook_io_scheduler->cancel_event(__fd, EPOLLOUT);
                                                              });


        if (s_hook_io_scheduler->add_event_as_fiber_yield(__fd, EPOLLOUT, false))
        {
            if (time_point != TimerManager::TimePoint::max())
                s_hook_io_scheduler->del_timer(time_point);

            if (is_timeout)
            {
                errno = ETIMEDOUT;
                return -1;
            }
        }
        else
        {
            if (time_point != TimerManager::TimePoint::max())
                s_hook_io_scheduler->del_timer(time_point);
        }

        // 检查连接是否成功
        int       error {};
        socklen_t len {sizeof(int)};
        if (getsockopt(__fd, SOL_SOCKET, SO_ERROR, &error, &len) == -1)
            return -1;
        if (!error)
            return 0;
        else
        {
            errno = error;
            return -1;
        }
    }

    int accept(int __fd, __SOCKADDR_ARG __addr, socklen_t* __restrict __addr_len)
    {
        int fd {static_cast<int>(socket_io(__fd, EPOLLIN, SO_RCVTIMEO, accept_origin, __addr, __addr_len))};
        if (st_hook_enable && fd > 0)
            fcntl_origin(fd, F_SETFL, fcntl_origin(fd, F_GETFL) | O_NONBLOCK);
        return fd;
    }

    ssize_t send(int __fd, const void* __buf, size_t __n, int __flags)
    {
        return socket_io(__fd, EPOLLOUT, SO_SNDTIMEO, send_origin, __buf, __n, __flags);
    }

    ssize_t recv(int __fd, void* __buf, size_t __n, int __flags)
    {
        return socket_io(__fd, EPOLLIN, SO_RCVTIMEO, recv_origin, __buf, __n, __flags);
    }

    ssize_t sendto(int __fd, const void* __buf, size_t __n, int __flags, __CONST_SOCKADDR_ARG __addr, socklen_t __addr_len)
    {
        return socket_io(__fd, EPOLLOUT, SO_SNDTIMEO, sendto_origin, __buf, __n, __flags, __addr, __addr_len);
    }

    ssize_t recvfrom(int __fd, void* __restrict __buf, size_t __n, int __flags, __SOCKADDR_ARG __addr, socklen_t* __restrict __addr_len)
    {
        return socket_io(__fd, EPOLLIN, SO_RCVTIMEO, recvfrom_origin, __buf, __n, __flags, __addr, __addr_len);
    }

    ssize_t sendmsg(int __fd, const struct msghdr* __message, int __flags)
    {
        return socket_io(__fd, EPOLLOUT, SO_SNDTIMEO, sendmsg_origin, __message, __flags);
    }

    ssize_t recvmsg(int __fd, struct msghdr* __message, int __flags)
    {
        return socket_io(__fd, EPOLLIN, SO_RCVTIMEO, recvmsg_origin, __message, __flags);
    }

    ssize_t readv(int __fd, const struct iovec* __iovec, int __count)
    {
        return socket_io(__fd, EPOLLIN, SO_RCVTIMEO, readv_origin, __iovec, __count);
    }

    ssize_t writev(int __fd, const struct iovec* __iovec, int __count)
    {
        return socket_io(__fd, EPOLLOUT, SO_SNDTIMEO, writev_origin, __iovec, __count);
    }

    int fcntl(int __fd, int __cmd, ...)
    {
        va_list va;
        va_start(va, __cmd);

        switch (__cmd)
        {
            case F_SETFL :
            {
                int arg {va_arg(va, int)};
                va_end(va);

                if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(__fd))
                    return fcntl_origin(__fd, F_SETFL, arg);

                return fcntl_origin(__fd, F_SETFL, arg | O_NONBLOCK);
            }
            break;

            case F_GETFL :
            {
                va_end(va);
                return fcntl_origin(__fd, F_GETFL);
            }
            break;

            case F_DUPFD :
            case F_DUPFD_CLOEXEC :
            case F_SETFD :
            case F_SETOWN :
            case F_SETSIG :
            case F_SETLEASE :
            case F_NOTIFY :
#ifdef F_SETPIPE_SZ
            case F_SETPIPE_SZ :
#endif
            {
                int arg {va_arg(va, int)};
                va_end(va);

                return fcntl_origin(__fd, __cmd, arg);
            }
            break;


            case F_GETFD :
            case F_GETOWN :
            case F_GETSIG :
            case F_GETLEASE :
#ifdef F_GETPIPE_SZ
            case F_GETPIPE_SZ :
#endif
            {
                va_end(va);

                return fcntl_origin(__fd, __cmd);
            }
            break;

            case F_SETLK :
            case F_SETLKW :
            case F_GETLK :
            {
                flock* arg {va_arg(va, flock*)};
                va_end(va);

                return fcntl_origin(__fd, __cmd, arg);
            }
            break;

            case F_GETOWN_EX :
            case F_SETOWN_EX :
            {
                f_owner_ex* arg {va_arg(va, f_owner_ex*)};
                va_end(va);

                return fcntl_origin(__fd, __cmd, arg);
            }
            break;

            default :
                va_end(va);
                return fcntl_origin(__fd, __cmd);
        }
    }

    int ioctl(int __fd, unsigned long int __request, ...) __THROW
    {
        va_list va;
        va_start(va, __request);
        void* arg {va_arg(va, void*)};
        va_end(va);

        if (__request == FIONBIO)
        {
            int yes {1};
            return ioctl_origin(__fd, FIONBIO, &yes);
        }
        return ioctl_origin(__fd, __request, arg);
    }
}

}    // namespace ltt