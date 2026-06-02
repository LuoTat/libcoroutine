module;
#include <cassert>
#include <cerrno>
#include <cstdarg>
#include <dlfcn.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>

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

namespace
{

bool is_socket(int fd)
{
    struct stat st {};
    if (fstat(fd, &st) == -1)
    {
        return false;
    }

    return S_ISSOCK(st.st_mode);    // 宏判断是否为 socket
}

template <typename OriginFun, typename... Args>
ssize_t socket_io(int fd, std::uint32_t event, int ms_type, OriginFun fun_origin, Args&&... args)
{
    // 如果没有启用 hook
    // 或者启用了 hook 但是 fd 不是 socket
    // 则直接调用原函数
    if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(fd))
    {
        return fun_origin(fd, std::forward<Args>(args)...);
    }

    // 获取超时时间
    timeval   v {};
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
            time_point = s_hook_io_scheduler->add_condition_timer(
                ms,
                false,
                weak_cond,
                [fd, event, &is_timeout]
                {
                    is_timeout = true;
                    s_hook_io_scheduler->cancel_event(fd, event);
                }
            );
        }

        // 将 fd 和事件添加到调度器中
        if (s_hook_io_scheduler->add_event_as_fiber_yield(fd, event, false))
        {
            // 如果添加事件成功，则挂起当前协程，等待事件触发
            // 事件触发后，检查定时器是否被取消
            if (time_point != TimerManager::TimePoint::max())
            {
                s_hook_io_scheduler->del_timer(time_point);
            }

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
            {
                s_hook_io_scheduler->del_timer(time_point);
            }

            return -1;
        }
    }
    cond.reset();    // 清理条件定时器的条件
    return ret;
}

}    // namespace

std::shared_ptr<IOScheduler> get_hook_io_scheduler()
{
    return s_hook_io_scheduler;
}

void set_hook_io_scheduler(std::shared_ptr<IOScheduler> hook_io_scheduler)
{
    s_hook_io_scheduler = std::move(hook_io_scheduler);
}

bool is_hook_enable()
{
    return st_hook_enable;
}

void set_hook_enable(bool flag)
{
    st_hook_enable = flag;
}

extern "C"
{
    unsigned int sleep(unsigned int seconds)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
        {
            return sleep_origin(seconds);
        }

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(
            seconds * 1000ms,
            false,
            [fiber] -> void
            {
                fiber->resume();
            }
        );
        fiber->yield();
        return 0;
    }

    int usleep(__useconds_t useconds)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
        {
            return usleep_origin(useconds);
        }

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(
            std::chrono::milliseconds {useconds / 1000},
            false,
            [fiber] -> void
            {
                fiber->resume();
            }
        );
        fiber->yield();
        return 0;
    }

    ssize_t read(int fd, void* buf, size_t nbytes)
    {
        return socket_io(fd, EPOLLIN, SO_RCVTIMEO, read_origin, buf, nbytes);
    }

    ssize_t write(int fd, const void* buf, size_t n)
    {
        return socket_io(fd, EPOLLOUT, SO_SNDTIMEO, write_origin, buf, n);
    }

    int close(int fd)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(fd))
        {
            return close_origin(fd);
        }

        s_hook_io_scheduler->cancel_all(fd);
        return close_origin(fd);
    }

    int nanosleep(const struct timespec* requested_time, struct timespec* remaining)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
        {
            return nanosleep_origin(requested_time, remaining);
        }

        auto ms {std::chrono::milliseconds(requested_time->tv_sec * 1000 + requested_time->tv_nsec / 1000 / 1000)};

        auto fiber {Fiber::get_running_fiber()};
        s_hook_io_scheduler->add_timer(
            ms,
            false,
            [fiber] -> void
            {
                fiber->resume();
            }
        );
        fiber->yield();
        return 0;
    }

    int socket(int domain, int type, int protocol) __THROW
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler())
        {
            return socket_origin(domain, type, protocol);
        }

        return socket_origin(domain, type | SOCK_NONBLOCK, protocol);
    }

    int getsockopt(int fd, int level, int optname, void* __restrict optval, socklen_t* __restrict optlen) __THROW
    {
        return getsockopt_origin(fd, level, optname, optval, optlen);
    }

    int setsockopt(int fd, int level, int optname, const void* optval, socklen_t optlen) __THROW
    {
        return setsockopt_origin(fd, level, optname, optval, optlen);
    }

    int connect(int fd, __CONST_SOCKADDR_ARG addr, socklen_t len)
    {
        if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(fd))
        {
            return connect_origin(fd, addr, len);
        }

        int ret {connect_origin(fd, addr, len)};
        if (ret == 0)
        {
            return 0;
        }
        if (ret != -1 && errno != EINPROGRESS)
        {    // 如果不是 EINPROGRESS 错误，则直接返回错误
            return ret;
        }

        // 用于条件定时器
        TimerManager::TimePoint time_point {TimerManager::TimePoint::max()};
        auto                    cond {std::make_shared<int>(0)};
        std::weak_ptr<void>     weak_cond {cond};
        bool                    is_timeout {false};

        // 默认连接超时时间为 1min
        time_point = s_hook_io_scheduler->add_condition_timer(
            1min,
            false,
            weak_cond,
            [fd, &is_timeout] -> void
            {
                is_timeout = true;
                s_hook_io_scheduler->cancel_event(fd, EPOLLOUT);
            }
        );

        if (s_hook_io_scheduler->add_event_as_fiber_yield(fd, EPOLLOUT, false))
        {
            if (time_point != TimerManager::TimePoint::max())
            {
                s_hook_io_scheduler->del_timer(time_point);
            }

            if (is_timeout)
            {
                errno = ETIMEDOUT;
                return -1;
            }
        }
        else
        {
            if (time_point != TimerManager::TimePoint::max())
            {
                s_hook_io_scheduler->del_timer(time_point);
            }
        }

        // 检查连接是否成功
        int       error {};
        socklen_t tmp_len {sizeof(int)};
        if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &tmp_len) == -1)
        {
            return -1;
        }
        if (error == 0)
        {
            return 0;
        }

        errno = error;
        return -1;
    }

    int accept(int fd, __SOCKADDR_ARG addr, socklen_t* __restrict addr_len)
    {
        int io_fd {static_cast<int>(socket_io(fd, EPOLLIN, SO_RCVTIMEO, accept_origin, addr, addr_len))};
        if (st_hook_enable && fd > 0)
        {
            fcntl_origin(io_fd, F_SETFL, fcntl_origin(io_fd, F_GETFL) | O_NONBLOCK);
        }
        return io_fd;
    }

    ssize_t send(int fd, const void* buf, size_t n, int flags)
    {
        return socket_io(fd, EPOLLOUT, SO_SNDTIMEO, send_origin, buf, n, flags);
    }

    ssize_t recv(int fd, void* buf, size_t n, int flags)
    {
        return socket_io(fd, EPOLLIN, SO_RCVTIMEO, recv_origin, buf, n, flags);
    }

    ssize_t sendto(int fd, const void* buf, size_t n, int flags, __CONST_SOCKADDR_ARG addr, socklen_t addr_len)
    {
        return socket_io(fd, EPOLLOUT, SO_SNDTIMEO, sendto_origin, buf, n, flags, addr, addr_len);
    }

    ssize_t
    recvfrom(int fd, void* __restrict buf, size_t n, int flags, __SOCKADDR_ARG addr, socklen_t* __restrict addr_len)
    {
        return socket_io(fd, EPOLLIN, SO_RCVTIMEO, recvfrom_origin, buf, n, flags, addr, addr_len);
    }

    ssize_t sendmsg(int fd, const struct msghdr* message, int flags)
    {
        return socket_io(fd, EPOLLOUT, SO_SNDTIMEO, sendmsg_origin, message, flags);
    }

    ssize_t recvmsg(int fd, struct msghdr* message, int flags)
    {
        return socket_io(fd, EPOLLIN, SO_RCVTIMEO, recvmsg_origin, message, flags);
    }

    ssize_t readv(int fd, const struct iovec* iovec, int count)
    {
        return socket_io(fd, EPOLLIN, SO_RCVTIMEO, readv_origin, iovec, count);
    }

    ssize_t writev(int fd, const struct iovec* iovec, int count)
    {
        return socket_io(fd, EPOLLOUT, SO_SNDTIMEO, writev_origin, iovec, count);
    }

    int fcntl(int fd, int cmd, ...)
    {
        va_list va;
        va_start(va, cmd);

        switch (cmd)
        {
            case F_SETFL :
            {
                int arg {va_arg(va, int)};
                va_end(va);

                if (!st_hook_enable || !Fiber::is_in_scheduler() || !is_socket(fd))
                {
                    return fcntl_origin(fd, F_SETFL, arg);
                }

                return fcntl_origin(fd, F_SETFL, arg | O_NONBLOCK);
            }
            break;

            case F_GETFL :
            {
                va_end(va);
                return fcntl_origin(fd, F_GETFL);
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

                return fcntl_origin(fd, cmd, arg);
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

                return fcntl_origin(fd, cmd);
            }
            break;

            case F_SETLK :
            case F_SETLKW :
            case F_GETLK :
            {
                flock* arg {va_arg(va, flock*)};
                va_end(va);

                return fcntl_origin(fd, cmd, arg);
            }
            break;

            case F_GETOWN_EX :
            case F_SETOWN_EX :
            {
                f_owner_ex* arg {va_arg(va, f_owner_ex*)};
                va_end(va);

                return fcntl_origin(fd, cmd, arg);
            }
            break;

            default : va_end(va); return fcntl_origin(fd, cmd);
        }
    }

    int ioctl(int fd, unsigned long int request, ...) __THROW
    {
        va_list va;
        va_start(va, request);
        void* arg {va_arg(va, void*)};
        va_end(va);

        if (request == FIONBIO)
        {
            int yes {1};
            return ioctl_origin(fd, FIONBIO, &yes);
        }
        return ioctl_origin(fd, request, arg);
    }
}

}    // namespace ltt
