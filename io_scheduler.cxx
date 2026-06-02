module;
#include <cassert>
#include <fcntl.h>
#include <sys/epoll.h>
#include <unistd.h>

module Coroutine.IOScheduler;

import Coroutine.Hook;

namespace ltt
{

FdCtx::FdCtx(int fd): fd {fd}
{
    assert(fd > 0);
}

EventCtx& FdCtx::get_event_ctx(std::uint32_t event)
{
    assert(event == EPOLLIN || event == EPOLLOUT);
    assert(events & event);

    if (event == EPOLLIN)
    {
        return read_ctx;
    }
    return write_ctx;
}

IOScheduler::IOScheduler(std::size_t threads): Scheduler {threads}, m_epfd(epoll_create(4096))
{
    assert(m_epfd > 0);
}

IOScheduler::~IOScheduler()
{
    close_origin(m_epfd);
}

bool IOScheduler::add_event(int fd, std::uint32_t event, bool keepalive, std::function<void()> func)
{
    assert(func);
    assert(fd > 0);
    assert(event == EPOLLIN || event == EPOLLOUT);

    // 如果是第一次添加
    if (!m_fd_ctxs.contains(fd))
    {
        m_fd_ctxs.emplace(fd, std::make_unique<FdCtx>(fd));
    }

    auto gp {m_fd_ctxs.get(fd)};
    if (!gp)
    {
        return false;
    }

    // 获取对应的 fd_ctx 后
    // 将其加锁，以防止其它线程对其进行修改
    auto&                       fd_ctx {gp->second};
    std::lock_guard<std::mutex> lock {fd_ctx->mutex};

    // 如果 fd_ctx 已经有了对应的事件类型
    // 则直接返回
    if ((fd_ctx->events & event) != 0U)
    {
        return false;
    }

    // 如果 fd_ctx->event 为 0 则添加一个事件
    // 否则，修改当前的事件
    int op {(fd_ctx->events != 0U) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD};
    fd_ctx->events |= event;
    if (event == EPOLLIN)
    {
        fd_ctx->read_keepalive = keepalive;
    }
    else
    {
        fd_ctx->write_keepalive = keepalive;
    }

    epoll_event ep_event {};
    ep_event.events  = fd_ctx->events | EPOLLET;
    ep_event.data.fd = fd_ctx->fd;
    int ret {epoll_ctl(m_epfd, op, fd, &ep_event)};
    assert(!ret);

    ++m_event_count;
    auto& event_ctx {fd_ctx->get_event_ctx(event)};
    event_ctx.ctx = std::move(func);
    return true;
}

bool IOScheduler::add_event_as_fiber_yield(int fd, std::uint32_t event, bool keepalive)
{
    assert(fd > 0);
    assert(event == EPOLLIN || event == EPOLLOUT);

    // 如果是第一次添加
    if (!m_fd_ctxs.contains(fd))
    {
        m_fd_ctxs.emplace(fd, std::make_unique<FdCtx>(fd));
    }

    auto gp {m_fd_ctxs.get(fd)};
    if (!gp)
    {
        return false;
    }

    // 获取对应的 fd_ctx 后
    // 将其加锁，以防止其它线程对其进行修改
    auto&                        fd_ctx {gp->second};
    std::unique_lock<std::mutex> lock {fd_ctx->mutex};
    gp.release();    // 这里必须提前释放 gp，否则在 yield 后会出现问题

    // 如果 fd_ctx 已经有了对应的事件类型
    // 则直接返回
    if ((fd_ctx->events & event) != 0U)
    {
        return false;
    }

    // 如果 fd_ctx->event 为 0 则添加一个事件
    // 否则，修改当前的事件
    int op {(fd_ctx->events != 0U) ? EPOLL_CTL_MOD : EPOLL_CTL_ADD};
    fd_ctx->events |= event;
    if (event == EPOLLIN)
    {
        fd_ctx->read_keepalive = keepalive;
    }
    else
    {
        fd_ctx->write_keepalive = keepalive;
    }

    epoll_event ep_event {};
    ep_event.events  = fd_ctx->events | EPOLLET;
    ep_event.data.fd = fd_ctx->fd;
    int ret {epoll_ctl(m_epfd, op, fd, &ep_event)};
    assert(!ret);

    ++m_event_count;
    auto& event_ctx {fd_ctx->get_event_ctx(event)};
    event_ctx.ctx = Fiber::get_running_fiber();

    m_should_unlock = true;
    m_unique_lock   = std::move(lock);
    Fiber::get_running_fiber()->yield();
    return true;
}

bool IOScheduler::del_event(int fd, std::uint32_t event)
{
    assert(fd > 0);
    assert(event == EPOLLIN || event == EPOLLOUT);

    // 如果 fd 不存在
    // 则直接返回
    auto gp {m_fd_ctxs.get(fd)};
    if (!gp)
    {
        return false;
    }

    // 找到对应的 fd_ctx 后
    // 将其加锁，以防止其它线程对其进行修改
    auto&                       fd_ctx {gp->second};
    std::lock_guard<std::mutex> lock {fd_ctx->mutex};

    // 如果 fd_ctx 没有对应的事件类型
    // 则直接返回
    if ((fd_ctx->events & event) == 0U)
    {
        return false;
    }

    // 如果 fd_ctx->event 去掉对应的事件为 0 则直接删除事件
    // 否则，修改当前的事件
    fd_ctx->events &= ~event;
    int op {(fd_ctx->events != 0U) ? EPOLL_CTL_MOD : EPOLL_CTL_DEL};

    epoll_event ep_event {};
    ep_event.events  = fd_ctx->events | EPOLLET;
    ep_event.data.fd = fd_ctx->fd;
    int ret {epoll_ctl(m_epfd, op, fd, &ep_event)};
    assert(!ret);

    --m_event_count;

    if (op == EPOLL_CTL_DEL)
    {
        m_fd_ctxs.erase(fd);
    }

    return true;
}

bool IOScheduler::cancel_event(int fd, std::uint32_t event)
{
    assert(fd > 0);
    assert(event == EPOLLIN || event == EPOLLOUT);

    // 如果 fd 不存在
    // 则直接返回
    auto gp {m_fd_ctxs.get(fd)};
    if (!gp)
    {
        return false;
    }

    // 找到对应的 fd_ctx 后
    // 将其加锁，以防止其它线程对其进行修改
    auto&                       fd_ctx {gp->second};
    std::lock_guard<std::mutex> lock {fd_ctx->mutex};

    // 如果 fd_ctx 没有对应的事件类型
    // 则直接返回
    if ((fd_ctx->events & event) == 0U)
    {
        return false;
    }

    // 如果 fd_ctx->event 去掉对应的事件为 0 则直接删除事件
    // 否则，修改当前的事件
    fd_ctx->events &= ~event;
    int op {(fd_ctx->events != 0U) ? EPOLL_CTL_MOD : EPOLL_CTL_DEL};

    epoll_event ep_event {};
    ep_event.events  = fd_ctx->events | EPOLLET;
    ep_event.data.fd = fd_ctx->fd;
    int ret {epoll_ctl(m_epfd, op, fd, &ep_event)};
    assert(!ret);

    --m_event_count;

    run_event(fd_ctx, event);
    if (op == EPOLL_CTL_DEL)
    {
        m_fd_ctxs.erase(fd);
    }

    return true;
}

bool IOScheduler::cancel_all(int fd)
{
    assert(fd > 0);

    // 如果 fd 不存在
    // 则直接返回
    auto gp {m_fd_ctxs.get(fd)};
    if (!gp)
    {
        return false;
    }

    // 找到对应的 fd_ctx 后
    // 将其加锁，以防止其它线程对其进行修改
    auto&                       fd_ctx {gp->second};
    std::lock_guard<std::mutex> lock {fd_ctx->mutex};

    // 如果 fd_ctx 没有事件
    // 则直接返回
    if (fd_ctx->events == 0U)
    {
        return false;
    }

    fd_ctx->read_keepalive  = false;
    fd_ctx->write_keepalive = false;

    epoll_event ep_event {};
    ep_event.events  = 0;
    ep_event.data.fd = fd_ctx->fd;
    int ret {epoll_ctl(m_epfd, EPOLL_CTL_DEL, fd, &ep_event)};
    assert(!ret);

    if ((fd_ctx->events & EPOLLIN) != 0U)
    {
        run_event(fd_ctx, EPOLLIN);
        --m_event_count;
    }

    if ((fd_ctx->events & EPOLLOUT) != 0U)
    {
        run_event(fd_ctx, EPOLLOUT);
        --m_event_count;
    }

    m_fd_ctxs.erase(fd);

    return true;
}

void IOScheduler::idle()
{
    constexpr uint64_t                  MAX_EVENTS {256};
    std::array<epoll_event, MAX_EVENTS> ep_events;

    int ret {};
    while (!is_stopping())
    {
        if (m_should_unlock)
        {
            m_unique_lock.unlock();
            m_should_unlock = false;
        }

        using namespace std::chrono_literals;
        constexpr auto MAX_TIMEOUT {100ms};
        auto           ms {get_time_until_next_expired()};

        ret = epoll_wait(m_epfd, ep_events.data(), MAX_EVENTS, static_cast<int>(std::min(ms, MAX_TIMEOUT).count()));

        // 如果 ms<=MAX_TIMEOUT，说明 epoll_wait 等待结束后一定有一个定时器的任务
        // 如果 ret > 0，说明 epoll_wait 有事件发生
        if (ms <= MAX_TIMEOUT || ret > 0)
        {
            for (const auto& func : get_expired_funcs())
            {
                add_task(func);
            }

            for (int i : std::views::iota(0, ret))
            {
                epoll_event& ep_event {ep_events.at(i)};
                auto         gp {m_fd_ctxs.get(ep_event.data.fd)};
                if (!gp)
                {
                    continue;
                }

                auto&                        fd_ctx {gp->second};
                std::unique_lock<std::mutex> lock {fd_ctx->mutex, std::try_to_lock};

                // 如果获取锁失败，则直接跳过
                if (!lock.owns_lock())
                {
                    continue;
                }

                // 如果是错误或挂起事件
                if ((ep_event.events & (EPOLLERR | EPOLLHUP)) != 0U)
                {
                    // 把 fd_ctx->events 重新加入 ep_event.events
                    ep_event.events |= fd_ctx->events;
                }

                std::uint32_t old_events {fd_ctx->events};
                if ((ep_event.events & EPOLLIN) != 0U)
                {
                    run_event(fd_ctx, EPOLLIN);
                    if (!fd_ctx->read_keepalive)
                    {
                        --m_event_count;
                    }
                }
                if ((ep_event.events & EPOLLOUT) != 0U)
                {
                    run_event(fd_ctx, EPOLLOUT);
                    if (!fd_ctx->write_keepalive)
                    {
                        --m_event_count;
                    }
                }

                // 把处理了的 event 删除
                if (fd_ctx->events != old_events)
                {
                    int op {(fd_ctx->events != 0U) ? EPOLL_CTL_MOD : EPOLL_CTL_DEL};
                    ep_event.events = fd_ctx->events | EPOLLET;
                    ret             = epoll_ctl(m_epfd, op, ep_event.data.fd, &ep_event);
                    assert(!ret);
                    if (op == EPOLL_CTL_DEL)
                    {
                        m_fd_ctxs.erase(ep_event.data.fd);
                    }
                }
            }
            Fiber::get_running_fiber()->yield();
        }
    }
}

bool IOScheduler::is_stopping()
{
    return get_time_until_next_expired() == std::chrono::milliseconds::max() && m_event_count == 0 &&
           Scheduler::is_stopping();
}

bool IOScheduler::run_event(std::unique_ptr<FdCtx>& fd_ctx, std::uint32_t event)
{
    assert(event == EPOLLIN || event == EPOLLOUT);

    // 如果 fd_ctx 里面没有 event
    // 则直接返回
    if ((fd_ctx->events & event) == 0U)
    {
        return false;
    }

    // 执行 event_ctx
    auto& event_ctx {fd_ctx->get_event_ctx(event)};
    std::visit(
        [this](auto& task) -> auto
        {
            if (task)
            {
                add_task(task);
            }
        },
        event_ctx.ctx
    );

    if ((event == EPOLLIN && !fd_ctx->read_keepalive) || (event == EPOLLOUT && !fd_ctx->write_keepalive))
    {
        fd_ctx->events &= ~event;
    }

    return true;
}

}    // namespace ltt
