module;
#include "cds/gc/hp.h"
#include <cassert>

module Coroutine.Scheduler;

import Coroutine.Hook;

namespace ltt
{

Scheduler::Scheduler(std::size_t threads): m_thread_size {threads}
{
    assert(m_thread_size > 0);
}

Scheduler::~Scheduler()
{
    assert(m_state == State::STOP);
}

void Scheduler::start()
{
    assert(m_thread_size);
    assert(m_state == State::STOP);

    m_state = State::RUNNING;

    m_threads.reserve(m_thread_size);
    for (size_t i = 0; i < m_thread_size; ++i)
    {
        m_threads.emplace_back(
            [self {shared_from_this()}] -> void
            {
                cds::threading::Manager::attachThread();

                Fiber::init_scheduler_fiber();
                set_hook_enable(true);
                self->scheduler();

                cds::threading::Manager::detachThread();
            }
        );
    }
}

void Scheduler::stop()
{
    assert(m_state == State::RUNNING);

    m_state = State::STOPPING;

    for (auto& thread : m_threads)
    {
        thread.join();
    }

    m_state = State::STOP;
}

void Scheduler::add_task(const std::function<void()>& func)
{
    m_tasks.emplace(std::make_shared<Fiber>(0, func));
}

void Scheduler::add_task(std::shared_ptr<Fiber> fiber)
{
    m_tasks.enqueue(std::move(fiber));
}

void Scheduler::scheduler()
{
    // 创建空闲协程
    auto idle_fiber {std::make_shared<Fiber>(
        0,
        [self {shared_from_this()}] -> void
        {
            self->idle();
        }
    )};

    bool                   has_task {};
    std::shared_ptr<Fiber> task;
    while (idle_fiber->get_state() != Fiber::State::TERM)
    {
        // 取出任务
        // 如果一个任务都没有取到
        // 则 has_task 为 false
        has_task = m_tasks.dequeue(task);

        if (has_task)
        {
            ++m_active_thread_count;
            task->resume();
            --m_active_thread_count;
        }
        else
        {
            ++m_idle_thread_count;
            idle_fiber->resume();
            --m_idle_thread_count;
        }
    }
}

void Scheduler::idle()
{
    while (m_state != State::STOPPING)
    {
        sleep_origin(1);
        Fiber::get_running_fiber()->yield();
    }
}

bool Scheduler::is_stopping()
{
    return m_state == State::STOPPING;
}

bool Scheduler::has_idle_threads() const
{
    return m_idle_thread_count > 0;
}

}    // namespace ltt
