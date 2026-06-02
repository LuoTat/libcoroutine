module;
#include <cassert>

module Coroutine.TimerManager;

namespace ltt
{

Timer::Timer(MS ms, TimePoint time_point, bool is_recurring, std::function<void()> func):
    m_interval {ms}, m_is_recurring {is_recurring}, m_func {std::move(func)}, m_expired_time {time_point}
{
    assert(m_interval >= MS::zero());
    assert(m_func);
}

bool Timer::operator==(const Timer& rhs) const
{
    return m_expired_time == rhs.m_expired_time;
}

std::strong_ordering Timer::operator<=>(const Timer& rhs) const
{
    return m_expired_time <=> rhs.m_expired_time;
}

TimerManager::TimePoint TimerManager::add_timer(MS ms, bool is_recurring, std::function<void()> func)
{
    assert(ms >= MS::zero());
    assert(func);
    TimePoint time_point {std::chrono::system_clock::now() + ms};
    auto      ret {m_timers.emplace(ms, time_point, is_recurring, std::move(func))};
    if (!ret)
    {
        return TimePoint::max();
    }

    return time_point;
}

TimerManager::TimePoint TimerManager::add_condition_timer(
    MS ms, bool is_recurring, const std::weak_ptr<void>& weak_cond, const std::function<void()>& func
)
{
    assert(ms >= MS::zero());
    assert(func);
    return add_timer(
        ms,
        is_recurring,
        [weak_cond, func] -> void
        {
            // 仅当 weak_cond 所指对象仍存活时才执行回调
            if (weak_cond.lock())
            {
                func();
            }
        }
    );
}

bool TimerManager::del_timer(TimePoint timer_point)
{
    return m_timers.erase(timer_point);
}

bool TimerManager::refresh_timer(TimePoint timer_point)
{
    auto gp = m_timers.extract(timer_point);
    if (!gp)
    {
        return false;
    }

    // 拿到对象，修改过期时间
    gp->m_expired_time = std::chrono::system_clock::now() + gp->m_interval;
    m_timers.insert(*gp);
    return true;
}

bool TimerManager::reset_timer(TimePoint timer_point, MS ms, bool from_now)
{
    assert(ms >= MS::zero());
    auto gp {m_timers.get(timer_point)};
    if (!gp)
    {
        return false;
    }

    if (gp->m_interval == ms && !from_now)
    {
        return true;
    }

    del_timer(timer_point);

    auto start {from_now ? std::chrono::system_clock::now() : gp->m_expired_time - gp->m_interval};
    gp->m_interval     = ms;
    gp->m_expired_time = start + gp->m_interval;
    m_timers.insert(*gp);
    return true;
}

TimerManager::MS TimerManager::get_time_until_next_expired()
{
    if (m_timers.empty())
    {
        return MS::max();
    }

    auto gp {m_timers.extract_min()};
    if (!gp)
    {
        return MS::max();
    }

    auto ms {std::chrono::duration_cast<MS>(gp->m_expired_time - std::chrono::system_clock::now())};
    m_timers.insert(*gp);
    return ms;
}

std::vector<std::function<void()>> TimerManager::get_expired_funcs()
{
    std::vector<std::function<void()>> funcs;

    auto now {std::chrono::system_clock::now()};
    bool is_rollover {detect_clock_rollover()};

    // 如果系统时间发生回退则清理所有 timer
    // 否则清理超时 timer
    for (auto gp {m_timers.extract_min()}; gp; gp = m_timers.extract_min())
    {
        if (is_rollover || gp->m_expired_time <= now)
        {
            funcs.push_back(gp->m_func);

            if (gp->m_is_recurring)
            {
                // 重新加入时间堆
                gp->m_expired_time = now + gp->m_interval;
                m_timers.insert(*gp);
            }
            else
            {
                gp->m_func = nullptr;
            }
        }
        else
        {
            m_timers.insert(*gp);
            break;
        }
    }
    return funcs;
}

bool TimerManager::empty() const
{
    return m_timers.empty();
}

bool TimerManager::detect_clock_rollover()
{
    using namespace std::chrono_literals;

    bool rollover {false};
    auto now {std::chrono::system_clock::now()};
    if (now + 1h < m_previouse_time)
    {
        rollover = true;
    }

    m_previouse_time = now;
    return rollover;
}

}    // namespace ltt
