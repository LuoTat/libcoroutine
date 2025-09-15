module;
#include <cassert>
#include <ucontext.h>

module Coroutine.Fiber;

namespace ltt
{

static thread_local bool                   st_in_scheduler {false};
static thread_local std::shared_ptr<Fiber> st_scheduler_fiber;
static thread_local std::shared_ptr<Fiber> st_running_fiber;

void func_wapper()
{
    assert(st_in_scheduler);
    assert(st_running_fiber);
    assert(st_running_fiber != st_scheduler_fiber);
    assert(st_running_fiber->m_func);
    assert(st_running_fiber->m_state == Fiber::State::RUNNING);

    st_running_fiber->m_func();
    st_running_fiber->m_func  = nullptr;
    st_running_fiber->m_state = Fiber::State::TERM;
    st_running_fiber->yield();
}

Fiber::Fiber(std::size_t stacksize, std::function<void()> func):
    m_state {State::READY}, m_stack_size {stacksize ? stacksize : 131072}, m_func {std::move(func)}
{
    assert(m_func);

    // 分配协程栈空间
    m_stack = operator new(m_stack_size);

    getcontext(&m_ctx);
    m_ctx.uc_link          = nullptr;
    m_ctx.uc_stack.ss_sp   = m_stack;
    m_ctx.uc_stack.ss_size = m_stack_size;
    makecontext(&m_ctx, &func_wapper, 0);
}

Fiber::Fiber(Fiber&& rhs) noexcept
{
    swap(rhs);
}

Fiber::~Fiber()
{
    operator delete(m_stack);
}

Fiber& Fiber::operator=(Fiber&& rhs) noexcept
{
    swap(rhs);
    return *this;
}

void Fiber::swap(Fiber& other) noexcept
{
    std::swap(m_state, other.m_state);
    std::swap(m_ctx, other.m_ctx);
    std::swap(m_stack, other.m_stack);
    std::swap(m_stack_size, other.m_stack_size);
    std::swap(m_func, other.m_func);
}

/*
 * 只有调度线程可以 resume 一个 fiber
 * 也就是说不允许一个 fiber 里面调用另一个 fiber
 * 因为这是一个对称协程
*/
void Fiber::resume()
{
    assert(st_in_scheduler);
    assert(st_running_fiber == st_scheduler_fiber);
    assert(st_scheduler_fiber->m_state == State::RUNNING);
    assert(m_state == State::READY);

    st_scheduler_fiber->m_state = State::READY;
    m_state                     = State::RUNNING;
    st_running_fiber            = shared_from_this();
    swapcontext(&st_scheduler_fiber->m_ctx, &m_ctx);
}

void Fiber::yield()
{
    assert(st_in_scheduler);
    assert(st_running_fiber == shared_from_this());
    assert(st_scheduler_fiber->m_state == State::READY);
    assert(m_state == State::RUNNING || m_state == State::TERM);

    st_scheduler_fiber->m_state = State::RUNNING;

    // 如果当前的协程是正在运行的状态
    // 则将其转为就绪状态
    if (m_state == State::RUNNING)
        m_state = State::READY;

    st_running_fiber = st_scheduler_fiber;
    swapcontext(&m_ctx, &st_scheduler_fiber->m_ctx);
}

Fiber::State Fiber::get_state() const
{
    return m_state;
}

void Fiber::init_scheduler_fiber()
{
    st_in_scheduler = true;

    st_scheduler_fiber          = std::make_shared<Fiber>();
    st_scheduler_fiber->m_state = State::RUNNING;
    getcontext(&st_scheduler_fiber->m_ctx);
    st_running_fiber = st_scheduler_fiber;
}

std::shared_ptr<Fiber> Fiber::get_running_fiber()
{
    return st_running_fiber;
}

bool Fiber::is_in_scheduler()
{
    return st_in_scheduler;
}

}    // namespace ltt