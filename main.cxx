#include <cerrno>
#include <cassert>
#include <dirent.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <netinet/in.h>

#include "cds/init.h"
#include "cds/gc/hp.h"

import std;
import Coroutine;

using namespace ltt;
using namespace std::literals;

// 构建HTTP响应
const char* response = "HTTP/1.1 200 OK\r\n"
                       "Content-Type: text/plain\r\n"
                       "Content-Length: 13\r\n"
                       "Connection: close\r\n"
                       "\r\n"
                       "Hello, World!";

int main()
{
    cds::Initialize();
    cds::gc::HP hpGC {64, 32};
    cds::threading::Manager::attachThread();

    auto io_scheduler {std::make_shared<IOScheduler>(16)};
    set_hook_io_scheduler(io_scheduler);
    io_scheduler->start();

    sockaddr_in server_addr;
    server_addr.sin_family      = AF_INET;
    server_addr.sin_port        = htons(8080);
    server_addr.sin_addr.s_addr = INADDR_ANY;

    int sock_listen_fd {socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0)};
    int yes {1};
    setsockopt(sock_listen_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));

    // 绑定套接字并监听连接
    if (bind(sock_listen_fd, reinterpret_cast<sockaddr*>(&server_addr), sizeof(server_addr)) == -1)
    {
        std::println("bind error: {}", std::strerror(errno));
        return -1;
    }

    listen(sock_listen_fd, 1024);

    io_scheduler->add_event(sock_listen_fd, EPOLLIN, true,
                            [sock_listen_fd, io_scheduler]
                            {
                                int service_fd;
                                while ((service_fd = accept_origin(sock_listen_fd, nullptr, nullptr)) > 0)
                                {
                                    // std::println("accept: {}", service_fd);
                                    assert(service_fd > 0);
                                    fcntl_origin(service_fd, F_SETFL, fcntl_origin(service_fd, F_GETFL) | O_NONBLOCK);

                                    io_scheduler->add_task(
                                        [service_fd]
                                        {
                                            char buffer[1024];

                                            recv(service_fd, buffer, sizeof(buffer), 0);
                                            // std::println("recv: {}", service_fd);

                                            send(service_fd, response, std::strlen(response), 0);
                                            // std::println("send: {}", service_fd);

                                            close(service_fd);
                                            // std::println("close: {}", service_fd);
                                        });
                                }
                                if (errno == EAGAIN)
                                {
                                    // 队列中无更多连接
                                    return;
                                }
                                if (errno == EINTR)
                                {
                                    // 被信号中断，重试
                                    std::println("EINTR, fd = {}", sock_listen_fd);
                                    return;
                                }
                                std::println("accept error, fd = {}, errno = {}", sock_listen_fd, std::strerror(errno));
                            });

    io_scheduler->stop();
}