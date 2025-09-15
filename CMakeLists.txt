cmake_minimum_required(VERSION 4.1.1)

set(CMAKE_EXPERIMENTAL_CXX_IMPORT_STD "d0edc3af-4c50-42ea-a356-e2862fe7a444")

project(libcoroutine LANGUAGES CXX)

set(CMAKE_CXX_MODULE_STD 1)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 23)
# 设置 C 标准
set(CMAKE_C_STANDARD 23)
# 设置编译器的参数
add_compile_options(-Wall -Wextra -Werror)

add_library(coroutine STATIC)
target_sources( coroutine

                PUBLIC
                FILE_SET CXX_MODULES FILES
                coroutine.ccm
                fiber.ccm
                scheduler.ccm
                timer_manager.ccm
                io_scheduler.ccm
                hook.ccm

                PRIVATE
                fiber.cxx
                scheduler.cxx
                timer_manager.cxx
                io_scheduler.cxx
                hook.cxx
)

include_directories(${CMAKE_SOURCE_DIR}/include)
link_directories(${CMAKE_SOURCE_DIR}/lib64)

find_package(Boost REQUIRED COMPONENTS thread system)

add_executable(main)
target_sources(main PRIVATE main.cxx)
target_link_libraries(  main
                        PRIVATE
                        coroutine
                        Boost::thread
                        Boost::system
                        cds
)