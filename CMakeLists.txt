cmake_minimum_required(VERSION 4.3.2)

set(CMAKE_EXPERIMENTAL_CXX_IMPORT_STD "451f2fe2-a8a2-47c3-bc32-94786d8fc91b")

project(libcoroutine LANGUAGES CXX)

set(CMAKE_CXX_MODULE_STD 1)

# 设置 C++ 标准
set(CMAKE_CXX_STANDARD 26)
# 设置 C 标准
set(CMAKE_C_STANDARD 26)


add_library(cds SHARED IMPORTED GLOBAL)
set_target_properties(cds PROPERTIES
    IMPORTED_LOCATION "${CMAKE_SOURCE_DIR}/3rdparty/cds/lib/libcds.so"
    INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/3rdparty/cds/include"
)

add_library(coroutine SHARED)
target_compile_options(coroutine PRIVATE -Wall -Wextra -Werror)
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
target_link_libraries(coroutine PUBLIC cds)

add_executable(main)
target_compile_options(main PRIVATE -Wall -Wextra -Werror)
target_sources(main PRIVATE main.cxx)
target_link_libraries(main PRIVATE coroutine)
