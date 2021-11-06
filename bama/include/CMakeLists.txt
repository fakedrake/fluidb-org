# the usual boilerplate setup
cmake_minimum_required(VERSION 3.16)
project(bama LANGUAGES CXX)
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# tell CMake to use CTest extension
find_package(fmt)

# create an executable, which instantiates a runner from
# GoogleTest, Boost.Test, QtTest or whatever framework you use
add_executable(record_stream_test record_stream_test.cpp)
target_link_libraries(record_stream_test PUBLIC fmt::fmt-header-only)
