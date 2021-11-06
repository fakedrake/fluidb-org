# the usual boilerplate setup
cmake_minimum_required(VERSION 3.16)
project(bama LANGUAGES CXX)
set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
# tell CMake to use CTest extension
find_package(fmt)

add_executable(test_record_map test_record_map.cpp)
target_link_libraries(test_record_map PUBLIC fmt::fmt-header-only)
add_executable(test_join test_join.cpp)
target_link_libraries(test_join PUBLIC fmt::fmt-header-only)
