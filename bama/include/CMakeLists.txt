cmake_minimum_required(VERSION 3.16)

set(CMAKE_BUILD_TYPE Debug)

find_package(fmt)

add_library(bama INTERFACE )
target_link_libraries(bama INTERFACE )
target_link_libraries(bama INTERFACE fmt::fmt-header-only)
target_compile_features(bama INTERFACE cxx_std_20)
target_include_directories(bama INTERFACE .)

add_executable(test_record_map test_record_map.cpp)
target_link_libraries(test_record_map bama)

add_executable(test_join test_join.cpp)
target_link_libraries(test_join  bama )
