cmake_minimum_required(VERSION 3.16)
project(fluidb-workload LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_BUILD_TYPE Release)
find_package(fmt)

add_subdirectory(bama)
add_subdirectory(ssb-workload)
