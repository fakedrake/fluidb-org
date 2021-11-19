cmake_minimum_required(VERSION 3.16)
project(bama)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_BUILD_TYPE RelWithDebInfo)

find_package(fmt)

add_library(bama INTERFACE
  util.hh
  unjoin.hh
  union.hh
  sort.hh
  select.hh
  run_tests.sh
  require.hh
  record_map.hh
  projection.hh
  print_record.hh
  perf.data.old
  perf.data
  page.hh
  merge_join.hh
  limit.hh
  join.hh
  io.hh
  heap_sort.hh
  grace_join.hh
  filesystem.hh
  file.hh
  expressions.hh
  defs.hh
  common.hh
  codegen.hh
  bamify.hh
  aggregation.hh
  join.hh)
target_link_libraries(bama INTERFACE )
target_link_libraries(bama INTERFACE fmt::fmt-header-only)
target_compile_features(bama INTERFACE cxx_std_20)
target_include_directories(bama INTERFACE .)

add_executable(test_record_map test_record_map.cpp)
target_link_libraries(test_record_map bama)

add_executable(test_join test_join.cpp)
target_link_libraries(test_join  bama )

add_executable(test_query test_query.cpp)
target_link_libraries(test_query bama)

add_executable(test_heap_sort test_heap_sort.cpp)
target_link_libraries(test_heap_sort  bama)
