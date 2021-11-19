#include <fmt/format.h>
#include <algorithm>
#include <vector>
#include <iostream>
#include "heap_sort.hh"
#include "require.hh"


void test(size_t num) {
  std::vector<int> a;
  for (int i = num; i >= 0; i--) {
    a.push_back(i);
  }

  hsort<32>(a.begin(),a.end());
  for (int i = 0; i < (int)num; i++) {
    require_eq(a[i], i, "test");
  }
}


int main(int argc, char *argv[])
{
  fmt::print("Start!\n");
  test(10);
  test(7000);
  // test<32>(1000);
  fmt::print("Success!\n");
  return 0;
}
