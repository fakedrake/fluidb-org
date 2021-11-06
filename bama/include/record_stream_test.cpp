#include <fmt/core.h>
#include <algorithm>

#include "file.hh"
#include "record_stream.hh"

#define TOTAL 100

int main() {
  fmt::print("start!\n");
  {
    Writer<size_t> w("/tmp/removeme.dat");
    for (int i = TOTAL; i >= 0; i--) {
      w.write(i);
    }
    w.close();
  }

  {
    RecordStream<size_t> fs("/tmp/removeme.dat");
    for (auto r : fs) {
    }

    std::sort(fs.begin(), fs.end());
    for (auto r : fs) {
    }
  }

  {
    size_t i = 0;
    eachRecord<size_t>("/tmp/removeme.dat", [&i](const size_t& s) {
      require_eq(s, i++, "The equal.");
    });
    assert(i == TOTAL + 1);
  }
  fmt::print("Success!\n");
  return 0;
}
