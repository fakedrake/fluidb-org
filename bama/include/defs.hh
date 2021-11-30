#pragma once

#include <chrono>

#include <cstring>
#include <cstdlib>
#include <sys/types.h>
#include <unistd.h>
#include <stdint.h>
#include <iostream>
#include <fstream>

#ifndef PAGESIZE
static const size_t PAGE_SIZE = 4096;
#else
static const size_t PAGE_SIZE= PAGESIZE;
#endif

static size_t reads = 0;
static size_t writes = 0;
static uint64_t write_delay = 0;
static uint64_t read_delay = 0;
static size_t cacheline_size = 128;

static inline size_t increment_reads(size_t i = 1) { reads += i; return i; }
static inline size_t increment_writes(size_t i = 1) { writes += i; return i; }
static inline void reset_reads() { reads = 0; }
static inline void reset_writes() { writes = 0; }
static inline void set_cacheline_size(size_t b) { cacheline_size = b; }
static inline void set_read_delay(uint64_t c) { read_delay = c; }
static inline void set_write_delay(uint64_t c) { write_delay = c; }
static inline void report() {
    std::cout << "Total Reads/Writes: " << reads << "/" << writes << std::endl;
}

static inline unsigned char* aligned_new(size_t size) {
    unsigned char* var;
#ifdef MEMALIGN
    if (::posix_memalign((void**) &var, 512, size)) return 0;
#else
    if (!(var = (unsigned char*) malloc(size))) return 0;
#endif
    ::memset(var, 0, size);
    //std::cout << "allocated " << (void*) var << std::endl;
    return var;
}

static inline void aligned_delete(unsigned char* p) {
    //std::cout << "deleting " << (void*) p << std::endl;
    free(p);
}

using seconds_t = std::chrono::seconds;
template <int budget>
void report_counters() {
  std::ofstream file("/tmp/io_perf.txt", std::ios_base::app);
  std::cout << "Writing perf to /tmp/io_perf.txt";
  const auto now = std::chrono::system_clock::now();
  size_t epoch =
      std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch())
          .count();
  file << "budgetx0.01:" << budget << ",reads:" << reads
       << ",writes:" << writes << std::endl;
  file.close();
}
