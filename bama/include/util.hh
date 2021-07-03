#ifndef __UTIL_HH__
#define __UTIL_HH__

#ifdef __linux
#include <stdint.h>
#endif

#include <sys/types.h>

static __inline__ uint64_t rdtsc() {
    uint32_t lo, hi;
/* We cannot use "=A", since this would use %rax on x86_64 */
    __asm__ __volatile__ ("rdtsc" : "=a" (lo), "=d" (hi));
    return (uint64_t)hi << 32 | lo;
}

static inline void delay(size_t cycles) {
    uint64_t start = rdtsc();
    while (rdtsc() - start < cycles) {}
}

/*
static double ticks_to_seconds(double t, double tps) {
    return t / tps;
}
*/

static inline double ticks_per_second(size_t duration = 10) {
    uint64_t ticks = 0;
    uint64_t start = 0, end = 0;
    for (size_t i = 0; i < duration; i++) {
        start = rdtsc();
        sleep(1);
        end = rdtsc();
        ticks += (end-start);
    }
    return ((double) ticks) / duration;
}

#endif
