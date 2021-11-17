#ifndef __REQUIRE_HH__
#define __REQUIRE_HH__

#include <iostream>
#include <string>
#include <cstdlib>


// #define NO_ASSERTIONS
#ifdef NO_ASSERTIONS
#define require(r, s) if (r) {}
#else
inline void mabort() {
    ::abort();
}

#define require(r, s) do {                                              \
        if (! (r)) {                                                    \
            std::cerr << __FILE__ << ":" << __LINE__                    \
                      << ":" << __func__ <<"(): " << s << ": errno:"          \
                      << (errno != 0 ? ::strerror(errno) : "(errno=0)") \
                      << std::endl;                                     \
            mabort();                                                   \
        }                                                               \
    } while(0)
#endif

#define require_op(v1, v2, s, op) do {                                  \
    auto v1_ = v1;                                                      \
    auto v2_ = v2;                                                      \
        require(                                                        \
            (v1_) op (v2_),                                             \
            #v1 << " (=" << (v1_) << ")"                                \
            << " " #op " " << #v2  << " (=" << (v2_) << ")"             \
            << ": " << s);                                              \
    } while (0)

#define require_eq(v1, v2, s) require_op(v1, v2, s, ==)
#define require_neq(v1, v2, s) require_op(v1, v2, s, !=)
#define require_lt(v1, v2, s) require_op(v1, v2, s, <)
#define require_le(v1, v2, s) require_op(v1, v2, s, <=)

inline void warn(bool r, const std::string& s) {
    if (! r) std::cerr << "Warning: " << s << std::endl;
}

#endif	// __REQUIRE_HH__ //:~
