#ifndef EXPRESSIONS_H
#define EXPRESSIONS_H

#include <array>
#include <set>

#define ASARR(L, str) (*(std::array<char, L>*)(str))

template<size_t L, size_t N>
const std::array<char, L> & prefix(const std::array<char, N> & x) {
    static_assert(L <= N, "Requested too large prefix.");
    return ASARR(L, x.data());
}

template<size_t From, size_t To, size_t N>
const std::array<char, To - From> & subseq(const std::array<char, N> & x) {
    static_assert(From < N && To < N, "Subsequence bounds not in range.");
    return ASARR(To - From, x.data() + From);
}

template<size_t L, size_t N>
const std::array<char, L> exact_length_or_null(const std::array<char, N> & x) {
    if (L == N) return ASARR(L, x.data());
    if (L > N) return {{0}};
    // Str Length must be exactly L or return NULL string.
    std::array<char, L> nil = {{0}};
    if (!(*(x.data() + L - 1) != 0 && *(x.data() + L) == 0)) return (nil);
    return ASARR(L, x.data());
}

template<size_t L, size_t N>
const std::array<char, L> & suffix(const std::array<char, N> & x) {
    static_assert(L <= N, "Requested too large suffix.");
    return ASARR(L, x.data() + std::min(N, strlen(x.data())) - L);
}

template<size_t N>
inline bool greedy_like(const std::array<char, N>& x, const std::array<char, N>& y) {
    return greedy_like(x,std::string(y.data(),N));
}
template<size_t N>
inline bool greedy_like(const std::array<char, N>& x, const std::string& str) {
#define UPDATE_STATE do {                                           \
        on_wildcard = false;                                        \
        if (c != str.end() && *c == '%') {                          \
            on_wildcard = true;                                     \
            c++;                                                    \
            if (c != str.end() && *c == '%') on_wildcard == false;  \
        }} while(0)

    bool on_wildcard;
    auto c = str.begin();
    UPDATE_STATE;

    for (auto i : x) {
        if (!i) break;
        if (on_wildcard && c == str.end()) return true;
        if (on_wildcard && *c != i) continue;
        if (*c == i) {
            c++;
            UPDATE_STATE;
        } else {
            return false;
        }
    }
    return (c == str.end());
#undef UPDATE_STATE
}

template<typename R, bool isMinimum>
class Extreme {
 public:
    Extreme() : is_set(false) , state(0) {}
    const R operator() (const R & r) {
        if (!is_set || isMinimum ? state > r : state < r)
            state = r;
        is_set = true;
        return state;
    }
 private:
    bool is_set;
    R state;
};

template<typename R>
using AggrMin = Extreme<R, true>;
template<typename R>
using AggrMax = Extreme<R, false>;


template<typename R>
class AggrSum {
 public:
    AggrSum() : state(0) {}
    const R operator()(const R& r) {
      state += r;
      return state;
    }

   private:
    R state;
};


template<typename R>
class AggrAvg {
 public:
    AggrAvg() : state(0), count(0) {}
    R operator() (const R & r) {
        state += r;
        return state / ++count;
    }
 private:
    R state;
    unsigned count;
};


// XXX: We should be able to tell if it's too large.
template<typename R>
class AggrCount {
 public:
    AggrCount() : state() {}
    unsigned operator() (const R & r) {
        state.insert(r);
        return state.size();
    }
 private:
    std::set<R> state;
};

template<typename R>
class AggrFirst {
 public:
    AggrFirst() : is_set(false) {}
    R operator() (const R & r) {
        if (!is_set) state = r;
        is_set = true;
        return state;
    }
 private:
    bool is_set;
    R state;
};

#define EXTRACT_FN(tm_key, fnname, offset)                              \
    unsigned extract_ ## fnname (unsigned x) {                          \
        struct tm t;                                                    \
        time_t nano = x;                                                \
        gmtime_r(&nano, &t);                                            \
        return (int)(t.tm_key + offset);                                \
    }
EXTRACT_FN(tm_mon, month, 1)
EXTRACT_FN(tm_mday, day, 0)
EXTRACT_FN(tm_year, year, 1900)

#define like greedy_like

#endif /* EXPRESSIONS_H */
