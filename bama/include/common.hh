#ifndef COMMON_H
#define COMMON_H

#include <type_traits>
#include <cstring>
#include <string>
#include <sstream>
#include <cassert>
#include <array>
#include <map>
#include <vector>
#include "book.hh"

typedef size_t page_num_t;
typedef size_t rec_num_t;
typedef size_t byte_num_t;

class Uninhabited;


#define TYPE_EQ(t1, t2) static_assert(std::is_same<t1, t2>::value)

#define ASSERT_BOTH static_assert(                                     \
    (!std::is_same<LeftRecord, Uninhabited>::value &&                  \
     !std::is_same<RightRecord, Uninhabited>::value),                  \
    "Shouldn't call both() in this class.")

#define ASSERT_LEFT static_assert(                                      \
    (!std::is_same<LeftRecord, Uninhabited>::value),                    \
    "Shouldn't call left() in this class.")
#define ASSERT_RIGHT static_assert(                                     \
    (!std::is_same<RightRecord, Uninhabited>::value),                   \
    "Shouldn't call right() in this class.")

// Breakers are our way of rendering limit
template<bool Val, typename LeftRecord, typename RightRecord>
class Always {
 public:
    bool operator() (const LeftRecord& x, const RightRecord& y) {return Val;}
    typedef LeftRecord Domain0;
    typedef RightRecord Domain1;
    typedef bool Codomain;
};

template <typename T>
struct Just {
    Just(T t) : value(t) {}
    T value;
    constexpr bool operator==(Just<T> const& j) const {
        return this->value == j.value;
    }
    static constexpr bool isNothing = false;
};

template <typename T=std::string>
struct Nothing {
    Nothing(T s) {}
    Nothing() {}
    T value;
    static constexpr bool isNothing = true;
};

template <typename T>
class Identity {
public:
    const T& operator()(const T& t) const { return t; }
    typedef T Domain0;
    typedef T Codomain;
};

template<typename T>
struct WrapRecord {
    T v;
    WrapRecord(T v_) : v(v_) {}
    std::string show() const;
    bool operator<(const WrapRecord<T>& d) const {
        return v < d.v;
    }
    bool operator==(const WrapRecord<T>& d) const {
        return v == d.v;
    }
    bool operator!=(const WrapRecord<T>& d) const {
        return v != d.v;
    }
};


template<typename T>
struct UnWrapRecord {
    typedef WrapRecord<T> Domain0;
    typedef T Codomain;
    Codomain operator()(const Domain0& x) {
        return x.v;
    }
};


template <typename A>
const std::string asString(const A& r) {
    return std::to_string(r);
}

template <typename R>
const std::string asString(const WrapRecord<R>& r) {
    return asString(r.v);
}

template <typename A, typename B>
const std::string asString(const std::pair<A, B>& r) {
    return "(" + asString(r.first) + "," + asString(r.second) + ")";
}

template<typename T>
std::string WrapRecord<T>::show() const {
    return asString(v);
}


template <typename F, typename S>
class PairCombiner {
public:
    typedef F Domain0;
    typedef S Domain1;
    typedef WrapRecord<std::pair<F, S> > Codomain;
    Codomain operator()(const F& f, const S& s) const {
        std::pair<F, S> pair(f, s);
        return pair;
    }
};

template <typename F, typename S>
class PairSecond {
public:
    typedef WrapRecord<std::pair<F, S> > Domain0;
    typedef S Codomain;
    Codomain operator()(const Domain0 p) const {
        return p.v.second;
    }
};

template <typename F, typename S>
class PairFirst {
public:
    typedef WrapRecord<std::pair<F, S> > Domain0;
    typedef F Codomain;
    Codomain operator()(const Domain0 p) const {
        return p.v.first;
    }
};




template <typename Dom, typename Cod, Cod val>
class Const {
 public:
    Cod operator()(const Dom & x) {return val;}
};

template <size_t s>
inline std::string arrToString(std::array<char, s> a) {
    std::string str(a.data(), s);
    for (char& c: str) {
        if (!c) c = ' ';
    };
    return str;
}

template<size_t n>
inline bool operator==(const std::array<char, n>& x, const std::string s) {
    return strncmp(x.data(), s.c_str(), n) == 0;
}

template<size_t n>
inline bool operator==(const std::string s, const std::array<char, n>& x) {
    return strncmp(x.data(), s.c_str(), n) == 0;
}

template<size_t n>
inline bool operator!=(const std::string s, const std::array<char, n>& x) {
    return strncmp(x.data(), s.c_str(), n) != 0;
}

template<size_t n>
inline bool operator!=(const std::array<char, n>& x, const std::string s) {
    return strncmp(x.data(), s.c_str(), n) != 0;
}

#define WITH(v, exp) do {if constexpr (!decltype(v)::isNothing) exp;} while(0)
#define WITHOUT(v, exp) do {if constexpr (decltype(v)::isNothing) exp;} while(0)

void deleteFile(const std::string& fn) {
    fs::remove(fn);
}

// #define CHECK_STRINGS
#ifdef CHECK_STRINGS
template<size_t n>
class fluidb_string : public std::array<char, n> {
 public:
    fluidb_string() : std::array<char, n>() {}
    fluidb_string(const std::array<char, n>& arr) : std::array<char, n>(arr) {check();}
    fluidb_string(const fluidb_string<n>& arr) : std::array<char, n>(arr) {check();}
 private:
    void check() {
        for (char c : *this)
            assert(isspace(c) || isalnum(c) || ispunct(c) || c == 0);
    }
};
#else
template<size_t n>
using fluidb_string=std::array<char,n>;
#endif

#endif /* COMMON_H */
