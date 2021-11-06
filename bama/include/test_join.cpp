#include <fmt/format.h>
#include <utility>

#include "common.hh"
#include "file.hh"
#include "require.hh"
#include "merge_join.hh"

struct A {
  size_t a1, a2;
  std::string show() { return fmt::format("A(a1={},a2={})", a1, a2); }
};

struct B {
  size_t b1, b2;
  std::string show() { return fmt::format("B(b1={},b2={})", b1, b2); }
};

#define A_DAT "/tmp/removeme_a.dat"
#define B_DAT "/tmp/removeme_b.dat"
#define LA_DAT (std::make_pair("/tmp/removeme_la.dat","/tmp/removeme_ila.dat"))
#define RA_DAT (std::make_pair("/tmp/removeme_la.dat","/tmp/removeme_ila.dat"))
#define O_DAT "/tmp/removeme_o.dat"

A mkA(size_t i) { return {i, i}; }
B mkB(size_t i) { return {i, i}; }

struct AB {
  size_t a1, a2, b1, b2;
};

struct ExA {
  typedef A Domain0;
  typedef size_t Codomain;
  Codomain operator()(const Domain0& r) { return r.a1; }
};

struct ExB {
  typedef B Domain0;
  typedef size_t Codomain;
  Codomain operator()(const Domain0& r) { return r.b1; }
};

struct Comb {
  typedef A Domain0;
  typedef B Domain1;
  typedef size_t Codomain;
  Codomain operator()(const Domain0& l,const Domain1& r) {
    require_eq(l.a1,r.b1, "Equal records");
    return l.a1;
  }
};

#define TOTAL 10
#define EXTRA 10

typedef std::pair<std::string,std::string> Dat;

int main() {
  {
    Writer<A> wa(A_DAT);
    Writer<B> wb(B_DAT);
    for (size_t i = 0; i < TOTAL; i++) {
      wa.write(mkA(i));
      wb.write(mkB(TOTAL - i - 1));
    }
    // Some extra records in B
    for (size_t i = TOTAL; i < TOTAL + EXTRA; i++) {
      wb.write(mkB(i));
    }
  }

  {
    auto j =
        mkEquiJoin<ExA, ExB, Comb>(Just<std::string>(O_DAT), Just<Dat>(LA_DAT),
                                   Just<Dat>(RA_DAT), A_DAT, B_DAT);
    j.run();
  }

  // Check the output
  {
    size_t i = 0;
    eachRecord<size_t>(O_DAT, [&i](size_t s) { require_eq(i++, s, "Equal"); });
    require_eq(i, TOTAL, "Total records");
    // Check the complement
    eachRecord<B>(RA_DAT.first,
                  [&i](const B& s) { require_eq(i++, s.b1, "Equal"); });
    require_eq(i, TOTAL + EXTRA, "Total records");
  }

  return 0;
}
