#include <fmt/format.h>
#include <utility>
#include <compare>

#include "common.hh"
#include "file.hh"
#include "require.hh"
#include "merge_join.hh"
#include "unjoin.hh"

struct A {
  size_t a1, a2;
  std::string show() const { return fmt::format("A(a1={},a2={})", a1, a2); }
  auto operator<=>(const A&) const = default;
};

struct B {
  size_t b1, b2;
  std::string show() const { return fmt::format("B(b1={},b2={})", b1, b2); }
  auto operator<=>(const B&) const = default;
};

#define A_DAT "/tmp/removeme_a.dat"
#define B_DAT "/tmp/removeme_b.dat"
#define LA_DAT "/tmp/removeme_la.dat"
#define RA_DAT "/tmp/removeme_ra.dat"
#define O_DAT "/tmp/removeme_o.dat"

A mkA(size_t i) { return {i, i}; }
B mkB(size_t i) { return {i, i}; }

struct AB {
  A a;
  B b;
  AB(const A& a, const B& b) : a(a), b(b) {}
  AB(const AB&) = default;
  AB() = default;
  std::string show() { return fmt::format("AB({},{})", a.show(), b.show()); }
};

struct KeyA {
  typedef A Domain0;
  typedef size_t Codomain;
  Codomain operator()(const Domain0& r) { return r.a1; }
};

struct KeyB {
  typedef B Domain0;
  typedef size_t Codomain;
  Codomain operator()(const Domain0& r) { return r.b1; }
};


struct ExA {
  typedef AB Domain0;
  typedef A Codomain;
  Codomain operator()(const Domain0& r) { return r.a; }
};

struct ExB {
  typedef AB Domain0;
  typedef B Codomain;
  Codomain operator()(const Domain0& r) { return r.b; }
};


struct Comb {
  typedef A Domain0;
  typedef B Domain1;
  typedef AB Codomain;
  Codomain operator()(const Domain0& l, const Domain1& r) {
    require_eq(l.a1, r.b1, "Equal records");
    return AB(l, r);
  }
};

#define TOTAL 1000
#define EXTRA 10

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
    auto j = mkEquiJoin<KeyA, KeyB, Comb>(Just<std::string>(O_DAT),
                                          Just<std::string>(LA_DAT),
                                          Just<std::string>(RA_DAT), A_DAT, B_DAT);
    j.run();
  }

  // Check the output
  {
    size_t i = 0;
    eachRecord<AB>(O_DAT, [&i](AB s) { require_eq(i++, s.a.a1, "Equal"); });
    require_eq(i, TOTAL, "Total records");
    // Check the complement
    eachRecord<B>(RA_DAT,
                  [&i](const B& s) { require_eq(i++, s.b1, "Equal"); });
    require_eq(i, TOTAL + EXTRA, "Total records");
    eachRecord<A>(LA_DAT, [](const A& s) {
      require(false, "No records from A should be left over.");
    });
  }

  // Now unjoin
  {
    // Empty ab
    Writer<B> wb(B_DAT);
    Writer<A> wa(A_DAT);
  }
  {
    // Unjoin B
    auto op = mkUnJoin<ExB>(B_DAT, O_DAT, RA_DAT);
    op.run();

    sortFile<KeyB>(B_DAT);
    size_t i = 0;
    eachRecord<B>(B_DAT, [&i](const B& b) { require_eq(i++, b.b1, "Eq"); });
    require_eq(i, TOTAL + EXTRA, "Eq");
  }
  {
    // Unjoin A
    auto op = mkUnJoin<ExA>(A_DAT, O_DAT, LA_DAT);
    op.run();

    sortFile<KeyA>(A_DAT);
    size_t i = 0;
    eachRecord<A>(A_DAT, [&i](const A& a) { require_eq(i++, a.a1, "Eq"); });
    require_eq(i, TOTAL, "Eq");
  }

  return 0;
}
