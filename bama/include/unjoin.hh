#ifndef UNJOIN_H
#define UNJOIN_H

#include <string>
#include <map>

#include "file.hh"
#include "record_map.hh"
#include "common.hh"
#include "print_record.hh"

template <typename Extract>
class UnJoin {
 private:
  // out_file :: the join input (output of unjoin)
  // join_file :: The join output (input of unjoin)
  // antijoin_file :: The antijoin
  const std::string out_file, join_file, antijoin_file;
  static Extract extract;

  typedef typename Extract::Domain0 Joined;
  typedef typename Extract::Codomain Record;

 public:
  void print_output(size_t x) {
    print_records<Record>(out_file, x);
    report();
  }

  UnJoin(const std::string& o, const std::string& jf,
         const std::string& antijoin)
      : out_file(o), join_file(jf), antijoin_file(antijoin) {}
  void run() {
    sortFile<Extract>(join_file);
    Reader<Joined> join(join_file);
    Writer<Record> output(out_file);
    eachRecord<Record>(antijoin_file,
                       [&output](const Record& r) { output.write(r); });

    if (!join.hasNext()) return;

    Record prev = extract(join.nextRecord());
    ;
    output.write(prev);
    while (join.hasNext()) {
      Record r = extract(join.nextRecord());
      if (r == prev) continue;
      prev = r;
      output.write(r);
    }

    output.close();
    join.close();
  }
};

template<typename Op1,typename Op2>
class CombOps {
  CombOps(Op1 l, Op2 r) : op1(l), op2(r) {}
  void run() {
    op1.run();
    op2.run();
  }
  Op1 op1;
  Op2 op2;
};

template<typename Extract>
Extract UnJoin<Extract>::extract;

template <typename Extract>
auto mkUnJoin(const std::string& joined, const std::string& anti_join,
              const std::string& left) {
  return UnJoin<Extract>(left, joined, anti_join);
}

template <typename ExtractL, typename ExtractR, typename J, typename JL,
          typename JR, typename IL, typename IR>
auto mkUnJoin(const J& joined, const JL& left_antijoin,
              const JR& right_antijoin, const IL& l, const IR& r) {
  if constexpr (IL::isNothing && JL::isNothing && !IR::isNothing &&
                !JR::isNothing && !J::isNothing) {
    return mkUnJoin<ExtractR>(joined.value, right_antijoin.value, r.value);
  }
  if constexpr (IR::isNothing && JR::isNothing && !IL::isNothing &&
                !JL::isNothing && !J::isNothing) {
    return mkUnJoin<ExtractL>(joined.value, left_antijoin.value, l.value);
  }
  if constexpr (!IR::isNothing && !JR::isNothing && !J::isNothing &&
                !IL::isNothing && !JL::isNothing) {
    return CombOps<UnJoin<ExtractL>, UnJoin<ExtractR>>(
      mkUnJoin<ExtractL>(l.value, joined.value, left_antijoin.value),
      mkUnJoin<ExtractR>(r.value, joined.value, right_antijoin.value));
  }
}

#endif /* UNJOIN_H */
