#ifndef UNJOIN_H
#define UNJOIN_H

#include <string>
#include <map>

#include "file.hh"
#include "common.hh"
#include "print_record.hh"

template <typename Extract>
class UnJoin {
 private:
  // out_file :: the join input (output of unjoin)
  // join_file :: The join output (input of unjoin)
  // antijoin_file :: The antijoin
  const std::string out_file, join_file, antijoin_file, skip_file;
  static Extract extract;

  typedef typename Extract::Domain0 Composite;
  typedef typename Extract::Codomain Record;

 public:
  void print_output(size_t x) {
    print_records<Record>(out_file, x);
    report();
  }

  UnJoin(const std::string& o, const std::string& jf,
         // First: record, second: indexes
         std::pair<const std::string, const std::string>& antijoin)
      : out_file(o),
        join_file(jf),
        antijoin_file(antijoin.first),
        skip_file(antijoin.second) {}
  void run() {
    Reader<Composite> cread(join_file);
    Writer<Record> output(out_file);
    size_t index = 0;
    eachRecord<size_t>(skip_file, [&](const size_t& skip) {
      while (cread.hasNext() && skip > index++) {
        output.write(extract(cread.nextRecord()));
      }
    });

    // Print whatever is in cread after the last skip.
    while (cread.hasNext()) {
      output.write(extract(cread.nextRecord()));
    }

    eachRecord<Record>(antijoin_file,
                       [&](const Record& rec) { output.write(rec); });

    output.close();
    cread.close();
  }
};

template<typename Op1,typename Op2>
class Comb {
  Comb(Op1 l, Op2 r) : op1(l), op2(r) {}
  void run() {
    op1.run();
    op2.run();
  }
  Op1 op1;
  Op2 op2;
};

template<typename Extract>
Extract UnJoin<Extract>::extract;


typedef const std::pair<std::string, std::string> filepair;
template <typename Extract>
auto mkUnJoin(const std::string& left, const std::string& joined,
              const filepair& anti_join) {
  return UnJoin<Extract>(left, joined, anti_join);
}

template <typename ExtractL, typename ExtractR>
auto mkUnJoin2(const std::string& l, const std::string& r,
               const filepair& left_antijoin, const std::string& joined,
               const filepair& right_antijoin) {
  return Comb<UnJoin<ExtractL>, UnJoin<ExtractR>>(
      mkUnJoin<ExtractL>(l, joined, left_antijoin),
      mkUnJoin<ExtractR>(r, joined, right_antijoin));
}

#endif /* UNJOIN_H */
