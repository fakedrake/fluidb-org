#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <cassert>
#include <string>

#include "common.hh"
#include "file.hh"
#include "print_record.hh"
#include "record_map.hh"

struct AggrVoid {};
struct NoExtract {
  typedef AggrVoid Codomain;
};

template <typename FoldFn, typename ExtractFn>
class Aggregation {
  typedef typename FoldFn::Codomain RecordTo;
  typedef typename FoldFn::Domain0 RecordFrom;
  typedef typename ExtractFn::Codomain SubTup;
  static ExtractFn extract;
  static constexpr bool global_aggr = std::is_same<SubTup, AggrVoid>::value;
 public:
  Aggregation(const Just<const std::string>& o, const std::string& i)
      : infile(i), outfile(o.value) {}

  ~Aggregation() {}

  void print_output(size_t x) {
    print_records<RecordTo>(outfile, x);
    report();
  }

  void run() {
    Writer<RecordTo> output(outfile);
    RecordFrom record, last_record;
    bool justWrote;
    RecordTo currentRec;
    FoldFn fold;
    SubTup cur_grp_key;
    size_t count = 0;
    // XXX:  Check if it is empty.

    if constexpr (!global_aggr) {
      sortFile<ExtractFn>(infile);
    }
    eachRecord<RecordFrom>(infile, [&](RecordFrom record) {
      justWrote = false;
      if constexpr (!global_aggr) {
        SubTup tmp_grp_key = extract(record);
        if (tmp_grp_key != cur_grp_key) {
          count++;
          fold = FoldFn();
          output.write(currentRec);
          currentRec = RecordTo();
          justWrote = true;
        }
      }
      last_record = record;
      currentRec = fold(record);
    });

    // If the last record was not already written.
    if (!justWrote && count > 0) output.write(currentRec);
    output.flush();
    output.close();
  }

 private:
  std::string infile;
  std::string outfile;
};
template <typename FoldFn, typename ExtractFn>
ExtractFn Aggregation<FoldFn, ExtractFn>::extract;

template<typename FoldFn>
auto mkTotalAggregation(const Just<const std::string>& prim,
                        const Just<const std::string>& sec,
                        const std::string& i) {
    assert(sec.value == i);  // Aggregation should always be short cirquiting
    return Aggregation<FoldFn, NoExtract>(prim,i);
}

template<typename FoldFn,typename ExtractFn>
auto mkAggregation(const Just<const std::string>& prim,
                   const Just<const std::string>& sec,
                   const std::string& i) {
    assert(sec.value == i);  // Aggregation should always be short cirquiting
    return Aggregation<FoldFn, ExtractFn>(prim,i);
}
#endif /* AGGREGATION_H */
