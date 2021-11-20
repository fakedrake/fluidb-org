#ifndef JOIN_H
#define JOIN_H

#include <cstdio>
#include <string>
#include <iostream>
#include <map>
#include <vector>

#include <fmt/format.h>

#include "file.hh"
#include "common.hh"
#include "record_map.hh"
#include "print_record.hh"

// Predicates
template <bool val>
class PredicateConst {
public:
  bool operator()(...) { return val; }
};

template <typename LeftExtract, typename RightExtract, typename Left,
          typename Right>
class EquiJoinPredicate {
  LeftExtract leftExtract;
  RightExtract rightExtract;

public:
  bool operator()(const Left& lr, const Right& rr) {
    return leftExtract(lr) == rightExtract(rr);
  }
};

// Joins

template <typename Predicate, typename Combine,
          typename OutType,           // Maybe(std::string)
          typename LeftTriagleType,   // Maybe(std::string)
          typename RightTriagleType>  // Maybe(std::string)
class Join {
  typedef typename Predicate::Domain0 Left;
  typedef typename Predicate::Domain1 Right;
  typedef typename Combine::Codomain Out;

public:
  Join(const OutType& o, const LeftTriagleType& lt, const RightTriagleType& rt,
       const std::string& l, const std::string& r)
    : leftfile(l),
      rightfile(r),
      outfile(o),
      left_antijoin_filename(lt),
      right_antijoin_filename(rt) {
    TYPE_EQ(typename Predicate::Domain0, Left);
    TYPE_EQ(typename Predicate::Domain1, Right);
    TYPE_EQ(typename Predicate::Codomain, bool);
    TYPE_EQ(typename Combine::Domain0, Left);
    TYPE_EQ(typename Combine::Domain1, Right);
    TYPE_EQ(typename Combine::Codomain, Out);
  }

  ~Join() {}
  void print_output(size_t x) {
    print_records<Left>(left_antijoin_filename, x);
    print_records<Right>(right_antijoin_filename, x);
    print_records<Out>(outfile, x);
    report();
  }

  void run() {
    Writer<Left> left_antijoin_writer;
    Writer<Right> right_antijoin_writer;
    Writer<Out> output(outfile.value);
    bool left_touched;
    size_t left_index = 0;

    // Open antijoin files
    WITH(left_antijoin_filename,
         left_antijoin_writer.open(left_antijoin_filename.value));

    std::vector<bool> right_touched_map;
    eachRecord<Left>(leftfile, [&](const Left& left_record) {
      left_touched = false;
      eachRecord<Right>(rightfile, [&](const Right& right_record) {
        if (left_index == 0) right_touched_map.push_back(false);
        if (predicate(left_record, right_record)) {
          Out temp = combiner(left_record, right_record);
          output.write(temp);
          if (left_touched) {
            // We are re-touching, this should be
            // removed if we are unjoining.
          }
        }
      });

      if constexpr (!LeftTriagleType::isNothing) {
        if (!left_touched) left_antijoin_writer.write(left_record);
      }
    });

    // Close antijoin
    if constexpr (!LeftTriagleType::isNothing) {
      left_antijoin_writer.close();
    }

    // Write the right antijoin.
    if constexpr (!RightTriagleType::isNothing) {
      Reader<Right> right_reader;
      right_reader.open(rightfile);
      right_antijoin_writer.open(right_antijoin_filename.value);
      for (size_t i = 0; i < right_touched_map.size() && right_reader.hasNext();
           i++) {
        auto rec = right_reader.nextRecord();
        if (!right_touched_map[i]) right_antijoin_writer.write(rec);
      }
      right_antijoin_writer.close();
      right_reader.close();
    }

    output.close();
  }

private:
  std::string leftfile;
  std::string rightfile;
  OutType outfile;
  Combine combiner;
  Predicate predicate;
  LeftTriagleType left_antijoin_filename;
  RightTriagleType right_antijoin_filename;
};

template <typename Predicate, typename Combine,
          typename OutType,           // Maybe(std::string)
          typename LeftTriagleType,   // Maybe(std::string)
          typename RightTriagleType>  // Maybe(std::string)
auto mkJoin(const OutType& o, const LeftTriagleType& lt,
            const RightTriagleType& rt, const std::string& l,
            const std::string& r) {
  return Join<Predicate, Combine, OutType, LeftTriagleType, RightTriagleType>(
      o, lt, rt, l, r);
}

template <typename Combine, typename OutType>
auto mkProduct(const OutType& o,
               const std::string& l,
               const std::string& r) {
  return Join<
    Always<true, typename Combine::Domain0, typename Combine::Domain1>,
    Combine,
    OutType,
    Nothing<std::string>,
    Nothing<std::string> >(o,
                           Nothing<std::string>(),
                           Nothing<std::string>(),
                           l, r);
}

template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFile, typename LeftAntijoin, typename RightAntijoin>
class MergeJoin {
 public:
  typedef typename LeftExtract::Domain0 Left;
  typedef typename RightExtract::Domain0 Right;
  typedef typename RightExtract::Codomain Key;
  typedef typename Combine::Codomain Out;

 public:
  MergeJoin(const OutFile& o, const LeftAntijoin& lt, const RightAntijoin& rt,
            const std::string& l, const std::string& r)
      : leftfile(l),
        rightfile(r),
        outfile(o),
        left_antijoin_file(lt),
        right_antijoin_file(rt) {
    TYPE_EQ(typename Combine::Domain0, Left);
    TYPE_EQ(typename Combine::Domain1, Right);
    TYPE_EQ(typename Combine::Codomain, Out);
    TYPE_EQ(typename LeftExtract::Codomain, Key);
  }

  ~MergeJoin() {}

  void print_output(size_t x) {
    print_records<Left>(left_antijoin_file, x);
    print_records<Right>(right_antijoin_file, x);
    print_records<Out>(outfile, x);
    report();
  }

  void run() {
    fmt::print("Sorting {}\n", leftfile);
    sortFile<LeftExtract>(leftfile);
    fmt::print("Sorting {}\n", rightfile);
    sortFile<RightExtract>(rightfile);
    fmt::print("Joining... {} {}\n", leftfile, rightfile);

    Reader<Left> left(leftfile);
    Reader<Right> right(rightfile);
    Writer<Out> output;
    Writer<Left> left_antijoin;
    Writer<Right> right_antijoin;
    Left left_record;
    Right right_record;
    bool outstanding_left = false, outstanding_right = false;

    WITH(outfile, output.open(outfile.value));
    WITH(left_antijoin_file, left_antijoin.open(left_antijoin_file.value));
    WITH(right_antijoin_file, right_antijoin.open(right_antijoin_file.value));

    if (left.hasNext() && right.hasNext()) {
      left_record = left.nextRecord();
      right_record = right.nextRecord();
      outstanding_left = outstanding_right = true;
    }

    while (left.hasNext() && right.hasNext()) {
      // Skip all the left records that are smaller
      while (left_extract(left_record) < right_extract(right_record) &&
             left.hasNext()) {
        WITH(left_antijoin_file, left_antijoin.write(left_record));
        left_record = left.nextRecord();
        outstanding_left = true;
      }

      // Skip all the right records that are smaller
      while (right_extract(right_record) < left_extract(left_record) &&
             right.hasNext()) {
        WITH(right_antijoin_file, right_antijoin.write(right_record));
        right_record = right.nextRecord();
        outstanding_right = true;
      }

      // Here we are in two equal blocks that we need to product
      // together.
      auto rec = left_extract(left_record);
      right.mark();
      while (rec == left_extract(left_record)) {
        right.rollback();
        // increment the right
        while (rec == right_extract(right_record)) {
          WITH(outfile, output.write(combine(left_record, right_record)));
          if (right.hasNext()) {
            right_record = right.nextRecord();
            outstanding_right = true;
          } else {
            outstanding_right = false;
            goto i_want_out;
          }
        }

        if (left.hasNext()) {
          left.nextRecord();
        } else {
          goto i_want_out;
        }
      }

i_want_out:
      if (outstanding_right)
        WITH(right_antijoin_file, right_antijoin.write(right_record));
      if (outstanding_left)
        WITH(left_antijoin_file, left_antijoin.write(left_record));

      WITH(right_antijoin_file, flush_rest(right, right_antijoin));
      WITH(left_antijoin_file, flush_rest(left, left_antijoin));
    }
  }

  template<typename T>
  inline void flush_rest(Reader<T>& r, Writer<T>& w) {
    while (r.hasNext()) w.write(r.nextRecord());
  }

private:
  std::string leftfile;
  std::string rightfile;
  const OutFile outfile;
  const LeftAntijoin left_antijoin_file;
  const RightAntijoin right_antijoin_file;
  static LeftExtract left_extract;
  static RightExtract right_extract;
  static Combine combine;
};

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
Combine MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::combine;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
LeftExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::left_extract;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
RightExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::right_extract;

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFilePathType,
          typename LeftTriagleFilePathType,
          typename RightTriagleFilePathType>
auto mkEquiJoin(const OutFilePathType& o,
                const LeftTriagleFilePathType& lt,
                const RightTriagleFilePathType& rt,
                const std::string& l,
                const std::string& r) {
  return MergeJoin<LeftExtract,
                   RightExtract,
                   Combine,
                   OutFilePathType,
                   LeftTriagleFilePathType,
                   RightTriagleFilePathType>(o, lt, rt, l, r);
}

#endif /* JOIN_H */
