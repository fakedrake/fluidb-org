#pragma once

#include <cassert>
#include <string>

#include <fmt/format.h>

#include "common.hh"
#include "file.hh"
#include "print_record.hh"
#include "record_map.hh"

template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFile, typename LeftAntijoin, typename RightAntijoin>
class MergeJoin {
 public:
  typedef typename LeftExtract::Domain0 Left;
  typedef typename RightExtract::Domain0 Right;
  typedef typename RightExtract::Codomain Key;
  typedef typename Combine::Codomain Out;

  static LeftExtract left_extract;
  static RightExtract right_extract;
  static Combine combine;

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
    sortFile<LeftExtract>(leftfile);
    sortFile<RightExtract>(rightfile);

    Writer<Out> output;
    Writer<Left> left_antijoin;
    Writer<Right> right_antijoin;
    WITH(outfile, output.open(outfile.value));
    WITH(left_antijoin_file,
         left_antijoin.open(left_antijoin_file.value.first));
    WITH(right_antijoin_file,
         right_antijoin.open(right_antijoin_file.value.first));

    // If left has no records all outputs are empty.
    Reader<Left> left(leftfile);
    if (!left.hasNext()) {
      if constexpr (!RightAntijoin::isNothing) {
        eachRecord<Right>(rightfile,
                          [&](const Right& r) { right_antijoin.write(r); });
      }
      return;
    }

    // If left has no records all outputs are empty.
    Reader<Right> right(rightfile);
    if (!right.hasNext()) {
      if constexpr (!LeftAntijoin::isNothing) {
        eachRecord<Left>(leftfile,
                         [&](const Left& r) { left_antijoin.write(r); });
      }
      return;
    }

    Right right_record = right.nextRecord();
    Left left_record = left.nextRecord();
    bool pending_left = true;
    bool pending_right = true;

#define UPD_RIGHT                                          \
  do {                                                     \
    assert(!pending_right);                                \
    if (right.hasNext()) {                                 \
      right_record = right.nextRecord();                   \
      pending_right = true;                                \
    } else {                                               \
      goto right_finished;                                 \
    }                                                      \
  } while (0)

#define UPD_LEFT                       \
  do {                                 \
    assert(!pending_left);             \
    if (left.hasNext()) {              \
      left_record = left.nextRecord(); \
      pending_left = true;             \
    } else {                           \
      goto left_finished;              \
    }                                  \
  } while (0)

    for (;;) {
      // Here we are in two equal blocks that we need to product
      // together.
      right.mark();
      auto domain = left_extract(left_record);
      while (domain == left_extract(left_record)) {
        right.rollback();
        while (domain == right_extract(right_record)) {
          WITH(outfile, output.write(combine(left_record, right_record)));
          pending_left = pending_right = false;
          UPD_RIGHT;
        }
        UPD_LEFT;
      }

      // Skip all the left records that are smaller
      while (left_extract(left_record) < right_extract(right_record)) {
        WITH(left_antijoin_file, left_antijoin.write(left_record));
        pending_left = false;
        UPD_LEFT;
      }

      // Skip all the right records that are smaller
      while (right_extract(right_record) < left_extract(left_record)) {
        WITH(right_antijoin_file, right_antijoin.write(right_record));
        pending_right = false;
        UPD_RIGHT;
      }
    }

  left_finished:
    if (pending_right) {
      WITH(right_antijoin_file, right_antijoin.write(right_record));
    }
    WITH(right_antijoin_file, flush_rest(right, right_antijoin));
    goto finished;

  right_finished:
    if (pending_left)
      WITH(left_antijoin_file, left_antijoin.write(left_record));
    WITH(right_antijoin_file, flush_rest(right, right_antijoin));
    goto finished;

  finished:
    WITH(left_antijoin_file, left_antijoin.close());
    WITH(right_antijoin_file, right_antijoin.close());
    WITH(outfile, output.close());

#undef UPD_LEFT
#undef UPD_RIGHT
  }

  template <typename T>
  inline void flush_rest(Reader<T>& r, Writer<T>& w) {
    while (r.hasNext()) {
      w.write(r.nextRecord());
    }
  }

 private:
  std::string leftfile;
  std::string rightfile;
  const OutFile outfile;
  const LeftAntijoin left_antijoin_file;
  const RightAntijoin right_antijoin_file;
};

template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFile, typename LeftAntijoin, typename RightAntijoin>
Combine MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin,
                  RightAntijoin>::combine;
template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFile, typename LeftAntijoin, typename RightAntijoin>
LeftExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin,
                      RightAntijoin>::left_extract;
template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFile, typename LeftAntijoin, typename RightAntijoin>
RightExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile,
                       LeftAntijoin, RightAntijoin>::right_extract;

template <typename LeftExtract, typename RightExtract, typename Combine,
          typename OutFilePathType, typename LeftTriagleFilePathType,
          typename RightTriagleFilePathType>
auto mkEquiJoin(const OutFilePathType& o, const LeftTriagleFilePathType& lt,
                const RightTriagleFilePathType& rt, const std::string& l,
                const std::string& r) {
  return MergeJoin<LeftExtract, RightExtract, Combine, OutFilePathType,
                   LeftTriagleFilePathType, RightTriagleFilePathType>(o, lt, rt,
                                                                      l, r);
}
