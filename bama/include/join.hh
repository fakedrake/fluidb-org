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

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFilePathType,  // Maybe(std::string)
          typename LeftTriagleFilePathType,  // Maybe(std::string)
          typename RightTriagleFilePathType> // Maybe(std::string)
class HashJoin {
private:
  typedef typename LeftExtract::Domain0 Left;
  typedef typename RightExtract::Domain0 Right;
  typedef typename RightExtract::Codomain Key;
  typedef typename Combine::Codomain Out;

  Writer<Left> left_antijoin_writer;
  Writer<Out> out_writer;
  const OutFilePathType outfile;
  const LeftTriagleFilePathType left_antijoin_file;
  const RightTriagleFilePathType right_antijoin_file;
  const std::string leftfile;
  const std::string rightfile;
  static LeftExtract left_extract;
  static RightExtract right_extract;
  static Combine combine;
  size_t number_of_partitions;
public:
  HashJoin(const OutFilePathType& o,
           const LeftTriagleFilePathType& lt,
           const RightTriagleFilePathType& rt,
           const std::string& l,
           const std::string& r,
           size_t np = 20)
    : outfile(o),
      left_antijoin_file(lt), right_antijoin_file(rt),
      leftfile(l), rightfile(r),
      number_of_partitions(np)
  {
    TYPE_EQ(typename Combine::Domain0, Left);
    TYPE_EQ(typename Combine::Domain1, Right);
    TYPE_EQ(typename Combine::Codomain, Out);
    TYPE_EQ(typename LeftExtract::Codomain, Key);

  }

  ~HashJoin() {}

  void print_output(size_t x) {
    print_records<Left>(left_antijoin_file, x);
    print_records<Right>(right_antijoin_file, x);
    print_records<Out>(outfile, x);
    report();
  }

  void run() {
    size_t processed_partitions = 0;

    WITH(outfile, out_writer.open(outfile.value));
    WITH(left_antijoin_file,
         left_antijoin_writer.open(left_antijoin_file.value));
    while (processed_partitions < number_of_partitions) {
      if (make_pass(processed_partitions++)) {
        break;
      }
    }
    WITH(left_antijoin_file, left_antijoin_writer.close());

    // Deal with the leftovers
    auto final_left =
        generate_partition_output(leftfile, processed_partitions - 1);
    auto final_right =
        generate_partition_output(rightfile, processed_partitions - 1);

    if (number_of_partitions > 1) {
      WITHOUT(right_antijoin_file, ::remove(final_right.c_str()));
      ::remove(final_left.c_str());
    }
    WITH(right_antijoin_file,
         fs::rename(final_right, right_antijoin_file.value));
    WITH(outfile, out_writer.close());
  }

private:
  inline std::string generate_partition_input(const std::string& f,
                                              size_t p) const {
    if (p == 0) return f;
    return generate_partition_output(f, p-1);
  }

  inline std::string generate_partition_output(const std::string &f,
                                               size_t p) const {
    char ret[sizeof(size_t) * 8 / 5 + f.size() + 1];
    sprintf(ret, "%s.%zu", f.c_str(), p);
    return ret;
  }

  size_t partition_of(const Key& key) const {
    return hash_of(key) % number_of_partitions;
  }

  inline std::pair<Key, std::pair<Left, bool> > hash_entry(const Left& record) const {
    return std::pair<Key, std::pair<Left, bool> >(
                                                  left_extract(record),
                                                  std::pair<Left, bool>(record, true));
  }

  // Returns true if finished.
  bool make_pass(size_t partition) {
    const std::string left_input = generate_partition_input(
                                                            leftfile, partition);
    const std::string right_input = generate_partition_input(
                                                             rightfile, partition);
    const std::string left_output = generate_partition_output(
                                                              leftfile, partition);
    const std::string right_output = generate_partition_output(rightfile, partition);
    bool finished = true;

    Writer<Left> left_writer;
    Writer<Right> right_writer;
    using map_type = std::multimap<Key, std::pair<Left, bool> >;
    map_type map;
    std::pair<typename map_type::iterator,
              typename map_type::iterator> range;

    left_writer.open(left_output);
    right_writer.open(right_output);

    // Read all records of left and if they belong in the current
    // partition put them in the map. If not throw them in the
    // partition file.
    eachRecord<Left>(
                     left_input,
                     [&](const Left& left_record) {
                       if (partition_of(left_extract(left_record)) == partition) {
                         map.insert(hash_entry(left_record));
                       } else {
                         left_writer.write(left_record);
                       }
                     });

    // Combine each record on the right with it's corresponding
    // ones on the current partition. Each left one used.
    eachRecord<Right>(
                      right_input,
                      [&](const Right& right_record) {
                        if (partition_of(right_extract(right_record)) == partition) {
                          range = map.equal_range(right_extract(right_record));
                          for (auto it = range.first; it != range.second; it++) {
                            WITH(outfile,
                                 out_writer.write(

                                                  combine(it->second.first, right_record)));
                            // If we've seen it before mark it as
                            // duplicate on the left.
                            if (it->second.second == false)
                            it->second.second = false;
                          }
                        } else {
                          right_writer.write(right_record);
                          finished = false;
                        }
                      });

    // Write out what we know won't be touched.
    for (auto p : map)
      if (p.second.second)
        WITH(left_antijoin_file,
             left_antijoin_writer.write(p.second.first));

    left_writer.close();
    right_writer.close();

    if (partition != 0) {
      ::remove(left_input.c_str());
      ::remove(right_input.c_str());
    }
    return finished;
  }


  size_t hash_of(const Key& value) const {
    char* k = (char*) &value;
    size_t hash = 5381;
    for (size_t i = 0; i < sizeof(Key); i++)
      hash = ((hash << 5) + hash) + k[i];
    return hash;
  }
};

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
Combine HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::combine;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
LeftExtract HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::left_extract;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
RightExtract HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftAntijoin, RightAntijoin>::right_extract;

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftAntijoin,
          typename RightAntijoin>
class MergeJoin {
public:
  typedef typename LeftExtract::Domain0 Left;
  typedef typename RightExtract::Domain0 Right;
  typedef typename RightExtract::Codomain Key;
  typedef typename Combine::Codomain Out;

public:
  MergeJoin(
            const OutFile& o,
            const LeftAntijoin& lt,
            const RightAntijoin& rt,
            const std::string& l,
            const std::string& r)
    : leftfile(l), rightfile(r), outfile(o),
      left_antijoin_file(lt), right_antijoin_file(rt)
  {
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
        fmt::print("Dropping: {} (right: {})\n", left_record.show(),
                   right_record.show());
        WITH(left_antijoin_file, left_antijoin.write(left_record));
        left_record = left.nextRecord();
        outstanding_left = true;
      }

      // Skip all the right records that are smaller
      while (right_extract(right_record) < left_extract(left_record) &&
             right.hasNext()) {
        fmt::print("Dropping: {} (left: )\n", right_record.show(),
                   left_record.show());
        WITH(right_antijoin_file, right_antijoin.write(right_record));
        right_record = right.nextRecord();
        outstanding_right = true;
      }

      // Here we are in two equal blocks that we need to product
      // together.
      right.mark();
      while (left_extract(left_record) == right_extract(right_record)) {
        right.rollback();
        while (left_extract(left_record) == right_extract(right_record)) {
          fmt::print("Matched: {}, {}\n", left_record.show(),
                     right_record.show());
          WITH(outfile, output.write(combine(left_record, right_record)));
          if (right.hasNext()) {
            right_record = right.nextRecord();
            outstanding_right = true;
          } else {
            outstanding_right = false;
          }
        }
      }

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
