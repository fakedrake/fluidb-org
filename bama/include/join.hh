#ifndef JOIN_H
#define JOIN_H

#include <string>
#include <iostream>
#include <map>
#include <vector>

#include "file.hh"
#include "common.hh"
#include "print_record.hh"

// Predicates
template<bool val>
class PredicateConst {
 public:
    bool operator()(...) {return val;}
};


template <typename LeftExtract,
          typename RightExtract,
          typename Left,
          typename Right>
class EquiJoinPredicate {
    LeftExtract leftExtract;
    RightExtract rightExtract;
 public:
    bool operator()(const Left & lr, const Right & rr) {
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
        left_triangle_filename(lt),
        right_triangle_filename(rt) {
    TYPE_EQ(typename Predicate::Domain0, Left);
    TYPE_EQ(typename Predicate::Domain1, Right);
    TYPE_EQ(typename Predicate::Codomain, bool);
    TYPE_EQ(typename Combine::Domain0, Left);
    TYPE_EQ(typename Combine::Domain1, Right);
    TYPE_EQ(typename Combine::Codomain, Out);
  }

  ~Join() {}
  void print_output(size_t x) {
    print_records<Left>(left_triangle_filename, x);
    print_records<Right>(right_triangle_filename, x);
    print_records<Out>(outfile, x);
    report();
  }

  void run() {
    Writer<Left> left_triangle_writer;
    Writer<Right> right_triangle_writer;
    Writer<size_t> left_indexes_writer, right_indexes_writer;
    Writer<Out> output(outfile.value);
    bool left_touched;
    size_t left_index = 0;

    // Open triangle files
    WITH(left_triangle_filename,
         left_triangle_writer.open(left_triangle_filename.value.first));
    WITH(left_triangle_filename,
         left_indexes_writer.open(left_triangle_filename.value.second));
    WITH(right_triangle_filename,
         right_indexes_writer.open(right_triangle_filename.value.second));

    std::vector<bool> right_touched_map;
    eachRecord<Left>(leftfile, [&](const Left& left_record) {
      left_touched = false;
      size_t right_index = 0;
      eachRecord<Right>(rightfile, [&](const Right& right_record) {
        if (left_index == 0) right_touched_map.push_back(false);
        if (predicate(left_record, right_record)) {
          Out temp = combiner(left_record, right_record);
          output.write(temp);
          if (left_touched) {
            // We are re-touching, this should be
            // removed if we are unjoining.
            WITH(left_triangle_filename, left_indexes_writer.write(left_index));
          } else {
            left_touched = true;
          }

          if (right_touched_map[right_index]) {
            // We are re-touching, this should be
            // removed if we are unjoining.
            WITH(right_triangle_filename,
                 right_indexes_writer.write(right_index));
          } else {
            right_touched_map[right_index] = true;
          }
        }

        right_index++;
      });

      if constexpr (!LeftTriagleType::isNothing) {
        if (!left_touched) left_triangle_writer.write(left_record);
      }
      left_index++;
    });

    // Close triangle
    if constexpr (!LeftTriagleType::isNothing) {
      left_triangle_writer.close();
      left_indexes_writer.close();
    }

    // Write the right triangle.
    if constexpr (!RightTriagleType::isNothing) {
      right_indexes_writer.close();
      Reader<Right> right_reader;
      right_reader.open(rightfile);
      right_triangle_writer.open(right_triangle_filename.value.first);
      for (size_t i = 0; i < right_touched_map.size() && right_reader.hasNext();
           i++) {
        auto rec = right_reader.nextRecord();
        if (!right_touched_map[i]) right_triangle_writer.write(rec);
      }
      right_triangle_writer.close();
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
  LeftTriagleType left_triangle_filename;
  RightTriagleType right_triangle_filename;
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

    Writer<Left> left_triangle_writer;
    Writer<Out> out_writer;
    const OutFilePathType outfile;
    const LeftTriagleFilePathType left_triangle_file;
    const RightTriagleFilePathType right_triangle_file;
    const std::string leftfile;
    const std::string rightfile;
    Writer<size_t> dup_indexes_left, dup_indexes_right;
    static LeftExtract left_extract;
    static RightExtract right_extract;
    static Combine combine;
    size_t number_of_partitions;
    size_t out_index;
 public:
    HashJoin(const OutFilePathType& o,
             const LeftTriagleFilePathType& lt,
             const RightTriagleFilePathType& rt,
             const std::string& l,
             const std::string& r,
             size_t np = 20)
            : outfile(o),
              left_triangle_file(lt), right_triangle_file(rt),
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
        print_records<Left>(left_triangle_file, x);
        print_records<Right>(right_triangle_file, x);
        print_records<Out>(outfile, x);
        report();
    }

    void run() {
        size_t processed_partitions = 0;

        out_index = 0;

        WITH(outfile, out_writer.open(outfile.value));
        WITH(left_triangle_file,
             dup_indexes_left.open(left_triangle_file.value.second));
        WITH(left_triangle_file,
             left_triangle_writer.open(left_triangle_file.value.first));
        while (processed_partitions < number_of_partitions) {
            if (make_pass(processed_partitions++)) {break;}
        }
        WITH(left_triangle_file, left_triangle_writer.close());
        WITH(left_triangle_file, dup_indexes_left.close());

        // Deal with the leftovers
        auto final_left = generate_partition_output(
            leftfile, processed_partitions-1);
        auto final_right = generate_partition_output(
            rightfile, processed_partitions-1);

        if (number_of_partitions > 1) {
            WITHOUT(right_triangle_file, fs::remove(final_right));
            fs::remove(final_left);
        }
        WITH(right_triangle_file,
             fs::rename(final_right, right_triangle_file.value.first));
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
                            WITH(left_triangle_file,
                                 dup_indexes_left.write(out_index));
                        it->second.second = false;
                        out_index++;
                    }
                } else {
                    right_writer.write(right_record);
                    finished = false;
                }
            });

        // Write out what we know won't be touched.
        for (auto p : map)
            if (p.second.second)
                WITH(left_triangle_file,
                     left_triangle_writer.write(p.second.first));

        left_writer.close();
        right_writer.close();

        if (partition != 0) {
            fs::remove(left_input);
            fs::remove(right_input);
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
          typename LeftTriangle,
          typename RightTriangle>
Combine HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::combine;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
LeftExtract HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::left_extract;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
RightExtract HashJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::right_extract;

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
class MergeJoin {
 public:
    typedef typename LeftExtract::Domain0 Left;
    typedef typename RightExtract::Domain0 Right;
    typedef typename RightExtract::Codomain Key;
    typedef typename Combine::Codomain Out;

 public:
    MergeJoin(
        const OutFile& o,
        const LeftTriangle& lt,
        const RightTriangle& rt,
        const std::string& l,
        const std::string& r)
            : leftfile(l), rightfile(r), outfile(o),
              left_triangle_file(lt), right_triangle_file(rt)
    {
        TYPE_EQ(typename Combine::Domain0, Left);
        TYPE_EQ(typename Combine::Domain1, Right);
        TYPE_EQ(typename Combine::Codomain, Out);
        TYPE_EQ(typename LeftExtract::Codomain, Key);
    }

    ~MergeJoin() {}

    void print_output(size_t x) {
        print_records<Left>(left_triangle_file, x);
        print_records<Right>(right_triangle_file, x);
        print_records<Out>(outfile, x);
        report();
    }

    void run() {
        Reader<Left> left(leftfile);
        Reader<Right> right(rightfile);
        Writer<Out> output;
        Writer<Left> left_triangle;
        Writer<Right> right_triangle;
        Left left_record;
        Right right_record;
        bool outstanding_left = false, outstanding_right = false;

        WITH(outfile, output.open(outfile.value));
        WITH(left_triangle_file, left_triangle.open(left_triangle_file.value.first));
        WITH(right_triangle_file, right_triangle.open(right_triangle_file.value.first));

        if (left.hasNext() && right.hasNext()) {
            left_record = left.nextRecord();
            right_record = right.nextRecord();
            outstanding_left = outstanding_right = true;
        }

        while (left.hasNext() && right.hasNext()) {
            while (left_extract(left_record) < right_extract(right_record)
                   && left.hasNext()) {
                WITH(left_triangle_file, left_triangle.write(left_record));
                left_record = left.nextRecord();
                outstanding_left = true;
            }

            while (right_extract(right_record) < left_extract(left_record)
                   && right.hasNext()) {
                WITH(right_triangle_file, right_triangle.write(right_record));
                right_record = right.nextRecord();
                outstanding_right = true;
            }

            // XXX: Here we are in two equal blocks that we need to
            // product together.
            while (left_extract(left_record) == right_extract(right_record)) {
                right.mark();
                while (left_extract(left_record) == right_extract(right_record)) {
                    right.rollback();
                    while (left_extract(left_record) == right_extract(right_record)) {
                        WITH(outfile,
                             output.write(combine(left_record, right_record)));
                        if (right.hasNext()) {
                            right_record = right.nextRecord();
                            outstanding_right = true;
                        } else {outstanding_right = false;}
                    }
                }
            }

            if (outstanding_right)
                WITH(right_triangle_file, right_triangle.write(right_record));
            if (outstanding_left)
                WITH(left_triangle_file, left_triangle.write(left_record));

            WITH(right_triangle_file, flush_rest(right, right_triangle));
            WITH(left_triangle_file, flush_rest(left, left_triangle));
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
    const LeftTriangle left_triangle_file;
    const RightTriangle right_triangle_file;
    static LeftExtract left_extract;
    static RightExtract right_extract;
    static Combine combine;
};

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
Combine MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::combine;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
LeftExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::left_extract;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
RightExtract MergeJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::right_extract;

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFilePathType,
          typename LeftTriagleFilePathType,
          typename RightTriagleFilePathType>
auto mkEquiJoin0(const OutFilePathType& o,
                const LeftTriagleFilePathType& lt,
                const RightTriagleFilePathType& rt,
                const std::string& l,
                const std::string& r) {
    return HashJoin<LeftExtract,
                    RightExtract,
                    Combine,
                    OutFilePathType,
                    LeftTriagleFilePathType,
                    RightTriagleFilePathType>(o, lt, rt, l, r);
}

#endif /* JOIN_H */