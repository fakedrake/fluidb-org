#ifndef GRACE_JOIN_H
#define GRACE_JOIN_H

#include "file.hh"
#include "common.hh"
#include "print_record.hh"

#include <string>
#include <map>


template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
class PartitionJoin {
 public:
    typedef typename LeftExtract::Domain0 Left;
    typedef typename RightExtract::Domain0 Right;
    typedef typename Combine::Codomain Out;
    typedef typename LeftExtract::Codomain Key;

    const std::string leftfile;
    const std::string rightfile;
    const OutFile outfile;
    const LeftTriangle left_triangle_file;
    const RightTriangle right_triangle_file;
    static LeftExtract left_extract;
    static RightExtract right_extract;
    static Combine combine;
    size_t number_of_partitions;

    PartitionJoin(const OutFile& o,
                  const LeftTriangle& lt,
                  const RightTriangle& rt,
                  const std::string& l,
                  const std::string& r,
                  size_t np = 20)
            : leftfile(l), rightfile(r), outfile(o),
              left_triangle_file(lt), right_triangle_file(rt),
              number_of_partitions(np)
    {
        TYPE_EQ(typename Combine::Domain0, Left);
        TYPE_EQ(typename Combine::Domain1, Right);
        TYPE_EQ(typename Combine::Codomain, Out);
        TYPE_EQ(typename LeftExtract::Codomain, Key);
    }

    ~PartitionJoin() {}

    void run() {
        if constexpr (OutFile::isNothing &&
                      RightTriangle::isNothing &&
                      LeftTriangle::isNothing)
                             return;

        Writer<Out> output;
        Writer<size_t> right_dup_index_writer, left_dup_index_writer;
        Writer<Right> right_triangle_writer;
        Writer<Left> left_triangle_writer;
        std::multimap<Key, std::pair<Left, bool> > left_map;
        size_t out_index = 0;
        std::string lt_dup_index_file;

        WITH(outfile, output.open(outfile.value));
        WITH(left_triangle_file,
             left_triangle_writer.open(left_triangle_file.value.first));
        WITH(left_triangle_file,
             left_dup_index_writer.open(left_triangle_file.value.second));
        WITH(right_triangle_file,
             right_triangle_writer.open(right_triangle_file.value.first));
        WITH(right_triangle_file,
             right_dup_index_writer.open(right_triangle_file.value.second));

        for (size_t p = 0; p < number_of_partitions; p++) {
            eachRecord<Left>(
                generate_partition_name(leftfile, p),
                [&](const Left& left_record) {
                    left_map.insert(std::pair(
                        left_extract(left_record),
                        std::pair(left_record, false)));

                });
            eachRecord<Right>(
                generate_partition_name(rightfile, p),
                [&](const Right& right_record) {
                    bool right_touched = false;
                    auto lbound = left_map.lower_bound(right_extract(right_record));
                    auto ubound = left_map.upper_bound(right_extract(right_record));
                    // Get the equal range into the output.
                    for (auto it = lbound; it != ubound; it++) {
                        // Skip the first output but then whenever we
                        // reuse the same right.
                        if (right_touched)
                            WITH(right_triangle_file,
                                 right_dup_index_writer.write(out_index));
                        right_touched = true;

                        // Write the combination to output
                        WITH(outfile,
                             output.write(
                                 combine(it->second.first, right_record)));

                        // If we saw the left before mark as.
                        if (it->second.second) {
                            WITH(left_triangle_file,
                                 left_dup_index_writer.write(out_index));
                        }
                        it->second.second = true;
                        out_index++;
                    }

                    // Put the right record in the triangle if we didn't use it.
                    if (!right_touched) {
                        WITH(right_triangle_file,
                             right_triangle_writer.write(right_record));
                    }
                });

            for (auto p : left_map) {
                if (!p.second.second) {
                    WITH(left_triangle_file,
                         left_triangle_writer.write(p.second.first));
                }
            }
            left_map.clear();
            fs::remove(generate_partition_name(leftfile, p));
            fs::remove(generate_partition_name(rightfile, p));
        }
        WITH(left_triangle_file, left_triangle_writer.close());
        WITH(left_triangle_file, left_dup_index_writer.close());
        WITH(right_triangle_file, right_dup_index_writer.close());
        WITH(right_triangle_file, right_triangle_writer.close());
    }

 private:
    std::string generate_partition_name(const std::string& p, size_t i) {
        return p + "." + std::to_string(i);
    }
};


template <typename Extract>
class Partition {
 public:
    typedef typename Extract::Domain0 Record;
    typedef typename Extract::Codomain Key;

    Partition(const std::string& fn,
              size_t np = 20)
            : filename(fn), outfile_prefix(fn),
              number_of_partitions(np)
    {}

    ~Partition() {}

    void partition() {
        std::vector<Writer<Record>* > writers;
        for (unsigned int i = 0; i < number_of_partitions; i++) {
            writers.push_back(new Writer<Record>(outfile_prefix + "."
                                                 + std::to_string(i)));
        }

        eachRecord<Record>(
            filename,
            [&](const Record& rec) {
                writers[hash_of(extract(rec)) % number_of_partitions]->write(rec);
            });

        for (auto w : writers) {
            w->flush();
            delete w;
        }
    }

 private:
    std::string filename;
    std::string outfile_prefix;
    static Extract extract;
    //Hash hasher;

    size_t number_of_partitions;

    size_t hash_of(const Key& value) {
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
          typename OutFilePathType,  // Maybe(std::string)
          typename LeftTriagleFilePathType,  // Maybe(std::string)
          typename RightTriagleFilePathType> // Maybe(std::string)
class GraceJoin {
 private:
    typedef typename LeftExtract::Domain0 Left;
    typedef typename RightExtract::Domain0 Right;
    typedef typename Combine::Codomain Out;
    typedef typename LeftExtract::Codomain Key;
    const OutFilePathType outfile;
    const LeftTriagleFilePathType left_triangle_file;
    const RightTriagleFilePathType right_triangle_file;
    const std::string leftfile;
    const std::string rightfile;
    static LeftExtract left_extract;
    static RightExtract right_extract;
    static Combine combine;
    size_t number_of_partitions;

 public:
    GraceJoin(const OutFilePathType& o,
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

    ~GraceJoin() {}

    void print_output(size_t x) {
        print_records<Left>(left_triangle_file, x);
        print_records<Right>(right_triangle_file, x);
        print_records<Out>(outfile, x);
        report();
    }

    void run() {
        Partition<LeftExtract>
                lpartitioner(leftfile, number_of_partitions);
        lpartitioner.partition();
        Partition<RightExtract>
                rpartitioner(rightfile, number_of_partitions);
        rpartitioner.partition();
        PartitionJoin<LeftExtract, RightExtract, Combine,
                      OutFilePathType,
                      LeftTriagleFilePathType,
                      RightTriagleFilePathType>
                joiner(outfile, left_triangle_file, right_triangle_file,
                       leftfile, rightfile, number_of_partitions);
        joiner.run();
    }
};

template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
Combine PartitionJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::combine;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
LeftExtract PartitionJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::left_extract;
template <typename LeftExtract,
          typename RightExtract,
          typename Combine,
          typename OutFile,
          typename LeftTriangle,
          typename RightTriangle>
RightExtract PartitionJoin<LeftExtract, RightExtract, Combine, OutFile, LeftTriangle, RightTriangle>::right_extract;

template <typename Extract>
Extract Partition<Extract>::extract;

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
    return GraceJoin<LeftExtract,
                     RightExtract,
                     Combine,
                     OutFilePathType,
                     LeftTriagleFilePathType,
                     RightTriagleFilePathType>(o, lt, rt, l, r);
}

template<typename Extract>
class UnJoin {
 private:
    const std::string out_file, join_file, triangle_file, skip_file;
    static Extract extract;

    typedef typename Extract::Domain0 Composite;
    typedef typename Extract::Codomain Record;

 public:

    void print_output(size_t x) {
        print_records<Record>(out_file, x);
        report();
    }

    UnJoin(const std::string& o,
           const std::string& jf,
           const std::string& tf,
           const std::string& indf) :
            out_file(o), join_file(jf), triangle_file(tf), skip_file(indf) {}
    void run() {
        Reader<Composite> cread(join_file);
        Writer<Record> output(out_file);
        size_t index = 0;
        eachRecord<size_t>(
            skip_file,
            [&](const size_t& skip) {
                while (cread.hasNext() && skip > index++) {
                    output.write(extract(cread.nextRecord()));
                }
            });

        // Print whatever is in cread after the last skip.
        while (cread.hasNext()) {
            output.write(extract(cread.nextRecord()));
        }


        eachRecord<Record>(
            triangle_file,
            [&](const Record& rec) {
                output.write(rec);
            });

        output.close();
        cread.close();
    }
};

template<typename Extract>
Extract UnJoin<Extract>::extract;

template<typename Extract>
auto mkUnJoin(const std::string& o,
              const std::string& jf,
              const std::string& tf,
              const std::string& indf) {
    return UnJoin<Extract>(o, jf, tf, indf);
}

#endif /* GRACE_JOIN_H */
