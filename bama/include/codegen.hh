// -*- coding: utf-8; mode: c++; tab-width: 4; -*-
#ifndef CODEGEN_H
#define CODEGEN_H

#include "book.hh"
#include "require.hh"
#include "util.hh"
#include "page.hh"
#include "io.hh"
#include "defs.hh"

#include <ctime>
#include <string>
#include <set>
#include <map>
#include <array>
#include <vector>
#include <cstdio>
#include <iomanip>
#include <sstream>
#include <algorithm>
#include <type_traits>

class Uninhabited;

#define ASSERT_BOTH static_assert(!std::is_same<LeftRecord, Uninhabited>::value && \
                                  !std::is_same<RightRecord, Uninhabited>::value, \
                                  "Shouldn't call both() in this class.")
#define ASSERT_LEFT static_assert(!std::is_same<LeftRecord, Uninhabited>::value, \
                                  "Shouldn't call left() in this class.")
#define ASSERT_RIGHT static_assert(!std::is_same<RightRecord, Uninhabited>::value, \
                                   "Shouldn't call right() in this class.")

// Breakers are our way of rendering limit
template <typename LeftRecord, typename RightRecord, bool Val>
class Always {
 public:
  bool operator()(const LeftRecord& x) { return Val; }
  static bool both(const LeftRecord& lr, const RightRecord& rr) {
    ASSERT_BOTH;
    return Val;
  }
  static bool left(const LeftRecord& r) {
    ASSERT_LEFT;
    return Val;
  }
  static bool right(const RightRecord& r) {
    ASSERT_RIGHT;
    return Val;
  }
};

template<typename LeftRecord, typename RightRecord, size_t size>
class LimitBreaker {
    size_t counter = 0;
 public:
    bool both(const LeftRecord& lr, const RightRecord& rr) {
        ASSERT_BOTH;
        return ++counter >= size;
    }
    bool left(const LeftRecord& r) {
        ASSERT_LEFT;
        return ++counter >= size;
    }
    bool right(const RightRecord& r) {
        ASSERT_RIGHT;
        return ++counter >= size;
    }
};

template<typename LeftRecord, typename RightRecord, size_t size>
class DropMask {
    size_t counter = 0;
 public:
    bool both(const LeftRecord& lr, const RightRecord& rr) {
        ASSERT_BOTH;
        return ++counter > size;
    }
    bool left(const LeftRecord& r) {
        ASSERT_LEFT;
        return ++counter > size;
    }
    bool right(const RightRecord& r) {
        ASSERT_RIGHT;
        return ++counter > size;
    }
};

template <typename Predicate,
          typename Left, typename Right,
          typename Combine,
          typename Breaker=Always<Left, Right, false> >
class Join {
 public:
    typedef Left left_record_type;
    typedef Right right_record_type;
    typedef typename Combine::return_type output_record_type;

 private:
    typedef Reader<left_record_type> left_reader_type;
    typedef Reader<right_record_type> right_reader_type;
    typedef Writer<output_record_type> writer_type;

 public:
    Join(const std::string& o,
         const std::string& l,
         const std::string& r) : leftfile(l), rightfile(r), outfile(o) {}

    ~Join() {}

    void run() {
        left_reader_type left_reader;
        right_reader_type right_reader;
        writer_type output(outfile);
        left_reader.open(leftfile);
        while (left_reader.hasNext()) {
            const left_record_type& left_record = left_reader.nextRecord();

            right_reader.open(rightfile);
            while (right_reader.hasNext()) {
                const right_record_type& right_record = right_reader.nextRecord();
                if (predicate(left_record, right_record)) {
                    output_record_type temp = combiner(left_record, right_record);
                    output.write(temp);
                }

                if (breaker.both(left_record, right_record)) break;
            }
            right_reader.close();
            if (breaker.left(left_record)) break;
        }
        left_reader.close();
        output.close();
    }

 private:
    std::string leftfile;
    std::string rightfile;
    std::string outfile;
    Combine combiner;
    Breaker breaker;
    Predicate predicate;
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

template <typename LeftExtract,
          typename RightExtract,
          typename KeyRecord,
          typename Left, typename Right,
          typename Combine,
          // Use normal map for inner left join and default map for
          // outer join.
          typename MapType=std::multimap<KeyRecord, Left>,
          typename Breaker=Always<Left, Right, false> >
class EquiJoin {
 public:
    typedef Left left_record_type;
    typedef Right right_record_type;
    typedef typename Combine::return_type output_record_type;
    typedef KeyRecord key_type;

 private:
    typedef Reader<left_record_type> left_reader_type;
    typedef Reader<right_record_type> right_reader_type;
    typedef Writer<left_record_type> left_writer_type;
    typedef Writer<right_record_type> right_writer_type;
    typedef Writer<output_record_type> writer_type;
    typedef BulkProcessor<left_record_type> bulk_processor_type;

 public:
    EquiJoin(const std::string& o,
             const std::string& l,
             const std::string& r,
             size_t np = 20)
            : leftfile(l), rightfile(r), outfile(o),
              number_of_partitions(np)
    {}

    ~EquiJoin() {}

    void run() {
        writer_type writer(outfile);

        for (size_t p = 0; p < number_of_partitions; p++) {
            make_pass(writer, p);
        }

        if (number_of_partitions > 1) {
            fs::remove(generate_partition_input(leftfile,
                                                number_of_partitions));
            fs::remove(generate_partition_input(rightfile,
                                                number_of_partitions));
        }
        writer.close();
    }

 private:
    std::string leftfile;
    std::string rightfile;
    std::string outfile;
    LeftExtract left_extractor;
    RightExtract right_extractor;
    Combine combiner;
    size_t number_of_partitions;
    bool left_sub;
    bool right_sub;
    Breaker breaker;

    inline std::string generate_partition_input(const std::string& f,
                                                size_t p) const {
        if (p == 0) return f;
        return generate_partition_output(f, p-1);
    }

    inline std::string generate_partition_output(const std::string &f,
                                                 size_t p) const {
        char ret[sizeof(size_t) * 8 / 5 + f.size() + 1];
        sprintf(ret, "%s.%zu", f.c_str(), p + 1);
        return ret;
    }

    void make_pass(Writer<Out>& output, size_t partition) {
        //left_reader_type left_reader(leftfile);
        //right_reader_type right_reader(rightfile);
        left_reader_type left_reader(generate_partition_input(leftfile,
                                                              partition));
        right_reader_type right_reader(generate_partition_input(rightfile,
                                                                partition));
        left_record_type left_record;
        right_record_type right_record;
        left_writer_type left_writer;
        right_writer_type right_writer;
        size_t prt;
        MapType map;
        std::pair<typename MapType::iterator,
                  typename MapType::iterator> range;

        left_writer.open(generate_partition_output(leftfile, partition));
        right_writer.open(generate_partition_output(rightfile, partition));
        while (left_reader.hasNext()) {
            left_record = left_reader.nextRecord();
            prt = hash_of(left_extractor(left_record)) % number_of_partitions;
            if (prt == partition) {
                map.insert(std::pair<key_type, left_record_type>(
                    left_extractor(left_record), left_record));
            }
            else {
                left_writer.write(left_record);
            }
            breaker.left(left_record);
        }
        while (right_reader.hasNext()) {
            right_record = right_reader.nextRecord();
            prt = hash_of(right_extractor(right_record)) % number_of_partitions;
            if (prt == partition) {
                range = map.equal_range(right_extractor(right_record));
                for (typename MapType::iterator it = range.first;
                     it != range.second; it++) {
                    output.write(combiner(it->second, right_record));
                }
            }
            else {
                right_writer.write(right_record);
            }
            breaker.right(right_record);
            //count++;
        }
        //std::cout << "right count: " << count << std::endl;
        left_reader.close();
        right_reader.close();
        left_writer.close();
        right_writer.close();
        //substitute_file(left_sub, leftfile,
        //                generate_temp_filename(leftfile));
        //substitute_file(right_sub, rightfile,
        //                generate_temp_filename(rightfile));
        if (partition != 0) {
            fs::remove(generate_partition_input(leftfile, partition));
            fs::remove(generate_partition_input(rightfile, partition));
        }
    }

    std::string generate_temp_filename(const std::string& p) const {
        char ret[sizeof(size_t) * 8 / 5 + p.size() + 2];
        sprintf(ret, "%s.%zu", p.c_str(), p);
        return ret;
    }

    void substitute_file(bool &first_sub,
                         std::string& orig,
                         const std::string& sub) const {
        std::cout << "subbing " << orig << " with " << sub
                  << "(" << first_sub << ")" << std::endl;
        if (first_sub) {
            orig.clear();
            orig.append(sub);
            first_sub = false;
        }
        else {
            fs::rename(sub, orig);
        }
    }

    size_t hash_of(const key_type& value) {
        char* k = (char*) &value;
        size_t hash = 5381;
        for (size_t i = 0; i < sizeof(key_type); i++)
            hash = ((hash << 5) + hash) + k[i];
        return hash;
    }
};

class PredicateTrue {
 public:
    bool operator()(...) {return true;}
};

template<typename FromRec>
class NeverChange {
 public:
    bool operator()(const FromRec & rec) {
        return false;
    }
};

template<typename Fn, typename FromRec, typename ToRec>
class OnChange {
 public:
    bool operator()(const FromRec & rec) {
        ToRec newRec = f(rec);
        if (first_run) {
            first_run = false;
            lastRec = newRec;
            return false;
        }

        if (newRec == lastRec) return false;
        lastRec = newRec;
        require(lastRec == newRec, "Equality operator is problematic");
        return true;
    }
 private:
    static Fn f;
    ToRec lastRec;
    bool first_run = true;
};

template<typename Fn, typename FromRec, typename ToRec>
Fn OnChange<Fn, FromRec, ToRec>::f;

template<typename FoldFn,
         typename RecordFrom,
         typename RecordTo,
         typename StartNewRecord,
         typename Breaker=Always<RecordFrom, Uninhabited, false> >
class Aggregation {
 public:
    typedef RecordFrom record_type;
    typedef RecordTo output_record_type;

 private:
    typedef Reader<record_type> reader_type;
    typedef Writer<output_record_type> writer_type;

 public:
    Aggregation (const std::string& o,
                 const std::string& f) : infile(f), outfile(o) {}

    ~Aggregation () {}

    void run() {
        reader_type reader;
        record_type record, last_record;
        writer_type output(outfile);
        bool justWrote;

        reader.open(infile);
        while (reader.hasNext()) {
            justWrote = false;
            record = reader.nextRecord();
            if (startNew(record)) {
                fold = FoldFn();
                output.write(currentRec);
                currentRec = RecordTo();
                justWrote = true;
            }
            last_record = record;
            currentRec = fold(record);
            if (breaker.left(record)) break;
        }

        if (!justWrote) output.write(currentRec);
        reader.close();
        output.flush();
        output.close();
    }

 private:
    std::string infile;
    std::string outfile;
    StartNewRecord startNew;
    RecordTo currentRec;
    FoldFn fold;
    Breaker breaker;
};


template<typename Predicate,
         typename RecordFrom,
         typename Project=Identity<RecordFrom>,
         typename RecordTo=RecordFrom,
         typename Breaker=Always<RecordFrom, Uninhabited, false>,
         typename Mask=Always<RecordFrom, Uninhabited, true> >
class Select {
 public:
    typedef RecordFrom record_type;

 private:
    typedef Reader<RecordFrom> reader_type;
    typedef Writer<RecordTo> writer_type;

 public:
    Select (const std::string& o,
            const std::string& f) : infiles(1, f), outfile(o) {}

    Select (const std::string& o,
            const std::string& f1,
            const std::string& f2) : infiles({f1, f2}), outfile(o) {}

    ~Select () {}

    void run() {
        reader_type reader;
        record_type record;
        writer_type output(outfile);

        for (auto infile : infiles) {
            reader.open(infile);
            while (reader.hasNext()) {
                record = reader.nextRecord();
                if (predicate(record) && mask.left(record)) {
                    RecordTo tmp = project(record);
                    output.write(tmp);
                }
                if (breaker.left(record)) break;
            }
            reader.close();
        }

        output.close();
    }

 private:
    std::vector<std::string> infiles;
    std::string outfile;
    static Project project;
    static Predicate predicate;
    static Breaker breaker;
    static Mask mask;
};

#define SELECT_DECL(type, var) template<typename Predicate,             \
                                        typename RecordFrom,            \
                                        typename Project,               \
                                        typename RecordTo,              \
                                        typename Breaker,               \
                                        typename Mask>                  \
    type Select<Predicate, RecordFrom, Project, RecordTo, Breaker, Mask>::var

SELECT_DECL(Project, project);
SELECT_DECL(Breaker, breaker);
SELECT_DECL(Predicate, predicate);
SELECT_DECL(Mask, mask);


template<typename ProjectFn,
         typename RecordFrom,
         typename RecordTo,
         typename Breaker=Always<RecordFrom, Uninhabited, false>,
         typename Mask=Always<RecordFrom, Uninhabited, true>>
using Project = Select<Const<RecordFrom, bool, true>,
                       RecordFrom,
                       ProjectFn,
                       RecordTo,
                       Breaker,
                       Mask>;

template<typename Record, size_t lim >
using Limit = Select<Const<Record, bool, true>,
                     Record,
                     Identity<Record>,
                     Record,
                     LimitBreaker<Record, Uninhabited, lim>,
                     Always<Record, Uninhabited, true> >;

template<typename Record, size_t lim >
using Drop = Select<Const<Record, bool, true>,
                    Record,
                    Identity<Record>,
                    Record,
                    Always<Record, Uninhabited, false>,
                    DropMask<Record, Uninhabited, lim> >;

template<typename Record, typename Breaker=Always<Record, Uninhabited, false> >
using Union = Select<Const<Record, bool, true>,
                     Record,
                     Identity<Record>,
                     Record,
                     Breaker>;


// WORK IN PROGRESS
// Buffer = n * page => the intermediate result size.
template <typename ToKeyFn,
          typename Record,
          typename KeyRecord,
          size_t BUFFER_SIZE=100>
class MergeSort {
 public:
    typedef Record record_type;
    typedef KeyRecord key_type;

 private:
    typedef Reader<Record> reader_type;
    typedef Writer<Record> writer_type;
    typedef BulkProcessor<Record> bulk_processor_type;
    static inline bool heap_comp(const std::pair<record_type, unsigned int>& x,
                                 const std::pair<record_type, unsigned int>& y) {
        return sort_comp(y.first, x.first);
        // return to_key(y.first) < to_key(x.first);
    }

    static ToKeyFn to_key;
    static inline bool sort_comp(const record_type& x, const record_type& y) {
        // Heap specific: we want the heap to be ascending by.
        return to_key(x) < to_key(y);
    }

 public:
    MergeSort(const std::string& out,
              const std::string& fn,
              size_t nb=5)
            : filename(fn), temp_prefix(out), outfile(out),
              number_of_runs(0), processed_so_far(0),
              number_of_buffers(nb) {
    }
    ~MergeSort() {}

    void run() {
        create_runs_bulk();
        //dump_runs();
        merge_runs();
        remove_runs();
        //dump();
    }

 private:

    void create_runs_bulk() {
        bulk_processor_type bulk_reader(filename);
        bulk_processor_type bulk_writer;
        number_of_runs = 0;

        size_t np = bulk_reader.numPages();
        size_t pages_so_far = 0;
        size_t buffers = number_of_buffers;
        size_t num_recs;
        record_type* recs = (record_type*) aligned_new(number_of_buffers * BUFFER_SIZE);
        require(recs, "Allocation failed.");
        while (pages_so_far < np) {
            bulk_reader.bulkRead(pages_so_far, recs, buffers, num_recs);
            std::sort(recs, recs + num_recs, sort_comp);
            // std::cout << "Recs: ";
            // for (int j = 0; j < 10; j++) {
            //     auto k = to_key(recs[j]); std::cout << *((int *)(&k)) << " ";
            // }
            // std::cout << std::endl;
            bulk_writer.open(generate_run());
            bulk_writer.bulkWrite(0, recs, num_recs);
            bulk_writer.close();
            pages_so_far += number_of_buffers;
        }
        aligned_delete((unsigned char*) recs);
        bulk_reader.close();
    }

    void create_runs() {
        reader_type reader(filename);
        writer_type writer;
        number_of_runs = 0;

        unsigned int count = 0;
        // we need -2 from the number of buffer pool pages since we have to
        // account for the two pages of the reader and writer
        const unsigned int limit =
                ((number_of_buffers-2) * BUFFER_SIZE) / sizeof(record_type);
        record_type records[limit];
        unsigned int idx = 0;
        while (reader.hasNext()) {
            // load up the buffer
            records[idx++] = reader.nextRecord();
            if (idx == limit) {
                // sort the contents of the buffer
                std::sort(records, records+limit, sort_comp);
                // open the writer and dump the contents
                writer.open(generate_run());
                //writer_type writer(generate_run());
                for (unsigned int i = 0; i < limit; i++) {
                    writer.write(records[i]);
                    count++;
                }
                // then close the writer
                writer.close();
                idx = 0;
            }
        }

        // last run is always smaller, so pick up the spares and write them
        // in a separate run
        std::sort(records, records+idx, sort_comp);
        writer.open(generate_run());
        for (unsigned int i = 0; i < idx; i++) {
            writer.write(records[i]);
            count++;
        }
        writer.close();

        // reset the bookkeeping so we know nothing has been processed
        processed_so_far = 0;
        delete [] records;
    }


    void merge_runs() {
        //require(::remove(filename.c_str()) == 0,
        //        "could not delete input file.");
        writer_type output;
        do {
            std::pair<unsigned int, unsigned int> pair = pick_runs();
            processed_so_far = pair.second;
            output.open(processed_so_far != number_of_runs
                        ? generate_run() : outfile);
            merge_runs(pair.first, pair.second, output);
            output.close();
        } while (processed_so_far < number_of_runs);
    }

    void merge_runs(unsigned int f, unsigned int l, writer_type& output) {
        std::vector<std::pair<record_type, unsigned int> > heap;
        std::vector<reader_type*> readers;
        std::vector<unsigned int> exhausted;

        for (unsigned int i = f; i < l; i++) {
            std::stringstream s;
            s << temp_prefix << "." << i << std::ends;
            readers.push_back(new reader_type(s.str()));
        }

        for (unsigned int i = 0; i < l-f; i++)
            if (readers[i]->hasNext())
                heap.push_back(std::make_pair(readers[i]->nextRecord(), i));

        bool done = heap.empty();
        while (! done) {
            std::make_heap(heap.begin(), heap.end(), heap_comp);
            output.write(heap[0].first);
            if (readers[heap[0].second]->hasNext())
                heap[0] = std::make_pair(readers[heap[0].second]->nextRecord(),
                                         heap[0].second);
            else {
                exhausted.push_back(heap[0].second);
                heap.erase(heap.begin());
            }
            done = exhausted.size() == readers.size();
        }

        for (typename std::vector<reader_type*>::iterator it = readers.begin();
             it != readers.end(); it++)
            delete *it;

        output.flush();
    }

    std::pair<unsigned int, unsigned int> pick_runs() const {
        return std::make_pair(processed_so_far,
                              (processed_so_far + number_of_buffers - 1
                               > number_of_runs
                               ? number_of_runs
                               : processed_so_far + number_of_buffers - 1));
    }

    void remove_runs() const {
        for (unsigned int i = 1; i < number_of_runs; i++) {
            std::stringstream s;
            s << temp_prefix << "." << i << std::ends;
            fs::remove(s.str());
            //require(::remove(s.str().c_str()) == 0,
            //        "could not delete run file.");
        }
    }

    void dump_runs() {
        reader_type reader;
        for (unsigned int i = 0; i < number_of_runs; i++) {
            std::stringstream s;
            s << temp_prefix << "." << i << std::ends;
            reader.open(s.str());
            unsigned j = 0;
            while (reader.hasNext())
                std::cout << "run " << i << ", " << j++ << ": "
                          << reader.nextRecord() << std::endl;
            reader.close();
        }
    }

    void dump() {
        reader_type reader(outfile);
        while (reader.hasNext())
            std::cout << "out " << reader.nextRecord() << std::endl;
    }

 private:
    std::string filename;
    std::string temp_prefix;
    std::string outfile;
    unsigned int number_of_runs;
    unsigned int processed_so_far;
    const size_t number_of_buffers;

    std::string generate_run() {
        std::stringstream s;
        s << temp_prefix << "." << number_of_runs++ << std::ends;
        return s.str();
    }
};


// Aliases
template <typename ToKeyFn,
          typename Record,
          typename KeyRecord,
          size_t BUFFER_SIZE=10>
using Sort = MergeSort<ToKeyFn, Record, KeyRecord, BUFFER_SIZE>;

template <typename ToKeyFn,
          typename Record,
          typename KeyRecord,
          size_t BUFFER_SIZE>
ToKeyFn  MergeSort<ToKeyFn, Record, KeyRecord, BUFFER_SIZE>::to_key;

template<typename Record, typename Breaker=Always<Record, Uninhabited, false> >
using Copy = Select<PredicateTrue, Record, Breaker>;




template <typename LeftExtract,
          typename RightExtract,
          typename KeyRecord,
          typename Left, typename Right,
          typename Combine,
          typename Breaker=Always<Left, Right, false> >
using LoopEquiJoin = Join<EquiJoinPredicate<LeftExtract, RightExtract,
                                            Left, Right>,
                          Left, Right, Combine, Breaker>;

#define ASARR(L, str) (*(std::array<char, L>*)(str))

template<size_t L, size_t N>
const std::array<char, L> & prefix(const std::array<char, N> & x) {
    static_assert(L <= N, "Requested too large prefix.");
    return ASARR(L, x.data());
}

template<size_t From, size_t To, size_t N>
const std::array<char, To - From> & subseq(const std::array<char, N> & x) {
    static_assert(From < N && To < N, "Subsequence bounds not in range.");
    return ASARR(To - From, x.data() + From);
}

template<size_t L, size_t N>
const std::array<char, L> exact_length_or_null(const std::array<char, N> & x) {
    if (L == N) return ASARR(L, x.data());
    if (L > N) return {0};
    // Str Length must be exactly L or return NULL
    std::array<char, L> nil = {0};
    if (!(*(x.data() + L - 1) != 0 && *(x.data() + L) == 0)) return (nil);
    return ASARR(L, x.data());
}

template<size_t L, size_t N>
const std::array<char, L> & suffix(const std::array<char, N> & x) {
    static_assert(L <= N, "Requested too large suffix.");
    return ASARR(L, x.data() + std::min(N, strlen(x.data())) - L);
}

template<size_t N, typename Iter>
inline bool greedy_like(const std::array<char, N> x&, const Iter& str) {
#define UPDATE_STATE do {                                           \
        on_wildcard = false;                                        \
        if (c != str.end() && *c == '%') {                          \
            on_wildcard = true;                                     \
            c++;                                                    \
            if (c != str.end() && *c == '%') on_wildcard == false;  \
        }} while(0)

    bool on_wildcard;
    auto c = str.begin();
    UPDATE_STATE;

    for (auto i : x) {
        if (!i) break;
        if (on_wildcard && c == str.end()) return true;
        if (on_wildcard && *c != i) continue;
        if (*c == i) {
            c++;
            UPDATE_STATE;
        } else {
            return false;
        }
    }
    return (c == str.end());
#undef UPDATE_STATE
}

template<typename R, bool isMinimum>
class Extreme {
 public:
    Extreme() : state(0), is_set(false) {}
    R operator() (const R & r) {
        if (!is_set || isMinimum ^ (state < r)) state = r;
        return state;
    }
 private:
    bool is_set;
    R state;
};

template<typename R>
using Minimum = Extreme<R, true>;
template<typename R>
using Maximum = Extreme<R, false>;


template<typename R>
class Sum {
 public:
    Sum() : state(0) {}
    R operator() (const R & r) {
        state += r;
        return state;
    }
 private:
    R state;
};


template<typename R>
class Average {
 public:
    Average() : state(0), count(0) {}
    R operator() (const R & r) {
        state += r;
        return state / ++count;
    }
 private:
    R state;
    unsigned count;
};


// XXX: We should be able to tell if it's too large.
template<typename R>
class DistinctCount {
 public:
    DistinctCount() : state() {}
    unsigned operator() (const R & r) {
        state.add(r);
        return state.size();
    }
 private:
    std::set<R> state;
};

template<typename R>
class ConstAggr {
 public:
    ConstAggr() : is_set(false) {}
    R operator() (const R & r) {
        if (!is_set) state = r;
        return state;
    }
 private:
    bool is_set;
    R state;
};

#define EXTRACT_FN(tm_key, fnname, offset)                              \
    unsigned extract_ ## fnname (unsigned x) {                          \
        struct tm t;                                                    \
        time_t nano = x;                                                \
        gmtime_r(&nano, &t);                                            \
        return (int)(t.tm_key + offset);                                \
    }
EXTRACT_FN(tm_mon, month, 1)
EXTRACT_FN(tm_mday, day, 0)
EXTRACT_FN(tm_year, year, 1900)

#endif /* CODEGEN_H */
