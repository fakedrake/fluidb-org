#ifndef SORT_H
#define SORT_H

#include <cstdio>
#include <string>
#include <sstream>
#include <array>
#include <map>
#include <vector>

#include "common.hh"
#include "defs.hh"
#include "io.hh"
#include "print_record.hh"

template <typename ToKeyFn,
          typename OutputPrim,  // Maybe
          typename OutputSec,  // Maybe
          size_t BUFFER_SIZE=100000000>
class MergeSort {
 private:
    typedef typename ToKeyFn::Domain0 Record;
    typedef typename ToKeyFn::Codomain KeyRecord;
    static inline bool heap_comp(const std::pair<Record, unsigned int>& x,
                                 const std::pair<Record, unsigned int>& y) {
        return sort_comp(y.first, x.first);
        // return to_key(y.first) < to_key(x.first);
    }

    static ToKeyFn to_key;
    static inline bool sort_comp(const typename ToKeyFn::Domain0& x,
                                 const typename ToKeyFn::Domain0& y) {
        // Heap specific: we want the heap to be ascending by.
        return to_key(x) < to_key(y);
    }

 public:
    MergeSort(const OutputPrim& out,
              const OutputSec& outsec, // Unused..
              const std::string& fn,
              size_t nb=5)
            : in_filename(fn), temp_prefix(out.value), out_filename(out),
              number_of_runs(0),
              number_of_buffers(nb) {
    }
    ~MergeSort() {}

    void print_output(size_t x) {
        print_records<Record>(out_filename, x);
        report();
    }

    void run() {
        create_runs_bulk();
        merge_runs();
    }

 private:
    void create_runs_bulk() {
        BulkWriter<Record> bulk_writer;
        eachRecordBulk<Record>(BUFFER_SIZE / sizeof(Record), in_filename,
            [&](Record* recs, size_t num_recs) {
                const std::string fname = generate_run();
                std::sort(recs, recs + num_recs, sort_comp);
                bulkWrite<Record>(num_recs, fname, recs);
            });
        return;
    }

    void merge_runs() {
        //require(::remove(in_filename.c_str()) == 0,
        //        "could not delete input file.");
        Writer<Record> output;
        size_t processed_so_far = 0;
        std::string fname;
        do {
            std::pair<unsigned int, unsigned int> pair = pick_runs(processed_so_far);
            processed_so_far = pair.second;
            fname = processed_so_far != number_of_runs
                    ? generate_run() : out_filename.value;
            output.open(fname);
            merge_runs(pair.first, pair.second, output);
            output.close();
        } while (processed_so_far < number_of_runs);
    }

    void merge_runs(unsigned int f, unsigned int l, Writer<Record>& output) {
        std::vector<std::pair<Record, unsigned int> > heap;
        std::vector<Reader<Record>*> readers;
        std::vector<unsigned int> exhausted;

        for (unsigned int i = f; i < l; i++) {
            std::stringstream s;
            s << temp_prefix << "." << i << std::ends;
            readers.push_back(new Reader<Record>(s.str()));
        }

        for (unsigned int i = 0; i < l-f; i++)
            if (readers[i]->hasNext())
                heap.push_back(std::make_pair(readers[i]->nextRecord(), i));

        bool done = heap.empty();
        while (! done) {
            std::make_heap(heap.begin(), heap.end(), heap_comp);
            output.write(heap[0].first);
            if (readers[heap[0].second]->hasNext())
                heap[0] = std::make_pair(
                    readers[heap[0].second]->nextRecord(),
                    heap[0].second);
            else {
                exhausted.push_back(heap[0].second);
                heap.erase(heap.begin());
            }
            done = exhausted.size() == readers.size();
        }

        for (auto r : readers) {
            auto fn = r->getFilename();
            delete r;
            ::remove(fn.c_str());
        }

        output.flush();
    }

    std::pair<unsigned int, unsigned int> pick_runs(size_t processed_so_far) const {
        return std::make_pair(processed_so_far,
                              (processed_so_far + number_of_buffers - 1
                               > number_of_runs
                               ? number_of_runs
                               : processed_so_far + number_of_buffers - 1));
    }

 private:
    std::string in_filename;
    std::string temp_prefix;
    OutputPrim out_filename;
    unsigned int number_of_runs;
    const size_t number_of_buffers;

    std::string generate_run() {
        std::stringstream s;
        s << temp_prefix << "." << number_of_runs++ << std::ends;
        return s.str();
    }
};

template <typename ToKeyFn,
          typename OutputPrim,  // Maybe
          typename OutputSec,  // Maybe
          size_t BUFFER_SIZE>
ToKeyFn MergeSort<ToKeyFn, OutputPrim, OutputSec, BUFFER_SIZE>::to_key;

template <typename ToKeyFn,
          typename OutputPrim,  // Maybe
          typename OutputSec,  // Maybe
          size_t BUFFER_SIZE=PAGE_SIZE * 100>
MergeSort<ToKeyFn,
          OutputPrim,  // Maybe
          OutputSec,  // Maybe
          BUFFER_SIZE> mkSort(const OutputPrim& out,
                                   const OutputSec& outsec,
                                   const std::string& fn,
                                   size_t nb=5) {
    return MergeSort<ToKeyFn,
                     OutputPrim,  // Maybe
                     OutputSec,  // Maybe
                     BUFFER_SIZE>(out, outsec, fn, nb);
}
#endif /* SORT_H */
