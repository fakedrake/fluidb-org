#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <string>
#include <cassert>

#include "file.hh"
#include "book.hh"
#include "common.hh"
#include "print_record.hh"


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

template<typename FoldFn,
         typename StartNewRecord>
class Aggregation {
    typedef typename FoldFn::Codomain RecordTo;
    typedef typename FoldFn::Domain0 RecordFrom;
 public:
    Aggregation (const Just<const std::string>& o, const std::string& i) :
            infile(i), outfile(o.value) {}

    ~Aggregation () {}

    void print_output(size_t x) {
        print_records<RecordTo>(outfile, x);
        report();
    }

    void run() {
        Writer<RecordTo> output(outfile);
        RecordFrom record, last_record;
        bool justWrote;
        RecordTo currentRec;
        StartNewRecord startNew;
        FoldFn fold;
        size_t count = 0;

        eachRecord<RecordFrom>(
            infile,
            [&](RecordFrom record) {
                justWrote = false;
                if (startNew(record)) {
                    count ++ ;
                    fold = FoldFn();
                    output.write(currentRec);
                    currentRec = RecordTo();
                    justWrote = true;
                }
                last_record = record;
                currentRec = fold(record);
            });

        // If the last record was not already written.
        if (!justWrote) output.write(currentRec);
        output.flush();
        output.close();
        std::cout << "Records: " << count << std::endl;
    }

 private:
    std::string infile;
    std::string outfile;
};

template<typename FoldFn,typename StartNewRecord>
auto mkAggregation(const Just<const std::string>& prim,
                   const Just<const std::string>& sec,
                   const std::string& i) {
    assert(sec.value == i);  // Aggregation should always be short cirquiting
    return Aggregation<FoldFn, StartNewRecord>(prim,i);
}
#endif /* AGGREGATION_H */
