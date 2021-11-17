#ifndef AGGREGATION_H
#define AGGREGATION_H

#include <string>
#include <cassert>

#include "file.hh"
#include "record_map.hh"
#include "common.hh"
#include "print_record.hh"

template<typename FoldFn,
         typename ExtractFn>
class Aggregation {
    typedef typename FoldFn::Codomain RecordTo;
    typedef typename FoldFn::Domain0 RecordFrom;
    typedef typename ExtractFn::Codomain SubTup;
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
        FoldFn fold;
        SubTup fn;
        size_t count = 0;
        sortFile<ExtractFn>(infile);

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
    }

 private:
    std::string infile;
    std::string outfile;
};

template<typename FoldFn,typename ExtractFn>
auto mkAggregation(const Just<const std::string>& prim,
                   const Just<const std::string>& sec,
                   const std::string& i) {
    assert(sec.value == i);  // Aggregation should always be short cirquiting
    return Aggregation<FoldFn, ExtractFn>(prim,i);
}
#endif /* AGGREGATION_H */
