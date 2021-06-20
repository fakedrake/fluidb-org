#ifndef LIMIT_H
#define LIMIT_H

#include <string>

#include "file.hh"
#include "print_record.hh"

template<size_t limit,
         typename Record,
         typename LimitOutType,
         typename DropOutType>
class Limit {
 public:
    Limit(const LimitOutType limitout,
          const DropOutType dropout,
          const std::string in) :
            infile(in), limitfile(limitout), dropfile(dropout) {
        static_assert(!LimitOutType::isNothing || !DropOutType::isNothing,
                      "Both primary and secondary output files are Nothing.");

    }
    void print_output(size_t x) {
        print_records<Record>(limitfile, x);
        print_records<Record>(dropfile, x);
        report();
    }

    void run() {
        Reader<Record> reader(infile);
        Writer<Record> writer;
        Writer<Record> dropwriter;
        size_t i = 0;

        WITH(limitfile, writer.open(limitfile.value));
        WITH(dropfile, dropwriter.open(dropfile.value));

        while (reader.hasNext()) {
            if (i++ >= limit && !(DropOutType::isNothing)) {
                while (reader.hasNext()) {
                    auto rec = reader.nextRecord();
                    writer.write(rec);
                }
                break;
            }

            auto rec = reader.nextRecord();
            WITH(limitfile, writer.write(rec));
        }

        reader.close();
        WITH(dropfile, dropwriter.close());
        WITH(limitfile, writer.close());
    }

 private:
    const std::string infile;
    const LimitOutType limitfile;
    const DropOutType dropfile;
};
template<size_t limit,
         typename Record,
         typename LimitOutType,
         typename DropOutType>
auto mkLimit(const LimitOutType limitfile,
             const DropOutType dropfile,
             const std::string infile) {
    return Limit<limit, Record, LimitOutType, DropOutType>(limitfile, dropfile, infile);
}

#endif /* LIMIT_H */
