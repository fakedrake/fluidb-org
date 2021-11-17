#ifndef SELECT_H
#define SELECT_H

#include <string>

#include "common.hh"
#include "print_record.hh"
#include "io.hh"
#include "defs.hh"

template<typename Predicate,
         typename PrimaryOutType,  // Maybe(std::string)
         typename SecondaryOutType // Maybe(std::string)
         >
class Select {
    typedef typename Predicate::Domain0 Record;
 public:
    Select (const PrimaryOutType& prim,
            const SecondaryOutType& sec,
            const std::string& in) :
            primary_file(prim), secondary_file(sec), infile(in) {
        static_assert(!PrimaryOutType::isNothing || !SecondaryOutType::isNothing,
                      "Both primary and secondary output files are Nothing.");
    }

    ~Select () {report();}

    void run() {
      Reader<Record> reader;
      // Record record;
      Writer<Record> primary_output;
      Writer<Record> secondary_output;

      WITH(primary_file, primary_output.open(primary_file.value));
      WITH(secondary_file, secondary_output.open(secondary_file.value));
      eachRecord<Record>(infile, [&](Record record) {
        if (predicate(record)) {
          WITH(primary_file, primary_output.write(record));
        } else {
          WITH(secondary_file, secondary_output.write(record));
        }
      });
      WITH(primary_file, primary_output.close());
      WITH(secondary_file, primary_output.close());
    }

    void print_output(size_t x) {
        print_records<Record>(primary_file, x);
        print_records<Record>(secondary_file, x);
        report();
    }

 private:
    PrimaryOutType primary_file;
    SecondaryOutType secondary_file;
    std::string infile;
    static Predicate predicate;
};

// Declare the static members
template<typename Predicate,
         typename PrimaryOutType,
         typename SecondaryOutType>
Predicate Select<Predicate, PrimaryOutType, SecondaryOutType>::predicate;

// C++17 can only infer typenames (primaryout secondaryout) in
// function templates.
template<typename Predicate,
         typename PrimaryOutType,   // Maybe(std::string)
         typename SecondaryOutType  // Maybe(std::string)
         >
auto mkSelect (const PrimaryOutType& prim,
               const SecondaryOutType& sec,
               const std::string& in) {
    return Select<Predicate,
                  PrimaryOutType,   // Maybe(std::string)
                  SecondaryOutType> // Maybe(std::string)
                  (prim, sec, in);
}

#endif /* SELECT_H */
