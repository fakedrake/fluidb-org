#ifndef PROJECTION_H
#define PROJECTION_H


#include <string>

#include "common.hh"
#include "file.hh"
#include "print_record.hh"

\
template<typename PrimFn,
         typename SecFn,
         typename PrimaryOutType,  // Maybe(std::string)
         typename SecondaryOutType> // Maybe(std::string)
class Projection
{
    typedef typename PrimFn::Domain0 InRecord;
    typedef typename PrimFn::Codomain PrimOutRecord;
    typedef typename SecFn::Codomain SecOutRecord;
 public:
    Projection (const PrimaryOutType& prim,
                const SecondaryOutType& sec,
                const std::string& in) :
            prim_filename(prim),
            sec_filename(sec),
            in_filename(in)
    {
        static_assert(!PrimaryOutType::isNothing || !SecondaryOutType::isNothing,
                      "Both primary and secondary output files are Nothing.");
        TYPE_EQ(typename PrimFn::Domain0, typename SecFn::Domain0);

    }
    ~Projection() {}

    void print_output(size_t x) {
        print_records<PrimOutRecord>(prim_filename, x);
        print_records<SecOutRecord>(sec_filename, x);
        report();
    }

    void run() {
        Writer<PrimOutRecord> prim_writer;
        Writer<SecOutRecord> sec_writer;
        WITH(prim_filename, prim_writer.open(prim_filename.value));
        WITH(sec_filename, sec_writer.open(sec_filename.value));

        eachRecord<InRecord>(
            in_filename,
            [&](const InRecord& r) {
                WITH(prim_filename, prim_writer.write(prim_fn(r)));
                WITH(sec_filename, sec_writer.write(sec_fn(r)));
            });
    }

 private:
    PrimaryOutType prim_filename;
    SecondaryOutType sec_filename;
    std::string in_filename;
    static PrimFn prim_fn;
    static SecFn sec_fn;
};

template<typename PrimFn,
         typename SecFn,
         typename PrimaryOutType,  // Maybe(std::string)
         typename SecondaryOutType> // Maybe(std::string)
PrimFn Projection<PrimFn, SecFn, PrimaryOutType, SecondaryOutType>::prim_fn;

template<typename PrimFn,
         typename SecFn,
         typename PrimaryOutType,  // Maybe(std::string)
         typename SecondaryOutType> // Maybe(std::string)
SecFn Projection<PrimFn, SecFn, PrimaryOutType, SecondaryOutType>::sec_fn;

// Zip two projected records.
template<typename Combine>
class Zip
{
    typedef typename Combine::Domain0 PrimInRecord;
    typedef typename Combine::Domain1 SecInRecord;
    typedef typename Combine::Codomain OutRecord;
 public:
    Zip(const std::string& prim_in,
        const std::string& sec_in,
        const std::string& out) :
            prim_filename(prim_in),
            sec_filename(sec_in),
            out_filename(out) {}
    ~Zip() {}

    void print_output(size_t x) {
        print_records<OutRecord>(prim_filename, x);
        report();
    }

    void run() {
        Reader<PrimInRecord> prim_reader(prim_filename);
        Reader<SecInRecord> sec_reader(sec_filename);
        Writer<OutRecord> out_writer(out_filename);
        while (prim_reader.hasNext() && sec_reader.hasNext())
            out_writer.write(combine(prim_reader.nextRecord(),
                                     sec_reader.nextRecord()));
        prim_reader.close();
        sec_reader.close();
    }

 private:
    std::string prim_filename, sec_filename, out_filename;
    static Combine combine;
};

template<typename Combine>
Combine Zip<Combine>::combine;

template<typename PrimFn,
         typename SecFn,
         typename PrimaryOutType,  // Maybe(std::string)
         typename SecondaryOutType> // Maybe(std::string)
auto mkProjection(const PrimaryOutType& prim,
                  const SecondaryOutType& sec,
                  const std::string& in) {
    return Projection<PrimFn,
                      SecFn,
                      PrimaryOutType,
                      SecondaryOutType>
            (prim, sec, in);
}



#endif /* PROJECTION_H */
