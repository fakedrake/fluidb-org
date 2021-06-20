#ifndef UNION_H
#define UNION_H

#include <string>

#include "common.hh"
#include "file.hh"

template<typename FromLeft, typename FromRight>
class Union {
    typedef typename FromLeft::Domain0 Left;
    typedef typename FromRight::Domain0 Right;
    typedef typename FromRight::Codomain Out;
 public:
    Union(const Just<const std::string>& out, const std::string& left, const std::string& right) :
            leftfile(left), rightfile(right), outfile(out.value) {}

    void run() {
        static FromLeft extrL;
        static FromRight extrR;

        Writer<Out> w(outfile);
        eachRecord<Left>(leftfile,[&](const Left& r){w.write(extrL(r));});
        eachRecord<Right>(rightfile,[&](const Right& r){w.write(extrR(r));});
        w.close();
    }
    ~Union () {}

    void print_output(size_t x) {
        print_records<Out>(outfile,x);
        report();
    }

 private:
    std::string leftfile, rightfile,outfile;
};

template<typename FromLeft, typename FromRight>
auto mkUnion(const Just<const std::string>& out, const std::string& l, const std::string& r) {
    return Union<FromLeft,FromRight>(out,l,r);
}

#endif /* UNION_H */
