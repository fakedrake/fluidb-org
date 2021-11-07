#ifndef __PAGE_HH__
#define __PAGE_HH__

#include "defs.hh"
#include "common.hh"
#include "require.hh"
#include <iostream>
#include <cmath>

/*
  template <typename K, typename P>
  struct Record: public boost::totally_ordered<Record<K, P> > {
  K key;
  P payload;

  bool operator==(const Record<K, P>& r) const { return key == r.key; }
  bool operator<(const Record<K, P>& r) const { return key < r.key; }
  friend std::ostream& operator<<(std::ostream& os,
  const Record<K, P>& r) {
  os << "<" << r.key << ", " << r.payload << ">";
  return os;
  }
  };
*/

// we need this since alingment of records in pages is necessary and this is
// done through unions, which imply the use of types with parameter-less
// constructor, so we can't use std::pair
/*
  template <typename P1, typename P2>
  struct CombinedRecord {
  P1 first;
  P2 second;
  };

  template <typename K, typename P>
  Record<K, P> makeRecord(const K& k, const P& p) {
  Record<K, P> rec;
  rec.key = k;
  rec.payload = p;
  return rec;
  }

  template <typename P1, typename P2>
  CombinedRecord<P1, P2> makeCombinedRecord(const P1& p1, const P2& p2) {
  CombinedRecord<P1, P2> cr;
  cr.first = p1;
  cr.second = p2;
  return cr;
  }
*/

template <typename R>
class /*alignas(PAGE_SIZE)*/ Page {
 public:
  // enum { size = get_pagesize() };
  // enum { header_size = sizeof(size_t) + sizeof(void*) };
  enum { header_size = sizeof(size_t) };
  typedef R record_type;

  Page() : numrecs(0) {}
  ~Page() {}

 private:
  static const size_t datasize = PAGE_SIZE - header_size;

 public:
  static const size_t allocation = datasize / sizeof(record_type);

 private:
  size_t numrecs;
  union __Data {
    R records[allocation];
    char padding[datasize - (datasize % sizeof(void*))];
    __Data() { ::memset(this, 0, sizeof(__Data)); }
  } data;

 public:
  static size_t capacity() { return allocation; }
  /*const*/ size_t length() const { return numrecs; }

  const record_type& get(size_t i) const {
    require_lt(i, numrecs, "Record out of bounds");
    return data.records[i];
  }
  record_type& get(size_t i) {
    require_lt(i, numrecs, "Record out of bounds");
    return data.records[i];
  }
  void set(size_t i, const record_type& r) { data.records[i] = r; }
  size_t add(const record_type& r) {
    require_le(numrecs, allocation, "out of space");
    data.records[numrecs++] = r;
    return numrecs;
  }
  void clear() { numrecs = 0; }

  // void setNumberOfRecords(size_t n) { numrecs = n; }
  void setAll(const record_type* recs, rec_num_t nr) {
    require_le(nr, allocation, "data does not fit");
    ::memmove(&data.records[0], recs, nr * sizeof(record_type));
    numrecs = nr;
  }

  rec_num_t getAll(record_type* recs, rec_num_t nr) {
    nr = std::min(nr, length());
    ::memmove(recs, &data.records[0], nr * sizeof(record_type));
    return nr;
  }

  const R* begin() const { return &(data.records[0]); }
  const R* end() const { return &(data.records[length()]); }
  /*
    void addAll(const record_type* recs, size_t nr) {
    require(nr < allocationdata.records
    }
  */
};

template<typename R>
constexpr rec_num_t firstRecIndex(page_num_t l) {
    return l * Page<R>::allocation;
}
template<typename R>
constexpr page_num_t recordToPageNum(rec_num_t l) {
    double after_header_size = PAGE_SIZE - Page<R>::header_size;
    double records_in_page = after_header_size / sizeof(R);
    return ::ceil(((double) l) / records_in_page);
}

template<typename R>
constexpr page_num_t recordToPageOff(rec_num_t l) {
    double after_header_size = PAGE_SIZE - Page<R>::header_size;
    double records_in_page = after_header_size / sizeof(R);
    return ::floor(((double) l) / records_in_page);
}

// Turn an array of records into an array of pages.
template<typename R>
page_num_t expand(R* recs, rec_num_t len) {
    if (len == 0) return 0;
    page_num_t np = recordToPageNum<R>(len);
    require(np > 0, "Expand by at least one page,");
    size_t capacity = (PAGE_SIZE-Page<R>::header_size) / sizeof(R);
    char* p = (char*) recs;
    size_t full_pages = np - 1;
    p += (PAGE_SIZE * full_pages);
    size_t nrecs = (len % capacity == 0 ? capacity : len % capacity);
    len -= nrecs;
    size_t prev_recs = nrecs;
    ::memmove(&p[Page<R>::header_size], (char*) (&recs[len]),
              sizeof(R)*nrecs);
    p -= PAGE_SIZE;
    nrecs = capacity;
    while (full_pages > 0) {
        len -= nrecs;
        ::memmove(&p[Page<R>::header_size], (char*) (&recs[len]),
                  sizeof(R)*nrecs);
        *((size_t*) (p+PAGE_SIZE)) = prev_recs;
        prev_recs = nrecs;
        p -= PAGE_SIZE;
        full_pages --;
    }
    *((size_t*) (p+PAGE_SIZE)) = prev_recs;
    return np;
}

template<typename R>
rec_num_t compact(Page<R>* recs, size_t np, rec_num_t skip_nr) {
    R* r = (R*)recs;
    Page<R>* pg_off = (Page<R>*)recs;
    for (Page<R>* p = pg_off; p < pg_off + np; p++) {
        size_t nr = p->length();
        ::memmove((char*)r, (char*)p->begin() + skip_nr, sizeof(R) * nr);
        skip_nr = 0;
        r += nr;
    }
    return (size_t)((char*)r - (char*)recs) / sizeof(R);
}

template<typename R>
Page<R>* allocatePages(size_t len) {
    Page<R>* ret = (Page<R>*)aligned_new(recordToPageNum<R>(len));
    require(ret, "Allocation failed.");
    return ret;
}

#endif //:~ __PAGE_HH__
