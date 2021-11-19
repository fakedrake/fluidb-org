#pragma once

// The record stream provides randm read/write acces to the file at
// the record level.

#include <map>
#include <queue>
#include <fmt/format.h>

#include <iostream>

#include "require.hh"
#include "page.hh"
#include "file.hh"
#include "heap_sort.hh"
#include "defs.hh"

// Fewer than 10M records for debugging
#define MAX_INDEX 10000000

template <typename R>
class RecordMap {
  struct RefCntPage {
    RefCntPage(Page<R>* pg) : m_pg(pg), m_refs(0) { incr(); }
    RefCntPage(const RefCntPage&) = delete;
    RefCntPage(RefCntPage&&) = delete;
    ~RefCntPage() {
      require(m_pg, "Double freing");
      aligned_delete((unsigned char*)m_pg);
    }

    int m_refs;
    Page<R>* m_pg;

    void incr() { m_refs++; require_lt(m_refs, 30, "too many refs"); }
    // Return whether we shoudl delete.
    bool decr() { return --m_refs <= 0; }
  };
  class Iterator;
  struct RecordLoc;

  typedef std::map<size_t, RefCntPage> pg_map;
  struct RecrodLoc;

public:
  RecordMap(RecordMap&&) = delete;
  RecordMap(const RecordMap&) = delete;
  RecordMap(const std::string fn) : m_file(fn, RW_FLAGS) {
    size_t pg = m_file.numPages();
    if (pg == 0) {
      m_record_num = 0;
      return;
    }
    Page<R>* page = get_page(pg - 1);
    RecordLoc loc(pg - 1, page->length());
    m_record_num = loc.record_index();
    drop_page(pg - 1);
  }
  ~RecordMap() { m_file.close(); }

  size_t page_num() { return m_record_num / Page<R>::capacity(); }

  Page<R>* peek_page(size_t page_id) {
    typename pg_map::iterator pg = m_page_cache.find(page_id);
    require(pg != m_page_cache.end(), "Peeked non existent page");
    return pg->second.m_pg;
  }

  Page<R>* get_page(size_t page_id) {
    typename pg_map::iterator pg = m_page_cache.find(page_id);
    if (pg != m_page_cache.end()) {
      pg->second.incr();
      return pg->second.m_pg;
    }
    auto pair =
      m_page_cache.emplace(std::make_pair(page_id, m_file.readPage(page_id)));
    require_eq(pair.first->second.m_refs, 1,
               "There is a single reference to the page.");
    return pair.first->second.m_pg;
  }

  void drop_page(size_t page_id) {
    typename pg_map::iterator pg = m_page_cache.find(page_id);
    require(pg != m_page_cache.end(), "Dropping nonexistent page");
    if (pg->second.decr()) {
      m_file.writePage(page_id, *pg->second.m_pg);
      m_page_cache.erase(page_id);
    }
  }

  Page<R>* swap_page(size_t pi1, size_t pi2) {
    if (pi1 == pi2) {
      return peek_page(pi2);
    }
    Page<R>* pg_ref;
    drop_page(pi1);
    return get_page(pi2);
  }

  R* get_record(size_t old_page_id, RecordLoc& loc) {
    Page<R>* pg = swap_page(old_page_id, loc.m_page_id);
    require(pg, "Couldn't find page");
    return &pg->get(loc.m_record_id);
  }
  R* get_record(RecordLoc& loc) {
    Page<R>* pg = get_page(loc.m_page_id);
    require(pg, "couldn't find page");
    return &pg->get(loc.m_record_id);
  }

  Iterator begin() { return {0, this}; }
  Iterator end() { return {m_record_num, this}; }

private:
  File<R> m_file;
  pg_map m_page_cache;
  size_t m_record_num;
};

template <typename R>
class RecordMap<R>::Iterator
  : public std::iterator<std::random_access_iterator_tag, R> {
public:
  using difference_type = int;
  using value_type = R;

  Iterator() : m_index(0), m_recs(nullptr), m_holding_page(-1) {}
  Iterator(size_t index, RecordMap* recs)
      : m_index(index), m_recs(recs), m_holding_page(-1) {}
  Iterator(Iterator&& rhs)
      : m_index(rhs.m_index),
        m_recs(rhs.m_recs),
        m_holding_page(rhs.m_holding_page) {
    rhs.m_holding_page = -1;
  }
  Iterator(const Iterator& rhs)
      : m_index(rhs.m_index), m_recs(rhs.m_recs), m_holding_page(-1) {
    require(m_index < MAX_INDEX, "Index too large.");
  }

  Iterator& operator=(const Iterator& rhs) noexcept {
    if (this == &rhs) return *this;

    m_index = rhs.m_index;
    require(m_index < MAX_INDEX, "Index too large.");
    // If there already is a valid holding page forget it
    if (m_holding_page >= 0) {
      m_recs->drop_page(m_holding_page);
      m_holding_page = -1;
    }
    m_recs = rhs.m_recs;
    return *this;
  }
  Iterator& operator=(Iterator&& rhs) noexcept {
    if (this == &rhs)
      return *this;

    m_index = rhs.m_index;
    require(m_index < MAX_INDEX, "Index too large.");
    m_recs = rhs.m_recs;
    // If there already is a valid holding page forget it
    if (m_holding_page >= 0) {
      m_recs->drop_page(m_holding_page);
    }
    m_holding_page = rhs.m_holding_page;
    rhs.m_holding_page = -1;
    return *this;
  }

  ~Iterator() {
    if (m_holding_page >= 0)
      m_recs->drop_page(m_holding_page);
  }
  /* inline Iterator& operator=(R* rhs) {_ptr = rhs; return *this;} */
  /* inline Iterator& operator=(const Iterator &rhs) {_ptr = rhs._ptr; return
   * *this;} */
  inline Iterator& operator+=(difference_type rhs) {
    m_index += rhs;
    return *this;
  }
  inline Iterator& operator-=(difference_type rhs) {
    m_index -= rhs;
    require(m_index < MAX_INDEX, "Index too large.");
    return *this;
  }
  R& operator*() {
    auto loc = RecordLoc::as_record_loc(m_index);
    auto old_page = m_holding_page;
    m_holding_page = loc.m_page_id;
    if (old_page >= 0) {
      return *m_recs->get_record(old_page, loc);
    }
    return *m_recs->get_record(loc);
  }

  inline Iterator operator+(difference_type n) {
    Iterator tmp{*this};
    tmp += n;
    return tmp;
  }
  inline Iterator operator-(difference_type n) {
    Iterator tmp{*this};
    tmp -= n;
    return tmp;
  }

  inline R& operator[](difference_type n) const { return *(*this + n); }

  inline Iterator& operator++() {
    ++m_index;
    return *this;
  }
  inline Iterator& operator--() {
    --m_index;
    require(m_index < MAX_INDEX, "Index too large.");
    return *this;
  }
  inline Iterator operator++(int) {
    Iterator tmp(*this);
    ++m_index;
    return tmp;
  }
  inline Iterator operator--(int) {
    Iterator tmp(*this);
    --m_index;
    require(m_index < MAX_INDEX, "Index too large.");
    return tmp;
  }
  /* inline Iterator operator+(const Iterator& rhs) {return
   * Iterator(m_index+rhs.ptr);} */
  inline difference_type operator-(const Iterator& rhs) const {
    return m_index - rhs.m_index;
  }

  inline Iterator operator+(difference_type rhs) const {
    return Iterator(m_index + rhs, m_recs);
  }
  inline Iterator operator-(difference_type rhs) const {
    return Iterator(m_index - rhs, m_recs);
  }

  inline bool operator==(const Iterator& rhs) const {
    return m_index == rhs.m_index;
  }
  inline bool operator!=(const Iterator& rhs) const {
    return m_index != rhs.m_index;
  }
  inline bool operator>(const Iterator& rhs) const {
    return m_index > rhs.m_index;
  }
  inline bool operator<(const Iterator& rhs) const {
    return m_index < rhs.m_index;
  }
  inline bool operator>=(const Iterator& rhs) const {
    return m_index >= rhs.m_index;
  }
  inline bool operator<=(const Iterator& rhs) const {
    return m_index <= rhs.m_index;
  }

private:
  RecordMap<R>* m_recs;
  int m_holding_page;
  size_t m_index;
};

// The location of a file.
template<typename R>
struct RecordMap<R>::RecordLoc {
  size_t m_page_id, m_record_id;
  RecordLoc(size_t page_id, size_t record_id)
    : m_page_id(page_id), m_record_id(record_id) {}
  ~RecordLoc() {}

  static RecordLoc as_record_loc(size_t n) {
    return {n / Page<R>::capacity(), n % Page<R>::capacity()};
  }


  size_t record_index() const {
    return Page<R>::capacity() * m_page_id + m_record_id;
  }
};

template <typename Extract>
struct CompareOn {
  typedef typename Extract::Domain0 R;
  bool operator()(const R& l, const R& r) { return extr(l) < extr(r); }
  Extract extr;
};

template <typename Extract>
void sortFile(const std::string& file) {
  CompareOn<Extract> cmp;
  bool sorted = true, started = false;
  RecordMap<typename Extract::Domain0> fs(file);
  auto tmp = fs.begin();
  fmt::print("Checking if file is sorted.. {}\n", file);
  for (auto r = fs.begin(); r != fs.end(); r++) {
    if (!started) {
      tmp = r;
      started = true;
      continue;
    }
    if (not cmp(*r, *tmp)) {
      sorted = false;
      break;
    }
    tmp = r;
  }
  if (sorted) {
    fmt::print("Already sorted! {}\n", file);
    return;
  }
  fmt::print("Not sorted... {}\n", file);
  std::sort(fs.begin(), fs.end(), cmp);
  // hsort<Page<typename Extract::Domain0>::allocation, Extract>(fs.begin(),
  //                                                             fs.end());
}
