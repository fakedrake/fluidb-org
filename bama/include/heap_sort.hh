#pragma once

#include <vector>
#include <array>
#include <algorithm>
#include <unistd.h>
#include <cassert>
#include "require.hh"

template <size_t page_recs, typename It, typename Extract>
struct HeapSort {
  static Extract extract;
  struct Range {
    Range(It begin, It end) : begin(begin), end(end) {}
    ~Range() {}
    std::pair<Range, Range> split() {
      size_t dist = (end - begin) / page_recs;
      auto mid = begin + (dist / 2 + dist % 2) * page_recs;
      return {Range(begin, mid), Range(mid, end)};
    }

    std::pair<Range, Range> snoc() const {
      if (end - begin < (int)page_recs)
        return {Range(begin, end), Range(end, end)};
      auto tmp = begin + page_recs;
      return {Range(begin, tmp), Range(tmp, end)};
    }

    bool empty() const { return begin == end; }

    size_t size() const { return end - begin; }

    bool pages_sorted() const {
      for (It p = begin; p < end; p += page_recs) {
        Range tmp{p, std::min(p + page_recs, end)};
        if (!tmp.is_sorted()) return false;
      }
      return true;
    }

    bool is_heap() const {
      auto [h, t] = snoc();
      return h < t;
    }

    bool is_sorted() const {
      for (It i = begin; i < end - 1; i++) {
        if (extract(*i) > extract(*(i + 1))) {
          return false;
        }
      }
      return true;
    }

    std::string show() {
      std::string ret = "[";
      for (It i = begin; i < end; i++) {
        ret += std::to_string(extract(*i)) + " ";
      }
      ret += "]";
      return ret;
    }

    inline bool is_discrim(Range& r) const {
      auto rv = *r.begin;
      return empty() or r.empty() or extract(*(end - 1)) < extract(rv);
    }

    It begin, end;
  };

  static bool less_than(const typename It::value_type& l,
                        const typename It::value_type& r) {
    return extract(l) < extract(r);
  }

  template <typename ArrIt>
  static void merge_internal(Range a, Range b, ArrIt result) {
    auto bk = extract(*b.begin), ak = extract(*a.begin);
    while (true) {
      if (a.begin == a.end) {std::copy(b.begin, b.end, result); return; }
      if (b.begin == b.end) {std::copy(a.begin, a.end, result); return; }
      if (bk < ak) {
        *result++ = *b.begin++;
        bk = extract(*b.begin);
      } else {
        *result++ = *a.begin++;
        ak = extract(*a.begin);
      }
    }
  }

  // Discriminate between two sorted pages.
  static void discriminate(Range a, Range b) {
    // Allocating into the stack avoids mallocs.
    std::array<typename It::value_type, page_recs * 2> arr;
    auto result = arr.begin();
    if (a.empty() or b.empty()) return;
    merge_internal(a, b, arr.begin());
    auto mid = arr.begin() + a.size();
    std::copy(arr.begin(), mid, a.begin);
    std::copy(mid, mid + b.size(), b.begin);
  }

  // Merge adjacent heaps.
  static void merge_adjheaps(Range A, Range B) {
    if (A.empty() and B.empty()) {
      return;
    }

    if (A.empty()) {
      auto [hB, tB] = B.snoc();
      auto [lB, rB] = tB.split();
      merge_adjheaps(lB, rB);
      return;
    }
    if (B.empty()) {
      auto [lA, rA] = A.snoc().second.split();
      merge_adjheaps(lA, rA);
      return;
    }

    auto [hA, tA] = A.snoc();
    auto [hB, tB] = B.snoc();
    auto [lB, rB] = tB.split();
    auto [lA, rA] = tA.split();

    discriminate(hA, hB);
    merge_adjheaps(lA, rA);  // PAR
    merge_adjheaps(lB, rB);
    if (not(hB.is_discrim(lB) and hB.is_discrim(rB))) {
      // If B is not a heap anymore we need to turn it into one again
      merge_page(hB, tB);
      return;
    }

    merge_sorted_contiguous(tA, B);
  }

  // XXX: This might still fuck up wrt the pages.
  static void merge_sorted_contiguous(Range A, Range B) {
    std::inplace_merge(A.begin, A.end, B.end, less_than);
  }

  // XXX: find a better implementation
  static void merge_page(Range pg, Range B) { merge_sorted_contiguous(pg, B); }

  static void make_heap(Range A) {
    if (A.empty()) return;
    auto [h, t] = A.snoc();
    for (It p = t.begin; p < t.end; p += page_recs) {
      Range tmp = {p, std::min(p + page_recs, A.end)};
      discriminate(h, tmp);
    }
    auto [l, r] = t.split();
    make_heap(l);  // PAR
    make_heap(r);  // PAR
  }

  static void inline_heapsort(It begin, It end) {
    Range A(begin, end);
    // Sort all pages
    for (It p = A.begin; p < A.end; p += page_recs) {
      Range tmp{p, std::min(p + page_recs, A.end)};
      std::sort(tmp.begin, tmp.end, less_than);
    }
    make_heap(A);
    auto [hA, tA] = A.snoc();
    auto [l, r] = tA.split();
    merge_adjheaps(l, r);
  }
};
template <size_t page_recs, typename It, typename Extract>
Extract HeapSort<page_recs, It, Extract>::extract;

template <size_t page_recs, typename Extract, typename It>
void hsort(It begin, It end) {
  HeapSort<page_recs, It, Extract>::inline_heapsort(begin, end);
}
