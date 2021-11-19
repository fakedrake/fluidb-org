#pragma once

#include <vector>
#include <array>
#include <algorithm>
#include <unistd.h>
#include <cassert>
#include "require.hh"
template<size_t page_recs,typename It>
struct HeapSort {
  struct Range {
    Range(It begin, It end) : begin(begin),  end(end) {}
    ~Range() {}

    std::pair<Range, Range> split() {
      size_t dist = (end - begin)/page_recs;
      auto mid = begin + (dist / 2 + dist % 2) * page_recs ;
      return {Range(begin,mid),Range(mid,end)};
    }

    std::pair<Range, Range> snoc() const {
      if (end - begin < (int)page_recs) return {Range(begin, end), Range(end, end)};
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
      auto [h,t] = snoc();
      return h < t;
    }

    bool is_sorted() const {
      for (It i = begin; i < end - 1; i++) {
        if (*i > *(i + 1)) {
          return false;
        }
      }
      return true;
    }

    std::string show() {
      std::string ret = "[";
      for (It i = begin; i < end; i++ ) {
        ret += std::to_string(*i) + " ";
      }
      ret += "]";
      return ret;
    }

    inline size_t operator<(const Range& r) const {
      return empty() or r.empty() or *(end - 1) <= *r.begin;
    }

    It begin, end;
  };

  // Discriminate between two sorted pages.
  static void discriminate(Range a, Range b) {
    // Allocating into the stack avoids mallocs.
    std::array<typename It::value_type, page_recs * 2> arr;
    std::merge(a.begin, a.end, b.begin, b.end, arr.begin());
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

    discriminate(hA, hB);    // PAR
    merge_adjheaps(lA, rA);  // PAR
    if (hB < lB and hB < rB) {
      merge_adjheaps(tA, B);
      return;
    }

    // If B is not a heap anymore it means that the upper bound of hA
    // spilled into the upper bound of the new hB and therefore now hB
    // is bounded by lA, lB.
    merge_adjheaps(lB, rB);
    merge_adjheaps(hB, tB);
    merge_adjheaps(tA, B);
  }

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
      std::sort(tmp.begin, tmp.end);
    }
    make_heap(A);
    auto [hA, tA] = A.snoc();
    auto [l, r] = tA.split();
    merge_adjheaps(l, r);
  }
};

template<size_t page_recs,typename It>
void hsort(It begin,It end) {
  HeapSort<page_recs,It>::inline_heapsort(begin, end);
}
