// // Include this only though io.hh
#include "book.hh"
#include "defs.hh"
#include "require.hh"
#include "page.hh"
#include "util.hh"
#include <memory>
#include <string>
#include <fcntl.h>	// O_RDWR, O_CREAT
#include <cmath>        // ceil
#include <cstdlib>      // posix_memalign
#include <cstring>      // memset
#include <deque>
#include <queue>
#include <vector>
#include <algorithm>
#include <cstdio>

template <typename From, typename To>
size_t floorCastIndex(size_t i) {
    return i * sizeof(From) / sizeof(To);
}
template <typename From, typename To>
size_t ceilCastIndex(size_t i) {
    return i * sizeof(From) / sizeof(To) + (bool)(i % sizeof(To));
}

template<typename T>
bool ptr_less(T* a, T* b) {return *a < *b;}


// |-------------|------------| <-- reflexive
// |----|----|----|----|----|
class GenericFile {
    std::string filename;
    std::vector<unsigned char>* raw_data;
    size_t fileSizeBytes(int descriptor = -1) const {
        return ::lseek(descriptor, 0, SEEK_END);
    }

 public:
    typedef unsigned char field_type;
    GenericFile() {}
    GenericFile(const std::string &fn) {load(fn);}

    void load(const std::string& fn) {
        require(raw_data->empty(), "file already open.");
        filename = fn;
        int descriptor;
        require((descriptor = ::open(filename.c_str(),
                                     O_RDWR | O_CREAT, 0644)) != -1,
                "could not open file: " + filename);
        raw_data = new std::vector<unsigned char>(
            fileSizeBytes(descriptor));
        require(::read(descriptor, raw_data->data(), raw_data->size())
                !=  -1,
                "Failed to read the entire file " + fn);
        require(::close(descriptor) != -1, "could not close backing file.");
    }

    void writeRange(size_t from, size_t to, unsigned char * source) {
        raw_data->resize(std::max(to, raw_data->size()));
        ::memcpy((void*)(raw_data->data() + from), source, to - from);
    }
    void readRange(size_t from, size_t to, unsigned char * dest) {
        ::memcpy(dest, (raw_data->data() + from), to - from);
    }

    template<typename R>
    size_t size() const {
        return floorCastIndex<unsigned char, R>(raw_data->size());
    }
};

// A view of the memory as a tiling of T types (fields). It can read
// and write ranges by translating the fileds into fields of the
// subview. The base subview is the GenericFile which reads/writes in
// byte granularity. For example:
//
// MemView<Record, <MemView<Page> >(in.dat)
template<typename T, typename SubView=GenericFile>
class MemView {
 private:
    std::shared_ptr<SubView> subview;

 public:
    typedef T field_type;
    typedef typename SubView::field_type subfield_type;
    MemView() : subview(new SubView()) {}
    MemView(const std::string & fn) : subview(new SubView(fn)) {}
    MemView(std::shared_ptr<SubView> _subview) : subview(_subview) {}
    ~MemView() {}

    // [from, to) range
    template<size_t small_batch_threshold=2>
    void doWriteRange(size_t from, size_t to, T* source) {
        size_t from_sub_floor = floorCastIndex<field_type, subfield_type>(from);
        size_t from_sub_ceil = ceilCastIndex<field_type, subfield_type>(from);
        size_t to_sub_floor = floorCastIndex<field_type, subfield_type>(to);
        size_t to_sub_ceil = ceilCastIndex<field_type, subfield_type>(to);
        size_t abs_from = floorCastIndex<field_type, unsigned char>(from);
        size_t abs_to = floorCastIndex<field_type, unsigned char>(to);
        size_t abs_bottom = floorCastIndex<subfield_type, unsigned char>(
            from_sub_floor);
        size_t abs_top = floorCastIndex<subfield_type, unsigned char>(to_sub_ceil);


        // If a batch of records is small enough then we don't mind
        // reading the whole batch and patching it. At the very least
        // this must be 1 as the entire write section may fit in a
        // single sub field. To write over a subfield border in one go
        // (read the adjacent subfields together, patch them and
        // rewrite them instead of patching them one by one) this
        // should be 2.
        if (to_sub_ceil - from_sub_floor < small_batch_threshold) {
            unsigned char small_batch[sizeof(subfield_type) * small_batch_threshold];
            // Read the small batch of records
            subview->readRange(from_sub_floor, to_sub_ceil,
                               (subfield_type*)&small_batch);
            // Patch it
            ::memcpy((small_batch + (abs_from - abs_bottom)),
                     source, abs_to - abs_from);
            // Write the record
            subview->writeRange(from_sub_floor, to_sub_ceil,
                                (subfield_type*)&small_batch);
            return;
        }

        // The part spilling in the first subfield
        if (from_sub_floor != from_sub_ceil) {
            unsigned char first_subfield[sizeof(subfield_type)];
            // Read the first record
            subview->readRange(from_sub_floor, from_sub_ceil,
                               (subfield_type*)&first_subfield);
            // Patch its tail.
            ::memcpy((&first_subfield + (abs_bottom - abs_from)),
                     source,
                     sizeof(subfield_type) - (abs_bottom - abs_from));
            // Write it
            subview->writeRange(from_sub_floor, from_sub_ceil,
                                (subfield_type*)&first_subfield);
        }

        // The part snapped to the middle subfields.
        if (from_sub_ceil != to_sub_floor) {
            subview->writeRange(
                from_sub_ceil, to_sub_floor,
                (subfield_type*)((void*)(source + abs_from - abs_top)));
        }

        // The part spilling in the last subfield
        if (to_sub_floor != to_sub_ceil) {
            unsigned char last_subfield[sizeof(subfield_type)];
            subview->readRange(to_sub_floor, to_sub_ceil,
                               (subfield_type*)&last_subfield);
            ::memcpy(&last_subfield, source, abs_top - abs_to);
            subview->writeRange(to_sub_floor, to_sub_ceil, (subfield_type*)&last_subfield);
        }
    }

    void doReadRange(size_t from, size_t to, T* dest) {
        size_t abs_from = floorCastIndex<field_type, unsigned char>(from);
        size_t abs_to = floorCastIndex<field_type, unsigned char>(to);
        size_t from_sub_floor = floorCastIndex<field_type, subfield_type>(from);
        size_t to_sub_ceil = ceilCastIndex<field_type, subfield_type>(to);

        std::vector<subfield_type> buf(to_sub_ceil - from_sub_floor);
        subview->readRange(from_sub_floor, to_sub_ceil, buf.data());
        ::memcpy(dest, buf.data(), abs_to - abs_from);
    }

    virtual void readRange(size_t from, size_t to, T* dest) {
        doReadRange(from, to, dest);
    }

    virtual void writeRange(size_t from, size_t to, T* source) {
        doWriteRange(from, to, source);
    }
};

// A memview that keeps it's own view of a subsection of the data and
// should read/write to that subsection for free.
template<size_t cache_size, typename T, typename SubView=GenericFile>
class LRUCache : public MemView<T, SubView> {
 private:
    struct CacheItem {
        size_t off;
        // XXX: maybe copying T around is too expenve. We might want
        // to keep around references.
        std::vector<T> data;
        bool dirty;
        bool  operator<(const CacheItem & it) {return off < it.off;}
    };
    std::deque<CacheItem> cache;

    MemView<T, SubView> __daclare_type;

 public:
    // Inherit constructors
    using MemView<T, SubView>::MemView;
    using MemView<T, SubView>::doWriteRange;
    using MemView<T, SubView>::doReadRange;

    // Try to evict len records. Return how many we managed to
    // evict. In case we can evict part of a range, Evict elements
    // from the beginning of a range.
    size_t evictFields(size_t len, size_t skipFrom, size_t skipTo) {
        if (cache.empty()) return 0;
        size_t remaining = len;
        std::vector<CacheItem> tmp;
        CacheItem el = cache.front();
        while (el.data.size() >= len) {
            // Put elements in the protected range in a temporary storage.
            if (el.off >= skipFrom && el.off < skipTo) {
                tmp.push_back(el);
                cache.pop_front();
                continue;
            }

            if (el.dirty) {
                // XXX: line a few of them up before flushing them to
                // avoid the lower layers making unnecessary reads.
                doWriteRange(el.off, el.off + el.data.size(), el.data.data());
            }

            remaining -= el.data.size();
            if (cache.empty()) return len - remaining;
            cache.pop_front();
            el = cache.front();
        }
        el.off += remaining;
        ::memmove(el.data.data(), (void*)(el.data.data() + remaining),
                  el.data.size() - remaining);
        el.data.resize(el.data.size() - remaining);

        // Put back the elements we should have skipped.
        for (auto i : tmp) cache.push_back(i);
        return 0;
    }

    void writeRange(size_t from, size_t to, T* source) {
        // First write on the cache elements.
        queryRange(from, to, source, ReadCacheHandle(this));
    }

    void readRange(size_t from, size_t to, T* dest) {
        queryRange(from, to, dest, WriteCacheHandle(this));
    }

    struct CacheHandle {
        static void onCacheHit(T* inBuf, CacheItem& item, size_t itemFrom,
                               size_t itemTo) {};
        static void onCacheMiss(size_t from, size_t to, T* buf) {};
        static CacheItem* onStoreFieldRange(
            size_t from, size_t to, const T* fields,
            std::deque<CacheItem> &cache) {};
    };

    struct WriteCacheHandle : public CacheHandle {
        MemView<T, SubView> *self;
        WriteCacheHandle(MemView<T, SubView>* _self) : self(_self) {}

        static void onCacheHit(T* inBuf, CacheItem& item, size_t itemFrom, size_t itemTo) {
            item.dirty = true;
            ::memcpy((void*)(item.data.data() + itemFrom),
                     inBuf,
                     (itemTo - itemFrom) * sizeof(T));
        }

        void onCacheMiss(size_t from, size_t to, T* buf) {
            self.doWriteRange(from, to, buf);
        }

        CacheItem* onStoreFieldRange(size_t from, size_t to, const T* fields,
                                            std::deque<CacheItem> &cache) {
            CacheItem *it = self.addFieldRange(from, to, fields);
            it->dirty = true;
            return it;
        }


    };

    struct ReadCacheHandle : public CacheHandle {
        MemView<T, SubView> * self;
        ReadCacheHandle(MemView<T, SubView> * _self) : self(_self) {}

        static void onCacheHit(T* inBuf, CacheItem& item,
                               size_t itemFrom, size_t itemTo) {
            ::memcpy(inBuf,
                     (void*)(item.data.data() + itemFrom),
                     (itemTo - itemFrom) * sizeof(T));
        }

        void onCacheMiss(size_t from, size_t to, T* buf) {
            doReadRange(from, to, buf);
        }

        CacheItem* onStoreFieldRange(size_t from, size_t to, const T* fields) {
            return self.addFieldRange(from, to, fields);
        }
    };

    CacheItem* addFieldRange(size_t from, size_t to, const T* fields) {
        cache.push_back(CacheItem());
        CacheItem& item = cache.front();
        item.off = from;
        item.data.resize(to - from);
        ::memcpy((void*)item.data.data(), (void*)fields, (to - from) * sizeof(T));
        return &item;
    }

    // TODO: Template to replace `memcpy` calls with calls to
    // `onCachedRange` and `doReadRange` with `onCacheMiss`. This way
    // we can convince the compiler to generat code for reading
    // (memcpy, onCachedRead) and for writing (makeDirty, makeDirty
    // . doReadRange). For writing we only want to dirty and let the
    // eviction flush the fields because:
    //
    // - the fields may already be dirty.
    //
    // - we want ot avoid writing, especially sparse writes, beacuse
    // some writes involve a read and a write.
    //
    void queryRange(size_t from, size_t to, T* dest, CacheHandle ch) {
        size_t tmp_from = from;
        size_t tmp_to = to;
        size_t interm_field_num = 0;
        T* tmp_dest = dest;
        std::vector<CacheItem*> initial_pass(cache.size());
        for (int i = 0; i < cache.size(); i++) *(initial_pass.at(i)) = cache.at(i);
        std::sort(initial_pass.begin(), initial_pass.end(), ptr_less<CacheItem>);
        std::priority_queue<CacheItem*> next_pass;

        // Try to make some use of each item.
        for (auto item : initial_pass) {
            // If we covered the whole query.
            if (tmp_from >= tmp_to) return;

            // Do nothing if there is no overlap;
            if (item->off < item->data.size() <tmp_from || item->off >= tmp_to)
                continue;

            // The query is a subset of cached item
            if (item->off <= tmp_from
                && item->data.size() + item->off >= tmp_to) {
                ch.onCacheHit(dest, *item, item->off - tmp_from , tmp_to - tmp_from);
                return;
            }

            // The item reduces the range from the start
            if (tmp_from >= item->off && tmp_from < item->off + item->data.size()) {
                ch.onCacheHit(tmp_dest, *item, item->off - item->off, item->data.size());
                tmp_dest += item->off + item->data.size() - tmp_from;
                tmp_from = item->off + item->data.size();
                continue;
            }

            // The item reduces the rangetmp_from the start
            if (tmp_from >= item->off && tmp_from < item->off + item->data.size()) {
                ch.onCacheHit(tmp_dest + item->off - tmp_from, *item,
                                        0, tmp_to - item->off);
                tmp_to = item->off;
                continue;
            }

            // The item is in the middle of the query. Store it for
            // the next pass.
            next_pass.push(item);
            interm_field_num += item->data.size();
        }

        // Maybe the last iteration covered the last part of the query.
        if (tmp_from >= tmp_to) return;
        size_t tmp_query_field_num = interm_field_num - tmp_from - tmp_to;
        // Number of fields that answer the query but won't fit in the
        // cache.
        //
        // XXX: Change the above so that we don't evict fields that we
        // used at the end of the query.
        size_t nocache_field_num = tmp_query_field_num -
                                   evictFields(tmp_query_field_num, tmp_to, to);

        // Copy all the remaining sections in the destination.
        while (!next_pass.empty()) {
            CacheItem* item = next_pass.top();

            // Do a real read
            ch.onCacheMiss(tmp_from, item->off, tmp_dest);

            // Maybe save an item. We assume that after onCacheMiss
            // the expected state of address space is the state of the
            // fields.
            size_t fields_to_save = tmp_from - item->off;
            if (nocache_field_num >= fields_to_save){
                nocache_field_num -= fields_to_save;
            } else {
                // Save an item
                ch.onStoreFieldRange(tmp_from + nocache_field_num,
                                               fields_to_save - nocache_field_num,
                                               tmp_dest, cache);
                nocache_field_num = 0;
            }

            // Update the cursor
            tmp_dest += item->off - tmp_from + item->data.size();
            tmp_from += item->off - tmp_from + item->data.size();

            // Copy the cached data
            ch.onCacheHit(tmp_dest, *item, 0, item->data.size());
            tmp_dest += item->data.size();
            tmp_from += item->data.size();
            next_pass.pop();
        }
    }
};

template<typename T, typename MemV>
class InMemIterator;
template<typename T, typename MemV>
class OutMemIterator;

template<typename T, typename MemV>
class GenericReader {
    MemV memview;
    friend class InMemIterator<T, MemV>;
 public:
    GenericReader() {}
    GenericReader(const MemV& _memview) : memview(_memview) {}
    InMemIterator<T, MemV> begin() {return InMemIterator<T, MemV>(*this, 0);}
    InMemIterator<T, MemV> end() {
        return InMemIterator<T, MemV>(*this, memview.template size<T>());
    }
};


template<typename T, typename MemV>
class GenericWriter {
    MemV memview;
    friend class OutMemIterator<T, MemV>;
 public:
    GenericWriter() {}
    GenericWriter(const MemV& _memview) : memview(_memview) {}
    OutMemIterator<T, MemV> begin() {return OutMemIterator<T, MemV>(*this, 0);}
    OutMemIterator<T, MemV> end() {
        return OutMemIterator<T, MemV>(*this, memview.template size<T>());
    }
};



template<typename T, typename MemV>
class OutMemIterator {
 private:
    size_t index;
    GenericWriter<T, MemV> &reader;
 public:
    OutMemIterator(GenericReader<T, MemV> &_reader, size_t _index) :
            index(_index), reader(_reader) {};
    OutMemIterator operator*() {
        return *this;
    }
    void operator=(const T& val) {reader.writeRange(index, index+1, &val);}
    void operator++() {index++;}
    bool operator!=(const OutMemIterator<T, MemV> & it) {
        return it.index == index && &reader == &it.reader;
    }
};

template<typename T, typename MemV>
class InMemIterator {
 private:
    size_t index;
    GenericReader<T, MemV> &reader;
 public:
    InMemIterator(GenericReader<T, MemV> &_reader, size_t _index) :
            index(_index), reader(_reader) {};
    T operator*() {
        T ret;
        reader.readRange(index, index+1, ret);
        return ret;
    }
    void operator++() {index++;}
    bool operator!=(const InMemIterator<T, MemV> & it) {
        return it.index == index && &reader == &it.reader;
    }
};
