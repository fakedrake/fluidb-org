// Include this only though io.hh
#ifndef __VECFILE_HH__
#define __VECFILE_HH__

#ifndef VECTOR
#error "Only valid for VECTOR."
#endif

#include "defs.hh"
#include "require.hh"
#include "page.hh"
#include "book.hh"
#include "util.hh"
#include <fstream>
#include <string>
#include <fcntl.h>	// O_RDWR, O_CREAT
#include <cmath>        // ceil
#include <cstdlib>      // posix_memalign
#include <cstring>      // memset
#include <map>
#include <vector>
#include <cstdio>
#include <cerrno>

template <typename R>
class File {
private:
    std::string filename;
    bool is_open;
    fs::page_info* pages;

public:
    typedef Page<R> page_type;
    typedef typename page_type::record_type record_type;

    File()
        : filename(""), is_open(false), pages(0) {}
    File(const std::string& fn, bool writer=false)
        : filename(fn), is_open(false), pages(0) {
        open(filename, writer);
    }
    ~File() { if (is_open) close(); }


    void open(const std::string& fn, bool write=false) {
        require(! is_open, "file already open.");
        filename = fn;
        std::map<std::string, fs::page_info*>::iterator data =
            fs::file_catalog.find(filename);
        if (data != fs::file_catalog.end()) {
            pages = data->second;
        }
        else {
            int descriptor;
            int flags = write ? O_CREAT | O_WRONLY | O_TRUNC : O_RDONLY;
            require_neq((descriptor = ::open(filename.c_str(), flags, 0644)),
                        -1,
                        "Couldn't open file:" + filename);
            pages = new fs::page_info;
            unsigned char* buffer = aligned_new(get_pagesize());
            require_neq(::lseek(descriptor, 0, SEEK_SET), -1,
                    "could not seek in file for read. (read())");
            for (size_t i = 0; i < numPages(descriptor); i++) {
                require_neq(::lseek(descriptor, get_pagesize()*i, SEEK_SET), -1,
                            "could not seek in file for read. (read())");
                ssize_t bytes_read = ::read(descriptor, buffer, get_pagesize());
                require(bytes_read != -1
                        && (size_t) bytes_read == get_pagesize(),
                        "Could not read page contents.");
                add_page(buffer);
            }
            aligned_delete(buffer);
            fs::file_catalog.insert(std::make_pair(filename, pages));
            require(::close(descriptor) != -1, "could not close backing file.");
        }
        is_open = true;
    }

    void close() {
        require(is_open, "file not open.");
        is_open = false;
    }

    page_type* readPage(size_t o) {
        require(o < pages->used_pages,
                "request to read from non-existent page");
        page_type* page = (page_type*) aligned_new(get_pagesize());
        ::memcpy(page, get_page(o), get_pagesize());
        delay(read_delay * (get_pagesize() / cacheline_size));
        increment_reads(get_pagesize() / cacheline_size);
        return page;
    }

    void writePage(size_t o, const page_type& p) {
        if (o < numPages()) {
            ::memcpy(get_page(o), &p, get_pagesize());
        }
        else if (o == numPages()) {
            add_page((unsigned char*) &p);
        }
        else {
            unsigned char* buffer = 0;
            for (size_t i = numPages(); i < o-1; i++) {
                buffer = aligned_new(get_pagesize());
                add_page(buffer);
            }
            if (buffer) { aligned_delete(buffer); }
            add_page((unsigned char*) &p);
        }
        delay(write_delay * (get_pagesize() / cacheline_size));
        increment_writes(get_pagesize() / cacheline_size);
    }

    void bulkRead(size_t o, page_type* pgs, size_t& ps) {
        size_t np = numPages();
        if (o == np) { ps = 0; return; }
        require(o < np, "Could not seek in file for read. (bulkRead())");
        if ((size_t) o + ps > (size_t) np) ps = np-o;
        for (size_t i = 0; i < ps; i++) {
            ::memcpy(&pgs[i], get_page(o + i), get_pagesize());
        }
        delay(read_delay * (ps*get_pagesize() / cacheline_size));
        increment_reads(ps * get_pagesize() / cacheline_size);
    }

    page_type* bulkRead(size_t o, size_t& ps) {
        size_t np = numPages();
        if (o == np) { ps = 0; return 0; }
        require(o < np, "Could not seek in file for read. (bulkRead())");
        if ((size_t) o + ps > (size_t) np) ps = np-o;
        page_type* pgs = (page_type*) aligned_new(get_pagesize()*ps);
        for (size_t i = 0; i < ps; i++) {
            ::memcpy(&pgs[i], get_page(o + i), get_pagesize());
        }
        delay(read_delay * (ps*get_pagesize() / cacheline_size));
        increment_reads(ps * get_pagesize() / cacheline_size);
        return pgs;
    }

    void bulkWrite(size_t o, page_type* pgs, size_t ps) {
        unsigned char* buffer;
        if (o > numPages()) {
            for (size_t i = numPages(); i < o; i++) {
                buffer = aligned_new(get_pagesize());
                add_page(buffer);
            }
        }
        for (size_t i = 0; i < ps; i++) {
            if (o + i == numPages()) {
                buffer = aligned_new(get_pagesize());
                add_page(buffer);
            }
            ::memcpy(get_page(o + i), &pgs[i], get_pagesize());
        }
        delay(write_delay * (ps*get_pagesize() / cacheline_size));
        increment_writes(ps * get_pagesize() / cacheline_size);
    }

    size_t numPages(int descriptor = -1) const {
        // hack hack hack
        return (descriptor == -1
                ? pages->used_pages
                : ::lseek(descriptor, 0, SEEK_END)/get_pagesize());
    }

    size_t numBytes(int descriptor = -1) const {
        // hack hack hack
        return (descriptor == -1
                ? pages->used_pages * get_pagesize()
                : ::lseek(descriptor, 0, SEEK_END));
    }

    bool isOpen() const { return is_open; }

private:
    size_t page_offset(size_t o) const { return o * get_pagesize(); }

    void add_page(const unsigned char* p) {
        if (pages->used_pages == pages->allocated_pages) {
            size_t alloc = (pages->allocated_pages == 0
                            ? 1 : 2*pages->allocated_pages);
            unsigned char* tmp = aligned_new(alloc * get_pagesize());
            if (pages->allocated_pages != 0) {
                ::memcpy(tmp, pages->data,
                         pages->allocated_pages*get_pagesize());
                aligned_delete(pages->data);
            }
            delay(write_delay * alloc * (get_pagesize() / cacheline_size));
            increment_writes(alloc * get_pagesize() / cacheline_size);
            pages->allocated_pages = alloc;
            pages->data = tmp;
        }
        ::memcpy(get_page(pages->used_pages), p, get_pagesize());
        delay(write_delay * (get_pagesize() / cacheline_size));
        increment_writes(get_pagesize() / cacheline_size);
        pages->used_pages++;
    }

    void *get_page(size_t o) const {
        return &((pages->data)[page_offset(o)]);
    }
}; //:~ class File


template <typename R>
class Reader {
public:
    typedef R record_type;
    typedef Page<R> page_type;

private:
    typedef File<R> file_type;

    std::string filename;
    page_type* page;
    size_t num_pages;
    size_t current_page;
    size_t record_index;
    file_type file;
    size_t marked_page;
    size_t marked_record;

public:
    Reader(): filename(""), page(0), num_pages(0), current_page(0),
              record_index(0), file(), marked_page(0), marked_record(0) {}

    Reader(const std::string& fn): filename(fn), page(0),
                                   num_pages(0), current_page(0),
                                   record_index(0), file(),
                                   marked_page(0), marked_record(0) {
        open(filename);
    }
    ~Reader() { if (file.isOpen()) close(); if (page) delete page; }

    void open(const std::string& fn) {
        filename = fn;
        file.open(fn);
        num_pages = file.numPages();
        if (num_pages) { page = file.readPage(0); }
        else { page = (page_type*) aligned_new(get_pagesize()); }
        current_page = 0;
        record_index = 0;
    }

    bool hasNext() {
        if (record_index < page->length()) {
            return true;
        }
        if (current_page+1 < num_pages) {
            if (page) aligned_delete((unsigned char*) page);
            page = 0;
            current_page++;
            record_index = 0;
            page = file.readPage(current_page);
            return true;
        }
        return false;
    }

    void mark() {
        marked_page = current_page;
        marked_record = record_index;
    }

    void rollback() {
        if (page) aligned_delete((unsigned char*) page);
        current_page = marked_page;
        page = file.read(current_page);
        record_index = marked_record;
    }

    bool skip(size_t offset) {
        if (current_page == num_pages-1 && record_index == page->length())
            return false;
        if (record_index + offset < page->length()) {
            record_index += offset;
            return true;
        }

        offset -= (page->length() - record_index);
        size_t npgs = (size_t) ::ceil((double) offset / page->capacity());
        record_index = offset - (npgs-1)*page->capacity();
        if (current_page + npgs < num_pages) {
            aligned_delete((unsigned char*) page);
            current_page += npgs;
            page = file.readPage(current_page);
            return true;
        }
        else {
            aligned_delete((unsigned char*) page);
            current_page = num_pages-1;
            page = file.readPage(current_page);
            record_index = page->length()-1;
            return false;
        }
    }

    record_type nextRecord() { return page->get(record_index++); }

    void close() { file.close(); }
    bool isOpen() const { return file.isOpen(); }
    size_t numPages() const { return file.numPages(); }
}; //:~ class Reader


template <typename R>
class Writer {
public:
    typedef R record_type;
    typedef Page<R> page_type;

private:
    std::string filename;
    File<R> file;
    page_type* page;
    size_t num_pages;

public:
    Writer(): filename(""), file(), page(0), num_pages(0) {}

    Writer(const std::string& fn)
        : filename(fn), file(), page(0), num_pages(0) {
        open(filename);
    }
    ~Writer() {
      if (file.isOpen()) close();
      if (page) aligned_delete((unsigned char*) page);
    }

    void open(const std::string& fn) {
        filename = fn;
        file.open(filename, true);
        // read in the last page of the file -- or materialize the
        // file if the file is empty
        num_pages = file.numPages();
        // the file has pages
        if (num_pages) {
            page = file.readPage(num_pages-1);
            num_pages--;
        }
        else {
            num_pages = 0;
        }
    }

    void write(const record_type& r) {
        // the page has no room so flush and clear
        if (! page) { page = (page_type*) aligned_new(get_pagesize()); }
        //std::cout << "lala" << std::endl;
        if (page->length() == page->capacity()) {
            file.writePage(num_pages, *page);
            page->clear();
            num_pages++;
        }
        // add the record
        page->add(r);
    }

    void close() { flush(); file.close(); }
    void flush() { if (page) { file.writePage(num_pages, *page); } }
    bool isOpen() const { return file.isOpen(); }
    size_t numPages() const { return file.numPages(); }
}; //:~ class Writer


template <typename R, size_t S = 4096>
class BulkProcessor {
public:
    typedef R record_type;
    typedef Page<R> page_type;

private:
    typedef File<R> file_type;

    std::string filename;
    file_type file;
    size_t num_pages;

public:
    BulkProcessor(): filename(""), file(), num_pages(0) {}

    BulkProcessor(const std::string& fn)
        : filename(fn), file(), num_pages(0) {
        open(filename);
    }
    ~BulkProcessor() {
      if (isOpen()) close();
    }

    void open(const std::string& fn) {
        filename = fn;
        file.open(filename);
        // read in the last page of the file -- or materialize the
        // file if the file is empty
        num_pages = file.numPages();
        // the file has pages
        if (! num_pages) {
            page_type* page = (page_type*) aligned_new(get_pagesize());
            file.writePage(0, *page);
            num_pages = 1;
            aligned_delete((unsigned char*) page);
        }
    }

    record_type* bulkRead(size_t o, size_t& ps, size_t& len) {
        page_type* pages = file.bulkRead(o, ps);
        record_type* recs = (record_type*) pages;
        //std::cout << "compacting" << std::endl;
        len = compact(recs, ps);
        return recs;
    }

    void bulkRead(size_t o, record_type* recs, size_t& ps, size_t& len) {
        file.bulkRead(o, (page_type*) recs, ps);
        len = compact(recs, ps);
    }

    void bulkWrite(size_t o, record_type* recs, size_t len, bool bpa = true) {
        record_type* nrecs;
        if (! bpa) nrecs = allocate(len);
        else nrecs = recs;
        size_t np = expand(nrecs, len);
        file.bulkWrite(o, (page_type*) nrecs, np);
        if (! bpa) aligned_delete((unsigned char*) nrecs);
    }

    void close() { file.close(); }
    bool isOpen() const { return file.isOpen(); }
    size_t numPages() const { return file.numPages(); }

private:
    size_t compact(record_type* recs, size_t np) const {
        char* p = (char*) recs;
        size_t len = 0;
        size_t nr = 0;
        for (size_t i = 0; i < np; i++) {
            nr = ((page_type*)p)->length();
            ::memmove((char*) (&recs[len]), p + page_type::header_size,
                      sizeof(record_type)*nr);
            len += nr;
            p += get_pagesize();
        }
        return len;
    }

    size_t expand(record_type* recs, size_t len) const {
        size_t np = numPages(len);
        size_t capacity = (get_pagesize()-page_type::header_size) / sizeof(record_type);
        char* p = (char*) recs;
        size_t i = np-1;
        p += (get_pagesize()*i);
        size_t nrecs = (len % capacity == 0 ? capacity : len % capacity);
        len -= nrecs;
        size_t prev_recs = nrecs;
        ::memmove(&p[page_type::header_size], (char*) (&recs[len]),
                  sizeof(record_type)*nrecs);
        i--;
        p -= get_pagesize();
        nrecs = capacity;
        while (i+1 >= 1) {
            len -= nrecs;
            ::memmove(&p[page_type::header_size], (char*) (&recs[len]),
                      sizeof(record_type)*nrecs);
            *((size_t*) (p+get_pagesize())) = prev_recs;
            prev_recs = nrecs;
            p -= get_pagesize();
            i--;
        }
        *((size_t*) (p+get_pagesize())) = prev_recs;
        return np;
    }

    record_type* allocate(size_t len) const {
        return (record_type*) aligned_new(numPages(len));
    }

    size_t numPages(size_t l) const {
        return ((double) l / ((double) (get_pagesize()-page_type::header_size)
                               / sizeof(record_type)) + 0.5);
    }
};


#endif
