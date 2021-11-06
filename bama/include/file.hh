#ifndef __FILE_HH__
#define __FILE_HH__

#include "defs.hh"
#include "require.hh"
#include "book.hh"
#include "page.hh"
#include <sys/types.h>
#include <iostream>
#include <string>
#include <fcntl.h>	// O_RDWR, O_CREAT
#include <cmath>        // ceil
#include <cstdlib>      // posix_memalign
#include <cstring>      // memset
#include <cstdio>
#include <functional>


#define READ_FLAGS O_RDONLY
#ifdef __linux
#define WRITE_FLAGS (O_DIRECT | O_RDWR | O_CREAT | O_TRUNC)
#else
#define WRITE_FLAGS (O_RDWR | O_CREAT | O_TRUNC)
#endif


#define RW_FLAGS O_RDWR

template <typename R>
class File;
template <typename R>
class ReaderIterator;

// Log the writes skipping.
template<typename T>
class Logger {
 private:
    size_t last_offset;
    const static T null_rec;
 public:
    Logger() : last_offset(0) {}
    ~Logger() {}
    void operator()(const File<T>* file, const Page<T>& p, size_t offset) {
        int skip = offset - last_offset;
        bool is_null = isNull(p);
        last_offset = offset;
        if (p.capacity() != p.length() || skip != 1 || is_null) {
            std::cout << "[" << file->filename << "](skip: " << skip
                      << ") Page write => " << (is_null ? "(null page) " : "")
                      << "offset: " << offset << ", capacity: " << p.capacity()
                      << ", length: " << p.length() << std::endl;
        }
    }

    bool isNull(const Page<T>& p) {
        for (auto c : p)
            if (c != null_rec) return false;
        return true;
    }
};

template<typename T>
const T Logger<T>::null_rec;

template <typename R>
class File {

    friend class Logger<R>;
 private:
    std::string filename;
    int descriptor;
    Logger<R> logger;
    bool is_open;


    // And clamp page number
    page_num_t seekToOff(page_num_t off, page_num_t page_num) {
        size_t max_pages = numPages();
        require_lt(off, max_pages, "Too large offset for reading.");
        if (off + page_num > max_pages)
            page_num = max_pages-off;
        require(::lseek(descriptor, PAGE_SIZE * off, SEEK_SET) != -1,
                "Could not seek in file for read. (bulkRead())");
        return page_num;
    }

    void readPages(Page<R>* pages, page_num_t page_num) {
        byte_num_t bytes_read = ::read(descriptor, pages, PAGE_SIZE*page_num);
        require_neq(bytes_read, -1,
                    "Could not perform bulk read "
                    + std::to_string(descriptor) + ", " + std::to_string(page_num));
        require_eq(bytes_read, PAGE_SIZE*page_num,
                   "Didn't read the right number of bytes.");
        // delay(read_delay * (page_num*PAGE_SIZE / cacheline_size));
        increment_reads(page_num * PAGE_SIZE / cacheline_size);
    }


 public:
    typedef Page<R> page_type;
    typedef typename page_type::record_type record_type;

    File():
            filename(""), descriptor(-1), logger(), is_open(false) {}
    File(const std::string& fn, int flags=READ_FLAGS):
            logger(), filename(fn), descriptor(0), is_open(false)
    {
        open(filename, flags);
    }
    ~File() { if (is_open) close(); }


    void close() {
        if (!is_open) return;
        require(::close(descriptor) != -1, "Could not close file ( fd="
                + std::to_string(descriptor) + ")");
        is_open = false;
    }

    void open(const std::string& fn, int flags=READ_FLAGS) {
        static_assert(PAGE_SIZE == sizeof(Page<R>));
        require(! is_open, "file already open: " + fn);
        filename = fn;
        require_neq(
            (descriptor = ::open(filename.c_str(), flags, 0644)), -1,
            "could not open file: '" + filename
            + "' (flags=" + std::to_string(flags) +")");
        is_open = (descriptor != -1);
    }

    void readPage(page_num_t o, Page<R>* page) {
        require(page, "Allocation failed.");
        require(::lseek(descriptor, PAGE_SIZE*o, SEEK_SET) != -1,
                "Could not seek in file for read. (read())");
        ssize_t bytes_read = ::read(descriptor, page, PAGE_SIZE);
        require_eq(bytes_read, PAGE_SIZE,
                   "Could not read entire page.");
        increment_reads(PAGE_SIZE / cacheline_size);
    }

    page_type* readPage(page_num_t o) {
        page_type* page = (page_type*) aligned_new(PAGE_SIZE);
        readPage(o, page);
        return page;
    }

    void writePage(page_num_t o, const page_type& p) {
        require(is_open, "File not open (" + filename +")");
        require(::lseek(descriptor, PAGE_SIZE*o, SEEK_SET) != -1,
                "Could not seek in file for write (" + filename + ").");
        ssize_t bytes_written =
                ::write(descriptor, (void*) &p, PAGE_SIZE);
        require(bytes_written != -1 && (byte_num_t) bytes_written == PAGE_SIZE,
                "Could not write page contents.");
        increment_writes(PAGE_SIZE / cacheline_size);
    }

    page_num_t bulkRead(page_num_t off, page_type* pages, page_num_t page_num) {
        require_lt(0, page_num, "We would read no pages.");
        size_t true_page_num = seekToOff(off, page_num);
        require_lt(0, true_page_num, "We would read no pages.");
        readPages(pages, true_page_num);
        return true_page_num;
    }

    Page<R>* bulkRead(page_num_t off, page_num_t& page_num) {
        seekToOff(off, page_num);
        page_type* pages = (page_type*) aligned_new(PAGE_SIZE*page_num);
        require(pages, "Allocation failed.");
        page_num = readPages(pages, page_num);
        return pages;
    }

    void bulkWrite(page_num_t off, const page_type* pgs, page_num_t page_num) {
        require(::lseek(descriptor, PAGE_SIZE * off, SEEK_SET) != -1,
                "Could not seek in file for write.");
        ssize_t bytes_written = ::write(descriptor, pgs, PAGE_SIZE*page_num);
        require(bytes_written != -1
                && (size_t) bytes_written == PAGE_SIZE*page_num,
                "Could not perform bulk write.");
        increment_writes(page_num * PAGE_SIZE / cacheline_size);
    }

    page_num_t numPages() const {
        return ::lseek(descriptor, 0, SEEK_END)/PAGE_SIZE;
    }
    byte_num_t numBytes() const { return ::lseek(descriptor, 0, SEEK_END); }
    bool isOpen() const { return is_open; }
    const std::string getFilename () const {return filename;}
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
    const std::string& getFilename() const {
        return filename;
    }

    Reader(): filename(""), page(0), num_pages(0), current_page(0),
              record_index(0), file(), marked_page(0), marked_record(0) {}

    Reader(const std::string& fn): filename(fn), page(0),
                                   num_pages(0), current_page(0),
                                   record_index(0), file(),
                                   marked_page(0), marked_record(0) {
        open(filename);
    }
    ~Reader() {
        if (file.isOpen()) close();
        if (page) aligned_delete((unsigned char*) page);
    }

    void open(const std::string& fn) {
        filename = fn;
        file.open(fn, READ_FLAGS);
        num_pages = file.numPages();
        if (num_pages) {
            page = file.readPage(0);
            current_page = 0;
            record_index = 0;
        }
        else {
            file.close();
            file.open(fn, WRITE_FLAGS);
            std::cerr << "File "
                      << fn
                      << " is empty! Making a blank page."
                      << std::endl;
            page = (page_type*) aligned_new(PAGE_SIZE);
            require(page, "Allocation failed.");
            // This is because we don't handle not being on a page.
            file.writePage(0, *page);
            file.close();
            file.open(fn, READ_FLAGS);
            //aligned_delete(page);
            num_pages = 1;
            current_page = 0;
            record_index = 0;
        }
    }

    bool hasNext() {
        if (record_index < page->length()) {
            return true;
        }
        if (current_page+1 < num_pages) {
            // page = 0;
            current_page++;
            record_index = 0;
            file.readPage(current_page, page);
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
        page = file.readPage(current_page);
        record_index = marked_record;
    }

    bool skip(rec_num_t offset) {
        if (current_page == num_pages-1 && record_index == page->length())
            return false;
        if (record_index + offset < page->length()) {
            record_index += offset;
            return true;
        }

        offset -= (page->length() - record_index);
        size_t npgs = (page_num_t) ::ceil((double) offset / page->capacity());
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

    const record_type& nextRecord() {
        require_lt(record_index, page->length(), "You are going too far");
        return page->get(record_index++);
    }

    void close() { file.close(); }
    bool isOpen() const { return file.isOpen(); }
    page_num_t numPages() const { return file.numPages(); }
    rec_num_t recordsCapacity() const {
        return file.numPages() * Page<R>::allocation;
    }
}; //:~ class Reader

template <typename R>
class Writer {
 private:
    std::string filename;
    File<R> file;
    Page<R>* page;
    page_num_t num_pages;

 public:
    Writer(): filename(), file(), page(0), num_pages(0) {}

    Writer(const std::string& fn)
            : filename(fn), file(), page(0), num_pages(0) {
        open(fn);
    }
    ~Writer() {
        if (file.isOpen()) close();
        if (page) aligned_delete((unsigned char*)page);
    }

    void open(const std::string& fn) {
        num_pages = 0;
        filename = fn;
        file.open(filename, WRITE_FLAGS);
        num_pages = file.numPages();
        // the file has pages
        page = new Page<R>();
        require(page, "Allocation failed.");
        file.writePage(0, *page);
        num_pages = 1;
    }

    void write(const R& r) {
        require(file.isOpen(), "Writer not open (" + filename + ")");
        // the page has no room so flush and clear
        if (page->length() == page->capacity()) {
            file.writePage(num_pages-1, *page);
            page->clear();
            num_pages++;
        }
        // add the record
        page->add(r);
    }
    std::string get_filename() const { return filename; }
    void close() {
      flush();
      file.close();
    }
    void flush() { if (page) file.writePage(num_pages-1, *page); }
    bool isOpen() const { return file.isOpen(); }
    page_num_t numPages() const { return file.numPages(); }
}; //:~ class Writer


template <typename R>
class BulkProcessor {

 protected:
    std::string filename;
    File<R> file;
    size_t num_pages;

    virtual void internal_open() = 0;

 public:
    BulkProcessor(): filename(""), file(), num_pages(0) {
    }

    ~BulkProcessor() {
        if (isOpen()) close();
    }


    void open(const std::string& fn) {
        filename = fn;
        internal_open();
        // read in the last page of the file -- or materialize the
        // file if the file is empty
        num_pages = file.numPages();
        // the file has pages
        if (!num_pages) {
            char page_data[sizeof(Page<R>)];
            ::memset(page_data, 0, sizeof(Page<R>));
            file.writePage(0, *((Page<R>*)page_data));
            num_pages = 1;
        }
    }

    void close() { file.close(); }
    bool isOpen() const { return file.isOpen(); }
    size_t numPages() const { return file.numPages(); }
};


template <typename R>
class BulkWriter : public BulkProcessor<R> {
 protected:
    void internal_open() override {
        this->file.open(this->filename, WRITE_FLAGS);
    }
 public:
    using BulkProcessor<R>::BulkProcessor;
    // This destroys recs.
    void bulkWrite(size_t o, const R* recs, size_t len, bool bpa = true) {
        const R* nrecs = (! bpa) ? (R*)allocatePages<R>(len) : recs;
        size_t np = expand(nrecs, len);
        this->file.bulkWrite(o, (Page<R>*) nrecs, np);
        if (! bpa) aligned_delete((unsigned char*) nrecs);
    }
    ~BulkWriter() {
        this->file.close();
    }

};

template <typename R>
class BulkReader : public BulkProcessor<R> {
 protected:
    size_t file_pages = 0;
    void internal_open() override {
        this->file.open(this->filename, READ_FLAGS);
        file_pages = this->file.numPages();
    }


 public:
    ~BulkReader() {
        this->file.close();
        file_pages = 0;
    }

    inline bool hasNext(size_t off) {
        return off < file_pages;
    }

    using BulkProcessor<R>::BulkProcessor;
    R* bulkRead(rec_num_t rec_off, rec_num_t req_rec_num, rec_num_t& out_rec_num) {
        page_num_t out_page_num = recordToPageNum<R>(req_rec_num);
        page_num_t page_off = recordToPageOff<R>(rec_off);
        Page<R>* pages = this->file.bulkRead(page_off, out_page_num);
        out_rec_num = compact(pages, out_page_num);
        return (R*)pages;
    }

    // Read as many pages as possible, compress (remove the headers)
    // releasing some space, bulk read into the released space until
    // there is one page left to read allocate it and move it's
    // contents to the space provided.
    rec_num_t bulkRead(rec_num_t rec_off, R* recs, const rec_num_t req_rec_num) {
        // Read the main part
        page_num_t page_off = recordToPageOff<R>(rec_off);
        page_num_t page_to = recordToPageNum<R>(rec_off + req_rec_num);
        require_le(page_off, page_to, "Page offsets are screwed up");
        page_num_t req_page_num = page_to - page_off;

        rec_num_t skip_recs = rec_off - firstRecIndex<R>(page_off);

        // remember that there's padding at the end of each page.
        size_t extra_read_size = req_page_num * sizeof(Page<R>)
                                 - req_rec_num * sizeof(R);
        page_num_t non_bulk_pages = ::ceil(float(extra_read_size)
                                           / sizeof(Page<R>));
        require_le((req_page_num - non_bulk_pages) * sizeof(Page<R>),
                   req_rec_num * sizeof(R),
                   "Reading more than allocated by the caller.");
        rec_num_t bulk_record_num = 0;

        // Fill the provided space with pages and compress.
        if (req_page_num - non_bulk_pages > 0) {
            page_num_t read_page_num = this->file.bulkRead(
                page_off,
                (Page<R>*)recs,
                req_page_num - non_bulk_pages);

            rec_num_t bulk_record_num = compact((Page<R>*)recs,
                                                read_page_num,
                                                skip_recs);
            // Read as many more bulk pages as possible.
            if (non_bulk_pages > 2) {
                return bulk_record_num
                        + this->bulkRead(rec_off + bulk_record_num,
                                         recs + bulk_record_num,
                                         req_rec_num - bulk_record_num);
            }
        }

        // Now we don't have any more space for pages but we need more
        // records. Read a page and copy the records back. Unless
        // there is no page to be read.
        if (this->file.numPages() <=
            recordToPageNum<R>(rec_off + bulk_record_num)) {
            return bulk_record_num;
        }
        uint8_t final_page_data[sizeof(Page<R>)];
        Page<R>* final_page = (Page<R>*)final_page_data;
        this->file.readPage(recordToPageNum<R>(rec_off + bulk_record_num),
                            final_page);
        return bulk_record_num +
                final_page->getAll(
                    recs + bulk_record_num,
                    req_rec_num - bulk_record_num);
    }
};

// Fn = std::function<void(const R&)>
template<typename R,typename Fn>
void eachRecord(const std::string& inpFile, Fn f) {
    Reader<R> reader;
    size_t i = 0;
    reader.open(inpFile);
    while (reader.hasNext()) {
        i++;
        f(reader.nextRecord());
    }
    reader.close();
}

template<typename R>
void eachRecordBulk(size_t len, const std::string& inpFile,
                    std::function<void(R*, size_t)> callback) {
    BulkReader<R> reader;
    size_t off = 0;
    const size_t bufSize = recordToPageNum<R>(len);
    require_lt(0, bufSize, "Empty buffer size");
    reader.open(inpFile);
    require_lt(bufSize, 1000, "Suspiciously large buffer size");
    std::vector<R> buf(len);
    while ((len = reader.bulkRead(off, buf.data(), bufSize))) {
        callback(buf.data(), len);
        off += len;
    }
    reader.close();
}

template<typename R>
void bulkWrite(size_t len, const std::string& inpFile, const R* recs)
{
    BulkWriter<R> wr;
    wr.open(inpFile);
    wr.bulkWrite(0, recs, len);
    wr.close();
}
#endif
