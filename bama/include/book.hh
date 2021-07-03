#ifndef __BOOK_HH__
#define __BOOK_HH__

#include "defs.hh"
#include <string>

#ifdef MEMORY
#include <map>
#include <vector>
#elif VECTOR
#include <map>
#endif

#include <fcntl.h>	// O_RDWR, O_CREAT
#include <cstdio>

namespace fs {

#ifdef VECTOR
struct page_info {
    size_t used_pages;
    size_t allocated_pages;
    unsigned char *data;

    page_info(): used_pages(0), allocated_pages(0), data(0) {}
};
#endif

#ifdef MEMORY
extern std::map<std::string, std::vector<unsigned char*>* > file_catalog;
#elif VECTOR
extern std::map<std::string, page_info*> file_catalog;
#endif

void init();
void flush(const std::string &);
void forget(const std::string &);
void remove(const std::string &);
void shutdown();
void rename(const std::string &, const std::string &);

}

#endif
