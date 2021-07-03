#include "require.hh"
#include "book.hh"

#include <cstdio>

namespace fs {

#ifdef MEMORY
std::map<std::string, std::vector<unsigned char*>* > file_catalog;
#elif VECTOR
std::map<std::string, page_info*> file_catalog;
#endif

void init() {
}

void flush(const std::string &filename) {
#ifdef MEMORY
    std::map<std::string, std::vector<unsigned char*>*>::iterator it =
        file_catalog.find(filename);
    if (it != file_catalog.end()) {
        int descriptor;
        require((descriptor = ::open((it->first).c_str(), O_RDWR, 0644)) != -1,
                "could not open file:" + it->first);
        std::vector<unsigned char*>::iterator to_erase;
        size_t i = 0;
        for (std::vector<unsigned char *>::iterator pit = it->second->begin();
             pit != it->second->end(); pit++, i++) {
            require(::lseek(descriptor, get_pagesize()*i, SEEK_SET) != -1,
                    "Could not seek in file for write.");
            ssize_t bytes_written =
                ::write(descriptor, (void*) *pit, get_pagesize());
            require(bytes_written != -1
                    && (size_t) bytes_written == get_pagesize(),
                    "Could not write page contents.");
        }
        require(::close(descriptor) != -1, "Could not close file.");
    }
#elif VECTOR
    std::map<std::string, page_info*>::iterator it =
        file_catalog.find(filename);
    if (it != file_catalog.end()) {
        int descriptor;
        require((descriptor = ::open((it->first).c_str(), O_RDWR, 0644)) != -1,
                "could not open file: " + it->first);
        //std::vector<page_*>::iterator to_erase;
        //size_t i = 0;
        //for (std::vector<unsigned char *>::iterator pit = it->second->begin();
        //     pit != it->second->end(); pit++, i++) {
        for (size_t i = 0; i < it->second->used_pages; i++) {
            require(::lseek(descriptor, get_pagesize()*i, SEEK_SET) != -1,
                    "Could not seek in file for write.");
            ssize_t bytes_written =
                ::write(descriptor, (void*) &((it->second)[get_pagesize()*i]),
                        get_pagesize());
            require(bytes_written != -1
                    && (size_t) bytes_written == get_pagesize(),
                    "Could not write page contents.");
        }
        aligned_delete(it->second->data);
        require(::close(descriptor) != -1, "Could not close file.");
    }
#else
    (void) filename; // remove warning
#endif
}

void forget(const std::string &filename) {
#ifdef MEMORY
    std::map<std::string, std::vector<unsigned char*>*>::iterator it =
        file_catalog.find(filename);
    if (it != file_catalog.end()) {
        while (it->second->size() > 0) {
            aligned_delete((unsigned char*)
                           (*(it->second))[it->second->size()-1]);
            it->second->pop_back();
        }
        file_catalog.erase(it);
    }
#elif VECTOR
    std::map<std::string, page_info*>::iterator it =
        file_catalog.find(filename);
    if (it != file_catalog.end()) {
        aligned_delete(it->second->data);
        file_catalog.erase(it);
    }
#else
    (void) filename; // remove warning
#endif
}

void remove(const std::string &filename) {
#if defined(MEMORY) || defined(VECTOR)
    forget(filename);
#endif
    //std::cout << "removing " << filename << std::endl;
    require(::remove(filename.c_str()) == 0, "could not delete file " + filename);
}

void shutdown() {
#ifdef MEMORY
    std::map<std::string, std::vector<unsigned char*>*>::iterator it;
    std::map<std::string, std::vector<unsigned char*>*>::iterator to_erase_info;
    for (it = file_catalog.begin(); it != file_catalog.end();) {
        int descriptor;
        require((descriptor = ::open((it->first).c_str(), O_RDWR, 0644)) != -1,
                "could not open file: " + it->first);
        // make two passes instead of dealing with compactions
        size_t i = 0;
        for (std::vector<unsigned char *>::iterator pit = it->second->begin();
             pit != it->second->end(); pit++, i++) {
            require(::lseek(descriptor, get_pagesize()*i, SEEK_SET) != -1,
                    "Could not seek in file for write.");
            ssize_t bytes_written =
                ::write(descriptor, (void*) *pit, get_pagesize());
            require(bytes_written != -1
                    && (size_t) bytes_written == get_pagesize(),
                    "Could not write page contents.");
        }
        while (it->second->size() > 0) {
            aligned_delete((unsigned char*)
                           (*(it->second))[it->second->size()-1]);
            it->second->pop_back();
        }
        require(::close(descriptor) != -1, "Could not close file.");
        delete it->second;
        to_erase_info = it;
        it++;
        file_catalog.erase(to_erase_info);
    }
#elif VECTOR
    std::map<std::string, page_info*>::iterator it;
    std::map<std::string, page_info*>::iterator to_erase_info;
    for (it = file_catalog.begin(); it != file_catalog.end();) {
        int descriptor;
        require((descriptor = ::open((it->first).c_str(), O_RDWR, 0644)) != -1,
                "could not open file: " + it->first);
        // make two passes instead of dealing with compactions
        for (size_t i = 0; i < it->second->used_pages; i++) {
            require(::lseek(descriptor, get_pagesize()*i, SEEK_SET) != -1,
                    "Could not seek in file for write.");
            ssize_t bytes_written =
                ::write(descriptor, (void*) &((it->second->data)[get_pagesize()*i]),
                        get_pagesize());
            require(bytes_written != -1
                    && (size_t) bytes_written == get_pagesize(),
                    "Could not write page contents.");
        }
        aligned_delete(it->second->data);
        require(::close(descriptor) != -1, "Could not close file.");
        delete it->second;
        to_erase_info = it;
        it++;
        file_catalog.erase(to_erase_info);
    }
#endif
}


size_t filesize(const std::string& f) {
    FILE *fd = ::fopen(f.c_str(), "rb");
    long size = -1;
    require_eq(::fseek(fd, 0, SEEK_END), 0, "Failed to seek file " + f);
    size = ::ftell(fd);
    ::fclose(fd);
    return size;
}

}
