#ifndef BAMIFY_H
#define BAMIFY_H

#include <fstream>
#include <iostream>
#include <cstdio>
#include "io.hh"

template<typename R, size_t batch_size=2000>
void bama_to_dat(const std::string& bama_file, const std::string& dat_file) {
    int fd;
    size_t read_bytes, total_bytes = 0;
    std::array<R, batch_size> batch;
    require_neq(fd = ::open(bama_file.c_str(), O_RDONLY), -1,
                "failed to open file.");
    size_t in_bytes = ::lseek(fd, 0L, SEEK_END);
    std::cout << "Bama to dat: " << bama_file
              << " (" << in_bytes << "b)"
              << " -> " << dat_file << std::endl;
    ::lseek(fd, 0L, 0);
    Writer<R> w(dat_file);
    do {
        read_bytes = ::read(fd, batch.data(), sizeof(batch));
        total_bytes += read_bytes;
        require_eq(read_bytes % sizeof(R), 0UL,
                   "Alignment error, probably wrong type.");
        for (size_t i = 0; i < read_bytes / sizeof(R); i++) {
             w.write(batch[i]);
        }
    } while (read_bytes == sizeof(batch));
    require_eq(total_bytes, in_bytes, "Didn't convert the entire file.");
    w.close();
    close(fd);
}

#endif /* BAMIFY_H */
