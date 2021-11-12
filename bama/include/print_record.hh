#ifndef PRINT_RECORD_H
#define PRINT_RECORD_H

#include <string>
#include <iostream>
#include "common.hh"
#include "io.hh"

template<typename R>
inline  void print_records(const std::string& filepath, const size_t n)
{
    Reader<R> reader(filepath);
    std::cout << "File: " << filepath
              << "(capacity: " << reader.recordsCapacity() << ")"
              << std::endl;
    for (size_t i = 0; i < n && reader.hasNext(); i++) {
        auto rec = reader.nextRecord();
        std::string str(rec.show());
        std::cout << str << std::endl;
    }
}

template <typename R, typename I = WrapRecord<size_t>>
inline void print_records(
    const std::pair<const std::string, const std::string>& filepath,
    const size_t n) {
  print_records<R>(filepath.first, n);
  print_records<I>(filepath.second, n);
}

template<typename R>
inline constexpr void print_records(
    const Just<const std::string>& filepath,
    const size_t n)
{
    print_records<R>(filepath.value, n);
}

template<typename R, typename F>
inline  void print_records(
    const Nothing<F>& filepath, const size_t n)
{
    return;
}
template<typename R, typename I=WrapRecord<size_t>>
inline constexpr void print_records(
    const Just<std::pair<const std::string, const std::string> >& filepath,
    const size_t n)
{
    print_records<R>(filepath.value.first, n);
    print_records<I>(filepath.value.second, n);
}

#endif /* PRINT_RECORD_H */
