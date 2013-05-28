#include "buffer.hpp"

#include <errno.h>

Buffer::Buffer(size_t size)
  : buffer_(new uint8_t[size])
  , read_pos_(buffer_)
  , write_pos_(buffer_)
  , limit_(write_pos_ + size)
{}

ssize_t Buffer::fill(int fd) {
  const size_t left = limit_ - write_pos_;

  for(;;) {
    const ssize_t got = recv(fd, write_pos_, left, 0);
    if(got > 0) {
      write_pos_ += got;
    } else if(got == -1 && errno == EINTR) {
      continue;
    }

    return got;
  }
}

int Buffer::read_int32() {
  const int s = ntohl(*((int*)read_pos_));
  advance_read(4);
  return s;
}
