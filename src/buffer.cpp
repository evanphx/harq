#include "buffer.hpp"

Buffer::Buffer(size_t size)
  : buffer_(new uint8_t[size])
  , read_pos_(buffer_)
  , write_pos_(buffer_)
  , limit_(write_pos_ + size)
{}

ssize_t Buffer::fill(int fd) {
  size_t left = limit_ - write_pos_;

  ssize_t got = recv(fd, write_pos_, left, 0);
  if(got > 0) {
    write_pos_ += got;
  }

  return got;
}

int Buffer::read_int32() {
  int s = *((int*)read_pos_);
  advance_read(4);
  return s;
}
