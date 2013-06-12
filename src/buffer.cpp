#include "buffer.hpp"

#include <errno.h>

Buffer::Buffer(size_t size)
  : buffer_(new uint8_t[size])
  , read_pos_(buffer_)
  , write_pos_(buffer_)
  , limit_(write_pos_ + size)

  // Could be tuned, but half is probably fine.
  , slop_pos_(buffer_ + (size / 2))
{}

void Buffer::clean_pigpen() {
  size_t unread = write_pos_ - read_pos_;
  if(unread == 0) {
    read_pos_ = buffer_;
    write_pos_ = buffer_;
  } else {
    size_t slop = read_pos_ - buffer_;

    memmove(buffer_, read_pos_, unread);
    write_pos_ -= slop;
    read_pos_ = buffer_;
  }
}

ssize_t Buffer::fill(int fd) {
  const size_t left = limit_ - write_pos_;

  for(;;) {
    const ssize_t got = recv(fd, write_pos_, left, 0);
    std::cout << "left= " << left << " got=" << got << "\n";
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
