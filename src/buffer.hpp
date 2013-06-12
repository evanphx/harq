#ifndef BUFFER_HPP
#define BUFFER_HPP

#include <stdint.h>
#include <sys/socket.h>

#include <iostream>

class Buffer {
  uint8_t* const buffer_;
  uint8_t* read_pos_;
  uint8_t* write_pos_;
  uint8_t* limit_;
  uint8_t* slop_pos_;

public:
  Buffer(size_t size);

  uint8_t* read_pos() {
    return read_pos_;
  }

  int read_available() {
    return write_pos_ - read_pos_;
  }

  bool too_much_slop_p() {
    return read_pos_ > slop_pos_;
  }

  ssize_t fill(int fd);

  int read_int32();

  void clean_pigpen();

  void advance_read(int size) {
    uint8_t* const ptr = read_pos_ + size;
    if(ptr > write_pos_) {
      read_pos_ = write_pos_;
    } else {
      read_pos_ = ptr;
    }

    // If we've consumed all the data, then auto-rewind
    // back to the front of the buffer
    if(read_pos_ == write_pos_) {
      read_pos_ = buffer_;
      write_pos_ = buffer_;
    } else if(too_much_slop_p()) {
      clean_pigpen();
    }
  }
};

#endif
