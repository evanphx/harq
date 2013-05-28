#ifndef SOCKET_HPP
#define SOCKET_HPP

#include <string>

#include "write_set.hpp"

namespace wire {
  class Message;
}

class Socket {
  WriteSet writes_;

public:
  int fd;

  Socket(int fd)
    : fd(fd)
  {}

  void set_nonblock();

  WriteStatus write(const wire::Message& msg);
  WriteStatus write_raw(const std::string val);

  bool read_block(wire::Message& msg);
  void write_block(const wire::Message& msg);

  WriteStatus flush() {
    return writes_.flush(fd);
  }
};

#endif
