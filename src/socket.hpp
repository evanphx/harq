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

  WriteStatus write(wire::Message& msg);
  WriteStatus write_raw(std::string val);

  bool read_block(wire::Message& msg);
  void write_block(wire::Message& msg);

  WriteStatus flush() {
    return writes_.flush(fd);
  }
};

#endif
