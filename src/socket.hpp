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
  bool read(wire::Message& msg);

  WriteStatus write(wire::Message& msg);
  WriteStatus write_raw(std::string val);

  void write_block(wire::Message& msg);

  WriteStatus flush() {
    return writes_.flush(fd);
  }
};

#endif
