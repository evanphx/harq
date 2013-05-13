#ifndef SOCKET_HPP
#define SOCKET_HPP

#include <string>

namespace wire {
  class Message;
}

struct Socket {
  int fd;

  Socket(int fd)
    : fd(fd)
  {}

  void set_nonblock();
  int write(wire::Message& msg);
  bool read(wire::Message& msg);

  int write_raw(std::string val);
};

#endif
