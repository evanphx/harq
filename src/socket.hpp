#ifndef SOCKET_HPP
#define SOCKET_HPP

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
};

#endif
