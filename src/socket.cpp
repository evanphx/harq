#include "socket.hpp"

#include "wire.pb.h"

#include <unistd.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>

int Socket::write(wire::Message& msg) {
  google::protobuf::io::FileOutputStream stream(fd);

  int size = msg.ByteSize();

  ::write(fd, &size, sizeof(int));

  msg.SerializeToZeroCopyStream(&stream);

  return size;
}

bool Socket::read(wire::Message& msg) {
  union sz {
    char buf[4];
    int i;
  } sz;

  ssize_t got = 0;

  do {
    int r = recv(fd, sz.buf+got, 4-got, 0);
    if(r == 0) return false;
    got += r;
  } while(got < 4);

  google::protobuf::io::FileInputStream ins(fd);

  msg.ParseFromBoundedZeroCopyStream(&ins, sz.i);

  return true;
}

void Socket::set_nonblock() {
  int flags = fcntl(fd, F_GETFL, 0);
  int r = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
  assert(0 <= r && "Setting socket non-block failed!");
}

