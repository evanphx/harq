#include "qadmus.hpp"
#include "write_set.hpp"

#include <errno.h>

#include <iostream>

WriteStatus WriteSet::flush(int fd) {
  while(slices_.size() > 0) {
    Slice* sl = slices_.front();

    uint8_t* buf = (uint8_t*)sl->buf.c_str();
    ssize_t left = sl->buf.size() - sl->start;

    while(left > 0) {
#ifdef SIMULATE_BAD_NETWORK
      ssize_t r = ::write(fd, buf + sl->start, left > 2 ? 2 : left);
#else
      ssize_t r = ::write(fd, buf + sl->start, left);
#endif

      if(r == -1) {
        if(errno == EAGAIN || errno == EWOULDBLOCK) return eWouldBlock;
        if(errno == EINTR) continue;
        return eFailure;
      }

      if(r == 0) return eWouldBlock;

      sl->start += r;
      left -= r;

#ifdef SIMULATE_BAD_NETWORK
      return eWouldBlock;
#endif
    }

    slices_.pop_front();
    delete sl;
  }

  return eOk;
}
