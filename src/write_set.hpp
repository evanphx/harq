#ifndef WRITE_SET_HPP
#define WRITE_SET_HPP

#include <list>
#include <string>

enum WriteStatus {
  eOk,
  eWouldBlock,
  eFailure
};

class WriteSet {
  struct Slice {
    std::string buf;
    int start;

    Slice(std::string buf, int s)
      : buf(buf)
      , start(s)
    {}
  };

  typedef std::list<Slice*> Slices;

  Slices slices_;

public:

  void add(std::string val, int s=0) {
    slices_.push_back(new Slice(val, 0));
  }

  WriteStatus flush(int fd);
};

#endif
