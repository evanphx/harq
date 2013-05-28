#ifndef ACK_RECORD_HPP
#define ACK_RECORD_HPP

#include "wire.pb.h"

class Queue;

struct AckRecord {
  wire::Message msg;
  Queue& queue;

  std::string durable_key;
  uint64_t durable_idx;

  AckRecord(wire::Message& m, Queue& q)
    : msg(m)
    , queue(q)
    , durable_idx(0)
  {}

  AckRecord(const wire::Message& m, Queue& q)
    : msg(m)
    , queue(q)
    , durable_idx(0)
  {}
};

#endif
