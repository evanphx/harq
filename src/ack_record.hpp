#ifndef ACK_RECORD_HPP
#define ACK_RECORD_HPP

#include "message.hpp"

class Queue;

struct AckRecord {
  Message msg;
  Queue& queue;

  AckRecord(Message& m, Queue& q)
    : msg(m)
    , queue(q)
  {}

  AckRecord(const Message& m, Queue& q)
    : msg(m)
    , queue(q)
  {}

  bool durable_p() {
    return msg.durable_p();
  }
};

#endif
