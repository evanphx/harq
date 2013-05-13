#ifndef SUBSCRIPTION_HPP
#define SUBSCRIPTION_HPP

#include <list>

#include "wire.pb.h"

class Subscription {
  typedef std::list<wire::Message> Messages;

  Messages messages_;

public:

  bool empty_p() {
    return messages_.size() == 0;
  }

  wire::Message pop() {
    wire::Message msg = messages_.back();
    messages_.pop_back();
    return msg;
  }

  void push(wire::Message& msg) {
    messages_.push_back(msg);
  }
};

#endif
