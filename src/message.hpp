#ifndef MESSAGE_HPP
#define MESSAGE_HPP

#include "wire.pb.h"

class Message {
  struct Data {
    int refs;
    wire::Message wire;

    bool durable;
    std::string key;
    uint64_t index;

    Data()
      : refs(1)
      , durable(false)
      , index(0)
    {}

    Data(const wire::Message& m)
      : refs(1)
      , wire(m)
      , durable(false)
      , index(0)
    {}
  
    Data(std::string k, uint64_t i)
      : refs(1) 
      , durable(true)
      , key(k)
      , index(i)
    {}
  };

  Data* data_;

public:
  Message()
    : data_(new Data)
  {}

  explicit
  Message(const wire::Message& m)
    : data_(new Data(m))
  {}

  Message(std::string k, uint64_t i)
    : data_(new Data(k, i))
  {}

  Message(const Message& other)
    : data_(other.data_)
  {
    data_->refs++;
  }

  Message& operator=(const Message& other) {
    decref();
    data_ = other.data_;
    data_->refs++;

    return *this;
  }

  void decref() {
    data_->refs--;

    if(data_->refs <= 0) {
      delete data_;
    }
  }

  ~Message() {
    decref();
  }

  wire::Message& wire() {
    return data_->wire;
  }

  wire::Message* operator->() {
    return &data_->wire;
  }

  const wire::Message* operator->() const {
    return &data_->wire;
  }

  const wire::Message& wire() const {
    return data_->wire;
  }

  bool durable_p() {
    return data_->durable;
  }

  std::string key() {
    return data_->key;
  }

  uint64_t index() {
    return data_->index;
  }

  void make_durable(std::string k, uint64_t i) {
    data_->durable = true;
    data_->key = k;
    data_->index = i;
  }

  void make_transient() {
    data_->durable = false;
  }

  std::string serialize() const {
    return data_->wire.SerializeAsString();
  }
};

#endif
