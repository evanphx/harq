#ifndef QUEUE_HPP
#define QUEUE_HPP

#include <list>
#include <string>

#include "message.hpp"

namespace wire {
  class Message;
  class Queue;
}

namespace leveldb {
  class DB;
}

class Connection;
class Server;
struct AckRecord;

class Queue {
public:
  enum Kind { eBroadcast, eTransient, eDurable, eEphemeral };
  typedef std::list<Queue*> List;

private:
  typedef std::list<Message> Messages;
  typedef std::list<Connection*> Connections;

  Server& server_;
  const std::string name_;
  Messages transient_;
  Connections subscribers_;

  List broadcast_into_;
  List bonded_to_;

  Kind kind_;

public:
  Queue(Server& s, std::string name, Kind k)
    : server_(s)
    , name_(name)
    , kind_(k)
  {}

  ~Queue();

  bool mem_only_p() {
    return kind_ == eTransient || kind_ == eEphemeral;
  }

  Server& server() {
    return server_;
  }

  std::string name() {
    return name_;
  }

  unsigned queued_messages() {
    return transient_.size();
  }

  unsigned durable_messages();

  void subscribe(Connection* con) {
    subscribers_.push_back(con);
  }

  void unsubscribe(Connection* con) {
    subscribers_.remove(con);
  }

  bool durable_p() {
    return kind_ == eDurable;
  }

  void broadcast_into(Queue* other) {
    broadcast_into_.push_back(other);
    other->bonded_to_.push_back(this);
  }

  bool change_kind(Kind k);

  int flush(Connection* con);
  int flush_at_most(Connection* con, int count);
  void deliver(Message& msg);

  void recorded_ack(AckRecord& rec);
  void acked(AckRecord& rec);

private:
  void write_transient(const Message& msg);
  bool write_durable(Message& msg);
  bool erase_durable(uint64_t index);

  bool flush_to_durable();
  std::string durable_key(int j);
};

#endif
