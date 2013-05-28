#ifndef QUEUE_HPP
#define QUEUE_HPP

#include <list>
#include <string>

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
  enum Kind { eBroadcast, eTransient, eDurable };

private:
  typedef std::list<wire::Message*> Messages;
  typedef std::list<Connection*> Connections;

  Server& server_;
  const std::string name_;
  Messages transient_;
  Connections subscribers_;

  Kind kind_;

public:
  Queue(Server& s, std::string name, Kind k)
    : server_(s)
    , name_(name)
    , kind_(k)
  {}

  Server& server() {
    return server_;
  }

  unsigned queued_messages() {
    return transient_.size();
  }

  void subscribe(Connection* con) {
    subscribers_.push_back(con);
  }

  void unsubscribe(Connection* con) {
    subscribers_.remove(con);
  }

  bool change_kind(Kind k);

  void queue(const wire::Message& msg);
  void flush(Connection* con, leveldb::DB* db);
  bool deliver(const wire::Message& msg);

  bool write_durable(const wire::Message& msg, std::string* key, uint64_t* idx);
  bool erase_durable(wire::Message& msg, std::string& str, int idx);

  void recorded_ack(AckRecord& rec);
  void acked(AckRecord& rec);

private:
  bool flush_to_durable();
  std::string durable_key(int j);
};

#endif
