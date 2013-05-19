#ifndef QUEUE_HPP
#define QUEUE_HPP

#include <list>
#include <string>

namespace wire {
  class Message;
}

namespace leveldb {
  class DB;
}

class Connection;
class Server;

class Queue {
  typedef std::list<wire::Message*> Messages;
  typedef std::list<Connection*> Connections;

  Server* server_;
  std::string name_;
  Messages transient_;
  Connections subscribers_;

public:

  Queue(Server* s, std::string name)
    : server_(s)
    , name_(name)
  {}

  Server* server() {
    return server_;
  }

  unsigned queued_message() {
    return transient_.size();
  }

  void subscribe(Connection* con) {
    subscribers_.push_back(con);
  }

  void unsubscribe(Connection* con) {
    subscribers_.remove(con);
  }

  void queue(wire::Message& msg);
  void flush(Connection* con, leveldb::DB* db);
  bool deliver(wire::Message& msg, leveldb::DB* db);
};

#endif
