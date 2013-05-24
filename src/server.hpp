#ifndef SERVER_HPP
#define SERVER_HPP

#include <vector>
#include <list>
#include <string>
#include <map>

#include <iostream>

#include "ev++.h"
#include <leveldb/db.h>
#include "queue.hpp"
#include "debugs.hpp"
#include "safe_ref.hpp"

#include "wire.pb.h"

class Connection;

namespace wire {
  class Message;
}

class Server {
  std::string db_path_;
  std::string hostaddr_;
  int port_;
  int fd_;

  leveldb::Options options_;
  leveldb::ReadOptions read_options_;
  leveldb::WriteOptions write_options_;

  leveldb::DB* db_;
  ev::dynamic_loop loop_;
  ev::io connection_watcher_;

  std::list<Connection*> connections_;
  uint64_t next_id_;

  typedef std::map<std::string, Queue*> Queues;
  Queues queues_;

public:
  int clients_num;

  ev::dynamic_loop& loop() {
    return loop_;
  }

  void remove_connection(Connection* con) {
    connections_.remove(con);
  }

  uint64_t next_id() {
    return ++next_id_;
  }

  uint64_t assign_id(wire::Message& msg) {
    uint64_t id = next_id();
    msg.set_id(id);
    debugs << "Assigned message id " << id << "\n";
    return id;
  }

  Queue& queue(std::string name) {
    Queues::iterator i = queues_.find(name);
    if(i != queues_.end()) return ref(i->second);

    Queue* q = new Queue(this, name);

    queues_[name] = q;

    return ref(q);
  }

  Server(std::string db_path, std::string hostaddr, int port);
  ~Server();
  void start();
  void on_connection(ev::io& w, int revents);

  void reserve(std::string dest, bool implicit);
  void deliver(wire::Message& msg);

  void subscribe(Connection* con, std::string dest, bool durable=false);
  void flush(Connection* con, std::string dest);

  void stat(Connection* con, std::string name);
};


#endif

