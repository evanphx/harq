#ifndef SERVER_HPP
#define SERVER_HPP

#include <vector>
#include <list>
#include <string>

#include "ev++.h"
#include <leveldb/db.h>

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

  Server(std::string db_path, std::string hostaddr, int port);
  ~Server();
  void start();
  void on_connection(ev::io& w, int revents);

  void reserve(std::string dest, bool implicit);
  void deliver(wire::Message& msg);
  void flush(Connection* con, std::string dest);
};


#endif

