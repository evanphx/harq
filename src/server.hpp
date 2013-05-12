#ifndef SERVER_HPP
#define SERVER_HPP

#include <vector>
#include <list>
#include <string>

#include "ev++.h"
#include <leveldb/c.h>

class Connection;

namespace wire {
  class Message;
}

class Server {
  int db_num_;
  std::string db_path_;
  std::string hostaddr_;
  int port_;
  int fd_;
  leveldb_options_t* options_;
  leveldb_readoptions_t* read_options_;
  leveldb_writeoptions_t* write_options_;
  leveldb_t **db_;
  ev::dynamic_loop loop_;
  ev::io connection_watcher_;

  std::list<Connection*> connections_;

public:
  int clients_num;

  ev::dynamic_loop& loop() {
    return loop_;
  }

  void remove_connection(Connection* con) {
    connections_.remove(con);
  }

  Server(const char *db_path, const char *hostaddr, int port, int dbn=0);
  ~Server();
  void start();
  void on_connection(ev::io& w, int revents);

  void deliver(wire::Message& msg);
};


#endif

