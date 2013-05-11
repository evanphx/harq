#ifndef SERVER_HPP
#define SERVER_HPP

#include <vector>
#include <string>

#include <ev.h>
#include <leveldb/c.h>

class Server {
  int db_num_;
  std::string db_path_;
  std::string hostaddr_;
  int port_;
  int fd_;
  ev_io connection_watcher_;
  leveldb_options_t* options_;
  leveldb_readoptions_t* read_options_;
  leveldb_writeoptions_t* write_options_;
  leveldb_t **db_;

public:
  struct ev_loop* loop_;
  int clients_num;

  Server(const char *db_path, const char *hostaddr, int port, int dbn=0);
  ~Server();
  void start();
  static void on_connection(struct ev_loop *loop, ev_io *watcher, int revents);
};


#endif

