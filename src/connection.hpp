#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <vector>
#include <list>
#include <string>
#include <map>

#include <ev.h>
#include <leveldb/c.h>

#include "qadmus.hpp"
#include "buffer.hpp"
#include "socket.hpp"

class Server;

namespace wire {
  class Message;
}

class Connection {
  std::list<std::string> subscriptions_;
  bool tap_;
  bool ack_;
  bool closing_;
  Socket sock_;
  ev::io read_w_;
  ev::io write_w_;

  typedef std::map<uint64_t, wire::Message> AckMap;
  AckMap to_ack_;

public:
  bool open;
  Server *server;

  Buffer buffer_;

  enum State { eReadSize, eReadMessage } state;

  int need;

  bool writer_started;

  ev_timer timeout_watcher;
  ev_timer goodbye_watcher;

  /*** methods ***/

  Connection(Server *s, int fd);
  ~Connection();

  Buffer& buffer() {
    return buffer_;
  }

  void on_readable(ev::io& w, int revents);
  void on_writable(ev::io& w, int revents);

  void start();
  bool do_read(int revents);
  int  do_write();

  void handle_message(wire::Message& msg);
  bool deliver(wire::Message& msg);

  void write_raw(std::string str);

  void clear_ack(uint64_t id);
};

#endif
