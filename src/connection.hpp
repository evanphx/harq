#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <vector>
#include <list>
#include <string>
#include <map>

#include <ev++.h>
#include <leveldb/c.h>

#include "harq.hpp"
#include "buffer.hpp"
#include "socket.hpp"

#include "ack_record.hpp"

class Server;
class Queue;

namespace wire {
  class Message;
  class Action;
  class ReplicaAction;
}

enum DeliverStatus { eIgnored, eWaitForAck, eConsumed };

class Connection {
public:
  enum State { eReadSize, eReadMessage };

private:
  std::list<std::string> subscriptions_;
  bool tap_;
  bool ack_;
  bool confirm_;
  bool closing_;
  bool replica_;
  Socket sock_;
  ev::io read_w_;
  ev::io write_w_;

  typedef std::map<const uint64_t, AckRecord> AckMap;
  AckMap to_ack_;

  bool open_;
  Server& server_;

  Buffer buffer_;

  State state_;

  int need_;

  bool writer_started_;

public:
  /*** methods ***/

  Connection(Server& s, int fd);
  ~Connection();

  Buffer& buffer() {
    return buffer_;
  }

  void on_readable(ev::io& w, int revents);
  void on_writable(ev::io& w, int revents);

  void start();
  void start_replica();
  bool do_read(int revents);

  void handle_message(const wire::Message& msg);
  void handle_action(const wire::Action& act);
  void handle_replica(const wire::ReplicaAction& act);

  DeliverStatus deliver(const wire::Message& msg, Queue& from);

  void write(const wire::Message& msg);

  void clear_ack(uint64_t id);

  void make_queue(std::string name, Queue::Kind k);
  void send_error(std::string name, std::string error);
};

#endif
