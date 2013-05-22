/*-*- c++ -*-
 *
 * rl_server.cpp
 * author : KDr2
 *
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/tcp.h> /* TCP_NODELAY */
#include <netinet/in.h>  /* inet_ntoa */
#include <arpa/inet.h>   /* inet_ntoa */

#include <iostream>
#include <sstream>

#include "leveldb/write_batch.h"

#include "debugs.hpp"
#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"

#include "flags.hpp"
#include "types.hpp"

#include "wire.pb.h"

#define EVBACKEND EVFLAG_AUTO

#ifdef __linux
#undef EVBACKEND
#define EVBACKEND EVBACKEND_EPOLL
#endif

#ifdef __APPLE__
#undef EVBACKEND
#define EVBACKEND EVBACKEND_KQUEUE
#endif

Server::Server(std::string db_path, std::string hostaddr, int port)
    : db_path_(db_path)
    , hostaddr_(hostaddr)
    , port_(port)
    , fd_(-1)
    , loop_(EVBACKEND)
    , connection_watcher_(loop_)
    , next_id_(0)
    , clients_num(0)
{
  options_.create_if_missing = true;

  leveldb::Status s = leveldb::DB::Open(options_, db_path_, &db_);
  if(!s.ok()) {
    puts(s.ToString().c_str());
    exit(1);
  }
}

Server::~Server() {
  delete db_;
  close(fd_);
}

void Server::start() {    
  if((fd_ = socket(AF_INET, SOCK_STREAM, 0)) == -1) {
    perror("socket()");
    exit(1);
  }

  int flags = 1;
  setsockopt(fd_, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
  setsockopt(fd_, SOL_SOCKET, SO_KEEPALIVE, (void *)&flags, sizeof(flags));

  struct linger ling = {0, 0};
  setsockopt(fd_, SOL_SOCKET, SO_LINGER, (void *)&ling, sizeof(ling));

  /* XXX: Sending single byte chunks in a response body? Perhaps there is a
   * need to enable the Nagel algorithm dynamically. For now disabling.
   */
  setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, (void *)&flags, sizeof(flags));

  struct sockaddr_in addr;

  /* the memset call clears nonstandard fields in some impementations that
   * otherwise mess things up.
   */
  memset(&addr, 0, sizeof(addr));

  addr.sin_family = AF_INET;
  addr.sin_port = htons(port_);

  if(!hostaddr_.empty()) {
    addr.sin_addr.s_addr = inet_addr(hostaddr_.c_str());
    if(addr.sin_addr.s_addr == INADDR_NONE){
      printf("Bad address(%s) to listen\n",hostaddr_.c_str());
      exit(1);
    }
  } else {
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
  }

  if(bind(fd_, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
    perror("bind()");
    if(fd_ > 0) close(fd_);
    exit(1);
  }

  if(listen(fd_, MAX_CONNECTIONS) < 0) {
    perror("listen()");
    exit(1);
  }

  set_nonblock(fd_);

  connection_watcher_.set<Server, &Server::on_connection>(this);
  connection_watcher_.start(fd_, EV_READ);

  loop_.run(0);
}

void Server::on_connection(ev::io& w, int revents) {
  if(EV_ERROR & revents) {
    puts("on_connection() got error event, closing server.");
    return;
  }

  struct sockaddr_in addr; // connector's address information
  socklen_t addr_len = sizeof(addr); 
  int fd = accept(fd_, (struct sockaddr*)&addr, &addr_len);

  if(fd < 0) {
    perror("accept()");
    return;
  }

  Connection* connection = new Connection(this, fd);

  if(connection == NULL) {
    close(fd);
    return;
  }

  connections_.push_back(connection);

  connection->start();
}

void Server::reserve(std::string dest, bool implicit) {
  std::string val;
  leveldb::Status s = db_->Get(read_options_, dest, &val);

  if(s.ok()) {
    // Upgrade an implicit persistance to an explicit one.
    if(!implicit) {
      wire::Queue q;
      if(!q.ParseFromString(val)) {
        std::cerr << "Corrupt queue info on disk, resetting..\n";
      } else if(!q.implicit()) {
        q.set_implicit(false);
        s = db_->Put(write_options_, dest, q.SerializeAsString());

        if(!s.ok()) {
          std::cerr << "Unable to upgrade queue to explicit: " << dest << "\n";
        } else {
          debugs << "Upgraded implicit persistance to explicit.\n";
        }

        return;
      }
    } else {
      debugs << "Already reserved " << dest << "\n";
      return;
    }
  }

  wire::Queue q;
  q.set_count(0);
  q.set_implicit(implicit);

  s = db_->Put(write_options_, dest, q.SerializeAsString());
  debugs << "Reserved " << dest << "\n";
  if(!s.ok()) {
    std::cerr << "Unable to reserve " << dest << "\n";
  }
}

void Server::deliver(wire::Message& msg) {
  std::string dest = msg.destination();

  debugs << "delivering to " << dest << " for "
         << connections_.size() << " connections\n";

  queue(dest).deliver(msg, db_);
}

void Server::subscribe(Connection* con, std::string dest, bool durable) {
  queue(dest).subscribe(con);
}

void Server::flush(Connection* con, std::string dest) {
  queue(dest).flush(con, db_);
}

void Server::stat(Connection* con, std::string dest) {
  Queue& q = queue(dest);

  wire::Stat stat;
  stat.set_name(dest);
  stat.set_transient_size(q.queued_messages());

  wire::Message msg;
  msg.set_destination("+");
  msg.set_type(eStat);
  msg.set_payload(stat.SerializeAsString());

  con->deliver(msg);
}

