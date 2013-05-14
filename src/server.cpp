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

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"

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
      q.ParseFromString(val);

      if(!q.implicit()) {
        q.set_implicit(implicit);
        s = db_->Put(write_options_, dest, q.SerializeAsString());

        if(!s.ok()) {
          std::cerr << "Unable to upgrade queue to explicit: " << dest << "\n";
        } else {
#ifdef DEBUG
          std::cout << "Upgraded implicit persistance to explicit.\n";
#endif
        }
      }
    }
#ifdef DEBUG
    std::cout << "Already reserved " << dest << "\n";
#endif
  } else {
    wire::Queue q;
    q.set_count(0);
    q.set_implicit(implicit);

    s = db_->Put(write_options_, dest, q.SerializeAsString());
#ifdef DEBUG
    std::cout << "Reserved " << dest << "\n";
#endif
    if(!s.ok()) {
      std::cerr << "Unable to reserve " << dest << "\n";
    }
  }
}

void Server::deliver(wire::Message& msg) {
  std::string dest = msg.destination();

  std::cout << "delivering to " << dest << " for "
            << connections_.size() << " connections\n";

  bool consumed = false;

  for(std::list<Connection*>::iterator i = connections_.begin();
      i != connections_.end();
      ++i) {
    Connection* con = *i;
    consumed |= con->deliver(msg);
  }

  if(!consumed) {
    std::string val;
    wire::Queue q;
    leveldb::Status s = db_->Get(read_options_, dest, &val);

    if(s.ok()) {
      q.ParseFromString(val);

      int c = q.count();
      std::stringstream ss;
      ss << dest;
      ss << ":";
      ss << c;

#ifdef DEBUG
      std::cout << "Writing persisted message for " << dest
                << " (" << c  << ")\n";
#endif

      c++;
      q.set_count(c);

      leveldb::WriteBatch batch;
      batch.Put(ss.str(), msg.SerializeAsString());
      batch.Put(dest, q.SerializeAsString());

      s = db_->Write(write_options_, &batch);

      if(!s.ok()) {
        std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
      } else {
#ifdef DEBUG
        std::cout << "Updated index of " << dest << " to " << c << "\n";
#endif
      }
      /*
    } else {
      s = db_->Put(write_options_, dest, "0");
      if(!s.ok()) {
        std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
      } else {
        std::stringstream ss;
        ss << dest;
        ss << ":0";

        s = db_->Put(write_options_, ss.str(), msg.SerializeAsString());
        if(!s.ok()) {
          std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
        }
      }
      */
    } else {
#ifdef DEBUG
      std::cout << "No persisted dest at " << dest << "\n";
#endif
    }
  } else {
    std::cout << "No persistance used\n";
  }
}

void Server::flush(Connection* con, std::string dest) {
  std::string val;
  leveldb::Status s = db_->Get(read_options_, dest, &val);

  if(!s.ok()) {
#ifdef DEBUG
    std::cout << "No message to flush from " << dest << "\n";
#endif
    return;
  }

  wire::Queue q;
  q.ParseFromString(val);

  int count = q.count();

#ifdef DEBUG
  std::cout << "Messages to flush: " << count << "\n";
#endif

  for(int i = 0; i < count; i++) {
    std::stringstream ss;
    ss << dest;
    ss << ":";
    ss << i;

    s = db_->Get(read_options_, ss.str(), &val);
    if(s.ok()) {
      con->write_raw(val);
      s = db_->Delete(write_options_, ss.str());
#ifdef DEBUG
      std::cout << "Flushed message " << i << "\n";
#endif
      if(!s.ok()) {
        std::cerr << "Unable to delete " << ss.str() << "\n";
      }
    } else {
      std::cerr << "Unable to get " << ss.str() << "\n";
    }
  }

  if(q.implicit()) {
    s = db_->Delete(write_options_, dest);
#ifdef DEBUG
    std::cout << "Deleted implicit queue: " << dest << "\n";
#endif
  } else {
    q.set_count(0);
    s = db_->Put(write_options_, dest, q.SerializeAsString());
  }

  if(!s.ok()) {
    std::cerr << "Unable to reset " << dest << "\n";
  }
}

