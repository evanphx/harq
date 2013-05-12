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

Server::Server(const char* db_path, const char* hostaddr, int port, int dbn)
    : db_num_(dbn)
    , db_path_(db_path)
    , hostaddr_(hostaddr)
    , port_(port)
    , fd_(-1)
    , loop_(EVBACKEND)
    , connection_watcher_(loop_)
    , clients_num(0)
{
  options_ = leveldb_options_create();
  leveldb_options_set_create_if_missing(options_, 1);

  read_options_ = leveldb_readoptions_create();
  write_options_ = leveldb_writeoptions_create();

  char* err = 0;

  if(db_num_ < 1) {
    db_=new leveldb_t*[1];
    db_[0] = leveldb_open(options_, db_path_.c_str(), &err);
    if(err) {
      puts(err);
      exit(1);
    }
  } else {
    db_=new leveldb_t*[db_num_];
    char buf[16];
    for(int i=0;i<db_num_;i++){
      int count = sprintf(buf, "/db-%03d", i);
      //TODO the db path
      db_[i] = leveldb_open(options_, (db_path+std::string(buf,count)).c_str(), &err);
      if(err) {
        puts(buf);
        puts(err);
        exit(1);
      }
    }
  }
}

Server::~Server() {
  if(db_num_ < 1) {
    leveldb_close(db_[0]);
  } else {
    for(int i=0;i<db_num_;i++){
      leveldb_close(db_[i]);
    }
  }

  delete[] db_;
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
    if(addr.sin_addr.s_addr==INADDR_NONE){
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

void Server::deliver(wire::Message& msg) {
  std::string dest = msg.destination();

  std::cout << "delivering to " << dest << " for "
            << connections_.size() << " connections\n";

  for(std::list<Connection*>::iterator i = connections_.begin();
      i != connections_.end();
      ++i) {
    Connection* con = *i;
    con->deliver(msg);
  }
}

