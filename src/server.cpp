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

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <errno.h>

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

#include "option.hpp"

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

bool Server::read_queues() {
  std::string val;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), "!harq.config", &val);
  if(s.IsNotFound()) return true;
  if(!s.ok()) {
    std::cerr << "Corrupt harq.config detected!\n";
    return false;
  }

  wire::QueueConfiguration cfg;
  if(!cfg.ParseFromString(val)) {
    std::cerr << "Corrupt harq.config detected!\n";
    return false;
  }

  for(int i = 0; i < cfg.queues_size(); i++) {
    const wire::QueueDeclaration& decl = cfg.queues(i);
    Queue::Kind k;

    // Don't couple the enum values to the disk values, that's why
    // we do this.
    switch(decl.type()) {
    case wire::QueueDeclaration::eBroadcast:
      k = Queue::eBroadcast;
      break;
    case wire::QueueDeclaration::eTransient:
      k = Queue::eTransient;
      break;
    case wire::QueueDeclaration::eDurable:
      k = Queue::eDurable;
      break;
    default:
      std::cerr << "Corrupt queue declaration (unknown type " << decl.type() << ")\n";
      return false;
    }

    if(!make_queue(decl.name(), k)) {
      std::cerr << "Unable to make queue '" << decl.name() << "'\n";
      return false;
    } else {
      debugs << "Added queue from config: " << decl.name() << "\n";
    }
  }

  return true;
}

bool Server::add_declaration(std::string name, Queue::Kind k) {
  std::string val;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), "!harq.config", &val);

  wire::QueueConfiguration cfg;

  if(s.ok()) {
    if(!cfg.ParseFromString(val)) {
      std::cerr << "Corrupt harq.config detected!\n";
      return false;
    }
  } else if(!s.IsNotFound()) {
    std::cerr << "Corrupt harq.config detected!\n";
    return false;
  }

  wire::QueueDeclaration_Type wk;

  switch(k) {
  case Queue::eBroadcast:
    wk = wire::QueueDeclaration::eBroadcast;
    break;
  case Queue::eTransient:
    wk = wire::QueueDeclaration::eTransient;
    break;
  case Queue::eDurable:
    wk = wire::QueueDeclaration::eDurable;
    break;
  }

  bool wrote = false;

  for(int i = 0; i < cfg.queues_size(); i++) {
    const wire::QueueDeclaration& decl = cfg.queues(i);

    if(decl.name() == name) {
      cfg.mutable_queues(i)->set_type(wk);
      wrote = true;
      break;
    }
  }

  if(!wrote) {
    wire::QueueDeclaration* decl = cfg.add_queues();
    decl->set_name(name);
    decl->set_type(wk);
  }

  s = db_->Put(leveldb::WriteOptions(), "!harq.config", cfg.SerializeAsString());
  if(!s.ok()) {
    std::cerr << "Unable to write harq.config!\n";
    return false;
  }

  return true;
}

optref<Queue> Server::queue(std::string name) {
  Queues::iterator i = queues_.find(name);
  if(i != queues_.end()) return ref(i->second);

  return optref<Queue>();
}

bool Server::make_queue(std::string name, Queue::Kind k) {
  Queues::iterator i = queues_.find(name);
  bool ok;

  if(i == queues_.end()) {
    Queue* q = new Queue(ref(this), name, k);
    queues_[name] = q;
    if(k == Queue::eDurable) reserve(name, false);
    ok = true;
  } else {
    ok = i->second->change_kind(k);
  }

  if(ok) add_declaration(name, k);

  return ok;
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

  Connection* connection = new Connection(ref(this), fd);

  if(connection == NULL) {
    close(fd);
    return;
  }

  connections_.push_back(connection);

  connection->start();
}

void Server::reserve(std::string dest, bool implicit) {
  if(replicas_.size() > 0) {
    wire::ReplicaAction act;
    act.set_type(wire::ReplicaAction::eReserve);
    act.set_payload(dest);

    wire::Message msg;
    msg.set_destination("+replica");
    msg.set_payload(act.SerializeAsString());

    for(Connections::const_iterator i = replicas_.begin();
        i != replicas_.end();
        ++i) {
      (*i)->write(msg);
    }
  }

  std::string val;
  leveldb::Status s = db_->Get(read_options_, dest, &val);

  if(s.ok()) {
    debugs << "Already reserved " << dest << "\n";
    return;
  }

  wire::Queue q;
  q.set_size(0);

  s = db_->Put(write_options_, dest, q.SerializeAsString());
  debugs << "Reserved " << dest << "\n";
  if(!s.ok()) {
    std::cerr << "Unable to reserve " << dest << "\n";
  }
}

bool Server::deliver(wire::Message& msg) {
  optref<Queue> q = queue(msg.destination());
  if(!q.set_p()) return false;

  // Send message to taps first.
  for(Connections::const_iterator i = taps_.begin();
      i != taps_.end();
      ++i) {
    (*i)->write(msg);
  }

  std::string dest = msg.destination();

  debugs << "delivering to " << dest << " for "
         << connections_.size() << " connections\n";

  q->deliver(msg);

  for(Connections::const_iterator i = replicas_.begin();
      i != replicas_.end();
      ++i) {
    (*i)->write(msg);
  }

  return true;
}

void Server::subscribe(Connection* con, std::string dest, bool durable) {
  optref<Queue> q = queue(dest);
  if(q.set_p()) q->subscribe(con);
}

void Server::flush(Connection* con, std::string dest) {
  optref<Queue> q = queue(dest);
  if(q.set_p()) q->flush(con, db_);
}

void Server::stat(Connection* con, std::string dest) {
  optref<Queue> q = queue(dest);

  wire::Stat stat;
  stat.set_name(dest);

  if(q.set_p()) {
    stat.set_transient_size(q.set_p() ? q->queued_messages() : -1);

    if(q.set_p() && q->durable_p()) {
      stat.set_durable_size(q->durable_messages());
    }
  } else {
    stat.set_exists(false);
  }

  wire::Message msg;
  msg.set_destination("+");
  msg.set_type(eStat);
  msg.set_payload(stat.SerializeAsString());

  con->write(msg);
}

void Server::connect_replica(std::string host, int c_port) {
  int s, rv;
  char port[6];  /* strlen("65535"); */
  struct addrinfo hints, *servinfo, *p;

  snprintf(port, 6, "%d", c_port);
  memset(&hints,0,sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo(host.c_str(), port, &hints, &servinfo)) != 0) {
    printf("Error: %s\n", gai_strerror(rv));
    return;
  }

  for (p = servinfo; p != NULL; p = p->ai_next) {
    if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == -1)
      continue;

    if (connect(s,p->ai_addr,p->ai_addrlen) == -1) {
      close(s);
      continue;
    }

    goto end;
  }

  if (p == NULL) {
    printf("Can't create socket: %s\n",strerror(errno));
    return;
  }

end:
  freeaddrinfo(servinfo);

  Connection* con = new Connection(ref(this), s);

  if(con == NULL) {
    close(s);
    return;
  }

  connections_.push_back(con);

  con->start_replica();
}

