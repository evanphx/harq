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

#define HARQ_CONFIG "!harq.config"

Server::Server(Config& cfg, std::string db_path, std::string hostaddr, int port)
    : config_(cfg)
    , db_path_(db_path)
    , hostaddr_(hostaddr)
    , port_(port)
    , fd_(-1)
    , loop_(EVBACKEND)
    , connection_watcher_(loop_)
    , sigint_watcher_(loop_)
    , sigterm_watcher_(loop_)
    , cleanup_watcher_(loop_)
    , next_id_(0)
{
  options_.create_if_missing = true;

  leveldb::Status s = leveldb::DB::Open(options_, db_path_, &db_);
  if(!s.ok()) {
    puts(s.ToString().c_str());
    exit(1);
  }

  sigint_watcher_.set<Server, &Server::on_signal>(this);
  sigint_watcher_.start(SIGINT);

  sigterm_watcher_.set<Server, &Server::on_signal>(this);
  sigterm_watcher_.start(SIGTERM);

  cleanup_watcher_.set<Server, &Server::cleanup>(this);
  cleanup_watcher_.start();
}

Server::~Server() {
  delete db_;
  close(fd_);
}

void Server::cleanup(ev::check& w, int revents) {
  // Now all the closing connections are detached from queues
  // and this in the only reference left to them, so we can have
  // them flush their un-ack'd messages safely and then delete them.

  for(Connections::iterator i = closing_connections_.begin();
      i != closing_connections_.end();
      ++i) {
    (*i)->cleanup();
    delete *i;
  }

  closing_connections_.clear();
}


DataStatus Server::read_queue(std::string name, wire::Queue& qi) {
  std::string val;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), dname(name), &val);

  if(s.IsNotFound()) return eMissing;

  if(s.ok()) {
    if(qi.ParseFromString(val)) {
      return eValid;
    }
  }

  return eInvalid;
}

DataStatus Server::read_message(std::string key, Message& msg) {
  std::string val;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), key, &val);

  if(s.IsNotFound()) return eMissing;

  if(s.ok()) {
    if(msg.wire().ParseFromString(val)) {
      return eValid;
    }
  }

  return eInvalid;
}

bool Server::update_queue(std::string name, wire::Queue& qi,
                          std::string key, const Message& msg)
{
  leveldb::WriteBatch batch;
  batch.Put(key, msg.serialize());
  batch.Put(dname(name), qi.SerializeAsString());

  leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);

  return s.ok();
}

bool Server::update_queue(std::string name, wire::Queue& qi) {
  leveldb::WriteBatch batch;
  batch.Put(dname(name), qi.SerializeAsString());

  leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);

  return s.ok();
}

bool Server::remove_message(std::string name, wire::Queue& qi, std::string key) {
  leveldb::WriteBatch batch;
  batch.Delete(key);
  batch.Put(dname(name), qi.SerializeAsString());

  leveldb::Status s = db_->Write(leveldb::WriteOptions(), &batch);

  return s.ok();
}

bool Server::read_queues() {
  std::string val;
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), HARQ_CONFIG, &val);
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
  leveldb::Status s = db_->Get(leveldb::ReadOptions(), HARQ_CONFIG, &val);

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
  case Queue::eEphemeral:
    std::cerr << "Tried to add a declaration for an ephemeral queue!\n";
    return false;
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

  s = db_->Put(leveldb::WriteOptions(), HARQ_CONFIG, cfg.SerializeAsString());
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
    if(k == Queue::eDurable) reserve(name);
    ok = true;
  } else {
    ok = i->second->change_kind(k);
  }

  if(ok && k != Queue::eEphemeral) add_declaration(name, k);

  return ok;
}

void Server::destroy_queue(Queue* q) {
  Queues::iterator i = queues_.find(q->name());
  if(i != queues_.end()) {
    queues_.erase(i);
  }

  delete q;
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

void Server::on_signal(ev::sig& w, int revents) {
  std::cerr << "Exitting...\n";
  loop_.break_loop();
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

void Server::reserve(std::string dest) {
  if(replicas_.size() > 0) {
    wire::ReplicaAction act;
    act.set_type(wire::ReplicaAction::eReserve);
    act.set_payload(dest);

    wire::Message msg;
    msg.set_destination("+replica");
    msg.set_payload(act.SerializeAsString());

    write_replicas(msg);
  }

  std::string val;
  leveldb::Status s = db_->Get(read_options_, dname(dest), &val);

  if(s.ok()) {
    debugs << "Already reserved " << dest << "\n";
    return;
  }

  wire::Queue q;
  q.set_size(0);

  s = db_->Put(write_options_, dname(dest), q.SerializeAsString());
  debugs << "Reserved " << dest << "\n";
  if(!s.ok()) {
    std::cerr << "Unable to reserve " << dest << "\n";
  }
}

bool Server::deliver(Message& msg) {
  std::string dest = msg->destination();

  optref<Queue> q = queue(dest);
  if(!q) return false;

  // Send message to taps first.
  for(Connections::iterator i = taps_.begin();
      i != taps_.end();)
  {
    if((*i)->write(msg.wire())) {
      ++i;
    } else {
      debugs << "Tap write error (disconnect) while writing to\n";
      i = taps_.erase(i);
    }
  }

  q->deliver(msg);

  write_replicas(msg.wire());

  return true;
}

void Server::bond(Connection* con, const wire::BondRequest& br) {
  if(optref<Queue> q = queue(br.queue())) {
    if(optref<Queue> q2 = queue(br.destination())) {
      q->broadcast_into(q2.ptr());
    } else {
      con->send_error(br.destination(), "No such queue to bond into");
    }
  } else {
    con->send_error(br.queue(), "No such queue to bond at");
  }
}

void Server::write_replicas(const wire::Message& msg) {
  for(Connections::iterator i = replicas_.begin();
      i != replicas_.end();) 
  {
    if((*i)->write(msg)) {
      ++i;
    } else {
      std::cerr << "Replica error (disconnected) while writing to\n";
      i = replicas_.erase(i);
    }
  }
}

optref<Queue> Server::subscribe(Connection* con, std::string dest) {
  optref<Queue> q = queue(dest);
  if(q.set_p()) {
    q->subscribe(con);
    return q;
  }

  return optref<Queue>();
}

void Server::flush(Connection* con, std::string dest) {
  optref<Queue> q = queue(dest);
  if(q.set_p()) q->flush(con);
}

void Server::stat(Connection* con, std::string dest) {
  wire::Stat stat;
  stat.set_name(dest);

  if(optref<Queue> q = queue(dest)) {
    stat.set_exists(true);
    stat.set_transient_size(q->queued_messages());

    if(q->durable_p()) {
      stat.set_durable_size(q->durable_messages());
    }
  } else {
    stat.set_exists(false);
  }

  wire::Message msg;
  msg.set_destination("+stat");
  msg.set_payload(stat.SerializeAsString());

  if(!con->write(msg)) {
    debugs << "Connection closed returning stat information\n";
  }
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

