#include <algorithm>
#include <iostream>

#include <stdio.h>
#include <string.h>

#include <errno.h>
#include <assert.h>
#include <unistd.h>
#include <sys/socket.h>

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"
#include "action.hpp"
#include "message.hpp"

#include "wire.pb.h"
#include "debugs.hpp"

#include <google/protobuf/io/zero_copy_stream_impl.h>

#define FLOW(str) debugs << "- " << str << "\n"

Connection::Connection(Server& s, int fd)
  : tap_(false)
  , ack_(false)
  , confirm_(false)
  , closing_(false)
  , replica_(false)
  , sock_(fd)
  , read_w_(s.loop())
  , write_w_(s.loop())
  , open_(true)
  , server_(s)
  , buffer_(1024)
  , state_(eReadSize)
  , writer_started_(false)
{
  read_w_.set<Connection, &Connection::on_readable>(this);
  write_w_.set<Connection, &Connection::on_writable>(this);

  sock_.set_nonblock();
}

Connection::~Connection() {
  if(open_) {
    read_w_.stop();

    if(writer_started_) {
      write_w_.stop();
    }

    for(;;) {
      int ret = close(sock_.fd);
      if(ret == 0) break;
      if(errno == EINTR) continue;

      std::cerr << "Error while closing fd " << sock_.fd
                << " (" << errno << ", " << strerror(errno) << ")\n";
      break;
    }
  }
}

void Connection::start() {
  FLOW("New Connection");
  read_w_.start(sock_.fd, EV_READ);
}

void Connection::start_replica() {
  FLOW("New Replica Connection");
  read_w_.start(sock_.fd, EV_READ);

  wire::ReplicaAction act;
  act.set_type(wire::ReplicaAction::eStart);

  wire::Message msg;
  msg.set_destination("+replica");
  msg.set_payload(act.SerializeAsString());

  if(!write(msg)) {
    debugs << "Master disconnected while starting as replica\n";
  }
}

void Connection::clear_ack(uint64_t id) {
  FLOW("Clear Ack");
  AckMap::iterator i = to_ack_.find(id);

  if(i != to_ack_.end()) {
    i->second.queue.acked(i->second);
    to_ack_.erase(i);
    debugs << "Successfully acked " << id << "\n";
  } else {
    debugs << "Unable to find id " << id << " to clear\n";
  }
}

void Connection::handle_action(const wire::Action& act) {
  ActionType type = (ActionType)act.type();

  switch(type) {
  case eSubscribe:
    FLOW("ACT eSubscribe");
    subscriptions_.push_back(act.payload());
    server_.subscribe(this, act.payload());
    server_.flush(this, act.payload());
    break;
  case eTap:
    FLOW("ACT eTap");
    if(!tap_) {
      tap_ = true;
      server_.add_tap(this);
    }
    break;
  case eDurableSubscribe:
    FLOW("ACT eDurableSubscribe");
    subscriptions_.push_back(act.payload());
    server_.subscribe(this, act.payload(), true);
    server_.reserve(act.payload());
    // fallthrough to flush also
  case eFlush:
    FLOW("ACT eFlush");
    server_.flush(this, act.payload());
    break;
  case eRequestAck:
    FLOW("ACT eRequestAck");
    ack_ = true;
    break;
  case eAck:
    FLOW("ACT eAck");
    if(act.has_id()) {
      clear_ack(act.id());
    } else {
      std::cerr << "Received ACK with no id\n";
    }
    break;
  case eRequestConfirm:
    FLOW("ACT eRequestConfirm");
    confirm_ = true;
    break;
  case eConfirm:
    FLOW("ACT eConfirm");
    std::cerr << "Server recieved Confirm action mistakenly\n";
    break;
  case eRequestStat:
    FLOW("ACT eRequestStat");
    server_.stat(this, act.payload());
    break;
  case eMakeBroadcastQueue:
    FLOW("ACT eMakeBroadcastQueue");
    make_queue(act.payload(), Queue::eBroadcast);
    break;
  case eMakeTransientQueue:
    FLOW("ACT eMakeTransientQueue");
    make_queue(act.payload(), Queue::eTransient);
    break;
  case eMakeDurableQueue:
    FLOW("ACT eMakeDurableQueue");
    make_queue(act.payload(), Queue::eDurable);
    break;
  case eBond:
    FLOW("ACT eBond");
    {
      wire::BondRequest br;
      if(br.ParseFromString(act.payload())) {
        server_.bond(this, br);
        debugs << "Bonded a broadcast queue\n";
      } else {
        std::cerr << "Recieved malformed bond request\n";
        send_error("+", "Bad bond request");
      }
    }
    break;
  default:
    std::cerr << "Received unknown action type: " << act.type() << "\n";
    break;
  }
}

void Connection::make_queue(std::string name, Queue::Kind k) {
  if(!server_.make_queue(name, k)) {
    send_error(name, "Unable to change queue type");
  }
}

void Connection::send_error(std::string name, std::string error) {
  wire::QueueError err;
  err.set_queue(name);
  err.set_error(error);

  wire::Action act;
  act.set_type(eQueueError);
  act.set_payload(err.SerializeAsString());

  wire::Message msg;
  msg.set_destination("+");
  msg.set_payload(act.SerializeAsString());

  if(!write(msg)) {
    debugs << "Connection closed while writing error\n";
  }
}

void Connection::handle_replica(const wire::ReplicaAction& act) {
  switch(act.type()) {
  case wire::ReplicaAction::eStart:
    if(!replica_) {
      server_.add_replica(this);
      replica_ = true;
    }
    break;
  case wire::ReplicaAction::eReserve:
    server_.reserve(act.payload());
    break;
  default:
    std::cerr << "Received unknown replica action: " << act.type() << "\n";
    return;
  }
}

void Connection::handle_message(const Message& msg) {
  std::string dest = msg->destination();

  if(dest == std::string("+")) {
    wire::Action act;

    FLOW("ACTION");

    if(act.ParseFromString(msg->payload())) {
      handle_action(act);
    } else {
      std::cerr << "Unable to parse message send to '+'\n";
    }
  } else if(dest == std::string("+replica")) {
    wire::ReplicaAction act;

    FLOW("REPLICA ACTION");

    if(act.ParseFromString(msg->payload())) {
      handle_replica(act);
    } else {
      std::cerr << "Unable to parse message send to '+replica'\n";
    }
  } else {
    Message out = msg;
    if(!server_.deliver(out)) {
      send_error(dest, "No such queue");
    } else if(confirm_) {
      // If the sender didn't specify a confirm id, it will
      // be 0 by default, which is fine. They can sort out what that means
      // on their own.
      wire::Action oa;
      oa.set_type(eConfirm);
      oa.set_id(msg->confirm_id());

      wire::Message om;

      om.set_destination("+");

      std::string data;
      if(oa.SerializeToString(&data)) {
        om.set_payload(data);

        if(write(om)) {
          debugs << "Sent confirmation of message id "
                 << msg->confirm_id() << "\n";
        } else {
          debugs << "Connection closed while writing confirmation\n";
        }
      } else {
        std::cerr << "Error creating confirmation message: "
                  << oa.InitializationErrorString() << "\n";
      }
    }
  }
}

bool Connection::write(const Message& msg) {
  return write(msg.wire());
}

bool Connection::write(const wire::Message& msg) {
  switch(sock_.write(msg)) {
  case eOk:
    return true;
  case eFailure:
    debugs << "Error writing to socket\n";
    signal_cleanup();
    return false;
  case eWouldBlock:
    writer_started_ = true;
    write_w_.start(sock_.fd, EV_WRITE);
    debugs << "Starting writable watcher\n";
    return true;
  }
}

DeliverStatus Connection::deliver(Message& msg, Queue& from) {
  if(closing_) return eIgnored;

  if(ack_) {
    uint64_t id = server_.assign_id(msg.wire());

    std::pair<AckMap::iterator, bool> ret;

    ret = to_ack_.insert(AckMap::value_type(id, AckRecord(msg, from)));

    from.recorded_ack(ret.first->second);

    if(!write(msg)) return eIgnored;

    return eWaitForAck;
  }

  if(!write(msg)) return eIgnored;
  return eConsumed;
}

bool Connection::do_read(int revents) {
  if(EV_ERROR & revents) {
    std::cerr << "Error event detected, closing connection\n";
    return false;
  }

  ssize_t recved = buffer_.fill(sock_.fd);

  if(recved < 0) {
    if(errno == EAGAIN || errno == EWOULDBLOCK) return false;
    debugs << "Error reading from socket: " << strerror(errno) << "\n";
    return false;
  }

  if(recved == 0) return false;

  debugs << "Read " << recved << " bytes\n";

  // Allow us to parse multiple messages in one read
  for(;;) {
    if(state_ == eReadSize) {
      FLOW("READ SIZE");

      debugs << "avail=" << buffer_.read_available() << "\n";

      if(buffer_.read_available() < 4) return true;

      int size = buffer_.read_int32();

      debugs << "msg size=" << size << "\n";

      need_ = size;

      state_ = eReadMessage;
    }

    debugs << "avail=" << buffer_.read_available() << "\n";

    if(buffer_.read_available() < need_) {
      FLOW("NEED MORE");
      return true;
    }

    FLOW("READ MSG");

    Message msg;

    bool ok = msg.wire().ParseFromArray(buffer_.read_pos(), need_);

    buffer_.advance_read(need_);

    if(ok) {
      handle_message(msg);
    } else {
      std::cerr << "Unable to parse request\n";
      return false;
    }

    state_ = eReadSize;
  }

  return true;
}

void Connection::on_readable(ev::io& w, int revents) {
  if(!do_read(revents)) signal_cleanup();
}

void Connection::unsubscribe() {
  for(std::list<std::string>::iterator i = subscriptions_.begin();
      i != subscriptions_.end();
      ++i) {
    optref<Queue> ref = server_.queue(*i);
    if(ref.set_p()) {
      ref->unsubscribe(this);
    }
  }
}

void Connection::cleanup() {
  for(AckMap::iterator i = to_ack_.begin();
      i != to_ack_.end();
      ++i) {
    FLOW("Persisting un-ack'd message");
    i->second.queue.deliver(i->second.msg);
  }
}

void Connection::signal_cleanup() {
  if(closing_) return;

  closing_ = true;

  // Unsubscribe now, which is early, so that during this cycle we don't
  // consider closing connections.

  unsubscribe();
  server_.remove_connection(this);
}

void Connection::on_writable(ev::io& w, int revents) {
  FLOW("WRITE READY");

  switch(sock_.flush()) {
  case eOk:
    debugs << "Flushed socket in writable event\n";
    writer_started_ = false;
    write_w_.stop();
    return;
  case eFailure:
    std::cerr << "Error writing to socket in writable event\n";
    signal_cleanup();
    writer_started_ = false;
    write_w_.stop();
    return;
  case eWouldBlock:
    debugs << "Flush didn't finish for writeable event\n";
    return;
  }
}

