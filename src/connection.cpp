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

#include "wire.pb.h"
#include "debugs.hpp"

#include <google/protobuf/io/zero_copy_stream_impl.h>

#define FLOW(str) debugs << "- " << str << "\n"

Connection::Connection(Server *s, int fd)
  : tap_(false)
  , ack_(false)
  , confirm_(false)
  , closing_(false)
  , sock_(fd)
  , read_w_(s->loop())
  , write_w_(s->loop())
  , server(s)
  , buffer_(1024)
  , state(eReadSize)
  , writer_started(false)
{
  read_w_.set<Connection, &Connection::on_readable>(this);
  write_w_.set<Connection, &Connection::on_writable>(this);

  sock_.set_nonblock();
  open=true;
  server->clients_num++;
}

Connection::~Connection() {
  if(open) {
    read_w_.stop();

    if(writer_started){
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

  server->clients_num--;
}

void Connection::start() {
  FLOW("New Connection");
  read_w_.start(sock_.fd, EV_READ);
}

void Connection::clear_ack(uint64_t id) {
  FLOW("Clear Ack");
  AckMap::iterator i = to_ack_.find(id);

  if(i != to_ack_.end()) {
    to_ack_.erase(i);
    debugs << "Successfully acked " << id << "\n";
  } else {
    debugs << "Unable to find id " << id << " to clear\n";
  }
}

void Connection::handle_action(wire::Action& act) {
  ActionType type = (ActionType)act.type();

  switch(type) {
  case eSubscribe:
    FLOW("ACT eSubscribe");
    subscriptions_.push_back(act.payload());
    server->subscribe(this, act.payload());
    server->flush(this, act.payload());
    break;
  case eTap:
    FLOW("ACT eTap");
    tap_ = true;
    break;
  case eDurableSubscribe:
    FLOW("ACT eDurableSubscribe");
    subscriptions_.push_back(act.payload());
    server->subscribe(this, act.payload(), true);
    server->reserve(act.payload(), false);
    // fallthrough to flush also
  case eFlush:
    FLOW("ACT eFlush");
    server->flush(this, act.payload());
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
    server->stat(this, act.payload());
    break;
  default:
    std::cerr << "Received unknown action type: " << act.type() << "\n";
    break;
  }
}

void Connection::handle_message(wire::Message& msg) {
  std::string dest = msg.destination();

  if(dest == std::string("+")) {
    wire::Action act;

    FLOW("ACTION");

    if(act.ParseFromString(msg.payload())) {
      handle_action(act);
    } else {
      std::cerr << "Unable to parse message send to '+'\n";
    }
  } else {
    server->deliver(msg);

    if(confirm_) {
      // If the sender didn't specify a confirm id, it will
      // be 0 by default, which is fine. They can sort out what that means
      // on their own.
      wire::Action oa;
      oa.set_type(eConfirm);
      oa.set_id(msg.confirm_id());

      wire::Message om;

      om.set_destination("+");

      std::string data;
      if(!oa.SerializeToString(&data)) {
        std::cerr << "Error creating confirmation message: "
                  << oa.InitializationErrorString() << "\n";
        return;
      }

      om.set_payload(data);

      write(om);
      debugs << "Sent confirmation of message id "
             << msg.confirm_id() << "\n";
    }
  }
}

void Connection::write(wire::Message& msg) {
  switch(sock_.write(msg)) {
  case eOk:
    return;
  case eFailure:
    std::cerr << "Error writing to socket\n";
    closing_ = true;
    return;
  case eWouldBlock:
    writer_started = true;
    write_w_.start(sock_.fd, EV_WRITE);
    debugs << "Starting writable watcher\n";
    return;
  }
}

bool Connection::deliver(wire::Message& msg) {
  if(closing_) return false;

  std::string dest = msg.destination();

  if(tap_) {
    write(msg);
    return false;
  }

  if(ack_) {
    to_ack_[server->assign_id(msg)] = msg;
  }

  write(msg);
  
  return true;
}

bool Connection::do_read(int revents) {
  if(EV_ERROR & revents) {
    std::cerr << "Error event detected, closing connection\n";
    closing_ = true;
    return false;
  }

  ssize_t recved = buffer_.fill(sock_.fd);

  if(recved < 0) {
    if(errno == EAGAIN || errno == EWOULDBLOCK) return false;
    std::cerr << "Error reading from socket: " << strerror(errno) << "\n";
    closing_ = true;
    return false;
  }

  if(recved == 0) return false;

  debugs << "Read " << recved << " bytes\n";

  // Allow us to parse multiple messages in one read
  for(;;) {
    if(state == eReadSize) {
      FLOW("READ SIZE");

      debugs << "avail=" << buffer_.read_available() << "\n";

      if(buffer_.read_available() < 4) return true;

      int size = buffer_.read_int32();

      debugs << "msg size=" << size << "\n";

      need = size;

      state = eReadMessage;
    }

    debugs << "avail=" << buffer_.read_available() << "\n";

    if(buffer_.read_available() < need) {
      FLOW("NEED MORE");
      return true;
    }

    FLOW("READ MSG");

    wire::Message msg;

    bool ok = msg.ParseFromArray(buffer_.read_pos(), need);

    buffer_.advance_read(need);

    if(ok) {
      handle_message(msg);
    } else {
      std::cerr << "Unable to parse request\n";
      closing_ = true;
      return false;
    }

    state = eReadSize;
  }

  return true;
}

void Connection::on_readable(ev::io& w, int revents)
{
  if(!do_read(revents)) {
    closing_ = true;

    for(AckMap::iterator i = to_ack_.begin();
        i != to_ack_.end();
        ++i) {
      FLOW("Persisting un-ack'd message");
      server->reserve(i->second.destination(), true);
      server->deliver(i->second);
    }

    for(std::list<std::string>::iterator i = subscriptions_.begin();
        i != subscriptions_.end();
        ++i) {
      server->queue(*i).unsubscribe(this);
    }

    server->remove_connection(this);
    delete this;
    return;
  }
}

void Connection::on_writable(ev::io& w, int revents) {
  FLOW("WRITE READY");

  switch(sock_.flush()) {
  case eOk:
    debugs << "Flushed socket in writable event\n";
    writer_started = false;
    write_w_.stop();
    return;
  case eFailure:
    std::cerr << "Error writing to socket in writable event\n";
    closing_ = true;
    writer_started = false;
    write_w_.stop();
    return;
  case eWouldBlock:
    debugs << "Flush didn't finish for writeable event\n";
    return;
  }
}

