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

#include <google/protobuf/io/zero_copy_stream_impl.h>

#define FLOW(str) std::cout << "- " << str << "\n"

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
  // write_w_->set<Connection, &Connection::on_writable>(this);

  timeout_watcher.data = this;

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

    close(sock_.fd);
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
#ifdef DEBUG
    std::cout << "Successfully acked " << id << "\n";
#endif
  } else {
#ifdef DEBUG
    std::cout << "Unable to find id " << id << " to clear\n";
#endif
  }
}

void Connection::handle_message(wire::Message& msg) {
  std::string dest = msg.destination();

  if(dest == std::string("+")) {
    wire::Action act;

    FLOW("ACTION");

    if(act.ParseFromString(msg.payload())) {
      ActionType type = (ActionType)act.type();
      switch(type) {
      case eSubscribe:
        FLOW("ACT eSubscribe");
        subscriptions_.push_back(act.payload());
        server->flush(this, act.payload());
        break;
      case eTap:
        FLOW("ACT eTap");
        tap_ = true;
        break;
      case eDurableSubscribe:
        FLOW("ACT eDurableSubscribe");
        subscriptions_.push_back(act.payload());
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
          std::cerr << "Recieved ACK with no id\n";
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
      }
    }
  } else {
#ifdef DEBUG
    std::cout << "dest='" << msg.destination() << "' "
              << "payload='" << msg.payload() << "'\n";
#endif

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
      om.set_payload(oa.SerializeAsString());

      sock_.write(om);
#ifdef DEBUG
      std::cout << "Sent confirmation of message id " << msg.confirm_id() << "\n";
#endif
    }
  }
}

bool Connection::deliver(wire::Message& msg) {
  if(closing_) return false;

  std::string dest = msg.destination();

  if(tap_) {
    sock_.write(msg);
    return false;
  }

  for(std::list<std::string>::iterator i = subscriptions_.begin();
      i != subscriptions_.end();
      ++i) {
    if(*i == dest) {
      if(ack_) {
        uint64_t id = server->next_id();

        msg.set_id(id);

#ifdef DEBUG
        std::cout << "Assign message id: " << id << "\n";
#endif

        to_ack_[id] = msg;
      }

      sock_.write(msg);
      return true;
    }
  }

  return false;
}

bool Connection::do_read(int revents) {
  if(EV_ERROR & revents) {
    puts("on_readable() got error event, closing connection.");
    return false;
  }

  ssize_t recved = buffer_.fill(sock_.fd);

  if(recved == 0) return false;

#ifdef DEBUG
  printf("Read %ld bytes\n", recved);
#endif

  if(recved <= 0) return false;

  // Allow us to parse multiple messages in one read
  for(;;) {
    if(state == eReadSize) {
      FLOW("READ SIZE");

      if(buffer_.read_available() < 4) return true;

      int size = buffer_.read_int32();

#ifdef DEBUG
      std::cout << "msg size=" << size << "\n";
#endif

      need = size;

      state = eReadMessage;
    }

#ifdef DEBUG
    std::cout << "avail=" << buffer_.read_available() << "\n";
#endif

    if(buffer_.read_available() < need) {
      FLOW("NEED MORE");
      return true;
    }

    FLOW("READ MSG");

    wire::Message msg;

    msg.ParseFromArray(buffer_.read_pos(), need);

    buffer_.advance_read(need);

    handle_message(msg);

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

    server->remove_connection(this);
    delete this;
    return;
  }

  /* rl_connection_reset_timeout(connection); */

  /* error: */
  /* rl_connection_schedule_close(connection); */
}

void Connection::write_raw(std::string val) {
  int w = sock_.write_raw(val);
#ifdef DEBUG
  std::cout << "Wrote " << w << " bytes to client\n";
#endif
  if(ack_) {
    wire::Message msg;
    msg.ParseFromString(val);

    if(msg.has_id()) {
      to_ack_[msg.id()] = msg;
    } else {
      std::cerr << "Tried to ack-save a message with no id (flushed)\n";
    }
  }

}

int Connection::do_write() {
  /*
    size_t nleft=write_buffer.size();
    ssize_t nwritten=0;
    const char *ptr=write_buffer.c_str();

    if ((nwritten = write(fd, ptr, nleft)) < 0) {
        if (nwritten < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)){
            return 0;
        }else{
            perror("Write Error Msg");
            writer_started=false;
            ev_io_stop(server->loop_, &write_watcher);
            return -1;
        }
    }
    write_buffer.erase(0,nwritten);
    if(write_buffer.size()<=0){
        writer_started=false;
        ev_io_stop(server->loop_, &write_watcher);
    }
    */
    return 1;
}


void Connection::on_writable(ev::io& w, int revents)
{
  /*
    Connection *connection = static_cast<Connection*>(watcher->data);
    int ret = connection->do_write();
    switch(ret) {
    case -1:
        puts("write error");
        break;
    case 0:
        //unwritable
        break;
    case 1:
        //done
        break;
    default:
        puts("unknown return error");
        break;
    }
    */
}

