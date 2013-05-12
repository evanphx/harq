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

Connection::Connection(Server *s, int fd)
  : tap_(false)
  , sock_(fd)
  , read_w_(s->loop())
  , write_w_(s->loop())
  , db_index(0)
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
  read_w_.start(sock_.fd, EV_READ);
}

void Connection::handle_message(wire::Message& msg) {
  std::string dest = msg.destination();

  if(dest == std::string("+")) {
    wire::Action act;

#ifdef DEBUG
    std::cout << "ACTION!\n";
#endif

    if(act.ParseFromString(msg.payload())) {
      ActionType type = (ActionType)act.type();
      switch(type) {
      case eSubscribe:
        subscriptions_.push_back(act.payload());
        break;
      case eTap:
        tap_ = true;
        break;
      }
    }
  } else {
#ifdef DEBUG
    std::cout << "dest='" << msg.destination() << "' "
              << "payload='" << msg.payload() << "'\n";
#endif

    server->deliver(msg);
  }
}

void Connection::deliver(wire::Message& msg) {
  std::string dest = msg.destination();

  bool deliver = tap_;

  if(!tap_) {
    for(std::list<std::string>::iterator i = subscriptions_.begin();
        i != subscriptions_.end();
        ++i) {
      if(*i == dest) {
        deliver = true;
        break;
      }
    }
  }

  if(deliver) sock_.write(msg);
}

bool Connection::do_read(int revents) {
  if(EV_ERROR & revents) {
    puts("on_readable() got error event, closing connection.");
    return false;
  }

  ssize_t recved = buffer_.fill(sock_.fd);

  if(recved == 0) return false;

  printf("Read %ld bytes\n", recved);

  if(recved <= 0) return false;

  if(state == eReadSize) {
#ifdef DEBUG
    std::cout << "READ SIZE\n";
#endif

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
#ifdef DEBUG
    std::cout << "NEED MORE\n";
#endif
    return true;
  }

#ifdef DEBUG
  std::cout << "READ MSG\n";
#endif

  wire::Message msg;

  msg.ParseFromArray(buffer_.read_pos(), need);

  buffer_.advance_read(need);

  handle_message(msg);

  state = eReadSize;

  return true;
}

void Connection::on_readable(ev::io& w, int revents)
{
  if(!do_read(revents)) {
    server->remove_connection(this);
    delete this;
    return;
  }

  /* rl_connection_reset_timeout(connection); */

  /* error: */
  /* rl_connection_schedule_close(connection); */
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

