#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/socket.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

#include <errno.h>

#include <algorithm>
#include <iostream>

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"
#include "action.hpp"
#include "flags.hpp"

#include "wire.pb.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>

int cli(int argc, char** argv) {
  int s, rv;
  char _port[6];  /* strlen("65535"); */
  struct addrinfo hints, *servinfo, *p;

  if(argc < 2) {
    printf("Usage: cli <dest> [<payload>]\n");
    return 1;
  }

  snprintf(_port, 6, "%d", 7621);
  memset(&hints,0,sizeof(hints));
  hints.ai_family = AF_INET;
  hints.ai_socktype = SOCK_STREAM;

  if ((rv = getaddrinfo("127.0.0.1",_port,&hints,&servinfo)) != 0) {
    printf("Error: %s\n", gai_strerror(rv));
    return 1;
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
    return 1;
  }

end:
  freeaddrinfo(servinfo);

  Socket sock(s);

  if(argc == 3) {
    wire::Message msg;

    if(getenv("CONFIRM")) {

      wire::Action act;
      act.set_type(eRequestConfirm);

      msg.set_destination("+");
      msg.set_payload(act.SerializeAsString());
      sock.write(msg);

      msg.set_confirm_id(7);
    }

    msg.set_destination(argv[1]);
    msg.set_payload(argv[2]);

    if(getenv("QUEUE")) {
      msg.set_flags(eQueue);
    }

    int size = sock.write(msg);

    std::cout << "Sent " << size << " bytes to " << argv[1] << "\n";

    if(getenv("CONFIRM")) {
      wire::Message in;
      sock.read(in);

      if(in.destination() == "+") {
        wire::Action act;

        act.ParseFromString(in.payload());

        if(act.id() == 7) {
          std::cout << "Confirmed message received\n";
        }
      }
    }

  } else {
    bool use_acks = (getenv("REQ_ACK") != 0);

    if(use_acks) {
      wire::Action act;

      act.set_type(eRequestAck);

      wire::Message msg;

      msg.set_destination("+");
      msg.set_payload(act.SerializeAsString());

      sock.write(msg);
    }

    wire::Action act;

    if(std::string(argv[1]) == "-t") {
      act.set_type(eTap);
      std::cout << "Tapped all messages\n";
    } else {
      if(*argv[1] == '+') {
        act.set_type(eDurableSubscribe);
        act.set_payload(argv[1] + 1);
      } else {
        act.set_type(eSubscribe);
        act.set_payload(argv[1]);
      }

      std::cout << "Listening on " << argv[1] << "\n";
    }

    wire::Message msg;

    msg.set_destination("+");
    msg.set_payload(act.SerializeAsString());

    sock.write(msg);

    for(;;) {
      wire::Message in;
      if(!sock.read(in)) {
        std::cout << "Socket closed by server\n";
        return 0;
      }

      std::cout << "{\n  'destination': '" << in.destination() << "',\n"
                << "  'payload': '" << in.payload() << "'\n}\n";

      if(use_acks) {
        if(getenv("ACK_CRASH")) return -1;
        if(in.has_id()) {
          act.set_type(eAck);
          act.set_id(in.id());

          msg.set_destination("+");
          msg.set_payload(act.SerializeAsString());

          sock.write(msg);
        } else {
          std::cerr << "Wanted to ACK a message with no id\n";
        }
      }

      if(getenv("ONCE")) break;
    }
  }

  return 0;
}
