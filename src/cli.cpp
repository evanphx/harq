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

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"

#include "wire.pb.h"

#include <google/protobuf/io/zero_copy_stream_impl.h>

int cli(int argc, char** argv) {
  int s, rv;
  char _port[6];  /* strlen("65535"); */
  struct addrinfo hints, *servinfo, *p;

  if(argc < 3) {
    printf("Usage: cli <dest> <payload>\n");
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

  wire::Message msg;

  msg.set_destination(argv[1]);
  msg.set_payload(argv[2]);

  google::protobuf::io::FileOutputStream stream(s);

  int size = msg.ByteSize();

  write(s, &size, sizeof(int));

  msg.SerializeToZeroCopyStream(&stream);

  printf("Wrote %d!\n", size);
  return 0;
}
