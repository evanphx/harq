#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <sys/socket.h>

#include <iostream>

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"

extern char *optarg;

Server *server=NULL;

extern int cli(int argc, char** argv);

int main(int argc, char** argv) {
  if(argv[1] && strcmp(argv[1], "cli") == 0) {
    return cli(argc-1, argv+1);
  }

  bool daemon = false;

  std::string host = "";

  int port=7621;

  std::string data_dir = "harq.db";

  int ch = 0;
  while((ch = getopt(argc, argv, "hDb:p:d:")) != -1) {
    switch(ch) {
    default:
    case 'h':
      std::cout
        << "Usage:\n\t./harq [options]\n"
        << "Options:\n"
        << "\t-D:\t\t daemon\n"
        << "\t-b host-ip:\t listen host\n"
        << "\t-p port:\t listen port\n"
        << "\t-d data-dir:\t data dir\n";

      exit(0);
    case 'D':
      daemon = true;
      break;
    case 'b':
      host = optarg;
      break;
    case 'p':
      port = (int)strtol(optarg, (char **)NULL, 10);
      if(!port){
        printf("Bad port(-p) value\n");
        exit(1);
      }
      break;
    case 'd':
      data_dir = optarg;
      break;
    }
  }

  if(daemon) {
    if(daemon_init() == -1) { 
      printf("can't run as daemon\n"); 
      exit(1);
    }
  }

  signal(SIGTERM, sig_term);
  signal(SIGINT,  sig_term);
  signal(SIGPIPE, SIG_IGN);

  Server server(data_dir, host, port);
  server.start();

  return 0;
}

