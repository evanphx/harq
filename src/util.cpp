#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

#include <errno.h>
#include <unistd.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/socket.h>

#include "util.hpp"
#include "server.hpp"

void set_nonblock(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    int r = fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    assert(0 <= r && "Setting socket non-block failed!");
}

int daemon_init(void) { 
    pid_t pid;
    if((pid = fork()) < 0) {
        return -1; 
    } else if(pid != 0) {
        exit(0); //parent exit
    }

    /* child continues */ 
    setsid(); /* become session leader */ 
    //chdir("/"); /* change working directory */ 
    
    umask(0); /* clear file mode creation mask */ 
    close(0); /* close stdin */ 
    close(1); /* close stdout */ 
    close(2); /* close stderr */ 
    
    return 0;
}

extern Server *server;

void sig_term(int signo) {
    if(signo == SIGTERM || signo==SIGINT) {
        printf("exiting...\n");
        if(server){
            delete server;
            server=NULL;
        }
        exit(0); 
    }
}

