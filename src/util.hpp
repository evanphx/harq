#ifndef UTIL_HPP
#define UTIL_HPP

void set_nonblock(int fd);

int daemon_init(void);

void sig_term(int signo);

#endif
