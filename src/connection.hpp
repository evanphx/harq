#ifndef CONNECTION_HPP
#define CONNECTION_HPP

#include <vector>
#include <string>

#include <ev.h>
#include <leveldb/c.h>

#include "qadmus.hpp"

class Server;


class Connection {

public:
    bool open;
    int db_index;
    int fd;
    Server *server;
    // Request *current_request;
    // Request *transaction;

    char *next_idx;        /* next val to handle*/
    int buffered_data;   /* data has been read */
    char read_buffer[READ_BUFFER];

    enum State { eReadSize, eReadMessage } state;

    char* next_data;
    int need;

    bool writer_started;
    std::string write_buffer;

    ev_io write_watcher;
    ev_io read_watcher;
    ev_timer timeout_watcher;
    ev_timer goodbye_watcher;

    /*** methods ***/

    Connection(Server *s, int fd);
    ~Connection();

    static void on_readable(struct ev_loop *loop, ev_io *watcher, int revents);
    static void on_writable(struct ev_loop *loop, ev_io *watcher, int revents);

    void start();
    size_t get_int();
    int  do_read();
    int  do_write();
    void do_request();

    /*
    void write_nil();
    void write_error(const char* msg);
    void write_status(const char* msg);
    void write_integer(const char *out, size_t out_size);
    void write_bulk(const char *out, size_t out_size);
    void write_bulk(const std::string &out);
    void write_mbulk_header(int n);
    */
};

#endif
