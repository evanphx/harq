#include "config.hpp"

#include <iostream>

static const char* cSchema =
  "create table if not exists peers (host string, port integer);";

bool Config::open() {
  int rc = sqlite3_open(path_.c_str(), &db_);
  if(rc) {
    error_ = sqlite3_errmsg(db_);
    return false;
  }

  char* errmsg = 0;
  rc = sqlite3_exec(db_, cSchema, 0, 0, &errmsg);

  if(rc) {
    error_ = errmsg;
    sqlite3_free(errmsg);
    return false;
  }

  return true;
}

void Config::close() {
  if(db_) sqlite3_close(db_);
}

static const char* cPeerSQL = "select host,port from peers";

bool Config::read() {
  sqlite3_stmt* stmt;

  int rc = sqlite3_prepare_v2(db_, cPeerSQL, strlen(cPeerSQL), &stmt, 0);
  if(rc != SQLITE_OK) {
    error_ = sqlite3_errmsg(db_);
    return false;
  }

  rc = sqlite3_step(stmt);

  while(rc == SQLITE_ROW) {
    const char* host = (const char*)sqlite3_column_text(stmt, 0);
    int port = sqlite3_column_int(stmt, 1);

    peers_.push_back(Peer(std::string(host), port));
    rc = sqlite3_step(stmt);
  }

  sqlite3_finalize(stmt);

  return true;
}

void Config::show() {
  std::cout << peers_.size() << " peers.\n";

  for(Peers::iterator i = peers_.begin();
      i != peers_.end();
      ++i) {
    std::cout << "  " << i->host() << ":" << i->port() << "\n";
  }
}

