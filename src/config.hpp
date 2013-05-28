#ifndef CONFIG_HPP
#define CONFIG_HPP

#include <string>
#include <sqlite3.h>
#include <list>

class Config {
public:
  class Peer {
    std::string host_;
    int port_;

  public:

    Peer(std::string h, int p)
      : host_(h)
      , port_(p)
    {}

    std::string host() {
      return host_;
    }

    int port() {
      return port_;
    }
  };

  typedef std::list<Peer> Peers;

private:
  std::string error_;

  std::string path_;
  sqlite3* db_;

  Peers peers_;

public:

  Config(std::string path)
    : path_(path)
    , db_(0)
  {}

  ~Config() {
    close();
  }

  std::string error() {
    return error_;
  }

  bool open();
  bool read();
  void close();

  void show();
};

#endif
