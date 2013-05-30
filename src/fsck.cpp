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
#include <sstream>

#include "util.hpp"
#include "server.hpp"
#include "connection.hpp"
#include "action.hpp"
#include "flags.hpp"
#include "debugs.hpp"
#include "json.hpp"

#include "wire.pb.h"

#include "leveldb/db.h"

using namespace leveldb;

int fsck(int argc, char** argv) {
  const char* path;

  if(argc == 2) {
    path = argv[1];
  } else {
    path = "./harq.db";
  }

  std::cout << "Checking " << path << "...\n";

  Options opts;
  opts.paranoid_checks = true;

  ReadOptions ro;
  ro.verify_checksums = true;

  DB* db;
  Status s = DB::Open(opts, path, &db);
  if(!s.ok()) {
    std::cout << "Unable to open: " << s.ToString() << "\n";
    return 1;
  }

  std::string val;
  wire::QueueConfiguration cfg;

  s = db->Get(ro, "!harq.config", &val);
  if(!s.ok()) {
    std::cout << "Unable to read configuration: " << s.ToString() << "\n";
    return 1;
  }

  if(!cfg.ParseFromString(val)) {
    std::cout << "Corrupt configuration detected.\n";
    return 1;
  }

  std::cout << cfg.queues_size() << " queues detected.\n";

  bool some_bad = false;

  for(int i = 0; i < cfg.queues_size(); i++) {
    const wire::QueueDeclaration& decl = cfg.queues(i);

    std::cout << "name: " << decl.name() << "\n";

    switch(decl.type()) {
    case wire::QueueDeclaration::eBroadcast:
      std::cout << "  type: broadcast\n";
      break;
    case wire::QueueDeclaration::eTransient:
      std::cout << "  type: transient\n";
      break;
    case wire::QueueDeclaration::eDurable:
      std::cout << "  type: durable\n";
      break;
    default:
      std::cout << "  type: UNKNOWN(" << decl.type() << ")\n";
      break;
    }

    s = db->Get(ro, decl.name(), &val);
    if(!s.ok()) {
      std::cout << "  Unable to find queue on disk!\n";
    } else {
      wire::Queue qi;
      if(!qi.ParseFromString(val)) {
        std::cout << "  Queue information corrupt on disk!\n";
      } else {
        std::cout << "  total messages: " << qi.size() << "\n"
                  << "  ranges:\n";

        for(int j = 0; j < qi.ranges_size(); j++) {
          const wire::MessageRange& range = qi.ranges(j);
          std::cout << "    " << range.start() << " - "
                    << range.start() + range.count() << "\n";
        }

        int valid = 0;

        for(int j = 0; j < qi.ranges_size(); j++) {
          const wire::MessageRange& range = qi.ranges(j);

          int fin = range.start() + range.count();

          for(int m = range.start(); m < fin; m++) {
            std::stringstream tmp;
            tmp << decl.name() << ":" << m;

            s = db->Get(ro, tmp.str(), &val);
            if(!s.ok()) {
              std::cerr << "Missing message '" << tmp.str() << "'\n";
            } else {
              wire::Message msg;
              if(!msg.ParseFromString(val)) {
                some_bad = true;
                std::cerr << "Corrupt message detected '" << tmp.str() << "'\n";
              } else {
                valid++;
              }
            }
          }
        }

        std::cout << "  valid messages: " << valid << "\n";
      }
    }
  }

  return some_bad ? 1 : 0;
}
