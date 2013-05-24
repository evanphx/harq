#include "queue.hpp"
#include "connection.hpp"
#include "flags.hpp"
#include "server.hpp"
#include "debugs.hpp"

#include "wire.pb.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <sstream>
#include <iostream>

void Queue::queue(wire::Message& msg) {
  wire::Message* mp = new wire::Message(msg);
  transient_.push_back(mp);
}

void Queue::flush(Connection* con, leveldb::DB* db) {
  for(Messages::iterator j = transient_.begin();
      j != transient_.end();
      ++j) {
    con->write(ref(*j));
    delete *j;
  }

  transient_.clear();

  std::string val;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), name_, &val);

  if(!s.ok()) {
    debugs << "No message to flush from " << name_ << "\n";
    return;
  }

  wire::Queue qi;
  qi.ParseFromString(val);

  int count = qi.count();

  debugs << "Messages to flush: " << count << "\n";

  for(int i = 0; i < count; i++) {
    std::stringstream ss;
    ss << name_;
    ss << ":";
    ss << i;

    s = db->Get(leveldb::ReadOptions(), ss.str(), &val);
    if(s.ok()) {
      wire::Message msg;

      if(msg.ParseFromString(val)) {
        con->deliver(msg);

        s = db->Delete(leveldb::WriteOptions(), ss.str());
        debugs << "Flushed message " << i << "\n";
        if(!s.ok()) {
          std::cerr << "Unable to delete " << ss.str() << "\n";
        }
      } else {
        std::cerr << "Encountered corrupt message on disk\n";
      }
    } else {
      std::cerr << "Unable to get " << ss.str() << "\n";
    }
  }

  if(qi.implicit()) {
    s = db->Delete(leveldb::WriteOptions(), name_);
    debugs << "Deleted implicit queue: " << name_ << "\n";
  } else {
    qi.set_count(0);
    s = db->Put(leveldb::WriteOptions(), name_, qi.SerializeAsString());
  }

  if(!s.ok()) {
    std::cerr << "Unable to reset " << name_ << "\n";
  }

  return;
}

bool Queue::deliver(wire::Message& msg, leveldb::DB* db) {
  bool consumed = false;

  for(Connections::iterator i = subscribers_.begin();
      i != subscribers_.end();
      ++i) {
    Connection* con = *i;
    consumed |= con->deliver(msg);
  }

  if(!consumed) {
    std::string val;
    wire::Queue qi;
    leveldb::Status s = db->Get(leveldb::ReadOptions(), name_, &val);

    if(s.ok()) {
      qi.ParseFromString(val);

      int c = qi.count();
      std::stringstream ss;
      ss << name_;
      ss << ":";
      ss << c;

      debugs << "Writing persisted message for " << name_
             << " (" << c  << ")\n";

      c++;
      qi.set_count(c);

      leveldb::WriteBatch batch;
      batch.Put(ss.str(), msg.SerializeAsString());
      batch.Put(name_, qi.SerializeAsString());

      s = db->Write(leveldb::WriteOptions(), &batch);

      if(!s.ok()) {
        std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
      } else {
        debugs << "Updated index of " << name_ << " to " << c << "\n";
      }
    } else {
      if(msg.flags() | eQueue) {
        debugs << "Queue'd transient message at " << name_ << "\n";
        queue(msg);
      } else {
        debugs << "No transient or persisted dest at " << name_ << "\n";
      }
    }
  } else {
    debugs << "No persistance used\n";
  }

  return consumed;
}
