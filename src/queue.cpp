#include "queue.hpp"
#include "connection.hpp"
#include "flags.hpp"
#include "server.hpp"

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
    con->write(**j);
    delete *j;
  }

  transient_.clear();

  std::string val;
  leveldb::Status s = db->Get(leveldb::ReadOptions(), name_, &val);

  if(!s.ok()) {
#ifdef DEBUG
    std::cout << "No message to flush from " << name_ << "\n";
#endif
    return;
  }

  wire::Queue qi;
  qi.ParseFromString(val);

  int count = qi.count();

#ifdef DEBUG
  std::cout << "Messages to flush: " << count << "\n";
#endif

  for(int i = 0; i < count; i++) {
    std::stringstream ss;
    ss << name_;
    ss << ":";
    ss << i;

    s = db->Get(leveldb::ReadOptions(), ss.str(), &val);
    if(s.ok()) {
      con->write_raw(val);
      s = db->Delete(leveldb::WriteOptions(), ss.str());
#ifdef DEBUG
      std::cout << "Flushed message " << i << "\n";
#endif
      if(!s.ok()) {
        std::cerr << "Unable to delete " << ss.str() << "\n";
      }
    } else {
      std::cerr << "Unable to get " << ss.str() << "\n";
    }
  }

  if(qi.implicit()) {
    s = db->Delete(leveldb::WriteOptions(), name_);
#ifdef DEBUG
    std::cout << "Deleted implicit queue: " << name_ << "\n";
#endif
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

#ifdef DEBUG
      std::cout << "Writing persisted message for " << name_
                << " (" << c  << ")\n";
#endif

      c++;
      qi.set_count(c);

      leveldb::WriteBatch batch;
      batch.Put(ss.str(), msg.SerializeAsString());
      batch.Put(name_, qi.SerializeAsString());

      s = db->Write(leveldb::WriteOptions(), &batch);

      if(!s.ok()) {
        std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
      } else {
#ifdef DEBUG
        std::cout << "Updated index of " << name_ << " to " << c << "\n";
#endif
      }
    } else {
      if(msg.flags() | eQueue) {
#ifdef DEBUG
        std::cout << "Queue'd transient message at " << name_ << "\n";
#endif
        if(!msg.has_id()) msg.set_id(server_->next_id());
        queue(msg);
      } else {
#ifdef DEBUG
        std::cout << "No transient or persisted dest at " << name_ << "\n";
#endif
      }
    }
  } else {
    std::cout << "No persistance used\n";
  }

  return consumed;
}
