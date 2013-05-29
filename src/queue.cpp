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

#define DURABLE_BROKEN() std::cerr << "Durable storage broken!\n";
#define UNREACHABLE(msg) std::cerr << "Unreachable branch hit: " << msg << "\n";

void Queue::queue(const wire::Message& msg) {
  wire::Message* mp = new wire::Message(msg);
  transient_.push_back(mp);
}

unsigned Queue::durable_messages() {
  std::string val;
  leveldb::Status s = server_.db()->Get(leveldb::ReadOptions(), name_, &val);

  if(!s.ok()) return 0;

  wire::Queue qi;
  if(!qi.ParseFromString(val)) return 0;

  return qi.size();
}

bool Queue::change_kind(Queue::Kind k) {
  switch(k) {
  case eBroadcast:
    if(kind_ == eBroadcast) return true;
    return false;
  case eTransient:
    if(kind_ == eTransient) return true;
    return false;
  case eDurable:
    switch(kind_) {
    case eBroadcast:
      return false;
    case eTransient:
      return flush_to_durable();
    case eDurable:
      return true;
    }
  }
}

bool Queue::flush_to_durable() {
  // pre(kind_ == eTransient);

  server_.reserve(name_, false);

  for(Messages::const_iterator i = transient_.begin();
      i != transient_.end();
      ++i) {
    if(!write_durable(ref(*i), 0, 0)) {
      std::cerr << "Critical error flushing transient messages to durable\n";
      // Leave the rest of the messages in transient and bail hard to
      // try to at least not loose any in memory messages. We leave the
      // type as eTransient since something is wrong with durable.
      //
      return false;
    }

    // We don't delete the messages because if any fail to flush to durable,
    // we stay as transient and keep the messages.
  }

  // Ok, all messages flushed to durable, let's go ahead and cleanup the
  // transient ones.

  for(Messages::const_iterator i = transient_.begin();
      i != transient_.end();
      ++i) {
    delete *i;
  }

  transient_.clear();

  kind_ = eDurable;

  return true;
}

std::string Queue::durable_key(int i) {
  std::stringstream ss;
  ss << name_;
  ss << ":";
  ss << i;

  return ss.str();
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

  int size = qi.size();

  debugs << "Messages to flush: " << size << "\n";

  for(int i = 0; i < qi.ranges_size(); i++) {
    const wire::MessageRange& range = qi.ranges(i);

    int fin = range.start() + range.count();

    for(int j = range.start(); j < fin; j++) {
      std::string key = durable_key(j);

      s = db->Get(leveldb::ReadOptions(), key, &val);
      if(s.ok()) {
        wire::Message msg;

        if(msg.ParseFromString(val)) {
          con->deliver(msg, ref(this));
          debugs << "Flushed message " << i << "\n";
        } else {
          std::cerr << "Encountered corrupt message on disk\n";
          // TODO: what should I do here? Delete it? Keep it around and
          // make the data fairy fixes it? HMMM....
        }
      } else {
        std::cerr << "Unable to get " << key << ". Corrupt QueueInfo?\n";
        // TODO: Keep going since we assuming haven't lost anything
        // and we'll fix the Queue later.
      }
    }
  }

  // Don't reuse qi because it might be corrupt in same way, so just
  // make a fresh one.
  wire::Queue new_qi;
  new_qi.set_size(0);
  s = db->Put(leveldb::WriteOptions(), name_, new_qi.SerializeAsString());

  if(!s.ok()) {
    std::cerr << "Unable to reset " << name_ << "\n";
    // TODO: durable seems to be busted! What should we do?!?
  }

  return;
}

bool Queue::write_durable(const wire::Message& msg, std::string* out_str,
                          uint64_t* out_idx)
{
  std::string val;
  leveldb::Status s = server_.db()->Get(leveldb::ReadOptions(), name_, &val);

  wire::Queue qi;

  if(s.IsNotFound()) {
    qi.set_size(0);
  } else if(!s.ok()) {
    DURABLE_BROKEN();
    return false;
  }

  if(!qi.ParseFromString(val)) {
    // Corrupt QueueInfo. Ug.
    //
    // Fail out so that we don't trash any messages on disk that can
    // be recovered.
    //
    // TODO perhaps this should be a paranoid option, whether or not
    // to reset the info or fail spectacularly.
    //
    std::cerr << "Corrupt queue info detected, unable to write durable\n";
    return false;
  }

  // Add the message to the end of the last range always.

  int idx = 0;

  if(qi.ranges_size() == 0) {
    wire::MessageRange* r = qi.add_ranges();
    r->set_start(0);
    r->set_count(1);
  } else {
    int range = qi.ranges_size() - 1;
    wire::MessageRange* r = qi.mutable_ranges(range);
    idx = r->start() + r->count();
    r->set_count(r->count() + 1);
  }

  std::string key = durable_key(idx);

  debugs << "Writing persisted message for " << name_
         << " (" << idx << ")\n";

  qi.set_size(qi.size() + 1);

  leveldb::WriteBatch batch;
  batch.Put(key,   msg.SerializeAsString());
  batch.Put(name_, qi.SerializeAsString());

  s = server_.db()->Write(leveldb::WriteOptions(), &batch);

  if(!s.ok()) {
    std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
    // TODO: durable is busted! What to do?!
    return false;
  } else {
    if(out_str) *out_str = key;
    if(out_idx) *out_idx = idx;
    debugs << "Updated index of " << name_ << "\n";
    return true;
  }
}

bool Queue::erase_durable(wire::Message& msg, std::string& str, int idx) {
  std::string val;
  leveldb::Status s = server_.db()->Get(leveldb::ReadOptions(), name_, &val);

  wire::Queue qi;

  if(!s.ok()) {
    DURABLE_BROKEN();
    return false;
  }

  if(!qi.ParseFromString(val)) {
    // Corrupt QueueInfo. Ug.
    //
    // Fail out so that we don't trash any messages on disk that can
    // be recovered.
    //
    // TODO perhaps this should be a paranoid option, whether or not
    // to reset the info or fail spectacularly.
    //
    std::cerr << "Corrupt queue info detected, unable to write durable\n";
    return false;
  }

  // Fixup the ranges to remove idx.

  if(qi.ranges_size() == 0) {
    // WTF. Well, indicate the weird situation and then continue on.
    std::cerr << "Attempted to delete from queue with no ranges.\n";
    return false;
  }

  google::protobuf::RepeatedPtrField<wire::MessageRange>* ranges = qi.mutable_ranges();

  for(int range = 0; range < qi.ranges_size(); range++) {
    wire::MessageRange* r = ranges->Mutable(range);

    // Each case that results in a different range change is seperated
    // out for clarity.
    //
    if(idx == r->start()) {
      // There was only one message, so we just nuke the range.
      if(r->count() == 0) {
        ranges->DeleteSubrange(range, 1);
      } else {
        // Shrink the range upward.
        r->set_start(r->start() + 1);
        r->set_count(r->count() - 1);
      }

      goto write;
    }

    // Ok, idx is in this range.
    if(idx > r->start() && idx < r->start() + r->count()) {
      // It's the last message, so just decrement count.
      if(idx == r->start() + r->count() - 1) {
        r->set_count(r->count() - 1);
      } else {
        // Ok, it's in the middle, so we have to split the range.

        // Make a new record at the end.
        ranges->Add();

        // Now put the new range into the right position.
        int target = range + 1;

        // We move from the end towards target, swapping elements
        // until nr is in the right position.
        for(int j = ranges->size() - 1; j > target; j--) {
          ranges->SwapElements(j, j-1);
        }

        // This will now be our fresh record.
        wire::MessageRange* nr = ranges->Mutable(target);

        nr->set_start(idx + 1);
        nr->set_count((r->start() + r->count()) - idx);

        r->set_count(idx - r->start());
      }

      goto write;
    }
  }

  // Fell through for and didn't find a range.
  std::cerr << "Unable to find message " << idx << " in queue " << name_ << "\n";
  return false;

write:

  std::string key = durable_key(idx);

  debugs << "Erasing persisted message for " << name_
         << " (" << idx << ")\n";

  qi.set_size(qi.size() - 1);

  leveldb::WriteBatch batch;
  batch.Delete(key);
  batch.Put(name_,  qi.SerializeAsString());

  s = server_.db()->Write(leveldb::WriteOptions(), &batch);

  if(!s.ok()) {
    std::cerr << "Unable to write message to DB: " << s.ToString() << "\n";
    // TODO: durable is busted! What to do?!
    return false;
  } else {
    debugs << "Updated index of " << name_ << "\n";
    return true;
  }
}

bool Queue::deliver(const wire::Message& msg) {
  // With broadcast, we don't support acks because wtf would that
  // even mean? So we handle it specially and invoke
  // Connection::write to just write the message directly to the
  // client.
  //
  if(kind_ == eBroadcast) {
    for(Connections::iterator i = subscribers_.begin();
        i != subscribers_.end();
        ++i) {
      Connection* con = *i;
      con->write(msg);
    }

    return true;
  }

  // If no one is subscribed, then queue it directly.
  if(subscribers_.empty()) {
    if(kind_ == eTransient) {
      queue(msg);
    } else {
      write_durable(msg,0,0);
    }

    return true;
  }

  // Here is where we load balance over subscribers_.
  Connection* con = subscribers_.front();
  subscribers_.pop_front();
  subscribers_.push_back(con);

  // If the connection requires ack'ing and the queue is in
  // durable mode, then we need to record the info about where
  // the message is in durable storage so we can delete it
  // later. This is done via callbacks from deliver as it
  // figures out how the connection needs the message to be
  // managed.

  con->deliver(msg, ref(this));

  return true;
}

void Queue::recorded_ack(AckRecord& rec) {
  switch(kind_) {
  case eBroadcast:
    UNREACHABLE("Recorded ack on broadcast queue");
    break;
  case eTransient:
    queue(rec.msg);
    break;
  case eDurable:
    if(!write_durable(rec.msg, &rec.durable_key, &rec.durable_idx)) {
      std::cerr << "Error saving messsage to durable!\n";
      // In this case, we really don't want to loose messages.
      // So we queue the message in memory at least.
      queue(rec.msg);
    }
    break;
  }
}

void Queue::acked(AckRecord& rec) {
  switch(kind_) {
  case eBroadcast:
    UNREACHABLE("Received ack on broadcast queue");
    break;
  case eTransient:
    // Nothing!
    break;
  case eDurable:
    if(!erase_durable(rec.msg, rec.durable_key, rec.durable_idx)) {
      std::cerr << "Error deleting messsage from durable!\n";
    }
    break;
  }

}
