#include "queue.hpp"
#include "connection.hpp"
#include "flags.hpp"
#include "server.hpp"
#include "debugs.hpp"
#include "message.hpp"

#include "wire.pb.h"
#include "leveldb/db.h"
#include "leveldb/write_batch.h"

#include <sstream>
#include <iostream>

#define DURABLE_BROKEN() std::cerr << "Durable storage broken!\n";
#define UNREACHABLE(msg) std::cerr << "Unreachable branch hit: " << msg << "\n";

void Queue::write_transient(const Message& msg) {
  transient_.push_back(msg);
}

unsigned Queue::durable_messages() {
  wire::Queue qi;

  switch(server_.read_queue(name_, qi)) {
  case eValid:
    // Ok!
    break;
  case eMissing:
  case eInvalid:
    return 0;
  }

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

  server_.reserve(name_);

  for(Messages::iterator i = transient_.begin();
      i != transient_.end();
      ++i) {
    if(!write_durable(*i)) {
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

  transient_.clear();

  kind_ = eDurable;

  return true;
}

std::string Queue::durable_key(int i) {
  std::stringstream ss;
  ss << "-";
  ss << name_;
  ss << ":";
  ss << i;

  return ss.str();
}

void Queue::flush(Connection* con, leveldb::DB* db) {
  for(Messages::iterator j = transient_.begin();
      j != transient_.end();)
  {
    if(con->write(*j)) {
      j = transient_.erase(j);
    } else {
      debugs << "Error while flushing transient messages.\n";
      return;
    }
  }

  wire::Queue qi;

  switch(server_.read_queue(name_, qi)) {
  case eValid:
    // Ok!
    break;
  case eMissing:
    debugs << "No message to flush from " << name_ << "\n";
    return;
  case eInvalid:
    std::cerr << "Corrupt queue info for '" << name_ << "' detected!\n";
    return;
  }

  int size = qi.size();

  debugs << "Messages to flush: " << size << "\n";

  for(int i = 0; i < qi.ranges_size(); i++) {
    const wire::MessageRange& range = qi.ranges(i);

    int fin = range.start() + range.count();

    for(int j = range.start(); j < fin; j++) {
      std::string key = durable_key(j);

      Message msg(key, j);

      switch(server_.read_message(key, msg)) {
      case eMissing:
        std::cerr << "Unable to get " << key << ". Corrupt QueueInfo?\n";
        // TODO: Keep going since we assuming haven't lost anything
        // and we'll fix the Queue later.
        break;
      case eInvalid:
        std::cerr << "Encountered corrupt message on disk\n";
        // TODO: what should I do here? Delete it? Keep it around and
        // make the data fairy fixes it? HMMM....
        break;
      case eValid:
        if(con->deliver(msg, ref(this)) == eIgnored) {
          // Ok, the connection is closing while we're
          // flushing. Because of the nature of the event
          // processing, there is no way we could have processed
          // an ack while flushing. Thusly, all those messages
          // would be preserved, so we can just bail on the flush
          // entirely and be safe.
          //
          // But if the connection isn't using acks, we should erase
          // the messages we've already handled.
          for(int ii = 0; ii < i; i++) {
            const wire::MessageRange& r = qi.ranges(ii);

            for(int jj = r.start(); jj < j; jj++) {
              erase_durable(jj);
            }
          }
          return;
        } else {
          debugs << "Flushed message " << j << "\n";
        }
        break;
      }
    }
  }

  // If the connection uses acks, then when the ack is received is
  // when we update the queue info.
  //
  if(!con->use_acks()) {
    // Don't reuse qi because it might be corrupt in same way, so just
    // make a fresh one.
    wire::Queue new_qi;
    new_qi.set_size(0);

    if(!server_.update_queue(name_, new_qi)) {
      std::cerr << "Unable to reset " << name_ << "\n";
      // TODO: durable seems to be busted! What should we do?!?
    }
  }

  return;
}

bool Queue::write_durable(Message& msg) {
  wire::Queue qi;

  switch(server_.read_queue(name_, qi)) {
  case eValid:
    // Ok!
    break;
  case eInvalid:
    std::cerr << "Corrupt queue info detected, unable to write durable\n";
    return false;
  case eMissing:
    qi.set_size(0);
    break;
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

  if(server_.update_queue(name_, qi, key, msg)) {
    msg.make_durable(key, idx);
    debugs << "Updated index of " << name_ << "\n";
    return true;
  } else {
    std::cerr << "Unable to write message to DB\n";
    // TODO: durable is busted! What to do?!
    return false;
  }
}

bool Queue::erase_durable(uint64_t idx) {
  wire::Queue qi;

  switch(server_.read_queue(name_, qi)) {
  case eValid:
    // Ok!
    break;
  case eInvalid:
    std::cerr << "Corrupt queue info detected, unable to write durable\n";
    return false;
  case eMissing:
    std::cerr << "Missing queue info detected, unable to write durable\n";
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

  if(!server_.remove_message(name_, qi, key)) {
    std::cerr << "Unable to write message to DB\n";
    // TODO: durable is busted! What to do?!
    return false;
  } else {
    debugs << "Updated index of " << name_ << "\n";
    return true;
  }
}

bool Queue::deliver(Message& msg) {
  // With broadcast, we don't support acks because wtf would that
  // even mean? So we handle it specially and invoke
  // Connection::write to just write the message directly to the
  // client.
  //
  if(kind_ == eBroadcast) {
    // Make a copy because subscribers_ might be changed if
    // there is an error with write.

    Connections cons = subscribers_;
    for(Connections::iterator i = cons.begin();
        i != cons.end();
        ++i) {
      Connection* con = *i;
      if(!con->write(msg)) {
        debugs << "Write error while broadcasting message\n";
      }
    }

    return true;
  }

  // So that we can loop if Connection::deliver fails.
  for(;;) {

    // If no one is subscribed, then queue it directly.
    if(subscribers_.empty()) {
      if(kind_ == eTransient) {
        write_transient(msg);
      } else {
        if(msg.durable_p()) {
          debugs << "Not re-writing already written durable message from ack\n";
        } else {
          write_durable(msg);
        }
      }

      break;
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
    //
    // Additionally, we may discover while trying to deliver the
    // message that the connection is ignoring us because it's dead,
    // so we loop again.
    //
    // NOTE this depends on the invariant that when a connection detects
    // that it's dying it removes it's subscriptions as soon as it detects
    // the error. Otherwise, this can turn into an infinite loop.

    if(con->deliver(msg, ref(this)) != eIgnored) break;
  }

  return true;
}

void Queue::recorded_ack(AckRecord& rec) {
  switch(kind_) {
  case eBroadcast:
    UNREACHABLE("Recorded ack on broadcast queue");
    break;
  case eTransient:
    write_transient(rec.msg);
    break;
  case eDurable:
    if(rec.msg.durable_p()) {
      debugs << "Detected ack on already durable message, not re-writing\n";
    } else {
      if(!write_durable(rec.msg)) {
        std::cerr << "Error saving messsage to durable!\n";
        // In this case, we really don't want to loose messages.
        // So we queue the message in memory at least.
        write_transient(rec.msg);
      }
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
    if(rec.msg.durable_p()) {
      if(!erase_durable(rec.msg.index())) {
        std::cerr << "Error deleting messsage from durable!\n";
      }
    } else {
      std::cerr << "Attempted to erase a non-durable message in a durable queue\n";
    }
    break;
  }

}
