#ifndef JSON_HPP
#define JSON_HPP

#include <iostream>

namespace wire {
  class Message;
}

void WriteJson(wire::Message& msg, std::ostream& os);

#endif
