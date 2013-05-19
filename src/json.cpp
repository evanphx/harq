#include "json.hpp"
#include "wire.pb.h"

static void write_escape(const std::string& s, std::ostream& os) {
  for(std::string::const_iterator i = s.begin();
      i != s.end();
      ++i) {
    char c = *i;
    if(c >= 0x20 && c <= 0x7E) {
      os << c;
    } else {
      switch(c) {
      case '\n':
        os << "\\n";
        break;
      case '\r':
        os << "\\r";
        break;
      default:
        os << "\\u" << (int)c;
      }
    }
  }
}

void WriteJson(wire::Message& msg, std::ostream& os) {
  os << "{\n";
  os << "  \"destination\": \"";
  write_escape(msg.destination(), os);

  os << "\",\n  \"id\": ";
  os << msg.id();

  os << ",\n  \"flags\": ";
  os << msg.flags();

  os << ",\n  \"confirm_id\": ";
  os << msg.confirm_id();

  os << ",\n  \"payload\": \"";
  write_escape(msg.payload(), os);
  os << "\"\n}\n";
}
