#ifndef DEBUGS_HPP
#define DEBUGS_HPP

#ifdef DEBUG
#include <iostream>

extern std::ostream& debugs;

#else

struct DebugSinkStream {
  template <typename T>
    DebugSinkStream& operator<<(T) { return *this; }
};

extern DebugSinkStream debugs;

#endif

#endif
