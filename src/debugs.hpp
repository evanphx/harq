
#ifdef DEBUG
#include <iostream>

std::ostream& debugs = std::cout;

#else

struct DebugSinkStream {
  template <typename T>
    DebugSinkStream& operator<<(T) { return *this; }
};

extern DebugSinkStream debugs;

#endif
