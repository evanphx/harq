
#include "debugs.hpp"

#ifdef DEBUG
  std::ostream& debugs = std::cout;
#else
  DebugSinkStream debugs;
#endif

