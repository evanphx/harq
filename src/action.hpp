#ifndef ACTION_HPP
#define ACTION_HPP

enum ActionType {
  eSubscribe = 1,
  eTap,
  eDurableSubscribe,
  eFlush,
  eRequestAck,
  eAck
};

#endif
