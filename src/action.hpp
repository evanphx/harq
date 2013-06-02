#ifndef ACTION_HPP
#define ACTION_HPP

enum ActionType {
  eSubscribe = 1,
  eConfigure = 2,
  eFlush = 4,
  eAck = 6,
  eConfirm = 8,
  eRequestStat = 9,
  eMakeBroadcastQueue = 10,
  eMakeTransientQueue = 11,
  eMakeDurableQueue = 12,
  eQueueError = 13,
  eBond = 14,
  eMakeEphemeralQueue = 15
};

#endif
