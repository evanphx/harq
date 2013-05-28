#ifndef ACTION_HPP
#define ACTION_HPP

enum ActionType {
  eSubscribe = 1,
  eTap = 2,
  eDurableSubscribe = 3,
  eFlush = 4,
  eRequestAck = 5,
  eAck = 6,
  eRequestConfirm = 7,
  eConfirm = 8,
  eRequestStat = 9,
  eMakeBroadcastQueue = 10,
  eMakeTransientQueue = 11,
  eMakeDurableQueue = 12,
  eQueueError = 13
};

#endif
