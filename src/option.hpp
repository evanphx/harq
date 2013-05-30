#ifndef OPTION_HPP
#define OPTION_HPP

template <typename T>
class option {
  bool set_;

  char bits_[sizeof(T)];

public:
  option()
    : set_(false)
  {}

  option(T v)
    : set_(true)
  {
    new(bits_) T(v);
  }

  T operator*() {
    return *((T*)bits_);
  }

  T operator->() {
    return *((T*)bits_);
  }

  bool set_p() {
    return set_;
  }
};

template <typename T>
class optref {
  bool set_;

  T* ptr_;

public:
  optref()
    : set_(false)
  {}

  optref(T& v)
    : set_(true)
    , ptr_(&v)
  {}

  optref(T* v)
    : set_(true)
    , ptr_(v)
  {}

  T& operator*() {
    return *ptr_;
  }

  T* operator->() {
    return ptr_;
  }

  T* ptr() {
    return ptr_;
  }

  bool set_p() {
    return set_;
  }

  operator bool() {
    return set_p();
  }
};

#endif
