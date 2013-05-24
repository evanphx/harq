#ifndef SAFE_REF_HPP
#define SAFE_REF_HPP

template <typename T>
  T& ref(T* ptr) {
    if(!ptr) abort();
    return *ptr;
  }

#endif
