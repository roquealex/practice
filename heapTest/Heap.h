/*
 * Heap.h
 *
 *  Created on: Nov 11, 2018
 *      Author: roquealex
 */

#ifndef HEAP_H_
#define HEAP_H_

#include <vector>
#include <iostream>
#include <iomanip>
#include <algorithm>

using namespace std;

template <class T>
class Heap {
private:
  vector<T>& _v;
  size_t _getH() const;
  size_t _left(size_t i) const { return 2*i + 1;}
  size_t _right(size_t i) const { return 2*(i + 1);}
  size_t _parent(size_t i) const { return (i - 1)/2;}
public:
  Heap(vector<T> &v) : _v(v) {};
  void printHeap() const;

  size_t getSize() const {
    return _v.size();
  }

  void maxHeapify(size_t i, size_t size) {
    auto lIndex = _left(i);
    auto rIndex = _right(i);
    auto max = i;
    if (rIndex < size && _v[rIndex] > _v[max]) {
      max = rIndex;
    }
    if (lIndex < size && _v[lIndex] > _v[max]) {
      max = lIndex;
    }
    if (max != i) {
      swap(_v[max],_v[i]);
      maxHeapify(max,size);
    }
  }

  void makeHeap() {
    size_t idx = _v.size()/2;
    while(idx>0) {
        idx--;
        maxHeapify(idx,_v.size());
    }
  }

  void sortHeap() {
    size_t size = _v.size();
    while(size > 1) {
      size--;
      swap(_v[0],_v[size]);
      maxHeapify(0,size);
    }
  }

  void heapifyUp(size_t idx) {
    auto pIndex = _parent(idx);
    while (idx > 0 && _v[idx] > _v[pIndex]) {
      swap(_v[idx] , _v[pIndex]);
      idx = pIndex;
      pIndex = _parent(idx);
    }
  }

  void pushHeap(const T& t) {
    _v.push_back(t);
    heapifyUp(_v.size()-1);
  }

  T& frontHeap() const {
    return _v[0];
  }

  void popHeap() {
    size_t size = _v.size();
    size--;
    swap(_v[0],_v[size]);
    _v.pop_back();
    maxHeapify(0,size);
  }

};

template <class T>
size_t Heap<T>::_getH() const {
  size_t h = 0;
  auto size = _v.size();
  while(size) {
    h++;
    size>>=1;
  }
  return h;
}

template <class T>
void Heap<T>::printHeap() const {
  unsigned int lim = 2;
  size_t h = _getH();
  int w = (1<<(h-1))*4;
  for(unsigned int i = 0 ; i < _v.size() ; i++) {
    if (lim == i+1) {
      cout<<endl;
      lim<<=1;
      w >>=1;
    }
    cout<<left<<setw(w)<<_v[i];
  }
  cout<<endl;

}



#endif /* HEAP_H_ */
