/*
 * HeapBase.cpp
 *
 *  Created on: Nov 11, 2018
 *      Author: roquealex
 */

#include "HeapBase.h"

size_t HeapBase::_left(size_t i) const { return 2*i + 1;}
size_t HeapBase::_right(size_t i) const { return 2*(i + 1);}
size_t HeapBase::_parent(size_t i) const { return (i - 1)/2;}

void HeapBase::maxHeapify(size_t i, size_t size) {
  auto lIndex = _left(i);
  auto rIndex = _right(i);
  auto max = i;
  if (rIndex < size && _compIndex(rIndex,max)){
    max = rIndex;
  }
  if (lIndex < size && _compIndex(lIndex,max) ) {
    max = lIndex;
  }
  if (max != i) {
    _swapIndex(max,i);
    maxHeapify(max,size);
  }
}

void HeapBase::makeHeap() {
  size_t idx = getSize()/2;
  while(idx>0) {
      idx--;
      maxHeapify(idx,getSize());
  }
}

void HeapBase::sortHeap() {
  size_t size = getSize();
  while(size > 1) {
    size--;
    _swapIndex(0,size);
    maxHeapify(0,size);
  }
}

void HeapBase::heapifyUp(size_t idx) {
  auto pIndex = _parent(idx);
  while (idx > 0 && _compIndex(idx,pIndex)){
    _swapIndex(idx , pIndex);
    idx = pIndex;
    pIndex = _parent(idx);
  }
}



