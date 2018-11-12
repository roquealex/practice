/*
 * HeapBase.h
 *
 *  Created on: Nov 11, 2018
 *      Author: roquealex
 */

#ifndef HEAPBASE_H_
#define HEAPBASE_H_

#include <cstddef>

class HeapBase
{
private:
  size_t _left(size_t i) const;
  size_t _right(size_t i) const;
  size_t _parent(size_t i) const;

  virtual bool _compIndex(size_t, size_t) = 0;
  virtual void _swapIndex(size_t, size_t) = 0;

protected:
  void maxHeapify(size_t i, size_t size);
  void heapifyUp(size_t idx);

public:
  virtual size_t getSize() const = 0;
  void makeHeap();
  void sortHeap();

};

#endif /* HEAPBASE_H_ */
