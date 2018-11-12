/*
 * HeapBase.h
 *
 *  Created on: Nov 11, 2018
 *      Author: roquealex
 */

#ifndef HEAPBASE_H_
#define HEAPBASE_H_

#include <cstddef>

/**
 * Heap base class. This class provides the implementation for common
 * operations that need to be done on the heap. It is abstract enough
 * to allow different kinds of implementation but it assumes some
 * array like storage based on indexes is used.
 */
class HeapBase
{
private:
  size_t _left(size_t i) const;
  size_t _right(size_t i) const;
  size_t _parent(size_t i) const;

  /**
   * This is the comparison function. It is based on indexes of the
   * heap rather than the value.
   * @param i First index.
   * @param j Second index.
   */
  virtual bool _compIndex(size_t i, size_t j) = 0;
  /**
   * This is the swap function. It is based on indexes of the
   * heap rather than the value.
   * @param i First index.
   * @param j Second index.
   */
  virtual void _swapIndex(size_t i, size_t j) = 0;

protected:
  /**
   * Heapify down starting from the given index. This is a detailed
   * description
   * @param i Start index.
   * @param size Size of the current heap.
   */
  void maxHeapify(size_t i, size_t size);
  /**
   * Heapify up starting from the given index.
   * @param idx Start index.
   */
  void heapifyUp(size_t idx);

public:
  /**
   * Returns the size of this heap. The size is implementation
   * specific so a derived class defines this method.
   * @return The number of elements in the heap.
   */
  virtual size_t getSize() const = 0;
  /**
   * Makes a heap in place out of the array-like used for storage.
   */
  void makeHeap();
  /**
   * Makes a sorted array from this heap. To perform a heap sort
   * on the array-like used for storage perform a makeHeap
   * followed by sortHeap.
   */
  void sortHeap();

};

#endif /* HEAPBASE_H_ */
