/*
 * heapTest.cpp
 *
 *  Created on: Nov 11, 2018
 *      Author: roquealex
 */

#include <iostream>
#include <iomanip>
#include <algorithm>

using namespace std;

size_t getH(size_t size) {
  size_t h = 0;
  while(size) {
    h++;
    size>>=1;
  }
  return h;
}
void printHeap(int *arr, size_t size) {
  // get h
  // 1 -> 1
  // 2,3 -> 2
  // 4,5,6,7 -> 3
  // 8,9,10,11,12,13,14,15 -> 4
  unsigned int lim = 2;
  size_t h = getH(size);
  int w = (1<<(h-1))*4;
  for(unsigned int i = 0 ; i < size ; i++) {
    if (lim == i+1) {
      cout<<endl;
      lim<<=1;
      w >>=1;
    }
    cout<<left<<setw(w)<<arr[i];
  }
  cout<<endl;
}

void printArray(int *arr, size_t size) {
  for(int i = 0 ; i < size ; i++) {
    cout<<arr[i]<<" ";
  }
}

int main() {
  int a[20];
  int b[20];
  int sizeA = sizeof(a)/sizeof(a[0]);
  int sizeB = sizeof(b)/sizeof(b[0]);
  for_each(a,&a[sizeA],[](int &n){
    static int num = 0;
    n = num++;
  });
  copy(a,&a[sizeA],b);
  cout<<"This is the heap test"<<endl;
  cout<<"Size of a "<<sizeA<<endl;
  cout<<"Size of b "<<sizeB<<endl;
  bool eq = equal(a,&a[sizeA],b);
  cout<<"Array a and b are "<<(eq?"Equal":"Different")<<endl;
  make_heap(a,&a[sizeA]);
  printHeap(a,sizeA);
  return 0;
}




