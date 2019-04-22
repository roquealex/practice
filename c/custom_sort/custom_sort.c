#include <stdio.h>
#include <stdlib.h>

// Program separates positive from negatives. Positives
// go first in the array and ordered, negatives go at
// the bottom and in the same order as they originally
// apeared


void printIntArr(const int *arr, int n) {
  for (int i = 0 ; i < n ; i++) printf("%d\n",arr[i]);
}

int compare(const void *l, const void *r) {
  return *((int *)l) - *((int *)r);
}

void customSort(int *arr, int n) {
  // put negative at the end:
  int rdIdx = n-1;
  int wrIdx = n;
  if(n <= 1) return;
  while(rdIdx >= 0) {
    if(arr[rdIdx]<0) {
      wrIdx--;
      if(wrIdx!=rdIdx) {
        int toWr = arr[rdIdx];
        arr[rdIdx] = arr[wrIdx];
        arr[wrIdx] = toWr;
      }
    }
    rdIdx--;
  }
  qsort(arr,wrIdx,sizeof(int),compare);
  //printf("%d\n",wrIdx);

}

int main() {
  int a[] = {1,4,-6,-1,3,-8,0,1,-3,7,-2,1,6,-9,5,2,-5,7,2};
  printIntArr(a,sizeof(a)/sizeof(a[0]));
  customSort(a,sizeof(a)/sizeof(a[0]));
  printf("After\n");
  printIntArr(a,sizeof(a)/sizeof(a[0]));
  return 0;
}
