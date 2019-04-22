#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

typedef struct link_t {
  int src;
  int dest;
  int cost;
} link, *link_p ;

struct link_t *create_link_array(const int *links, size_t length, size_t *n_links) {
  int capacity = (length*(length-1))/2;
  //int capacity = 3;
  struct link_t *list = (struct link_t *)malloc(capacity*sizeof(struct link_t));
  int idx = 0;
  for (int i = 0 ; i < length ; i++) {
    for (int j = i+1 ; j < length ; j++) {
      int cost = (links[i*length + j]);
      if (cost > 0) {
        if (idx >= capacity) {
          // resize
          assert(0);
        } else {
          list[idx].src = i;
          list[idx].dest = j;
          list[idx].cost = cost;
          idx++;
        }
      }
    }
  }
  *n_links = idx;
  return list;
}

void print_link_array(struct link_t *arr, size_t n_links) {
  for (int i = 0 ; i < n_links ; i++) {
    printf("from %d to %d cost %d\n",arr[i].src,arr[i].dest,arr[i].cost);
  }
}

int comp(const void *l, const void *r) {
  const struct link_t *ls = (struct link_t *)l;
  const struct link_t *rs = (struct link_t *)r;
  return ls->cost - rs->cost;
}

int find(int *parents, int node) {
  // own parent
  if (parents[node] == node) {
    return node;
  } else {
    return(find(parents,parents[node]));
  }
}

void union_sets(int *parents, int *sizes, int n1, int n2) {
  int r1 = find(parents,n1);
  int r2 = find(parents,n2);

  if (r1==r2) return;

  if ( sizes[r1] > sizes[r2] ) {
    sizes[r1] += sizes[r2];
    parents[r2] = r1;
  } else {
    sizes[r2] += sizes[r1];
    parents[r1] = r2;
  }

}

int min_cost_to_connect(const int *links, size_t length) {
  size_t n_links = 0;
  int total = 0;
  struct link_t *arr = create_link_array(links, length, &n_links);
  print_link_array(arr, n_links);
  puts("After\n");
  qsort(arr,n_links,sizeof(struct link_t),comp);
  print_link_array(arr, n_links);
  // Array of parents:
  int *parents = malloc(length*sizeof(int));
  for(int i = 0 ; i < length ; i++) {
    parents[i] = i;
  }
  // array of size
  int *sizes = malloc(length*sizeof(int));
  for(int i = 0 ; i < length ; i++) {
    sizes[i] = 1;
  }

  for (int i = 0 ; i < n_links ; i++) {
    // check if left and right belong to the same 
    if ( find(parents, arr[i].src) != find(parents,arr[i].dest)) {
      printf("Using link from %d to %d cost %d\n",arr[i].src,arr[i].dest,arr[i].cost);
      total += arr[i].cost;
      union_sets(parents, sizes, arr[i].src, arr[i].dest);
    }
  }

  // Dump parents and sizes
  for(int i = 0 ; i < length ; i++) {
    printf("%d ",parents[i]);
  }
  printf("\n");
  for(int i = 0 ; i < length ; i++) {
    printf("%d ",sizes[i]);
  }
  printf("\n");



  free(parents);
  free(sizes);
  free(arr);
  return total;
}

void print_links(const int *mat, size_t length) {
  for (int i = 0 ; i < length ; i++) {
    for (int j = i+1 ; j < length ; j++) {
      printf("%d ",mat[i*length + j]);
    }
    printf("\n");
  }
}

int main() {
  /*
  int links[5][5] = {
    {0, 1, 2, 3, 4},
    {1, 0, 5, 0, 7},
    {2, 5, 0, 6, 0},
    {3, 0, 6, 0, 0},
    {4, 7, 0, 0, 0}
  };
  */

  int links[9][9] = {
    {0, 3, 0, 0, 0, 0, 0, 10, 0},
    {0, 0, 26, 0, 12, 0, 0, 0, 0},
    {0, 0, 0, 14, 17, 13, 0, 0, 0},
    {0, 0, 0, 0, 0, 9, 16, 0, 11},
    {0, 12, 17, 0, 0, 15, 0, 7, 0},
    {0, 0, 0, 0, 0, 0, 6, 8, 0},
    {0, 0, 0, 0, 0, 0, 0, 4, 0},
    {10, 0, 0, 0, 0, 0, 0, 0, 0},
    {0, 0, 0, 0, 0, 0, 0, 0, 0}
  };

  //print_links(&links[0][0],5);
  //size_t n_links;
  //print_link_array(arr, n_links);
  int min = min_cost_to_connect(&links[0][0], 9);
  printf("Min cost is %d\n",min);
  return 0;
}


