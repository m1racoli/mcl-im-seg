#ifndef heap_h
#define heap_h

#include "types.h"

typedef struct {

} heapItem;

typedef struct {
    void *base;
    dim heap_size;
    dim elem_size;
    int (*cmp)(const void *i1, const void *i2);
    dim n_inserted;
} mclHeap;

#define mclh mclHeap

mclh *heapInit(void* h);

mclh *heapNew(mclh *h, dim heap_size, dim elem_size, int (*cmp)  (const void* lft, const void* rgt));

void heapReset(mclh *h);

void heapFree(mclh **h);

void heapInsert(mclh *h, void *elem);

void *heapRemove(mclh *h);

#endif