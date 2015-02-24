#ifndef heap_h
#define heap_h

#include "types.h"
#include "heapitem.h"

typedef struct {
    hpi *base;
    dim heap_size;
    hpi *root;
    dim n_inserted;
    int (*cmp)(const void *i1, const void *i2);
} mclHeap;

#define mclh mclHeap

mclh *heapNew(mclh *h, dim heap_size, int (*cmp)  (const void* lft, const void* rgt));

void heapReset(mclh *h);

void heapFree(mclh **h);

void heapInsert(mclh *h, void *elem);

void *heapRemove(mclh *h);

#endif