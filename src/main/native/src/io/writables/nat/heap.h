#ifndef heap_h
#define heap_h

#include "types.h"

typedef struct heapItem {
    void *data;
    struct heapItem *child;
    struct heapItem *left, *right;
    int degree;
} heapItem;

typedef heapItem hpi;

typedef struct {
    const dim max_size;
    hpi *root;
    dim n_inserted;
    int (*cmp)(const void *i1, const void *i2);
} mclHeap;

#define mclh mclHeap

mclh *heapNew(mclh *h, dim max_size, int (*cmp) (const void*, const void*));

void heapReset(mclh *h);

void heapFree(mclh **h);

void heapInsert(mclh *h, void *elem);

void *heapRemove(mclh *h);

void heapDump(const mclh *h, void *dst, size_t elem_size);

#endif