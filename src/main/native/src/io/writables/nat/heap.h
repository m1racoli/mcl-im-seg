#ifndef heap_h
#define heap_h

/*
* The Original Software is GraphMaker. The Initial Developer of the Original
* Software is Nathan L. Fiedler. Portions created by Nathan L. Fiedler
* are Copyright (C) 1999-2008. All Rights Reserved.
*
* Parts Nathan Fiedler's Java implementation of a Fibonacci Heap has been used for this implementation.
*
*/

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
    const dim max_rank;
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

void heapPrint(mclh *h);

#endif