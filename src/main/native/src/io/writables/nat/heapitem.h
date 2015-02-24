#ifndef heapitem_h
#define heapitem_h

#include <stdbool.h>

typedef struct heapItem {
    void *obj;
    struct heapItem *parent, *child;
    struct heapItem *left, *right;
    int rank;
    bool mark;
} heapItem;

typedef heapItem hpi;

#endif