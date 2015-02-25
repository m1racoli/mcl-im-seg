#ifndef heapitem_h
#define heapitem_h

#include <stdbool.h>
#include "heap.h"

typedef struct heapItem {
    void *data;
    struct heapItem *parent, *child;
    struct heapItem *left, *right;
    int degree;
} heapItem;

typedef heapItem hpi;

hpi *hpiNew(void *data, hpi *neighbor);

void hpiInsertSibling(hpi* root, hpi* item);

void hpiLink(hpi *parent, hpi *node);

void hpiFree(hpi **i);

#endif