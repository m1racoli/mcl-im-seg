#ifndef item_h
#define item_h

#include <stdbool.h>
#include "types.h"

typedef struct {
    rowInd id;
    value val;
} mclItem;

#define mcli mclItem

mcli *itemInit(mcli *item, rowInd idx, value val);

bool itemEquals(const mcli *i1, const mcli *i2);

jint itemIdComp(void *i1, void *i2);

void itemSet(mcli *item, const rowInd id, const value val);

void itemCopy(mcli *dst,const mcli *src);

#endif