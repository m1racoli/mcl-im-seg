#ifndef item_h
#define item_h

#include "types.h"

typedef struct {
    rowInd id;
    value val;
} mclItem;

#define mcli mclItem

mcli *itemInit(mcli *item, rowInd idx, value val);

jboolean itemEquals(const mcli *i1, const mcli *i2);

jint itemIdComp(void *i1, void *i2);

void itemSet(mcli *item, rowInd id, value val);

void itemCopy(mcli *dst,const mcli *src);

#endif