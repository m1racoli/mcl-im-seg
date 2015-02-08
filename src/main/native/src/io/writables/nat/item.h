#ifndef item_h
#define item_h

#include "types.h"

typedef struct {
    rowInd id;
    value val;
} mclItem;

#define mcli mclItem

mcli *itemInit(mcli *item, rowInd idx, value val);

jboolean itemEquals(mcli *i1, mcli *i2);

jint itemIdComp(void *i1, void *i2);

#endif