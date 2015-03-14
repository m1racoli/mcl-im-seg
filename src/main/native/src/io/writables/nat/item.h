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

mcli *itemNNew(dim size);

bool itemEquals(const mcli *i1, const mcli *i2);

int itemIdComp(const void *i1, const void *i2);

int itemValComp(const void *i1, const void *i2);

void itemSet(mcli *item, const rowInd id, const value val);

mcli *itemNCopy(mcli *dst, const mcli *src, dim n);

mcli *itemNMove(mcli *dst, const mcli *src, dim n);

void itemValidate(const mcli *self);

#endif