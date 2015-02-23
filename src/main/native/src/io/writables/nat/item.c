#include <string.h>
#include "item.h"
#include "alloc.h"

mcli* itemInit (mcli* item, rowInd id, value val) {
    if(!item)
        item = mclAlloc(sizeof(mcli));

    item->id = id;
    item->val = val;

    return item;
}

bool itemEquals(mclItem const *i1, mclItem const *i2) {
    return i1->id == i2->id && i1->val == i2->val;
}

int itemIdComp(const void *i1, const void *i2) {
    rowInd d = ((mcli*)i1)->id - ((mcli*)i2)->id;
    return d < 0 ? -1 : d > 0 ? 1 : 0;
}

int itemValComp(const void *i1, const void * i2) {
    //TODO
    return 0;
}

void itemSet(mcli *item, const rowInd id, const value val) {
    item->id = id; item->val = val;
}

void itemCopy(mcli *dst, mclItem const *src) {
    dst->id = src->id; dst->val = src->val;
}

mcli *itemNCopy(mcli *dst, const mcli *src, dim n){
    return memcpy(dst, src, n * sizeof(mcli));
}

mcli *itemNMove(mcli *dst, const mcli *src, dim n){
    return memmove(dst, src, n * sizeof(mcli));
}