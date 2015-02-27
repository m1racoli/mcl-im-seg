#include <string.h>
#include "item.h"
#include "alloc.h"
#include "logger.h"

mcli* itemInit (mcli* item, rowInd id, value val) {
    if(!item)
        item = mclAlloc(sizeof(mcli));

    item->id = id;
    item->val = val;

    return item;
}

mcli *itemNNew(dim size){
    return mclAlloc(size * sizeof(mcli));
}

bool itemEquals(mclItem const *i1, mclItem const *i2) {
    return i1->id == i2->id && i1->val == i2->val;
}

int itemIdComp(const void *i1, const void *i2) {
    return ((((mcli*)i1)->id > ((mcli*)i2)->id) - (((mcli*)i1)->id < ((mcli*)i2)->id));;
}

int itemValComp(const void *i1, const void * i2) {
    return ((((mcli*)i1)->val > ((mcli*)i2)->val) - (((mcli*)i1)->val < ((mcli*)i2)->val));
}

void itemSet(mcli *item, const rowInd id, const value val) {
    item->id = id; item->val = val;
}

mcli *itemNCopy(mcli *dst, const mcli *src, dim n){
    //logDebug("itemNCopy %u items: [%p,%p) -> [%p,%p)",n,src,src+n,dst,dst+n);
    return memcpy(dst, src, n * sizeof(mcli));
}

mcli *itemNMove(mcli *dst, const mcli *src, dim n){
    return memmove(dst, src, n * sizeof(mcli));
}