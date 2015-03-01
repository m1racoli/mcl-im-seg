#include <string.h>
#include <inttypes.h>
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
    return ((mcli*)i1)->id > ((mcli*)i2)->id ? 1 : -1;
}

int itemValComp(const void *i1, const void * i2) {
    return ((mcli*)i1)->val > ((mcli*)i2)->val ? 1 : -1;
}

void itemSet(mcli *item, const rowInd id, const value val) {
    //if(IS_TRACE){
    //    logTrace("itemSet: (%" PRId64 "; %f) -> %p",id,val,item);
    //}
    item->id = id; item->val = val;
}

void itemMultSet(mcli *dst, const value f, const mcli *src){
    dst->id = src->id;
    dst->val = f * src->val;
}

void itemAddSet(mcli *dst, const mcli *src, const value a){
    dst->id = src->id;
    dst->val = src->val + a;
}

void itemCopy(mcli *dst, const mcli *src){
    *dst = *src;
}

void itemAddMultSet(mcli *dst, const value f, const mcli* src, const value c){
    dst->id = src->id;
    dst->val = f * src->val + c;
}

mcli *itemNCopy(mcli *dst, const mcli *src, dim n){
//    if(IS_TRACE) {
//        logTrace("itemNCopy %u items: [%p,%p) -> [%p,%p)",n,src,src+n,dst,dst+n);
//    }
    return memcpy(dst, src, n * sizeof(mcli));
}

mcli *itemNMove(mcli *dst, const mcli *src, dim n){
//    if(IS_TRACE){
//        logTrace("itemNMove %u items: [%p,%p) -> [%p,%p)",n,src,src+n,dst,dst+n);
//    }
    return memmove(dst, src, n * sizeof(mcli));
}

void itemValidate(const mcli *self){
    if(self->id < 0 || self->val <= 0.0 || self->val > 1.0){
        logFatal("invalid item[id: %" PRId64 ", val:%f]", self->id, self->val);
    }
}