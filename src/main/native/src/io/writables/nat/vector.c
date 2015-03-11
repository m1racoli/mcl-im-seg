#include <stdlib.h>
#include <math.h>
#include <inttypes.h>
#include "vector.h"
#include "alloc.h"
#include "logger.h"
#include "item.h"

mclv *vecInit(mclv *vec, dim n, mcli *items) {

    if (!vec)
        vec = mclAlloc(sizeof(mclv));

    vec->n = n;
    vec->items = items;

    return vec;
}

mclv *vecNew(dim n) {
    return vecInit(NULL, 0, itemNNew(n));
}

void vecFree(mclv **v){
    if(*v){
        if((*v)->items)
            mclFree((*v)->items);

        mclFree(*v);
        *v = NULL;
    }
}

jboolean vecEquals(mclVector const *v1, mclVector const *v2) {
    mcli *s1 = v1->items;
    mcli *t1 = s1 + v1->n;
    mcli *s2 = v2->items;
    mcli *t2 = s2 + v2->n;

    while (s1 != t1 && s2 != t2) {
        if (!itemEquals(s1++, s2++))
            return JNI_FALSE;
    }

    return (jboolean) (s1 == t1 && s2 == t2);
}

jdouble vecSumSquaredDiffs(mclVector const *v1, mclVector const *v2) {
    mcli *s1 = v1->items;
    mcli *t1 = s1 + v1->n;
    mcli *s2 = v2->items;
    mcli *t2 = s2 + v2->n;
    jdouble sum = 0.0;

    while (s1 != t1 && s2 != t2) {
        if (s1->id > s2->id) {
            sum += (s2++)->val;
        } else if (s1->id < s2->id) {
            sum += (s1++)->val;
        } else {
            jdouble s = (s1++)->val - (s2++)->val;
            sum += s * s;
        }
    }

    while (s1 != t1) {
        sum += (s1++)->val;
    }
    while (s2 != t2) {
        sum += (s2++)->val;
    }

    return sum;
}

void vecAddLoops(mclv *v, rowInd d) {
    mcli *c = NULL;
    value max = 0.0;
    mcli *i, *t;

    //logDebug("vecAddLoops: v->n = %d, d = %ld\n", v->n, (long) d);

    for(i = v->items, t = i + v->n; i != t; i++){
        //logDebug("vecAddLoops: iterate[row = %ld/%f]\n",(long) i->id, i->val);

        if(i->id == d){
            //logDebug("vecAddLoops: diag found\n");
            c = i;
        }

        if(max < i->val) max = i->val;
    }

    if(!c){
        logFatal("column %ld does not contain diagonal element. exit!!!\n",(long) d);
    }

    c->val = max;
}

void vecMakeStochastic(mclv *v) {
    double sum = 0.0;
    mcli *s, *i;

    for(s = v->items, i = s + v->n; i != s;){
        sum += (--i)->val;
    }

    for(s = v->items, i = s + v->n; i != s;){
        (--i)->val /= sum;
    }
}

void vecInflateAndStats(mclv *v, double inf, double *sum, double *max) {
    double s = 0.0;
    double m = 0.0;
    mcli *i, *t;

    for(i = v->items, t = i + v->n; i != t; ++i){
        i->val = (value) pow(i->val, inf);
        s += i->val;
        if(m < i->val) m = i->val;
    }

    *sum = s;
    *max = m;
}

void vecThresholdPrune(mclv *v, value threshold, mclStats *stats) {

    if(IS_TRACE){
        logTrace("vecThresholdPrune");
        vectorDescribe(v);
    }

    if(IS_DEBUG){
        vectorValidate(v);
    }

    dim n = 0;
    mcli *i, *t, *d;

    for(i = v->items, t = i + v->n, d = i; i != t; ++i){
        if(i->val >= threshold){
            *(d++) = *i;
            n++;
        } else {
            stats->cutoff++;
        }
    }

    v->n = n;

    if(IS_DEBUG){
        vectorValidate(v);
    }
}

void vecSelectionPrune(mclv *v, mclh *h, dim _select) {

    if(IS_TRACE){
        logTrace("vecSelectionPrune");
        vectorDescribe(v);
    }

    if(IS_DEBUG){
        vectorValidate(v);
    }

    mcli *tmp = itemNNew(_select);

    for(mcli *i = v->items, *t = i + v->n; i != t;){
        heapInsert(h, i++);
    }

    v->n = _select;
    heapDump(h, tmp, sizeof(mcli));
    itemNCopy(v->items, tmp, _select);
    qsort(v->items, _select, sizeof(mcli), itemIdComp);
    heapReset(h);

    mclFree(tmp);

    if(IS_DEBUG){
        vectorValidate(v);
    }
}

void vecAddForward(const mclv *v1, const mclv *v2, mclv *dst){

//    if(IS_TRACE){
//        logTrace("vecAddForward");
//        vectorDescribe(v1);
//        vectorDescribe(v2);
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(v1);
        vectorValidate(v2);
    }

    mcli *i1 = v1->items, *i2 = v2->items, *id = dst->items;
    mcli *t1 = i1 + v1->n, *t2 = i2 + v2->n;

    while(i1 != t1 && i2 != t2){
        if(i1->id < i2->id){
            *(id++) = *(i1++);
        } else if (i1->id > i2->id) {
            *(id++) = *(i2++);
        } else {
            //itemAddSet(id++, i1++, (i2++)->val);
            id->id = i1->id;
            (id++)->val = (i1++)->val + (i2++)->val;
        }
    }

    while(i1 != t1){
        *(id++) = *(i1++);
    }

    while(i2 != t2){
        *(id++) = *(i2++);
    }

    dst->n = id - dst->items;

//    if(IS_TRACE){
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(dst);
    }
}

void vecAddBackward(const mclv *v1, const mclv *v2, mclv *dst){

//    if(IS_TRACE){
//        logTrace("vecAddBackward");
//        vectorDescribe(v1);
//        vectorDescribe(v2);
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(v1);
        vectorValidate(v2);
    }

    mcli *s1 = v1->items, *s2 = v2->items;
    mcli *i1 = s1 + v1->n - 1, *i2 = s2 + v2->n - 1, *id = dst->items;

    while(i1 >= s1 && i2 >= s2){
        if(i1->id < i2->id){
            *(--id) = *(i2--);
        } else if (i1->id > i2->id) {
            *(--id) = *(i1--);
        } else {
            (--id)->id = i1->id;
            id->val = (i1--)->val + (i2--)->val;
        }
    }

    while(i1 >= s1){
        *(--id) = *(i1--);
    }

    while(i2 >= s2){
        *(--id) = *(i2--);
    }

    dst->n = dst->items - id;
    dst->items = id;

//    if(IS_TRACE){
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(dst);
    }
}

void vecAddMultForward(value f, const mclv* v1, const mclv *v2, mclv *dst){
//    if(IS_TRACE){
//        logTrace("vecAddMultForward[val:%f]",val);
//        vectorDescribe(v1);
//        vectorDescribe(v2);
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(v1);
        vectorValidate(v2);
    }

    mcli *i1 = v1->items, *i2 = v2->items, *id = dst->items;
    mcli *t1 = i1 + v1->n, *t2 = i2 + v2->n;

    while(i1 != t1 && i2 != t2){
        if(i1->id < i2->id){
            id->id = i1->id;
            (id++)->val = f * (i1++)->val;
        } else if (i1->id > i2->id) {
            *(id++) = *(i2++);
        } else {
            id->id = i1->id;
            (id++)->val = f * (i1++)->val + (i2++)->val;
        }
    }

    while(i1 != t1){
        id->id = i1->id;
        (id++)->val = f * (i1++)->val;
    }

    while(i2 != t2){
        *(id++) = *(i2++);
    }

    dst->n = id - dst->items;

//    if(IS_TRACE){
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(dst);
    }
}

void vecAddMultBackward(value val, const mclv* v1, const mclv *v2, mclv *dst){
//    if(IS_TRACE){
//        logTrace("vecAddMultBackward[val:%f]",val);
//        vectorDescribe(v1);
//        vectorDescribe(v2);
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        vectorValidate(v1);
        vectorValidate(v2);
    }

    mcli *s1 = v1->items, *s2 = v2->items;
    mcli *i1 = s1 + v1->n - 1, *i2 = s2 + v2->n - 1, *id = dst->items;

    while(i1 >= s1 && i2 >= s2){
        if(i1->id < i2->id){
            *(--id) = *(i2--);
        } else if (i1->id > i2->id) {
            (--id)->id = i1->id;
            id->val = val * (i1--)->val;
        } else {
            (--id)->id = i1->id;
            id->val = val * (i1--)->val + (i2--)->val;
        }
    }

    while(i1 >= s1){
        (--id)->id = i1->id;
        id->val = val * (i1--)->val;
    }

    while(i2 >= s2){
        *(--id) = *(i2--);
    }

    dst->n = dst->items - id;
    dst->items = id;

    if(IS_DEBUG){
        vectorValidate(dst);
    }
}

void vectorDescribe(const mclv* self){

    if(!self){
        printf("mclVector[(nil)]\n");
        return;
    }

    if(!self->n){
        if(self->items)
            printf("mclVector[n:0, items: %p]\n",self->items);
        else
            printf("mclVector[n:0, items: (nil)]\n");
        return;
    }

    mcli *t = self->items + (self->n - 1);
    printf("mclVector[p:%p 0:(%" PRId64 "; %f) ... %lu:(%" PRId64 "; %f) p:%p]\n",
            self->items,self->items->id,self->items->val,self->n-1,t->id,t->val,self->items + self->n);
}

void vectorValidate(const mclv* self){
    //logTrace("vectorValidate");
    if(self->n < 0)
        logFatal("vector dimension is negative");
    if(!self->items)
        logFatal("vector items are NULL");

    rowInd last_row = -1;

    for(mcli *i = self->items, *t = self->items + self->n; i != t; i++){
        itemValidate(i);

        if(i->id <= last_row){
            logFatal("items[%lu].row = %"PRId64" <= %"PRId64" = last row",i-(self->items),i->id,last_row);
        }

        last_row = i->id;
    }
}