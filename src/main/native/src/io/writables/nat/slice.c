#include <inttypes.h>
#include "slice.h"
#include "alloc.h"
#include "logger.h"

static dim _nsub;
static dim _select;
static jboolean _autoprune;
static jdouble _inflation;
static jdouble _prune_A;
static jdouble _prune_B;
static value _cutoff;
static dim _kmax;

static value computeThreshold(double avg, double max){
    double thresh = _prune_A * avg * (1.0 - _prune_B * (max - avg));
    thresh = thresh < 1.0e-7 ? 1.0e-7 : thresh;
    return (value) (thresh < max ? thresh : max);
}

void sliceSetParams(dim nsub, dim select, jboolean autoprune, jdouble inflation,
        value cutoff, jdouble pruneA, jdouble pruneB, dim kmax) {
    _nsub = nsub;
    _select = select;
    _autoprune = autoprune;
    _inflation = inflation;
    _cutoff = cutoff;
    _prune_A = pruneA;
    _prune_B = pruneB;
    _kmax = kmax;
}

mcls *sliceInitFromAdress(mcls *s, void *obj){

    mcls *slice = s ? s : mclAlloc(sizeof(mcls));

    jbyte *arr = obj;
    slice->data = obj;
    slice->align = arr;
    slice->colPtr = (colInd *) (arr + 1);
    slice->items = (mcli*) (slice->colPtr + _nsub + 1);

    //if(IS_TRACE){
    //    logTrace("initSlice");
    //    sliceDescribe(slice);
    //}

    return slice;
}

mcls *sliceInitFromArr(mcls *s, JNIEnv *env, jbyteArray arr) {
    mcls *slice = s ? s : mclAlloc(sizeof(mcls));

    jboolean isCopy;

    jbyte *a = (*env)->GetByteArrayElements(env,arr,&isCopy);

    if((*env)->ExceptionCheck(env)){
        (*env)->ExceptionDescribe(env);
        (*env)->FatalError(env,"error getting the byte array pointer");
    }

    if(IS_DEBUG){
        logDebug("array is copy: %s", isCopy ? "TRUE" : "FALSE");
    }

    return sliceInitFromAdress(slice, a);
}

mcls *sliceInitFromBB(mcls *s, JNIEnv *env, jobject buf) {
    mcls *slice = s ? s : mclAlloc(sizeof(mcls));

    void *obj= (*env)->GetDirectBufferAddress(env,buf);

    if((*env)->ExceptionCheck(env)){
        (*env)->ExceptionDescribe(env);
        (*env)->FatalError(env,"error getting the ByteBuffers adress");
    }

    return sliceInitFromAdress(slice, obj);
}

jboolean sliceEquals(mclSlice const *s1, mclSlice const *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;
    colInd *t1,*t2;

    for(i = _nsub - 1; i >= 0; --i) {
        t1 = c1--;
        t2 = c2--;
        v1 = vecInit(v1, (dim) (*t1 - *c1), s1->items + *c1);
        v2 = vecInit(v2, (dim) (*t2 - *c2), s2->items + *c2);

        if(!vecEquals(v1, v2)){
            mclFree(v1);
            mclFree(v2);
            return JNI_FALSE;
        }
    }

    mclFree(v1);
    mclFree(v2);
    return JNI_TRUE;
}

dim sliceGetDataSize(dim nsub, dim kmax){
    return sizeof(jbyte) + nsub * sizeof(colInd) + nsub * kmax * sizeof(mcli);
}

jdouble sliceSumSquaredDiffs(const mcls *s1, const mcls *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;
    colInd *t1,*t2;
    jdouble sum = 0.0;

    for(i = _nsub - 1; i >= 0; --i) {
        t1 = c1--;
        t2 = c2--;
        v1 = vecInit(v1, (dim) (*t1 - *c1), s1->items + *c1);
        v2 = vecInit(v2, (dim) (*t2 - *c2), s2->items + *c2);
        sum += vecSumSquaredDiffs(v1,v2);
    }

    mclFree(v1);
    mclFree(v2);
    return sum;
}

void sliceAddLoops(mcls *slice, jint id) {
    mclv *v = NULL;
    colInd *s = slice->colPtr;
    colInd *c = s + _nsub, *t;
    rowInd d = (rowInd) id * (rowInd) _nsub + (rowInd) _nsub;

    while(c != s){
        t = c--;
        v = vecInit(v, (dim) (*t - *c), slice->items + *c);
        if(v->n) vecAddLoops(v,--d);
    }

    mclFree(v);
}

void sliceMakeStochastic(mcls *slice) {
    mclv *v = NULL;
    colInd *s, *c, *t;

    for(s = slice->colPtr, c = s + _nsub; c != s;) {
        t = c--;
        v = vecInit(v, (dim) (*t - *c), slice->items + *c);
        vecMakeStochastic(v);
    }
    mclFree(v);
}

void sliceInflateAndPrune(mcls *slice, mclStats *stats) {

    if(IS_TRACE){
        logTrace("sliceInflateAndPrune");
        sliceDescribe(slice);
    }

    if(IS_DEBUG){
        sliceValidate(slice, false);
    }

    mclv *v = NULL;
    colInd *cs, *ct, *t;
    colInd num_new_items = 0;
    mcli *new_items = slice->items;
    //mcli *it, *ii;
    value threshold;
    double center;
    double max;
    double sum;
    double chaos;
    mclh *h = NULL;

    for(cs = slice->colPtr, ct = slice->colPtr+1, t = slice->colPtr + _nsub; cs != t; cs = ct++) {
        v = vecInit(v, (dim) (*ct - *cs), slice->items + *cs);
        *cs = num_new_items;

        switch (v->n) {
            case 0:
                continue;
            case 1:
                new_items->id = v->items->id;
                (new_items++)->val = 1.0;
                num_new_items++;
                if(!stats->kmax) stats->kmax = 1;
                stats->attractors++;
                stats->homogen++;
                continue;
            default:
                break;
        }

        if(_autoprune){
            vecInflateAndStats(v, _inflation, &sum, &max);
            threshold = computeThreshold(sum/v->n, max);
        } else {
            threshold = _cutoff;
        }

        vecThresholdPrune(v, threshold, stats);

        if(v->n > _select){
            h = heapNew(h, _select, itemValComp);
            stats->prune += v->n - _select;
            vecSelectionPrune(v, h, _select);
        }

        if(stats->kmax < v->n) stats->kmax = (jint) v->n;

        if(v->n == 1){
            new_items->id = v->items->id;
            (new_items++)->val = 1.0;
            num_new_items++;
            stats->attractors++;
            stats->homogen++;
            continue;
        }

        if(_autoprune){
            vecMakeStochasticAndStats(v, &center, &max);
        } else {
            vecInflateMakeStochasticAndStats(v, _inflation, &center, &max);
        }


        for(mcli *ii = v->items, *it = new_items + v->n; new_items != it;){
            *(new_items++) = *(ii++);
        }

        //new_items = itemNMove(new_items, v->items, v->n) + v->n;

        num_new_items += v->n;
        chaos = (max - center) * v->n;
        if(max > 0.5) stats->attractors++;
        if(chaos < 1.0e-4) stats->homogen++;
        if(stats->chaos < chaos) stats->chaos = chaos;
    }
    *slice->align = TOP_ALIGNED;
    *cs = num_new_items;
    mclFree(v);
    heapFree(&h);

    if(IS_TRACE){
        sliceDescribe(slice);
    }

    if(IS_DEBUG){
        sliceValidate(slice, false);
    }
}

dim sliceSize(const mcls *slice){
    return (dim) (slice->colPtr[_nsub] - slice->colPtr[0]);
}

void sliceAdd(mcls *s1, const mcls *s2){

//    if(IS_TRACE){
//        logTrace("sliceAdd");
//        sliceDescribe(s1);
//        sliceDescribe(s2);
//    }

    if(IS_DEBUG){
        sliceValidate(s1, false);
        sliceValidate(s2, false);
    }

    mclv *v1 = NULL, *v2 = NULL, *vd = NULL;
    colInd *cs1, *cs2, *ct1, *ct2;
    colInd num_new_items;

    if(*s1->align){
        //aligned at end

        cs1 = s1->colPtr;
        ct1 = cs1 + 1;
        cs2 = s2->colPtr;
        ct2 = cs2 + 1;
        num_new_items = 0;

        for(dim i = _nsub; i > 0; --i, cs1 = ct1++, cs2 = ct2++)
        {
            v1 = vecInit(v1, (dim) (*ct1 - *cs1), s1->items + *cs1);
            v2 = vecInit(v2, (dim) (*ct2 - *cs2), s2->items + *cs2);
            vd = vecInit(vd, 0, s1->items + num_new_items);
            vecAddForward(v1, v2, vd);
            *cs1 = num_new_items;
            num_new_items += vd->n;
        }

        *cs1 = num_new_items;
        *s1->align = TOP_ALIGNED;
    } else {
        //aligned at beginning

        ct1 = s1->colPtr + _nsub;
        cs1 = ct1 - 1;
        ct2 = s2->colPtr + _nsub;
        cs2 = ct2 - 1;
        num_new_items = (colInd) (_nsub * _kmax);

        for(dim i = _nsub; i > 0; --i, ct1 = cs1--, ct2 = cs2--)
        {
            v1 = vecInit(v1, (dim) (*ct1 - *cs1), s1->items + *cs1);
            v2 = vecInit(v2, (dim) (*ct2 - *cs2), s2->items + *cs2);
            vd = vecInit(vd, 0, s1->items + num_new_items);
            vecAddBackward(v1, v2, vd);
            *ct1 = num_new_items;
            num_new_items -= vd->n;
        }

        *ct1 = num_new_items;
        *s1->align = BOTTOM_ALIGNED;
    }

    mclFree(v1);
    mclFree(v2);
    mclFree(vd);

    if(IS_DEBUG){
        sliceValidate(s1, false);
    }
}

void sliceMultiply(const mcls *s1, mcls *s2) {
    // s1 left side read-only
    // s2 right side and destination

//    if(IS_TRACE){
//        logTrace("sliceMultiply");
//        sliceDescribe(s1);
//        sliceDescribe(s2);
//    }

    if(IS_DEBUG){
        sliceValidate(s1,false);
        sliceValidate(s2,false);
    }

    colInd *cs, *ct;
    colInd num_new_items;
    mclv *tmp = vecNew(_kmax);
    num_new_items = *s2->align ? 0 : (colInd) _nsub * (colInd) _kmax;
    mclv *d = vecInit(NULL, 0, s2->items);

    if(*s2->align){
        //if(IS_TRACE){
        //    logTrace("bottom aligned => forward");
        //}
        //bottom aligned => forward

        cs = s2->colPtr;
        ct = cs + 1;

        for(dim i = _nsub; i > 0; --i, cs = ct++)
        {
            tmp->n = (dim) (*ct - *cs);
            *cs = num_new_items;

            if(tmp->n){
                itemNCopy(tmp->items, s2->items + *cs, tmp->n);
                sliceVecMult(s1, tmp, d, s2->items, num_new_items, *ct, true);
                num_new_items -= d->n;
            }
        }

        *cs = num_new_items;
        *s2->align = TOP_ALIGNED;
    } else {
        //if(IS_TRACE){
        //    logTrace("top aligned => backward");
        //}
        //top aligned => backward

        ct = s2->colPtr + _nsub;
        cs = ct - 1;

        for(dim i = _nsub; i > 0; --i, ct = cs--)
        {
            tmp->n = (dim) (*ct - *cs);
            *ct = num_new_items;

            if(tmp->n){
                itemNCopy(tmp->items, s2->items + *cs, tmp->n);
                sliceVecMult(s1, tmp, d, s2->items, *cs, num_new_items, false);
                num_new_items -= d->n;
            }
        }

        *ct = num_new_items;
        *s2->align = BOTTOM_ALIGNED;
    }

    mclFree(d);
    vecFree(&tmp);

    //if(IS_TRACE){
    //    sliceDescribe(s2);
    //}

    if(IS_DEBUG){
        sliceValidate(s2,false);
    }
}

void sliceVecMult(const mcls *slice, const mclv *v, mclv *dst, mcli *items, const colInd s, const colInd t, bool top){

//    if(IS_TRACE){
//        logTrace("sliceVecMult[items: %p, s: %lu, t: %lu, top: %d]",items, s, t, top);
//        sliceDescribe(slice);
//        vectorDescribe(v);
//        vectorDescribe(dst);
//    }

    if(IS_DEBUG){
        sliceValidate(slice, false);
        vectorValidate(v);
    }

    //mcli *item = v->items;
    bool inorder = false;
    mclv *v1 = NULL, *v2 = NULL;
    colInd cs;
    dst->n = 0;

    for(mcli *i = v->items, *e = v->items + v->n; i != e; ++i){
        cs = slice->colPtr[i->id];
        v1 = vecInit(v1, (dim) (slice->colPtr[i->id+1] - cs), slice->items + cs);
        v2 = vecInit(v2, dst->n, dst->items);
        dst->n = 0;

        if(inorder){
            dst->items = items + t;
            vecAddMultBackward(i->val, v1, v2, dst);
        } else {
            dst->items = items + s;
            vecAddMultForward(i->val, v1, v2, dst);
        }

        inorder = !inorder;
    }

    if(top != inorder){
        //if(IS_TRACE) logTrace("top:%d != inorder:%d",top,inorder);
        if(top){
            dst->items = itemNMove(items + s, dst->items, dst->n);
        } else {
            dst->items = itemNMove(items + (t - dst->n), dst->items, dst->n);
        }
    }

    mclFree(v1);
    mclFree(v2);

    if(IS_DEBUG){
        vectorValidate(dst);
    }
}

void sliceDescribe(const mcls* self){
    char* root = self->data;
    colInd *cs = self->colPtr;
    colInd *ct = cs + _nsub;
    mcli *is = self->items + *cs;
    mcli *it = self->items + (*ct - 1);

    printf("mclSlice[p:%p, align:%i, colPtr[0:[%lu:%i] ... %lu:[%lu:%i]], items[0:[%lu:(%" PRId64 "; %f)] ... %lu:[%lu:(%" PRId64 "; %f)]], %lu]\n",
            self->data,*self->align,
            (char*)cs - root,*cs,_nsub,(char*)ct-root,*ct,
            (char*)is-root,is->id,is->val,it-is,(char*)it-root,it->id,it->val,(char*)(it+1)-root);
}

void sliceValidate(const mcls *self, bool can_be_empty) {
    //logTrace("sliceValidate");
    if (*self->align < 0 || *self->align > 1)
        logFatal("illegal allignment %i", *self->align);

    colInd *colPtr = self->colPtr;

    if (!colPtr) {
        logFatal("colPtr is NULL");
        return;
    }

    dim max_nnz = _nsub * _kmax;

    if(*colPtr < 0)
        logFatal("colPtr[0] = %lu < 0",*colPtr);

    if(*colPtr > max_nnz)
        logFatal("colPtr[0] = %lu > %lu = max_nnz",*colPtr,max_nnz);

    if((*self->align && colPtr[_nsub] != max_nnz) || (!*self->align && colPtr[0] != 0))
        logFatal("alignemtn & colPtr missmatch: align: %i, colPtr[0]: %lu, colPtr[%lu] = %lu",*self->align,self->colPtr[0],_nsub,colPtr[_nsub]);

    for(int i = 1, t = _nsub+1; i<t;i++){
        if(colPtr[i] < colPtr[i-1])
            logFatal("colPtr[%i] = %lu < %lu = colPtr[%i]",i,colPtr[i],colPtr[i-1],i-1);

        if(colPtr[i] > max_nnz)
            logFatal("colPtr[%i] = %lu > %lu = max_nnz",i,*colPtr,max_nnz);
    }

    if(!can_be_empty){
        if(colPtr[_nsub] - colPtr[0] == 0)
            logFatal("slice should not be empty");
    }

    for(colInd *cs = colPtr, *ct = colPtr + 1, *end = colPtr + _nsub; cs != end; cs = ct++){
        rowInd last_row = -1;

        for(mcli *i = self->items + *cs, *t = self->items + *ct; i != t; i++){
            itemValidate(i);

            if(i->id <= last_row){
                logFatal("items[%lu].row = %"PRId64" <= %"PRId64" = last row",i-(self->items + colPtr[0]),i->id,last_row);
            }

            last_row = i->id;
        }

    }
}