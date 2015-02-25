#include "slice.h"
#include "alloc.h"

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

colInd *colIdxFromByteBuffer(JNIEnv *env, jobject buf) {
    jbyte *arr = (*env)->GetDirectBufferAddress(env,buf);
    return (colInd*) (arr + 1);
}

mcls *sliceInit(mcls *slice, JNIEnv *env, jobject buf) {
    if(!slice)
        slice = mclAlloc(sizeof(mcls));

    jbyte *arr = (*env)->GetDirectBufferAddress(env,buf);

    slice->align = *arr;
    slice->colPtr = (colInd *) (arr + 1);
    slice->items = (mcli*) (slice->colPtr + _nsub + 1);
    return slice;
}

jboolean sliceEquals(mclSlice const *s1, mclSlice const *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;

    for(i = _nsub - 1; i >= 0; --i) {
        v1 = vecInit(v1, (dim) (*(c1--) - *c1), s1->items + *c1);
        v2 = vecInit(v2, (dim) (*(c2--) - *c2), s2->items + *c2);

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

dim sliceGetDataSize(dim nsub, dim items){
    return 1 + nsub * sizeof(colInd) + items * sizeof(mcli);
}

jdouble sliceSumSquaredDiffs(const mcls *s1, const mcls *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;
    jdouble sum = 0.0;

    for(i = _nsub - 1; i >= 0; --i) {
        v1 = vecInit(v1, (dim) (*(c1--) - *c1), s1->items + *c1);
        v2 = vecInit(v2, (dim) (*(c2--) - *c2), s2->items + *c2);
        sum += vecSumSquaredDiffs(v1,v2);
    }

    mclFree(v1);
    mclFree(v2);
    return sum;
}

void sliceAddLoops(mcls *slice, jint id) {
    mclv *v = NULL;
    colInd *s = slice->colPtr;
    colInd *c = s + _nsub;
    rowInd d = (rowInd) id * (rowInd) _nsub + (rowInd) _nsub;

    while(c != s){
        v = vecInit(v, (dim) (*(c--) - *c), slice->items + *c);
        if(v->n) vecAddLoops(v,--d);
    }

    mclFree(v);
}

void sliceMakeStochastic(mcls *slice) {
    mclv *v = NULL;
    colInd *s, *c;

    for(s = slice->colPtr, c = s + _nsub; c != s;) {
        v = vecInit(v, (dim) (*(c--) - *c), slice->items + *c);
        vecMakeStochastic(v);
    }
    mclFree(v);
}

void sliceInflateAndPrune(mcls *slice, mclStats *stats) {
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
                itemSet(new_items++, v->items->id, 1.0);
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
            vecSelectionPrune(v, h, _select);
        }

        if(_autoprune){
            vecMakeStochasticAndStats(v, &center, &max);
        } else {
            vecInflateMakeStochasticAndStats(v, _inflation, &center, &max);
        }

        /*
        for(ii = v->items, it = new_items + v->n; new_items != it;){
            *(new_items++) = *(ii++);
        }*/

        new_items = itemNMove(new_items, v->items, v->n) + v->n;

        num_new_items += v->n;
        chaos = (max - center) * v->n;
        if(max > 0.5) stats->attractors++;
        if(chaos < 1.0e-4) stats->homogen++;
        if(stats->chaos < chaos) stats->chaos = chaos;
        if(stats->kmax < v->n) stats->kmax = (jint) v->n;
    }

    *cs = num_new_items;
    mclFree(v);
    heapFree(&h);
}

dim sliceSize(const mcls *slice){
    return (dim) (slice->colPtr[_nsub]-slice->colPtr[0]);
}

void sliceAdd(mcls *s1, const mcls *s2){
    mclv *v1 = NULL, *v2 = NULL, *vd = NULL;
    colInd *cs1, *cs2, *ct1, *ct2;
    colInd num_new_items;

    if(s1->align){
        //aligned at end

        cs1 = s1->colPtr, ct1 = cs1 + 1;
        cs2 = s2->colPtr, ct2 = cs1 + 1;
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
        s1->align = TOP_ALIGNED;
    } else {
        //aligned at beginning

        ct1 = s1->colPtr + _nsub, cs1 = ct1 - 1;
        ct2 = s2->colPtr + _nsub, cs2 = ct2 - 1;
        num_new_items = (colInd) (_nsub * _kmax);

        for(dim i = _nsub; i > 0; --i, ct1 = cs1--, ct2 = cs2--)
        {
            v1 = vecInit(v1, (dim) (*ct1 - *cs1), s1->items + *ct1);
            v2 = vecInit(v2, (dim) (*ct2 - *cs2), s2->items + *ct2);
            vd = vecInit(vd, 0, s1->items + num_new_items);
            vecAddBackward(v1, v2, vd);
            *ct1 = num_new_items;
            num_new_items -= vd->n;
        }

        *ct1 = num_new_items;
        s1->align = BOTTOM_ALIGNED;
    }

    mclFree(v1);
    mclFree(v2);
    mclFree(vd);
}

void sliceMultiply(const mcls *s1, mcls *s2) {
    // s1 left side read-only
    // s2 right side and destination

    colInd *cs, *ct;
    colInd num_new_items;
    mclv *tmp = vecNew(_kmax);
    mclv *d = vecInit(NULL, 0, NULL);

    if(s2->align){
        //bottom aligned => forward

        cs = s2->colPtr, ct = cs + 1;
        num_new_items = 0;

        for(dim i = _nsub; i > 0; --i, cs = ct++)
        {
            itemNCopy(tmp->items, s2->items + *cs, (dim) (*ct - *cs));
            sliceVecMult(s1, tmp, d, s2->items, num_new_items, *ct, true);
            *cs = num_new_items;
            num_new_items -= d->n;
        }

        *cs = num_new_items;
        s2->align = TOP_ALIGNED;
    } else {
        //top aligned => backward

        ct = s2->colPtr + _nsub, cs = ct - 1;
        num_new_items = (colInd) (_nsub * _kmax);

        for(dim i = _nsub; i > 0; --i, ct = cs--)
        {
            itemNCopy(tmp->items, s2->items + *cs, (dim) (*ct - *cs));
            sliceVecMult(s1, tmp, d, s2->items, *cs, num_new_items, false);
            *ct = num_new_items;
            num_new_items -= d->n;
        }

        *ct = num_new_items;
        s2->align = BOTTOM_ALIGNED;
    }

    mclFree(d);
    vecFree(&tmp);
}

void sliceVecMult(const mcls *slice, const mclv *v, mclv *dst, mcli *items, const colInd s, const colInd t, bool top){
    mcli *item = v->items;
    bool inorder = false;
    mclv *v1 = NULL, *v2 = NULL;
    colInd cs;
    dst->n = 0;

    for(int i = v->n; i > 0; --i){
        cs = slice->colPtr[item->id];
        v1 = vecInit(v1, (dim) (slice->colPtr[item->id+1] - cs), slice->items + cs);
        v2 = vecInit(v2, dst->n, dst->items);

        if(inorder){
            dst->items = items + t;
            vecAddMultBackward(item->val, v1, v2, dst);
        } else {
            dst->items = items + s;
            vecAddMultForward(item->val, v1, v2, dst);
        }

        inorder = !inorder;
    }

    if(top != inorder){
        if(top){
            dst->items = itemNMove(items + (t - dst->n) , dst->items, dst->n);
        } else {
            dst->items = itemNMove(items, dst->items, dst->n);
        }
    }

    mclFree(v1);
    mclFree(v2);
}