#include "slice.h"
#include "alloc.h"
#include "vector.h"

static dim _nsub;
static dim _select;
static jboolean _autoprune;
static jdouble _inflation;
static jdouble _prune_A;
static jdouble _prune_B;
static value _cutoff;

static value computeThreshold(double avg, double max){
    double thresh = _prune_A * avg * (1.0 - _prune_B * (max - avg));
    thresh = thresh < 1.0e-7 ? 1.0e-7 : thresh;
    return (value) (thresh < max ? thresh : max);
}

void sliceSetNsub(dim nsub) {
    if(!_nsub) _nsub = nsub;
}

void sliceSetSelect(dim select) {
    if(!_select) _select = select;
}

void sliceSetAutoprune(jboolean autoprune) {
    if(!_autoprune) _autoprune = autoprune;
}

void sliceSetInflation(jdouble inflation) {
    if(!_inflation) _inflation = inflation;
}

void sliceSetCutoff(value cutoff) {
    if(!_cutoff) _cutoff = cutoff;
}

void sliceSetPruneA(jdouble pruneA) {
    if(!_prune_A) _prune_A = pruneA;
}

void sliceSetPruneB(jdouble pruneB) {
    if(!_prune_B) _prune_B = pruneB;
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
        vecAddLoops(v,--d);
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
    mcli *it, *ii;
    value threshold;
    double center;
    double max;
    double sum;
    double chaos;

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
            vecSelectionPrune(v,_select);
        }

        if(_autoprune){
            vecMakeStochasticAndStats(v, &center, &max);
        } else {
            vecInflateMakeStochasticAndStats(v, _inflation, &center, &max);
        }

        for(ii = v->items, it = new_items + v->n; new_items != it;){
            *(new_items++) = *(ii++);
        }

        num_new_items += v->n;
        chaos = (max - center) * v->n;
        if(max > 0.5) stats->attractors++;
        if(chaos < 1.0e-4) stats->homogen++;
        if(stats->chaos < chaos) stats->chaos = chaos;
        if(stats->kmax < v->n) stats->kmax = (jint) v->n;
    }

    *cs = num_new_items;
    mclFree(v);
}

dim sliceSize(const mcls *slice){
    return (dim) (slice->colPtr[_nsub]-slice->colPtr[0]);
}

void sliceAdd(mcls *s1, mcls *s2, mcls *dst){
    //TODO
}