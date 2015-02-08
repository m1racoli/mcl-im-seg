#include "slice.h"
#include "alloc.h"
#include "vector.h"

static dim _nsub;

void sliceSetNsub(dim nsub) {
    if(!_nsub) _nsub = nsub;
}

colInd *colIdxFromByteBuffer(JNIEnv *env, jobject buf) {
    void* dBuf = (*env)->GetDirectBufferAddress(env,buf);
    return (colInd*) dBuf;
}

mcls *sliceInit(mcls *slice, JNIEnv *env, jobject buf) {
    if(!slice)
        slice = mclAlloc(sizeof(mcls));

    slice->colPtr = colIdxFromByteBuffer(env, buf);
    slice->items = (mcli*) (slice->colPtr + _nsub + 1);
    return slice;
}

jboolean sliceEquals(mcls *s1, mcls *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;

    for(i = _nsub - 1; i >= 0; --i) {
        v1 = vecInit(v1, *(c1--) - *c1, s1->items + *c1);
        v2 = vecInit(v2, *(c2--) - *c2, s2->items + *c2);

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

jdouble sliceSumSquaredDiffs(mcls *s1, mcls *s2) {
    int i;
    mclv *v1 = NULL;
    mclv *v2 = NULL;
    colInd *c1 = s1->colPtr + _nsub;
    colInd *c2 = s2->colPtr + _nsub;
    jdouble sum = 0.0;

    for(i = _nsub - 1; i >= 0; --i) {
        v1 = vecInit(v1, *(c1--) - *c1, s1->items + *c1);
        v2 = vecInit(v2, *(c2--) - *c2, s2->items + *c2);
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
        v = vecInit(v, *(c--) - *c, slice->items + *c);
        vecAddLoops(v,--d);
    }

    mclFree(v);
}