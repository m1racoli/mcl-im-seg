#include <jni.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"
#include "types.h"
#include "slice.h"
#include "alloc.h"
#include "blockiterator.h"
#include "logger.h"

static dim _nsub;
static dim _kmax;
static mclit *_blockIterator = NULL;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setParams
        (JNIEnv *env, jclass cls, jint nsub, jint select, jboolean autoprune, jdouble inflation,
                jfloat cutoff, jfloat pruneA, jfloat pruneB, jint kmax, jboolean debug){
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_setParams");
    }

    _nsub = (dim) nsub;
    _kmax = (dim) kmax;
    sliceSetParams((dim) nsub, (dim) select, autoprune, inflation, cutoff, (jdouble) pruneA, (jdouble) pruneB, (dim) kmax);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_clear(JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_clear");
    }

    jint *colIdx = colIdxFromByteBuffer(env, buf);
    int i;

    for(i = _nsub; i >= 0; --i) {
        *(colIdx++) = 0;
    }
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_add
        (JNIEnv *env, jclass cls, jobject b1, jobject b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_add");
    }

    mcls *s1 = sliceInitFromBB(NULL, env, b1);

    if(sliceSize(s1) == 0){
        mclFree(s1);
        return JNI_FALSE;
    }

    mcls *s2 = sliceInitFromBB(NULL, env, b2);
    sliceAdd(s1, s2);

    mclFree(s1);
    mclFree(s2);
    return JNI_TRUE;
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_equals
        (JNIEnv *env, jclass cls, jobject b1, jobject b2){
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_equals");
    }

    mcls *s1 = sliceInitFromBB(NULL, env, b1);
    mcls *s2 = sliceInitFromBB(NULL, env, b2);
    jboolean v = sliceEquals(s1, s2);
    mclFree(s1);
    mclFree(s2);
    return v;
}

JNIEXPORT jdouble JNICALL Java_io_writables_nat_NativeCSCSliceHelper_sumSquaredDifferences
        (JNIEnv *env, jclass cls, jobject b1, jobject b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_sumSquaredDifferences");
    }

    mcls *s1 = sliceInitFromBB(NULL, env, b1);
    mcls *s2 = sliceInitFromBB(NULL, env, b2);
    jdouble sum = sliceSumSquaredDiffs(s1, s2);
    mclFree(s1);
    mclFree(s2);
    return sum;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_addLoops
        (JNIEnv *env, jclass cls, jobject buf, jint id) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_addLoops");
    }

    //logDebug("init slice");
    mcls *slice = sliceInitFromBB(NULL, env, buf);
    //logDebug("add loops");
    sliceAddLoops(slice, id);
    //logDebug("free slice");
    mclFree(slice);
    //logDebug("return");
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_makeStochastic
        (JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_makeStochastic");
    }

    mcls *slice = sliceInitFromBB(NULL, env, buf);
    sliceMakeStochastic(slice);
    mclFree(slice);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_inflateAndPrune
        (JNIEnv *env, jclass cls, jobject buf, jobject jstats) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_inflateAndPrune");
    }

    mclStats *stats = statsInit(env, jstats);
    if(!stats)return;
    mcls *slice = sliceInitFromBB(NULL, env, buf);
    sliceInflateAndPrune(slice, stats);
    mclFree(slice);
    statsDump(stats, env, jstats);
}

JNIEXPORT jint JNICALL Java_io_writables_nat_NativeCSCSliceHelper_size
        (JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_size");
    }

    colInd *colIdx = colIdxFromByteBuffer(env, buf);
    return colIdx[_nsub]-colIdx[0];
}

JNIEXPORT jobject JNICALL Java_io_writables_nat_NativeCSCSliceHelper_startIterateBlocks
        (JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_startIterateBlocks");
    }

    //if(loggerIsDebugEnabled()){
        //logDebug("start iterate subBlocks");
    //}
    _blockIterator = iteratorInit(_blockIterator, env, buf, _nsub, _kmax);

    if(!Java_io_writables_nat_NativeCSCSliceHelper_nextBlock(env, cls)){
        return NULL;
    }

    return _blockIterator->buf;
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_nextBlock
        (JNIEnv *env, jclass cls) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_nextBlock");
    }


    //logDebug("iterate next subBlocks");

    if(iteratorNext(_blockIterator)){
        //logDebug("has next");
        return JNI_TRUE;
    }

    iteratorFree(&_blockIterator, env);

    return JNI_FALSE;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_multiply
        (JNIEnv *env, jclass cls, jobject b2, jobject b1) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_multiply");
    }

    mcls *s1 = sliceInitFromBB(NULL, env, b1);
    mcls *s2 = sliceInitFromBB(NULL, env, b2);
    sliceMultiply(s1, s2);
    mclFree(s1);
    mclFree(s2);

}