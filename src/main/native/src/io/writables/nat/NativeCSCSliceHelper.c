#include <jni.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"
#include "types.h"
#include "slice.h"
#include "alloc.h"
#include "blockiterator.h"
#include "logger.h"
#include "exception.h"

static dim _nsub;
static mclit *_blockIterator = NULL;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setParams
        (JNIEnv *env, jclass cls, jint nsub, jint select, jboolean autoprune, jdouble inflation,
                jfloat cutoff, jfloat pruneA, jfloat pruneB, jint kmax, jboolean debug){
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_setParams");
    }

    _nsub = (dim) nsub;
    sliceSetParams((dim) nsub, (dim) select, autoprune, inflation, cutoff, (jdouble) pruneA, (jdouble) pruneB, (dim) kmax);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_clear(JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_clear");
    }

    jbyte *arr = (*env)->GetDirectBufferAddress(env,buf);

    *arr = TOP_ALIGNED;

    jint *colIdx = (jint*)(arr + 1);
    int i;

    for(i = _nsub; i >= 0; --i) {
        *(colIdx++) = 0;
    }

    if(IS_DEBUG){
        checkException(env,true);
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

    if(IS_DEBUG){
        checkException(env,true);
    }

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

    if(IS_DEBUG){
        checkException(env,true);
    }

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

    if(IS_DEBUG){
        checkException(env,true);
    }

    return sum;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_addLoops
        (JNIEnv *env, jclass cls, jobject buf, jint id) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_addLoops");
    }

    mcls *slice = sliceInitFromBB(NULL, env, buf);
    sliceAddLoops(slice, id);
    mclFree(slice);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_makeStochastic
        (JNIEnv *env, jclass cls, jobject buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_makeStochastic");
    }

    mcls *slice = sliceInitFromBB(NULL, env, buf);
    sliceMakeStochastic(slice);
    mclFree(slice);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_inflateAndPrune
        (JNIEnv *env, jclass cls, jobject buf, jobject jstats) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_inflateAndPrune");
    }

    mclStats *stats = statsInit(env, jstats);

    mcls *slice = sliceInitFromBB(NULL, env, buf);
    sliceInflateAndPrune(slice, stats);
    //TODO align top if not done already
    mclFree(slice);
    statsDump(stats, env, jstats);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_startIterateBlocks
        (JNIEnv *env, jclass cls, jobject src_buf, jobject dst_buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_startIterateBlocks");
    }

    _blockIterator = iteratorInit(_blockIterator, env, src_buf, dst_buf, _nsub);

    if(!Java_io_writables_nat_NativeCSCSliceHelper_nextBlock(env, cls)){
        return NULL;
    }

    if(IS_DEBUG){
        checkException(env,true);
    }

    return Java_io_writables_nat_NativeCSCSliceHelper_nextBlock(env, cls);
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_nextBlock
        (JNIEnv *env, jclass cls) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_nextBlock");
    }

    if(iteratorNext(_blockIterator)){
        return JNI_TRUE;
    }

    iteratorFree(&_blockIterator);
    return JNI_FALSE;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_multiply
        (JNIEnv *env, jclass cls, jobject b1, jobject b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeCSCSliceHelper_multiply");
    }

    mcls *s1 = sliceInitFromBB(NULL, env, b1);
    mcls *s2 = sliceInitFromBB(NULL, env, b2);
    sliceMultiply(s1, s2);
    mclFree(s1);
    mclFree(s2);

    if(IS_DEBUG){
        checkException(env,true);
    }
}