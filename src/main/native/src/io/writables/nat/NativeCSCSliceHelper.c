#include <jni.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"
#include "types.h"
#include "slice.h"
#include "alloc.h"
#include "blockiterator.h"

static dim _nsub;
static dim _kmax;
static mclit *_blockIterator = NULL;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setParams
        (JNIEnv *env, jclass cls, jint nsub, jint select, jboolean autoprune, jdouble inflation,
                jfloat cutoff, jfloat pruneA, jfloat pruneB, jint kmax){
    _nsub = (dim) nsub;
    _kmax = (dim) kmax;
    sliceSetParams((dim) nsub, (dim) select, autoprune, inflation, cutoff, (jdouble) pruneA, (jdouble) pruneB, (dim) kmax);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_clear(JNIEnv *env, jclass cls, jobject buf) {
    jint *colIdx = colIdxFromByteBuffer(env, buf);
    int i;

    for(i = _nsub; i >= 0; --i) {
        *(colIdx++) = 0;
    }
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_add
        (JNIEnv *env, jclass cls, jobject b1, jobject b2) {
    mcls *s1 = sliceInit(NULL, env, b1);

    if(sliceSize(s1) == 0){
        mclFree(s1);
        return JNI_FALSE;
    }

    mcls *s2 = sliceInit(NULL, env, b2);
    sliceAdd(s1, s2);

    mclFree(s1);
    mclFree(s2);
    return JNI_TRUE;
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_equals
        (JNIEnv *env, jclass cls, jobject b1, jobject b2){
    mcls *s1 = sliceInit(NULL, env, b1);
    mcls *s2 = sliceInit(NULL, env, b2);
    jboolean v = sliceEquals(s1, s2);
    mclFree(s1);
    mclFree(s2);
    return v;
}

JNIEXPORT jdouble JNICALL Java_io_writables_nat_NativeCSCSliceHelper_sumSquaredDifferences
        (JNIEnv *env, jclass cls, jobject b1, jobject b2) {
    mcls *s1 = sliceInit(NULL, env, b1);
    mcls *s2 = sliceInit(NULL, env, b2);
    jdouble sum = sliceSumSquaredDiffs(s1, s2);
    mclFree(s1);
    mclFree(s2);
    return sum;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_addLoops
        (JNIEnv *env, jclass cls, jobject buf, jint id) {
    mcls *slice = sliceInit(NULL, env, buf);
    sliceAddLoops(slice, id);
    mclFree(slice);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_makeStochastic
        (JNIEnv *env, jclass cls, jobject buf) {
    mcls *slice = sliceInit(NULL, env, buf);
    sliceMakeStochastic(slice);
    mclFree(slice);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_inflateAndPrune
        (JNIEnv *env, jclass cls, jobject buf, jobject jstats) {
    mclStats *stats = statsInit(env, jstats);
    if(!stats)return;
    mcls *slice = sliceInit(NULL, env, buf);
    sliceInflateAndPrune(slice, stats);
    mclFree(slice);
    statsDump(stats, env, jstats);
}

JNIEXPORT jint JNICALL Java_io_writables_nat_NativeCSCSliceHelper_size
        (JNIEnv *env, jclass cls, jobject buf) {
    colInd *colIdx = colIdxFromByteBuffer(env, buf);
    return colIdx[_nsub]-colIdx[0];
}

JNIEXPORT jobject JNICALL Java_io_writables_nat_NativeCSCSliceHelper_startIterateBlocks
        (JNIEnv *env, jclass cls, jobject buf) {
    _blockIterator = iteratorInit(_blockIterator, env, buf, _nsub, _kmax);
    return Java_io_writables_nat_NativeCSCSliceHelper_nextBlock(env, cls);
}

JNIEXPORT jobject JNICALL Java_io_writables_nat_NativeCSCSliceHelper_nextBlock
        (JNIEnv *env, jclass cls) {
    if(iteratorNext(_blockIterator)){
        return _blockIterator->buf;
    }

    iteratorFree(&_blockIterator, env);
    return NULL;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_multiply
        (JNIEnv *env, jclass cls, jobject b2, jobject b1) {
    mcls *s1 = sliceInit(NULL, env, b1);
    mcls *s2 = sliceInit(NULL, env, b2);
    sliceMultiply(s1, s2);
    mclFree(s1);
    mclFree(s2);
}