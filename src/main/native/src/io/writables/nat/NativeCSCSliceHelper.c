#include <jni.h>
#include <stdio.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"
#include "types.h"
#include "slice.h"
#include "vector.h"
#include "stats.h"
#include "alloc.h"

static dim _nsub;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setNsub
        (JNIEnv *env, jclass cls, jint nsub) {
    _nsub = nsub;
    sliceSetNsub(nsub);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setSelect
        (JNIEnv *env, jclass cls, jint select) {
    sliceSetSelect(select);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setAutoprune
        (JNIEnv *env, jclass cls, jboolean autoprune) {
    sliceSetAutoprune(autoprune);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setInflation
        (JNIEnv *env, jclass cls, jdouble inflation) {
    sliceSetInflation(inflation);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setCutoff
        (JNIEnv *env, jclass cls, jfloat cutoff) {
    sliceSetCutoff(cutoff);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setPruneA
        (JNIEnv *env, jclass cls, jfloat pruneA) {
    sliceSetPruneA(pruneA);
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setPruneB
        (JNIEnv *env, jclass cls, jfloat pruneB) {
    sliceSetPruneB(pruneB);
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
    sliceAdd(s1, s2, s1);

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