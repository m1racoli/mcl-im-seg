#include <jni.h>
#include "io_writables_nat_NativeArrayCSCSliceHelper.h"
#include "types.h"
#include "slice.h"
#include "alloc.h"
#include "blockiterator.h"
#include "logger.h"
#include "exception.h"

static dim _nsub;
static mclit *_blockIterator = NULL;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_setParams
        (JNIEnv *env, jclass cls, jint nsub, jint select, jboolean autoprune, jdouble inflation,
                jfloat cutoff, jfloat pruneA, jfloat pruneB, jint kmax, jboolean debug){
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_setParams");
    }

    _nsub = (dim) nsub;
    sliceSetParams((dim) nsub, (dim) select, autoprune, inflation, cutoff, (jdouble) pruneA, (jdouble) pruneB, (dim) kmax);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_clear(JNIEnv *env, jclass cls, jbyteArray buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_clear");
    }

    jboolean isCopy;

    jbyte *arr = (*env)->GetByteArrayElements(env,buf,&isCopy);

    if(IS_DEBUG){
        logDebug("array is copy: %s", isCopy ? "TRUE" : "FALSE");
    }

    *arr = TOP_ALIGNED;

    jint *colIdx = (jint*)(arr + 1);
    int i;

    for(i = _nsub; i >= 0; --i) {
        *(colIdx++) = 0;
    }

    (*env)->ReleaseByteArrayElements(env,buf,arr,0);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_add
        (JNIEnv *env, jclass cls, jbyteArray b1, jbyteArray b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_add");
    }

    mcls *s1 = sliceInitFromArr(NULL, env, b1);

    if(sliceSize(s1) == 0){
        mclFree(s1);
        return JNI_FALSE;
    }

    mcls *s2 = sliceInitFromArr(NULL, env, b2);
    sliceAdd(s1, s2);

    (*env)->ReleaseByteArrayElements(env,b1,s1->data,0);
    (*env)->ReleaseByteArrayElements(env,b2,s2->data,0);

    mclFree(s1);
    mclFree(s2);

    if(IS_DEBUG){
        checkException(env,true);
    }

    return JNI_TRUE;
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_equals
        (JNIEnv *env, jclass cls, jbyteArray b1, jbyteArray b2){
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_equals");
    }

    mcls *s1 = sliceInitFromArr(NULL, env, b1);
    mcls *s2 = sliceInitFromArr(NULL, env, b2);
    jboolean v = sliceEquals(s1, s2);

    (*env)->ReleaseByteArrayElements(env,b1,s1->data,0);
    (*env)->ReleaseByteArrayElements(env,b2,s2->data,0);
    mclFree(s1);
    mclFree(s2);

    if(IS_DEBUG){
        checkException(env,true);
    }

    return v;
}

JNIEXPORT jdouble JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_sumSquaredDifferences
        (JNIEnv *env, jclass cls, jbyteArray b1, jbyteArray b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_sumSquaredDifferences");
    }

    mcls *s1 = sliceInitFromArr(NULL, env, b1);
    mcls *s2 = sliceInitFromArr(NULL, env, b2);
    jdouble sum = sliceSumSquaredDiffs(s1, s2);

    (*env)->ReleaseByteArrayElements(env,b1,s1->data,0);
    (*env)->ReleaseByteArrayElements(env,b2,s2->data,0);
    mclFree(s1);
    mclFree(s2);

    if(IS_DEBUG){
        checkException(env,true);
    }

    return sum;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_addLoops
        (JNIEnv *env, jclass cls, jbyteArray buf, jint id) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_addLoops");
    }

    mcls *slice = sliceInitFromArr(NULL, env, buf);
    sliceAddLoops(slice, id);

    (*env)->ReleaseByteArrayElements(env,buf,slice->data,0);
    mclFree(slice);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_makeStochastic
        (JNIEnv *env, jclass cls, jbyteArray buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_makeStochastic");
    }

    mcls *slice = sliceInitFromArr(NULL, env, buf);
    sliceMakeStochastic(slice);

    (*env)->ReleaseByteArrayElements(env,buf,slice->data,0);
    mclFree(slice);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_inflateAndPrune
        (JNIEnv *env, jclass cls, jbyteArray buf, jobject jstats) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_inflateAndPrune");
    }

    mclStats *stats = statsInit(env, jstats);

    mcls *slice = sliceInitFromArr(NULL, env, buf);
    sliceInflateAndPrune(slice, stats);

    (*env)->ReleaseByteArrayElements(env,buf,slice->data,0);
    mclFree(slice);
    statsDump(stats, env, jstats);

    if(IS_DEBUG){
        checkException(env,true);
    }
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_startIterateBlocks
        (JNIEnv *env, jclass cls, jbyteArray src_buf, jbyteArray dst_buf) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_startIterateBlocks");
    }

    _blockIterator = iteratorInit(_blockIterator, env, src_buf, dst_buf, _nsub);

    if(!Java_io_writables_nat_NativeArrayCSCSliceHelper_nextBlock(env, cls)){
        return NULL;
    }

    if(IS_DEBUG){
        checkException(env,true);
    }

    return Java_io_writables_nat_NativeArrayCSCSliceHelper_nextBlock(env, cls);
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_nextBlock
        (JNIEnv *env, jclass cls) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_nextBlock");
    }

    if(iteratorNext(_blockIterator)){
        return JNI_TRUE;
    }

    iteratorFree(&_blockIterator);
    return JNI_FALSE;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeArrayCSCSliceHelper_multiply
        (JNIEnv *env, jclass cls, jbyteArray b1, jbyteArray b2) {
    if(IS_TRACE){
        logTrace("Java_io_writables_nat_NativeArrayCSCSliceHelper_multiply");
    }

    mcls *s1 = sliceInitFromArr(NULL, env, b1);
    mcls *s2 = sliceInitFromArr(NULL, env, b2);
    sliceMultiply(s1, s2);
    mclFree(s1);
    mclFree(s2);

    if(IS_DEBUG){
        checkException(env,true);
    }
}