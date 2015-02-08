
#ifndef nat_slice_h
#define nat_slice_h

#include "jni.h"
#include "item.h"

typedef struct {
    colInd *colPtr;
    mcli *items;
} mclSlice;

#define mcls mclSlice

void sliceSetNsub(dim nsub);

jint* colIdxFromByteBuffer(JNIEnv *env, jobject buf);

mclSlice *sliceInit(mcls *slice, JNIEnv *env, jobject buf);

jboolean sliceEquals(mcls *s1, mcls *s2);

jdouble sliceSumSquaredDiffs(mcls *s1, mcls *s2);

void sliceAddLoops(mcls *slice, jint id);

#endif