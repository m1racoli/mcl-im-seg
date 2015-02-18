
#ifndef nat_slice_h
#define nat_slice_h

#include "jni.h"
#include "item.h"
#include "stats.h"

typedef struct {
    alignment align;
    colInd *colPtr;
    mcli *items;
} mclSlice;

#define mcls mclSlice

void sliceSetNsub(dim nsub);

void sliceSetSelect(dim select);

void sliceSetAutoprune(jboolean autoprune);

void sliceSetInflation(jdouble inflation);

void sliceSetCutoff(value cutoff);

void sliceSetPruneA(jdouble pruneA);

void sliceSetPruneB(jdouble pruneB);

jint* colIdxFromByteBuffer(JNIEnv *env, jobject buf);

mclSlice *sliceInit(mcls *slice, JNIEnv *env, jobject buf);

jboolean sliceEquals(const mcls *s1, const mcls *s2);

jdouble sliceSumSquaredDiffs(const mcls *s1, const mcls *s2);

void sliceAddLoops(mcls *slice, jint id);

void sliceMakeStochastic(mcls *slice);

void sliceInflateAndPrune(mcls *slice, mclStats *stats);

dim sliceSize(const mcls *slice);

void sliceAdd(mcls *s1, mcls *s2, mcls *dst);

mclSlice *sliceFlipped(mcls *slice);

#endif