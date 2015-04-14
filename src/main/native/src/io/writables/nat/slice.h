
#ifndef nat_slice_h
#define nat_slice_h

#include "jni.h"
#include "item.h"
#include "stats.h"
#include "vector.h"

typedef struct {
    void* data;
    alignment *align;
    colInd *colPtr;
    mcli *items;
} mclSlice;

#define mcls mclSlice

void sliceSetParams(dim nsub, dim select, jboolean autoprune, jdouble inflation,
        value cutoff, jdouble pruneA, jdouble pruneB, dim kmax);

mcls *sliceInitFromAddress(mcls *slice, void *obj);

mcls *sliceInitFromArr(mcls *s, JNIEnv *env, jbyteArray arr);

mcls *sliceInitFromBB(mcls *slice, JNIEnv *env, jobject buf);

dim sliceGetDataSize(dim nsub, dim kmax);

jboolean sliceEquals(const mcls *s1, const mcls *s2);

jdouble sliceSumSquaredDiffs(const mcls *s1, const mcls *s2);

void sliceAddLoops(mcls *slice, jint id);

void sliceMakeStochastic(mcls *slice);

void sliceInflateAndPrune(mcls *slice, mclStats *stats);

dim sliceSize(const mcls *slice);

void sliceAdd(mcls *s1, const mcls *s2);

void sliceMultiply(const mcls *s1, mcls *s2);

void sliceVecMult(const mcls *slice, const mclv *v, mclv *dst, mcli *items, const colInd s, const colInd t, bool top);

void sliceDescribe(const mcls* self);

void sliceValidate(const mcls *self, bool can_be_empty);

#endif