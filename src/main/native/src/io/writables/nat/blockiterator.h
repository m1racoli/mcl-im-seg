
#ifndef blockiterator_h
#define blockiterator_h

#include "slice.h"

typedef struct {
    const mcls *slice;
    mcls *block;
    jobject buf;
    dim nsub;
} mclBlockIterator;

#define mclit mclBlockIterator

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub);

bool iteratorNext(mclit *it);

void iteratorFree(mclit **it);

#endif