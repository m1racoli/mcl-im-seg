
#ifndef blockiterator_h
#define blockiterator_h

#include "slice.h"

typedef struct {
    int col;
    dim size;
    jint id;
} subBlockItem;

#define sbi subBlockItem

sbi *sbiNNew(dim n);

int subBlockItemComp(const void *i1, const void *i2);

typedef struct {
    const mcls *slice;
    mcls *block;
    jobject buf;
    dim nsub;
    mclh *h;
    sbi *blockItems;
} mclBlockIterator;

#define mclit mclBlockIterator

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub, const dim kmax);

bool iteratorNext(mclit *it);

void iteratorFree(mclit **it);

#endif