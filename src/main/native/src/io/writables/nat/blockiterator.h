
#ifndef blockiterator_h
#define blockiterator_h

#include "slice.h"

typedef struct {
    jint id;
    const dim col;
    mcli *s;
    dim size;
    mcli *const t;
} subBlockItem;

#define sbi subBlockItem

sbi *sbiNNew(const dim n, const mcls *slice);

int subBlockItemComp(const void *i1, const void *i2);

typedef struct {
    mcls *slice;
    mcls *block;
    jobject buf;
    mclh *h;
    sbi *blockItems;
    void* data;
} mclBlockIterator;

#define mclit mclBlockIterator

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub, const dim kmax);

bool iteratorNext(mclit *it);

void iteratorFree(mclit **it, JNIEnv *env);

#endif