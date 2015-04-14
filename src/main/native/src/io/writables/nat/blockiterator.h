
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
    mclh *h;
    sbi *blockItems;
} mclBlockIterator;

#define mclit mclBlockIterator

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject src_buf, jobject dst_buf,const dim nsub);

mclit *iteratorInitFromArr(mclit *it, JNIEnv *env, jbyteArray src_buf, jbyteArray dst_buf,const dim nsub);

bool iteratorNext(mclit *it);

void iteratorFree(mclit **it);

#endif