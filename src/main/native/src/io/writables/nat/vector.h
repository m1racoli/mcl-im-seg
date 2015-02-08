#ifndef nat_vector_h
#define nat_vector_h

#include "types.h"
#include "vector.h"
#include "item.h"

typedef struct {
    dim n; //number of items
    mcli* items;
} mclVector;

#define mclv mclVector

mclv* vecInit (mclv *vec, dim n, mcli *items);

jboolean vecEquals(mclv *v1, mclv *v2);

jdouble vecSumSquaredDiffs(mclv *v1, mclv *v2);

void vecAddLoops(mclv *v, rowInd d);

#endif