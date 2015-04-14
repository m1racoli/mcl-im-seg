#ifndef nat_vector_h
#define nat_vector_h

#include "types.h"
#include "item.h"
#include "stats.h"
#include "heap.h"

typedef struct {
    dim n; //number of items
    mcli* items;
} mclVector;

#define mclv mclVector

mclv* vecInit (mclv *vec, dim n, mcli *items);

mclv *vecNew(dim n);

void vecFree(mclv **v);

jboolean vecEquals(const mclv *v1, const mclv *v2);

jdouble vecSumSquaredDiffs(const mclv *v1, const mclv *v2);

void vecAddLoops(mclv *v, rowInd d);

void vecMakeStochastic(mclv *v);

void vecInflateAndStats(mclv *v, double inf, double *sum, double *max);

void vecThresholdPrune(mclv *v, value threshold, mclStats *stats);

void vecSelectionPrune(mclv *v, mclh *h, dim _select);

void vecAddForward(const mclv *v1, const mclv *v2, mclv *dst);

void vecAddBackward(const mclv *v1, const mclv *v2, mclv *dst);

void vecAddMultForward(value f, const mclv* v1, const mclv *v2, mclv *dst);

void vecAddMultBackward(value val, const mclv* v1, const mclv *v2, mclv *dst);

void vectorDescribe(const mclv* self);

void vectorValidate(const mclv* self);

#endif