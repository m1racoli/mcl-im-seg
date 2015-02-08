#ifndef nat_vector_h
#define nat_vector_h

#include "types.h"
#include "vector.h"
#include "item.h"
#include "stats.h"

typedef struct {
    dim n; //number of items
    mcli* items;
} mclVector;

#define mclv mclVector

mclv* vecInit (mclv *vec, dim n, mcli *items);

jboolean vecEquals(mclv *v1, mclv *v2);

jdouble vecSumSquaredDiffs(mclv *v1, mclv *v2);

void vecAddLoops(mclv *v, rowInd d);

void vecMakeStochastic(mclv *v);

void vecMakeStochasticAndStats(mclv *v, double *center, double *max);

void vecInflateAndStats(mclv *v, double inf, double *sum, double *max);

void vecInflateMakeStochasticAndStats(mclv *v, double inf, double *center, double *max);

void vecThresholdPrune(mclv *v, value threshold, mclStats *stats);

void vecSelectionPrune(mclv *v, dim _select);

#endif