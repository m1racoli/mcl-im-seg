#include <stdlib.h>
#include <math.h>
#include "vector.h"
#include "alloc.h"

mclv *vecInit(mclv *vec, dim n, mcli *items) {

    if (!vec)
        vec = mclAlloc(sizeof(mclv));

    vec->n = n;
    vec->items = items;

    return vec;
}

jboolean vecEquals(mclVector const *v1, mclVector const *v2) {
    mcli *s1 = v1->items;
    mcli *t1 = s1 + v1->n;
    mcli *s2 = v2->items;
    mcli *t2 = s2 + v2->n;

    while (s1 != t1 && s2 != t2) {
        if (!itemEquals(s1++, s2++))
            return JNI_FALSE;
    }

    return (jboolean) (s1 == t1 && s2 == t2);
}

jdouble vecSumSquaredDiffs(mclVector const *v1, mclVector const *v2) {
    mcli *s1 = v1->items;
    mcli *t1 = s1 + v1->n;
    mcli *s2 = v2->items;
    mcli *t2 = s2 + v2->n;
    jdouble sum = 0.0;

    while (s1 != t1 && s2 != t2) {
        if (s1->id > s2->id) {
            sum += (s2++)->val;
        } else if (s1->id < s2->id) {
            sum += (s1++)->val;
        } else {
            jdouble s = (s1++)->val - (s2++)->val;
            sum += s * s;
        }
    }

    while (s1 != t1) {
        sum += (s1++)->val;
    }
    while (s2 != t2) {
        sum += (s2++)->val;
    }

    return sum;
}

void vecAddLoops(mclv *v, rowInd d) {
    mcli *c = NULL;
    value max = 0.0;
    mcli *i, *t;

    for(i = v->items, t = i + v->n; i != t; i++){
        if(i->id == d){
            c = i;
        }
        if(max < c->val) max = c->val;
    }

    if(!c){
        //TODO error
        exit(1);
    }

    c->val = max;
}

void vecMakeStochastic(mclv *v) {
    double sum = 0.0;
    mcli *s, *i;

    for(s = v->items, i = s + v->n; i != s;){
        sum += (--i)->val;
    }

    for(s = v->items, i = s + v->n; i != s;){
        (--i)->val /= sum;
    }
}

void vecMakeStochasticAndStats(mclv *v, double *center, double *max) {
    double c = 0.0;
    double m = 0.0;
    double s = 0.0;
    mcli *i, *t;

    for(i = v->items, t = i + v->n; i != t; ++i){
        s += i->val;
    }

    for(i = v->items, t = i + v->n; i != t; ++i){
        i->val /= s;
        if(m < i->val) m = i->val;
        c += i->val*i->val;
    }

    *center = c;
    *max = m;
}

void vecInflateAndStats(mclv *v, double inf, double *sum, double *max) {
    double s = 0.0;
    double m = 0.0;
    mcli *i, *t;

    for(i = v->items, t = i + v->n; i != t; ++i){
        i->val = (value) pow(i->val, inf);
        s += i->val;
        if(m < i->val) m = i->val;
    }

    *sum = s;
    *max = m;
}

void vecInflateMakeStochasticAndStats(mclv *v, double inf, double *center, double *max) {
    double s = 0.0;
    double m = 0.0;
    double c = 0.0;
    mcli *i, *t;

    for(i = v->items, t = i + v->n; i != t; ++i){
        i->val = (value) pow(i->val, inf);
        s += i->val;
    }

    for(i = v->items, t = i + v->n; i != t; ++i){
        i->val /= s;
        if(m < i->val) m = i->val;
        c += i->val*i->val;
    }

    *center = c;
    *max = m;
}

void vecThresholdPrune(mclv *v, value threshold, mclStats *stats) {
    dim n = 0;
    mcli *i, *t, *d;

    for(i = v->items, t = i + v->n, d = i; i != t; ++i){
        if(i->val >= threshold){
            *(d++) = *i;
            n++;
        } else {
            stats->cutoff++;
        }
    }

    v->n = n;
}

void vecSelectionPrune(mclv *v, dim _select) {
    //TODO selection prune
}