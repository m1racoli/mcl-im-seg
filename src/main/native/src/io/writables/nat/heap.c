#include "heap.h"
#include "alloc.h"

static mclh *heapInit(void* h){
    mclh *heap = h;

    if(!heap && !(heap = mclAlloc(sizeof(mclh)))){
        return NULL;
    }

    heap->base = NULL;
    heap->heap_size = 0;
    heap->elem_size = 0;
    heap->cmp = NULL;
    heap->n_inserted = 0;

    return heap;
}

mclh *heapNew(mclh *h, dim heap_size, dim elem_size, int (*cmp)  (const void* lft, const void* rgt)){
    mclh *heap = h ? h : heapInit(NULL);


    //TODO

    return heap;
}

void heapReset(mclh *h){
    h->n_inserted = 0;
}

void heapFree(mclh **h){
    if(*h){
        if((*h)->base)
            mclFree((*h)->base);

        mclFree(*h);
        *h = NULL;
    }
}

