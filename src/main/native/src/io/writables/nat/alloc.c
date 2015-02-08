#include <stdlib.h>
#include "alloc.h"

void* mclAlloc (dim size) {
    return mclRealloc(NULL, size);
}

void* mclRealloc (void* object, dim new_size) {
    void* mblock = NULL;

    if(!new_size){
        if(object){
            mclFree(object);
        }
    } else {
        //TODO alloc
    }

    //TODO check

    return mblock;
}

void mclFree (void* object) {
    if (object) free(object);
}

void mclNFree (void* base, dim n_elem, dim elem_size, void (*objRelease)(void *)) {
    //TODO implement
}

void* mclNAlloc (dim n_elem, dim elem_size, void* (*obInit)(void*)) {
    //TODO implement
    return NULL;
}

void* mclNRealloc (void* mem, dim n_elem, dim n_elem_prev, dim elem_size, void* (*obInit)(void*)) {
    //TODO implement
    return NULL;
}