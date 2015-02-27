#include <stdlib.h>
#include "alloc.h"
#include "logger.h"

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
        mblock = object
                ? realloc(object, new_size)
                : malloc(new_size);
    }

    //if(loggerIsDebugEnabled()){
        //logDebug("allocated [%p, %u]",mblock,new_size);
    //}

    if (new_size && (!mblock)) {
        logErr("could not allocate %i bytes of memory",new_size);
        exit(1);
    }

    return mblock;
}

void mclFree (void* object) {
    //if(loggerIsDebugEnabled()){
    //    logDebug("free [%p]",object);
    //}
    if (object) free(object);
}

void mclNFree (void* base, dim n_elem, dim elem_size, void (*objRelease)(void *)) {
    if(n_elem && objRelease){
        char *ob = base;
        while (n_elem-- > 0){
            objRelease(ob);
            ob += elem_size;
        }
        mclFree(base);
    }
}

void* mclNAlloc (dim n_elem, dim elem_size, void* (*obInit)(void*)) {
    return mclNRealloc(NULL, n_elem, 0, elem_size, obInit);
}

void* mclNRealloc (void* mem, dim n_elem, dim n_elem_prev, dim elem_size, void* (*obInit)(void*)) {
    char *ob;
    mem = mclRealloc(mem, n_elem * elem_size);

    if(!mem)
        return NULL;

    if(obInit && n_elem > n_elem_prev){
        ob = ((char*) mem) + (elem_size * n_elem_prev);
        while(n_elem-- > n_elem_prev) {
            obInit(ob);
            ob += elem_size;
        }
    }
    return mem;
}