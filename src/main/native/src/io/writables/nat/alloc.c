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