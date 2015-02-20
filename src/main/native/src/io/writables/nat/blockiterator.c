#include "blockiterator.h"
#include "alloc.h"
#include "slice.h"

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub) {

    if (!it)
        it = mclAlloc(sizeof(mclit));

    it->slice = sliceInit(NULL, env, buf);
    it->nsub = nsub;

    //TODO alloc matrix block
    it->block = NULL; //TODO
    it->buf = (*env)->NewDirectByteBuffer(env, NULL, 0); //TODO

    //TODO init and fill heap

    return it;
}

bool iteratorNext(mclit *it) {

}

void iteratorFree(mclit **it) {

}