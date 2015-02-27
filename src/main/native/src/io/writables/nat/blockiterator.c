#include <stdlib.h>
#include "blockiterator.h"
#include "alloc.h"
#include "logger.h"
#include "slice.h"
#include "heap.h"
#include "item.h"

static dim _nsub;

sbi *sbiNNew(const dim n, const mcls *slice){

    //if(loggerIsDebugEnabled()){
    //    logDebug("new sbis [n= %u]",n);
    //    logDebug("slice is %p",slice);
    //}

    sbi *items = mclAlloc(n * sizeof(*items));
    sbi *it = items;
    colInd *c = slice->colPtr;

    for(dim i = 0; i < n; ++i, ++it){
        *(dim*)&it->col = i;
        it->s = slice->items + *(c++);
        it->size = 0;
        *(mcli**)&it->t = slice->items + *c;
    }

    return items;
}

int subBlockItemComp(const void *i1, const void *i2){
    //if(loggerIsDebugEnabled()){
    //    logDebug("subBlockItem cmp: %p vs %p",i1,i2);
    //}
    const sbi *v1 = i1, *v2 = i2;
    int cmp = ((v1->id > v2->id) - (v1->id < v2->id));
    if(cmp != 0) return cmp;
    return v1->col < v2->col ? -1 : 1;
}

static void fetch(sbi *sbi, mclh *heap){

    sbi->s += sbi->size;

    if(sbi->s == sbi->t) return;

    mcli *item = sbi->s;
    sbi->id = (jint) ((item++)->id/_nsub);
    rowInd end = (rowInd) (sbi->id + 1) * (rowInd) _nsub;

    while(item != sbi->t && item->id < end){
        item++;
    }

    sbi->size = item - sbi->s;

    //if(loggerIsDebugEnabled()){
    //    logDebug("insert sbi [id: %i, col: %i, size :%u]",sbi->id,sbi->col,sbi->size);
    //}

    heapInsert(heap, sbi);
}

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub, const dim kmax) {

    //if(loggerIsDebugEnabled()){
    //    logDebug("init BlockIterator");
    //}

    if (!it)
        it = mclAlloc(sizeof(mclit));

    _nsub = nsub;

    // source slice
    it->slice = sliceInitFromBB(NULL, env, buf);
    //logDebug("returned slice is %p",it->slice);

    // subblock
    dim datasize = sliceGetDataSize(nsub, nsub < kmax ? nsub : kmax) + sizeof(jint);
    it->data = mclAlloc(datasize);

    if(loggerIsDebugEnabled()){
        logDebug("data for subBlock [p:%p, l:%u, until:%p]",it->data,datasize,it->data + datasize);
    }

    /*it->buf = (*env)->NewDirectByteBuffer(env, it->data, datasize);
    if((*env)->ExceptionCheck(env)){
        logErr("exception for creating direct buffer at %p with length %u",it->data,datasize);
        (*env)->ExceptionDescribe(env);
        return NULL;
    }*/

    jobject loc_buf = (*env)->NewDirectByteBuffer(env, it->data, datasize);
    if((*env)->ExceptionCheck(env)){
        logErr("exception for creating direct buffer at %p with length %u",it->data,datasize);
        (*env)->ExceptionDescribe(env);
        return NULL;
    }
    it->buf = (*env)->NewGlobalRef(env,loc_buf);
    if((*env)->ExceptionCheck(env)){
        logErr("exception for creating global ref of ByteBuffer");
        (*env)->ExceptionDescribe(env);
        return NULL;
    }

    it->block = sliceInitFromAdress(NULL, it->data); //sliceInitFromBB(NULL, env, it->buf);
    it->block->align = TOP_ALIGNED;
    it->blockItems = sbiNNew(nsub, it->slice);
    it->h = heapNew(NULL, nsub, subBlockItemComp);

    for(sbi *sbii = it->blockItems + nsub; sbii != it->blockItems;){
        fetch(--sbii, it->h);
    }

    return it;
}

bool iteratorNext(mclit *it) {

    if(IS_TRACE){
        logDebug("iteratorNext with %u items in heap",it->h->n_inserted);
    }

    if(!it->h->root){
        return false;
    }

    const jint id = ((sbi*)it->h->root->data)->id;
    const rowInd shift = - (rowInd) id * (rowInd) _nsub;

    jint current_col = 0;
    colInd new_items = 0;
    mcli *items = it->block->items;
    colInd *ct = it->block->colPtr;

    while(it->h->root && id == ((sbi*)it->h->root->data)->id){
        //logDebug("retrieve next subBlockColumn");
        //heapPrint(it->h);
        sbi *sbii = heapRemove(it->h);
        //logDebug("add to subBlock [id: %i, col: %u", sbii->id,sbii->col);

        while(current_col <= sbii->col) {
            current_col++;
            *(ct++) = new_items;
        }

        items = itemNCopy(items, sbii->s, sbii->size) + sbii->size;
        new_items += sbii->size;
        //*(ct++) = new_items;
        fetch(sbii, it->h);
    }

    while(current_col <= _nsub) {
        current_col++;
        *(ct++) = new_items;
    }

    for(mcli *i = it->block->items; i != items;){
        i++->id += shift;
    }

    *(jint*) items = id;

    if(IS_TRACE){
        logDebug("subBlock id = %i written to %p. thats an offset of %i bytes. current_col = %d",id,items,(char*) items - (char*)it->data,current_col);
        sliceDescribe(it->block);
    }

    return true;
}

void iteratorFree(mclit **it, JNIEnv *env) {
    /*
    mcls *slice;
    mcls *block;
    jobject buf;
    dim nsub;
    mclh *h;
    sbi *blockItems;
    void *data;
    */
    if(IS_TRACE){
        logDebug("iterator free [%p]",*it);
    }


    if(*it){
        mclFree((*it)->slice);
        mclFree((*it)->data);
        mclFree((*it)->block);
        (*env)->DeleteGlobalRef(env, (*it)->buf);
        mclFree((*it)->blockItems);
        heapFree(&(*it)->h);
        mclFree(*it);
        *it = NULL;
    }
}