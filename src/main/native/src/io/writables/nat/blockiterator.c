#include <stdlib.h>
#include "blockiterator.h"
#include "alloc.h"

static dim _nsub;

sbi *sbiNNew(const dim n, const mcls *slice){
    sbi *items = mclAlloc(n * sizeof(sbi));
    sbi *it = items;
    colInd *c = slice->colPtr;

    for(dim i = 0; i < n; ++i, ++it){
        *(dim*)&it->col = i;
        it->s = slice->items + *c++;
        it->size = 0;
        *(mcli**)&it->t = slice->items + *c;
    }

    return items;
}

int subBlockItemComp(const void *i1, const void *i2){
    const sbi *v1 = i1, *v2 = i2;
    int cmp = ((v1->id > v2->id) - (v1->id < v2->id));
    if(cmp != 0) return cmp;
    return v1->col < v2->col ? -1 : 1;
}

static void fetch(sbi *sbi, mclh *heap){

    if(sbi->s == sbi->t) return;

    sbi->s += sbi->size;
    mcli *item = sbi->s;
    sbi->id = (jint) ((item++)->id/_nsub);
    rowInd end = (rowInd) (sbi->id + 1) * (rowInd) _nsub;

    while(item != sbi->t && item->id < end){
        item++;
    }

    sbi->size = item - sbi->s;
    heapInsert(heap, sbi);
}

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub, const dim kmax) {

    if (!it)
        it = mclAlloc(sizeof(mclit));

    _nsub = nsub;

    // source slice
    it->slice = sliceInit(NULL, env, buf);

    // subblock
    dim datasize = sliceGetDataSize(nsub, nsub < kmax ? nsub : kmax) + sizeof(jint);
    it->data = mclAlloc(datasize);
    jobject local_buf = (*env)->NewDirectByteBuffer(env, it->data, datasize);
    it->buf = (*env)->NewGlobalRef(env, local_buf);
    it->block = sliceInit(NULL, env, it->buf);
    it->blockItems = sbiNNew(nsub, it->slice);
    it->h = heapNew(NULL, nsub, subBlockItemComp);

    for(sbi *sbii = it->blockItems + nsub; sbii > it->blockItems;){
        fetch(--sbii, it->h);
    }

    return it;
}

bool iteratorNext(mclit *it) {

    if(!it->h->root){
        return false;
    }

    jint last_col = -1;
    colInd new_items = 0;
    jint id = ((sbi*)it->h->root->data)->id;
    mcli *items = it->block->items;
    colInd *ct = it->block->colPtr;
    *ct++ = 0;

    while(it->h->root && id == ((sbi*)it->h->root->data)->id){
        sbi *const sbii = heapRemove(it->h);

        for(jint i = last_col + 1; i < sbii->col; i++){
            *ct++ = new_items;
        }

        items = itemNCopy(items, sbii->s, sbii->size) + sbii->size;
        new_items += sbii->size;
        *ct++ = new_items;
        last_col = (jint) sbii->col;
        fetch(sbii, it->h);
    }

    for(jint i = last_col + 1; i < _nsub; i++){
        *ct++ = new_items;
    }

    *(jint*) items = id;

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