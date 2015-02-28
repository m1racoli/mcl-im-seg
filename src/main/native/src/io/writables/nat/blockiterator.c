#include <stdlib.h>
#include "blockiterator.h"
#include "alloc.h"
#include "logger.h"

static dim _nsub;

sbi *sbiNNew(const dim n, const mcls *slice){

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

    heapInsert(heap, sbi);
}

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject src_buf, jobject dst_buf, const dim nsub) {

    if(IS_TRACE){
        logTrace("iteratorInit");
    }

    if (!it)
        it = mclAlloc(sizeof(mclit));

    _nsub = nsub;

    it->slice = sliceInitFromBB(NULL, env, src_buf);
    it->block = sliceInitFromBB(NULL, env, dst_buf);

    if(IS_DEBUG){
        sliceValidate(it->slice, false);
        sliceValidate(it->slice, true);
    }

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
        sbi *sbii = heapRemove(it->h);

        while(current_col <= sbii->col) {
            current_col++;
            *(ct++) = new_items;
        }

        items = itemNCopy(items, sbii->s, sbii->size) + sbii->size;
        new_items += sbii->size;
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
        sliceDescribe(it->block);
    }

    if(IS_DEBUG){
        sliceValidate(it->block, false);
    }

    return true;
}

void iteratorFree(mclit **it) {

    if(IS_TRACE){
        logDebug("iterator free [%p]",*it);
    }

    if(*it){
        mclFree((*it)->slice);
        mclFree((*it)->block);
        mclFree((*it)->blockItems);
        heapFree(&(*it)->h);

        mclFree(*it);
        *it = NULL;
    }
}