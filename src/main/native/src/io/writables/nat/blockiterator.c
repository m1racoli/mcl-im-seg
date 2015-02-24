#include <stdlib.h>
#include <mshtml.h>
#include "blockiterator.h"
#include "alloc.h"
#include "heap.h"
#include "slice.h"
#include "item.h"

sbi *sbiNNew(dim n){
    return mclAlloc(n * sizeof(sbi));
}

int subBlockItemComp(const void *i1, const void *i2){
    const sbi *v1 = i1, *v2 = i2;
    int cmp = ((v1->id > v2->id) - (v1->id < v2->id));
    if(cmp != 0) return cmp;
    return v1->col < v2->col ? -1 : 1;
}

static void fetch(sbi *sbi, const mcls *slice, mclh *heap, dim nsub){
    /*
            int s = offset[column], t = colPtr[column + 1];

			if(s == t) {
				return;
			}

			int id = (int) (rowInd[s] / nsub), i = s + 1;
			long end = (long) (id + 1) * (long) nsub;

			while(i < t && rowInd[i] < end){
				i++;
			}

			queue.add(new SubBlockSlice(column, i - s, id));
			offset[column] = i;
     */

    const colInd *s = slice->colPtr + sbi->col, *t = s + 1;

    if(*s == *t) return;

    mcli *item = slice->items + *s;
    mcli *item_end = slice->items + *t;
    sbi->id = (jint) ((item++)->id/nsub);
    rowInd end = (rowInd) (sbi->id + 1) * (rowInd) nsub;

    while(item != item_end && item->id < end){
        item++;
    }

    //TODO size
    heapInsert(heap, sbi);
    //TODO offset?
}

mclit *iteratorInit(mclit *it, JNIEnv *env, jobject buf, const dim nsub, const dim kmax) {

    if (!it)
        it = mclAlloc(sizeof(mclit));
    else {
        //should not happen
        printf("cannot start iteration. blockiterator already exists. exit!!!");
        exit(1);
    }

    // source slice
    it->slice = sliceInit(NULL, env, buf);

    // subblock
    dim datasize = sliceGetDataSize(nsub, nsub < kmax ? nsub : kmax);
    void* data = mclAlloc(datasize);
    it->buf = (*env)->NewDirectByteBuffer(env, data, datasize);
    it->block = sliceInit(NULL, env, it->buf);

    //do we need nsub?
    it->nsub = nsub;

    it->blockItems = sbiNNew(nsub);
    it->h = heapNew(NULL, nsub, sizeof(sbi), subBlockItemComp);

    for(int col = nsub; col > 0;){
        fetch(--col, it->slice, it->h);
    }

    return it;
}

bool iteratorNext(mclit *it) {
    if(it->h->n_inserted == 0){
        return false;
    }

    //TODO refill matrix
}

void iteratorFree(mclit **it) {

}