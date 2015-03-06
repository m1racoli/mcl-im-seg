#include <string.h>
#include <math.h>
#include "heap.h"
#include "alloc.h"
#include "logger.h"

static inline int hpiCmp(mclh *h, const hpi *i1, const hpi *i2){
    //logDebug("hpiCmp[%p %p]",i1,i2);
    return h->cmp(i1->data,i2->data);
}

static inline dim maxRank(dim max_size){
    return (dim) floor(log(max_size)/ log((1.0 + sqrt(5.0))/2.0));
}

static hpi *hpiNew(void *data, hpi *neighbor){

    //if(loggerIsDebugEnabled()){
    //    logDebug("new heap item");
    //}

    hpi *i = mclAlloc(sizeof(hpi));

    i->data = data;
    i->child = NULL;
    i->degree = 0;

    if(neighbor){
        i->right = neighbor;
        i->left = neighbor->left;
        neighbor->left = i;
        i->left->right = i;
    } else {
        i->left = i;
        i->right =  i;
    }

    return i;
}

static inline void hpiLink(hpi *parent, hpi *node){
    node->left->right = node->right;
    node->right->left = node->left;

    if(!parent->child){
        parent->child = node;
        node->right = node;
        node->left = node;
    } else {
        node->left = parent->child;
        node->right = parent->child->right;
        parent->child->right = node;
        node->right->left = node;
    }

    parent->degree++;
}

static void hpiFree(hpi **node){

//    if(loggerIsDebugEnabled()){
//        logDebug("hpi free [%p]",*node);
//    }

    hpi *child = (*node)->child;
    if(child){
        child->left->right = NULL;

        for(hpi *i = child, *n; i; i = n){
            n = i->right;
            hpiFree(&i);
        }
    }

    mclFree(*node);
    *node = NULL;
}

mclh *heapNew(mclh *h, dim max_size, int (*cmp)  (const void*, const void*)){
    if(h) return h;

    if(IS_TRACE){
        logTrace("heapNew");
    }

    if(max_size <= 0){
        logFatal("heap size must be positive! %d",max_size);
    }

    mclh *heap = mclAlloc(sizeof(mclh));

    heap->root = NULL;
    *(dim*)&heap->max_size = max_size;
    heap->cmp = cmp;
    heap->n_inserted = 0;
    *(dim*)&heap->max_rank = maxRank(max_size);

    return heap;
}

void heapReset(mclh *h){
    h->n_inserted = 0;
    if(h->root){
        h->root->left->right = NULL;

        for(hpi *i = h->root, *n; i; i = n){
            n = i->right;
            hpiFree(&i);
        }

        h->root = NULL;
    }
}

void heapFree(mclh **h){
    if(*h){
        if((*h)->root){
            heapReset(*h);
        }

        mclFree(*h);
        *h = NULL;
    }
}

void heapInsert(mclh *h, void *elem){

    if(!h->root){
        h->root = hpiNew(elem, NULL);
        //logDebug("heap remove [%p]",h->root);
        h->n_inserted = 1;
        return;
    }

    if(h->max_size == h->n_inserted){
        //logDebug("pre check items");
        if(h->cmp(elem,h->root->data) <= 0){
            return;
        }

        heapRemove(h);
    }

    hpi *item = hpiNew(elem, h->root);
    h->n_inserted++;
    //logDebug("heap insert [%p]",item);
    //logDebug("after check items");
    if(hpiCmp(h, item, h->root) < 0){
        h->root = item;
    }
}

static void consolidate(mclh* h){

    //logDebug("consolidate %u items",h->n_inserted);
    hpi *arr[h->max_rank];

    for(int i = h->max_rank; i>0;)
        arr[--i] = NULL;

    hpi *s = h->root;
    hpi *w = h->root;

    do {
        hpi *x = w;
        hpi *nextW = w->right;
        int d = x->degree;

        while(arr[d]){
            hpi *y = arr[d];

            //logDebug("check in while loop");
            if(hpiCmp(h, x, y) > 0){
                hpi *tmp = y;
                y = x;
                x = tmp;
            }

            if(y == s){
                s = s->right;
            }

            if(y == nextW){
                nextW = nextW->right;
            }

            hpiLink(x, y);
            arr[d] = NULL;
            d++;
        }

        arr[d] = x;
        w = nextW;
    } while (w != s);

    h->root = s;

    for(hpi **i = arr, **e = arr + h->max_rank; i != e; ++i){
        //logDebug("check in for loop");
        if(*i && hpiCmp(h, *i, h->root) < 0){
            h->root = *i;
        }
    }
}

void *heapRemove(mclh *h){
    hpi *z = h->root;
    if(!z){
        logWarn("trying to get item from empty heap");
        return NULL;
    }

    //logDebug("heap remove [%p]",z);

    if(z->child){

        hpi *min_left = h->root->left;
        hpi *z_child_left = z->child->left;
        h->root->left = z_child_left;
        z_child_left->right = h->root;
        z->child->left = min_left;
        min_left->right = z->child;
    }

    h->n_inserted--;

    if(z == z->right){
        h->root = NULL;
    } else {
        z->left->right = z->right;
        z->right->left = z->left;
        h->root = z->right;
        consolidate(h);
    }

    void *data = z->data;
    mclFree(z);

    return data;
}

static char *hpiDump(char * dst, const hpi *node, size_t elem_size){

    dst = memcpy(dst, node->data, elem_size) + elem_size;

    if(!node->child)
        return dst;

    dst = hpiDump(dst, node->child, elem_size);

    for(hpi *i = node->child->right; i != node->child; i = i->right){
        dst = hpiDump(dst, i, elem_size);
    }

    return dst;
}

void heapDump(const mclh *h, void *dst, size_t elem_size){
    if(!h->root)
        return;

    char *it = hpiDump(dst, h->root, elem_size);

    for(hpi *i = h->root->right; i != h->root; i = i->right){
        it = hpiDump(it, i, elem_size);
    }
}

static void hpiPrint(hpi *h, dim l){
    if(!h){
        printf("heap: %u -> hpi[ NULL WTF! ]\n",l);
        return;
    }

    printf("heap: %u -> hpi[%p -> d:%i, l:%p, r:%p, c:%p | d:%p]\n",l,h,h->degree, h->left,h->right,h->child,h->data);

    if(h->child){
        l++;
        hpi *i = h->child;
        hpi *t = h->child;
        do {
            hpiPrint(i,l);
            i = i->right;
        }while(i != t);
    }

}

void heapPrint(mclh *h){
    if(!h->root){
        puts("heap: (empty heap)");
        return;
    }

    hpi *i = h->root;
    hpi *t = h->root;
    do {
        hpiPrint(i,0);
        i = i->right;
    }while(i != t);
}

