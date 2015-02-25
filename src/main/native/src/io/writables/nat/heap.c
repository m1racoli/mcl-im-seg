#include <string.h>
#include "heap.h"
#include "alloc.h"

static inline int hpiCmp(mclh *h, const hpi *i1, const hpi *i2){
    return h->cmp(i1->data,i2->data);
}

static hpi *hpiNew(void *data, hpi *neighbor){
    hpi *i = mclAlloc(sizeof(hpi));

    i->data = data;
    i->child = NULL;
    i->parent = NULL;
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
    node->parent = parent;

    if(parent->child == NULL){
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

    mclh *heap = mclAlloc(sizeof(mclh));

    heap->root = NULL;
    *(dim*)&heap->max_size = max_size;
    heap->cmp = cmp;
    heap->n_inserted = 0;

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
        h->n_inserted = 1;
        return;
    }

    hpi *item = hpiNew(elem, h->root);
    h->n_inserted++;

    if(hpiCmp(h, item, h->root) < 0){
        h->root = item;
    }

    if(h->max_size && h->max_size < h->n_inserted){
        heapRemove(h);
    }
}

static void consolidate(mclh* h){
    hpi *arr[45];

    for(int i = 45; i>0;)
        arr[--i] = NULL;

    hpi *s = h->root;
    hpi *w = h->root;

    do {
        hpi *x = w;
        hpi *nextW = w->right;
        int d = x->degree;

        while(arr[d]){
            hpi *y = arr[d];

            if(hpiCmp(h, x, y) > 1){
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

    for(hpi **i = arr, **e = arr + 45; i != e; ++i){
        if(*i && hpiCmp(h, *i, h->root) < 0){
            h->root = *i;
        }
    }
}

void *heapRemove(mclh *h){
    hpi *z = h->root;
    if(!z){
        return NULL;
    }

    if(z->child){
        z->child->parent = NULL;

        for(hpi *i = z->child->right; i != z->child; i = i->right){
            i->parent = NULL;
        }

        hpi *min_left = h->root->left;
        hpi * z_child_left = z->child->left;
        h->root->left = z_child_left;
        z_child_left->right = h->root;
        z->child->left = min_left;
        min_left->right = z->child;
    }

    if(z == z->right){
        h->root = NULL;
    } else {
        z->left->right = z->right;
        z->right->left = z->left;
        h->root = z->right;
        consolidate(h);
    }

    h->n_inserted--;
    void *data = z->data;
    mclFree(z);

    return data;
}

static char *hpiDump(char * dst, const hpi *node, size_t elem_size){

    dst = memcpy(dst, node->data, elem_size) + elem_size;

    if(!node->child)
        return dst;

    dst = hpiDump(dst, node->child, elem_size);

    for(hpi *t = node->child, *i = t->right; i != t; i = i->right){
        dst = hpiDump(dst, i, elem_size);
    }

    return dst;
}

void heapDump(const mclh *h, void *dst, size_t elem_size){
    if(!h->root)
        return;

    char *it = hpiDump(dst, h->root, elem_size);

    for(hpi *t = h->root, *i = t->right; i != t; i = i->right){
        it = hpiDump(it, i, elem_size);
    }
}

