#include "heapitem.h"
#include "alloc.h"

hpi *hpiNew(void *data, hpi *neighbor){
    hpi *i = mclAlloc(sizeof(hpi));

    i->data = data;
    i->child = NULL;
    i->parent = NULL;
    i->degree = 0;

    if(neighbor){
        hpiInsertSibling(neighbor, i);
    } else {
        i->left = i;
        i->right =  i;
    }

    return i;
}

void hpiInsertSibling(hpi* root, hpi* item){
    item->right = root;
    item->left = root->left;
    root->left = item;
    item->left->right = item;
}

void hpiLink(hpi *parent, hpi *node){
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

void hpiFree(hpi **node){
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