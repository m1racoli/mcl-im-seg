#include "jni.h"

typedef long pnum;
typedef float pval;

typedef struct {
    pnum idx;
    pval val;
} ivp; // index value pair

typedef struct {
    jint* colPtr;
    ivp* colVal;
} mclSlice;

jint* colIdxFromByteBuffer(JNIEnv *env, jobject buf);

mclSlice * initSlice(mclSlice *slice, JNIEnv *env, jobject buf, int nsub);
