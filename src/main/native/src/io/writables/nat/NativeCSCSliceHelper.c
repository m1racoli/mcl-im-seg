#include <jni.h>
#include <stdio.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"
#include "slice.h"

static int _nsub;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setNsub
        (JNIEnv *env, jclass cls, jint nsub) {
    _nsub = nsub;
}

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_clear(JNIEnv *env, jclass cls, jobject buf) {
    jint *colIdx = colIdxFromByteBuffer(env, buf);
    int i;

    for(i = _nsub; i >= 0; --i) {
        *(colIdx++) = 0;
    }

    return;
}

JNIEXPORT jboolean JNICALL Java_io_writables_nat_NativeCSCSliceHelper_equals
        (JNIEnv *env, jclass cls, jobject b1, jobject b2){
    //TODO
}