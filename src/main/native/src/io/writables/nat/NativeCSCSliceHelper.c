#include <jni.h>
#include <stdio.h>
#include "io_writables_nat_NativeCSCSliceHelper.h"

static int _nsub;

JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_setNsub
        (JNIEnv *env, jclass cls, jint nsub) {
    _nsub = nsub;
}

// Implementation of native method clear(ByteBuffer bb, int nsub) of NativeCSCSliceHelper class
JNIEXPORT void JNICALL Java_io_writables_nat_NativeCSCSliceHelper_clear(JNIEnv *env, jclass cls, jobject buf) {
    jbyte *dBuf = (*env)->GetDirectBufferAddress(env, buf);
    jint *colIdx = (jint *) dBuf;
    int i = 0;

    for(i = _nsub; i >= 0; --i){
        *(colIdx++) = 0;
    }

    return;
}