#include "slice.h"

jint *colIdxFromByteBuffer(JNIEnv *env, jobject buf) {
    void* dBuf = (*env)->GetDirectBufferAddress(env,buf);
    return (jint *) dBuf;
}

mclSlice *initSlice(mclSlice *slice, JNIEnv *env, jobject buf, int nsub) {
    //TODO malloc
    slice->colPtr = colIdxFromByteBuffer(env, buf);
    slice->colVal = (ivp*) slice->colPtr + nsub + 1;
    return slice;
}