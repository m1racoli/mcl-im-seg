#include <jni.h>
#include <stdio.h>
#include "io_test_NativeTest.h"

JNIEXPORT void JNICALL Java_io_test_NativeTest_hello(JNIEnv *env, jclass cls, jobject buf) {
   jbyte *dBuf = (*env)->GetDirectBufferAddress(env, buf);
   printf("c int: %i\n",((jint*) dBuf)[0]);
   printf("c double: %f\n",((jdouble*) dBuf)[1]);
   ((jint*) dBuf)[0] = 2;
   ((jdouble*) dBuf)[1] = 4.44;
   return;
}