#ifndef exception_h
#define exception_h

#include <jni.h>
#include <stdbool.h>

jint throwOutOfMemoryError( JNIEnv *env, char *message );

void checkException(JNIEnv *env, bool exit_on_fail);

#endif