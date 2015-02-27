#ifndef exception_h
#define exception_h

#include <jni.h>
#include "logger.h"

jint throwOutOfMemoryError( JNIEnv *env, char *message )
{
    jclass exClass;
    char *className = "java/lang/OutOfMemoryError" ;

    exClass = (*env)->FindClass( env, className );
    if ( exClass == NULL ) {
        logErr("could not find exception class %s",className);
        return -1;
    }

    return (*env)->ThrowNew( env, exClass, message );
}

#endif