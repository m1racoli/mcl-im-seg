#include "exception.h"
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

void checkException(JNIEnv *env,bool exit_on_fail){
    if((*env)->ExceptionCheck(env)){
        (*env)->ExceptionDescribe(env);
        if(exit_on_fail)(*env)->FatalError(env,"exception ocurred");
    }
}