#include <jni.h>

#ifndef stats_h
#define stats_h

typedef struct {
    jdouble chaos;
    jint kmax;
    jint prune;
    jint cutoff;
    jlong attractors;
    jlong homogen;
} mclStats;

mclStats *statsInit(JNIEnv *env, jobject jstats);

void statsDump(mclStats *stats, JNIEnv *env, jobject jstats);

#endif