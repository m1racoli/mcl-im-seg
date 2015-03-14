#include <jni.h>

#ifndef stats_h
#define stats_h

typedef struct {
    double chaos;
    jint kmax;
    jlong prune;
    jlong cutoff;
    jlong attractors;
    jlong homogen;
} mclStats;

mclStats *statsInit(JNIEnv *env, jobject jstats);

void statsDump(mclStats *stats, JNIEnv *env, jobject jstats);

#endif