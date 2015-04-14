#include <inttypes.h>
#include "stats.h"
#include "alloc.h"
#include "logger.h"
#include "exception.h"

static jclass cls = NULL;
static jfieldID CHAOS_ID;
static jfieldID KMAX_ID;
static jfieldID PRUNE_ID;
static jfieldID CUTOFF_ID;
static jfieldID ATTRACTORS_ID;
static jfieldID HOMOGEN_ID;

/*  Fields of MCLStats:
    public double maxChaos = 0.0;
	public int kmax = 0;
	public int prune = 0;
	public int cutoff = 0;
	public long attractors = 0;
	public long homogen = 0;
 */

mclStats *statsInit(JNIEnv *env, jobject jstats) {
    mclStats *stats = mclAlloc(sizeof(mclStats));

    if(!cls){
        jclass local_cls;
        local_cls = (*env)->FindClass(env,"mapred/MCLStats");

        if((*env)->ExceptionCheck(env)){
            (*env)->ExceptionDescribe(env);
            (*env)->FatalError(env, "could not find class mapred/MCLStats");
        }

        cls = (jclass) (*env)->NewGlobalRef(env, local_cls);
        (*env)->DeleteLocalRef(env, local_cls);

        CHAOS_ID = (*env)->GetFieldID(env,cls,"chaos","D");
        KMAX_ID = (*env)->GetFieldID(env,cls,"kmax","I");
        PRUNE_ID = (*env)->GetFieldID(env,cls,"prune","J");
        CUTOFF_ID = (*env)->GetFieldID(env,cls,"cutoff","J");
        ATTRACTORS_ID = (*env)->GetFieldID(env,cls,"attractors","J");
        HOMOGEN_ID = (*env)->GetFieldID(env,cls,"homogen","J");
    }

    stats->chaos = (double)(*env)->GetDoubleField(env,jstats,CHAOS_ID);
    stats->kmax = (*env)->GetIntField(env,jstats,KMAX_ID);
    stats->prune = (*env)->GetLongField(env,jstats,PRUNE_ID);
    stats->cutoff = (*env)->GetLongField(env,jstats,CUTOFF_ID);
    stats->attractors = (*env)->GetLongField(env,jstats,ATTRACTORS_ID);
    stats->homogen = (*env)->GetLongField(env,jstats,HOMOGEN_ID);

    checkException(env, true);

    return stats;
}

void statsDump(mclStats *stats, JNIEnv *env, jobject jstats) {

    if(IS_TRACE){
        logTrace("statsDump: chaos:%f, kmax:%lu, prune:%" PRId64 ", cutoff:%" PRId64 ", attractors:%" PRId64 ", homogen:%"PRId64);
    }

    (*env)->SetDoubleField(env,jstats,CHAOS_ID,(jdouble) stats->chaos);
    (*env)->SetIntField(env,jstats,KMAX_ID,stats->kmax);
    (*env)->SetLongField(env,jstats,PRUNE_ID,stats->prune);
    (*env)->SetLongField(env,jstats,CUTOFF_ID,stats->cutoff);
    (*env)->SetLongField(env,jstats,ATTRACTORS_ID,stats->attractors);
    (*env)->SetLongField(env,jstats,HOMOGEN_ID,stats->homogen);

    if((*env)->ExceptionCheck){
        (*env)->ExceptionDescribe(env);
    }

    mclFree(stats);
}