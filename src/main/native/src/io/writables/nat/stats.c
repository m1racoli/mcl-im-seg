#include "stats.h"
#include "alloc.h"

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
        cls = (*env)->GetObjectClass(env,jstats);
        CHAOS_ID = (*env)->GetFieldID(env,cls,"chaosMax","D");
        KMAX_ID = (*env)->GetFieldID(env,cls,"kmax","I");
        PRUNE_ID = (*env)->GetFieldID(env,cls,"prune","I");
        CUTOFF_ID = (*env)->GetFieldID(env,cls,"cutoff","I");
        ATTRACTORS_ID = (*env)->GetFieldID(env,cls,"attractors","J");
        HOMOGEN_ID = (*env)->GetFieldID(env,cls,"homogen","J");
    }

    stats->chaos = (*env)->GetDoubleField(env,jstats,CHAOS_ID);
    stats->kmax = (*env)->GetIntField(env,jstats,KMAX_ID);
    stats->prune = (*env)->GetIntField(env,jstats,PRUNE_ID);
    stats->cutoff = (*env)->GetIntField(env,jstats,CUTOFF_ID);
    stats->attractors = (*env)->GetLongField(env,jstats,ATTRACTORS_ID);
    stats->homogen = (*env)->GetLongField(env,jstats,HOMOGEN_ID);

    return stats;
}

void statsDump(mclStats *stats, JNIEnv *env, jobject jstats) {
    (*env)->SetDoubleField(env,jstats,CHAOS_ID,stats->chaos);
    (*env)->SetIntField(env,jstats,KMAX_ID,stats->kmax);
    (*env)->SetIntField(env,jstats,PRUNE_ID,stats->prune);
    (*env)->SetIntField(env,jstats,CUTOFF_ID,stats->cutoff);
    (*env)->SetLongField(env,jstats,ATTRACTORS_ID,stats->attractors);
    (*env)->SetLongField(env,jstats,HOMOGEN_ID,stats->homogen);

    mclFree(stats);
}