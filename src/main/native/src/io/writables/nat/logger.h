#ifndef logger_h
#define logger_h

#ifdef TRACE
#define IS_TRACE 1
#define DEBUG
#else
#define IS_TRACE 0
#endif

#ifdef DEBUG
#define IS_DEBUG 1
#define INFO
#else
#define IS_DEBUG 0
#endif

#ifdef INFO
#define IS_INFO 1
#define WARN
#else
#define IS_INFO 0
#endif

#ifdef WARN
#define IS_WARN 1
#define ERROR
#else
#define IS_WARN 0
#endif

#ifdef ERROR
#define IS_ERROR 1
#else
#define IS_ERROR 0
#endif

/*
TODO logging levels
TRACE
DEBUG
INFO
WARN
ERROR
 */

void logDebug(char const *__format, ...);

void logErr(char const *__format, ...);

void logInfo(char const *__format, ...);

void logWarn(char const *__format, ...);

void logTrace(char const *__format, ...);

void logFatal(char const *__format, ...);

#endif