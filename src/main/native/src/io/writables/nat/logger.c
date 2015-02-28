#include <time.h>
#include <stdio.h>
#include <stdarg.h>
#include <stdlib.h>
#include "logger.h"

#define LINE_END "\n"

static inline void header(FILE *__stream, const char* level){
    // 15/02/25 23:50:25 INFO:
    time_t timer;
    char buffer[32];
    struct tm* tm_info;

    time(&timer);
    tm_info = localtime(&timer);

    strftime(buffer, 32, "%y/%m/%d %H:%M:%S", tm_info);

    fprintf(__stream,"%s %5s native: ", buffer, level);
}

static inline void lineEnd(FILE *__stream){
    fprintf(__stream, LINE_END);
}

void logDebug(char const *__format, ...){
    if(!IS_DEBUG) return;

    header(stdout,"DEBUG");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout, __format, arglist);
    va_end(arglist);
    lineEnd(stdout);
}

void logInfo(char const *__format, ...){
    if(!IS_INFO) return;
    header(stdout,"INFO");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout,__format, arglist);
    va_end(arglist);
    lineEnd(stdout);
}

void logErr(char const *__format, ...){
    if(!IS_ERROR) return;
    header(stdout,"ERROR");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout ,__format, arglist);
    va_end(arglist);
}

void logWarn(char const *__format, ...){
    if(!IS_WARN) return;
    header(stdout,"WARN");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout ,__format, arglist);
    va_end(arglist);
    lineEnd(stdout);
}

void logTrace(char const *__format, ...){
    if(!IS_TRACE) return;
    header(stdout,"TRACE");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout ,__format, arglist);
    va_end(arglist);
    lineEnd(stdout);
}

void logFatal(char const *__format, ...){
    header(stdout,"FATAL");
    va_list arglist;
    va_start(arglist, __format);
    vfprintf(stdout ,__format, arglist);
    va_end(arglist);
    lineEnd(stdout);
    exit(1);
}