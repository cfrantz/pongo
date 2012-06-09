#ifndef PONGO_LOG_H
#define PONGO_LOG_H

extern void log_init(const char *logfile, int _level);
extern void log_emit(int _level, const char *file, int line, const char *msg, ...);
extern void log_bare(const char *msg, ...);

#define LOG_ERROR 10
#define LOG_CRITICAL 20
#define LOG_WARNING 30
#define LOG_INFO 40
#define LOG_VERBOSE 50
#define LOG_DEBUG 60
#define LOG_MORON 100

#ifndef WIN32
#define log_error(args...)    log_emit(LOG_ERROR, __FILE__, __LINE__, ## args)
#define log_critical(args...) log_emit(LOG_CRITICAL, __FILE__, __LINE__, ## args)
#define log_warning(args...)  log_emit(LOG_WARNING, __FILE__, __LINE__, ## args)
#define log_info(args...)     log_emit(LOG_INFO, __FILE__, __LINE__, ## args)
#define log_verbose(args...)  log_emit(LOG_VERBOSE, __FILE__, __LINE__, ## args)
#define log_debug(args...)    log_emit(LOG_DEBUG, __FILE__, __LINE__, ## args)
#define log_moron(args...)    log_emit(LOG_MORON, __FILE__, __LINE__, ## args)
#else
#define log_error(...)    log_emit(LOG_ERROR, __FILE__, __LINE__, __VA_ARGS__)
#define log_critical(...) log_emit(LOG_CRITICAL, __FILE__, __LINE__, __VA_ARGS__)
#define log_warning(...)  log_emit(LOG_WARNING, __FILE__, __LINE__, __VA_ARGS__)
#define log_info(...)     log_emit(LOG_INFO, __FILE__, __LINE__, __VA_ARGS__)
#define log_verbose(...)  log_emit(LOG_VERBOSE, __FILE__, __LINE__, __VA_ARGS__)
#define log_debug(...)    log_emit(LOG_DEBUG, __FILE__, __LINE__, __VA_ARGS__)
#define log_moron(...)    log_emit(LOG_MORON, __FILE__, __LINE__, __VA_ARGS__)
#endif

#endif
