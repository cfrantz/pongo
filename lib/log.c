#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <time.h>
#include <pongo/log.h>

static FILE *logf;
static int level;


void log_init(const char *logfile, int lvl)
{
	level = lvl;
	if (logfile) {
		logf = fopen(logfile, "a");
		if (!logf) {
			perror("opening logfile");
			abort();
		}
	} else {
		logf = stderr;
	}
}

void log_emit(int lvl, const char *file, int line, const char *msg, ...)
{
	time_t now;
	char tm[32];
	va_list ap;
	if (lvl <= level) {
		time(&now);
		strftime(tm, sizeof(tm), "%Y/%m/%d-%H:%M:%S", localtime(&now));
		fprintf(logf, "%s:%s:%d:", tm, file, line);
		va_start(ap, msg);
		vfprintf(logf, msg, ap);
		fprintf(logf, "\n");
		va_end(ap);
	}
}

void log_bare(const char *msg, ...)
{
	va_list ap;
	va_start(ap, msg);
	vfprintf(logf, msg, ap);
	fprintf(logf, "\n");
	va_end(ap);
}
