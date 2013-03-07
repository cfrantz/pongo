#include <math.h>
#include <pongo/misc.h>

#ifndef WIN32
int64_t utime_now(void)
{
    struct timespec tm;
    int64_t now;

    clock_gettime(CLOCK_REALTIME, &tm);
    now = tm.tv_sec * 1000000LL + tm.tv_nsec/1000;
    return now;
}

int gettid(void)
{
    int tid;
    tid = syscall(SYS_gettid);
    return tid;
}
#else
int getpid(void)
{
    return (int)GetCurrentProcessId();
}

int gettid(void)
{
    return (int)GetCurrentThreadId();
}

int64_t utime_now(void)
{
    int64_t now;
    FILETIME ft;

    GetSystemTimeAsFileTime(&ft);
    now = ft.dwHighDateTime;
    now <<= 32;
    now |= ft.dwLowDateTime;
    return now;
}
#endif
/*
 * Cool version of mktime from QEMU.
 *
 * Computes number of seconds since 1/1/1970 UTC.
 */
time_t mktimegm(struct tm *tm)
{
    time_t t;
    int y = tm->tm_year + 1900, m = tm->tm_mon + 1, d = tm->tm_mday;
    if (m < 3) {
         m += 12;
         y--;
    }
    t = 86400 * (d + (153*m-457) / 5 + 365*y + y/4 - y/100 +
                 y/400 - 719469);
    t += 3600 * tm->tm_hour + 60 * tm->tm_min + tm->tm_sec;
    return t;
}

/*
 * Naive prime test for an integer.
 *
 * Checks if it has a factor of 2 or 3.  Then checks
 * factors of 6k+1/6k-1 up until sqrt(n)
 */
int is_prime(uint32_t n)
{
	uint32_t r, k, p;
    if (n<11 && (n==2 || n==3 || n==5 || n==7))
        return 1;

	if (n%2==0 || n%3==0)
		return 0;

	r = sqrt(n)+1.0;
	k = 1;
	do {
		p = 6*k;
		if (n%(p-1) == 0) return 0;
		if (n%(p+1) == 0) return 0;
		k++;
	} while(p<r);
	return 1;
}

/*
 * vim: ts=4 sts=4 sw=4 expandtab:
 */
