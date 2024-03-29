#ifndef PONGO_MISC_H
#define PONGO_MISC_H

#include <time.h>
#ifndef WIN32
#define _GNU_SOURCE
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>
#else
#include <windows.h>
#endif
#include <pongo/stdtypes.h>

static inline uint32_t ror32(uint32_t x)
{
	return (x>>1) | (x<<31);
}

// Pow2, population count and number of trailing zeros, from Hackers Delight
static inline unsigned pow2(unsigned v)
{
	v--;
	v |= v>>1;
	v |= v>>2;
	v |= v>>4;
	v |= v>>8;
	v |= v>>16;
	v++;
	return v;
}

static inline int pop32(uint32_t x) {
   x = x - ((x >> 1) & 0x55555555);
   x = (x & 0x33333333) + ((x >> 2) & 0x33333333);
   x = (x + (x >> 4)) & 0x0F0F0F0F;
   x = x + (x << 8);
   x = x + (x << 16);
   return x >> 24;
}

static inline int ntz(uint32_t x) {
   return pop32(~x & (x - 1));
}

static inline uint32_t hash_python(const uint8_t *str)
{
	uint8_t ch;
	uint32_t len = 0, hash = 0;

	while((ch = *str++) != 0) {
		hash = (1000003*hash) ^ ch;
		len++;
	}
	hash ^= len;
	if (hash == 0xFFFFFFFF)
		hash = 0xFFFFFFFE;
	return hash;
}

static inline uint32_t hash_x31(const uint8_t *str, int len)
{
	uint8_t ch;
	uint32_t hash = 0;

	while(len--) {
		ch = *str++;
		hash = (hash*31) + ch;
	}
	if (hash == 0xFFFFFFFF)
		hash = 0xFFFFFFFE;
	return hash;
}

extern int64_t utime_now(void);
extern time_t mktimegm(struct tm *tm);
extern int is_prime(uint32_t n);

#ifdef WIN32
extern int getpid(void);
#endif
#endif
