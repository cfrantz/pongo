#ifndef PONGO_STDTYPES_H
#define PONGO_STDTYPES_H

#ifndef WIN32
#include <inttypes.h>
#else
#include <windows.h>
typedef unsigned char uint8_t;
typedef signed char int8_t;

typedef unsigned short uint16_t;
typedef signed short int16_t;

typedef unsigned int uint32_t;
typedef signed int int32_t;

typedef unsigned long long uint64_t;
typedef signed long long int64_t;

#define inline __inline
#endif

#endif
