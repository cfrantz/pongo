#ifndef PONGO_ATOMIC_H
#define PONGO_ATOMIC_H
#include <pongo/stdtypes.h>

static inline int cmpxchg(uint64_t *ptr, uint64_t oldval, uint64_t newval)
{
#ifdef WIN32
    return _InterlockedCompareExchange64(ptr, newval, oldval) == oldval;
#else
    return __sync_bool_compare_and_swap(ptr, oldval, newval);
#endif
}


#endif
