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

static inline uint32_t
atomic_add(uint32_t *a, uint32_t addval)
{
    return __sync_add_and_fetch(a, addval);
}

static inline uint32_t
atomic_subtract(uint32_t *a, uint32_t subval)
{
    return __sync_sub_and_fetch(a, subval);
}

static inline uint32_t
atomic_inc(uint32_t *a)
{
    return atomic_add(a, 1);
}

static inline uint32_t
atomic_dec(uint32_t *a)
{
    return atomic_subtract(a, 1);
}

#endif
