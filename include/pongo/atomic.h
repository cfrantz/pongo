#ifndef PONGO_ATOMIC_H
#define PONGO_ATOMIC_H
#include <pongo/stdtypes.h>

static inline int cmpxchg32(volatile void *ptr, uint32_t oldval, uint32_t newval)
{
    volatile uint32_t *p = (volatile uint32_t*)ptr;
#ifdef WIN32
    return _InterlockedCompareExchange32(p, newval, oldval) == oldval;
#else
    return __sync_bool_compare_and_swap(p, oldval, newval);
#endif
}

static inline int cmpxchg64(volatile void *ptr, uint64_t oldval, uint64_t newval)
{
    volatile uint64_t *p = (volatile uint64_t*)ptr;
#ifdef WIN32
    return _InterlockedCompareExchange64(p, newval, oldval) == oldval;
#else
    return __sync_bool_compare_and_swap(p, oldval, newval);
#endif
}

static inline uint32_t
atomic_add(volatile uint32_t *a, uint32_t addval)
{
    return __sync_add_and_fetch(a, addval);
}

static inline uint32_t
atomic_subtract(volatile uint32_t *a, uint32_t subval)
{
    return __sync_sub_and_fetch(a, subval);
}

static inline uint32_t
atomic_inc(volatile uint32_t *a)
{
    return atomic_add(a, 1);
}

static inline uint32_t
atomic_dec(volatile uint32_t *a)
{
    return atomic_subtract(a, 1);
}

#endif
