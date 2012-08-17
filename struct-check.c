#include <stdio.h>
#include <stdlib.h>
#include <pongo/dbtypes.h>

#define offsetof(s, e) ((long)(&((s*)0)->e))

#define check(t, n) do {					\
	int x = offsetof(t, n);					\
	int y = offsetof(dbtype_t, n);				\
	printf("%16s: %6s %2d %2d", #t, #n, x, y);		\
	if (x != y) { printf(" error"); error = 1; }		\
	printf("\n"); \
	} while(0)

int
main()
{
	int error = 0;
	check(dbboolean_t, type);
	check(dbboolean_t, _pad);
	check(dbboolean_t, bval);
	
	check(dbint_t, type);
	check(dbint_t, _pad);
	check(dbint_t, ival);

	check(dbtime_t, type);
	check(dbtime_t, _pad);
	check(dbtime_t, utctime);

	check(dbfloat_t, type);
	check(dbfloat_t, _pad);
	check(dbfloat_t, fval);

	check(dbstring_t, type);
	check(dbstring_t, len);
	check(dbstring_t, hash);
	check(dbstring_t, sval);

	check(dbuuid_t, type);
	check(dbuuid_t, _uupad);
	check(dbuuid_t, uuval);

	check(dblist_t, type);
	check(dblist_t, _pad);
	check(dblist_t, list);

	check(dbobject_t, type);
	check(dbobject_t, _pad);
	check(dbobject_t, obj);

	check(dbcollection_t, type);
	check(dbcollection_t, refcnt);
	check(dbcollection_t, obj);

	check(dbcache_t, type);
	check(dbcache_t, _pad);
	check(dbcache_t, cache);

	check(dbnode_t, type);
	check(dbnode_t, _pad);
	check(dbnode_t, left);
	check(dbnode_t, right);
	check(dbnode_t, key);
	check(dbnode_t, value);
	check(dbnode_t, size);

	return error;
}
