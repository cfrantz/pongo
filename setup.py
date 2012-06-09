from distutils.core import setup, Extension
import sys

if sys.platform == 'win32':
    native = Extension("_pongo",
            sources = ['python/pythonobj.c'],
            extra_objects = ['lib/pongo.lib', 'yajl/yajl.lib', 'kernel32.lib'],
            include_dirs = ['include'],
            extra_compile_args=['-DWIN32=1', '/Zi'],
            extra_link_args=['/pdb:_pongo.pdb'])
else:
    native = Extension("_pongo",
            sources = ['python/pythonobj.c'],
            extra_objects = ['lib/libpongo.a', 'yajl/libyajl.a', '-luuid', '-lrt'],
            include_dirs = ['include'],
            extra_compile_args=['-fms-extensions'])

setup(
	name="pongo",
    version="0.0.1",
    description="PongoDB",
    ext_modules=[native])

# vim: ts=4 sts=4 sw=4 expandtab:
