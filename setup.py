from distutils.core import setup, Extension
import sys

sources = [
    'python/pongo.c',
    'python/pongo_list.c',
    'python/pongo_dict.c',
    'python/pongo_coll.c',
    'python/pongo_ptr.c',
    'python/pongo_iter.c',
]
coverage = []
#coverage = ['-fprofile-arcs', '-ftest-coverage']

if sys.platform == 'win32':
    native = Extension("_pongo",
            sources=sources,
            extra_objects = ['lib/pongo.lib', 'yajl/yajl.lib', 'kernel32.lib'],
            include_dirs = ['include'],
            extra_compile_args=['-DWIN32=1', '/Zi'],
            extra_link_args=['/pdb:_pongo.pdb'])
else:
    native = Extension("_pongo",
            sources=sources,
            extra_objects = ['lib/libpongo.a', 'yajl/libyajl.a', '-luuid', '-lrt'],
            include_dirs = ['include'],
            extra_compile_args=['-fms-extensions', '-g3', '-DWANT_UUID_TYPE']+coverage,
#            extra_link_args=['--coverage'],

            )

setup(
	name="pongo",
    version="0.0.1",
    description="PongoDB",
    #package_dir={'pongo': 'python'},
    #packages=['pongo'],
    ext_modules=[native])

# vim: ts=4 sts=4 sw=4 expandtab:
