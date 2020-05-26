from distutils.core import setup, Extension
setup(name = 'hello', version = '1.0',  \
   ext_modules = [Extension('hello', ['test.c'])])
