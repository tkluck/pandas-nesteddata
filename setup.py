#!/usr/bin/env python

from distutils.core import setup

setup(name='pandas-nesteddata',
      version='0.1',
      description='Transform hierarchical data (nested arrays/hashes) to a pandas DataFrame according to a compact, readable, user-specified pattern ',
      author='Timo Kluck',
      author_email='tkluck@infty.nl',
      url='https://www.python.org/sigs/distutils-sig/',
      packages=['pandas.nesteddata'],
     )
