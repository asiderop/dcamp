#!/usr/bin/env python3

from sys import argv, path
from os.path import dirname, join

BASEDIR = dirname(argv[0])
SRCDIR = join(BASEDIR, '../src')

path.insert(0, SRCDIR)
