#!/usr/bin/env python3

from json import JSONEncoder

def seconds_to_str(seconds):
	'''
	Method takes given number of seconds and determines how to best (concisely) represent
	it in a string.

	    >>> seconds_to_str(90)
	    '90s'

	@see str_to_seconds() does the exact opposite of this
	'''
	return '%ds' % seconds

def str_to_seconds(string):
	'''
	Method determines how time in given string is specified and returns an int value in
	seconds.

	    >>> str_to_seconds('90s')
	    90

	@see seconds_to_str() does the exact opposite of this

	valid time units:
		s -- seconds

	@todo add this to validation routine / issue #23
	'''
	valid_units = ['s']
	if string.endswith('s'):
		return int(string[:len(string)-1])
	else:
		raise NotImplementedError('invalid time unit given--valid units: %s' % valid_units)
