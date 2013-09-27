'''
dCAMP Utility Functions module
'''

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

def plural(count, ending='s', word=None):
	return (word or '') + (count == 1.0 and '' or ending)

def bytes_to_str(given, use_short=True):
	'''
	Converts given number of bytes (int) into a human-friendly string.
	'''
	long_units  = [ '', 'Kilo', 'Mega', 'Giga', 'Tera', 'Peta'] # with 'byte' suffix
	short_units = ['B',   'Kb',   'Mb',   'Gb',   'Tb',   'Pb']

	num_orders = 0
	while round(given, 2) >= 1024.0:
		num_orders += 1
		given /= 1024.0

	result = '{:.2f} '.format(given)
	if use_short:
		result += short_units[num_orders]
	else:
		result += long_units[num_orders] + plural(given, word='byte')
	return result
