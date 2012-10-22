#!/usr/bin/env python3
'''
@author: Alexander
'''
from argparse import ArgumentParser, ArgumentTypeError
import dcamp.app

def endpoint(string):
	errmsg = None
	parts = string.split(':')
	if len(parts) != 2:
		errmsg = 'endpoint must be "host:port"'
	elif len(parts[0]) == 0:
		errmsg = 'must provide host name or address'
	elif not parts[1].isdecimal():
		errmsg = 'port must be an integer'

	if errmsg:
		raise ArgumentTypeError(errmsg)

	return (parts[0], int(parts[1]))

def main():
	# setup CLI parser and parse arguments
	#parser = ArgumentParser(usage="usage: %prog [options]", version="%prog 0.1")
	parser = ArgumentParser(prog='dcamp', description='the dcamp app')
	parser.add_argument('--version', action='version', version='%(prog)s 0.1')

	parser.add_argument("-c", "--command",
			dest="command",
			help="run COMMAND")
	parser.add_argument("-i", "--input",
			dest="input",
			help="use FILE as configuration",
			metavar="FILE")

	parser.add_argument("-d", "--debug",
			dest="debug",
			help="make dCAMP uber verbose",
			action="store_true")
	parser.add_argument("-v", "--verbose",
			dest="verbose",
			help="make dCAMP verbose",
			action="store_true")

	parser.add_argument('-r', '--root',
			dest="root",
			type=endpoint)
	parser.add_argument('-b', '--base',
			dest="bases",
			action='append',
			type=endpoint)

	parser.set_defaults(verbose=False)
	args = parser.parse_args()

	dcamp.app.App(args)

if __name__ == '__main__':
	main()
