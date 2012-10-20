#!/usr/bin/env python3
'''
@author: Alexander
'''
from argparse import ArgumentParser
import dcamp.app

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
			dest="root_port",
			type=int)
	parser.add_argument('-b', '--base',
			dest="base_port",
			type=int)

	parser.set_defaults(verbose=False)
	args = parser.parse_args()

	dcamp.app.App(args)

if __name__ == '__main__':
	main()
