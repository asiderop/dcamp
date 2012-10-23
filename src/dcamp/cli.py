#!/usr/bin/env python3
'''
@author: Alexander
'''
import logging
from argparse import ArgumentParser, ArgumentTypeError, FileType

from dcamp.app import App
from dcamp.config import DCParsingError, DCConfig

def main():
	# setup CLI parser and parse arguments
	parser = ArgumentParser(prog='dcamp', description='the %(prog)s cli')
	parser.add_argument('--version', action='version', version='%(prog)s 0.1', help='show %(prog)s version and exit')

	# logging output arguments
	parser_logging = ArgumentParser(add_help=False)
	parser_logging.add_argument("-v", "--verbose",
			dest="verbose",
			help="make %(prog)s verbose",
			action="store_true")
	parser_logging.add_argument("-d", "--debug",
			dest="debug",
			help="make %(prog)s uber verbose",
			action="store_true")

	# configuration file
	parser_config = ArgumentParser(add_help=False)
	parser_config.add_argument("-f", "--file",
			dest="configfile",
			help="use FILE as configuration",
			type=FileType('r'),
			metavar="FILE",
			required=True)

	subparsers = parser.add_subparsers(title='dcamp commands')

	# commands
	parser_root = subparsers.add_parser('root',
			parents=[parser_config, parser_logging],
			help='run root command')
	parser_root.set_defaults(func=do_app, cmd='root')

	parser_base = subparsers.add_parser('base',
			parents=[parser_logging],
			help='run base command')
	parser_base.add_argument('-p', '--port',
			dest='port',
			type=int,
			required=True)
	parser_base.set_defaults(func=do_app, cmd='base')

	parser_validate = subparsers.add_parser('validate',
			parents=[parser_config, parser_logging],
			help='validate the given %(prog)s config file')
	parser_validate.set_defaults(func=do_validate)

	parser.set_defaults(verbose=False, debug=False)

	args = parser.parse_args()

	logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
	logger = logging.getLogger('dcamp')
	if (args.verbose):
		logger.setLevel(logging.INFO)
		logger.debug('set logging level to verbose')
	elif (args.debug):
		logger.setLevel(logging.DEBUG)
		logger.debug('set logging level to debug')

	exit(args.func(args))

def do_app(args):
	dapp = App(args)
	return dapp.run()

def do_validate(args):
	import sys

	config = DCConfig()
	try:
		config.validate(args.configfile)
	except DCParsingError as e:
		print('\n', e, '\n')
		return -1

	return 0

if __name__ == '__main__':
	main()
