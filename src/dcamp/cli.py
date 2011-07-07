'''
@author: Alexander
'''
from optparse import OptionParser
import sys
import dcamp.app

def main():
	# setup CLI parser and parse arguments
	parser = OptionParser(usage="usage: %prog [options]", version="%prog 0.1")
	parser.add_option("-c", "--command",
			dest="command",
			help="run COMMAND",
			metavar="COMMAND")
	parser.add_option("-i", "--input",
			dest="input",
			help="use FILE as configuration",
			metavar="FILE")
	parser.add_option("-v", "--verbose",
			dest="verbose",
			help="make dCAMP verbose",
			action="store_true")
	
	parser.set_defaults(input="dcamp.cfg", verbose=False)
	(options, args) = parser.parse_args()
	
	dcamp.app.App(options)
	
	sys.exit(0)
	
if __name__ == '__main__':
	main()
