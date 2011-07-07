from ez_setup import use_setuptools
use_setuptools(version='0.6c9')
		
from setuptools import setup, find_packages
setup(
	name = "dCAMP",
	version = "0.1",

	packages = find_packages('src'),
	package_dir = {'':'src'},

	entry_points = {
		'console_scripts': [
			'dcamp = dcamp.cli:main'
			]
	},
	install_requires = [
		'camp >= 1.1',
		'rpyc >= 3.0.0',
		],
)

