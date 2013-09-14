from distutils.core import setup

setup(
	name = "dCAMP",
	description = "Distributed Common API for Measuring Performance",
	version = "0.1",

	author = "Alexander Sideropoulos",
	author_email = "alexander@thequery.net",
	url = "https://bitbucket.org/asiderop/dcamp/",

	package_dir = { '' : 'src' },
	packages = [
		'dcamp',
		'dcamp.role',
		'dcamp.service',
		'dcamp.types',
		'dcamp.types.messages',
		'dcamp.util',
		],
	
	)
