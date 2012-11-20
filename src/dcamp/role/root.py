import logging, zmq

from dcamp.role.role import Role
from dcamp.service.management import Management

class Root(Role):
	'''
	Root Role
	'''

	def __init__(self,
			context,
			pipe,
			config):
		super().__init__(context, pipe)

		self.services = [
				Management(self.ctx, config)
		]
