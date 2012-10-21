import logging

from dcamp.service.management import Management

class Root(Role):
	'''
	Root Role
	'''

	def __init__(self,
			port=None,
			nodes=None,
			subnets=None):
		super().__init__()

		self.ctx = zmq.Context.instance()
		self.services = [
				Management(self.ctx, port, nodes, subnets)
		]
