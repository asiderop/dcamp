'''
@author: Alexander
'''

from .d_service import CustomService 
from threading import Thread,Timer
from .d_server import CustomServer
import camp.app

class CAMPThread(Thread):
	'''
	Remotely invokable CAMP API
	'''
	
	def __init__(self, config, dApp):
		Thread.__init__(self, name='CAMPService')
		
		self.server = self.CAMPServer(dApp, config,
											service=self.CAMPService,
											port=int(config.camp('port')),
											auto_register=False)
	
	def run(self):
		self.server.start()
	
	def stop(self):
		self.server.close()
	
#----------------------------------------------------------------------------- 
	class CAMPServer(CustomServer):
		pass
	
#----------------------------------------------------------------------------- 
	class CAMPService(CustomService):
		#--------------------------------------------------------- global metrics
		def exposed_GetPercentProcessorTime(self, *params):
			return camp.app.GetPercentProcessorTime(*params)
		
		def exposed_GetFreePhysicalMemory(self, *params):
			return camp.app.GetFreePhysicalMemory(*params)
		
		#-------------------------------------------------------- network metrics
		def exposed_GetNetBytesSent(self, *params):
			return camp.app.GetNetBytesSent(*params)
		
		def exposed_GetNetPacketsSent(self, *params):
			return camp.app.GetNetPacketsSent(*params)
		
		def exposed_GetNetBytesRecvd(self, *params):
			return camp.app.GetNetBytesRecvd(*params)
		
		def exposed_GetNetPacketsRecvd(self, *params):
			return camp.app.GetNetPacketsRecvd(*params)
	
		#---------------------------------------------------- per-process metrics
		def exposed_GetNumPageFaults(self, *params):
			return camp.app.GetNumPageFaults(*params)
		
		def exposed_GetCPUTime_Total(self, *params):
			return camp.app.GetCPUTime_Total(*params)
		
		def exposed_GetCPUTime_User(self, *params):
			return camp.app.GetCPUTime_User(*params)
		
		def exposed_GetCPUTime_Kernel(self, *params):
			return camp.app.GetCPUTime_Kernel(*params)
	
		def exposed_GetWorkingSetKb(self, *params):
			return camp.app.GetWorkingSetKb(*params)
		
		def exposed_GetVMSizeKb(self, *params):
			return camp.app.GetVMSizeKb(*params)
		
		def exposed_GetThreadCount(self, *params):
			return  camp.app.GetThreadCount(*params)
	
		#------------------------------------------------------- disk i/o metrics
		def exposed_GetNumReads_Disk(self, *params):
			return camp.app.GetNumReads_Disk(*params)
		
		def exposed_GetNumWrites_Disk(self, *params):
			return camp.app.GetNumWrites_Disk(*params)
		
		def exposed_GetNumReads_Partition(self, *params):
			return camp.app.GetNumReads_Partition(*params)
		
		def exposed_GetNumWrites_Partition(self, *params): 
			return camp.app.GetNumWrites_Partition(*params)
		
		#-------------------------------------------------------- inquiry metrics
		def exposed_EnumDiskPartitions(self, *params):
			return camp.app.EnumDiskPartitions(*params)
		
		def exposed_EnumPhysicalDisks(self, *params):
			return camp.app.EnumPhysicalDisks(*params)
		
		def exposed_EnumNetworkInterfaces(self, *params):
			return camp.app.EnumNetworkInterfaces(*params)
		
		def exposed_GetCPUCount(self, *params):
			return camp.app.GetCPUCount(*params)
		
		def exposed_GetProcessIdentifier(self, *params):
			return camp.app.GetProcessIdentifier(*params)
		
		def exposed_GetProcessIdentifiers(self, *params):
			return camp.app.GetProcessIdentifiers(*params)