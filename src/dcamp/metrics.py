'''
Created on Aug 21, 2009

@author: Alexander
'''
import camp.app

class Metrics():
	'''
	This is the main class for sampling metrics
	'''

	def __init__(self):
		'''
		Constructor
		'''
		pass
		
	#-------------------------------------------------------------------- global
	def GetPercentProcessorTime(self):
		return camp.app.GetPercentProcessorTime()
	def GetFreePhysicalMemory(self):
		return camp.app.GetFreePhysicalMemory()

	#------------------------------------------------------------------- network
	def GetNetBytesSent(self):
		return self.__GetSumMetric(
				camp.app.EnumNetworkInterfaces,
				camp.app.GetNetBytesSent)
	def GetNetPacketsSent(self):
		return self.__GetSumMetric(
				camp.app.EnumNetworkInterfaces,
				camp.app.GetNetPacketsSent)
	def GetNetBytesRecvd(self):
		return self.__GetSumMetric(
				camp.app.EnumNetworkInterfaces,
				camp.app.GetNetBytesRecvd)
	def GetNetPacketsRecvd(self):
		return self.__GetSumMetric(
				camp.app.EnumNetworkInterfaces,
				camp.app.GetNetPacketsRecvd)

	#---------------------------------------------------------------------- disk
	def GetNumReads_Disk(self):
		return self.__GetSumMetric(
				camp.app.EnumPhysicalDisks,
				camp.app.GetNumReads_Disk)
	def GetNumWrites_Disk(self):
		return self.__GetSumMetric(
				camp.app.EnumPhysicalDisks,
				camp.app.GetNumWrites_Disk)
	def GetNumReads_Partition(self):
		return self.__GetSumMetric(
				camp.app.EnumDiskPartitions,
				camp.app.GetNumReads_Partition)
	def GetNumWrites_Partition(self):
		return self.__GetSumMetric(
				camp.app.EnumDiskPartitions,
				camp.app.GetNumWrites_Partition)

	#-------------------------------------------------- private helper functions
	def __GetAvgMetric(self, enumerator, sampler):
		sum = 0;
		items = enumerator()
		for item in items:
			sum += sampler(item)
		return sum/len(items)

	def __GetSumMetric(self, enumerator, sampler):
		sum = 0;
		for item in enumerator():
			sum += sampler(item)
		return sum