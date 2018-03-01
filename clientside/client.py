#!/usr/bin/env python
# -*- coding: utf-8 -*-
import errno
import os
import os.path
import itertools

import rpyc
from fuse import FUSE, FuseOSError, Operations

class RPYCFuse(Operations):
	# +============================
	# | Variables
	# +============================
	server1_status = False
	server2_status = False
	server3_status = False
	# +============================
	# | Helpers
	# +============================
	def __init__(self):
		try:
			self.server1 = rpyc.connect("192.168.56.101", 11025)
			self.server1_status = True
		except Exception, e:
			print("Server1 not available!")
		try:
			self.server2 = rpyc.connect("192.168.56.103", 11025)
			self.server2_status = True
		except Exception, e:
			print("Server2 not available!")
		try:
			self.server3 = rpyc.connect("192.168.56.104", 11025)
			self.server3_status = True
		except Exception, e:
			print("Server3 not available!")

	def try_reconnect(self):
		try:
			if self.server1_status is False:
				self.server1 = rpyc.connect("192.168.56.101", 11025)
				self.server1_status = True
		except Exception, e:
			print("Server1 not available!")
		try:
			if self.server2_status is False:
				self.server2 = rpyc.connect("192.168.56.103", 11025)
				self.server2_status = True
		except Exception, e:
			print("Server2 not available!")
		try:
			if self.server3_status is False:
				self.server3 = rpyc.connect("192.168.56.104", 11025)
				self.server3_status = True
		except Exception, e:
			print("Server3 not available!")

	# +============================
	# | Filesystem Operations
	# +============================
	def access(self, path, mode):
		try:
			print("Access: " + path)
			try:
				result = self.server1.root.access(path,mode)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.access(path,mode)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.access(path,mode)
			except Exception, e:
				result3 = None

			return result
		except Exception, e:
			print("Error: ACCESS")
			print(str(e))

	def chmod(self, path, mode):
		try:
			print("Chmod.")
			try:
				result = self.server1.root.chmod(path, mode)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.chmod(path, mode)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.chmod(path, mode)
			except Exception, e:
				result3 is None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
        			return result
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: CHMOD")
			print(str(e))

	def chown(self, path, uid, guid):
		try:
			print("Chown.")
			try:
				result = self.server1.root.chown(path,uid,guid)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.chown(path,uid,guid)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.chown(path,uid,guid)
			except Exception, e:
				result3 = None
			if result is None and result2 is None:
				return result3
			elif result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
		except Exception, e:
			print("Error: CHOWN")
			print(str(e))

	def getattr(self, path, fh=None):
		try:
			print("Getattr: " + path)
			try:
				result = self.server1.root.getattr(path, fh)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.getattr(path, fh)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.getattr(path, fh)
			except Exception, e:
				result3 = None

			if result is None and result3 is None:
				return result2
			elif result is None and result2 is None:
				return result3
			elif result2 is None and result3 is None:
				return result
			else:
				if result is None:
					return result2
				elif result2 is None:
					return result3
				elif result3 is None:
					return result
				else:
					return result
		except Exception, e:
			print("Error: GETATTR")
			print(str(e))

	#getxattr	= None

	def readdir(self, path, fh):
		try:
			print("Readdir.")
			try:
				self.try_reconnect()
				#print "Reconnect tried!"
			except Exception, e:
				print("Cannot reconnect.")
				print(str(e))
			
			try:
				result = self.server1.root.readdir(path, fh)
			except Exception, e:
				self.server1_status = False
				result = []
			try:
				result2 = self.server2.root.readdir(path, fh)
			except Exception, e:
				self.server2_status = False
				result2 = []
			try:
				result3 = self.server3.root.readdir(path, fh)
			except Exception, e:
				self.server3_status = False
				result3 = []
			resgen = itertools.chain(result, result2)
			resgenfinal = itertools.chain(resgen, result3)
			return resgenfinal
		except Exception, e:
			print("Error: READDIR")
			print(str(e))

	def readlink(self, path):
		try:
			print("Readlink.")
			result = self.server1.root.readlink(path)
			return result
		except Exception, e:
			print("Error: READLINK")
			print(str(e))

	def mknod(self, path, mode, dev):
		try:
			print("Mknod.")
			result = self.server1.root.mknod(path, mode, dev)
			return result
		except Exception, e:
			print("Error: MKNOD")
			print(str(e))

	def rmdir(self, path):
		try:
			print("Rmdir.")
			try:
				result = self.server1.root.rmdir(path)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.rmdir(path)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.rmdir(path)
			except Exception, e:
				result3 = None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
			elif result is NOne and result2 is None:
				return result3
		except Exception, e:
			print("Error: RMDIR")
			print(str(e))

	def unlink(self, path):
		try:
			print("Unlink.")
			return self.server1.root.unlink(path)
		except Exception, e:
			print("Error: UNLINK")
			print(str(e))

	def mkdir(self, path, mode):
		try:
			print("Mkdir.")
			result = self.server1.root.mkdir(path, mode)
			return result
		except Exception, e:
			print("Error: MKDIR")
			print(str(e))

	def statfs(self, path):
		try:
			print("Statfs.")
			try:
				result = self.server1.root.statfs(path)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.statfs(path)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.statfs(path)
			except Exception, e:
				result3 = None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: STATFS")
			print(str(e))

	def symlink(self, name, target):
		try:
			print("Symlink.")
			result = self.server1.root.symlink(name, target)
			return result
		except Exception, e:
			print("Error: SYMLINK")
			print(str(e))
		
	def rename(self, old, new):
		try:
			print("Rename.")
			result = self.server1.root.rename(old, new)
			return result
		except Exception, e:
			print("Error: RENAME")
			print(str(e))

	def link(self, target, name):
		try:
			print("Link.")
			result = self.server1.root.link(target, name)
			return result
		except Exception, e:
			print("Error: LINK")
			print(str(e))

	def utimens(self, path, times=None):
		try:
			print("Utimens.")
			result = self.server1.root.utimens(path, times)
			return result
		except Exception, e:
			print("Error: UTIMENS")
			print(str(e))
 
	# +============================
	# | File Operations
	# +============================
	def open(self, path, flags, mode=None):
		try:
			print("Open.")
			try:
				result = self.server1.root.open(path, flags, mode)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.open(path, flags, mode)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.open(path, flags, mode)
			except Exception, e:
				result3 = None
			#print(result)
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: OPEN")
			print(str(e))

	def create(self, path, mode):
		try:
			print("Create.")
			return self.open(path, os.O_WRONLY | os.O_CREAT, mode)
		except Exception, e:
			print("Error: CREATE")
			print(str(e))

	def read(self, path, length, offset, fh):
		try:
			print("Read.")
			try:
				result = self.server1.root.read(path, length, offset, fh)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.read(path, length, offset, fh)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.read(path, length, offset, fh)
			except Exception, e:
				result3 = None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: READ")
			print(str(e))

	def write(self, path, buf, offset, fh):
		try:
			print("Write.")
			try:
				result = self.server1.root.write(path, buf, offset, fh)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.write(path, buf, offset, fh)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.write(path, buf, offset, fh)
			except Exception, e:
				result3 = None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: WRITE")
			print(str(e))

	def truncate(self, path, length, fh=None):
		try:
			print("Truncate.")
			return self.server1.root.truncate(path, length, fh)
		except Exception, e:
			print("Error: TRUNCATE")
			print(str(e))

	def flush(self, path, fh):
		try:
			print("Flush.")
			try:
				result = self.server1.root.flush(path, fh)
			except Exception, e:
				result = None
			try:
				result2 = self.server2.root.flush(path, fh)
			except Exception, e:
				result2 = None
			try:
				result3 = self.server3.root.flush(path, fh)
			except Exception, e:
				result3 = None
			if result is None and result3 is None:
				return result2
			elif result2 is None and result3 is None:
				return result 
			elif result is None and result2 is None:
				return result3
		except Exception, e:
			print("Error: FLUSH")
			print(str(e))

	def release(self, path, fh):
		try:
			return self.server1.root.release(path, fh)
		except Exception, e:
			print("Error: RELEASE")
			print(str(e))

	def fsync(self, path, fdatasync, fh):
		try:
			return self.flush(path, fh)
		except Exception, e:
			print("Error: FSYNC")
			print(str(e))

if __name__ == "__main__":
	mntpoint = '/home/deneme2/Desktop/fs_virt'
        FUSE(RPYCFuse(), mntpoint, foreground=True, nothreads=True)



