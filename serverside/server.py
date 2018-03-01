#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import rpyc
import atexit
import socket
import sys
import hashlib
from rpyc.utils.server import ThreadedServer

def md5(fname):
	hash_md5 = hashlib.md5()
	with open(fname, "rb") as f:
		for chunk in iter(lambda: f.read(4096), b""):
			hash_md5.update(chunk)
	return hash_md5.hexdigest()

def putfile(server_address, file_name):
	path = '/home/deneme4/Desktop/fs_real/'
	data = b""
	try:
		if os.path.isfile(path + file_name):
			md5_file = md5(path + file_name)
			f = open(path + file_name, 'rb')
			l = f.read(1024)
			l = l.strip('\n')
			while(l):
				data += l.strip('\n')
				l = f.read(1024)
			f.close()
			message = file_name + '#' + md5_file + '#' + data 
			sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
			sock.connect(server_address)
			sock.send(message)
			sock.shutdown(socket.SHUT_WR)
			back = sock.recv(1024)
			print back.strip()
			return True
		else:
			print 'File does not exist in folder!'
			return False 
	except Exception, e:
		print "Putfile doesn't work"
		print(str(e))
		return False


def getfile(server_address, file_name):
	sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	path = "/home/deneme4/Desktop/fs_real/"
	try:
		sock.connect(server_address)
		sock.send(file_name)
		sock.shutdown(socket.SHUT_WR)
		data = sock.recv(32)
		md5fromserver = data
		generaldata = b""
		#print 'Server MD5: ' + md5fromserver
		while data:
			data = sock.recv(1024)
			generaldata += data

		if(md5fromserver.strip() != "nf"):
			f = open(path + file_name,"w+")
			f.write(generaldata)
			f.close()
			md5local = md5(path + file_name)
			#print 'Calculated MD5: ' +  md5local
			if md5fromserver == md5local:
				print 'File succesfully transferred!'
		return True
	except Exception, e:
		print("Getfile doesn't work!")
		print(str(e))
		return False

def mkdir_on_server(server_address, path):
	sock_mkdir = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock_mkdir.settimeout(1)
	try:
		sock_mkdir.connect(server_address)
		sock_mkdir.send(path)
		sock_mkdir.shutdown(socket.SHUT_WR)
		retval = sock_mkdir.recv(1024)
	except Exception, e:
		print "Mkdir didn't work!"

def getlist_on_server(server_address, path):
	sock_getlist = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock_getlist.settimeout(1)
	try:
		sock_getlist.connect(server_address)
		sock_getlist.send(path)
		sock_getlist.shutdown(socket.SHUT_WR)
		retval = sock_getlist.recv(1024)
		return retval
	except Exception, e:
		print "Getlist didn't work!"

def rmall(server_address, path):
	sock_rmall = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock_rmall.settimeout(1)
	try:
		sock_rmall.connect(server_address)
		sock_rmall.send(path)
		sock_rmall.shutdown(socket.SHUT_WR)
		retval = sock_rmall.recv(1024)
	except Exception, e:
		print "Rmall didn't work!"

def getsize(server_address):
	empty_message = 'a'
	sock_s1_getsize = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	sock_s1_getsize.settimeout(1)
	try:
		sock_s1_getsize.connect(server_address)
		sock_s1_getsize.send(empty_message)
		sock_s1_getsize.shutdown(socket.SHUT_WR)
		server1_size = sock_s1_getsize.recv(1024)
		server1_status = True
	except Exception, e:
		print "Server is down!"
		server1_size = sys.maxint
		server1_status = False
	return server1_size


@atexit.register
def goodbye():
	server1_address = ('192.168.121.4', 3006)
	server2_address = ('192.168.121.6', 3006)
	server1_mkdir = ('192.168.121.4', 3005)
	server2_mkdir = ('192.168.121.6', 3005)
	server1_putf = ('192.168.121.4', 3009)
	server2_putf = ('192.168.121.6', 3009)
	empty_message = 'a'
	server1_size = 0
	server2_size = 0
	server1_status = False
	server2_status = False
	server_string = ""
	n = 5
	nop = False

	server1_size = getsize(server1_address)
	server2_size = getsize(server2_address)
	if int(server1_size) < sys.maxint:
		server1_status = True
	if int(server1_size) < sys.maxint:
		server2_status = True
	
	if server1_status is False and server2_status is False:
		nop = True
	else:
		if int(server1_size) < int(server2_size):
			server_address_mkdir = server1_mkdir
			server_address_putf = server1_putf
			server_string = "1"
		else:
			server_address_mkdir = server2_mkdir
			server_address_putf = server2_putf
			server_string = "2"
		

	print("Server 1 size: " + str(server1_size) + " Server 1 status: " + str(server1_status))
	print("Server 2 size: " + str(server2_size) + " Server 2 status: " + str(server2_status))
	if nop is False:
		metadatafile = open("metadata.txt", "w")
		for root, directories, filenames in os.walk('/home/deneme4/Desktop/fs_real'):
			for directory in directories:
				sd = server_string + "#dir#" + os.path.join(root,directory) + "\n"
				groups = os.path.join(root,directory).split('/')
				result = '/'.join(groups[n:])
				print("Making dir: " + server_address_mkdir[0] + " " + result)
				mkdir_on_server(server_address_mkdir, result)
				#print result
				metadatafile.write(sd)		
			for filename in filenames:
				sf = server_string + "#file#" + os.path.join(root,filename) + "\n"
				fgroups = os.path.join(root,filename).split('/')
				fresult = '/'.join(fgroups[n:])
				putfile(server_address_putf, fresult)
				print("Copying file: " + server_address_mkdir[0] + " " + fresult)
				metadatafile.write(sf)
		metadatafile.close()
		for root, directories, filenames in os.walk('/home/deneme4/Desktop/fs_real', topdown=False):
			for directory in directories:
				os.rmdir(os.path.join(root,directory))	
			for filename in filenames:
				os.remove(os.path.join(root,filename))
	else:
		print("No other available cluster servers!")
		metadatafile = open("metadata.txt", "w")
		metadatafile.write("NOP")
		metadatafile.close()
	print("Server shutdown! ")

class RPYCFuseService(rpyc.Service):
	# +========================
	# | Variables
 	# +========================
	root = '/home/deneme4/Desktop/fs_real'
	# +========================
	# | Helpers
 	# +========================
    	def __init__(self, root):
        	self.root = root

	def getpath(self, path):
		try:
			return '/home/deneme4/Desktop/fs_real' + path
		except Exception, e:
			print("Error: GETPATH")
			print(str(e))

	def on_connect(self):
		print "Connection from client established!"

	def on_disconnect(self):
		print "Client disconnected!"
	

	# +========================
	# | Filesystem Operations
 	# +========================

 	def exposed_access(self, path, mode):
		try:
			full_path = self.getpath(path)
			print("Access: " + full_path)
			return os.access(full_path, mode=os.F_OK)
		except Exception, e:
			print("Error: ACCESS")
			print(str(e))

 	def exposed_chmod(self, path, mode):
		try:
			print("Chmod.")
			full_path = self.getpath(path)
			return os.chmod(full_path, mode)
		except Exception, e:
			print("Error: CHMOD")
			print(str(e))

    	def exposed_chown(self, path, uid, gid):
		try:
			print("Chown.")
        		full_path = self.getpath(path)
        		return os.chown(full_path, uid, gid)
		except Exception, e:
			print("Error: CHOWN")
			print(str(e))

	def exposed_getattr(self, path, fh):
		try:
			#if "deneme3" in self.getpath(path)
			print("Getattr: " + path)
			path = path.decode("utf-8").replace("autorun.inf","").encode("utf-8")
			path = path.decode("utf-8").replace(".xdg-volume-info","").encode("utf-8")
			path = path.decode("utf-8").replace(".Trash-1000","").encode("utf-8")
			path = path.decode("utf-8").replace(".Trash","").encode("utf-8")
			path = path.decode("utf-8").replace(".hidden","").encode("utf-8")
			path = path.decode("utf-8").replace("/files","").encode("utf-8")
			path = path.decode("utf-8").replace("/1000","").encode("utf-8")
			path = path.decode("utf-8").replace("/Untitled Folder 2","").encode("utf-8")
			path = path.decode("utf-8").replace("/info","").encode("utf-8")
			path = path.decode("utf-8").replace("/.trashinfo","").encode("utf-8")
			#path = path.decode("utf-8").replace(".Trash-1000","").encode("utf-8")
			st = os.lstat(self.getpath(path))
			#print(st)
			result = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
			'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
			#result.update({'server':'1'})
			return result
		except Exception, e:
			print("Error: GETATTR")
			print(str(e))

	def exposed_readdir(self, path, fh):
		try:
			print("Readdir: " + path)
			dirents = []
			if os.path.isdir(self.getpath(path)):
				dirents.extend(os.listdir(self.getpath(path)))
			for r in dirents:
				yield r
			#return ['.', '..'] + os.listdir(self.getpath(path))
		except Exception, e:
			print("Error: READDIR")
			print(str(e))

    	def exposed_readlink(self, path):
		try:
			print("Readlink.")
        		pathname = os.readlink(self.getpath(path))
			#print(pathname)
			return pathname
		except Exception, e:
			print("Error: READLINK")
			print(str(e))
    	def exposed_mknod(self, path, mode, dev):
		try:
			print("Mknod.")
        		return os.mknod(self.getpath(path), mode, dev)
		except Exception, e:
			print("Error: MKNOD")
			print(str(e))

	def exposed_rmdir(self, path):
		try:
			print("Rmdir.")
			full_path = self.getpath(path)
			return os.rmdir(full_path)
		except Exception, e:
			print("Error: RMDIR")
			print(str(e))
			

	def exposed_unlink(self, path):
		try:
			print("Unlink.")
			return os.unlink(self.getpath(path))
		except Exception, e:
			print("Error: UNLINK")
			print(str(e))

	def exposed_mkdir(self, path, mode=os.F_OK):
		try:
			print("Mkdir.")
			return os.mkdir(self.getpath(path), mode=os.F_OK)
		except Exception, e:
			print("Error: MKDIR")
			print(str(e))

    	def exposed_statfs(self, path):
		try:
			print("Statfs.")
			path = path.decode("utf-8").replace("autorun.inf","").encode("utf-8")
			path = path.decode("utf-8").replace(".xdg-volume-info","").encode("utf-8")
			path = path.decode("utf-8").replace(".Trash-1000","").encode("utf-8")
			path = path.decode("utf-8").replace(".Trash","").encode("utf-8")
			path = path.decode("utf-8").replace(".hidden","").encode("utf-8")
			path = path.decode("utf-8").replace("/files","").encode("utf-8")
			path = path.decode("utf-8").replace("/1000","").encode("utf-8")
        		full_path = self.getpath(path)
        		stv = os.statvfs(full_path)
        		return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            			'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            			'f_frsize', 'f_namemax'))
		except Exception, e:
			print("Error: STATFS")
			print(str(e))

    	def exposed_symlink(self, name, target):
		try:
			print("Symlink.")
        		return os.symlink(name, self.getpath(target))
		except Exception, e:
			print("Error: SYMLINK")
			print(str(e))

    	def exposed_rename(self, old, new):
		try:
			print("Rename.")
        		return os.rename(self.getpath(old), self.getpath(new))
		except Exception, e:
			print("Error: RENAME")
			print(str(e))

    	def exposed_link(self, target, name):
		try:
			print("Link.")
        		return os.link(self.getpath(target), self.getpath(name))
		except Exception, e:
			print("Error: LINK")
			print(str(e))


	# +========================
	# | File Operations
 	# +========================

	def exposed_open(self, path, flags, mode):
		try:
			print("Open.")
			full_path = self.getpath(path)
			return os.open(full_path, flags)
		except Exception, e:
			print("Error: OPEN")
			print(str(e))

	def exposed_read(self, path, length, offset, fh):
		try:
			print("Read.")
			os.lseek(fh, offset, os.SEEK_SET)
			return os.read(fh, length)
		except Exception, e:
			print("Error: READ")
			print(str(e))

	def exposed_write(self, path, buf, offset, fh):
		try:
			print("Write.")
			os.lseek(fh, offset, os.SEEK_SET)
			return os.write(fh, buf)
		except Exception, e:
			print("Error: WRITE")
			print(str(e))

	def exposed_flush(self, path, fh):
		try:
			print("Flush.")
			return os.close(fh)
		except Exception, e:
			print("Error: FLUSH")
			print(str(e))

    	def exposed_truncate(self, path, length, fh=None):
		try:
			print("Truncate.")
        		full_path = self.getpath(path)
        		with open(full_path, 'r+') as f:
            			f.truncate(length)
		except Exception, e:
			print("Error: TRUNCATE")
			print(str(e))

    	def exposed_release(self, path, fh):
		try:
			print("Release.")
        		return os.close(fh)
		except Exception, e:
			print("Error: RELEASE")
			print(str(e))

    	def exposed_fsync(self, path, fdatasync, fh):
		try:
			print("Fsync.")
        		return self.flush(path, fh)
		except Exception, e:
			print("Error: FSYNC")
			print(str(e))



if __name__ == "__main__":
	n = 5
	nop = False
	path = '/home/deneme4/Desktop/fs_real/'
	server1_getfile = ('192.168.121.4', 3002)
	server2_getfile = ('192.168.121.6', 3002)
	server1_rmall = ('192.168.121.4', 3010)
	server2_rmall = ('192.168.121.6', 3010)
	server1_getlist = ('192.168.121.4', 3011)
	server2_getlist = ('192.168.121.6', 3011)
	active_server = ""
	thread = ThreadedServer(RPYCFuseService, port=11025, protocol_config={"allow_public_attrs":True})
	dir_list = []
	file_list = []
	with open("metadata.txt", "r") as f:
		for line in f:
			#print line
			if "NOP" in line:
				nop = True
				break
			if "#" in line:
				metadata = line.split('#')
				print("Server: " + metadata[0])
				print("Type: " + metadata[1])
				print("Path: " + metadata[2])
				active_server = metadata[0]
				groups = metadata[2].split('/')
				result = '/'.join(groups[n:])
				print result
				if "dir" in metadata[1]:
					print "Making dir at: " + path + result
					dir_list.append(result.strip('\n'))
					#os.mkdir(path + result.strip('\n'))
				elif "file" in metadata[1]:
					print "Copying file at: " + path + result
					file_list.append(result.strip('\n'))
					#if "1" in metadata[0]: 
					#	getfile(server1_getfile, result.strip('\n'))
					#elif "2" in metadata[0]:
					#	getfile(server2_getfile, result.strip('\n'))
	#print "Folder list: "
	#print dir_list
	#print "File list: "
	#print file_list
	print active_server
	for direlem in dir_list:
		#print 'Dir elem: ' + direlem 
		if "1" in active_server:
			retval = getlist_on_server(server1_getlist, direlem)
		elif "2" in active_server:
			retval = getlist_on_server(server2_getlist, direlem)
		if len(retval) > 0:
			list_cand = retval.split(':')
		for elem in list_cand:
			if '#' in elem:
				elem_splitted = elem.split('#')
				if 'file' in elem_splitted[1] and elem_splitted[0] not in file_list:
					file_list.append(elem_splitted[0])
				elif 'dir' in elem_splitted[1] and elem_splitted[0] not in dir_list:
					dir_list.append(elem_splitted[0])
	for direlem in dir_list:
		os.mkdir(path + direlem)
	for fileelem in file_list:
		if "1" in active_server:
			getfile(server1_getfile, fileelem)
		if "2" in active_server:
			getfile(server2_getfile, fileelem)

	if nop is False:
		#for line in reversed(open("metadata.txt").readlines()):
		for line in file_list:
				#if "#" in line:
				#	metadata = line.split('#')
				#	print("Server: " + metadata[0])
				#	print("Type: " + metadata[1])
				#	print("Path: " + metadata[2])
				#	groups = metadata[2].split('/')
				#	result = '/'.join(groups[n:])
				#print result
				if "1" in active_server:
					rmall(server1_rmall, line)
					print "Removing file at: " + server1_rmall[0] + " " + line
				elif "2" in active_server:
					rmall(server2_rmall, line)
					print "Removing file at: " + server2_rmall[0] + " " + line
		for line in reversed(dir_list):
				if "1" in active_server:
					rmall(server1_rmall, line)
					print "Removing file at: " + server1_rmall[0] + " " + line
				elif "2" in active_server:
					rmall(server2_rmall, line)
					print "Removing file at: " + server2_rmall[0] + " " + line
	else:
		print("Server opened, NOP engaged!")

thread.start()
