import socket
import sys
import os.path
import argparse

	# +============================
	# | Variables
	# +============================

server1_size = 0
server2_size = 0
server3_size = 0
empty_message = 'a'

server1_getsize_address = ('192.168.56.101', 3006)
server2_getsize_address = ('192.168.56.103', 3006)
server3_getsize_address = ('192.168.56.104', 3006)

server1_exists_address = ('192.168.56.101', 3007)
server2_exists_address = ('192.168.56.103', 3007)
server3_exists_address = ('192.168.56.104', 3007)

server1_address = ('192.168.56.101', 3004)
server2_address = ('192.168.56.103', 3004)
server3_address = ('192.168.56.104', 3004)

server1_path_exists = "def"
server2_path_exists = "def"
server3_path_exists = "def"

	# +============================
	# | Sockets
	# +============================


sock_server1_getsize = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server1_getsize.settimeout(5)
sock_server2_getsize = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server2_getsize.settimeout(5)
sock_server3_getsize = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server3_getsize.settimeout(5)

sock_server1_exists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server1_exists.settimeout(5)
sock_server2_exists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server2_exists.settimeout(5)
sock_server3_exists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server3_exists.settimeout(5)

sock_server1_fexists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server1_fexists.settimeout(5)
sock_server2_fexists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server2_fexists.settimeout(5)
sock_server3_fexists = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock_server3_fexists.settimeout(5)

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	# +============================
	# | Argument Parser
	# +============================

parser = argparse.ArgumentParser(description="Folder name.")
parser.add_argument('filename', help="Folder name.")
args = parser.parse_args()
file_name = args.filename
pathtofile = []
	# +============================
	# | Xinetd Operations
	# +============================

"""
PATH EXISTS
"""
if "/" in file_name:
	pathtofile = file_name.rsplit('/',1)
	try:
		sock_server1_exists.connect(server1_exists_address)
		sock_server1_exists.send(pathtofile[0])
		sock_server1_exists.shutdown(socket.SHUT_WR)
		server1_path_exists = sock_server1_exists.recv(1024)
		print(server1_path_exists)
	except socket.error, e:
		print("Server 1 is down!")
		print(str(e))
		server1_path_exists = "0"

	try:
		sock_server2_exists.connect(server2_exists_address)
		sock_server2_exists.send(pathtofile[0])
		sock_server2_exists.shutdown(socket.SHUT_WR)
		server2_path_exists = sock_server2_exists.recv(1024)
		print(server2_path_exists)
	except Exception, e:
		print("Server 2 is down!")
		server2_path_exists = "0"
	try:
		sock_server3_exists.connect(server3_exists_address)
		sock_server3_exists.send(pathtofile[0])
		sock_server3_exists.shutdown(socket.SHUT_WR)
		server3_path_exists = sock_server3_exists.recv(1024)
		print(server3_path_exists)
	except Exception, e:
		print("Server 3 is down!")
		server3_path_exists = "0"

"""
FILE EXISTS
"""
try:
	sock_server1_fexists.connect(server1_exists_address)
	sock_server1_fexists.send(file_name)
	sock_server1_fexists.shutdown(socket.SHUT_WR)
	server1_path_fexists = sock_server1_fexists.recv(1024)
except Exception, e:
	print("Server 1 is down!")
	server1_path_fexists = "0"
try:
	sock_server2_fexists.connect(server2_exists_address)
	sock_server2_fexists.send(file_name)
	sock_server2_fexists.shutdown(socket.SHUT_WR)
	server2_path_fexists = sock_server2_fexists.recv(1024)
except Exception, e:
	print("Server 2 is down!")
	server2_path_fexists = "0"

try:
	sock_server3_fexists.connect(server3_exists_address)
	sock_server3_fexists.send(file_name)
	sock_server3_fexists.shutdown(socket.SHUT_WR)
	server3_path_fexists = sock_server3_fexists.recv(1024)
except Exception, e:
	print("Server 3 is down!")
	server3_path_fexists = "0"

try:	
	sock_server1_getsize.connect(server1_getsize_address)
	sock_server1_getsize.send(empty_message)
	sock_server1_getsize.shutdown(socket.SHUT_WR)
	server1_size = sock_server1_getsize.recv(1024)
except Exception, e:
	print("Server 1 is down!")
	server1_size = sys.maxint

try:
	sock_server2_getsize.connect(server2_getsize_address)
	sock_server2_getsize.send(empty_message)
	sock_server2_getsize.shutdown(socket.SHUT_WR)	
	server2_size = sock_server2_getsize.recv(1024)
except Exception, e:
	print("Server 2 is down!")
	server2_size = sys.maxint

try:
	sock_server3_getsize.connect(server3_getsize_address)
	sock_server3_getsize.send(empty_message)
	sock_server3_getsize.shutdown(socket.SHUT_WR)	
	server3_size = sock_server3_getsize.recv(1024)
except Exception, e:
	print("Server 3 is down!")
	server3_size = sys.maxint
	# +============================
	# | Mkdir
	# +============================

print("Server1 size: " + server1_size)
print("Server2 size: " + server2_size)
print("Server3 size: " + server3_size)

#print(type(server1_size))
#print(type(server2_size))
try:
	print("SV1: " + server1_path_exists)
	print("SV2: " + server2_path_exists)
	print("SV3: " + server3_path_exists)
except Exception, e:
	print("Error!")
	print(str(e))

if "1" in server1_path_fexists or "1" in server2_path_fexists or "1" in server3_path_fexists:
	print("File exists.")
else:
	if "1" in server1_path_exists and "0" in server2_path_exists and "0" in server3_path_exists:
		server_address = server1_address
	elif "0" in server1_path_exists and "1" in server2_path_exists and "0" in server3_path_exists:
		server_address = server2_address
	elif "0" in server1_path_exists and "0" in server2_path_exists and "1" in server3_path_exists:
		server_address = server3_address
	elif "0" in server1_path_exists and "0" in server2_path_exists and "0" in server3_path_exists:
		if int(server1_size) <= int(server2_size) and int(server1_size) <= int(server3_size):
			server_address = server1_address
		elif int(server2_size) <= int(server1_size) and int(server2_size) <= int(server3_size):
			server_address = server2_address
		elif int(server3_size) <= int(server1_size) and int(server3_size) <= int(server2_size):
			server_address = server3_address
	elif "def" in server1_path_exists and "def" in server2_path_exists and "def" in server3_path_exists:
		if int(server1_size) <= int(server2_size) and int(server1_size) <= int(server3_size):
			server_address = server1_address
		elif int(server2_size) <= int(server1_size) and int(server2_size) <= int(server3_size):
			server_address = server2_address
		elif int(server3_size) <= int(server1_size) and int(server3_size) <= int(server2_size):
			server_address = server3_address
	else:
		print("There is something wrong with the file system! Contact system administrator!")

	try:
		message = file_name
		sock.connect(server_address)
		sock.send(message)
		sock.shutdown(socket.SHUT_WR)
		back = sock.recv(1024)
		print back.strip()
	except Exception, e:
		print("Connection could not be established!")
