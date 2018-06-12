import socket
import sys
import os.path
import mimetypes
from threading import Thread, Lock
from worker import WorkerThread

class Server:

	def __init__(self):

		# Create a TCP socket
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

		# Bind the socket to the port
		self.host_name = socket.gethostname()
		self.port_number = 0
		self.directory = 'www'
		self.s.bind((self.host_name, self.port_number))

		# To store access times of resources
		self.dictionary = {}

		self.mutex = Lock()
		self.number_of_connections = 10000

		# Listen for incoming connections
		self.s.listen(self.number_of_connections)

		# Print Host Name and Port Number
		print "Host Name: %s Port Number: %d" % (self.host_name, self.s.getsockname()[1])

	def start_server(self):

		# Check if directory 'www' exists
		self.check_directory()

		while True:

			# Wait for a connection
			conn, addr = self.s.accept()

			# Create a new thread for each connection
			new_thread = WorkerThread(addr,conn,self.dictionary,self.mutex,self.directory)
			new_thread.start()

	def check_directory(self):

		# Exit with error message if 'www' does not exist
		if not os.path.isdir(self.directory):
			sys.exit('\'www\' directory does not exist!')


if __name__ == '__main__':
	mimetypes.init('/etc/mime.types')
	server = Server()

	# Start server
	server.start_server()
