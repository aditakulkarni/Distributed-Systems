from email.utils import formatdate
import os.path, time, sys
import mimetypes
from threading import Thread, Lock

class WorkerThread(Thread):

	def __init__(self,addr,conn,dictionary,mutex,directory):
		Thread.__init__(self)
		(self.ip,self.port) = addr
		self.conn = conn
		self.mutex = mutex
		self.dictionary = dictionary
		self.directory = directory

	def run(self):

		# Receive the data
		data = self.conn.recv(1024)

		# Check if it is a GET request
		if data.split(' ')[0] == 'GET':

			# Get the requested resource name
			requested_object = data.split(' ')
			http_version = requested_object[2]
			requested_object = requested_object[1]
			complete_requested_object = self.directory + requested_object

			try:

				# Check if resource is present in 'www'
				if os.path.exists(complete_requested_object):

					# Read the resource
					file_descriptor = open(complete_requested_object,'rb')
					response_body = file_descriptor.read()
					file_descriptor.close()

					# Check and set mimetype
					mtype, encoding = mimetypes.guess_type(complete_requested_object)
					if mtype == None:
						mtype = 'application/octet-stream'

					# Build response header
					response_header = "HTTP/1.1 200 OK\r\n" + "Date: " + formatdate(timeval=None, localtime=False, usegmt=True) + "\r\nServer: HTTP_Server" + "\r\nLast-Modified: " + formatdate(timeval=os.path.getmtime(complete_requested_object), localtime=False, usegmt=True) +  "\r\nContent-Type: " + str(mtype) + "\r\nContent-Length: " + str(os.path.getsize(complete_requested_object)) + "\r\n\r\n"

					# Send the resource
					self.conn.send(response_header+response_body)

					# Increment access count of resource
					self.mutex.acquire()
					if self.dictionary.has_key(requested_object):
						self.dictionary[requested_object] = self.dictionary.get(requested_object) + 1
					else:
						self.dictionary[requested_object] = 1
					self.mutex.release()

					# Print information
					print requested_object + "|" + str(self.ip) + "|" + str(self.port) + "|" + str(self.dictionary[requested_object])

				else:

					# Send error page if resource not found
					response_body = '<!DOCTYPE html><html><title>ErrorPage</title><body><h1>Error 404 Not Found</h1></body></html>'

					# Build response header
					response_header = "HTTP/1.1 404 Not Found\r\n" + "Date: " + formatdate(timeval=None, localtime=False, usegmt=True) + "\r\nServer: HTTP_Server" + "\r\nLast-Modified: " + formatdate(timeval=None, localtime=False, usegmt=True) +  "\r\nContent-Type: text/html" + "\r\nContent-Length: " + str(len(response_body.encode('utf-8'))) + "\r\n\r\n"

					# Send the page
					self.conn.send(response_header+response_body)

			except Exception as e:
				print "Exception: ", e

		# Close connection
		self.conn.close()
