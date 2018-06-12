import sys, socket
import select
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2
from threading import Thread, Lock
import random
from time import sleep

class BranchWorker(object):

	def __init__(self):
		self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.s.bind((socket.gethostname(), int(sys.argv[2])))
		self.s.listen(100)
		self.branch_names = []
		self.branch_ip = []
		self.branch_port = []
		self.balance = 0
		self.mutex = Lock()
		self.server_branch_Message = bank_pb2.BranchMessage()
		self.client_branch_Message = bank_pb2.BranchMessage()
		self.marker_Message = bank_pb2.Marker()
		self.local_snapshot_Message = bank_pb2.ReturnSnapshot.LocalSnapshot()
		self.my_name = ''
		self.sock_list = [self.s]
		self.sock_dict = {}
		self.sock_dict_rev = {}

		self.state = []
		self.rec = []
		self.fm = []
		self.channel_state = []
		self.m_rcvd = []
		self.m_rcvd_cnt = []
		self.m_sent = []
		self.msg_count = 0
		self.map = {}
		self.check_branch_names = []
		self.check_branch_ip = []
		self.check_branch_port = []

	def checkMessage(self):
		server_thread = Server(self)
		server_thread.start()

		client_thread = Client(self)
		client_thread.start()

class Server(Thread):
	def __init__(self,branch_worker):
		Thread.__init__(self)
		self.branch_worker = branch_worker

	def run(self):
		while 1:

			read_sockets, write_sockets, error_sockets = select.select(self.branch_worker.sock_list,[],[])

			for rs in read_sockets:
				if rs == self.branch_worker.s:
					# Wait for a connection
					conn, addr = self.branch_worker.s.accept()
					data = conn.recv(1024)
					if "Sender:" in data:
						sender = data[7:]
						self.branch_worker.sock_dict[sender.encode('utf-8')] = conn
						self.branch_worker.sock_dict_rev[conn] = sender.encode('utf-8')
						self.branch_worker.sock_list.append(conn)
					else:
						self.branch_worker.server_branch_Message.ParseFromString(data)
						self.handleMessage(self.branch_worker.server_branch_Message,conn)
				else:
					# Receive the data
					data = rs.recv(1024)
					self.branch_worker.server_branch_Message.ParseFromString(data)
					self.handleMessage(self.branch_worker.server_branch_Message,rs)

	def handleMessage(self,branch_Message,rs):
		if branch_Message.WhichOneof("branch_message") == "init_branch":
			self.branch_worker.balance = branch_Message.init_branch.balance
			for br in branch_Message.init_branch.all_branches:
				if br.ip == socket.gethostbyname(socket.gethostname()) and int(br.port) == int(self.branch_worker.s.getsockname()[1]):
					self.branch_worker.my_name = br.name
				else:
					self.branch_worker.check_branch_names.append(br.name.encode('utf-8'))
					self.branch_worker.check_branch_ip.append(br.ip)
					self.branch_worker.check_branch_port.append(br.port)

				self.branch_worker.branch_names.append(br.name.encode('utf-8'))
				self.branch_worker.branch_ip.append(br.ip)
				self.branch_worker.branch_port.append(br.port)

			count = 0
			while self.branch_worker.my_name != self.branch_worker.branch_names[count]:
				count += 1

			for i in range(count+1,len(self.branch_worker.branch_names)):
				s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				s.connect((self.branch_worker.branch_ip[i], self.branch_worker.branch_port[i]))
				s.send("Sender:"+self.branch_worker.my_name)
				self.branch_worker.sock_dict[self.branch_worker.branch_names[i]] = s
				self.branch_worker.sock_dict_rev[s] = self.branch_worker.branch_names[i]
				self.branch_worker.sock_list.append(s)
		elif branch_Message.WhichOneof("branch_message") == "transfer":

			recv_branch_index = self.branch_worker.check_branch_names.index(self.branch_worker.sock_dict_rev.get(rs).encode('utf-8'))
			if self.branch_worker.map:
				for m in self.branch_worker.map.keys():
					if self.branch_worker.rec[self.branch_worker.map.get(m)] == 1 and self.branch_worker.m_rcvd[self.branch_worker.map.get(m)][recv_branch_index] != 1:
						self.branch_worker.channel_state[self.branch_worker.map.get(m)][recv_branch_index] = branch_Message.transfer.money

			self.branch_worker.mutex.acquire()
			self.branch_worker.balance += branch_Message.transfer.money
			self.branch_worker.mutex.release()
			#print "\nMoney Received from: ",self.branch_worker.sock_dict_rev.get(rs).encode('utf-8')
			#print "Balance: ",self.branch_worker.balance

		elif branch_Message.WhichOneof("branch_message") == "init_snapshot":

			self.branch_worker.map[branch_Message.init_snapshot.snapshot_id] = self.branch_worker.msg_count
			self.branch_worker.msg_count += 1
			index = self.branch_worker.map.get(branch_Message.init_snapshot.snapshot_id)
			self.branch_worker.channel_state.insert(index,[0] * len(self.branch_worker.check_branch_names))
			self.branch_worker.m_rcvd.insert(index,[-1] * len(self.branch_worker.check_branch_names))
			self.branch_worker.mutex.acquire()
			self.branch_worker.state.insert(index,self.branch_worker.balance)
			self.branch_worker.mutex.release()
			self.branch_worker.rec.insert(index,1)
			self.branch_worker.fm.insert(index,1)
			self.branch_worker.m_rcvd_cnt.insert(index,0)

			self.branch_worker.server_branch_Message.marker.snapshot_id = branch_Message.init_snapshot.snapshot_id

			for i in range(0,len(self.branch_worker.branch_names)):
				if self.branch_worker.branch_names[i] != self.branch_worker.my_name and (self.branch_worker.m_sent == None or len(self.branch_worker.m_sent) < index+1):
					#print "Marker sent from: "+self.branch_worker.my_name+ "->"+ self.branch_worker.branch_names[i]
					self.branch_worker.sock_dict.get(self.branch_worker.branch_names[i]).send(self.branch_worker.server_branch_Message.SerializeToString())
			self.branch_worker.m_sent.insert(index,1)

		elif branch_Message.WhichOneof("branch_message") == "marker":

			if not branch_Message.marker.snapshot_id in self.branch_worker.map.keys():
				self.branch_worker.map[branch_Message.marker.snapshot_id] = self.branch_worker.msg_count
				self.branch_worker.msg_count += 1

			index = self.branch_worker.map.get(branch_Message.marker.snapshot_id)

			if self.branch_worker.fm == None or len(self.branch_worker.fm) < index+1:
				self.branch_worker.channel_state.insert(index,[0] * len(self.branch_worker.check_branch_names))
				self.branch_worker.m_rcvd.insert(index,[-1] * len(self.branch_worker.check_branch_names))
				self.branch_worker.mutex.acquire()
				self.branch_worker.state.insert(index,self.branch_worker.balance)
				self.branch_worker.mutex.release()
				recv_branch_index = self.branch_worker.check_branch_names.index(self.branch_worker.sock_dict_rev.get(rs).encode('utf-8'))
				self.branch_worker.channel_state[index][recv_branch_index] = 0
				self.branch_worker.m_rcvd[index][recv_branch_index] = 1
				self.branch_worker.m_rcvd_cnt.insert(index,1)
				self.branch_worker.rec.insert(index,1)
				self.branch_worker.fm.insert(index,1)

				self.branch_worker.server_branch_Message.marker.snapshot_id = branch_Message.marker.snapshot_id

				for i in range(0,len(self.branch_worker.branch_names)):
					if self.branch_worker.branch_names[i] != self.branch_worker.my_name and (self.branch_worker.m_sent == None or len(self.branch_worker.m_sent) < index+1):
						#print "Marker sent from: "+self.branch_worker.my_name+"->"+self.branch_worker.branch_names[i]
						self.branch_worker.sock_dict.get(self.branch_worker.branch_names[i]).send(self.branch_worker.server_branch_Message.SerializeToString())
				self.branch_worker.m_sent.insert(index,1)
			else:
				recv_branch_index = self.branch_worker.check_branch_names.index(self.branch_worker.sock_dict_rev.get(rs).encode('utf-8'))
				self.branch_worker.m_rcvd[index][recv_branch_index] = 1
				self.branch_worker.m_rcvd_cnt[index] += 1

		elif branch_Message.WhichOneof("branch_message") == "retrieve_snapshot":

			index = self.branch_worker.map.get(branch_Message.retrieve_snapshot.snapshot_id)
			while self.branch_worker.m_rcvd_cnt[index] != len(self.branch_worker.check_branch_names):
				sleep(1)

			self.branch_worker.server_branch_Message.return_snapshot.local_snapshot.snapshot_id = branch_Message.retrieve_snapshot.snapshot_id
			self.branch_worker.server_branch_Message.return_snapshot.local_snapshot.balance = self.branch_worker.state[index]
			self.branch_worker.server_branch_Message.return_snapshot.local_snapshot.channel_state.extend(self.branch_worker.channel_state[index])
			rs.send(self.branch_worker.server_branch_Message.SerializeToString())
			rs.close()

class Client(Thread):
	def __init__(self, branch_worker):
		Thread.__init__(self)
		self.branch_worker = branch_worker

	def run(self):
		while len(self.branch_worker.branch_names) == 0:
			sleep(1)

		while 1:
			sleep(random.randint(0,5))
			branch_number = random.randint(0,len(self.branch_worker.branch_names)-1)
			percent_amount = int(random.randint(1,5)*self.branch_worker.balance/100)

			if self.branch_worker.balance-percent_amount >= 0 and socket.gethostbyname(socket.gethostname()) != self.branch_worker.branch_ip[branch_number]:
				self.branch_worker.mutex.acquire()
				self.branch_worker.balance -= percent_amount
				self.branch_worker.mutex.release()
				transfer_message = bank_pb2.Transfer()
				transfer_message.money = percent_amount
				self.branch_worker.client_branch_Message.transfer.money = transfer_message.money
				self.branch_worker.sock_dict.get(self.branch_worker.branch_names[branch_number]).send(self.branch_worker.client_branch_Message.SerializeToString())


if __name__ == '__main__':
	if len(sys.argv) != 3:
		print "Usage:", sys.argv[0], "Branch_name", "Port_No"
		sys.exit(-1)

	branch_worker = BranchWorker()
	branch_worker.checkMessage()
