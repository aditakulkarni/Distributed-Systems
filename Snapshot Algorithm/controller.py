import sys, socket
sys.path.append('/home/phao3/protobuf/protobuf-3.4.0/python')
import bank_pb2
from time import sleep
import random

def main():

	branch_names = []
	branch_ip = []
	branch_port = []
	snapshot_count = 1

	f1 = open(sys.argv[2],'r')
	for f in f1:
		columns = f.split(' ')
		columns = [c.strip() for c in columns]
		branch_names.append(columns[0])
		branch_ip.append(columns[1])
		branch_port.append(columns[2])
	f1.close()

	init_Branch = bank_pb2.InitBranch()
	init_Branch.balance = int(sys.argv[1])/len(branch_names)
	all_branches = [None]*len(branch_names)
	for i in range(len(branch_names)):
		all_branches[i] = bank_pb2.InitBranch.Branch()#init_Branch.all_branches.add()
		all_branches[i].name = branch_names[i]
		all_branches[i].ip = branch_ip[i]
		all_branches[i].port = int(branch_port[i])

	init_Branch.all_branches.extend(all_branches)

	branch_Message = bank_pb2.BranchMessage()
	branch_Message1 = bank_pb2.BranchMessage()
	branch_Message.init_branch.balance = init_Branch.balance
	branch_Message.init_branch.all_branches.extend(init_Branch.all_branches)

	for i in range(len(branch_names)):
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #out of loop
		s.connect((branch_ip[i], int(branch_port[i])))
		s.send(branch_Message.SerializeToString())
		s.close()

	while True:
		sleep(1)
		branch_number = random.randint(0,len(branch_names)-1)
		init_snapshot_message = bank_pb2.InitSnapshot()
		init_snapshot_message.snapshot_id = snapshot_count
		branch_Message.init_snapshot.snapshot_id = init_snapshot_message.snapshot_id
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((branch_ip[branch_number], int(branch_port[branch_number])))
		s.send(branch_Message.SerializeToString())
		s.close()

		sleep(2)
		retrieve_snapshot_message = bank_pb2.RetrieveSnapshot()
		retrieve_snapshot_message.snapshot_id = snapshot_count
		branch_Message.retrieve_snapshot.snapshot_id = retrieve_snapshot_message.snapshot_id
		for i in range(len(branch_names)):
			br_n = []
			for j in range(len(branch_names)):
				if i!=j:
					br_n.append(branch_names[j])
			s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) #out of loop
			s.connect((branch_ip[i], int(branch_port[i])))
			s.send(branch_Message.SerializeToString())
			data = s.recv(1024)
			branch_Message1.ParseFromString(data)
			print "\nSnapshot ID:",branch_Message1.return_snapshot.local_snapshot.snapshot_id
			print branch_names[i]+":",branch_Message1.return_snapshot.local_snapshot.balance
			count = 0
			for f in branch_Message1.return_snapshot.local_snapshot.channel_state:
				print br_n[count]+"->"+branch_names[i]+":"+str(f)
				count+=1
			s.close()

		snapshot_count += 1

if __name__ == '__main__':
	if len(sys.argv) != 3:
		print "Usage:", sys.argv[0], "Total_money", "Branch_file_name"
		sys.exit(-1)

	main()
