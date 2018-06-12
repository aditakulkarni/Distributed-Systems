import glob
import sys
import socket
import hashlib
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer

class FileStoreHandler:
    def __init__(self, port_no, ip_address):
        self.finger_table = None
        self.port_no = port_no
        self.ip_address = ip_address
        self.node = NodeID(self.calculate_hash(self.ip_address, self.port_no), self.ip_address, self.port_no)
        self.rFile = None
        self.dictionary = {}

    def writeFile(self, rFile):
        self.rFile = rFile

        if(self.rFile.meta.filename == None or self.rFile.meta.owner == None):
            x = SystemException("\nFilename and owner cannot be 'None'.")
            raise x            

        # Calculate Hash Value of owner and filename
        fileid = self.calculate_hash(self.rFile.meta.owner, self.rFile.meta.filename)

        # Find Successor of the File id
        node = self.findSucc(fileid)

        # If Successor node id matches current node id, write file
        if self.node.id == node.id:
            if self.dictionary.has_key(fileid):
                self.dictionary[fileid].meta.version = int(self.dictionary[fileid].meta.version)+1
                self.dictionary[fileid].content = self.rFile.content
                self.dictionary[fileid].meta.contentHash = self.calculate_hash(None,None,self.rFile.content)
            else:
                self.rFile.meta.version = 0
                self.rFile.meta.contentHash = self.calculate_hash(None,None,self.rFile.content)
                self.dictionary[fileid] = self.rFile
            print "\nFile %s written on node %s:%d" %(self.rFile.meta.filename, self.ip_address, self.port_no)

        else:
            x = SystemException("\nCurrent node does not own the file id. %s not written. The file id is owned by node %s:%d" % (self.rFile.meta.filename, node.ip, node.port))
            raise x

    def readFile(self, filename, owner):

        # Calculate Hash Value of owner and filename
        fileid = self.calculate_hash(owner, filename)

        # Find Successor of the File id
        node = self.findSucc(fileid)

        # If Successor node id matches current node id, read file
        if self.node.id == node.id:
            if self.dictionary.has_key(fileid):
                return self.dictionary[fileid]
            else:
                x = SystemException("\nCurrent node owns the file id, but file %s does not exist on server." % filename)
                raise x

        else:
            x = SystemException("\nCurrent node does not own the file id. %s cannot be read. The file id is owned by node %s:%d" % (self.rFile.meta.filename, node.ip, node.port))
            raise x

    def setFingertable(self, node_list):
        # Initialize finger table
        self.finger_table = node_list

    def findSucc(self, key):
        if int(key,16) == int(self.node.id,16):
            return self.node

        if(int(self.node.id,16) < int(key,16) and int(key,16) <= int(self.getNodeSucc().id,16)):
            return self.getNodeSucc()

        n_prime = self.findPred(key)

        if int(n_prime.id,16) == int(self.node.id,16):
            return self.getNodeSucc()

        # RPC
        transport = TSocket.TSocket(n_prime.ip, n_prime.port)
        transport = TTransport.TBufferedTransport(transport)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = FileStore.Client(protocol)
        transport.open()
        return client.findSucc(key)


    def findPred(self, key):
        if self.finger_table == None:
            x = SystemException("\nFinger table for current node does not exist.")
            raise x

        # Get preceding node
        if(int(self.node.id,16) < int(key,16)):
            n_prime = self.closest_preceding_finger(key,True)
        else:
            n_prime = self.closest_preceding_finger(key,False)
        return n_prime


    def getNodeSucc(self):
        if self.finger_table == None:
            x = SystemException("\nFinger table for current node does not exist.")
            raise x

        return self.finger_table[0]

    # Get closest preceding node from finger table
    def closest_preceding_finger(self,key,condition):
        if condition:
            for i in xrange(len(self.finger_table)-1,-1,-1):
                if(int(self.node.id,16) < int(self.finger_table[i].id,16) and int(self.finger_table[i].id,16) < int(key,16)):
                    return self.finger_table[i]
        else:
            for i in xrange(len(self.finger_table)-1,-1,-1):
                if(int(self.node.id,16) < int(self.finger_table[i].id,16) or int(self.finger_table[i].id,16) < int(key,16)):
                    return self.finger_table[i]
        return self.node

    # Calculate SHA-256 hash values
    def calculate_hash(self, ip_address=None, port_no=None, contents=None):
        if contents is None:
            s = str(ip_address) + ':' + str(port_no)
        else:
            s = contents
        sha256=hashlib.sha256()
        sha256.update(s)
        k=sha256.hexdigest()
        return k

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "\nPort Number Not Specified.\nUsage: ./server.sh [server_port]"
        sys.exit(1)

    handler = FileStoreHandler(int(sys.argv[1]),socket.gethostbyname(socket.gethostname()))
    processor = FileStore.Processor(handler)
    transport = TSocket.TServerSocket(port=int(sys.argv[1]))
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    print('Starting the server...')
    server.serve()
    print('Done.')
