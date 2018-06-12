import sys
import glob
sys.path.append('gen-py')
sys.path.insert(0, glob.glob('/home/yaoliu/src_code/local/lib/lib/python2.7/site-packages')[0])

from chord import FileStore
from chord.ttypes import SystemException, RFileMetadata, RFile, NodeID

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol


def main():

    if len(sys.argv) < 3:
        print "Arguments missing.\nUsage: ./client.sh [server_ip] [server_port]"
        sys.exit(1)

    # Make socket
    transport = TSocket.TSocket(sys.argv[1], int(sys.argv[2]))
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TBinaryProtocol.TBinaryProtocol(transport)

    # Create a client to use the protocol encoder
    client = FileStore.Client(protocol)

    # Connect
    transport.open()

    try:
        rfile = RFile()
        rfile.meta = RFileMetadata()
        rfile.meta.filename = "dream.txt"
        rfile.meta.owner = "Bob"
        rfile.content = "XYZWER"

        client.writeFile(rfile)
        print "\nFile %s written on server.." % rfile.meta.filename

    except SystemException as e:
        print e.message

    try:
        rfile1 = RFile()
        rfile1.meta = RFileMetadata()
        rfile1.meta.filename = "example123.txt"
        rfile1.meta.owner = "Alex"
        rfile1.content = "Hello"

        client.writeFile(rfile1)
        print "\nFile %s written on server.." % rfile1.meta.filename

    except SystemException as e:
        print e.message

    try:
        rfile1 = RFile()
        rfile1.meta = RFileMetadata()
        rfile1.meta.filename = "example.txt"
        rfile1.meta.owner = "guest"
        rfile1.content = "ABCDEF"

        client.writeFile(rfile1)
        print "\nFile %s written on server.." % rfile1.meta.filename
    except SystemException as e:
        print e.message

    try:
        rfile2 = RFile()
        rfile2 = client.readFile("example.txt","guest")

        print "\nContent: ", rfile2.content
        print "Meta Information:\nFileName: %s , Owner: %s, ContentHash: %s, Version: %s" % (rfile2.meta.filename, rfile2.meta.owner, rfile2.meta.contentHash, rfile2.meta.version)

    except SystemException as e:
        print e.message

    try:
        rfile2 = RFile()
        rfile2 = client.readFile("demo.txt","guest")

        print "\nContent: ", rfile2.content
        print "Meta Information:\nFileName: %s , Owner: %s, ContentHash: %s, Version: %s" % (rfile2.meta.filename, rfile2.meta.owner, rfile2.meta.contentHash, rfile2.meta.version)

    except SystemException as e:
        print e.message

    # Close
    transport.close()

if __name__ == '__main__':
    try:
        main()
    except Thrift.TException as tx:
        print('%s' % tx.message)
