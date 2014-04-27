from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TBufferedTransport
from thrift.protocol import TBinaryProtocol
import sys
sys.path.append('/home/hadoop/hbase-0.94.18/gen-py/')
from hbase import Hbase

host = 'localhost'
port = 9090
transport = TBufferedTransport(TSocket(host, port))
transport.open()
protocol = TBinaryProtocol.TBinaryProtocol(transport)

client = Hbase.Client(protocol)
print client.getTableNames()
