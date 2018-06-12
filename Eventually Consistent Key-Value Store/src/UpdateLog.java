
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;



public class UpdateLog extends Thread {
	String Caller_IPAddr;
	int caller_port;
	int key;
	String value;
	String currentTS;
	StubImplementor stubImplementor;
	int index;
	int consistencyLevel;
	String result;
	int requestID;
	ArrayList<Integer> temp = new ArrayList<Integer>();

	public UpdateLog(StubImplementor stubImplementor,int key, String value, int consistencyLevel, String timestamp, int index, int requestID) {
		// TODO Auto-generated constructor stub
		Caller_IPAddr = stubImplementor.IPAddr;
		caller_port = stubImplementor.port;
		this.key = key;
		this.value = value;
		this.currentTS = timestamp;
		this.stubImplementor = stubImplementor;
		this.index = index;
		this.consistencyLevel = consistencyLevel;
		this.requestID = requestID;
	}

	public void run() {
		List<String> checkList = new ArrayList<String>();

		if(stubImplementor.NodeList.get(index).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(index).port == caller_port) {
			try {
				result = stubImplementor.UpdateReplicas(key,value,currentTS);
				stubImplementor.consistencyCount.put(requestID, stubImplementor.consistencyCount.get(requestID)+1);
				temp = stubImplementor.availableNodes.get(requestID);
				System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
				System.out.println(temp);
				temp.set(index, 1);
				stubImplementor.availableNodes.put(requestID, temp);
			} catch (SystemException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}else {
			//RPC call to replicas and wait for response = consistencyLevel
			try {
				TTransport transport;
				transport = new TSocket(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				ReplicaServices.Client client = new ReplicaServices.Client(protocol);
				result = client.UpdateReplicas(key,value,currentTS);
				transport.close();
				stubImplementor.consistencyCount.put(requestID, stubImplementor.consistencyCount.get(requestID)+1);
				temp = stubImplementor.availableNodes.get(requestID);
				System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
				System.out.println(temp);
				temp.set(stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port), 1);
				stubImplementor.availableNodes.put(requestID, temp);

			} catch (TException e) {
				// TODO Auto-generated catch block
				System.out.println(stubImplementor.NodeList.get(index).ip+":"+stubImplementor.NodeList.get(index).port+" is down");
				temp = stubImplementor.availableNodes.get(requestID);
				System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
				System.out.println(temp);
				temp.set(stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port), 0);
				stubImplementor.availableNodes.put(requestID, temp);
				stubImplementor.nodeStatus.put(index, false);
				if(stubImplementor.hintedoff) {
					HashMap<Integer, DataValue> newMap = null;
					if(stubImplementor.hintsMap.containsKey(index)) {
						newMap = stubImplementor.hintsMap.get(index);
					}
					else {
						newMap = new HashMap<Integer, DataValue>();
					}			
					newMap.put(key, new DataValue(currentTS, value));
					stubImplementor.hintsMap.put(index,newMap);
					System.out.println("Hints Map"+stubImplementor.hintsMap);
				}
			}
		}
		System.out.println("before exiting");
		while(stubImplementor.availableNodes.get(requestID).contains(-1)) {} //keep looping
		System.out.println("final list"+stubImplementor.availableNodes.get(requestID));
		int occurrences = Collections.frequency(stubImplementor.availableNodes.get(requestID), 1);
		if(occurrences >= consistencyLevel) {
			if(stubImplementor.NodeList.get(index).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(index).port == caller_port) {
				while(stubImplementor.consistencyCount.get(requestID)< consistencyLevel) {
					//keep looping
				}
				System.out.println("here.......");
				stubImplementor.returnWrite = "Write Completed";
			}
		}else {
			stubImplementor.returnWrite = "Error";
			if(stubImplementor.hintedoff) {
				stubImplementor.hintsMap.remove(index);
				try {
				ArrayList<Integer> deleteList = stubImplementor.availableNodes.get(requestID);
				for(int i=0;i<deleteList.size();i++) {
					if(deleteList.get(i) == 1) {
						if(stubImplementor.NodeList.get(i).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(i).port == caller_port) {
							stubImplementor.deleteData(key+":"+value+":"+currentTS);
						}else {
						
							TTransport transport;
							transport = new TSocket(stubImplementor.NodeList.get(i).ip, stubImplementor.NodeList.get(i).port);
							transport.open();
							TProtocol protocol = new TBinaryProtocol(transport);
							ReplicaServices.Client client = new ReplicaServices.Client(protocol);
							client.deleteData(key+":"+value+":"+currentTS);
							transport.close();
						}
					}
				}
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}

		System.out.println("exiting");
	}
}

