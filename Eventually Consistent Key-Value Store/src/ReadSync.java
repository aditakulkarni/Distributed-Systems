
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ReadSync extends Thread{
	StubImplementor stubImplementor;
	String Caller_IPAddr;
	int caller_port;
	int key;
	
	public ReadSync(StubImplementor stubImplementor, int key) {
		// TODO Auto-generated constructor stub
		this.stubImplementor = stubImplementor;
		Caller_IPAddr = stubImplementor.IPAddr;
		caller_port = stubImplementor.port;
		this.key = key;
	}
	
	public void run() {
		System.out.println("Running read synchronization");
		DataValue temp = null;
		List<DataValue> currentNodes = new ArrayList<DataValue>();
		for(int i=0;i<stubImplementor.NodeList.size();i++) {
			if(stubImplementor.NodeList.get(i).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(i).port == caller_port) {
				try {
					temp = stubImplementor.ReadData(key);
					currentNodes.add(temp);
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
				transport = new TSocket(stubImplementor.NodeList.get(i).ip, stubImplementor.NodeList.get(i).port);
				transport.open();
				TProtocol protocol = new TBinaryProtocol(transport);
				ReplicaServices.Client client = new ReplicaServices.Client(protocol);
				temp = client.ReadData(key);
				transport.close();
				currentNodes.add(temp);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		//wait for all values
		while(currentNodes.size() != stubImplementor.NodeList.size()) {
		//keep looping
		}
		//check for timestamp and update values if required
		updateAllReplicas(currentNodes);
	}
	
	void updateAllReplicas(List<DataValue> currentNodes) {
		Boolean check = checkforUpdates(currentNodes);
		if(check) {
			int latestNode = findMaxTimestampNode(currentNodes);
			int currentNodeIndex = stubImplementor.getNodeIndex(Caller_IPAddr, caller_port);
			for(int i=0;i<currentNodes.size();i++) {
				if(currentNodes.get(i).timestamp.equals("NoValue")) {
				if(i == currentNodeIndex) {
					try {
						System.out.println("READ REPAIR"+key+currentNodes.get(latestNode).value+currentNodes.get(latestNode).timestamp);
						stubImplementor.UpdateReplicas(key,currentNodes.get(latestNode).value,currentNodes.get(latestNode).timestamp);
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
					transport = new TSocket(stubImplementor.NodeList.get(i).ip, stubImplementor.NodeList.get(i).port);
					transport.open();
					TProtocol protocol = new TBinaryProtocol(transport);
					ReplicaServices.Client client = new ReplicaServices.Client(protocol);
					System.out.println("ELSE READ REPAIR"+key+currentNodes.get(latestNode).value+currentNodes.get(latestNode).timestamp);
					client.UpdateReplicas(key,currentNodes.get(latestNode).value,currentNodes.get(latestNode).timestamp); //put ka log??
					transport.close();
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				}
			}
		}
		
	}
	
	Boolean checkforUpdates(List<DataValue> currentNodes) {
		Boolean check = false;
		String temp = currentNodes.get(0).timestamp;
		for(int i=0; i<currentNodes.size(); i++){
	        if(currentNodes.get(i).timestamp.compareTo(temp) != 0){
	            check = true;
	            break;
	        }
	    }
		return check;
	}
	
	int findMaxTimestampNode(List<DataValue> currentNodes) {
		int max=0;
		String temp = null;
		for(int i=0; i<currentNodes.size(); i++){
	        if(currentNodes.get(i).timestamp.compareTo("NoValue") != 0){
	            temp = currentNodes.get(i).timestamp;
				break;
	        }
	    }
		for(int i=0; i<currentNodes.size(); i++){
	        if(currentNodes.get(i).timestamp.compareTo("NoValue") != 0 && currentNodes.get(i).timestamp.compareTo(temp) > 1){
	            max = i;
	            temp = currentNodes.get(i).timestamp;
	        }
	    }
		return max;
	}

}
