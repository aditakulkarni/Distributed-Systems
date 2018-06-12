
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.swing.text.html.HTMLDocument.HTMLReader.HiddenAction;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class ReadLog extends Thread {
	String Caller_IPAddr;
	int caller_port;
	int key;
	StubImplementor stubImplementor;
	int index;
	int consistencyLevel;
	DataValue result;
	int requestID;
	ArrayList<Integer> temp = new ArrayList<Integer>();
	
	public ReadLog(StubImplementor stubImplementor,int key, int consistencyLevel, int index, int requestID) {
		// TODO Auto-generated constructor stub
		Caller_IPAddr = stubImplementor.IPAddr;
		caller_port = stubImplementor.port;
		this.key = key;
		this.stubImplementor = stubImplementor;
		this.index = index;
		this.consistencyLevel = consistencyLevel;
		this.requestID = requestID;
	}

	public void run() {
	try {	
	if(stubImplementor.NodeList.get(index).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(index).port == caller_port) {
			result = stubImplementor.ReadData(key);
			stubImplementor.readList.add(result);
			stubImplementor.consistencyCount.put(requestID, stubImplementor.consistencyCount.get(requestID)+1);
			System.out.println("Added to checklist by"+index+" : "+stubImplementor.readList.size());
			temp = stubImplementor.availableNodes.get(requestID);
			System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
			System.out.println(temp);
			temp.set(index, 1);
			stubImplementor.availableNodes.put(requestID, temp);
	}else {
		//RPC call to replicas and wait for response = consistencyLevel
		TTransport transport;
		transport = new TSocket(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port);
		transport.open();
		TProtocol protocol = new TBinaryProtocol(transport);
		ReplicaServices.Client client = new ReplicaServices.Client(protocol);
		result = client.ReadData(key);
		System.out.println("result is :"+ result);
		transport.close();
		
		
		if("NoValue".equals(result.getTimestamp())) {
			throw new SystemException().setMessage("No Data");
		}
		
		stubImplementor.readList.add(result);
		stubImplementor.consistencyCount.put(requestID, stubImplementor.consistencyCount.get(requestID)+1);
		System.out.println("Added to checklist "+index+" : "+stubImplementor.readList.size());
		temp = stubImplementor.availableNodes.get(requestID);
		System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
		System.out.println(temp);
		temp.set(index, 1);
		stubImplementor.availableNodes.put(requestID, temp);
	}
	
	
	while(stubImplementor.availableNodes.get(requestID).contains(-1)) {} //keep looping
	System.out.println("final list"+stubImplementor.availableNodes.get(requestID));
	int occurrences = Collections.frequency(stubImplementor.availableNodes.get(requestID), 1);
	if(occurrences >= consistencyLevel) {
		if(stubImplementor.NodeList.get(index).ip.equals(Caller_IPAddr) && stubImplementor.NodeList.get(index).port == caller_port) {
			System.out.println("CheckL Size:"+stubImplementor.readList.size());
			while(stubImplementor.consistencyCount.get(requestID)< consistencyLevel) {
				//keep looping
			}
			System.out.println(stubImplementor.readList);
			DataValue dv = null;
			String maxTimeStamp = "";
			
			String temp = stubImplementor.readList.get(0).timestamp;
			if(!(stubImplementor.hintedoff)) {
				Boolean check = false;
				dv = stubImplementor.readList.get(0);
				for(int i=0; i<consistencyLevel; i++){
			        if(stubImplementor.readList.get(i).timestamp.compareTo(temp) != 0){
			            check = true;
			            break;
			        }
			    }
				if(check) {
					System.out.println("Calling readSync");
					ReadSync readrepair = new ReadSync(stubImplementor,key);
					readrepair.start();
				}
			}
			
			
			for(int i=0;i<consistencyLevel;i++) {
				if(temp.compareTo(stubImplementor.readList.get(i).getTimestamp()) <= 0) {
					temp = stubImplementor.readList.get(i).getTimestamp();
					dv = stubImplementor.readList.get(i);
				}
				System.out.println("TS:"+stubImplementor.readList.get(i).getTimestamp());
			}
			stubImplementor.datavalue = dv;
		}
	}else {
		stubImplementor.datavalue = null;
	}
	
	} catch (TException e) {
		// TODO Auto-generated catch block
		System.out.println(stubImplementor.NodeList.get(index).ip+":"+stubImplementor.NodeList.get(index).port+" is down");
		temp = stubImplementor.availableNodes.get(requestID);
		System.out.println("updating :"+stubImplementor.getNodeIndex(stubImplementor.NodeList.get(index).ip, stubImplementor.NodeList.get(index).port));
		System.out.println(temp);
		temp.set(index, 0);
		stubImplementor.availableNodes.put(requestID, temp);
	}
	}	
}
