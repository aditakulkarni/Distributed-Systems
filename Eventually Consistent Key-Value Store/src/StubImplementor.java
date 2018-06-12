import java.io.BufferedReader;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;



public class StubImplementor implements ReplicaServices.Iface {
	List<NodeID> NodeList = new ArrayList<NodeID>();
	NodeID nodeID = null;
	Map<Integer,DataValue> KeyValueStoreMap = new HashMap<Integer, DataValue>();
	Map<Integer,Integer> consistencyCount = new HashMap<Integer,Integer>();
	Map<Integer, ArrayList<Integer>> availableNodes = new HashMap<Integer, ArrayList<Integer>>();
	Map<Integer, DataValue> resultMap = new HashMap<Integer,DataValue>();
	String IPAddr;
	int port;
	String returnWrite = "Write Completed";
	private static int count = 0;
	DataValue datavalue = new DataValue();
	List<DataValue> readList = new ArrayList<DataValue>();
	Map<Integer, HashMap<Integer,DataValue>> hintsMap = new HashMap<Integer, HashMap<Integer,DataValue>>();
	boolean hintedoff = false;
	boolean restarted = true;
	HashMap<Integer, Boolean> nodeStatus = new HashMap<Integer, Boolean>(); //to check replica status

	public StubImplementor(String ipAddr, int port) throws NoSuchAlgorithmException {
		this.nodeID = new NodeID(ipAddr, port);
		this.IPAddr = ipAddr;
		this.port = port;
		restarted = true;
		try {
			this.setFingertable(NodeList);
			this.loadMap();
			for(int i=0; i<NodeList.size(); i++) {
				nodeStatus.put(i, true);
			}
		} catch (TException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void deleteData(String record) throws SystemException , TException {
		KeyValueStoreMap.remove(Integer.parseInt(record.split(":")[0]));
		String logFile = IPAddr+port+ ".txt";
		Boolean deletefile = DeleteFromFile(record,logFile);
		if(deletefile) {
			System.out.println("File replaced");
		}
		loadMap();
	}
	
	private boolean DeleteFromFile(String record, String filename) {
		boolean successful= false;
		try {
		File inputFile = new File(filename);
		File tempFile = new File("myTempFile.txt");

		BufferedReader reader = new BufferedReader(new FileReader(inputFile));
		BufferedWriter writer = new BufferedWriter(new FileWriter(tempFile));

		String lineToRemove = record;
		String currentLine;

		while((currentLine = reader.readLine()) != null) {
		    // trim newline when comparing with lineToRemove
		    String trimmedLine = currentLine.trim();
		    if(trimmedLine.equals(lineToRemove)) continue;
		    writer.write(currentLine + System.getProperty("line.separator"));
		}
		writer.close(); 
		reader.close(); 
		successful = tempFile.renameTo(inputFile);
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return successful;
	}
	

	private void loadMap() {
		File logFile = new File(IPAddr+port+ ".txt");
		try {
			if(!logFile.exists()){logFile.createNewFile();}
			BufferedReader b = new BufferedReader(new FileReader(logFile));
			String readLine = "";
			System.out.println("updating map");
			while ((readLine = b.readLine()) != null) {
				String[] data = readLine.split(":");
				KeyValueStoreMap.put(Integer.parseInt(data[0]), new DataValue(data[2],data[1]));
			}	
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static synchronized int getID() {
		return count++;
	}


	public void setFingertable(List<NodeID> node_list) throws TException {
		try {
			File f = new File("nodes.txt");
			BufferedReader b = new BufferedReader(new FileReader(f));
			String readLine = "";
			System.out.println("Setting up nodes");
			while ((readLine = b.readLine()) != null) {
				String[] data = readLine.split(":");
				NodeID node = new NodeID(data[0],Integer.parseInt(data[1]));
				NodeList.add(node);
			}
			for(int i=0;i<NodeList.size();i++) {
				System.out.println(NodeList.get(i).ip+":"+NodeList.get(i).port);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	@Override
	public String WriteData(int key, String value, int consistencyLevel) throws SystemException, TException {
		synchronized(this) {
			if(hintedoff) {
				if(restarted) {
					restarted = false;
					for(int i=0; i<NodeList.size();i++) {
						//if(!NodeList.get(i).ip.equals(IPAddr) && NodeList.get(i).port != port) {
						if(!(NodeList.get(i).getIp().equals(IPAddr) && NodeList.get(i).getPort() == port)) {
							try {
								TTransport transport;
								transport = new TSocket(NodeList.get(i).ip, NodeList.get(i).port);
								transport.open();
								TProtocol protocol = new TBinaryProtocol(transport);
								ReplicaServices.Client client = new ReplicaServices.Client(protocol);
								System.out.println("calling checkhints");
								resultMap = client.checkHints(IPAddr,port);
								System.out.println("resultmap"+resultMap);
								transport.close();
								System.out.println("return");
								if(resultMap != null) {
									System.out.println("inside 1");
									for(Integer tempkey : resultMap.keySet()) {
										if(tempkey!=-1 && resultMap.get(tempkey) != null) {
											if(!KeyValueStoreMap.containsKey(tempkey)) {
												writeToLog(tempkey, resultMap.get(tempkey).value, resultMap.get(tempkey).timestamp);
												KeyValueStoreMap.put(tempkey,resultMap.get(tempkey));
											}
											else if(KeyValueStoreMap.get(tempkey).getTimestamp().compareTo(resultMap.get(tempkey).getTimestamp()) < 0) {
												writeToLog(tempkey, resultMap.get(tempkey).value, resultMap.get(tempkey).timestamp);
												KeyValueStoreMap.put(tempkey,resultMap.get(tempkey));
											} 					
										}
									}
									System.out.println("Main map:"+KeyValueStoreMap);
								}
							} catch (TException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}
				}

			}
		


		List<UpdateLog> currentThreads = new ArrayList<UpdateLog>();
		String currentTS = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss").format(new Date()); 
		UpdateLog updatelog = null;
		System.out.println("inside WriteData");
		System.out.println("IP:"+IPAddr);
		System.out.println("port:"+port);
		int requestID = getID();
		consistencyCount.put(requestID, 0);
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int i=0;i<NodeList.size();i++) {
			temp.add(-1);
		}
		availableNodes.put(requestID, temp);
		if(hintedoff) {
			for(int i=0;i<NodeList.size();i++) {
				//write thread
				if(hintsMap.get(i) == null && nodeStatus.get(i) == true) {
					updatelog = new UpdateLog(this,key,value,consistencyLevel,currentTS,i,requestID);
					currentThreads.add(updatelog);
					updatelog.start();
				}
				else {
					ArrayList<Integer> temp1 = availableNodes.get(requestID);
					System.out.println("updating :"+getNodeIndex(NodeList.get(i).ip, NodeList.get(i).port));
					System.out.println(temp1);
					temp1.set(getNodeIndex(NodeList.get(i).ip, NodeList.get(i).port), 0);
					availableNodes.put(requestID, temp1);
					HashMap<Integer, DataValue> newMap = null;
					if(hintsMap.containsKey(i)) {
						newMap = hintsMap.get(i);
					}
					else {
						newMap = new HashMap<Integer, DataValue>();
					}			
					newMap.put(key, new DataValue(currentTS, value));
					hintsMap.put(i,newMap);
					System.out.println("Hints Map"+hintsMap);
				}
			}
		}else {
			for(int i=0;i<NodeList.size();i++) {
				//write thread
				updatelog = new UpdateLog(this,key,value,consistencyLevel,currentTS,i,requestID);
				currentThreads.add(updatelog);
				updatelog.start();
			}
		}

		System.out.println("loop completed");
		try {
			currentThreads.get(getCurrentIndex()).join();
			System.out.println("operation completed");
			if(returnWrite.equals("Error")){
				throw new SystemException().setMessage("Write not completed");
			}
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
		return returnWrite;
	}
	


public Map<Integer,DataValue> checkHints(String ip, int port) {
	int index = 0;

	for(int i=0; i<NodeList.size(); i++) {
		if(NodeList.get(i).getIp().equals(ip) && NodeList.get(i).getPort() == port) {
			index = i;
			break;
		}
	}
	Map<Integer,DataValue> tempMap = new HashMap<Integer, DataValue>();
	tempMap.put(-1, new DataValue());
	if(hintsMap.size()>0) {
		tempMap = hintsMap.get(index);
		hintsMap.remove(index);
	}
	return tempMap;
}

public void putInMap(int key, String value, String ts) throws SystemException, TException{
	DataValue dataObj = new DataValue(ts, value);
	KeyValueStoreMap.put(key, dataObj);
}


@Override
public String UpdateReplicas(int key, String value, String ts) throws SystemException, TException {
	//write to log
	writeToLog(key,value,ts);
	//write to map
	putInMap(key, value, ts);
	return "OK";
}

@Override
public DataValue ReadData(int key) throws SystemException, TException {
	// TODO Auto-generated method stub
	//wait for response from number of replicas specified
	//compare timestamp
	//update accordingly
	//call readrepair(key);
	//return value
	System.out.println("In Read Data : " + KeyValueStoreMap.get(key));
	if(null == KeyValueStoreMap.get(key)) {
		return new DataValue("NoValue","");//throw new SystemException().setMessage("No Data");
	} else {
		return KeyValueStoreMap.get(key);
	}
}

@Override
public String ReadThread(int key, int consistencyLevel) throws SystemException, TException {
	// TODO Auto-generated method stub
	//wait for response from number of replicas specified
	//compare timestamp
	//update accordingly
	//call readrepair(key);
	//return value
	
	synchronized(this) {
		if(hintedoff) {
			if(restarted) {
				restarted = false;
				for(int i=0; i<NodeList.size();i++) {
					//if(!NodeList.get(i).ip.equals(IPAddr) && NodeList.get(i).port != port) {
					  if(!(NodeList.get(i).getIp().equals(IPAddr) && NodeList.get(i).getPort() == port)) {	
						try {
							TTransport transport;
							transport = new TSocket(NodeList.get(i).ip, NodeList.get(i).port);
							transport.open();
							TProtocol protocol = new TBinaryProtocol(transport);
							ReplicaServices.Client client = new ReplicaServices.Client(protocol);
							System.out.println("calling checkhints");
							resultMap = client.checkHints(IPAddr,port);
							System.out.println("resultmap"+resultMap);
							transport.close();
							System.out.println("return");
							if(resultMap != null) {
								System.out.println("inside 1");
								for(Integer tempkey : resultMap.keySet()) {
									if(tempkey!=-1 && resultMap.get(tempkey) != null) {
										if(!KeyValueStoreMap.containsKey(tempkey)) {
											writeToLog(tempkey, resultMap.get(tempkey).value, resultMap.get(tempkey).timestamp);
											KeyValueStoreMap.put(tempkey,resultMap.get(tempkey));
										}
										else if(KeyValueStoreMap.get(tempkey).getTimestamp().compareTo(resultMap.get(tempkey).getTimestamp()) < 0) {
											writeToLog(tempkey, resultMap.get(tempkey).value, resultMap.get(tempkey).timestamp);
											KeyValueStoreMap.put(tempkey,resultMap.get(tempkey));
										} 					
									}
								}
								System.out.println("Main map:"+KeyValueStoreMap);
							}
						} catch (TException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
					}
				}
			}

		}
		
		List<ReadLog> currentThreads = new ArrayList<ReadLog>();
		ReadLog readlog = null;
		System.out.println("inside WriteData");
		System.out.println("IP:"+IPAddr);
		System.out.println("port:"+port);
		int requestID = getID();
		consistencyCount.put(requestID, 0);
		ArrayList<Integer> temp = new ArrayList<Integer>();
		for(int i=0;i<NodeList.size();i++) {
			temp.add(-1);
		}
		availableNodes.put(requestID, temp);
		if(hintedoff) {
			for(int i=0;i<NodeList.size();i++) {
				//write thread
				if(hintsMap.get(i) == null && nodeStatus.get(i) == true) {
					readlog = new ReadLog(this,key,consistencyLevel,i,requestID);
					currentThreads.add(readlog);
					readlog.start();
				}
				else {
					ArrayList<Integer> temp1 = availableNodes.get(requestID);
					System.out.println("updating :"+getNodeIndex(NodeList.get(i).ip, NodeList.get(i).port));
					System.out.println(temp1);
					temp1.set(getNodeIndex(NodeList.get(i).ip, NodeList.get(i).port), 0);
					availableNodes.put(requestID, temp1);
				}
			}
		}else {
			for(int i=0;i<NodeList.size();i++) {
			//read thread
			readlog = new ReadLog(this,key,consistencyLevel,i,requestID);
			currentThreads.add(readlog);
			readlog.start();
			}
		}
		try {
			currentThreads.get(getCurrentIndex()).join();
			if(datavalue == null) {
				throw new SystemException().setMessage("Read not completed");
			}else {
				System.out.println("DataValue:"+datavalue.getValue()+datavalue.getTimestamp());
			}
			readList.clear();

		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return datavalue.getValue()+" "+"ts :"+datavalue.getTimestamp();
	}
}

private void writeToLog(int key, String value, String timestamp) {
	System.out.println(IPAddr);
	List<String> fileData = new ArrayList<String>();
	File logFile = new File(IPAddr+port+ ".txt");
	try {
		if(!logFile.exists()){logFile.createNewFile();}
		System.out.println("entry");
		FileWriter fw = new FileWriter(logFile,true);
		BufferedWriter bw = new BufferedWriter(fw);
		bw.write(key+":"+value+":"+timestamp+System.lineSeparator());
		bw.close();
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}


public int getCurrentIndex() {
	int currentIndex = 0;
	for(int i=0;i<NodeList.size();i++) {
		if(NodeList.get(i).getIp().equals(IPAddr) && NodeList.get(i).getPort() == port) {
			currentIndex =  i;
			break;
		}
	}
	return currentIndex;
}

public int getNodeIndex(String ip, int port) {
	int index = 0;
	for(int i=0;i<NodeList.size();i++) {
		if(NodeList.get(i).getIp().equals(ip) && NodeList.get(i).getPort() == port) {
			index =  i;
			break;
		}
	}
	// TODO Auto-generated method stub
	return index;
}



}
