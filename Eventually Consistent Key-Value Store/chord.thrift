
exception SystemException {
  1: optional string message
}

struct NodeID {
  1: string ip;
  2: i32 port;
}

struct DataValue {
 1: string timestamp;
 2: string value;
}

service ReplicaServices {
  string WriteData(1: i32 key, 2: string value, 3: i32 consistencyLevel)
    throws (1: SystemException systemException),
  
  string UpdateReplicas(1: i32 key, 2: string value, 3: string ts)
    throws (1: SystemException systemException),

  DataValue ReadData(1: i32 key)
    throws (1: SystemException systemException),

  string ReadThread(1: i32 key, 2: i32 consistencyLevel)
    throws (1: SystemException systemException),
  
  map<i32,DataValue> checkHints(1: string ip, 2: i32 port)
	throws (1: SystemException systemException),
	
  void deleteData (1: string record)
    throws (1: SystemException systemException),
}
