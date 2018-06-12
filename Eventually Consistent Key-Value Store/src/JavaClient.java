import org.apache.thrift.TException;

import org.apache.thrift.transport.TSSLTransportFactory;
import org.apache.thrift.transport.TTransport;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TSSLTransportFactory.TSSLTransportParameters;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

import java.util.Scanner;

public class JavaClient {

	static TTransport transport;
	public static void main(String [] args) {
		readCommandLine();
	}

	private static void readCommandLine() {

		Scanner scanner = new Scanner(System.in);
		while(true) {
		System.out.println("Enter IP address and port number of coordinator (Usage: [ip] [port]): ");
		String coordinator = scanner.nextLine();
		String details[] = coordinator.split(" ");
			try {
				transport = new TSocket(details[0], Integer.valueOf(details[1]));
				transport.open();    	  
				TProtocol protocol = new  TBinaryProtocol(transport);
				ReplicaServices.Client client = new ReplicaServices.Client(protocol);
				while (true) {
					try {
						System.out.println("Enter operation (Usage: [read/write] [key] [value if write] [consistency_level]): ");
						String input = scanner.nextLine();
						System.out.println(input);

						String splitstr[] = input.split(" ");

						if(splitstr[0].equalsIgnoreCase("read")) {
							if(splitstr.length != 3) {
								System.out.println("Invalid input.");
							}
							else {
								String value = "";
								value = client.ReadThread(Integer.valueOf(splitstr[1]), Integer.valueOf(splitstr[2]));
								System.out.println("File Read:"+value);
							}
						}
						else if(splitstr[0].equalsIgnoreCase("write")){
							if(splitstr.length != 4) {
								System.out.println("Invalid input.");
							}
							else {
								String value = client.WriteData(Integer.valueOf(splitstr[1]), splitstr[2], Integer.valueOf(splitstr[3]));
								System.out.println(value);
								System.out.println("File written");
							}
						}
						else {
							System.out.println("Invalid input.");
						}
					} catch(TException x) {
						x.printStackTrace();
					}
				}
			} catch (TException x) {
				x.printStackTrace();
				transport.close();
			} 
		}
	}
}
