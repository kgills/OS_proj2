import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;


/******************************************************************************/
class Application implements Runnable {

	private int d; // Delay between critical section
	private int c; // Delay inside of critical secion
	private int iter; // Number of iterations to make before exiting
	private Protocol p;

	//public volatile int csGranted;


	Application(int d, int c, int iter, Protocol p) {
		this.d = d;
		this.c = c;
		this.iter = iter;
		this.p = p;
	}

	// Return an exponential random variable from mean lambda
	private double nextExp(int lambda) {
		return (-lambda)*Math.log(1-Math.random())/Math.log(2);
	}

	public void run() {
		//long threadId = Thread.currentThread().getId();

		/*System.out.println("Application running "+threadId);
		System.out.println("d = "+d);
		System.out.println("c = "+c);
		System.out.println("iter = "+iter);*/

		while(iter > 0) {

			System.out.println("ID:"+p.getID()+" Iteration:"+iter);
			try {
				Thread.sleep((long)nextExp(d));
			} catch(Exception e) {
				e.printStackTrace();
			}

			System.out.println("ID:"+p.getID()+"Before Entering");
			p.enterCS();

			try {
				FileWriter writer = new FileWriter("Maekawa.txt", true);
				System.out.println("ID:"+p.getID()+"Entering entry");
				writer.write("Enter CS "+p.getID()+"\n");
				writer.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println("ID:"+p.getID()+"Waiting in critical section");
			try {
				Thread.sleep((long)nextExp(c));
			} catch(Exception e) {
				e.printStackTrace();
			}

			try {
				FileWriter writer = new FileWriter("Maekawa.txt", true);
				System.out.println("ID:"+p.getID()+"Leaving entry");
				writer.write("Exit CS "+p.getID()+"\n");
				writer.close();
			} catch(Exception e) {
				e.printStackTrace();
			}

			p.leaveCS();
			System.out.println("ID:"+p.getID()+"After Leaving");
			iter--;
		}

		// Signal to the protocol thread that the application is complete
		p.appComplete();
	}
}

/******************************************************************************/
enum MessageType {
	REQUEST, GRANT, COMPLETE, RELEASE
}

/******************************************************************************/
class Message implements Serializable {

	// Variables for the messages being passes
	int sender;
	int clock;
	MessageType type;

	Message(int sender, int clock, MessageType type) {
		this.sender = sender;
		this.clock = clock;
		this.type = type;
	}
}

/******************************************************************************/
class Server implements Runnable{

	private Protocol p;

	Server(Protocol p) {
		this.p = p;
	}

	public void run() {
		//long threadId = Thread.currentThread().getId();
		//System.out.println("Server running "+threadId);

		// Listen for messages from other nodes, pass them to the Maekawa class
		try {
			SocketAddress serverSocketAddress = new InetSocketAddress(Integer.parseInt(p.getPort()));         
			SctpServerChannel sctpServerChannel = SctpServerChannel.open().bind(serverSocketAddress); 
			SctpChannel sctpChannel;

			while ((sctpChannel = sctpServerChannel.accept()) != null) { 
				ServerThread t= new ServerThread(sctpChannel, p);
				new Thread(t).start();

			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}


		// Testing the message sending
		/*Message m = new Message();
        m.clock = 0;
        m.type = MessageType.REQUEST;
        p.putQueue(m);*/
	}
}

/******************************************************************************/
class ServerThread implements Runnable {

	SctpChannel sctpChannel; 
	Protocol p;

	public ServerThread(SctpChannel sctpChannel, Protocol p) {
		this.sctpChannel = sctpChannel;
		this.p = p;
	}
	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			ByteBuffer dst = ByteBuffer.allocate(64000);
			sctpChannel.receive(dst , null, null); 
			byte[] message = dst.array();
			ByteArrayInputStream b = new ByteArrayInputStream(message);
			ObjectInputStream o = new ObjectInputStream(b);
			Message m = (Message)o.readObject();
			// Increment clock upon receiving message.
			if(m.clock > p.getClock())
				p.setClock(m.clock+1);
			else
				p.setClock(p.getClock()+1);

			if(m.type == MessageType.GRANT) {
				System.out.println("Received-"+"Origin:"+m.sender+" received at:"+p.getID()+" type"+m.type);
				/*if(p.getID()==2)
					System.out.println("Iter"+p.getIter()+"-------------------------------------------------");*/
				p.incrementGrantCount();
			}
			else if(m.type == MessageType.RELEASE) {
				System.out.println("Received-"+"Origin:"+m.sender+" received at:"+p.getID()+" type"+m.type);
				p.setKeyGranted(0);
			}
			else {
				System.out.println("Received-"+"Origin:"+m.sender+" received at:"+p.getID()+" type"+m.type);
				p.putQueue(m);
			}
		} catch(Exception e) {
			e.printStackTrace();
		}

	}	
} 

/******************************************************************************/
class Protocol implements Runnable{
	// Execute Maekawa's protocol

	// Class variables
	//TODO Added ID, HashMap
	private int n;
	private int ID;
	private int clock;
	private int grantCount;
	private int iter;
	private int keyGranted;
	private int totalCompleted;
	private boolean ownRequestReceived;
	private int[] quorumMembers;
	private String[] hostnames;
	private String[] ports;
	private boolean[] completeArray;

	//hostnames[quoruMembers[0]];

	//private HashMap<Integer, >

	// Volatile flags set and cleared by the Protocol and Application
	private volatile int csRequest;
	private volatile int csGrant;
	private volatile int appComplete;


	// Queue used for storing messages sent from the Server to the protocol
	// TODO Shouldn't this be thread safe?
	private volatile ConcurrentLinkedQueue<Message> rcvQueue;

	Protocol(int n, int n_i, String[] hostnames, String[] ports, int[] quorumMembers) {

		// Initialize class variables
		this.n = n;
		ID = n_i;
		rcvQueue = new ConcurrentLinkedQueue<Message>();
		clock = 0;
		grantCount = 0;
		iter = -1;
		csRequest = 0;
		csGrant = 0;
		appComplete = 0;
		keyGranted = 0;
		ownRequestReceived=false;
		totalCompleted = 0;
		//TODO Change the length of these arrays
		this.quorumMembers= quorumMembers;
		Arrays.sort(quorumMembers);
		this.hostnames = hostnames;
		this.ports = ports;
		completeArray = new boolean[hostnames.length];

	}

	// Class methods

	public int getID(){
		return ID;
	}

	public String getPort() {
		return ports[ID];
	}

	public void setKeyGranted(int i) {
		keyGranted = 0;
	}

	public int getIter() {
		return iter;
	}
	
	public void enterCS() {
		setGrantCount();
		iter = -1;
		csRequest = 1;
		Message m = new Message(ID, clock, MessageType.REQUEST);
		/*if(ID==2 || ID==1)
			System.out.println("Request added to queue for ID:"+ID+"***************************************************");*/
		rcvQueue.add(m);
		//System.out.println("PID:"+Thread.currentThread().getId()+"csRequest "+csRequest);
		//System.out.println("ID:"+ID+"csGrant"+csGrant);
		while(csGrant == 0) {}
	}

	public void leaveCS() {
		csRequest = 0;
		csGrant = 0;

		clock++;
		Message release = new Message(ID, clock, MessageType.RELEASE);
		for(int i=0;i<quorumMembers.length;i++) {
			sendMessage(release, quorumMembers[i]);
		}
	}

	private void grantCS() {
		csGrant = 1;
	}

	public void appComplete() {
		//appComplete = 1;
		//totalCompleted++;
		completeArray[ID] = true;
		clock++;
		Message complete = new Message(ID, clock, MessageType.COMPLETE);
		for(int i=0;i<hostnames.length;i++) {
			sendMessage(complete, i);
		}
	}

	public void putQueue(Message m) {
		try {
			rcvQueue.add(m);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void setGrantCount() {
		grantCount = 0;
	}

	public void incrementGrantCount() {
		grantCount++;
	}

	public int getClock() {
		return clock;
	}

	public void setClock(int i) {
		clock = i;
	}
	// Send message to destination node
	private void sendMessage(Message m, int dest) {

		try {
			System.out.println("Sending:"+"Origin:"+ ID+ " Destination:"+dest+" type:"+m.type);
			SocketAddress socketAddress = new InetSocketAddress( hostnames[dest], Integer.parseInt(ports[dest])); 
			SctpChannel sctpChannel = SctpChannel.open(socketAddress, 0, 0); 

			ByteBuffer byteBuffer = ByteBuffer.allocate(64000); 
			ByteArrayOutputStream b= new ByteArrayOutputStream();
			ObjectOutputStream o = new ObjectOutputStream(b);
			o.writeObject(m);
			byte[] message = b.toByteArray();

			MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0); 
			byteBuffer.put(message); 
			byteBuffer.flip();

			try { 
				sctpChannel.send(byteBuffer, messageInfo); 
			} catch (Exception e) { 
				e.printStackTrace(); 
			}  
			sctpChannel.close();
		}
		catch(Exception e) {
			e.printStackTrace();
		}

	}

	public void run() {

		while(totalCompleted < n) {

			Message m = rcvQueue.peek();
			if((m!=null && m.type == MessageType.REQUEST) || (ownRequestReceived && iter < grantCount)) {
				
				if((m!=null && m.sender == ID) || (ownRequestReceived && iter < grantCount)) {
					
					/*if(m!=null && m.sender == ID &&ID==2)
						System.out.println("Processing own REQUEST from ID:"+ID+"***************************************************");*/
					if(!ownRequestReceived) {
						rcvQueue.poll();
						ownRequestReceived = true;
					}
					
					if(iter < grantCount && grantCount < quorumMembers.length) {
						//iter++;
						clock++;
						/*if(ID==2) {
							System.out.println("iter < grandCount and so inside this loop+++++++++++++++++++++++++++++++++++++++++++++");
							System.out.println("quorumMember:"+quorumMembers[iter+1]+" iter:"+(iter++)+" keyGranted"+keyGranted);
						}*/
						if(quorumMembers[iter+1]==ID && keyGranted==0) {
							iter++;
							keyGranted = 1;
							/*if(ID==2)
								System.out.println("Sending GRANT to own request from ID:"+ID+"--------------------------------------------");*/
							Message grant= new Message(ID, clock, MessageType.GRANT);
							sendMessage(grant, quorumMembers[iter]);
						}
						else if(quorumMembers[iter+1]!=ID) {
							iter++;
							Message request= new Message(ID, clock, MessageType.REQUEST);
							sendMessage(request, quorumMembers[iter]);
						}
					}
				}
				else if(keyGranted==0) {
					keyGranted = 1;
					rcvQueue.poll();
					clock++;
					Message grant= new Message(ID, clock, MessageType.GRANT);
					sendMessage(grant, m.sender);
				}
			}
			else if(m!=null && m.type == MessageType.COMPLETE) {
				totalCompleted++;
				rcvQueue.poll();
				completeArray[m.sender]=true;
			}

			if(grantCount == quorumMembers.length) {
				setGrantCount();
				ownRequestReceived = false;
				grantCS();
			}
		}
	}
}

/******************************************************************************/
public class Maekawa {

	public static void main(String[] args) throws IOException {
		System.out.println("*** Maekawa ***");

		// parse the input arguments
		// n n_i d c iter hostname[0] port[0] ... q_size q[0] ...

		int n = Integer.parseInt(args[0]);
		int n_i = Integer.parseInt(args[1]);
		int d = Integer.parseInt(args[2]);
		int c = Integer.parseInt(args[3]);
		int iter = Integer.parseInt(args[4]);

		System.out.println("n: "+n);
		System.out.println("n_i: "+n_i);
		System.out.println("d: "+d);
		System.out.println("c: "+c);
		System.out.println("iter: "+iter);

		String[] hostnames = new String[n];
		String[] ports = new String[n];
		System.out.println("Nodes:");
		int i;
		for(i = 0; i < n; i++) {
			hostnames[i] = args[2*i + 5];
			ports[i] = args[2*i + 5 + 1];

			System.out.println(hostnames[i]+" "+ports[i]);
		}

		int q_size = Integer.parseInt(args[2*i+5]);
		System.out.println("q_size: "+q_size);
		System.out.println("q_members:");
		int saved_i = 2*i+5+1;
		int[] q_members = new int[q_size];
		for(i = 0; i < q_size; i++) {
			q_members[i] = Integer.parseInt(args[saved_i+i]);
			System.out.println(q_members[i]);
		}

		// Start the server and protocol threads
		//TODO remove loop when done
		for(int x=0;x<ports.length;x++)
			ports[x]= Integer.toString(Integer.parseInt(ports[x])+66); 
		Protocol prot = new Protocol(n, n_i, hostnames, ports, q_members);
		
		FileWriter writer = new FileWriter("Maekawa.txt", false);
		writer.write("");
		writer.close();
		
		Thread protocol_thread = new Thread(prot);
		protocol_thread.start();

		Thread server_thread = new Thread(new Server(prot));
		server_thread.start();

		// Wait 5 secodns for the applications to start
		try {
			Thread.sleep(5000);
		} catch(Exception e) {
			e.printStackTrace();
		}

		Thread app_thread = new Thread(new Application(d, c, iter, prot));
		app_thread.start();

		// Wait for all of the threads to exit
		try {
			app_thread.join();
			protocol_thread.join();
			server_thread.join();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
}
