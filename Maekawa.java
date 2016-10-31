import static java.lang.Math.*;
import java.util.Comparator;
import java.util.Queue;
import java.util.PriorityQueue;
import java.util.Arrays;
import java.io.*;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer; 
import java.nio.channels.ClosedChannelException;

import com.sun.nio.sctp.MessageInfo;
import com.sun.nio.sctp.SctpChannel;
import com.sun.nio.sctp.SctpServerChannel;


/******************************************************************************/
class Application implements Runnable {

    private int d; // Delay between critical section
    private int c; // Delay inside of critical secion
    private int iter; // Number of iterations to make before exiting
    private int n_i; 
    private Protocol p;

    public volatile int csGranted;


    Application(int d, int c, int iter, Protocol p, int n_i) {
        this.d = d;
        this.c = c;
        this.iter = iter;
        this.p = p;
        this.n_i = n_i;
    }

    // Return an exponential random variable from mean lambda
    private double nextExp(int lambda) {
        return (-lambda)*Math.log(1-Math.random())/Math.log(2);
    }

    private void delay(double usec) {
        long start = System.nanoTime();
        long stop = start + (long)usec*1000;

        while(System.nanoTime() < stop) {}
    }

    public void run() {
        long threadId = Thread.currentThread().getId();

        System.out.println("Application running "+threadId);
        System.out.println("d = "+d);
        System.out.println("c = "+c);
        System.out.println("iter = "+iter);

        while(iter > 0) {

            delay(nextExp(d));

            p.enterCS();

            try {
                FileWriter writer = new FileWriter("Maekawa.txt", true);
                writer.write("Enter CS "+n_i+"\n");
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }


            delay(nextExp(c));

            try {
                FileWriter writer = new FileWriter("Maekawa.txt", true);
                writer.write("Exit CS "+n_i+"\n");
                writer.close();
            } catch(Exception e) {
                e.printStackTrace();
            }


            p.leaveCS();


            iter--;
        }

        // Signal to the protocol thread that the application is complete
        p.appComplete();
    }
}

/******************************************************************************/
enum MessageType {
    REQUEST, GRANT, COMPLETE, RELEASE, INQUIRE, FAILED, YIELD
}

/******************************************************************************/
class Message implements java.io.Serializable{

    // Variables for the messages being passes
    int clock;
    int origin;
    MessageType type;
}

/******************************************************************************/
class Server implements Runnable{

    private Protocol p;
    private int port;
    SctpServerChannel ssc;
    private volatile Boolean closeFlag;

    Server(Protocol p, int port) {
        this.p = p;
        this.port = port;
        closeFlag = false;
    }

    public void closeServer() {
        closeFlag = true;
    }

    public void run() {
        System.out.println("Server running "+port);

        try {
            ssc = SctpServerChannel.open();
            InetSocketAddress serverAddr = new InetSocketAddress(port);
            ssc.bind(serverAddr);            
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Listen for the super thread to close the server
        Thread closeListener = new Thread() {
            public void run(){
                while(true) {
                    if(closeFlag) {
                        try {
                            ssc.close();
                            System.out.println("Closed ssc");
                        } catch(Exception e) {
                            e.printStackTrace();
                        }
                        return;
                    }
                }
            }
        };
        closeListener.start();

        while(true) {

            try {

                SctpChannel sc = ssc.accept();

                if(sc == null) {
                    return;
                }

                Thread clientThread = new Thread() {
                    public void run() {
                        try {
                            // Listen for messages from other nodes, pass them to the Maekawa class
                            byte[] data = new byte[512];
                            ByteBuffer buf = ByteBuffer.wrap(data);

                            sc.receive(buf, null, null);

                            ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
                            ObjectInputStream ois = new ObjectInputStream(bytesIn);
                            Message m = (Message)ois.readObject();
                            ois.close();

                            // Echo back data to ensure FIFO
                            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
                            sc.send(buf, messageInfo);

                            sc.close();

                            p.putQueue(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }

                };
                clientThread.start();

            
            } catch (Exception e) {
                System.out.println("Closing server");
                return;
            }
        }
    }
}

/******************************************************************************/
class Protocol implements Runnable{
    // Execute Maekawa's protocol

    // Class variables
    private int n;
    private int n_i;
    private int quorumSize;
    private int[] quorumMembers;
    private String[] hosts;
    private int[] ports;

    // Variables for running the protocol
    private int clock;
    private Boolean granted;
    private Boolean inquired;
    private Message grantedMessage;

    // Volatile flags set and cleared by the Protocol and Application
    private volatile int csRequest;
    private volatile int csGrant;
    private volatile int appComplete;
    private volatile Boolean everyNodeComplete;

    // Queue used for storing messages sent from the Server to the protocol
    private volatile Queue<Message> receiveQueue;
    private Queue<Message> requestQueue;
    private Boolean[] completeArray;
    private Boolean[] grantArray;
    private Boolean[] failArray;

    public static Comparator<Message> messageComparator = new Comparator<Message>() {
        @Override
        public int compare(Message m1, Message m2) {
            if(m1.clock < m1.clock) {
                return 1;
            } else if (m1.clock > m1.clock) {
                return -1;
            }

            if(m1.origin < m2.origin) {
                return 1;
            }

            return -1;
        }
    };


    Protocol(int n, int n_i, int quorumSize, int[] quorumMembers, String[] hosts, int[] ports) {

        this.n = n;
        this.n_i = n_i;
        this.quorumSize = quorumSize;
        this.quorumMembers = quorumMembers;
        this.hosts = hosts;
        this.ports = ports;

        // Initialize class variables
        clock = 0;
        granted = false;
        inquired = false;
        grantedMessage.clock = 0;
        grantedMessage.origin = 0;

        csRequest = 0;
        csGrant = 0;
        appComplete = 0;
        everyNodeComplete = false;

        receiveQueue = new PriorityQueue<Message>(n, messageComparator);  // Server produces messages, protocol consumes
        requestQueue = new PriorityQueue<Message>(n, messageComparator);  // que the received messages
        completeArray = new Boolean[n];
        for(int i = 0; i < n; i++) {
            completeArray[i] = false;
        }
        grantArray = new Boolean[quorumSize];
        for(int i = 0; i < quorumSize; i++) {
            grantArray[i] = false;
        }
        failArray = new Boolean[quorumSize];
        for(int i = 0; i < quorumSize; i++) {
            failArray[i] = false;
        }
    }

    // Class methods
    public void enterCS() {
        broadcastMessage(MessageType.REQUEST);
        while(csGrant == 0) {}
    }

    public void leaveCS() {
        csGrant = 0;
        granted = false;

        for(int i = 0; i < quorumSize; i++) {
            if(quorumMembers[i] != n_i) {
                grantArray[i] = false;
                sendMessage(MessageType.RELEASE, quorumMembers[i]);
            }
        }
    }

    public void appComplete() {
        appComplete = 1;
    }

    public void putQueue(Message m) {
        try {
            receiveQueue.add(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private byte[] serializeObject(Message m) throws IOException{
        ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bytesOut);
        oos.writeObject(m);
        oos.flush();
        byte[] bytes = bytesOut.toByteArray();
        bytesOut.close();
        oos.close();
        return bytes;
    }

    // Send message to destination node
    private void sendMessage(MessageType type, int dest) {
        Message m = new Message();
        m.type = type;
        m.origin = n_i;
        m.clock = clock;
        clock++;

        try {
            // Send message to dest
            InetSocketAddress serverAddr = new InetSocketAddress(hosts[dest], ports[dest]);
            SctpChannel sc = SctpChannel.open(serverAddr, 0, 0);

            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sc.send(ByteBuffer.wrap(serializeObject(m)), messageInfo);

            // Wait for echo from server
            byte[] data = new byte[512];
            ByteBuffer buf = ByteBuffer.wrap(data);

            sc.receive(buf, null, null);

            sc.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void broadcastMessage(MessageType type) {
        for(int i = 0; i < quorumSize; i++) {
            sendMessage(type, quorumMembers[i]);
        }
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);

        while(true) {

            // Process any received messages
            while(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                // Convert m.origin to the index into our quorum members
                int m_q = 0;
                for(int i = 0; i < quorumSize; i++) {
                    if(m.origin == quorumMembers[i]) {
                        m_q = i;
                    }
                }

                clock++;
                if(m.clock > clock) {
                    clock = m.clock;
                }

                switch(m.type) {
                    case REQUEST:
                        try {
                            requestQueue.add(m);

                            // Message comparator
                            if(granted && (messageComparator.compare(grantedMessage, m) == 1)) {
                                sendMessage(MessageType.FAILED, m.origin);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    break;
                    case GRANT:
                        grantArray[m_q] = true;
                        failArray[m_q] = false;
                    break;
                    case FAILED:
                        failArray[m_q] = true;
                    break;
                    case COMPLETE:
                        completeArray[m.origin] = true;
                    break;
                    case RELEASE:
                        granted = false;
                        // Remove request from our requestQueue
                        // TODO: Remoe request with the smallest timestamp
                        Message[] requests = new Message[requestQueue.size()];
                        requestQueue.toArray(requests);
                        for(int i = 0; i < requests.length; i++) {
                            if(requests[i].origin == m.origin) {
                                requestQueue.remove(requests[i]);
                            }
                        }

                    break;
                    case INQUIRE:
                        // Yield lock if received a failed message
                        // TODO: Need some more logic here
                        for(int i = 0; i < quorumSize; i++) {
                            if(failArray[i] == true) {
                                grantArray[m_q] = false;
                                sendMessage(MessageType.YIELD, m.origin);
                            }
                        }
                    break;
                    case YIELD:
                        inquired = false;

                        // Need to grant to request we inquired about
                        if((requestQueue.peek() != null)) {
                            // Grant the request
                            Message mes = requestQueue.remove();
                            granted = true;
                            grantedMessage.clock = mes.clock;
                            grantedMessage.origin = mes.origin;
                            sendMessage(MessageType.GRANT, m.origin);
                        }

                    break;
                    default:
                    System.out.println("Unknown message received by protocol");
                    return;
                }
            }

            // Process any request messages
            if((requestQueue.peek() != null)) {

                if(granted && !inquired && messageComparator.compare(requestQueue.peek(),grantedMessage)) {
                    // Need to inquire our grant
                    inquired = true;
                    sendMessage(MessageType.INQUIRE, grantedMessage.origin);

                } else if(!granted) {
                    // Grant the request
                    Message m = requestQueue.remove();
                    granted = true;
                    grantedMessage.clock = m.clock;
                    grantedMessage.origin = m.origin;
                    sendMessage(MessageType.GRANT, m.origin);
                }
            }

            // Check to see if we've received all the grant messags
            Boolean allGranted = true;
            for(int i; i < quorumSize; i++) {
                if(grantArray[i] == false) {
                    allGranted = false;
                    break;
                }
            }
            if(allGranted && !granted) {
                granted = true;
                csGrant = 1;
                csRequest = 0;
            }

            // Check for app completion
            if(appComplete == 1) {
                appComplete = 2;

                // Send complete message to all of the nodes
                for(int i = 0; i < n; i++) {
                    if(i != n_i) {
                        sendMessage(MessageType.COMPLETE, i);                        
                    }
                }
                completeArray[n_i]=true;

            }

            // Check to see if all of the nodes have completed
            Boolean allComplete = true;
            for(int i = 0; i < n; i++) {
                if(completeArray[i] == false) {
                    allComplete = false;
                    break;
                }
            }
            if(allComplete) {
                return;
            }
        }
    }
}

/******************************************************************************/
public class Maekawa {

    public static void main(String[] args) {
        System.out.println("*** Maekawa ***");

        // parse the input arguments
        // n n_i d c iter hostname[0] port[0] ... quorumSize q[0] ...

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
        int[] ports = new int[n];
        System.out.println("Nodes:");
        int i;
        for(i = 0; i < n; i++) {
            hostnames[i] = args[2*i + 5];
            ports[i] = Integer.parseInt(args[2*i + 5 + 1]);

            System.out.println(hostnames[i]+" "+ports[i]);
        }

        int quorumSize = Integer.parseInt(args[2*i+5]);
        System.out.println("quorumSize: "+quorumSize);
        System.out.println("quorumMembers:");
        int saved_i = 2*i+5+1;
        int[] quorumMembers = new int[quorumSize];
        for(i = 0; i < quorumSize; i++) {
            quorumMembers[i] = Integer.parseInt(args[saved_i+i]);
            System.out.println(quorumMembers[i]);
        }

        // Sort the quorum members array
        Arrays.sort(quorumMembers);

        // Start the server and protocol threads
        Protocol prot = new Protocol(n, n_i, quorumSize, quorumMembers, hostnames, ports);
        Thread protocol_thread = new Thread(prot);
        protocol_thread.start();

        Server server = new Server(prot, ports[n_i]);
        Thread server_thread = new Thread(server);
        server_thread.start();

        // Wait 5 secodns for the applications to start
        try {
            Thread.sleep(5000);
        } catch(Exception e) {
            e.printStackTrace();
        }

        Thread app_thread = new Thread(new Application(d, c, iter, prot, n_i));
        app_thread.start();

        // Wait for all of the threads to exit
        try {
            app_thread.join();
            protocol_thread.join();
            server.closeServer();
            server_thread.join();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
