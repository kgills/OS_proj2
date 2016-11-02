import static java.lang.Math.*;
import java.util.Comparator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.PriorityQueue;
import java.util.Arrays;
import java.util.Iterator;
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
        System.out.println("Application init");
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
            System.out.println(n_i+" In CS");

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


            System.out.println(n_i+" Out CS");

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
class ServerHandler implements Runnable{

    Protocol p;
    SctpChannel sc;

    ServerHandler(Protocol p, SctpChannel sc) {
        this.p = p;
        this.sc = sc;
    }

    public void run() {
        try {
            // Listen for messages from other nodes, pass them to the Maekawa class
            byte[] data = new byte[256];
            ByteBuffer buf = ByteBuffer.wrap(data);

            sc.receive(buf, null, null);

            ByteArrayInputStream bytesIn = new ByteArrayInputStream(data);
            ObjectInputStream ois = new ObjectInputStream(bytesIn);
            Message m = (Message)ois.readObject();
            ois.close();

            System.out.println("RX "+m.type+" from "+m.origin+" "+m.clock);

            // Echo back data to ensure FIFO
            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sc.send(buf, messageInfo);

            sc.close();

            p.putQueue(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/******************************************************************************/
class Server implements Runnable{

    private Protocol p;
    private int port;
    private SctpServerChannel ssc;
    private SctpChannel sc;
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

                sc = ssc.accept();

                if(sc == null) {
                    return;
                }

                // Start a ServerHandler thread
                ServerHandler serverHandler = new ServerHandler(p, sc);
                Thread serverHandler_thread = new Thread(serverHandler);
                serverHandler_thread.start();

            
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
    private Boolean granted;
    private Boolean[] failArray;
    private Boolean[] inquireArray;
    private Boolean inquired;




    private int loopcount;

    public static Comparator<Message> messageComparator = new Comparator<Message>() {
        @Override
        public int compare(Message m2, Message m1) {
            if(m1.clock < m2.clock) {
                return 1;
            } 
            if (m1.clock > m2.clock) {
                return -1;
            }
            if(m1.origin < m2.origin) {
                return 1;
            } 
            if(m1.origin == m2.origin) {
                return 0;
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
        grantedMessage = new Message();
        grantedMessage.clock = 0;
        grantedMessage.origin = 0;

        csRequest = 0;
        csGrant = 0;
        appComplete = 0;
        everyNodeComplete = false;

        receiveQueue = new LinkedBlockingQueue<Message>();  // Server produces messages, protocol consumes
        requestQueue = new PriorityQueue<Message>(n, messageComparator);  // que the received messages
        completeArray = new Boolean[n];
        for(int i = 0; i < n; i++) {
            completeArray[i] = false;
        }
        grantArray = new Boolean[quorumSize];
        for(int i = 0; i < quorumSize; i++) {
            grantArray[i] = false;
        }
        granted = false;

        failArray = new Boolean[quorumSize];
        for(int i = 0; i < quorumSize; i++) {
            failArray[i] = false;
        }
        inquireArray = new Boolean[n];
        for(int i = 0; i < n; i++) {
            inquireArray[i] = false;
        }
        inquired = false;


        loopcount = 0;
    }

    // Class methods
    public void enterCS() {
        csRequest = 1;
        broadcastMessage(MessageType.REQUEST);
        while(csGrant == 0) {}
    }

    public void leaveCS() {

        csGrant = 0;
        for(int i = 0; i < grantArray.length; i++) {
            grantArray[i] = false;
        }
        broadcastMessage(MessageType.RELEASE);

    }

    public void appComplete() {
        appComplete = 1;
    }

    public synchronized void putQueue(Message m) {
        try {;
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
        m.clock = clock++;

        try {

            System.out.println(n_i+" TX "+m.type+" to "+dest+" "+m.clock);

            // Send message to dest
            InetSocketAddress serverAddr = new InetSocketAddress(hosts[dest], ports[dest]);
            SctpChannel sc = SctpChannel.open(serverAddr, 0, 0);

            MessageInfo messageInfo = MessageInfo.createOutgoing(null, 0);
            sc.send(ByteBuffer.wrap(serializeObject(m)), messageInfo);

            // Wait for echo from server
            byte[] data = new byte[256];
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
                int m_q = -1;
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
                            if(granted && (messageComparator.compare(grantedMessage, m) == -1)) {
                                System.out.println("Transmitting failed");
                                System.out.println(m.origin+" "+grantedMessage.origin);
                                System.out.println(m.clock+" "+grantedMessage.clock);
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
                        for(int i = 0; i < n; i++) {
                            if(inquireArray[i] == true) {
                                // grantArray[m_q] = false;
                                sendMessage(MessageType.YIELD, i);
                            }
                        }
                    break;
                    case COMPLETE:
                        completeArray[m.origin] = true;
                    break;
                    case RELEASE:
                        granted = false;

                        Iterator queIterator = requestQueue.iterator();

                        while(queIterator.hasNext()) {
                            Message queM = (Message)queIterator.next();
                            if(queM.origin == m.origin) {
                                requestQueue.remove(queM);
                                break;
                            }
                        }

                    break;
                    case INQUIRE:
                        // Yield lock if received a failed message
                        // TODO: Need some more logic here
                        inquireArray[m.origin] = true;
                        for(int i = 0; i < quorumSize; i++) {
                            if(failArray[i] == true) {
                                // grantArray[m.origin] = false;
                                // granted = true;
                                sendMessage(MessageType.YIELD, m.origin);
                            }
                        }
                    break;
                    case YIELD:
                        inquired = false;

                        // Need to grant to request we inquired about
                        if((requestQueue.peek() != null)) {
                            // Grant the request
                            Message mes = requestQueue.peek();
                            granted = true;
                            grantedMessage.clock = mes.clock;
                            grantedMessage.origin = mes.origin;
                            sendMessage(MessageType.GRANT, mes.origin);
                        } else {
                            System.out.println("Error, Received yelid without any request");
                        }

                    break;
                    default:
                    System.out.println("Unknown message received by protocol");
                    return;
                }
            }

            // Process any request messages
            Message peeked = requestQueue.peek();
            if((peeked != null)) {

                if(granted && !inquired && (messageComparator.compare(peeked,grantedMessage) == -1)) {
                    // Need to inquire our grant

                    System.out.println(peeked.origin+" "+grantedMessage.origin);
                    System.out.println(peeked.clock+" "+grantedMessage.clock);

                    inquired = true;
                    sendMessage(MessageType.INQUIRE, grantedMessage.origin);

                    System.out.println("*******************************************************");

                    // TODO: Do we need to cancel the inquire once we recieve a FAILED message

                } else if(!granted) {
                    // Grant the request
                    granted = true;
                    grantedMessage.clock = peeked.clock;
                    grantedMessage.origin = peeked.origin;
                    sendMessage(MessageType.GRANT, peeked.origin);
                } else {
                    loopcount++;
                }

                if(loopcount == 1000000000) {
                    loopcount = 0;
                    System.out.println("Peeked "+peeked.clock+" "+peeked.origin);
                    System.out.println("grantedMessage "+grantedMessage.clock+" "+grantedMessage.origin);
                    System.out.println("granted "+granted);
                    System.out.println("inquired "+inquired);
                    System.out.println("comparator "+messageComparator.compare(peeked,grantedMessage));

                    Iterator queIterator = requestQueue.iterator();

                    while(queIterator.hasNext()) {
                        Message queM = (Message)queIterator.next();
                        System.out.println("queM "+queM.clock+" "+queM.origin);
                    }
                }
            }

            // Check to see if we've received all the grant messags
            if(csRequest == 1) {
                Boolean allGranted = true;
                for(int i = 0; i < quorumSize; i++) {
                    if(grantArray[i] == false) {
                        allGranted = false;
                        break;
                    }
                }
                if(allGranted) {
                    csGrant = 1;
                    csRequest = 0;
                }
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
            Thread.sleep(3000);
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
