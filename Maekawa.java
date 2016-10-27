import static java.lang.Math.*;
import java.util.concurrent.ConcurrentLinkedQueue;
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
    REQUEST, GRANT, COMPLETE, RELEASE
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
    private int grantCount;
    private int currentRequest;
    private Boolean granted;

    // Volatile flags set and cleared by the Protocol and Application
    private volatile int csRequest;
    private volatile int csGrant;
    private volatile int appComplete;
    private volatile Boolean everyNodeComplete;

    // Queue used for storing messages sent from the Server to the protocol
    private volatile ConcurrentLinkedQueue<Message> receiveQueue;
    private ConcurrentLinkedQueue<Message> requestQueue;
    private Boolean[] completeArray;

    Protocol(int n, int n_i, int quorumSize, int[] quorumMembers, String[] hosts, int[] ports) {

        this.n = n;
        this.n_i = n_i;
        this.quorumSize = quorumSize;
        this.quorumMembers = quorumMembers;
        this.hosts = hosts;
        this.ports = ports;

        // Initialize class variables
        clock = 0;
        grantCount = 0;
        currentRequest = 0;
        granted = false;

        csRequest = 0;
        csGrant = 0;
        appComplete = 0;
        everyNodeComplete = false;

        receiveQueue = new ConcurrentLinkedQueue<Message>();  // Server produces messages, protocol consumes
        requestQueue = new ConcurrentLinkedQueue<Message>();  // que the received messages
        completeArray = new Boolean[n];
        for(int i = 0; i < n; i++) {
            completeArray[i] = false;
        }
    }

    // Class methods
    public void enterCS() {
        csRequest = 1;
        while(csGrant == 0) {}
    }

    public void leaveCS() {
        csGrant = 0;
        granted = false;

        for(int i = 0; i < quorumSize; i++) {
            if(quorumMembers[i] != n_i) {
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

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);

        while(true) {
            if((csRequest == 1) && (currentRequest == grantCount)) {

                // Process the CS request from the application
                if((quorumMembers[currentRequest] == n_i) && (!granted)) {
                    granted = true;
                    currentRequest++;
                    grantCount++;
                } else if(quorumMembers[currentRequest] != n_i) {
                   // Send request to next node
                    sendMessage(MessageType.REQUEST, quorumMembers[currentRequest++]); 
                }
            }

            // Process any received messages
            while(receiveQueue.peek() != null) {
                Message m = receiveQueue.remove();

                switch(m.type) {
                    case REQUEST:
                        try {
                            requestQueue.add(m);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    break;
                    case GRANT:
                        grantCount++;
                    break;
                    case COMPLETE:
                        completeArray[m.origin] = true;
                    break;
                    case RELEASE:
                        granted = false;
                    break;
                    default:
                    System.out.println("Unknown message received by protocol");
                    return;
                }
            }

            // Process any request messages
            if((!granted) && (requestQueue.peek() != null)) {
                Message m = requestQueue.remove();
                granted = true;
                sendMessage(MessageType.GRANT, m.origin);
            }

            if(grantCount == quorumSize) {
                // Grant the CS to the application
                granted = true;
                csGrant = 1;
                csRequest = 0;
                grantCount = 0;
                currentRequest = 0;
            }

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
