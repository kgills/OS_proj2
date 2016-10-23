import static java.lang.Math.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


/******************************************************************************/
class Application implements Runnable {

    private int d; // Delay between critical section
    private int c; // Delay inside of critical secion
    private int iter; // Number of iterations to make before exiting
    private Protocol p;

    public volatile int csGranted;


    Application(int d, int c, int iter, Protocol p) {
        this.d = d;
        this.c = c;
        this.iter = iter;
        this.p = p;
    }

    // Return an exponential random variable from mean lambda
    public double nextExp(int lambda) {
        return (-lambda)*Math.log(1-Math.random())/Math.log(2);
    }

    public void run() {
        long threadId = Thread.currentThread().getId();

        System.out.println("Application running "+threadId);
        System.out.println("d = "+d);
        System.out.println("c = "+c);
        System.out.println("iter = "+iter);

        while(iter > 0) {

            try {
                Thread.sleep((long)nextExp(d));
            } catch(Exception e) {
                e.printStackTrace();
            }

            p.enterCS();

            try {
                Thread.sleep((long)nextExp(c));
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
class Message {

    // Variables for the messages being passes
    int clock;
    MessageType type;
}

/******************************************************************************/
class Server implements Runnable{

    private Protocol p;

    Server(Protocol p) {
        this.p = p;
    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Server running "+threadId);

        // Listen for messages from other nodes, pass them to the Maekawa class
        Message m = new Message();
        m.clock = 0;
        m.type = MessageType.REQUEST;
        p.putQueue(m);
    }


}

/******************************************************************************/
class Protocol implements Runnable{
    // Execute Maekawa's protocol

    // Class variables
    private int clock;
    private boolean occupied;

    private int pendingRequest;
    private Boolean[] granted;

    // Volatile flags set and cleared by the Protocol and Application
    private volatile int csRequest;
    private volatile int csGrant;
    private volatile int csReleased;
    private volatile int appComplete;

    // Queue used for storing messages sent from the Server to the protocol
    // Should probably use a priority queue to order the messages by their time stamp
    private volatile BlockingQueue<Message> rcvQueue;

    Protocol(int n, int n_i) {
        rcvQueue = new LinkedBlockingQueue<Message>();
        granted = new Boolean[n];
    } 
    // Class methods
    public void enterCS() {
        csRequest = 1;
        while(csGrant == 0) {}

    }

    public void leaveCS() {
        csGrant = 0;
        csReleased = 1;
    }
    
    private void grantCS() {
        csGrant = 1;
    }

    public void appComplete() {
        appComplete = 1;
    }

    public void putQueue(Message m) {
        try {
            rcvQueue.put(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Send message to destination node
    private void sendMessage(Message m, int dest) {

    }

    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);
        csRequest = 0;
        csGrant = 0;
        appComplete = 0;

        while(appComplete == 0) {

            // Wait for the application to request the CS
            while(csRequest == 0) {
                if(appComplete == 1) {
                    // sendMessage COMPLETE to all other quorum members
                    break;
                }
            }

            if(appComplete == 1) {
                break;
            }
            System.out.println("Requesting CS "+threadId);

            // Perform steps for Maekawa's protocol

            // Get latest message from the queue
            if(rcvQueue.peek() != null) {
                Message m = rcvQueue.remove();
                System.out.println("Message type: "+m.type);
            }


            grantCS();

            // Wait for application to release CS
            while(csGrant == 1) {}
            System.out.println("CS Freed "+threadId);

        }


        // Still wondering best way to process messages/application requests
        return;

/*
        while(true) {
            // Perform steps for Maekawa's protocol, loop here until all
            // nodes have completed

            if(csRequest == 1) {
                // Process the CS request from the application
            }

            if(csReleased == 0) {
                // Process the release of the CS
            }
        }
*/
    }
}

/******************************************************************************/
public class Maekawa {

    public static void main(String[] args) {
            System.out.println("*** Maekawa ***");

            // parse the input arguments

            int n = 1;
            int n_i = 0;

            // All we need here are the nodes in our quorum
            // Put the host names into an array or vector of strings
            // Put the ports into an arrary or vector of ints

            // Use the values to test
            int d = 100;
            int c = 100;
            int iter = 10;

            // Start the server and protocol threads
            Protocol prot = new Protocol(n, n_i);
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
