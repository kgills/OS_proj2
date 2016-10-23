import static java.lang.Math.*;
import java.util.concurrent.BlockingQueue;


/******************************************************************************/
class Application implements Runnable {

    private Integer d; // Delay between critical section
    private Integer c; // Delay inside of critical secion
    private Integer iter; // Number of iterations to make before exiting
    private Protocol p;

    public volatile Integer csGranted;


    Application(Integer d, Integer c, Integer iter, Protocol p) {
        this.d = d;
        this.c = c;
        this.iter = iter;
        this.p = p;
    }

    // Return an exponential random variable from mean lambda
    public double nextExp(Integer lambda) {
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

            p.enterCS();;

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
class Message {

    public enum MessageType {
        REQUEST, GRANT, COMPLETE
    }

    // Variables for the messages being passes
    Integer clock;
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
    }


}

/******************************************************************************/
class Protocol implements Runnable{
    // Execute Maekawa's protocol

    // Class variables
    private Integer clock;
    private boolean occupied;

    // Volatile flags set and cleared by the Protocol and Application
    private volatile Integer csRequest;
    private volatile Integer csGrant;
    private volatile Integer complete;

    // Queue used for storing messages sent from the Server to the protocol
    private BlockingQueue<Message> rcvQueue;


    // Class methods
    public synchronized void enterCS() {
        csRequest = 1;
    }

    public synchronized void leaveCS() {
        csGrant = 0;
        csRequest = 0;
    }
    
    public synchronized void grantCS() {
        csGrant = 1;
    }

    public synchronized void appComplete() {
        complete = 1;
    }

    public synchronized void putQueue(Message m) {
        try {
            rcvQueue.put(m);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Send message to destination node
    private sendMessage(Message m, Integer dest) {

    }



    public void run() {
        long threadId = Thread.currentThread().getId();
        System.out.println("Protocol running "+threadId);
        csRequest = 0;
        csGrant = 0;
        complete = 0;

        while(complete == 0) {
            while(csRequest == 0) {
                if(complete == 1) {
                    break;
                }
            }
            System.out.println("Requesting CS");

            // Perform steps for Maekawa's protocol


            grantCS();

            // Wait for application to release CS
            while(csGrant == 1) {
                if(complete == 1) {
                    break;
                }
            }
            System.out.println("CS Freed");

        }
    }
}

/******************************************************************************/
public class Maekawa {

    public static void main(String[] args) {
            System.out.println("*** Maekawa ***");

            // parse the input arguments

            // Use the values to test
            Integer d = 100;
            Integer c = 100;
            Integer iter = 10;

            // Start the server and protocol threads
            Protocol prot = new Protocol();
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
