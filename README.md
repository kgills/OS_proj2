# OS_proj2

## Description


public class application {
	
	@Override
	run()  {

		while(iter--) {
			delay
			csEnter
			// Write to the output file


			delay


			// Write to the output file
			// <real_time> enterCS <node number> <scalar_clock> 
			csExit
		}

	}
}

public class message {
	// Message type
	// Clock value
	// Sender node
}

public class server {
		
	// Listening for messages from other nodes
	// Premption, hold and wait, asking for permission from other quorum members in certain order

	@override
	run() {
		// Listen for messages
		// Parse and send to the protocol object

		// On new message send to protocol class
		parse_message();
	}

}

public class protocol {

	private integer clock;
	private boolean occupied;

	public csEnter {
		// Send request messages to other quorum members
	}

	public csExit {
		// Send release messages to other quorum members
	}

	public releaseMessage {

	}

	public grantMessage {

	}

	// Method for each message type

	public parse_message {
		// 
	}


	main {

		// Spawn these threads
		applicaiton.run()
		server.run()
	}
}
</code>


