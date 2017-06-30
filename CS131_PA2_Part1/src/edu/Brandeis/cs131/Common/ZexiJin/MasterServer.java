package edu.Brandeis.cs131.Common.ZexiJin;

import java.util.HashMap;
import java.util.LinkedList;

import edu.Brandeis.cs131.Common.Abstract.Client;
import edu.Brandeis.cs131.Common.Abstract.Log.Log;
import edu.Brandeis.cs131.Common.Abstract.Server;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
/**
 * contains a map of BasicServers, each of which have their own FIFO wait-queue The client in the front of a
 * BasicServer’s wait-queue will attempt to connect to that server, and if unsuccessful wait for a
 * client currently connected to that server to disconnect. 
 * @author JoshJin
 *
 */
public class MasterServer extends Server {

    private final Map<Integer, List<Client>> mapQueues = new HashMap<Integer, List<Client>>();
    private final Map<Integer, Server> mapServers = new HashMap<Integer, Server>();
    //A lock and a condition to make threads awaits. 
	final Lock conditionLock = new ReentrantLock();
	final Condition condition1 = conditionLock.newCondition();
    
    public MasterServer(String name, Collection<Server> servers, Log log) {
        super(name, log);
        Iterator<Server> iter = servers.iterator();
        while (iter.hasNext()) {
            this.addServer(iter.next());
        }
    }

    public void addServer(Server server) {
        int location = mapQueues.size();
        this.mapServers.put(location, server);
        this.mapQueues.put(location, new LinkedList<Client>());
    }

    /**
     * It checks the condition. If the server is not empty or client is not the first one in the list,
     * it waits for its turn at the end of the list. If the list is empty, a client is first entered into
     * the list and then decides what happens next
     */
    @Override
    public boolean connectInner(Client client) {
        // TODO Auto-generated method stub
    	//lock the critical session
    	conditionLock.lock();
    	try {
    		//get temporary variables from the list and everything
    		int serverNum = this.getKey(client);
    		BasicServer server = (BasicServer) mapServers.get(serverNum);
    		LinkedList<Client> clientList = (LinkedList<Client>) mapQueues.get(serverNum);
    		//add the client into corresponding list in all cases
    		clientList.add(client);
    		//main loop always runs
    		while (true) {
    			// case that this client is not the first one, then wait
		    	if (clientList.peek() != client) {
		    		condition1.await();
		    	} 
		    	// client is the first, so we try to put it into server
		    	else {
		    		//use basic server's function to test conditions
		    		//returns false, not pass the conditions, then wait
		    		if (!server.connectInner(client)) {
		    			condition1.await();
		    		} 
		    		//pass the conditions, return true and remove from the list
		    		else {
    					clientList.removeFirst();
    					return true;
		    		}
		    	}	
	    	}
    	} 
    	//catch interruped exception 
    	catch (InterruptedException e) {
    		System.err.println("error message 1");
    	} 
    	//unlock using finally, for it must be carried out
    	finally {
			conditionLock.unlock();
		}
    	//this code is probably never reached
        return false;
    }

    /**
     * the disconnectInner only locks, removes the client by calling basic server's disconnectInner,
     * signals everyone, and finally unlocks
     */
    @Override
    public void disconnectInner(Client client) {
        // TODO Auto-generated method stub
    	//locks first
    	conditionLock.lock();
    	try {
    		//get the server
    		int serverNum = this.getKey(client);
    		BasicServer server = (BasicServer) mapServers.get(serverNum);
    		//disconnect from the server
    		server.disconnectInner(client);
    		//signals all
    		condition1.signalAll();
    	} finally {
    		//finally unlocks everything
			conditionLock.unlock();
		}
    }

	//returns a number from 0- mapServers.size -1
    // MUST be used when calling get() on mapServers or mapQueues
    private int getKey(Client client) {
        return client.getSpeed() % mapServers.size();
    }
}
