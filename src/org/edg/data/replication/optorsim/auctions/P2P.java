package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.GridSite;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.*;

/**
 * Class P2P represents the P2P Mediator, a thread running on each
 * site of the Grid able to "listen" to messages coming from both remote
 * and local sites and to process them. This means that it has a
 * queue, where such messages are stored, and that it uses one specific
 * handler to cope with what is required by every specific kind of
 * message it can receive.
 * <p>
 * By "local site" we mean the site on which the particular instance
 * of P2P runs its thread.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */ 

public class P2P extends Thread {

    // Set of SBs associated with the SEs present on the local site
    private Vector _storageBrokerCollection;
	
    //Queue of messages to be processed
    private List _messageQueue = new Vector();
    
    // Register of Auctions that the P2P has already dealt with (sync on _auctionContact)
    private Set _auctionContacts = Collections.synchronizedSet( new HashSet());

    //While this flag is set true the P2P is "alive". When the flag is turned to
    // false the P2P shuts down
    private boolean _iAmAlive = true;

    //The GridSite on which the P2P runs
    private GridSite _site;

    private GridTime _time = GridTimeFactory.getGridTime();

    /**
     * Creates a new P2P within OptorSim.
     * @param site the GridSite on which the P2P has to run.
     */ 
    public P2P(GridSite site) {
        super(GridTimeFactory.getThreadGroup(), "P2P@"+site);
		_site = site;

		P2PManager.register( _site, this);
		
		int sbId = 0;
		_storageBrokerCollection = new Vector();
		for( Enumeration en = _site.getSEEnum(); en.hasMoreElements();) {
			StorageElement se = (StorageElement) en.nextElement();
			StorageBroker sb = new StorageBroker( sbId++, se, this);
			_storageBrokerCollection.add(sb);
		}
		
		this.start();
    }

    /**
     * Makes the thread wait until a message is received.
     */
    private synchronized void standByForMessage() {

    	synchronized( _messageQueue) {
            // If there are messages to process, return immediately.
    		if( _messageQueue.size() != 0)
    			return;
        }
        _time.gtWait(this);
    }

    /**
     * Adds a Message to the queue and notifies it. This method is
     * usually called within other threads.
     * @param message the Message to be added to the queue.
     */
    protected synchronized void acceptMessage(Message message) {

		synchronized( _messageQueue) {
	    	_messageQueue.add(message);
        }
        _time.gtNotify(this);
    }


    /**
     * The run() method of the thread. It waits until any Message is
     * received and then processes the messages in the queue until
     * there are none left.
     */   
    public void run() {
		for( Enumeration en = _storageBrokerCollection.elements(); en.hasMoreElements();) {
	    	StorageBroker sb = (StorageBroker)en.nextElement();
	    	sb.start();
		}

		List msgToProcess = new LinkedList();

		while(_iAmAlive) {
	    	standByForMessage();
	    
	    		// Empty the messageQueue as quickly as possible.  We process these messages in a bit
		    synchronized( _messageQueue) {
			    while(!_messageQueue.isEmpty()) {
					Message message = (Message)_messageQueue.get(0);
				    _messageQueue.remove(message);

					msgToProcess.add( message);				    
		    	}
			}
			
				// Process all messages.  This might take time, so we do it outside the messageQueue monitor
			for( Iterator i = msgToProcess.iterator(); i.hasNext();) {
				Message msg = (Message) i.next();
				msg.handler( this);
			}
			
			msgToProcess.clear();
		} // while
    }


    /**
     * Sends a Message to another P2P.  
     * @param message the Message to be sent;
     * @param site the site to which the message should be sent.
     */
    protected void sendMessage( Message message, GridSite site) {
		P2P p2pAddressee  = P2PManager.find( site);
		try {
	    	p2pAddressee.acceptMessage(message);
		}
		catch (Exception e) {
            e.printStackTrace();
		}
    }


	/**
	 * Check if we have already seen a particular auction.  If not, mark it as seen and return false
	 * @param auction The auction to check
	 * @return true if auction has already been processed, false if auction is new to this P2P
	 */
    public boolean alreadyProcessed( Auction auction) {  

    	synchronized( _auctionContacts) {  	
			if( _auctionContacts.contains(auction))
				return true;
			_auctionContacts.add( auction);
    	}
    	
		return false;
    }

    /**
     * Remove an Auction from the register of auctions which 
     * this P2P hss processed.
     * @param auction The Auction which should be removed.
     */
    public void removeContact( Auction auction) {
    	synchronized( _auctionContacts) {
			_auctionContacts.remove( auction);
    	}
    }

    /***************************************************************************/

    /**
     * Returns the set of SBs of the local site.
     */
    private Enumeration getStorageBrokers(){
		return _storageBrokerCollection.elements();
    }

    /**
     * Returns the GridSite on which this P2P runs.
     */
    public GridSite getSite() {
		return _site;
    }
	
    /**
     * Returns the name of this P2P.
     */
    public String toString() {
		return getName();
    }

	/**
	 * Sends a message to all local SBs, possibly including the one that originated the auction
	 * @param msg The message to propagate
	 * @param includeOriginator whether to include the SB that originated the auction.
	 */	
	public void sendToLocalSBs( Message msg, boolean includeOriginator) {
		Auctioneer origAuctioneer = msg.getAuction().getAuctioneer();
		
		for( Enumeration eSB = getStorageBrokers();
				eSB.hasMoreElements();) {
			StorageBroker sb = (StorageBroker) eSB.nextElement();

			if( (sb != origAuctioneer) || includeOriginator)
				sb.addMessage(msg);
		}
	}
	
	

    /**
     *  Tell a P2P mediator that it should ask all SBs to terminate.
     *  The method will wait for this to happen.
     */
    void shutDownAllSBs() {
	
		for( Enumeration en = _storageBrokerCollection.elements();
		     		en.hasMoreElements();) {
	    	StorageBroker sb = (StorageBroker)en.nextElement();
		
		    sb.shutDownSB();
            _time.gtJoin(sb);
		}
    }


    /**
     * Terminates the thread of this P2P and all the threads of the
     * local StorageBrokers.
     */
    public synchronized void shutDownP2P() {
		_iAmAlive = false;
	    _time.gtNotify(this);
    }

}
