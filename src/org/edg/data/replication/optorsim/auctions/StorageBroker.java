package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.*;
import org.edg.data.replication.optorsim.optor.FileWorthStorageElement;
import org.edg.data.replication.optorsim.reptorsim.*;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.*;


/**
 * StorageBroker class is a thread which handles the auctions
 * on behalf of a {@link StorageElement}. One of these threads is started
 * for each StorageElement present on a site. It can receive
 * Auction objects coming from other threads, send bids in
 * response to auctions, and start new auctions to try to find a file
 * of which there is not a replica in the local storage.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class StorageBroker extends Thread implements Auctioneer {


    // contains the current nested auctions pending result
    // maps file name to auction
    private Map _nestedAuctions = new Hashtable();

    // contains the current replications to this SE
    // maps file name to nested auction
    private Map _currentReplications = new Hashtable();

    // auctions we have yet to reply to
    private Set _auctionsBidIn = new LinkedHashSet();

    // auctions we have won for which we are waiting for the file
    private Set _auctionsPendingReplication = new LinkedHashSet();

   // the inbox of messages waiting to be read
    private List _messageQueue = new Vector();

    // made false when shutting down SB
    private boolean _iAmAlive;

   // the SE to which this is the broker
    private StorageElement _se;

    // the local p2p mediator to communicate messages
    private P2P _p2pMediator;

    // the max price this SB will pay in a nested auction
    private float _maxPrice = Float.MAX_VALUE;

    // Instance of GridTime
    private GridTime _time = GridTimeFactory.getGridTime();

	/**
	 * Construct a StorageBroker. Note caller must also start() this thread
	 * @param id The numerical counter for this thread. Only used for display purposes
	 * @param se The StorageElement associated with this SB
	 * @param p2p The P2PMediator used for communication
	 */
    public StorageBroker( int id, StorageElement se, P2P p2p) {
        super(GridTimeFactory.getThreadGroup(), "sb"+id+"@"+ se.getGridSite());
		_se = se;
		_p2pMediator = p2p;
        _iAmAlive = true;

		// our caller starts us off
    }

    /**
     * Run the StorageBroker thread until it is shut down and finished
     * replicating files.
     */
    public void run() {
		List msgToProcess = new LinkedList();

		while (_iAmAlive || !_currentReplications.isEmpty()) {

	    	standBy();

			synchronized( _messageQueue) {
				while (!_messageQueue.isEmpty()) {
					Message msg = (Message)_messageQueue.get(0);
		    		_messageQueue.remove(msg);
		    	
					msgToProcess.add( msg);
			    }
			}

			for( Iterator i = msgToProcess.iterator(); i.hasNext();) {
				Message msg = (Message) i.next();
				if( msg instanceof BidRequestMessage) 
					handleBidRequestMessage( msg );
				else if( msg instanceof AnnounceMessage) {
					Auction announcedAuction = msg.getAuction();

							// If the SB bid in this auction, process the message
						if( isBidderIn( announcedAuction))
							processAnnounceAsBidder( (AnnounceMessage) msg);
				} else {
					System.out.println( "DEBUG> Assert failed, StorageBroker received message "+msg);
				}
					
			}

			msgToProcess.clear();
		}
	}

    /**
     * The SB waits until it receives a message
     */
    private synchronized void standBy() {
        synchronized( _messageQueue) {
            // If there are messages to process, return immediately.
            if( _messageQueue.size() != 0)
                return;
        }
        _time.gtWait(this);
    }

    /**
     * Method to shut down this StorageBroker.
     */
    public synchronized void shutDownSB() {
		_iAmAlive = false;
        _time.gtNotify(this);
    }

    /**
     * Add a message to the message queue
     * and notify the StorageBroker.
     */
    public synchronized void addMessage(Message msg) {
		synchronized( _messageQueue) {
			_messageQueue.add(msg);
        }
		_time.gtNotify(this);
    }

    /**
     * The name of this StorageBroker.
     */
    public String toString() {
		return getName();
    }

    /**
     * The site where this StorageBroker is located.
     * */
    public GridSite getSite() {
		return _se.getGridSite();
    }

    /**
     * The {@link StorageElement} for which this StorageBroker is responsible.
     * @return The StorageElement local to this StorageBroker.
     */
    public StorageElement getLocalSE() {
		return _se;
    }

    /**
     * The maximum price which this StorageBroker is willing to pay
     * in a single bid.
     */
    public float getMaxPrice() {
		return _maxPrice;
    }

    /********* Methods to handle the incoming messages **********/

    private void handleBidRequestMessage( Message brm) {
	
    	Auction auction = brm.getAuction();

	String lfn = auction.getSampleDataFile().lfn();


        // if we have the file, pin it and send a BidReplyMessage
	if( _se.hasFile(lfn)) {
		
	    DataFile file = _se.requestFile( lfn);
	    file.addPin();

	    synchronized( _auctionsBidIn) {
		_auctionsBidIn.add( auction);
	    }

	    Bid bid = getBid( auction);
	    BidReplyMessage bidReplyMessage = new BidReplyMessage(auction, bid);
	    _p2pMediator.acceptMessage(bidReplyMessage);
	    
	    Debugger.printDebugMessage(this+"> local file satisfies "
					+auction+", bidding "+bid.getOffer());
	    return;
	}

	// if we are already replicating the file we can still
	// send a BidReplyMessage in the knowledge we will 
	// have the file soon
	if( _currentReplications.containsKey(lfn)) {

	    synchronized( _auctionsBidIn) {
		_auctionsBidIn.add( auction);
	    }

	    Bid bid = getBid( auction);
	    BidReplyMessage bidReplyMessage =
		new BidReplyMessage( auction, bid);
	    
	    Debugger.printDebugMessage(this+"> ongoing replication satisfies "
					+auction+", bidding "+bid.getOffer());
	    _p2pMediator.acceptMessage(bidReplyMessage);
	    
	    return;
	}

	// Evaluate whether it is worth replicating the file locally.
	// If it is then start a nested auction

	// OPEN ISSUE: In real life an auction has to be performed
	// before we know if it is worth replicating the file. This
	// will create lots of auction traffic / threads in simulation

	if( shouldStartNestedAuction( auction) &&
	    economicallyOptimalToReplicate(auction)) {
		 			
	    synchronized( _auctionsBidIn) {
		_auctionsBidIn.add( auction);
	    }
			
	    Auction nestedAuction = new Auction( auction.getLFN(), this, auction.getNestingLevel()+1);			
	    nestedAuction.setIsReplicating( _se);
	    _nestedAuctions.put(lfn, nestedAuction);
	    
	    Debugger.printDebugMessage(this+"> Starting "+
					nestedAuction+" (level "
					+nestedAuction.getNestingLevel()+") for "
					+lfn+" reacting to "+auction);
			       
	    AuctionThreadPool.getThread( nestedAuction);
	}
    }



	/**
	 * Returns true if this SB is bidding in this auction, false otherwise.
	 */
	private boolean isBidderIn( Auction auction) {
	    boolean biddingIn;
	    synchronized(_auctionsBidIn) {
		biddingIn = _auctionsBidIn.contains( auction);
	    }
	    return biddingIn;
    }

   
   	/**
   	 * We've received the result of an auction we were bidding in.  We
   	 * now take care of the consequences.
   	 * @param msg The AnnounceMessage about the Auction
   	 */ 
    private void processAnnounceAsBidder( AnnounceMessage msg) {
		Auction announcedAuction = msg.getAuction();
		String lfn = announcedAuction.getLFN();
		StorageBroker sb = msg.getWinningSB();

		//  We're no longer waiting on the outcome ...
		synchronized(_auctionsBidIn) {
		    _auctionsBidIn.remove( announcedAuction);   
		}

		// if we've lost the auction release the pin and return
		if( sb == null || !sb.equals(this)) {
			if( _se.hasFile( lfn) )
				_se.requestFile( lfn).releasePin();
				
			return;
		}

		// if the file is already local immediately send a RFD message
		if( _se.hasFile( lfn)) {
			DataFile file = _se.requestFile( lfn);

			Debugger.printDebugMessage(this+"> I won auction "+
					   announcedAuction+", file is local so sending RFD message");

			ReadyForDownloadMessage rfdMessage =
		   					new ReadyForDownloadMessage( announcedAuction,  file);
			_p2pMediator.acceptMessage(rfdMessage);
				
			return;
		}


		// If we are currently replicating the file add to auctions pending file
	   	if(_currentReplications.containsKey( lfn)) {
			Debugger.printDebugMessage(this+"> won "+ announcedAuction
							   +", but still replicating, updating pending file");

			synchronized( _auctionsPendingReplication) {
				_auctionsPendingReplication.add( announcedAuction);
			}
			
			return;
		}
	    
	    	// If we've got here, something's wrong    
		System.out.println(this+"> BUG won "+announcedAuction+
				   " without having the file or being in the process"+
				   " of replicating it");
 	}


	/**
	 * Called when this StorageBroker has received the result of its nested auction.  This means it can
	 * bid in all the auctions that want this file, if its auction found a winner.
	 */
	public void auctionComplete( Auction thisAuction, StorageBroker winningSB) {
		String lfn = thisAuction.getLFN();
		Debugger.printDebugMessage(this+"> hello? why am I not bidding?");
		// we're no longer waiting on this auction to finish.
		_nestedAuctions.remove( lfn);

		// If nobody won our auction, we can't do anything...
		if( winningSB == null) {
			Debugger.printDebugMessage( this+"> Nested auction "+ thisAuction+ " didn't result in a winner.");
			return;
		}

		// Put the file we are now replicating into _currentReplications
		_currentReplications.put( lfn, thisAuction);
	   
		// Search through the list of auctions we are participating in
		// for any which were waiting on the result of this nested auction
		// before we sent a bid.
		synchronized(_auctionsBidIn) {
			
		    for( Iterator i = _auctionsBidIn.iterator(); i.hasNext();) {
	   		Auction participatingAuction = (Auction) i.next();
	   		
			// Skip over any auctions for a different LFN
	   		if( !participatingAuction.getLFN().equals( lfn))
	   			continue;

			// Figure out how much we want to bid, and send off the offer
			Bid bid = getBid( participatingAuction);
			BidReplyMessage bidReplyMessage = new BidReplyMessage( participatingAuction, bid);
			_p2pMediator.acceptMessage(bidReplyMessage);	
		    
			Debugger.printDebugMessage(this+"> Nested auction "+
						    thisAuction+" complete, sending"
						    +" bid of "+bid.getOffer()+" to auction "
						    + participatingAuction);
		    }
		}
	} // processAuctionAsAuctioneer


	/**
	 * Called when an auction has resulted in an available file.
	 */
    public void fileAvailable( Auction finishedAuction, DataFile winningFile) {
    	String lfn = finishedAuction.getLFN();
		DataFile file = _se.requestFile( lfn);

			// If noone won this auction, don't do anything
		if( winningFile == null || file == null)
			return;

        winningFile.releasePin(); //Gimun's fix

			// since we've received this message, the replica must be ready.
		_currentReplications.remove( lfn);

			// if any auctions were waiting for this file (should be at least one) tell them the file's available
		synchronized( _auctionsPendingReplication) {
			for(Iterator i = _auctionsPendingReplication.iterator(); i.hasNext();) {
			    Auction queuedAuction = (Auction) i.next();

			    // skip over auctions for different files	    	
			    if( !queuedAuction.getLFN().equals( lfn))
	    			continue;
	    
			    // Remove this auction from the _auctionsPendingFile set
			    i.remove();

			    // Pin it for the auction
			    file.addPin();

			    // Send the ReadyForDownload message to the auctioneer
			    Debugger.printDebugMessage(this+"> Replica ready, sending RFD to "
							+queuedAuction);
			    ReadyForDownloadMessage rfdMessage = new ReadyForDownloadMessage(queuedAuction, file);
			    _p2pMediator.acceptMessage(rfdMessage);
			}
		}

		// if we have bid in any auctions for this file for which we
		// not received the result yet place a pin for each one. The pin
		// status when we receive each auction result will be the same as if
		// the file was already local when we bid.
		synchronized( _auctionsBidIn) {
		    for( Iterator i = _auctionsBidIn.iterator(); i.hasNext();) {
	   		Auction participatingAuction = (Auction) i.next();
	   		
			// Skip over any auctions for a different LFN
	   		if( !participatingAuction.getLFN().equals( lfn))
	   			continue;
			file.addPin();
		    }
		}

		// release the pin the DataFile has by default
		file.releasePin();
    }

    /********** end of message handling methods ************/


    private Bid getBid(Auction auction) {

	NetworkClient nClient = new NetworkClient();
	DataFile sampleFile = auction.getSampleDataFile();
		
	NetworkCost nc = nClient.getNetworkCosts(this.getSite(),
						 auction.getAuctioneer().getSite(),
						 sampleFile.size());

	// if we are currently replicating the file add on the extra cost
	// commented out to bias bids from local SBs
	/*
	if(_currentReplications.containsKey(auction.getFileName())) {

	    Auction nestedAuction = 
		(Auction)_currentReplications.get(auction.getFileName());

	    NetworkCost extraNetworkCost = 
		networkClient.getNetworkCosts(nestedAuction.getSite(),
					      this.getSite(),
					      nestedAuction.getFileSize());
	    networkCost = new NetworkCost(networkCost.getCost()+extraNetworkCost.getCost(),
					  networkCost.getError(),
					  networkCost.getRouteIndex());
	}
	*/
		return new Bid(this, nc);
    }

    /**
     * Returns true if the internal logic says we are allowed to start a nested auction
     * Current implementation simply checks if the calling site is local
     */
    private boolean shouldStartNestedAuction( Auction auction) {
    	if( auction.getNestingLevel() >= Auction.MAX_NESTING_LEVEL)
			return false;
		if(auction.getAuctioneer().getSite() == getSite())
			return true;
		return false;
    }


    /**
     * Evaluate if creating a replica of the file would be economically
     * viable.
     */
    private boolean economicallyOptimalToReplicate(Auction auction) {

        ReplicaManager rm = ReplicaManager.getInstance();
        DataFile newFile = auction.getSampleDataFile();

        // If there is space for the file in the SE, always replicate
        // TODO: do we want to do this. It shouldn't matter in the long-run, but
        // may lead to a sub-optimal state at the initial stages of simulation

        if (newFile.size() <= _se.getAvailableSpace()){
            Debugger.printDebugMessage(this + "> enough space, so replicate anyway");
            return true;
        }

        // check we could create enough space by deleting files if we need to
        if(!_se.isTherePotentialAvailableSpace(newFile))
            return false;

        // Now comes a slightly tricky bit. We want the collection of files with
        // the lowest combined value such that their combined size is greater or
        // equal to the size of the file we want to replicate.

        // In general, this is a "difficult problem", so we cheat and use a heuristic:
        // just keep selecting the cheapest file and adding up the combined file size
        // stopping when we get enough disk space.

        List leastValuableFiles = _se.filesToDelete(newFile);
        if( leastValuableFiles == null)
            return false;

        double deleteableFilesValue = 0;
        for(Iterator i = leastValuableFiles.iterator(); i.hasNext();) {
            DataFile file = (DataFile)i.next();
            deleteableFilesValue += file.lastEstimatedValue();
        }

        double futureFileValue = 0;
        if (_se instanceof FileWorthStorageElement)
           futureFileValue =
                   ((FileWorthStorageElement)_se).evaluateFileWorth(newFile.fileIndex());

        // If estimated future revenue from owning this file not greater
        // than cost of deleting files, don't replicate.
        // TODO > or >=? >= would encourage more replication

        if( deleteableFilesValue > futureFileValue) {
            Debugger.printDebugMessage( this + "> chose not to replicate file."+
                            " File(s) too valuable");
            return false;
        }

        // Delete the condemned files
        System.out.println(this+"> Deleting files "+leastValuableFiles+
                   ", worth "+deleteableFilesValue+" to replicate "+auction.getLFN()+
                   " worth "+futureFileValue);

        for(Iterator i = leastValuableFiles.iterator(); i.hasNext();) {
            rm.deleteFile( (DataFile)i.next());
        }

        // Check that we still have enough space to replicate the file
        if( _se.getAvailableSpace() < newFile.size()) {
            Debugger.printDebugMessage(this+ "> ERROR: failed to free up enough space.");
            return false;
        }

        // If we get here, we've decided to replicate.
        return true;
    } /* economicallyOptimalToReplicate */

}

