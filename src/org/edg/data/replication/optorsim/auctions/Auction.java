package org.edg.data.replication.optorsim.auctions;

import org.edg.data.replication.optorsim.infrastructure.DataFile;
import org.edg.data.replication.optorsim.infrastructure.OptorSimParameters;
import org.edg.data.replication.optorsim.infrastructure.StorageElement;
import org.edg.data.replication.optorsim.reptorsim.ReplicaManager;
import org.edg.data.replication.optorsim.time.GridTime;
import org.edg.data.replication.optorsim.time.GridTimeFactory;

import java.util.Enumeration;
import java.util.Vector;

/**
 * Auction threads form a pool of threads and sleep when they are not being
 * used. If all threads in the pool are currently in use, a new one is created.
 * <p>
 * An auction is used by an {@link AccessMediator} when it wants to find
 * a file for a ComputingElement or a {@link StorageBroker} when it
 * wants to replicate a file to its {@link StorageElement}. The Auction runs
 * as a Thread. A {@link BidRequestMessage} is propogated via the P2P network
 * to StorageBrokers requesting Bids for the file. The Auction sleeps until
 * a specified timeout, when it opens all the Bids it has received and
 * chooses the StorageBroker replying the lowest bid. Then the Auction waits
 * until the remote file is ready before sending the result to the AM or SB.
 * If the Auction was conducted by a StorageBroker it replicates the file
 * and then sends a ReplicaReadyMessage back to the SB.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 * @since JDK1.4
 */
public class Auction {

    // the counter used to get the unique auction id
    private static int _auctionIDCounter = 0;

	// the maximum nesting level allowed
	public static final int MAX_NESTING_LEVEL = 2;

    // the winning file returned to the auctioneer
	private DataFile _winningFile = null;    

    // the unique id of the auction
    private int _auctionID;

    // contains the bids from the SBs
    private Vector _currentBids = new Vector();

	// A randomly chosen instance of the DataFile
	// This is used to pass metadata about the file	
	private DataFile _sampleDataFile;

    // the auctioneer that started the auction (AM or SB)
    private Auctioneer _auctioneer;

    // the nesting level of this auction (default 0)
    private int _nestingLevel = 0;
    
    // Meaningful name for this auction
    private String _name;

    // an OptorSimParameters instance
    private OptorSimParameters _params;

    // a ReplicaManager instance
    private ReplicaManager _rm;
    
    // If we're replicating, to which SE should we store this file?
    private StorageElement _destSE = null;

    // the bid which wins the auction
    private Bid _winningBid=null;

    // used in case we get the rfd message before we have a chance to wait
    private RFDStatusHandler _myRFDState;

    private GridTime _time = GridTimeFactory.getGridTime();


    public Auction( String lfn,
		    Auctioneer auctioneer,
		    int nestingLevel) {

		_rm = ReplicaManager.getInstance();
		_params = OptorSimParameters.getInstance();
		    	
			// Find the first replica and use it as a "sample"
		DataFile replicas[] = _rm.listReplicas( lfn);
		if( replicas.length < 1) {
			System.out.println( "Auction constructor> cannot find any instance of datafile "+lfn
			                   +".  I was started by auctioneer on "+auctioneer.getSite());
			System.exit(1); // Make this a fatal error, we could ignore it; but there's a bug somewhere
		}
				
		_sampleDataFile = replicas[0];
	    
			// parameters for the new auction
		_auctioneer = auctioneer;
		_auctionID = _auctionIDCounter++;
		_nestingLevel = nestingLevel;
	
		_name = "Auction"+_auctionID;
    }
    
    /**
     * Returns the name of this auction.
     */
    public String toString() {
    	return _name;
    }


    /**
     * Process the auction.
     */
    public void doAuction() {

        Debugger.printDebugMessage(this+"> starting level "+_nestingLevel+" auction for file " + _sampleDataFile.lfn()
	    		+ " for "+_auctioneer);

	    /**	    
	     * Sometimes nested messages will be processed by a (remote)
	     * SB before the 1st level auction. One way around this is to
	     * delay nested auctions by "a little bit" to give the originating
	     * auction RequestForBidMessage a chance. What we do here is to
	     * delay for half of the remaining time, so we centre the nested
	     * auction within its parent auction time-frame, ie
	     *
	     *  [------------- PARENT AUCTION --------------]
	     *  :   [-------- NESTED AUCTION #1 --------]   :
	     *  :   :    [--- NESTED AUCTION #2 ---]    :   :
	     *  :   :    :                         :    :   :
	     * -o---o----o--- increasing time -----o----o---o---->
	     */

        int sleepTime = (int)((_params.getInitialTimeout() - (_params.getInitialTimeout() *
                Math.pow(_params.getTimeoutReductionFactor(),
                     _nestingLevel)))/2);

        _time.gtSleep(sleepTime);

			// discover the auctioneer's P2P mediator
		P2P myP2P = P2PManager.find(  _auctioneer.getSite());
	   
		    // send out a request for bids for the file
	    BidRequestMessage bidMsg = new BidRequestMessage(this);
	    myP2P.acceptMessage(bidMsg);

		    // wait for replies
        int waitTime = (int)(_params.getInitialTimeout() *
                Math.pow(_params.getTimeoutReductionFactor(),
                     _nestingLevel));
        _time.gtSleep(waitTime);
        
			//select the winning bid
		_winningBid = getWinningBid();
			
			// If noone won the auction, quit early
		if( _winningBid == null) {
			Debugger.printDebugMessage( this+"> no winner found!");
			if( _auctioneer instanceof StorageBroker)
				((StorageBroker)_auctioneer).auctionComplete( this, null);
			_auctioneer.fileAvailable( this, null);
			return;
		}
			
			//announce who won this auction
		StorageBroker winningSB = _winningBid != null ? _winningBid.storageBroker() : null;
		AnnounceMessage annMsg = new AnnounceMessage(this,  winningSB);
		myP2P.acceptMessage( annMsg);

//		 Inform the Auctioneer of who won.
        if( _auctioneer instanceof StorageBroker)
                ((StorageBroker) _auctioneer).auctionComplete( this, _winningBid != null ? _winningBid.storageBroker() : null);
			
        //wait until the file is ready on the remote site
		_myRFDState.waitForRFD(_winningBid);

			/**
			* This is a cheat.  If _destSE is not null, then a SB wants this file on its SE.  Since we
			* don't want to block the SB, we do the replication here.
			*/
		boolean replicated = false;
		if( _destSE != null) {
			Debugger.printDebugMessage( this+"> starting to replicate, winning file is " + _winningFile);
			DataFile replica = _rm.replicateFile( _winningFile, _destSE);
			if( replica != null)
				replicated = true;
		}	
	    
		// Inform the Auctioneer of who won.
//			if( _auctioneer instanceof StorageBroker)
//				((StorageBroker) _auctioneer).auctionComplete( this, replicated == true ? _winningBid.storageBroker() : null);
	    
	 	    // inform the auctioneer that the auction is finished
	 	if(_auctioneer instanceof StorageBroker) {
		 	if( replicated == true)
				_auctioneer.fileAvailable( this, _winningFile);
			else _auctioneer.fileAvailable(this,null);
	 	}
	 	else _auctioneer.fileAvailable( this, _winningFile);
	 	
    } // doAuction


    /**
     * The bid reply message handler calls addBid(), which
     * records the bids received.
     */
    public void addBid(Bid bid) {
		Debugger.printDebugMessage(this+"> adding bid of "+
				   bid.getOffer()+" from "+bid.storageBroker());
		synchronized( _currentBids) {
			_currentBids.add(bid);
		}
    }

    /**
     * Called when the file is ready at the winning SB
     */
    public void receiveRFDMessage( ReadyForDownloadMessage msg) {
		_winningFile = msg.file();
		_myRFDState.messageReceived();
		Debugger.printDebugMessage( this+"> received rfd message");
    }

    /**
     * The name of the file the auction is for. Use getWinningFileName()
     * to get the FileName object of the winning file.
     * @return The LFN which is being auctioned for.
     */
    public String getLFN() {
		return _sampleDataFile.lfn();
    }

    public DataFile getWinningFile() {
		if (_winningBid == null)
			return null;
		return _winningFile;
    }

	/**
	 * Get a sample DataFile.  This allows any metadata stored in the DataFile
	 * to be recovered
	 * @return A DataFile that would satisfy the auction
	 */
	public DataFile getSampleDataFile() {
		return _sampleDataFile;
	}

	/**
	 * Get the auctioneer (either an SB or an AM) for this auction
	 * @return Auctioneer conducting the Auction.
	 */
    public Auctioneer getAuctioneer() {
		return _auctioneer;
    }

   /**
    * Return the nesting level of this auction. 
    */
    public int getNestingLevel() {
		return _nestingLevel;
    }

    /**
     * Set the state of this auction to Replicating. 
     * @param se The StorageElement to which the file is being replicated.
     */
    public void setIsReplicating(StorageElement se) {
		_destSE = se;
    }

    private Bid getWinningBid() {
		Bid winner = null;
		float minBid = _auctioneer.getMaxPrice();
		float secondMinBid = minBid;

		synchronized( _currentBids) {	
			for( Enumeration en =_currentBids.elements(); en.hasMoreElements();) {
	    		Bid bid = (Bid)en.nextElement();
	    		if (bid == null) {
	    			Debugger.printDebugMessage(this+"> received null bid from someone");
	    			continue;
	    		}
				float offer = bid.getOffer();
				if(offer < minBid) {
				   	secondMinBid = minBid;
			    	minBid = offer;
		   			winner = bid;
				}

				if(offer < secondMinBid  &&  offer > minBid) {
			    	secondMinBid = offer;
	    		}
			}
		}
		
		if(winner != null) 
	    	Debugger.printDebugMessage(this +"> "
	    			       +winner.storageBroker()
	    			       +" wins at price "+secondMinBid);
		
		_currentBids.remove(winner);
		
		return winner;
    }

    /**
     * Assign a RFDStatusHandler, which handles the 
     * status of the Ready For Download message, to this auction.
     * @param rfdStatusHandler
     */
    protected void assignRFDSH(RFDStatusHandler rfdStatusHandler) {
        _myRFDState = rfdStatusHandler;
    }

}


/**
 * Handles the status of the RFD message - whether we have received
 * it or not. The Auction waits synchronised on this until it gets
 * the message.
 */
class RFDStatusHandler {
    private boolean _receivedRFD = false;
    private GridTime _time = GridTimeFactory.getGridTime();

    /**
     * The timeout for the ReadyForDownload Message.
     * Set at 100 minutes.
     */ 
    public static final int RFDTIMEOUT = 100 * 60 * 1000; // 100 min

    /**
     * Method to set the "received ReadyForDownload message" state to true and
     * notify the auction.
     */
    protected synchronized void messageReceived() {
		_receivedRFD = true;
		_time.gtNotify(this);
    }
    
    protected RFDStatusHandler() {
    }

    /**
     * Set the Auction to wait until the ReadyForDownload message is received or
     * the timeout expires. 
     * @param winningBid The Bid which has won the auction, the file
     * from which the auction is waiting to download.
     */
    protected synchronized void waitForRFD(Bid winningBid) {
		// wait for the ReadyForDownload message or timeout expires
		if(winningBid != null && !_receivedRFD) {

	    	// take out the timeout when we know this works
            _time.gtWait(this, RFDTIMEOUT);

            if( !_receivedRFD)
				System.out.println( " waiting for RFD message timed out!");

		}
        _receivedRFD = false;
    }
}
