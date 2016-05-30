package org.edg.data.replication.optorsim.auctions;


/**
 * This Message is used to propagate the result of an auction.
 * <p>
 * Copyright (c) 2002 CERN, ITC-irst, PPARC, on behalf of the EU DataGrid.
 * For license conditions see LICENSE file or
 * <a href="http://www.edg.org/license.html">http://www.edg.org/license.html</a>
 * <p>
 */
public class AnnounceMessage extends PropagatingMessage {
    
  	private StorageBroker _winningSB;
  
    /**
     * Creates a new AnnounceMessage from the specified parameters.
     */
    public AnnounceMessage(Auction auction, StorageBroker sb) {
		super(auction);

		_winningSB = sb;
	}

    /****    Suitable get and set methods   *****/ 
    public StorageBroker getWinningSB() {
    	return _winningSB;
    }
    
    public void handler( P2P p2pMediator) {
    	
			// pass the message to all of the local SBs
		p2pMediator.sendToLocalSBs( this, true);
		
			// pass the message on to other sites
		super.handler( p2pMediator);

    }
    
}
